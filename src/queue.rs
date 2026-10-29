//! The work queue: calls waiting for the engine, worked off the event thread.
//!
//! Live calls go first (oldest first), then retries that are due, then archive
//! work (a backfill or a retranscription) on one worker at a time, so the
//! recorder's live calls never wait behind a backfill. Every live job ends in
//! exactly one `call.result`; archive and retranscribe jobs send none.

use std::collections::{HashSet, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use trunk_recorder_plugin::{ConcludedCall, Host, Outcome};

/// Where a job came from.
#[derive(Debug, Clone, PartialEq)]
pub enum Kind {
    /// A `call.concluded`: answered with a `call.result`.
    Live,
    /// A backfill of a call recorded before.
    Archive,
    /// A call transcribed again on request.
    Retranscribe,
}

impl Kind {
    /// Archive and retranscribe jobs share the one archive worker and send no `call.result`.
    fn is_archive(&self) -> bool {
        *self != Kind::Live
    }
}

#[derive(Debug, Clone)]
pub struct Job {
    pub call: ConcludedCall,
    pub kind: Kind,
    /// When it was pushed (a retry keeps it).
    pub enqueued: Instant,
    /// Retries so far.
    pub tries: u32,
}

/// What became of one try at a job.
#[derive(Debug, Clone, PartialEq)]
pub enum Done {
    Ok {
        took: Duration,
    },
    Skipped(String),
    Failed(String),
    /// The engine is down for now: try again later.
    Retry(String),
}

#[derive(Debug, Clone)]
pub struct QueueOptions {
    /// Worker threads.
    pub workers: usize,
    /// Jobs waiting (ready or due for retry) before the oldest is dropped.
    pub max: usize,
    /// The wait before each retry (2 s, 8 s, 30 s); after the last, the job gives up.
    pub retry_after: Vec<Duration>,
    /// A job older than this when a worker takes it fails with "engine down" (50 min).
    pub max_age: Duration,
}

/// The queue's state and its counters.
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    /// Waiting: `live + archive + retrying`.
    pub queued: usize,
    /// Live jobs ready to go.
    pub live: usize,
    /// Archive and retranscribe jobs ready to go.
    pub archive: usize,
    /// Jobs waiting for a retry.
    pub retrying: usize,
    pub in_flight: usize,
    /// Retries scheduled.
    pub retries: u64,
    pub ok: u64,
    /// Jobs that ended failed, for whatever reason.
    pub failed: u64,
    /// Engine time of `Ok` jobs, an EWMA (alpha 0.2).
    pub latency_ms: Option<f64>,
    /// Unix seconds.
    pub last_ok_s: Option<u64>,
    /// Unix seconds of the engine's last `Retry` or `Failed`.
    pub last_error_s: Option<u64>,
    pub last_error: String,
    /// The engine's `Retry`/`Failed` answers in a row (an `Ok` resets it).
    pub in_a_row: u32,
    /// Since `in_a_row` reached 3 (an `Ok` clears it).
    pub down_since: Option<Instant>,
}

const LATENCY_ALPHA: f64 = 0.2;
const DOWN_AFTER: u32 = 3;
const FULL_WARN_EVERY: Duration = Duration::from_secs(60);

type Work = dyn Fn(&Job) -> Done + Send + Sync;
type Failed = dyn Fn(&Job, &str) + Send + Sync;

/// How a job ended.
enum End {
    Ok(Duration),
    Skipped(String),
    Failed(String),
}

#[derive(Default)]
struct Inner {
    live: VecDeque<Job>,
    archive: VecDeque<Job>,
    /// Jobs waiting for a retry, with when it is due.
    retrying: Vec<(Instant, Job)>,
    /// The paths queued or in flight.
    paths: HashSet<String>,
    in_flight: usize,
    /// A worker is on an archive job.
    archive_busy: bool,
    closing: bool,
    last_full_warn: Option<Instant>,
    /// The counters (its queue fields are filled in by [`Queue::snapshot`]).
    stats: Snapshot,
}

impl Inner {
    fn waiting(&self) -> usize {
        self.live.len() + self.archive.len() + self.retrying.len()
    }

    /// The next job for a worker: live first, then a due retry, then archive
    /// work; an archive job (ready or retrying) only while no other worker is on one.
    fn take(&mut self, now: Instant) -> Option<Job> {
        if let Some(job) = self.live.pop_front() {
            return Some(job);
        }
        let archive_free = !self.archive_busy;
        let due =
            self.retrying.iter().enumerate().filter(|(_, (at, job))| *at <= now && (archive_free || !job.kind.is_archive())).min_by_key(|(_, (at, _))| *at);
        if let Some((i, _)) = due {
            return Some(self.retrying.swap_remove(i).1);
        }
        if archive_free { self.archive.pop_front() } else { None }
    }

    /// When the next retry not yet due falls due.
    fn next_due(&self, now: Instant) -> Option<Instant> {
        self.retrying.iter().map(|(at, _)| *at).filter(|at| *at > now).min()
    }

    /// Takes the oldest waiting job of one sort (archive or live), ready or waiting for a retry.
    fn take_oldest(&mut self, archive: bool) -> Option<Job> {
        let ready = if archive { &mut self.archive } else { &mut self.live };
        let retry = self.retrying.iter().enumerate().filter(|(_, (_, job))| job.kind.is_archive() == archive).min_by_key(|(_, (_, job))| job.enqueued);
        match (ready.front(), retry) {
            (Some(r), Some((i, (_, job)))) if job.enqueued < r.enqueued => Some(self.retrying.swap_remove(i).1),
            (Some(_), _) => ready.pop_front(),
            (None, Some((i, _))) => Some(self.retrying.swap_remove(i).1),
            (None, None) => None,
        }
    }

    /// The engine said `Retry` or `Failed`.
    fn engine_error(&mut self, message: &str) {
        let s = &mut self.stats;
        s.last_error = message.to_string();
        s.last_error_s = Some(unix_now());
        s.in_a_row += 1;
        if s.in_a_row >= DOWN_AFTER && s.down_since.is_none() {
            s.down_since = Some(Instant::now());
        }
    }

    fn engine_ok(&mut self, took: Duration) {
        let s = &mut self.stats;
        let ms = took.as_secs_f64() * 1000.0;
        s.latency_ms = Some(s.latency_ms.map_or(ms, |l| l + LATENCY_ALPHA * (ms - l)));
        s.last_ok_s = Some(unix_now());
        s.ok += 1;
        s.in_a_row = 0;
        s.down_since = None;
    }

    /// The worker that was on `archive` work is done with it.
    fn release(&mut self, archive: bool) {
        self.in_flight -= 1;
        if archive {
            self.archive_busy = false;
        }
    }
}

fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

struct Shared {
    host: Host,
    opts: QueueOptions,
    work: Box<Work>,
    failed: Box<Failed>,
    inner: Mutex<Inner>,
    changed: Condvar,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn worker(&self) {
        let mut inner = self.lock();
        loop {
            if inner.closing {
                return;
            }
            let now = Instant::now();
            if let Some(job) = inner.take(now) {
                inner.in_flight += 1;
                inner.archive_busy |= job.kind.is_archive();
                drop(inner);
                self.run(job);
                inner = self.lock();
                continue;
            }
            inner = match inner.next_due(now) {
                Some(at) => self.changed.wait_timeout(inner, at - now).unwrap_or_else(|e| e.into_inner()).0,
                None => self.changed.wait(inner).unwrap_or_else(|e| e.into_inner()),
            };
        }
    }

    /// Works `job` (taken, and counted in flight) to its end or its next retry.
    fn run(&self, mut job: Job) {
        let archive = job.kind.is_archive();
        let end = if job.enqueued.elapsed() > self.opts.max_age {
            self.lock().stats.failed += 1;
            End::Failed("engine down".into())
        } else {
            let done = catch_unwind(AssertUnwindSafe(|| (self.work)(&job))).unwrap_or_else(|_| Done::Failed("the work panicked".into()));
            let mut inner = self.lock();
            match done {
                Done::Ok { took } => {
                    inner.engine_ok(took);
                    End::Ok(took)
                }
                Done::Skipped(m) => End::Skipped(m),
                Done::Failed(m) => {
                    inner.engine_error(&m);
                    inner.stats.failed += 1;
                    End::Failed(m)
                }
                Done::Retry(m) => {
                    inner.engine_error(&m);
                    let tries = job.tries as usize;
                    if inner.closing {
                        inner.stats.failed += 1;
                        End::Failed("shutting down".into())
                    } else if tries < self.opts.retry_after.len() {
                        let message = format!("retry {}/{} for {}: {m}", tries + 1, self.opts.retry_after.len(), job.call.path);
                        job.tries += 1;
                        inner.stats.retries += 1;
                        inner.retrying.push((Instant::now() + self.opts.retry_after[tries], job));
                        inner.release(archive);
                        self.changed.notify_all();
                        drop(inner);
                        self.host.warn(message);
                        return;
                    } else {
                        inner.stats.failed += 1;
                        End::Failed(format!("gave up: {m}"))
                    }
                }
            }
        };
        self.answer(&job, &end);
        let mut inner = self.lock();
        inner.paths.remove(&job.call.path);
        inner.release(archive);
        self.changed.notify_all();
    }

    /// Tells the plugin of a failure (first, so its record exists), then the
    /// recorder of a live job's end. Called with no lock held.
    fn answer(&self, job: &Job, end: &End) {
        let live = job.kind == Kind::Live;
        let path = job.call.path.as_str();
        match end {
            End::Ok(took) if live => self.host.call_result_took(path, Outcome::Ok, "", "", *took),
            End::Skipped(m) if live => self.host.call_result(path, Outcome::Skipped, m.as_str(), ""),
            End::Failed(m) => {
                let _ = catch_unwind(AssertUnwindSafe(|| (self.failed)(job, m)));
                if live {
                    self.host.call_result(path, Outcome::Failed, m.as_str(), "");
                }
            }
            _ => {}
        }
    }
}

/// A bounded priority queue of calls with its worker threads.
pub struct Queue {
    shared: Arc<Shared>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

impl Queue {
    /// Starts `opts.workers` threads (`transcribe-<i>`) running `work` on each
    /// job; `failed` hears of every job that ends failed, on the thread that
    /// ends it, before its `call.result`.
    pub fn start(
        host: Host,
        opts: QueueOptions,
        work: impl Fn(&Job) -> Done + Send + Sync + 'static,
        failed: impl Fn(&Job, &str) + Send + Sync + 'static,
    ) -> Queue {
        let workers = opts.workers;
        let shared =
            Arc::new(Shared { host, opts, work: Box::new(work), failed: Box::new(failed), inner: Mutex::new(Inner::default()), changed: Condvar::new() });
        let workers = (0..workers)
            .map(|i| {
                let s = shared.clone();
                thread::Builder::new().name(format!("transcribe-{i}")).spawn(move || s.worker()).expect("start a queue worker")
            })
            .collect();
        Queue { shared, workers: Mutex::new(workers) }
    }

    /// Queues `call`. False (and nothing happens) when a job with its path is
    /// queued or in flight. When `max` jobs are waiting, the oldest archive
    /// job is dropped ("queue full"), else the oldest live one. Once shutdown
    /// has begun, the job is answered at once with "shutting down".
    pub fn push(&self, call: ConcludedCall, kind: Kind) -> bool {
        let s = &self.shared;
        let job = Job { call, kind, enqueued: Instant::now(), tries: 0 };
        let mut inner = s.lock();
        if inner.paths.contains(&job.call.path) {
            return false;
        }
        if inner.closing {
            inner.stats.failed += 1;
            drop(inner);
            s.answer(&job, &End::Failed("shutting down".into()));
            return true;
        }
        let mut dropped = None;
        let mut warn = false;
        if inner.waiting() >= s.opts.max
            && let Some(old) = inner.take_oldest(true).or_else(|| inner.take_oldest(false))
        {
            inner.paths.remove(&old.call.path);
            inner.stats.failed += 1;
            let now = Instant::now();
            warn = inner.last_full_warn.is_none_or(|t| now.duration_since(t) >= FULL_WARN_EVERY);
            if warn {
                inner.last_full_warn = Some(now);
            }
            dropped = Some(old);
        }
        inner.paths.insert(job.call.path.clone());
        if job.kind.is_archive() {
            inner.archive.push_back(job)
        } else {
            inner.live.push_back(job)
        }
        s.changed.notify_all();
        drop(inner);
        if let Some(old) = dropped {
            if warn {
                s.host.warn(format!("queue full ({} waiting): dropping the oldest, {}", s.opts.max, old.call.path));
            }
            s.answer(&old, &End::Failed("queue full".into()));
        }
        true
    }

    pub fn snapshot(&self) -> Snapshot {
        let inner = self.shared.lock();
        Snapshot {
            queued: inner.waiting(),
            live: inner.live.len(),
            archive: inner.archive.len(),
            retrying: inner.retrying.len(),
            in_flight: inner.in_flight,
            ..inner.stats.clone()
        }
    }

    /// Jobs queued and in flight.
    pub fn len(&self) -> usize {
        let inner = self.shared.lock();
        inner.waiting() + inner.in_flight
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Stops taking work: every waiting job is answered "shutting down", then
    /// the jobs in flight get up to 0.8 × `grace` to finish.
    pub fn shutdown(&self, grace: Duration) {
        let s = &self.shared;
        let deadline = Instant::now() + grace.mul_f64(0.8);
        let waiting: Vec<Job> = {
            let mut guard = s.lock();
            let inner = &mut *guard;
            inner.closing = true;
            let retrying = std::mem::take(&mut inner.retrying).into_iter().map(|(_, job)| job);
            let mut jobs: Vec<Job> = inner.live.drain(..).chain(inner.archive.drain(..)).collect();
            jobs.extend(retrying);
            for job in &jobs {
                inner.paths.remove(&job.call.path);
            }
            inner.stats.failed += jobs.len() as u64;
            s.changed.notify_all();
            jobs
        };
        for job in &waiting {
            s.answer(job, &End::Failed("shutting down".into()));
        }
        let mut inner = s.lock();
        while inner.in_flight > 0 {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            inner = s.changed.wait_timeout(inner, deadline - now).unwrap_or_else(|e| e.into_inner()).0;
        }
        let idle = inner.in_flight == 0;
        drop(inner);
        if idle {
            let workers = std::mem::take(&mut *self.workers.lock().unwrap_or_else(|e| e.into_inner()));
            for w in workers {
                let _ = w.join();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use trunk_recorder_plugin::{Level, PluginMessage, testing};

    const TAKES: Duration = Duration::from_millis(100);

    fn opts(workers: usize) -> QueueOptions {
        QueueOptions { workers, max: 100, retry_after: vec![Duration::from_millis(20); 3], max_age: Duration::from_secs(3000) }
    }

    fn call(dir: &std::path::Path, tg: u32) -> ConcludedCall {
        testing::call(dir, "sys1", tg)
    }

    /// Polls `cond` until it holds, failing the test after 10 s.
    fn wait_for(what: &str, cond: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !cond() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            thread::sleep(Duration::from_millis(5));
        }
    }

    type Failures = Arc<Mutex<Vec<(String, String)>>>;

    /// A `failed` callback that keeps (path, message).
    fn failures() -> (Failures, impl Fn(&Job, &str) + Send + Sync + 'static) {
        let kept = Failures::default();
        let k = kept.clone();
        (kept, move |job: &Job, m: &str| k.lock().unwrap().push((job.call.path.clone(), m.to_string())))
    }

    /// A gate `work` can block on until the test opens it (by dropping the sender).
    fn gate() -> (mpsc::Sender<()>, Arc<Mutex<mpsc::Receiver<()>>>) {
        let (tx, rx) = mpsc::channel();
        (tx, Arc::new(Mutex::new(rx)))
    }

    #[test]
    fn live_goes_before_archive_and_archive_takes_one_worker() {
        let (host, _out) = testing::capture();
        let started = Arc::new(Mutex::new(Vec::<(Kind, u32, Instant)>::new()));
        let s = started.clone();
        let q = Queue::start(
            host,
            opts(2),
            move |job: &Job| {
                s.lock().unwrap().push((job.kind.clone(), job.call.call.talkgroup, Instant::now()));
                thread::sleep(TAKES);
                Done::Ok { took: TAKES }
            },
            |_: &Job, _: &str| {},
        );
        let dir = testing::temp_dir("queue-order");
        assert!(q.push(call(&dir, 1), Kind::Archive));
        assert!(q.push(call(&dir, 2), Kind::Archive));
        assert!(q.push(call(&dir, 3), Kind::Live));
        wait_for("three starts", || started.lock().unwrap().len() == 3);

        let s = started.lock().unwrap().clone();
        let at = |tg: u32| s.iter().find(|x| x.1 == tg).map(|x| x.2).unwrap();
        assert!(at(3) < at(2), "live 3 starts before archive 2: {s:?}");
        assert!(at(2).duration_since(at(1)) >= TAKES, "archive 1 and 2 overlap: {s:?}");
        assert_eq!(s.iter().find(|x| x.1 == 3).unwrap().0, Kind::Live);
    }

    #[test]
    fn results_go_back_for_live_only() {
        let (host, out) = testing::capture();
        let (failures, on_failed) = failures();
        let worked = Arc::new(Mutex::new(Vec::<u32>::new()));
        let w = worked.clone();
        let q = Queue::start(
            host,
            opts(1),
            move |job: &Job| {
                let tg = job.call.call.talkgroup;
                w.lock().unwrap().push(tg);
                match tg {
                    2 => Done::Skipped("silent".into()),
                    3 => Done::Failed("HTTP 400".into()),
                    _ => Done::Ok { took: Duration::from_millis(7) },
                }
            },
            on_failed,
        );
        let dir = testing::temp_dir("queue-results");
        let calls: Vec<_> = (1..=4).map(|tg| call(&dir, tg)).collect();
        let p: Vec<String> = calls.iter().map(|c| c.path.clone()).collect();
        for (i, c) in calls.into_iter().enumerate() {
            assert!(q.push(c, if i < 3 { Kind::Live } else { Kind::Archive }));
        }
        wait_for("all four done", || worked.lock().unwrap().len() == 4 && q.is_empty());

        let o = out.output();
        assert_eq!(
            o.results(),
            vec![
                (p[0].clone(), Outcome::Ok, String::new(), String::new()),
                (p[1].clone(), Outcome::Skipped, "silent".into(), String::new()),
                (p[2].clone(), Outcome::Failed, "HTTP 400".into(), String::new()),
            ]
        );
        assert_eq!(*failures.lock().unwrap(), vec![(p[2].clone(), "HTTP 400".to_string())]);
        let took = o.messages.iter().find_map(|m| match m {
            PluginMessage::CallResult { path, took_ms, .. } if *path == p[0] => Some(*took_ms),
            _ => None,
        });
        assert_eq!(took, Some(Some(7)));
        let s = q.snapshot();
        assert_eq!((s.ok, s.failed), (2, 1));
        assert!(s.latency_ms.is_some() && s.last_ok_s.is_some());
    }

    #[test]
    fn retries_then_gives_up() {
        let (host, out) = testing::capture();
        let (failures, on_failed) = failures();
        let tries = Arc::new(Mutex::new(0u32));
        let t = tries.clone();
        let q = Queue::start(
            host,
            opts(1),
            move |_: &Job| {
                *t.lock().unwrap() += 1;
                Done::Retry("down".into())
            },
            on_failed,
        );
        let dir = testing::temp_dir("queue-retry");
        let c = call(&dir, 1);
        let p = c.path.clone();
        assert!(q.push(c, Kind::Live));
        wait_for("a result", || !out.output().results().is_empty());

        assert_eq!(*tries.lock().unwrap(), 4);
        let o = out.output();
        let results = o.results();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, p);
        assert_eq!(results[0].1, Outcome::Failed);
        assert!(results[0].2.starts_with("gave up: down"), "{:?}", results[0]);
        assert_eq!(*failures.lock().unwrap(), vec![(p.clone(), "gave up: down".to_string())]);
        let warns: Vec<String> = o.logs().into_iter().filter(|(l, _)| *l == Level::Warn).map(|(_, m)| m).collect();
        assert_eq!(warns, (1..=3).map(|n| format!("retry {n}/3 for {p}: down")).collect::<Vec<_>>());
        let s = q.snapshot();
        assert_eq!(s.retries, 3);
        assert_eq!(s.in_a_row, 4);
        assert!(s.down_since.is_some());
        assert_eq!(s.last_error, "down");
        assert!(s.last_error_s.is_some());
    }

    #[test]
    fn the_queue_full_rule_drops_the_oldest_archive_first() {
        let (host, out) = testing::capture();
        let (failures, on_failed) = failures();
        let (open, shut) = gate();
        let started = Arc::new(Mutex::new(Vec::<u32>::new()));
        let s = started.clone();
        let q = Queue::start(
            host,
            QueueOptions { max: 3, ..opts(1) },
            move |job: &Job| {
                s.lock().unwrap().push(job.call.call.talkgroup);
                let _ = shut.lock().unwrap().recv();
                Done::Ok { took: Duration::ZERO }
            },
            on_failed,
        );
        let dir = testing::temp_dir("queue-full");
        // The one worker blocks on this first job, so everything after it waits
        // ("max" counts waiting jobs, not the one in flight).
        assert!(q.push(call(&dir, 10), Kind::Live));
        wait_for("the worker to be busy", || !started.lock().unwrap().is_empty());
        let a1 = call(&dir, 1);
        let l1 = call(&dir, 2);
        let (pa1, pl1) = (a1.path.clone(), l1.path.clone());
        assert!(q.push(a1, Kind::Archive));
        assert!(q.push(l1, Kind::Live));
        assert!(q.push(call(&dir, 3), Kind::Live));
        assert!(failures.lock().unwrap().is_empty());

        assert!(q.push(call(&dir, 4), Kind::Live));
        assert_eq!(*failures.lock().unwrap(), vec![(pa1.clone(), "queue full".to_string())]);
        assert!(out.output().results().is_empty(), "an archive job gets no call.result");

        assert!(q.push(call(&dir, 5), Kind::Live));
        assert_eq!(*failures.lock().unwrap(), vec![(pa1, "queue full".to_string()), (pl1.clone(), "queue full".to_string())]);
        let o = out.output();
        assert_eq!(o.results(), vec![(pl1, Outcome::Failed, "queue full".into(), String::new())]);
        let warns = o.logs().into_iter().filter(|(l, m)| *l == Level::Warn && m.contains("queue full")).count();
        assert_eq!(warns, 1, "the full warning comes at most once a minute");
        let s = q.snapshot();
        assert_eq!((s.queued, s.live, s.in_flight, s.failed), (3, 3, 1, 2));
        assert_eq!(q.len(), 4);
        drop(open);
    }

    #[test]
    fn a_duplicate_path_is_queued_once() {
        let (host, out) = testing::capture();
        let (open, shut) = gate();
        let worked = Arc::new(Mutex::new(0u32));
        let w = worked.clone();
        let q = Queue::start(
            host,
            opts(1),
            move |_: &Job| {
                *w.lock().unwrap() += 1;
                let _ = shut.lock().unwrap().recv();
                Done::Ok { took: Duration::ZERO }
            },
            |_: &Job, _: &str| {},
        );
        let dir = testing::temp_dir("queue-dup");
        let c = call(&dir, 1);
        assert!(q.push(c.clone(), Kind::Live));
        assert!(!q.push(c.clone(), Kind::Live), "queued or in flight: refused");
        assert!(!q.push(c.clone(), Kind::Archive), "whatever its kind");
        drop(open);
        wait_for("its result", || out.output().results().len() == 1 && q.is_empty());
        assert_eq!(*worked.lock().unwrap(), 1);

        assert!(q.push(c, Kind::Live), "once done, the path can come again");
        wait_for("its second result", || out.output().results().len() == 2);
        assert_eq!(*worked.lock().unwrap(), 2);
    }

    #[test]
    fn an_old_job_is_failed_without_work() {
        let (host, out) = testing::capture();
        let (failures, on_failed) = failures();
        let (open, shut) = gate();
        let worked = Arc::new(Mutex::new(Vec::<u32>::new()));
        let w = worked.clone();
        // Long enough that job 1 is surely taken young on a loaded machine.
        let max_age = Duration::from_millis(200);
        let q = Queue::start(
            host,
            QueueOptions { max_age, ..opts(1) },
            move |job: &Job| {
                w.lock().unwrap().push(job.call.call.talkgroup);
                let _ = shut.lock().unwrap().recv();
                Done::Ok { took: Duration::ZERO }
            },
            on_failed,
        );
        let dir = testing::temp_dir("queue-old");
        let (c1, c2) = (call(&dir, 1), call(&dir, 2));
        let (p1, p2) = (c1.path.clone(), c2.path.clone());
        assert!(q.push(c1, Kind::Live));
        wait_for("job 1 to start", || !worked.lock().unwrap().is_empty());
        assert!(q.push(c2, Kind::Live));
        thread::sleep(max_age * 2); // job 2 ages past max_age while job 1 holds the worker
        drop(open);
        wait_for("both results", || out.output().results().len() == 2);

        assert_eq!(*worked.lock().unwrap(), vec![1]);
        assert_eq!(*failures.lock().unwrap(), vec![(p2.clone(), "engine down".to_string())]);
        assert_eq!(out.output().results(), vec![(p1, Outcome::Ok, String::new(), String::new()), (p2, Outcome::Failed, "engine down".into(), String::new())]);
    }

    #[test]
    fn shutdown_fails_what_waits() {
        let (host, out) = testing::capture();
        let (failures, on_failed) = failures();
        let (open, shut) = gate();
        let started = Arc::new(Mutex::new(0u32));
        let s = started.clone();
        let q = Queue::start(
            host,
            opts(1),
            move |_: &Job| {
                *s.lock().unwrap() += 1;
                let _ = shut.lock().unwrap().recv();
                Done::Ok { took: Duration::ZERO }
            },
            on_failed,
        );
        let dir = testing::temp_dir("queue-shutdown");
        assert!(q.push(call(&dir, 1), Kind::Live));
        wait_for("the worker to be busy", || *started.lock().unwrap() == 1);
        let (c2, c3, c4) = (call(&dir, 2), call(&dir, 3), call(&dir, 4));
        let (p2, p3, p4) = (c2.path.clone(), c3.path.clone(), c4.path.clone());
        assert!(q.push(c2, Kind::Live));
        assert!(q.push(c3, Kind::Archive));

        let t = Instant::now();
        q.shutdown(Duration::from_millis(100));
        assert!(t.elapsed() < Duration::from_millis(150), "shutdown took {:?}", t.elapsed());
        assert_eq!(*failures.lock().unwrap(), vec![(p2.clone(), "shutting down".to_string()), (p3, "shutting down".to_string())]);
        assert_eq!(out.output().results(), vec![(p2, Outcome::Failed, "shutting down".into(), String::new())]);

        assert!(q.push(c4, Kind::Live), "after shutdown, a job is answered at once");
        assert_eq!(failures.lock().unwrap().last().cloned(), Some((p4.clone(), "shutting down".to_string())));
        assert_eq!(out.output().results().last().cloned(), Some((p4, Outcome::Failed, "shutting down".into(), String::new())));
        assert_eq!(*started.lock().unwrap(), 1);
        drop(open);
    }
}
