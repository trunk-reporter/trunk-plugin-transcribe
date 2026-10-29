//! The work queue: calls waiting for the engine, worked off the event thread.
//!
//! Live calls go first (oldest first), then retries that are due, then archive
//! work (a backfill or a retranscription) on one worker at a time, so the
//! recorder's live calls never wait behind a backfill. Every live job ends in
//! exactly one `call.result`; archive and retranscribe jobs send none, unless a
//! live call for the same path joins them, which makes them live.

use std::collections::{HashMap, HashSet, VecDeque};
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
    /// The engine failed it.
    Failed(String),
    /// It failed for a cause of our own (unreadable audio, the store, a
    /// panic): ended like `Failed`, but not counted against the engine.
    Local(String),
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
    /// A live job older than this when a worker takes it fails with "engine down" (50 min).
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
type Failed = dyn Fn(&Job, &str, bool) + Send + Sync;

/// How a job ended.
enum End {
    Ok(Duration),
    Skipped(String),
    /// Why, and whether the job was ever tried (a drop before any try is not).
    Failed(String, bool),
}

/// A job a worker is on.
struct Flight {
    /// Its end goes to the recorder as a `call.result`: a live job, or an
    /// archive job a live call joined while it ran.
    live: bool,
    /// That `call.result` has gone already (at shutdown): its end sends none.
    answered: bool,
}

#[derive(Default)]
struct Inner {
    live: VecDeque<Job>,
    archive: VecDeque<Job>,
    /// Jobs waiting for a retry, with when it is due.
    retrying: Vec<(Instant, Job)>,
    /// The paths queued or in flight.
    paths: HashSet<String>,
    /// The jobs in flight, by path.
    flying: HashMap<String, Flight>,
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

    /// A live call for a path already queued or in flight. An archive job for
    /// it becomes live (moved to the live lane, or, in flight, answered as
    /// live when it ends): true. A live job for it answers already: false.
    fn join_live(&mut self, live: Job) -> bool {
        let path = live.call.path.as_str();
        if let Some(i) = self.archive.iter().position(|j| j.call.path == path) {
            self.archive.remove(i);
            self.live.push_back(live);
            return true;
        }
        if let Some((_, job)) = self.retrying.iter_mut().find(|(_, j)| j.call.path == path) {
            if job.kind == Kind::Live {
                return false;
            }
            job.kind = Kind::Live;
            job.call = live.call;
            return true;
        }
        match self.flying.get_mut(path) {
            Some(flight) if !flight.live => {
                flight.live = true;
                true
            }
            _ => false,
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
                inner.flying.insert(job.call.path.clone(), Flight { live: job.kind == Kind::Live, answered: false });
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
        // The age rule is for live calls: archive work is expected to wait.
        let end = if job.kind == Kind::Live && job.enqueued.elapsed() > self.opts.max_age {
            self.lock().stats.failed += 1;
            End::Failed("engine down".into(), false)
        } else {
            let done = catch_unwind(AssertUnwindSafe(|| (self.work)(&job))).unwrap_or_else(|_| Done::Local("the work panicked".into()));
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
                    End::Failed(m, true)
                }
                Done::Local(m) => {
                    inner.stats.failed += 1;
                    End::Failed(m, true)
                }
                Done::Retry(m) => {
                    inner.engine_error(&m);
                    let tries = job.tries as usize;
                    if inner.closing {
                        inner.stats.failed += 1;
                        End::Failed("shutting down".into(), true)
                    } else if tries < self.opts.retry_after.len() {
                        let message = format!("retry {}/{} for {}: {m}", tries + 1, self.opts.retry_after.len(), job.call.path);
                        // A live call that joined it while it ran makes it live from here on.
                        if inner.flying.remove(&job.call.path).is_some_and(|f| f.live) {
                            job.kind = Kind::Live;
                        }
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
                        End::Failed(format!("gave up: {m}"), true)
                    }
                }
            }
        };
        // Whether the end goes to the recorder is settled as the flight ends, under the lock.
        let result = {
            let mut inner = self.lock();
            inner.paths.remove(&job.call.path);
            let flight = inner.flying.remove(&job.call.path);
            if flight.as_ref().is_some_and(|f| f.live) {
                job.kind = Kind::Live;
            }
            flight.map_or(job.kind == Kind::Live, |f| f.live && !f.answered)
        };
        self.answer(&job, &end, result);
        self.lock().release(archive);
        self.changed.notify_all();
    }

    /// Tells the plugin of a failure (first, so its record exists), then, when
    /// `result`, the recorder of the job's end. Called with no lock held.
    fn answer(&self, job: &Job, end: &End, result: bool) {
        let path = job.call.path.as_str();
        match end {
            End::Ok(took) if result => self.host.call_result_took(path, Outcome::Ok, "", "", *took),
            End::Skipped(m) if result => self.host.call_result(path, Outcome::Skipped, m.as_str(), ""),
            End::Failed(m, ran) => {
                let _ = catch_unwind(AssertUnwindSafe(|| (self.failed)(job, m, *ran)));
                if result {
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
    /// job; `failed` hears of every job that ends failed (why, and whether it
    /// was ever tried), on the thread that ends it, before its `call.result`.
    pub fn start(
        host: Host,
        opts: QueueOptions,
        work: impl Fn(&Job) -> Done + Send + Sync + 'static,
        failed: impl Fn(&Job, &str, bool) + Send + Sync + 'static,
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

    /// Queues `call`. When a job with its path is queued or in flight, a live
    /// call joins an archive job for it, which then answers the recorder
    /// (true); anything else is refused (false, and nothing happens). When
    /// `max` jobs are waiting, the oldest archive job is dropped ("queue
    /// full"), else the oldest live one. Once shutdown has begun, the job is
    /// answered at once with "shutting down".
    pub fn push(&self, call: ConcludedCall, kind: Kind) -> bool {
        let s = &self.shared;
        let job = Job { call, kind, enqueued: Instant::now(), tries: 0 };
        let mut inner = s.lock();
        if inner.paths.contains(&job.call.path) {
            let joined = job.kind == Kind::Live && inner.join_live(job);
            s.changed.notify_all();
            return joined;
        }
        if inner.closing {
            inner.stats.failed += 1;
            drop(inner);
            let live = job.kind == Kind::Live;
            s.answer(&job, &End::Failed("shutting down".into(), false), live);
            return true;
        }
        let mut dropped = None;
        let mut warn = false;
        let oldest = if inner.waiting() >= s.opts.max { inner.take_oldest(true).or_else(|| inner.take_oldest(false)) } else { None };
        if let Some(old) = oldest {
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
            let (ran, live) = (old.tries > 0, old.kind == Kind::Live);
            s.answer(&old, &End::Failed("queue full".into(), ran), live);
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

    /// Archive and retranscribe jobs waiting, ready or for a retry.
    pub fn archive_waiting(&self) -> usize {
        let inner = self.shared.lock();
        inner.archive.len() + inner.retrying.iter().filter(|(_, j)| j.kind.is_archive()).count()
    }

    /// How many jobs may wait.
    pub fn max(&self) -> usize {
        self.shared.opts.max
    }

    /// Answers every live job still in flight with `failed, reason`, once: its
    /// own end, should it come, sends nothing more.
    pub fn answer_in_flight(&self, reason: &str) {
        let s = &self.shared;
        let mut paths: Vec<String> = {
            let mut inner = s.lock();
            inner
                .flying
                .iter_mut()
                .filter(|(_, f)| f.live && !f.answered)
                .map(|(path, f)| {
                    f.answered = true;
                    path.clone()
                })
                .collect()
        };
        paths.sort();
        for path in paths {
            s.host.call_result(&path, Outcome::Failed, reason, "");
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
            s.answer(job, &End::Failed("shutting down".into(), job.tries > 0), job.kind == Kind::Live);
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

    type Failures = Arc<Mutex<Vec<(String, String, bool)>>>;

    /// A `failed` callback that keeps (path, message, tried).
    fn failures() -> (Failures, impl Fn(&Job, &str, bool) + Send + Sync + 'static) {
        let kept = Failures::default();
        let k = kept.clone();
        (kept, move |job: &Job, m: &str, ran: bool| k.lock().unwrap().push((job.call.path.clone(), m.to_string(), ran)))
    }

    fn failed(path: &str, m: &str, ran: bool) -> (String, String, bool) {
        (path.to_string(), m.to_string(), ran)
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
            |_: &Job, _: &str, _: bool| {},
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
        assert_eq!(*failures.lock().unwrap(), vec![failed(&p[2], "HTTP 400", true)]);
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
        assert_eq!(*failures.lock().unwrap(), vec![failed(&p, "gave up: down", true)]);
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
        assert_eq!(*failures.lock().unwrap(), vec![failed(&pa1, "queue full", false)], "never tried");
        assert!(out.output().results().is_empty(), "an archive job gets no call.result");

        assert!(q.push(call(&dir, 5), Kind::Live));
        assert_eq!(*failures.lock().unwrap(), vec![failed(&pa1, "queue full", false), failed(&pl1, "queue full", false)]);
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
            |_: &Job, _: &str, _: bool| {},
        );
        let dir = testing::temp_dir("queue-dup");
        let c = call(&dir, 1);
        assert!(q.push(c.clone(), Kind::Live));
        assert!(!q.push(c.clone(), Kind::Live), "live, queued or in flight: refused");
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
        assert_eq!(*failures.lock().unwrap(), vec![failed(&p2, "engine down", false)]);
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
        assert_eq!(*failures.lock().unwrap(), vec![failed(&p2, "shutting down", false), failed(&p3, "shutting down", false)]);
        assert_eq!(out.output().results(), vec![(p2, Outcome::Failed, "shutting down".into(), String::new())]);

        assert!(q.push(c4, Kind::Live), "after shutdown, a job is answered at once");
        assert_eq!(failures.lock().unwrap().last().cloned(), Some(failed(&p4, "shutting down", false)));
        assert_eq!(out.output().results().last().cloned(), Some((p4, Outcome::Failed, "shutting down".into(), String::new())));
        assert_eq!(*started.lock().unwrap(), 1);
        drop(open);
    }

    type Started = Arc<Mutex<Vec<(Kind, u32)>>>;

    /// A queue whose work blocks until the gate opens, keeping (kind, tg) of each job it starts.
    fn blocking(host: Host, opts: QueueOptions, on_failed: impl Fn(&Job, &str, bool) + Send + Sync + 'static) -> (Queue, Started, mpsc::Sender<()>) {
        let (open, shut) = gate();
        let started = Started::default();
        let s = started.clone();
        let q = Queue::start(
            host,
            opts,
            move |job: &Job| {
                s.lock().unwrap().push((job.kind.clone(), job.call.call.talkgroup));
                let _ = shut.lock().unwrap().recv();
                Done::Ok { took: Duration::ZERO }
            },
            on_failed,
        );
        (q, started, open)
    }

    #[test]
    fn a_live_call_joins_its_waiting_archive_job() {
        let (host, out) = testing::capture();
        let (q, started, open) = blocking(host, opts(1), |_: &Job, _: &str, _: bool| {});
        let dir = testing::temp_dir("queue-join");
        assert!(q.push(call(&dir, 10), Kind::Live));
        wait_for("the worker to be busy", || !started.lock().unwrap().is_empty());
        let a = call(&dir, 1);
        let pa = a.path.clone();
        assert!(q.push(a.clone(), Kind::Archive));
        assert!(q.push(call(&dir, 2), Kind::Archive));
        assert!(q.push(a.clone(), Kind::Live), "a live call joins the archive job");
        let s = q.snapshot();
        assert_eq!((s.live, s.archive), (1, 1), "moved to the live lane");
        assert!(!q.push(a, Kind::Live), "now live: a second live call is refused");
        drop(open);
        wait_for("all three done", || started.lock().unwrap().len() == 3 && q.is_empty());
        assert_eq!(started.lock().unwrap()[1], (Kind::Live, 1), "worked once, as live, before archive 2");
        let results: Vec<_> = out.output().results().into_iter().filter(|r| r.0 == pa).map(|r| r.1).collect();
        assert_eq!(results, [Outcome::Ok]);
    }

    #[test]
    fn a_live_call_joins_its_archive_job_in_flight() {
        let (host, out) = testing::capture();
        let (q, started, open) = blocking(host, opts(1), |_: &Job, _: &str, _: bool| {});
        let dir = testing::temp_dir("queue-join-flight");
        let a = call(&dir, 1);
        let pa = a.path.clone();
        assert!(q.push(a.clone(), Kind::Archive));
        wait_for("the archive job to start", || !started.lock().unwrap().is_empty());
        assert!(q.push(a.clone(), Kind::Live), "joins the archive job in flight");
        assert!(!q.push(a, Kind::Live), "a second live call is refused");
        drop(open);
        wait_for("its result", || !out.output().results().is_empty() && q.is_empty());
        thread::sleep(Duration::from_millis(50));
        assert_eq!(out.output().results(), vec![(pa, Outcome::Ok, String::new(), String::new())], "exactly one call.result");
        assert_eq!(started.lock().unwrap().len(), 1, "worked once");
    }

    #[test]
    fn an_old_archive_job_still_runs() {
        let (host, out) = testing::capture();
        let (failures, on_failed) = failures();
        let max_age = Duration::from_millis(200);
        let (q, started, open) = blocking(host, QueueOptions { max_age, ..opts(1) }, on_failed);
        let dir = testing::temp_dir("queue-old-archive");
        assert!(q.push(call(&dir, 1), Kind::Live));
        wait_for("job 1 to start", || !started.lock().unwrap().is_empty());
        assert!(q.push(call(&dir, 2), Kind::Archive));
        thread::sleep(max_age * 2); // job 2 ages past max_age while job 1 holds the worker
        drop(open);
        wait_for("both worked", || started.lock().unwrap().len() == 2 && q.is_empty());
        assert_eq!(started.lock().unwrap()[1], (Kind::Archive, 2));
        assert!(failures.lock().unwrap().is_empty());
        assert_eq!(out.output().results().len(), 1, "the live one's");
    }

    #[test]
    fn a_local_failure_is_not_the_engines() {
        let (host, out) = testing::capture();
        let (failures, on_failed) = failures();
        let q = Queue::start(host, opts(1), |_: &Job| Done::Local("store: disk full".into()), on_failed);
        let dir = testing::temp_dir("queue-local");
        let c = call(&dir, 1);
        let p = c.path.clone();
        assert!(q.push(c, Kind::Live));
        wait_for("its result", || !out.output().results().is_empty() && q.is_empty());
        assert_eq!(out.output().results(), vec![(p.clone(), Outcome::Failed, "store: disk full".into(), String::new())]);
        assert_eq!(*failures.lock().unwrap(), vec![failed(&p, "store: disk full", true)]);
        let s = q.snapshot();
        assert_eq!((s.failed, s.in_a_row, s.down_since, s.last_error_s), (1, 0, None, None));
    }

    #[test]
    fn answer_in_flight_answers_each_live_job_once() {
        let (host, out) = testing::capture();
        let (q, started, open) = blocking(host, opts(2), |_: &Job, _: &str, _: bool| {});
        let dir = testing::temp_dir("queue-answer-in-flight");
        let (l, a) = (call(&dir, 1), call(&dir, 2));
        let pl = l.path.clone();
        assert!(q.push(l, Kind::Live));
        assert!(q.push(a, Kind::Archive));
        wait_for("both in flight", || started.lock().unwrap().len() == 2);
        q.answer_in_flight("shutting down");
        q.answer_in_flight("shutting down");
        let want = vec![(pl, Outcome::Failed, "shutting down".to_string(), String::new())];
        assert_eq!(out.output().results(), want, "the live one only, once");
        drop(open);
        wait_for("both done", || q.is_empty());
        thread::sleep(Duration::from_millis(50));
        assert_eq!(out.output().results(), want, "the late end sends nothing");
    }
}
