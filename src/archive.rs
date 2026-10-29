//! The archive pass: pages the recorder's call store through `calls.query` and
//! queues the calls that have no transcript yet (or, for a re-transcribe, a
//! transcript from another model). It queues at most half the queue's room;
//! past that it pauses, and the next pass takes up the rest.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use trunk_recorder_plugin::{CallFiles, CallsPage, CallsQuery, ConcludedCall, Host, StoredCall};

use crate::queue::{Kind, Queue};
use crate::store::{Row, Status, Store};
use crate::work::{SystemView, length_ms, start_ms};

const DAY_MS: i64 = 86_400_000;
const PAGE: usize = 500;
/// A failed call is tried again by a pass until it has failed this often.
const GIVE_UP_AFTER: u32 = 10;
/// A page not come this long after it was asked ends the pass.
const PAGE_TIMEOUT: Duration = Duration::from_secs(60);

/// What a pass is for.
#[derive(Debug, Clone, PartialEq)]
pub enum Mode {
    /// At startup: every call of the last `days` days without a transcript.
    Start,
    /// On a timer: stops at the first page with nothing to do.
    Periodic,
    /// Calls whose transcript came from a model other than `model`.
    Retranscribe { everything: bool, model: String },
}

impl Mode {
    fn label(&self) -> String {
        match self {
            Mode::Start => "start".into(),
            Mode::Periodic => "periodic".into(),
            Mode::Retranscribe { everything, .. } => format!("re-transcribe{}", if *everything { " of everything" } else { "" }),
        }
    }
}

/// The running pass.
struct Pass {
    request: u64,
    mode: Mode,
    /// The window's lower bound, asked on every page.
    from: Option<i64>,
    /// Re-transcribe only: the paths to do again.
    candidates: HashSet<String>,
    /// Ends at the first page with nothing to do (a periodic pass, unless the last pass paused).
    stop_early: bool,
    /// When the page awaited was asked for.
    asked: Instant,
    queued: u32,
    skipped: u32,
    pages: u32,
}

pub struct Archive {
    host: Host,
    store: Store,
    queue: Arc<Queue>,
    systems: Arc<RwLock<Vec<SystemView>>>,
    days: Arc<AtomicU32>,
    capture_dir: PathBuf,
    pass: Option<Pass>,
    /// The last pass paused with calls left: the next periodic one goes through the whole window.
    resume: bool,
}

impl Archive {
    pub fn new(host: Host, store: Store, queue: Arc<Queue>, systems: Arc<RwLock<Vec<SystemView>>>, days: Arc<AtomicU32>, capture_dir: PathBuf) -> Archive {
        Archive { host, store, queue, systems, days, capture_dir, pass: None, resume: false }
    }

    pub fn running(&self) -> bool {
        self.pass.is_some()
    }

    /// Starts a pass: `Ok(true)` when it started (or had nothing to do), `Ok(false)` when one is
    /// running, `Err` when the store could not be read.
    pub fn begin(&mut self, mode: Mode, now_ms: i64) -> Result<bool, String> {
        if self.pass.is_some() {
            return Ok(false);
        }
        let from = match &mode {
            Mode::Retranscribe { everything: true, .. } => None,
            _ => Some(now_ms - i64::from(self.days.load(Ordering::Relaxed)) * DAY_MS),
        };
        let mut candidates = HashSet::new();
        if let Mode::Retranscribe { model, .. } = &mode {
            match self.store.retranscribe_candidates(model, from) {
                Ok(paths) if paths.is_empty() => {
                    let window = if from.is_some() { format!(" in the last {} days", self.days.load(Ordering::Relaxed)) } else { String::new() };
                    self.host.info(format!("re-transcribe: nothing to do{window}, every transcript is from {model}"));
                    return Ok(true);
                }
                Ok(paths) => candidates = paths.into_iter().collect(),
                Err(e) => {
                    let message = format!("re-transcribe: the store could not be read: {e}");
                    self.host.warn(message.clone());
                    return Err(message);
                }
            }
        }
        let stop_early = match mode {
            Mode::Periodic => !std::mem::take(&mut self.resume),
            Mode::Start => {
                self.resume = false;
                false
            }
            Mode::Retranscribe { .. } => false,
        };
        self.host.info(format!("archive pass started ({})", mode.label()));
        let request = self.host.calls(CallsQuery { from, limit: Some(PAGE), ..Default::default() });
        self.pass = Some(Pass { request, mode, from, candidates, stop_early, asked: Instant::now(), queued: 0, skipped: 0, pages: 0 });
        Ok(true)
    }

    /// Ends the pass when the page it asked for has not come in `PAGE_TIMEOUT`; true when it did.
    pub fn check_timeout(&mut self, now: Instant) -> bool {
        let Some(pass) = self.pass.take_if(|p| now.saturating_duration_since(p.asked) >= PAGE_TIMEOUT) else { return false };
        self.host.warn(format!("archive pass: no answer from the recorder's call store in {} s", PAGE_TIMEOUT.as_secs()));
        self.finish(&pass);
        true
    }

    /// Handles a page of the recorder's call store; false when it is not this pass's.
    pub fn on_page(&mut self, request: u64, page: CallsPage) -> bool {
        let Some(mut pass) = self.pass.take() else { return false };
        if pass.request != request {
            self.pass = Some(pass);
            return false;
        }
        pass.pages += 1;
        if let Some(e) = &page.error {
            self.host.warn(format!("archive pass: the recorder's call store answered: {e}"));
            self.finish(&pass);
            return true;
        }
        let (queued, skipped, paused) = self.work_page(&pass, &page.entries);
        pass.queued += queued;
        pass.skipped += skipped;
        let stop = pass.stop_early && queued == 0 && skipped == 0;
        if paused {
            self.host.info(format!("archive pass paused: {} calls queued, the rest waits for the next pass", self.queue.archive_waiting()));
            self.resume = !matches!(pass.mode, Mode::Retranscribe { .. });
            self.finish(&pass);
        } else if let Some(cursor) = page.next.filter(|_| !stop) {
            pass.request = self.host.calls(CallsQuery { from: pass.from, cursor: Some(cursor), limit: Some(PAGE), ..Default::default() });
            pass.asked = Instant::now();
            self.pass = Some(pass);
        } else {
            self.finish(&pass);
        }
        true
    }

    fn finish(&self, pass: &Pass) {
        self.host.info(format!("archive pass: {} queued, {} skipped, {} pages", pass.queued, pass.skipped, pass.pages));
    }

    /// Queues what needs a transcript, until the queue holds half its room of
    /// archive work; returns (queued, skipped, whether it stopped there).
    fn work_page(&self, pass: &Pass, entries: &[StoredCall]) -> (u32, u32, bool) {
        let budget = (self.queue.max() / 2).max(1);
        let retranscribe = matches!(pass.mode, Mode::Retranscribe { .. });
        let settled = if retranscribe {
            Default::default()
        } else {
            let paths: Vec<String> = entries.iter().map(|e| e.path.clone()).collect();
            self.store.settled(&paths).unwrap_or_else(|e| {
                self.host.warn(format!("archive pass: the store could not be read: {e}"));
                // Without the answer, treat everything as settled rather than queue it all again.
                paths.iter().map(|p| (p.clone(), (Status::Ok, 0))).collect()
            })
        };
        let systems = self.systems.read().unwrap_or_else(|p| p.into_inner());
        let (mut queued, mut skipped) = (0, 0);
        for e in entries {
            if retranscribe {
                if !(pass.candidates.contains(&e.path) && e.audio) {
                    continue;
                }
            } else {
                match settled.get(&e.path) {
                    Some(&(status, attempts)) if !(status == Status::Failed && attempts < GIVE_UP_AFTER) => continue,
                    Some(_) if !e.audio => continue,
                    _ => {}
                }
                if !e.audio {
                    if self.skip_no_audio(e) {
                        skipped += 1;
                    }
                    continue;
                }
            }
            if self.queue.archive_waiting() >= budget {
                return (queued, skipped, true);
            }
            let system = systems.iter().position(|s| s.short_name == e.record.short_name).unwrap_or(0) as u16;
            let call = ConcludedCall {
                path: e.path.clone(),
                system,
                call: e.record.clone(),
                files: CallFiles { json: self.capture_dir.join(format!("{}.json", e.path)), wav: self.capture_dir.join(format!("{}.wav", e.path)), m4a: None },
            };
            let kind = if retranscribe { Kind::Retranscribe } else { Kind::Archive };
            if self.queue.push(call, kind) {
                queued += 1;
            }
        }
        (queued, skipped, false)
    }

    /// Writes the `skipped, "no audio"` row of a call whose audio was not kept.
    fn skip_no_audio(&self, e: &StoredCall) -> bool {
        let r = &e.record;
        let row = Row {
            path: e.path.clone(),
            short_name: r.short_name.clone(),
            talkgroup: r.talkgroup,
            start_ms: start_ms(r),
            length_ms: length_ms(r),
            status: Status::Skipped,
            reason: Some("no audio".into()),
            text: None,
            language: None,
            engine: String::new(),
            model_asked: String::new(),
            model_reported: None,
            prompt: None,
            took_ms: None,
            attempts: 1,
            transcribed_ms: SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as i64),
            raw: None,
        };
        match self.store.put(&row, &[]) {
            Ok(()) => true,
            Err(err) => {
                self.host.warn(format!("archive pass: could not record {}: {err}", row.path));
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::queue::{Done, QueueOptions};
    use std::time::{Duration, Instant};
    use trunk_recorder_plugin::testing::{self, Captured};
    use trunk_recorder_plugin::{CallRecord, Level, PluginMessage};

    type Seen = Arc<std::sync::Mutex<Vec<(Kind, String)>>>;
    type Files = Arc<std::sync::Mutex<Vec<CallFiles>>>;

    struct Fx {
        archive: Archive,
        out: Captured,
        seen: Seen,
        store: Store,
        queue: Arc<Queue>,
    }

    fn fixture(store: Store) -> Fx {
        fixture_of(store, 1, 1000)
    }

    /// A fixture whose queue has `workers` workers (0: nothing is ever taken) and room for `max`.
    fn fixture_of(store: Store, workers: usize, max: usize) -> Fx {
        let (host, out) = testing::capture();
        let seen = Seen::default();
        let s = seen.clone();
        let queue = Arc::new(Queue::start(
            host.clone(),
            QueueOptions { workers, max, retry_after: vec![Duration::from_millis(20)], max_age: Duration::from_secs(3000) },
            move |job| {
                s.lock().unwrap().push((job.kind.clone(), job.call.path.clone()));
                Done::Ok { took: Duration::ZERO }
            },
            |_, _, _| {},
        ));
        let systems = Arc::new(RwLock::new(vec![SystemView { short_name: "sys1".into(), name: String::new(), enabled: true, settings: Config::default() }]));
        let archive = Archive::new(host, store.clone(), queue.clone(), systems, Arc::new(AtomicU32::new(7)), PathBuf::from("cap"));
        Fx { archive, out, seen, store, queue }
    }

    fn stored(path: &str, audio: bool) -> StoredCall {
        StoredCall {
            path: path.into(),
            record: CallRecord { short_name: "sys1".into(), talkgroup: 1, call_length: 3.0, ..Default::default() },
            audio,
            json: true,
        }
    }

    fn row(path: &str, status: Status, attempts: u32, model: Option<&str>) -> Row {
        Row {
            path: path.into(),
            short_name: "sys1".into(),
            talkgroup: 1,
            start_ms: 1000,
            length_ms: 3000,
            status,
            reason: None,
            text: None,
            language: None,
            engine: "e".into(),
            model_asked: "default".into(),
            model_reported: model.map(String::from),
            prompt: None,
            took_ms: None,
            attempts,
            transcribed_ms: 1010,
            raw: None,
        }
    }

    fn page(entries: Vec<StoredCall>, next: Option<&str>) -> CallsPage {
        CallsPage { entries, next: next.map(str::to_owned), error: None }
    }

    fn queries(f: &Fx) -> Vec<(u64, CallsQuery)> {
        f.out
            .output()
            .messages
            .into_iter()
            .filter_map(|m| match m {
                PluginMessage::CallsQuery { request, query } => Some((request, query)),
                _ => None,
            })
            .collect()
    }

    fn logs(f: &Fx, level: Level) -> Vec<String> {
        f.out.output().logs().into_iter().filter(|(l, _)| *l == level).map(|(_, m)| m).collect()
    }

    /// Waits until the queue is drained and returns what the worker saw, sorted.
    fn drained(f: &Fx, expect: usize) -> Vec<(String, String)> {
        let deadline = Instant::now() + Duration::from_secs(10);
        while f.seen.lock().unwrap().len() < expect || !f.queue.is_empty() {
            assert!(Instant::now() < deadline, "timed out waiting for the worker");
            std::thread::sleep(Duration::from_millis(5));
        }
        let mut seen: Vec<(String, String)> = f.seen.lock().unwrap().iter().map(|(k, p)| (format!("{k:?}"), p.clone())).collect();
        seen.sort();
        seen
    }

    fn pair(kind: &str, path: &str) -> (String, String) {
        (kind.to_string(), path.to_string())
    }

    #[test]
    fn a_start_pass_queues_every_unsettled_call_and_pages_to_the_end() {
        let f0 = Store::in_memory();
        f0.put(&row("a", Status::Ok, 1, Some("m1")), &[]).unwrap();
        f0.put(&row("f", Status::Failed, 2, None), &[]).unwrap();
        f0.put(&row("g", Status::Failed, 10, None), &[]).unwrap();
        let mut f = fixture(f0);
        let now = 100 * DAY_MS;
        assert!(f.archive.begin(Mode::Start, now).unwrap());
        let q = queries(&f);
        assert_eq!(q.len(), 1);
        assert_eq!((q[0].1.from, q[0].1.limit, q[0].1.cursor.as_deref()), (Some(now - 7 * DAY_MS), Some(500), None));

        let req = q[0].0;
        assert!(f.archive.on_page(req, page(vec![stored("a", true), stored("b", true), stored("f", true), stored("g", true)], Some("500"))));
        let q = queries(&f);
        assert_eq!(q.len(), 2);
        assert_eq!((q[1].1.cursor.as_deref(), q[1].1.from), (Some("500"), Some(now - 7 * DAY_MS)));
        assert!(f.archive.running());

        assert!(f.archive.on_page(q[1].0, page(vec![stored("c", false), stored("d", true)], None)));
        assert!(!f.archive.running());
        assert_eq!(drained(&f, 3), [pair("Archive", "b"), pair("Archive", "d"), pair("Archive", "f")]);
        assert_eq!(queries(&f).len(), 2);

        let c = f.store.get("c").unwrap().unwrap().0;
        assert_eq!((c.status, c.reason.as_deref(), c.engine.as_str(), c.model_asked.as_str(), c.attempts), (Status::Skipped, Some("no audio"), "", "", 1));
        assert_eq!((c.short_name.as_str(), c.talkgroup, c.length_ms), ("sys1", 1, 3000));
        let infos = logs(&f, Level::Info);
        assert!(infos.contains(&"archive pass started (start)".to_string()), "{infos:?}");
        assert!(infos.contains(&"archive pass: 3 queued, 1 skipped, 2 pages".to_string()), "{infos:?}");
    }

    #[test]
    fn a_periodic_pass_stops_at_the_first_settled_page() {
        let s = Store::in_memory();
        s.put(&row("a", Status::Ok, 1, None), &[]).unwrap();
        s.put(&row("b", Status::Empty, 1, None), &[]).unwrap();
        let mut f = fixture(s);
        assert!(f.archive.begin(Mode::Periodic, DAY_MS * 50).unwrap());
        let req = queries(&f)[0].0;
        assert!(f.archive.on_page(req, page(vec![stored("a", true), stored("b", true)], Some("500"))));
        assert_eq!(queries(&f).len(), 1);
        assert!(!f.archive.running());
        assert!(logs(&f, Level::Info).contains(&"archive pass: 0 queued, 0 skipped, 1 pages".to_string()));
    }

    #[test]
    fn the_store_being_off_is_logged_once_and_the_pass_ends() {
        let mut f = fixture(Store::in_memory());
        f.archive.begin(Mode::Start, DAY_MS * 50).unwrap();
        let req = queries(&f)[0].0;
        let p = CallsPage { error: Some("the call store is off".into()), ..Default::default() };
        assert!(f.archive.on_page(req, p));
        assert!(!f.archive.running());
        assert_eq!(queries(&f).len(), 1);
        let warns = logs(&f, Level::Warn);
        assert_eq!(warns, ["archive pass: the recorder's call store answered: the call store is off"]);
        assert!(f.archive.begin(Mode::Start, DAY_MS * 50).unwrap());
        assert_eq!(queries(&f).len(), 2);
    }

    #[test]
    fn retranscribe_queues_only_the_candidates() {
        let s = Store::in_memory();
        s.put(&row("a", Status::Ok, 1, Some("m1")), &[]).unwrap();
        s.put(&row("b", Status::Ok, 1, Some("m2")), &[]).unwrap();
        s.put(&row("c", Status::Empty, 1, None), &[]).unwrap();
        s.put(&row("d", Status::Skipped, 1, None), &[]).unwrap();
        let mut f = fixture(s);
        let now = 7 * DAY_MS;
        assert!(f.archive.begin(Mode::Retranscribe { everything: false, model: "m2".into() }, now).unwrap());
        let q = queries(&f);
        assert_eq!(q[0].1.from, Some(now - 7 * DAY_MS));
        assert!(f.archive.on_page(q[0].0, page(vec![stored("a", true), stored("b", true), stored("c", true), stored("d", true)], None)));
        assert_eq!(drained(&f, 2), [pair("Retranscribe", "a"), pair("Retranscribe", "c")]);
        assert!(logs(&f, Level::Info).contains(&"archive pass started (re-transcribe)".to_string()));

        // Everything: no lower bound.
        assert!(f.archive.begin(Mode::Retranscribe { everything: true, model: "m2".into() }, now).unwrap());
        let q = queries(&f);
        assert_eq!(q.len(), 2);
        assert_eq!(q[1].1.from, None);
        assert!(logs(&f, Level::Info).contains(&"archive pass started (re-transcribe of everything)".to_string()));
        // A candidate without audio is left as it is.
        assert!(f.archive.on_page(q[1].0, page(vec![stored("a", false)], None)));
        assert_eq!(f.store.get("a").unwrap().unwrap().0.status, Status::Ok);
        assert!(!f.archive.running());

        // Nothing to do: no query, an info line.
        let s = Store::in_memory();
        s.put(&row("a", Status::Ok, 1, Some("m1")), &[]).unwrap();
        s.put(&row("c", Status::Empty, 1, Some("m1")), &[]).unwrap();
        let mut f = fixture(s);
        assert!(f.archive.begin(Mode::Retranscribe { everything: false, model: "m1".into() }, now).unwrap());
        assert!(queries(&f).is_empty());
        assert!(!f.archive.running());
        assert!(logs(&f, Level::Info).contains(&"re-transcribe: nothing to do in the last 7 days, every transcript is from m1".to_string()));
    }

    #[test]
    fn a_dotted_path_keeps_its_dot_in_the_file_names() {
        let (host, _out) = testing::capture();
        let files = Files::default();
        let w = files.clone();
        let queue = Arc::new(Queue::start(
            host.clone(),
            QueueOptions { workers: 1, max: 10, retry_after: vec![Duration::from_millis(20)], max_age: Duration::from_secs(3000) },
            move |job| {
                w.lock().unwrap().push(job.call.files.clone());
                Done::Ok { took: Duration::ZERO }
            },
            |_, _, _| {},
        ));
        let systems = Arc::new(RwLock::new(Vec::new()));
        let mut archive = Archive::new(host, Store::in_memory(), queue.clone(), systems, Arc::new(AtomicU32::new(7)), PathBuf::from("cap"));
        assert!(archive.begin(Mode::Start, DAY_MS * 50).unwrap());
        assert!(archive.on_page(1, page(vec![stored("sys1/851012500.0-call_1", true)], None)));
        let deadline = Instant::now() + Duration::from_secs(10);
        while files.lock().unwrap().is_empty() {
            assert!(Instant::now() < deadline, "timed out waiting for the worker");
            std::thread::sleep(Duration::from_millis(5));
        }
        let got = files.lock().unwrap()[0].clone();
        assert!(got.json.to_string_lossy().ends_with("851012500.0-call_1.json"), "{:?}", got.json);
        assert!(got.wav.to_string_lossy().ends_with("851012500.0-call_1.wav"), "{:?}", got.wav);
    }

    #[test]
    fn begin_reports_a_store_that_cannot_be_read() {
        let s = Store::in_memory();
        s.0.lock().unwrap().execute_batch("DROP TABLE transcripts").unwrap();
        let mut f = fixture(s);
        let r = f.archive.begin(Mode::Retranscribe { everything: false, model: "m1".into() }, DAY_MS * 50);
        assert!(r.is_err(), "{r:?}");
        assert!(!f.archive.running());
        assert!(queries(&f).is_empty());
        assert_eq!(logs(&f, Level::Warn).len(), 1);
    }

    #[test]
    fn a_pass_queues_up_to_half_the_queue_and_pauses() {
        let mut f = fixture_of(Store::in_memory(), 0, 10);
        assert!(f.archive.begin(Mode::Start, DAY_MS * 50).unwrap());
        let req = queries(&f)[0].0;
        let entries: Vec<_> = (0..8).map(|i| stored(&format!("c{i}"), true)).collect();
        assert!(f.archive.on_page(req, page(entries, Some("500"))));
        assert_eq!(f.queue.archive_waiting(), 5, "the budget is half of max 10");
        assert!(!f.archive.running(), "the pass ended");
        assert_eq!(queries(&f).len(), 1, "no further page asked");
        let infos = logs(&f, Level::Info);
        assert!(infos.contains(&"archive pass paused: 5 calls queued, the rest waits for the next pass".to_string()), "{infos:?}");
        assert!(infos.contains(&"archive pass: 5 queued, 0 skipped, 1 pages".to_string()), "{infos:?}");
        assert!(f.store.get("c0").unwrap().is_none(), "queueing writes no row");

        // The next periodic pass goes on past a page with nothing to do...
        f.store.put(&row("s1", Status::Ok, 1, None), &[]).unwrap();
        assert!(f.archive.begin(Mode::Periodic, DAY_MS * 50).unwrap());
        let req = queries(&f)[1].0;
        assert!(f.archive.on_page(req, page(vec![stored("s1", true)], Some("400"))));
        assert_eq!(queries(&f).len(), 3, "it asked for the next page");
        let req = queries(&f)[2].0;
        assert!(f.archive.on_page(req, page(vec![], None)));
        // ...and the one after stops early again.
        assert!(f.archive.begin(Mode::Periodic, DAY_MS * 50).unwrap());
        let req = queries(&f)[3].0;
        assert!(f.archive.on_page(req, page(vec![stored("s1", true)], Some("400"))));
        assert_eq!(queries(&f).len(), 4);
        assert!(!f.archive.running());
    }

    #[test]
    fn a_page_that_never_comes_ends_the_pass() {
        let mut f = fixture(Store::in_memory());
        assert!(!f.archive.check_timeout(Instant::now()), "no pass, nothing to end");
        assert!(f.archive.begin(Mode::Start, DAY_MS * 50).unwrap());
        assert!(!f.archive.check_timeout(Instant::now() + Duration::from_secs(30)));
        assert!(f.archive.running());
        assert!(f.archive.check_timeout(Instant::now() + Duration::from_secs(61)));
        assert!(!f.archive.running());
        assert_eq!(logs(&f, Level::Warn), ["archive pass: no answer from the recorder's call store in 60 s"]);
        let req = queries(&f)[0].0;
        assert!(!f.archive.on_page(req, page(vec![stored("late", true)], None)), "a late page is not the pass's");
        assert!(f.archive.begin(Mode::Start, DAY_MS * 50).unwrap(), "a new pass can start");
    }

    #[test]
    fn a_second_begin_while_running_is_refused() {
        let mut f = fixture(Store::in_memory());
        assert!(f.archive.begin(Mode::Start, DAY_MS * 50).unwrap());
        assert!(!f.archive.begin(Mode::Periodic, DAY_MS * 50).unwrap());
        assert_eq!(queries(&f).len(), 1);
        // A page for another request is ignored.
        assert!(!f.archive.on_page(999, page(vec![stored("x", true)], None)));
        assert!(f.archive.running());
        let req = queries(&f)[0].0;
        assert!(f.archive.on_page(req, page(vec![], None)));
        assert!(f.archive.begin(Mode::Start, DAY_MS * 50).unwrap());
    }
}
