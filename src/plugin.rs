//! The plugin: wires the store, the engine, the queue, the archive pass and the
//! HTTP side together; answers the two buttons; takes new settings while it
//! runs; keeps its metrics, status and health current; and shuts down without
//! leaving a live call unanswered.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use trunk_recorder_plugin::{Action, CallsPage, ConcludedCall, Endpoint, EndpointState, Host, Manifest, Metrics, Outcome, Plugin, Reply, Setup, State, topic};

use crate::archive::{Archive, Mode};
use crate::audio::test_tone;
use crate::config::{ACTION_RETRANSCRIBE, ACTION_TEST, Config, EngineConfig, HttpConfig, SystemConfig};
use crate::engine::{Audio, Engine, EngineError, host_of};
use crate::http::{self, AppState, Health, ServerHandle};
use crate::queue::{Kind, Queue, QueueOptions, Snapshot};
use crate::store::Store;
use crate::work::{Ctx, process, record_failed, systems_of};

const VERSION: &str = env!("CARGO_PKG_VERSION");
/// The upkeep thread's beat.
const TICK: Duration = Duration::from_secs(10);
/// How often the engine is asked which model it runs.
const MODELS_EVERY: Duration = Duration::from_secs(600);
/// The models probe's own timeout, so a hung engine cannot hold up shutdown for long.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
/// The first retention sweep, and the time between sweeps.
const RETAIN_FIRST: Duration = Duration::from_secs(60);
const RETAIN_EVERY: Duration = Duration::from_secs(86_400);
/// The engine down this long turns the status to an error.
const DOWN_IS_ERROR: Duration = Duration::from_secs(300);
const RETRY_AFTER: [Duration; 3] = [Duration::from_secs(2), Duration::from_secs(8), Duration::from_secs(30)];
const MAX_AGE: Duration = Duration::from_secs(50 * 60);
/// The longest `engine.timeoutSeconds` allows.
const MAX_ENGINE_TIMEOUT: Duration = Duration::from_secs(600);
const DAY_MS: i64 = 86_400_000;
const RESTART_WARNING: &str = "Concurrency, Queue size and HTTP settings take effect when recording next starts";

pub struct Transcribe {
    host: Host,
    ctx: Ctx,
    queue: Arc<Queue>,
    archive: Arc<Mutex<Archive>>,
    http: Option<ServerHandle>,
    upkeep: Option<(mpsc::Sender<()>, JoinHandle<()>)>,
    started: Instant,
    /// The archive pass's window, shared with it.
    days: Arc<AtomicU32>,
    /// The settings that only a restart changes, as they were at start.
    running: Running,
    report: Arc<Report>,
    /// "Test the engine" presses still working.
    tests: Vec<JoinHandle<()>>,
}

/// What the queue and the HTTP server were started with.
struct Running {
    concurrency: usize,
    queue_max: usize,
    http: HttpConfig,
}

impl Running {
    fn of(c: &Config) -> Running {
        Running { concurrency: c.engine.concurrency, queue_max: c.queue.max, http: c.http.clone() }
    }

    /// Whether `c` changes anything that waits for a restart.
    fn differs(&self, c: &Config) -> bool {
        let h = &c.http;
        self.concurrency != c.engine.concurrency
            || self.queue_max != c.queue.max
            || self.http.port != h.port
            || self.http.bind != h.bind
            || self.http.origins != h.origins
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn read<T>(l: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    l.read().unwrap_or_else(|e| e.into_inner())
}

fn write<T>(l: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    l.write().unwrap_or_else(|e| e.into_inner())
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as i64)
}

impl Plugin for Transcribe {
    type Config = Config;
    type SystemConfig = SystemConfig;

    fn manifest() -> Manifest {
        Manifest {
            name: "Transcribe".into(),
            subscribe: vec![topic::CALL_CONCLUDED.into()],
            audio_formats: vec![],
            live_settings: true,
            ..trunk_recorder_plugin::manifest!()
        }
    }

    fn start(host: Host, setup: Setup<Config, SystemConfig>) -> Result<Self, String> {
        let config = setup.config.clone();
        config.validate()?;
        let store = {
            let host = host.clone();
            Store::open(&setup.data_dir.join("transcripts.sqlite"), &move |m| host.warn(m))?
        };
        let ctx = Ctx {
            config: Arc::new(RwLock::new(config.clone())),
            systems: Arc::new(RwLock::new(systems_of(&setup))),
            store: store.clone(),
            // Each request takes the settings' timeout as it is then; this is only the most it may be.
            engine: Engine::new(MAX_ENGINE_TIMEOUT),
            reported_model: Arc::new(RwLock::new(None)),
            announce: tokio::sync::broadcast::channel(256).0,
            capture_dir: setup.capture_dir.clone(),
            transcribed: Arc::new(AtomicU64::new(0)),
        };

        let queue = {
            let (work_ctx, work_host, failed_ctx, failed_host) = (ctx.clone(), host.clone(), ctx.clone(), host.clone());
            let opts = QueueOptions { workers: config.engine.concurrency, max: config.queue.max, retry_after: RETRY_AFTER.to_vec(), max_age: MAX_AGE };
            Arc::new(Queue::start(
                host.clone(),
                opts,
                move |job| process(&work_ctx, job, &|m| work_host.warn(m)),
                move |job, reason, ran| record_failed(&failed_ctx, job, reason, ran, &|m| failed_host.warn(m)),
            ))
        };
        let days = Arc::new(AtomicU32::new(config.archive.days));
        let archive =
            Arc::new(Mutex::new(Archive::new(host.clone(), store.clone(), queue.clone(), ctx.systems.clone(), days.clone(), setup.capture_dir.clone())));

        let health = Arc::new(RwLock::new(Health::default()));
        let (http, serving) = match config.http.port {
            Some(port) => {
                let state = AppState { store, events: ctx.announce.clone(), health: health.clone(), origins: config.http.origins.clone() };
                match http::start(state, &config.http.bind, port) {
                    Ok((handle, addr)) => (Some(handle), format!("serving http://{addr}")),
                    Err(e) => {
                        queue.shutdown(Duration::ZERO);
                        return Err(e);
                    }
                }
            }
            None => (None, "not serving".to_string()),
        };

        let started = Instant::now();
        let report = Arc::new(Report {
            host: host.clone(),
            ctx: ctx.clone(),
            queue: queue.clone(),
            queue_max: config.queue.max,
            health,
            refused: Mutex::new(None),
            reprobe: AtomicBool::new(false),
        });
        let upkeep = {
            let mut upkeep = Upkeep {
                report: report.clone(),
                archive: archive.clone(),
                probe: Engine::new(PROBE_TIMEOUT),
                next_models: started,
                last_periodic: started,
                next_retain: started + RETAIN_FIRST,
            };
            let (stop, stopped) = mpsc::channel::<()>();
            let thread = thread::Builder::new()
                .name("upkeep".into())
                .spawn(move || {
                    loop {
                        upkeep.tick(Instant::now());
                        match stopped.recv_timeout(TICK) {
                            Err(RecvTimeoutError::Timeout) => {}
                            _ => break,
                        }
                    }
                })
                .map_err(|e| format!("cannot start the upkeep thread: {e}"))?;
            (stop, thread)
        };

        host.info(format!("transcribe {VERSION}: engine {}, model {}, {serving}", host_of(&config.engine.url), config.engine.model));
        if let Err(e) = lock(&archive).begin(Mode::Start, now_ms()) {
            host.warn(e);
        }
        Ok(Transcribe { host, ctx, queue, archive, http, upkeep: Some(upkeep), started, days, running: Running::of(&config), report, tests: Vec::new() })
    }

    fn call_concluded(&mut self, call: ConcludedCall) {
        let path = call.path.clone();
        if !self.queue.push(call, Kind::Live) {
            self.host.call_result(&path, Outcome::Skipped, "already queued", "");
        }
    }

    fn calls(&mut self, page: CallsPage) {
        lock(&self.archive).on_page(page);
    }

    fn action(&mut self, a: Action, reply: Reply) {
        match a.action.as_str() {
            ACTION_TEST => self.test(&a.value, reply),
            ACTION_RETRANSCRIBE => {
                let everything = a
                    .value
                    .pointer("/archive/retranscribeEverything")
                    .and_then(Value::as_bool)
                    .unwrap_or_else(|| read(&self.ctx.config).archive.retranscribe_everything);
                let reported = read(&self.ctx.reported_model).clone();
                let model = reported.unwrap_or_else(|| read(&self.ctx.config).engine.model.clone());
                let begun = lock(&self.archive).begin(Mode::Retranscribe { everything, model }, now_ms());
                match begun {
                    Ok(true) => reply.ok("Re-transcription started; the log says how many calls were queued"),
                    Ok(false) => reply.fail("An archive pass is already running"),
                    Err(m) => reply.fail(m),
                }
            }
            other => reply.fail(format!("Transcribe has no action \"{other}\"")),
        }
    }

    fn settings(&mut self, setup: Setup<Config, SystemConfig>) -> Result<(), String> {
        if let Err(e) = setup.config.validate() {
            // Kept as the status (the SDK shows it now; the upkeep keeps showing it) until good settings come.
            *lock(&self.report.refused) = Some(format!("new settings not taken: {e}"));
            return Err(e);
        }
        let restart = self.running.differs(&setup.config);
        let engine_changed = {
            let now = &read(&self.ctx.config).engine;
            now.url != setup.config.engine.url || now.model != setup.config.engine.model
        };
        if engine_changed {
            // What the old engine named is not what answers now: asked again on the next tick.
            *write(&self.ctx.reported_model) = None;
            self.report.reprobe.store(true, Ordering::Relaxed);
        }
        self.days.store(setup.config.archive.days, Ordering::Relaxed);
        *write(&self.ctx.systems) = systems_of(&setup);
        *write(&self.ctx.config) = setup.config;
        *lock(&self.report.refused) = None;
        if restart {
            self.host.warn(RESTART_WARNING);
        }
        Ok(())
    }

    fn shutdown(&mut self, grace: Duration) {
        self.host.debug(format!("stopping after {} s, {grace:?} to do it in", self.started.elapsed().as_secs()));
        // Everything below ends inside 0.9 x grace, so the recorder never has to kill the process.
        let t0 = Instant::now();
        let deadline = t0 + grace.mul_f64(0.9);
        let remaining = || deadline.saturating_duration_since(Instant::now());
        // Told to stop now; joined (if it has finished) at the end, never waited on beyond the deadline.
        let upkeep = self.upkeep.take().map(|(stop, thread)| {
            let _ = stop.send(());
            thread
        });
        // A call in flight at stop gets its answer, and a pressed button its reply.
        let idle_by = (t0 + grace.mul_f64(0.5)).min(deadline);
        while (!self.queue.is_empty() || self.tests.iter().any(|t| !t.is_finished())) && Instant::now() < idle_by {
            thread::sleep(Duration::from_millis(50));
        }
        let http_budget = if self.http.is_some() { Duration::from_secs(2).min(grace / 4) } else { Duration::ZERO };
        self.queue.shutdown(remaining().saturating_sub(http_budget));
        // A live call still in flight gets its answer now; should it finish later, it sends no second one.
        self.queue.answer_in_flight("shutting down");
        self.report.send();
        if let Some(http) = self.http.take() {
            http.stop(Duration::from_secs(2).min(remaining()));
        }
        if let Some(thread) = upkeep {
            while !thread.is_finished() && !remaining().is_zero() {
                thread::sleep(Duration::from_millis(50));
            }
            if thread.is_finished() {
                let _ = thread.join();
            } // else detached: the process exits around it
        }
    }
}

impl Transcribe {
    /// "Test the engine": the one-second tone to the engine in the form (or the
    /// running one), on a thread of its own so the protocol loop goes on.
    fn test(&mut self, value: &Value, reply: Reply) {
        let running = read(&self.ctx.config).engine.clone();
        let cfg = value.get("engine").and_then(|v| serde_json::from_value::<EngineConfig>(v.clone()).ok()).unwrap_or(running);
        let reported = self.ctx.reported_model.clone();
        let spawned = thread::Builder::new().name("transcribe-test".into()).spawn(move || {
            let engine = Engine::new(Duration::from_secs(cfg.timeout_seconds.clamp(5, 600)));
            let tone = test_tone();
            let audio = Audio { bytes: &tone, file_name: "test.wav", content_type: "audio/wav" };
            let t0 = Instant::now();
            match engine.transcribe(&cfg, audio, &cfg.language, "") {
                Ok(answer) => {
                    let ms = t0.elapsed().as_millis();
                    let model = answer.model.or_else(|| read(&reported).clone()).unwrap_or_else(|| "the engine".into());
                    reply.ok(format!("{model} answered in {ms} ms"));
                }
                Err(EngineError::Retry(e) | EngineError::Fail(e)) => reply.fail(e),
            }
        });
        self.tests.retain(|t| !t.is_finished());
        match spawned {
            Ok(t) => self.tests.push(t),
            // The reply went with the closure; dropped unsent, it answers that it failed.
            Err(e) => self.host.warn(format!("cannot start the engine test: {e}")),
        }
    }
}

/// Metrics, status and health, made from the queue's snapshot.
struct Report {
    host: Host,
    ctx: Ctx,
    queue: Arc<Queue>,
    /// The queue's size as it was started.
    queue_max: usize,
    health: Arc<RwLock<Health>>,
    /// Why the last settings were refused: the status until good ones come.
    refused: Mutex<Option<String>>,
    /// The engine or its model changed: the models probe runs on the next tick.
    reprobe: AtomicBool,
}

impl Report {
    /// The model as the engine names it, else as configured.
    fn model(&self) -> String {
        let reported = read(&self.ctx.reported_model).clone();
        reported.unwrap_or_else(|| read(&self.ctx.config).engine.model.clone())
    }

    fn send(&self) {
        let snap = self.queue.snapshot();
        let url = read(&self.ctx.config).engine.url.clone();
        let model = self.model();
        let transcribed = self.ctx.transcribed.load(Ordering::Relaxed);
        self.host.metrics(&metrics_of(&snap, &url, transcribed, &model));
        {
            // Held while sending, so a refusal recorded by `settings` is never overwritten by a stale Ok.
            let refused = lock(&self.refused);
            let (state, message) = status_of(refused.as_deref(), &snap, self.queue_max, &model, Instant::now());
            self.host.status(state, message);
        }
        let transcribed_24h = self.ctx.store.transcribed_since(now_ms() - DAY_MS).unwrap_or(0);
        *write(&self.health) = Health {
            engine_url: url,
            model,
            up: snap.down_since.is_none() && (snap.last_ok_s.is_some() || snap.in_a_row == 0),
            last_ok_ms: snap.last_ok_s.map(|s| s as i64 * 1000),
            last_error: snap.last_error.clone(),
            queue: snap.queued as u64,
            in_flight: snap.in_flight as u64,
            transcribed_24h,
        };
    }
}

/// The metrics the recorder shows: the queue's figures and the engine as one endpoint.
fn metrics_of(snap: &Snapshot, url: &str, transcribed: u64, model: &str) -> Metrics {
    let last_ok = snap.last_ok_s.map(|s| s as f64);
    let last_error = snap.last_error_s.map(|s| s as f64);
    let state = if snap.down_since.is_some() {
        EndpointState::Down
    } else if snap.in_a_row > 0 {
        EndpointState::Degraded
    } else if snap.last_ok_s.is_some() {
        EndpointState::Up
    } else {
        EndpointState::Unknown
    };
    let engine = Endpoint { name: host_of(url), state, latency_ms: snap.latency_ms, last_ok, last_error: snap.last_error.clone() };
    let extra = BTreeMap::from([
        ("transcribed".to_string(), json!(transcribed)),
        ("archive_queued".to_string(), json!(snap.archive)),
        ("failures".to_string(), json!(snap.failed)),
        ("model".to_string(), json!(model)),
    ]);
    Metrics {
        queued: Some(snap.queued as u64),
        retrying: Some(snap.retrying as u64),
        in_flight: Some(snap.in_flight as u64),
        retries: Some(snap.retries),
        latency_ms: snap.latency_ms,
        last_ok,
        last_error,
        last_error_text: snap.last_error.clone(),
        endpoints: vec![engine],
        extra,
        ..Default::default()
    }
}

/// The status: refused settings, then the engine down for 5 minutes, a queue
/// over half full of live calls and retries (archive work is meant to queue
/// up), an engine failing, else the model.
fn status_of(refused: Option<&str>, snap: &Snapshot, queue_max: usize, model: &str, now: Instant) -> (State, String) {
    if let Some(m) = refused {
        return (State::Error, m.to_string());
    }
    if let Some(since) = snap.down_since {
        let down = now.saturating_duration_since(since);
        if down >= DOWN_IS_ERROR {
            return (State::Error, format!("engine down since {} min: {}", down.as_secs() / 60, snap.last_error));
        }
    }
    let waiting = snap.live + snap.retrying;
    if waiting > queue_max / 2 {
        (State::Warning, format!("{waiting} calls queued"))
    } else if snap.in_a_row > 0 {
        (State::Warning, format!("engine degraded: {}", snap.last_error))
    } else {
        (State::Ok, model.to_string())
    }
}

/// The upkeep thread's state: what is due when.
struct Upkeep {
    report: Arc<Report>,
    archive: Arc<Mutex<Archive>>,
    probe: Engine,
    next_models: Instant,
    last_periodic: Instant,
    next_retain: Instant,
}

impl Upkeep {
    /// One beat: the model probe, the archive pass's page timeout, the
    /// periodic pass and the retention sweep when due, then metrics, status and health.
    fn tick(&mut self, now: Instant) {
        let ctx = &self.report.ctx;
        let host = &self.report.host;
        if self.report.reprobe.swap(false, Ordering::Relaxed) || now >= self.next_models {
            self.next_models = now + MODELS_EVERY;
            let engine = read(&ctx.config).engine.clone();
            // An error leaves the model the engine last named; an answer about an engine since replaced is dropped.
            let probed = self.probe.models(&engine).ok().filter(|_| {
                let current = &read(&ctx.config).engine;
                current.url == engine.url && current.model == engine.model
            });
            if let Some(model) = probed {
                let changed = {
                    let mut cached = write(&ctx.reported_model);
                    let changed = *cached != model;
                    *cached = model.clone();
                    changed
                };
                if let (true, Some(name)) = (changed, model) {
                    host.info(format!("engine model: {name}"));
                }
            }
        }
        lock(&self.archive).check_timeout(now);
        let (every_minutes, retention_days) = {
            let c = read(&ctx.config);
            (c.archive.every_minutes, c.store.retention_days)
        };
        if every_minutes > 0 && now.saturating_duration_since(self.last_periodic) >= Duration::from_secs(u64::from(every_minutes) * 60) {
            self.last_periodic = now;
            if let Err(e) = lock(&self.archive).begin(Mode::Periodic, now_ms()) {
                host.warn(e);
            }
        }
        if now >= self.next_retain {
            self.next_retain = now + RETAIN_EVERY;
            if retention_days > 0 {
                match ctx.store.retain(retention_days, now_ms()) {
                    Ok(0) => {}
                    Ok(n) => host.info(format!("store: {n} transcripts older than {retention_days} days deleted")),
                    Err(e) => host.warn(format!("store: the retention sweep failed: {e}")),
                }
            }
        }
        self.report.send();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::host_of;
    use crate::store::Store;
    use crate::work::tests::speech_call;
    use serde_json::{Value, json};
    use std::path::Path;
    use std::time::{Duration, Instant};
    use trunk_recorder_plugin::testing::{self, MockServer};
    use trunk_recorder_plugin::{Action, CallsPage, EndpointState, Hello, HostInfo, HostMessage, Level, Outcome, PluginMessage, State, StoredCall};

    const OK: &str = r#"{"task":"transcribe","language":"en","text":"Tower 73 go ahead","model":"Qwen3-ASR-1.7B","words":[{"word":"Tower","start":0.1,"end":0.4},{"word":"73","start":0.5,"end":0.9}]}"#;
    const MODELS: &str = r#"{"object":"list","data":[{"id":"Qwen3-ASR-1.7B"}]}"#;

    fn engine() -> MockServer {
        MockServer::start(|r| if r.path == "/v1/models" { (200, MODELS.into()) } else { (200, OK.into()) })
    }

    /// Objects merged key by key; anything else replaced.
    fn merge(into: &mut Value, from: Value) {
        match (into, from) {
            (Value::Object(a), Value::Object(b)) => {
                for (k, v) in b {
                    merge(a.entry(k).or_insert(Value::Null), v);
                }
            }
            (into, from) => *into = from,
        }
    }

    /// `testing::hello` with `{"engine":{"url":url}}` merged with `extra`.
    fn hello_with(dir: &Path, url: &str, extra: Value) -> Hello {
        let mut config = json!({ "engine": { "url": url } });
        merge(&mut config, extra);
        testing::hello(dir, config)
    }

    fn action_results(out: &testing::Output) -> Vec<(u64, bool, String)> {
        out.messages
            .iter()
            .filter_map(|m| match m {
                PluginMessage::ActionResult { request, ok, message } => Some((*request, *ok, message.clone())),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn the_manifest_is_transcribe() {
        let m = trunk_recorder_plugin::describe::<Transcribe>();
        assert_eq!((m.id.as_str(), m.name.as_str(), m.api), ("transcribe", "Transcribe", 1));
        assert_eq!(m.subscribe, ["call.concluded"]);
        assert!(m.live_settings && m.config.is_some() && m.system_config.is_some());
    }

    #[test]
    fn bad_settings_stop_it_and_good_ones_start_it() {
        let dir = testing::temp_dir("transcribe");
        let out = testing::run::<Transcribe>([HostMessage::Hello(testing::hello(&dir, json!({})))]);
        assert_eq!(out.exit_code, trunk_recorder_plugin::EXIT_CONFIG);
        let out = testing::run::<Transcribe>([HostMessage::Hello(testing::hello(&dir, json!({ "engine": { "url": "http://127.0.0.1:1" } })))]);
        assert!(out.ready());
    }

    #[test]
    fn a_live_call_is_transcribed_and_answered() {
        let s = engine();
        let dir = testing::temp_dir("plugin");
        let c = speech_call(&dir, 101);
        let out = testing::run::<Transcribe>([HostMessage::Hello(hello_with(&dir, s.url(), json!({}))), HostMessage::CallConcluded(c.clone())]);
        assert!(out.ready());
        assert_eq!(out.results().iter().map(|r| (r.0.as_str(), r.1)).collect::<Vec<_>>(), [(c.path.as_str(), Outcome::Ok)]);
        assert!(Store::open(&dir.join("data/transcripts.sqlite"), &|_| {}).unwrap().get(&c.path).unwrap().is_some());
        let q = out.messages.iter().filter(|m| matches!(m, PluginMessage::CallsQuery(_))).count();
        assert_eq!(q, 1, "the start pass asked for calls once");
    }

    #[test]
    fn a_call_concluded_twice_gets_one_answer_each() {
        let s = MockServer::start(|r| {
            if r.path == "/v1/models" {
                return (200, MODELS.into());
            }
            std::thread::sleep(Duration::from_millis(300));
            (200, OK.into())
        });
        let dir = testing::temp_dir("plugin");
        let c = speech_call(&dir, 102);
        let out = testing::run::<Transcribe>([
            HostMessage::Hello(hello_with(&dir, s.url(), json!({}))),
            HostMessage::CallConcluded(c.clone()),
            HostMessage::CallConcluded(c.clone()),
        ]);
        let results: Vec<_> = out.results().into_iter().map(|r| (r.1, r.2)).collect();
        assert_eq!(results, [(Outcome::Skipped, "already queued".to_string()), (Outcome::Ok, String::new())]);
    }

    #[test]
    fn a_page_from_the_recorder_feeds_the_archive() {
        let s = engine();
        let dir = testing::temp_dir("plugin");
        let c = speech_call(&dir, 201);
        let stored = StoredCall { path: c.path.clone(), record: c.call.clone(), audio: true, json: true };
        let page = CallsPage { request: 1, entries: vec![stored], more: false, before: 0, error: None };
        let out = testing::run::<Transcribe>([HostMessage::Hello(hello_with(&dir, s.url(), json!({}))), HostMessage::Calls(page)]);
        assert!(out.ready());
        let (row, _) = Store::open(&dir.join("data/transcripts.sqlite"), &|_| {}).unwrap().get(&c.path).unwrap().expect("a row for the archived call");
        assert_eq!(row.text.as_deref(), Some("Tower 73 go ahead"));
        assert!(out.results().is_empty(), "an archive call sends no call.result");
    }

    #[test]
    fn the_test_button_reports_model_and_latency() {
        let s = engine();
        let dir = testing::temp_dir("plugin");
        let test = |request: u64, url: &str| Action { request, action: "test".into(), value: json!({ "engine": { "url": url } }) };
        let out = testing::run::<Transcribe>([
            HostMessage::Hello(hello_with(&dir, s.url(), json!({}))),
            HostMessage::Action(test(7, s.url())),
            HostMessage::Action(test(8, "http://127.0.0.1:1")),
            HostMessage::Action(Action { request: 9, action: "nope".into(), value: json!({}) }),
        ]);
        let mut results = action_results(&out);
        results.sort();
        assert_eq!(results.len(), 3, "{results:?}");
        let (request, ok, message) = &results[0];
        assert!(*request == 7 && *ok && message.contains("Qwen3-ASR-1.7B") && message.contains(" ms"), "{message}");
        assert!(s.requests().iter().any(|r| r.path == "/v1/audio/transcriptions" && r.form_file_name("file").as_deref() == Some("test.wav")));
        assert_eq!((results[1].0, results[1].1), (8, false));
        assert_eq!((results[2].0, results[2].1), (9, false));
    }

    #[test]
    fn the_retranscribe_button_starts_a_pass_or_says_one_runs() {
        let s = engine();
        let dir = testing::temp_dir("plugin");
        let press = |request: u64| Action { request, action: "retranscribe".into(), value: json!({ "archive": { "retranscribeEverything": true } }) };
        // The start pass is still running (no page came back): a second pass is refused.
        let out = testing::run::<Transcribe>([HostMessage::Hello(hello_with(&dir, s.url(), json!({}))), HostMessage::Action(press(3))]);
        assert_eq!(action_results(&out), [(3, false, "An archive pass is already running".to_string())]);
        // Once it is done, one starts (with nothing to do, that is still a start).
        let done = CallsPage { request: 1, entries: vec![], more: false, before: 0, error: None };
        let out =
            testing::run::<Transcribe>([HostMessage::Hello(hello_with(&dir, s.url(), json!({}))), HostMessage::Calls(done), HostMessage::Action(press(4))]);
        assert_eq!(action_results(&out), [(4, true, "Re-transcription started; the log says how many calls were queued".to_string())]);
    }

    #[test]
    fn new_settings_are_taken_and_some_wait_for_a_restart() {
        let s1 = engine();
        let s2 = engine();
        let dir = testing::temp_dir("plugin");
        let c = speech_call(&dir, 301);
        let out = testing::run::<Transcribe>([
            HostMessage::Hello(hello_with(&dir, s1.url(), json!({}))),
            HostMessage::Settings(hello_with(&dir, s2.url(), json!({ "engine": { "concurrency": 4 } }))),
            HostMessage::CallConcluded(c.clone()),
        ]);
        assert_eq!(out.results().iter().map(|r| (r.0.as_str(), r.1)).collect::<Vec<_>>(), [(c.path.as_str(), Outcome::Ok)]);
        assert!(s2.requests().iter().any(|r| r.path == "/v1/audio/transcriptions"), "the call went to the new engine");
        assert!(!s1.requests().iter().any(|r| r.path == "/v1/audio/transcriptions"));
        assert!(out.logs().iter().any(|(l, m)| *l == Level::Warn && m.contains("Concurrency")), "{:?}", out.logs());
        assert_eq!(out.status().map(|s| s.0), Some(State::Ok));

        let out =
            testing::run::<Transcribe>([HostMessage::Hello(hello_with(&dir, s1.url(), json!({}))), HostMessage::Settings(hello_with(&dir, "", json!({})))]);
        let (state, message) = out.status().unwrap();
        assert_eq!(state, State::Error);
        assert!(message.starts_with("new settings not taken: "), "{message}");
        assert!(!out.logs().iter().any(|(_, m)| m.contains("Concurrency")), "nothing that waits for a restart changed");
    }

    #[test]
    fn shutdown_fails_what_is_still_queued() {
        let s = MockServer::start(|r| {
            if r.path == "/v1/models" {
                return (200, MODELS.into());
            }
            std::thread::sleep(Duration::from_secs(3));
            (200, OK.into())
        });
        let dir = testing::temp_dir("plugin");
        let calls: Vec<_> = (1..=3).map(|tg| speech_call(&dir, 400 + tg)).collect();
        let mut messages = vec![HostMessage::Hello(hello_with(&dir, s.url(), json!({ "engine": { "concurrency": 1 } })))];
        messages.extend(calls.iter().cloned().map(HostMessage::CallConcluded));
        let t0 = Instant::now();
        let out = testing::run::<Transcribe>(messages);
        assert!(t0.elapsed() < Duration::from_secs(12), "took {:?}", t0.elapsed());
        let results: Vec<_> = out.results().into_iter().map(|r| (r.0, r.1, r.2)).collect();
        assert_eq!(
            results,
            [
                (calls[0].path.clone(), Outcome::Ok, String::new()),
                (calls[2].path.clone(), Outcome::Failed, "shutting down".to_string()),
                (calls[1].path.clone(), Outcome::Ok, String::new()),
            ]
        );
    }

    #[test]
    fn a_new_engine_or_model_forgets_the_reported_model() {
        let s = engine();
        let dir = testing::temp_dir("plugin");
        let setup = |model: &str| Setup::<Config, SystemConfig> {
            config: serde_json::from_value(json!({ "engine": { "url": s.url(), "model": model } })).unwrap(),
            systems: Vec::new(),
            host: HostInfo::default(),
            capture_dir: dir.join("capture"),
            data_dir: dir.join("data"),
            audio_formats: vec!["wav".into()],
        };
        std::fs::create_dir_all(dir.join("data")).unwrap();
        let (host, _out) = testing::capture();
        let mut plugin = Transcribe::start(host, setup("default")).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while read(&plugin.ctx.reported_model).is_none() {
            assert!(Instant::now() < deadline, "the start probe never named the model");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(read(&plugin.ctx.reported_model).as_deref(), Some("Qwen3-ASR-1.7B"));

        plugin.settings(setup("default")).unwrap();
        assert!(read(&plugin.ctx.reported_model).is_some(), "the same engine and model: kept");
        plugin.settings(setup("Qwen3-ASR-1.7B")).unwrap();
        assert_eq!(*read(&plugin.ctx.reported_model), None, "a new model: forgotten");
        assert!(plugin.report.reprobe.load(Ordering::Relaxed), "and asked again on the next tick");
        plugin.shutdown(Duration::from_secs(1));
    }

    #[test]
    fn a_taken_port_stops_it_and_a_free_one_serves() {
        let s = engine();
        let dir = testing::temp_dir("plugin");
        let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = taken.local_addr().unwrap().port();
        let out = testing::run::<Transcribe>([HostMessage::Hello(hello_with(&dir, s.url(), json!({ "http": { "port": port } })))]);
        assert_eq!(out.exit_code, trunk_recorder_plugin::EXIT_CONFIG);
        assert!(out.status().is_some_and(|(state, m)| state == State::Error && m.contains("cannot listen")), "{:?}", out.status());
        drop(taken);
        let out = testing::run::<Transcribe>([HostMessage::Hello(hello_with(&dir, s.url(), json!({ "http": { "port": port } })))]);
        assert!(out.ready());
        let serving = format!("serving http://127.0.0.1:{port}");
        assert!(out.logs().iter().any(|(l, m)| *l == Level::Info && m.starts_with("transcribe ") && m.ends_with(&serving)), "{:?}", out.logs());
    }

    #[test]
    fn the_status_rules_in_order() {
        let now = Instant::now();
        let ok = Snapshot::default();
        assert_eq!(status_of(None, &ok, 100, "m", now), (State::Ok, "m".to_string()));
        let failing = Snapshot { in_a_row: 1, last_error: "HTTP 503".into(), ..Default::default() };
        assert_eq!(status_of(None, &failing, 100, "m", now), (State::Warning, "engine degraded: HTTP 503".to_string()));
        let full = Snapshot { queued: 51, live: 31, retrying: 20, ..failing.clone() };
        assert_eq!(status_of(None, &full, 100, "m", now), (State::Warning, "51 calls queued".to_string()));
        let backlog = Snapshot { queued: 90, live: 10, archive: 80, ..Default::default() };
        assert_eq!(status_of(None, &backlog, 100, "m", now), (State::Ok, "m".to_string()), "archive work is meant to queue up");
        let down = Snapshot { down_since: Some(now - Duration::from_secs(360)), ..full.clone() };
        assert_eq!(status_of(None, &down, 100, "m", now), (State::Error, "engine down since 6 min: HTTP 503".to_string()));
        let lately = Snapshot { down_since: Some(now - Duration::from_secs(60)), ..full };
        assert_eq!(status_of(None, &lately, 100, "m", now).0, State::Warning, "down under 5 min is a warning");
        assert_eq!(status_of(Some("new settings not taken: x"), &ok, 100, "m", now), (State::Error, "new settings not taken: x".to_string()));
        let m = metrics_of(&down, "http://eddie:8766", 3, "m");
        assert_eq!((m.endpoints[0].name.as_str(), m.endpoints[0].state, m.queued), ("eddie:8766", EndpointState::Down, Some(51)));
        assert_eq!(metrics_of(&failing, "http://e", 0, "m").endpoints[0].state, EndpointState::Degraded);
        assert_eq!(metrics_of(&ok, "http://e", 0, "m").endpoints[0].state, EndpointState::Unknown);
    }

    #[test]
    fn shutdown_ends_inside_the_grace_with_a_call_still_in_flight() {
        // An engine that answers nothing (the models probe included) for longer than the grace.
        let s = MockServer::start(|_| {
            std::thread::sleep(Duration::from_secs(15));
            (200, OK.into())
        });
        let dir = testing::temp_dir("plugin");
        let c = speech_call(&dir, 451);
        let hello = hello_with(&dir, s.url(), json!({ "engine": { "concurrency": 1 } }));
        let t0 = Instant::now();
        let out = testing::run::<Transcribe>([HostMessage::Hello(hello), HostMessage::CallConcluded(c.clone())]);
        assert!(t0.elapsed() < Duration::from_millis(9500), "took {:?} of a 10 s grace", t0.elapsed());
        let results: Vec<_> = out.results().into_iter().map(|r| (r.0, r.1, r.2)).collect();
        assert_eq!(results, [(c.path.clone(), Outcome::Failed, "shutting down".to_string())], "exactly one answer for the call in flight");
    }

    #[test]
    fn metrics_and_status_carry_the_engine() {
        let s = engine();
        let dir = testing::temp_dir("plugin");
        let c = speech_call(&dir, 501);
        let out = testing::run::<Transcribe>([HostMessage::Hello(hello_with(&dir, s.url(), json!({}))), HostMessage::CallConcluded(c)]);
        assert_eq!(out.results()[0].1, Outcome::Ok);
        let metrics = out.metrics();
        let m = metrics.last().expect("metrics");
        assert_eq!(m.endpoints.len(), 1);
        assert_eq!((m.endpoints[0].name.as_str(), m.endpoints[0].state), (host_of(s.url()).as_str(), EndpointState::Up));
        assert_eq!(m.extra["transcribed"], json!(1));
        assert_eq!(m.extra["failures"], json!(0));
        assert_eq!(metrics_of(&Snapshot { failed: 4, ..Default::default() }, "http://e", 0, "m").extra["failures"], json!(4));
        assert_eq!(m.extra["model"], json!("Qwen3-ASR-1.7B"));
        assert!(m.last_ok.is_some() && m.latency_ms.is_some());
        assert_eq!(out.status(), Some((State::Ok, "Qwen3-ASR-1.7B".to_string())));
    }
}
