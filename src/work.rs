//! The pipeline: one call from its audio to a stored, announced transcript.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use trunk_recorder_plugin::{CallRecord, Setup, SrcEntry};

use crate::audio::{self, Pcm};
use crate::config::{Config, SystemConfig};
use crate::engine::{self, Answer, Engine, EngineError};
use crate::filter::{self, Verdict};
use crate::queue::{Done, Job};
use crate::store::{Row, SegmentRow, Status, Store, Transcript};
use crate::{gate, prompt, segment};

/// One system as the pipeline sees it.
#[derive(Clone, Debug)]
pub struct SystemView {
    pub short_name: String,
    /// What people call it (for the prompt); empty when it has no name.
    pub name: String,
    pub enabled: bool,
    /// The plugin's settings as they are for this system.
    pub settings: Config,
}

/// Each system's name, switch and effective settings.
pub fn systems_of(setup: &Setup<Config, SystemConfig>) -> Vec<SystemView> {
    setup
        .systems
        .iter()
        .map(|s| SystemView {
            short_name: s.short_name.clone(),
            name: s.name.clone(),
            enabled: s.config.as_ref().is_none_or(|c| c.enabled),
            settings: s.settings.clone(),
        })
        .collect()
}

/// What every job shares.
#[derive(Clone)]
pub struct Ctx {
    /// The settings for the whole recorder (a call of an unknown system uses these).
    pub config: Arc<RwLock<Config>>,
    pub systems: Arc<RwLock<Vec<SystemView>>>,
    pub store: Store,
    pub engine: Engine,
    /// The model the engine last said it runs, for answers that do not name one.
    pub reported_model: Arc<RwLock<Option<String>>>,
    /// Every stored transcript, as it is written.
    pub announce: tokio::sync::broadcast::Sender<Transcript>,
    pub capture_dir: PathBuf,
    /// `ok` transcripts so far.
    pub transcribed: Arc<AtomicU64>,
}

/// The call's start, in Unix milliseconds.
pub fn start_ms(r: &CallRecord) -> i64 {
    if r.start_time_ms > 0 { r.start_time_ms } else { r.start_time * 1000 }
}

/// The call's length, in milliseconds.
pub fn length_ms(r: &CallRecord) -> i64 {
    if r.call_length_ms > 0 { r.call_length_ms } else { (r.call_length * 1000.0).round() as i64 }
}

fn read<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|e| e.into_inner())
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as i64)
}

/// The call's system, or the root settings (enabled, unnamed) for one we do not know.
fn system_of(ctx: &Ctx, short_name: &str) -> SystemView {
    if let Some(s) = read(&ctx.systems).iter().find(|s| s.short_name == short_name) {
        return s.clone();
    }
    SystemView { short_name: short_name.to_string(), name: String::new(), enabled: true, settings: read(&ctx.config).clone() }
}

/// A row for `job` with the call's fields and the engine that is (or would be) asked; the outcome is the caller's.
fn row_for(job: &Job, settings: &Config, status: Status, reason: Option<String>) -> Row {
    let r = &job.call.call;
    Row {
        path: job.call.path.clone(),
        short_name: r.short_name.clone(),
        talkgroup: r.talkgroup,
        start_ms: start_ms(r),
        length_ms: length_ms(r),
        status,
        reason,
        engine: engine::host_of(&settings.engine.url),
        model_asked: settings.engine.model.clone(),
        attempts: job.tries + 1,
        transcribed_ms: now_ms(),
        ..Default::default()
    }
}

/// Store `row` with `segments` and tell the listeners (none listening is fine).
fn write(ctx: &Ctx, row: &Row, segments: &[SegmentRow]) -> Result<(), String> {
    ctx.store.put(row, segments).map_err(|e| format!("store: {e}"))?;
    let _ = ctx.announce.send(Transcript::from_row(row, segments));
    Ok(())
}

/// A `skipped` row, and `Done::Skipped`.
fn skip(ctx: &Ctx, job: &Job, settings: &Config, reason: &str) -> Done {
    match write(ctx, &row_for(job, settings, Status::Skipped, Some(reason.to_string())), &[]) {
        Ok(()) => Done::Skipped(reason.to_string()),
        Err(e) => Done::Failed(e),
    }
}

/// The call's audio file.
enum Source {
    Wav(PathBuf),
    M4a(PathBuf),
}

/// `files.wav`, `files.m4a`, then the same two under the capture folder.
fn find_audio(ctx: &Ctx, job: &Job) -> Option<Source> {
    let files = &job.call.files;
    if files.wav.is_file() {
        return Some(Source::Wav(files.wav.clone()));
    }
    if let Some(m4a) = files.m4a.as_ref().filter(|p| p.is_file()) {
        return Some(Source::M4a(m4a.clone()));
    }
    let base = ctx.capture_dir.join(&job.call.path);
    let in_capture = |ext: &str| Some(PathBuf::from(format!("{}.{ext}", base.display()))).filter(|p| p.is_file());
    in_capture("wav").map(Source::Wav).or_else(|| in_capture("m4a").map(Source::M4a))
}

/// The audio as read: a WAV decoded, an M4A still a file.
enum Loaded {
    Wav(Pcm),
    M4a(PathBuf),
}

/// What goes to the engine.
struct Upload {
    bytes: Vec<u8>,
    file_name: &'static str,
    content_type: &'static str,
}

/// A WAV made 16 kHz (and filtered when asked); an M4A's bytes as they are.
fn prepare(loaded: Loaded, settings: &Config) -> Result<Upload, String> {
    match loaded {
        Loaded::Wav(pcm) => {
            let mut samples = audio::resample_to_16k(&pcm);
            if settings.prepare.p25_filter {
                audio::p25_filter(&mut samples);
            }
            Ok(Upload { bytes: audio::wav_16k(&samples), file_name: "call.wav", content_type: "audio/wav" })
        }
        Loaded::M4a(path) => {
            let bytes = std::fs::read(path).map_err(|e| format!("unreadable audio: {e}"))?;
            Ok(Upload { bytes, file_name: "call.m4a", content_type: "audio/mp4" })
        }
    }
}

/// Runs one job through the pipeline. `warn` hears of a rejected transcript
/// (its path and reason, never its text).
pub fn process(ctx: &Ctx, job: &Job, warn: &dyn Fn(String)) -> Done {
    let record = &job.call.call;

    // 1. The system.
    let system = system_of(ctx, &record.short_name);
    let settings = &system.settings;
    if !system.enabled {
        return skip(ctx, job, settings, "system disabled");
    }

    // 2. The audio.
    let Some(source) = find_audio(ctx, job) else {
        return skip(ctx, job, settings, "no audio");
    };

    // 3. The gate.
    let loaded = match source {
        Source::Wav(path) => match audio::read_wav(&path) {
            Ok(pcm) => Loaded::Wav(pcm),
            Err(e) => return Done::Failed(format!("unreadable audio: {e}")),
        },
        Source::M4a(path) => Loaded::M4a(path),
    };
    let pcm = match &loaded {
        Loaded::Wav(pcm) => Some(pcm),
        Loaded::M4a(_) => None,
    };
    if let Some(reason) = gate::check(record, pcm, &settings.gate) {
        return skip(ctx, job, settings, reason);
    }

    // 4. Prepare.
    let upload = match prepare(loaded, settings) {
        Ok(upload) => upload,
        Err(e) => return Done::Failed(e),
    };

    // 5. The prompt.
    let prompt = prompt::build(record, &system.name, &settings.prompt);

    // 6. The engine.
    let audio = engine::Audio { bytes: &upload.bytes, file_name: upload.file_name, content_type: upload.content_type };
    let started = Instant::now();
    let answer = match ctx.engine.transcribe(&settings.engine, audio, &settings.engine.language, &prompt) {
        Ok(answer) => answer,
        Err(EngineError::Retry(e)) => return Done::Retry(e),
        Err(EngineError::Fail(e)) => return Done::Failed(e),
    };
    let took = started.elapsed();

    // 7. The filters.
    let length_s = length_ms(record) as f64 / 1000.0;
    let (status, reason) = match filter::check(&answer.text, length_s, &settings.filter) {
        Verdict::Ok => (Status::Ok, None),
        Verdict::Empty => (Status::Empty, None),
        Verdict::Rejected(why) => {
            warn(format!("rejected {}: {why}", job.call.path));
            (Status::Rejected, Some(why.to_string()))
        }
    };

    // 8. The segments.
    let segments = if status == Status::Ok { segments_of(&answer, &record.src_list, length_s) } else { Vec::new() };

    // 9. The row.
    let Answer { text, language, model, raw, .. } = answer;
    let row = Row {
        text: (status == Status::Ok).then_some(text),
        language,
        model_reported: model.or_else(|| read(&ctx.reported_model).clone()),
        prompt: Some(prompt),
        took_ms: Some(took.as_millis() as u64),
        raw: Some(raw),
        ..row_for(job, settings, status, reason)
    };

    // 10. Store and announce.
    if let Err(e) = write(ctx, &row, &segments) {
        return Done::Failed(e);
    }
    if status == Status::Ok {
        ctx.transcribed.fetch_add(1, Ordering::Relaxed);
    }
    Done::Ok { took }
}

fn segments_of(answer: &Answer, src_list: &[SrcEntry], length_s: f64) -> Vec<SegmentRow> {
    segment::split(&answer.words, &answer.text, src_list, length_s)
        .into_iter()
        .enumerate()
        .map(|(i, s)| SegmentRow { seq: i as u32, unit: s.unit, start_s: s.start_s, end_s: s.end_s, text: s.text })
        .collect()
}

/// A `failed` row for a job that gave up, counting the attempts of earlier passes; then announced.
pub fn record_failed(ctx: &Ctx, job: &Job, reason: &str) {
    let system = system_of(ctx, &job.call.call.short_name);
    let mut row = row_for(job, &system.settings, Status::Failed, Some(reason.to_string()));
    if let Ok(Some((before, _))) = ctx.store.get(&job.call.path) {
        row.attempts = before.attempts + 1;
    }
    // A store error has no one to tell here; the queue logs the failure itself.
    let _ = write(ctx, &row, &[]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audio::{read_wav, sine, write_temp};
    use crate::queue::Kind;
    use crate::store::Status;
    use std::cell::RefCell;
    use std::path::Path;
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};
    use trunk_recorder_plugin::testing::{self, MockServer};
    use trunk_recorder_plugin::{CallRecord, ConcludedCall, HostInfo, Setup, SrcEntry, System};

    const OK: &str = r#"{"task":"transcribe","language":"en","text":"Tower 73 go ahead","model":"Qwen3-ASR-1.7B","words":[{"word":"Tower","start":0.1,"end":0.4},{"word":"73","start":0.5,"end":0.9}]}"#;

    fn ctx(url: &str) -> Ctx {
        let mut config = Config::default();
        config.engine.url = url.to_string();
        let system = SystemView { short_name: "sys1".into(), name: "System One".into(), enabled: true, settings: config.clone() };
        Ctx {
            config: Arc::new(RwLock::new(config)),
            systems: Arc::new(RwLock::new(vec![system])),
            store: Store::in_memory(),
            engine: Engine::new(Duration::from_secs(5)),
            reported_model: Arc::new(RwLock::new(None)),
            announce: tokio::sync::broadcast::channel(16).0,
            capture_dir: testing::temp_dir("work-capture"),
            transcribed: Arc::new(AtomicU64::new(0)),
        }
    }

    /// `testing::call` with 3 s of a 0.3-amplitude 1 kHz tone at 8 kHz in place of its silence, and no M4A.
    fn speech_call(dir: &Path, tg: u32) -> ConcludedCall {
        let mut c = testing::call(dir, "sys1", tg);
        let spec = hound::WavSpec { channels: 1, sample_rate: 8000, bits_per_sample: 16, sample_format: hound::SampleFormat::Int };
        let mut w = hound::WavWriter::create(&c.files.wav, spec).unwrap();
        for s in sine(8000, 1000.0, 3.0, 0.3) {
            w.write_sample((s * i16::MAX as f32) as i16).unwrap();
        }
        w.finalize().unwrap();
        c.files.m4a = None;
        c
    }

    fn job(c: ConcludedCall) -> Job {
        Job { call: c, kind: Kind::Live, enqueued: Instant::now(), tries: 0 }
    }

    #[test]
    fn a_call_ends_as_an_ok_row_with_segments_and_an_announcement() {
        let s = MockServer::start(|_| (200, OK.into()));
        let ctx = ctx(s.url());
        let mut rx = ctx.announce.subscribe();
        let dir = testing::temp_dir("work");
        let mut c = speech_call(&dir, 101);
        c.call.src_list = vec![SrcEntry { src: 1001, pos: 0.0, tag: "Engine 3".into(), ..Default::default() }];
        c.call.talkgroup_tag = "Fire".into();
        assert!(matches!(process(&ctx, &job(c.clone()), &|_| {}), Done::Ok { .. }));
        let (row, segs) = ctx.store.get(&c.path).unwrap().unwrap();
        assert_eq!(
            (row.status, row.text.as_deref(), row.model_reported.as_deref(), row.attempts),
            (Status::Ok, Some("Tower 73 go ahead"), Some("Qwen3-ASR-1.7B"), 1)
        );
        assert_eq!((segs.len(), segs[0].unit, segs[0].text.as_str()), (1, Some(1001), "Tower 73"));
        assert_eq!(row.prompt.as_deref(), Some("Fire. System One. Units: Engine 3")); // testing::call leaves the description empty
        assert_eq!((row.engine.as_str(), row.model_asked.as_str()), (s.url().trim_start_matches("http://"), "default"));
        assert_eq!((row.start_ms, row.length_ms, row.raw.as_deref()), (1_700_000_101_000, 3000, Some(OK)));
        assert!(row.took_ms.is_some() && row.transcribed_ms > 1_700_000_000_000);
        let r = &s.requests()[0];
        assert_eq!(r.form_file_name("file").unwrap(), "call.wav");
        assert_eq!(read_wav(&write_temp(&r.form_field("file").unwrap())).unwrap().rate, 16000, "posted at 16 kHz");
        assert_eq!(rx.try_recv().unwrap().path, c.path);
        assert_eq!(ctx.transcribed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn the_gate_skips_before_the_network() {
        let s = MockServer::start(|_| (200, OK.into()));
        let ctx = ctx(s.url());
        let dir = testing::temp_dir("work");
        let silent = testing::call(&dir, "sys1", 1); // silence
        assert!(matches!(process(&ctx, &job(silent.clone()), &|_| {}), Done::Skipped(m) if m == "silent"));
        let mut enc = speech_call(&dir, 2);
        enc.call.encrypted = true;
        assert!(matches!(process(&ctx, &job(enc), &|_| {}), Done::Skipped(m) if m == "encrypted"));
        let mut short = speech_call(&dir, 3);
        short.call.call_length = 0.4;
        assert!(matches!(process(&ctx, &job(short), &|_| {}), Done::Skipped(m) if m == "too short"));
        assert!(s.requests().is_empty());
        let (row, _) = ctx.store.get(&silent.path).unwrap().unwrap();
        assert_eq!((row.status, row.reason.as_deref(), row.text.as_deref()), (Status::Skipped, Some("silent"), None));
        assert_eq!((row.engine.as_str(), row.model_asked.as_str()), (s.url().trim_start_matches("http://"), "default"));
    }

    #[test]
    fn a_disabled_system_and_a_missing_file() {
        let s = MockServer::start(|_| (200, OK.into()));
        let ctx = ctx(s.url());
        let dir = testing::temp_dir("work");
        let c = speech_call(&dir, 4);
        ctx.systems.write().unwrap()[0].enabled = false;
        assert!(matches!(process(&ctx, &job(c.clone()), &|_| {}), Done::Skipped(m) if m == "system disabled"));
        assert_eq!(ctx.store.get(&c.path).unwrap().unwrap().0.reason.as_deref(), Some("system disabled"));
        ctx.systems.write().unwrap()[0].enabled = true;

        let mut gone = c.clone();
        gone.files.wav = dir.join("nowhere.wav");
        gone.files.m4a = Some(dir.join("nowhere.m4a"));
        assert!(matches!(process(&ctx, &job(gone.clone()), &|_| {}), Done::Skipped(m) if m == "no audio"));
        assert_eq!(ctx.store.get(&c.path).unwrap().unwrap().0.reason.as_deref(), Some("no audio"));
        assert!(s.requests().is_empty());

        let found = Ctx { capture_dir: dir.clone(), ..ctx.clone() };
        assert!(matches!(process(&found, &job(gone), &|_| {}), Done::Ok { .. }), "<capture_dir>/<path>.wav is the fallback");
        assert_eq!(s.requests()[0].form_file_name("file").unwrap(), "call.wav");
    }

    #[test]
    fn an_m4a_goes_as_it_is() {
        let s = MockServer::start(|_| (200, OK.into()));
        let ctx = ctx(s.url());
        let dir = testing::temp_dir("work");
        let c = testing::call(&dir, "sys1", 5); // its WAV was silent: an M4A has no RMS gate
        std::fs::remove_file(&c.files.wav).unwrap();
        assert!(matches!(process(&ctx, &job(c.clone()), &|_| {}), Done::Ok { .. }));
        let r = &s.requests()[0];
        assert_eq!(r.form_file_name("file").unwrap(), "call.m4a");
        assert_eq!(r.form_field("file").unwrap(), std::fs::read(c.files.m4a.as_ref().unwrap()).unwrap());
        assert!(String::from_utf8_lossy(&r.body).contains("Content-Type: audio/mp4"));
        assert_eq!(ctx.store.get(&c.path).unwrap().unwrap().0.status, Status::Ok);
    }

    #[test]
    fn empty_and_rejected_keep_the_raw_and_no_text() {
        let dir = testing::temp_dir("work");
        let empty = r#"{"text":""}"#;
        let s = MockServer::start(move |_| (200, empty.into()));
        let ctx = ctx(s.url());
        let c = speech_call(&dir, 6);
        assert!(matches!(process(&ctx, &job(c.clone()), &|_| {}), Done::Ok { .. }));
        let (row, segs) = ctx.store.get(&c.path).unwrap().unwrap();
        assert_eq!((row.status, row.text.as_deref(), row.raw.as_deref(), segs.len()), (Status::Empty, None, Some(empty), 0));
        assert_eq!(ctx.transcribed.load(Ordering::Relaxed), 0);

        let thanks = r#"{"text":"Thank you for watching"}"#;
        let s = MockServer::start(move |_| (200, thanks.into()));
        let ctx = self::ctx(s.url());
        let c = speech_call(&dir, 7);
        let warned = RefCell::new(Vec::new());
        assert!(matches!(process(&ctx, &job(c.clone()), &|m| warned.borrow_mut().push(m)), Done::Ok { .. }));
        let (row, segs) = ctx.store.get(&c.path).unwrap().unwrap();
        assert_eq!(
            (row.status, row.reason.as_deref(), row.text.as_deref(), row.raw.as_deref(), segs.len()),
            (Status::Rejected, Some("hallucination"), None, Some(thanks), 0)
        );
        assert_eq!(warned.into_inner(), vec![format!("rejected {}: hallucination", c.path)], "the reason, never the text");
    }

    #[test]
    fn engine_failures_write_no_row_and_record_failed_does() {
        let s = MockServer::start(|_| (503, "".into()));
        let ctx = ctx(s.url());
        let dir = testing::temp_dir("work");
        let c = speech_call(&dir, 9);
        assert!(matches!(process(&ctx, &job(c.clone()), &|_| {}), Done::Retry(_)));
        assert!(ctx.store.get(&c.path).unwrap().is_none());
        let mut rx = ctx.announce.subscribe();
        record_failed(&ctx, &Job { tries: 3, ..job(c.clone()) }, "gave up: 503");
        let (row, _) = ctx.store.get(&c.path).unwrap().unwrap();
        assert_eq!((row.status, row.reason.as_deref(), row.attempts), (Status::Failed, Some("gave up: 503"), 4));
        assert_eq!(rx.try_recv().unwrap().status, "failed");
        record_failed(&ctx, &job(c.clone()), "again");
        assert_eq!(ctx.store.get(&c.path).unwrap().unwrap().0.attempts, 5, "attempts accumulate across passes");

        let s = MockServer::start(|_| (400, "bad".into()));
        let ctx = self::ctx(s.url());
        assert!(matches!(process(&ctx, &job(c.clone()), &|_| {}), Done::Failed(_)));
        assert!(ctx.store.get(&c.path).unwrap().is_none());
    }

    #[test]
    fn a_record_without_ms_fields_gets_seconds_times_1000() {
        let r = CallRecord { start_time: 1_700_000_000, call_length: 4.5, ..Default::default() };
        assert_eq!((start_ms(&r), length_ms(&r)), (1_700_000_000_000, 4500));
        let r = CallRecord { start_time: 1, start_time_ms: 1_700_000_000_123, call_length_ms: 4512, ..Default::default() };
        assert_eq!((start_ms(&r), length_ms(&r)), (1_700_000_000_123, 4512));
    }

    #[test]
    fn systems_of_takes_each_systems_name_switch_and_settings() {
        let mut own = Config::default();
        own.engine.model = "big".into();
        let system = |short: &str, config: Option<SystemConfig>, settings: Config| System {
            index: 0,
            short_name: short.into(),
            kind: "p25".into(),
            name: format!("{short} name"),
            fcc: Default::default(),
            fcc_call_sign: String::new(),
            fcc_name: String::new(),
            sites: Vec::new(),
            config,
            settings,
        };
        let setup = Setup {
            config: Config::default(),
            systems: vec![system("a", None, own), system("b", Some(SystemConfig { enabled: false }), Config::default())],
            host: HostInfo::default(),
            capture_dir: Default::default(),
            data_dir: Default::default(),
            audio_formats: Vec::new(),
        };
        let views = systems_of(&setup);
        let got: Vec<_> = views.iter().map(|v| (v.short_name.as_str(), v.name.as_str(), v.enabled, v.settings.engine.model.as_str())).collect();
        assert_eq!(got, vec![("a", "a name", true, "big"), ("b", "b name", false, "default")]);
    }
}
