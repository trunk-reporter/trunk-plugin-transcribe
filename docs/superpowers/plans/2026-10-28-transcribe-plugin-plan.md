# Transcribe Plugin Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Trunk Recorder Pro plugin, `transcribe`, that posts every concluded call's audio to an OpenAI-compatible `/v1/audio/transcriptions` endpoint, keeps the transcripts in its own SQLite file, and serves them over HTTP with CORS and server-sent events.

**Architecture:** One Rust binary in four parts: the SDK's protocol loop (never blocks), a bounded priority work queue with `concurrency` worker threads (live calls first, archive work on at most one worker), a WAL-mode SQLite store with an FTS5 index, and an axum HTTP server on a thread of its own. The archive pass pages TRP's call store through `calls.query` and queues what has no transcript yet.

**Tech Stack:** Rust edition 2024 (rust-version 1.85); `trunk-recorder-plugin` 0.9.0 as a git dependency on TRP PR #104's branch; `reqwest` 0.12 (blocking, rustls, no default features); `rusqlite` 0.41 bundled (FTS5 is compiled in); `hound` 3 for WAV reading; `axum` 0.8, `tokio` 1 (`rt`, `sync`, `net`, `time`, `macros`), `tower-http` 0.6; `schemars` 1, `serde`, `serde_json`.

**Spec:** `docs/superpowers/specs/2026-10-27-transcribe-plugin-design.md` (a copy of the approved design; the original lives in the trunk-reporter repo).

## Global Constraints

- Plugin id, crate name and executable name: `transcribe`. Display name `Transcribe`. Repo `https://github.com/trunk-reporter/trunk-plugin-transcribe`, license MIT.
- `edition = "2024"`, `rust-version = "1.85"`. `rustfmt.toml`: `max_width = 160`, `use_small_heuristics = "Max"` (the template's and TRP's style). `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` and `cargo test` pass at every commit.
- SDK: `trunk-recorder-plugin = { git = "https://github.com/TrunkRecorder/trunk-recorder-pro", rev = "8870d27d513f54575b59dbae819486b00f5e47e4" }` (PR #104's tip, SDK 0.9.0 with `Host::calls`). Switch to `"0.9"` from crates.io when it is published; nothing else changes.
- Pure Rust TLS: `reqwest` with `default-features = false, features = ["blocking", "rustls-tls"]`. No OpenSSL, no ffmpeg, no AAC decoder, no model.
- Never `println!` (stdout is the protocol). Logs go through `Host`. Transcript text is never logged above `debug`. The API key is `x-secret` and never logged.
- Every `call.concluded` on the live path gets exactly one `call.result`. Archive and re-transcribe jobs (from `calls.query`) send none.
- Settings keys are camelCase exactly as the spec's settings block names them (`engine.timeoutSeconds`, `gate.minSeconds`, `archive.everyMinutes`, `store.retentionDays`, `http.origins`, …). Defaults are the spec's.
- HTTP JSON shapes are the spec's `Transcript` object and route table, byte for byte as the plan's tests assert them.
- Tests use the SDK's `testing` module (`testing::run`, `testing::capture`, `testing::call`, `testing::MockServer`) and never a live engine.

## Decisions the plan makes (where the spec left room or the SDK settles it)

- **Own queue, not the SDK's `CallQueue`.** `CallQueue` has no priority between lanes and gives every lane the same thread count, so "live first, archive on one worker" cannot be expressed with it. `queue.rs` is a small priority queue; its retry schedule (2 s, 8 s, 30 s), queue-full rule (oldest dropped), 50-minute age rule and shutdown drain are the spec's own words.
- **Per-system `language` and `prompt.extra` come from the SDK's per-system override** of the whole settings object (`System::settings`), which TRP already offers for every plugin. `SystemConfig` therefore has one field, `enabled`. Health, the model probe and the engine's status use the root settings' engine.
- **The archive pass walks the whole window on start and on re-transcribe**; only the periodic passes use the spec's stop rule ("first page whose calls all have rows"). Otherwise a pass at start would stop at the last page before the shutdown and never reach a gap.
- **`failed` rows are retried by the archive pass** while their `attempts` is under 10; after that they are left alone. A `failed` row is what the archive pass's "no row" means in practice, or the engine being down for an hour would leave holes forever.
- **The P25 filter chain** (`prepare.p25Filter`) is the model card's sox chain approximated with biquads: 2nd-order Butterworth high-pass at 300 Hz and low-pass at 3400 Hz, notches at 1200 Hz (Q 5), 1800 Hz (Q 5) and 1000 Hz (Q 8), then peak-normalised to −3 dBFS. Applied after resampling, at 16 kHz.
- **The resampler** is a windowed-sinc interpolator: for each output sample at time `t = m / 16000`, the sum over the 33 nearest input samples `n` of `x[n] · sinc(2·fc·(t − n/r)) · hann(n)`, with `fc = 0.45 · min(r, 16000)`. It takes any input rate, so a WAV that is not 8 kHz still works; stereo is averaged to mono first.
- **FTS5** is kept in sync by the external-content triggers from the SQLite documentation. The search string is turned into a bag of quoted words (each whitespace token wrapped in double quotes, inner quotes doubled), so FTS5 syntax in user input cannot error.
- **Replacing a row** is `DELETE` then `INSERT` in one transaction (`INSERT OR REPLACE` would not fire the FTS delete trigger). The old row is visible until the transaction commits.
- **The test-engine sample** is synthesised at run time: one second of a 440 Hz tone at −20 dBFS, 16 kHz 16-bit mono. Only the HTTP status, the `model` field and the latency are reported; the text is not checked.
- **Re-transcribe's "everything" box** is a setting, `archive.retranscribeEverything` (default false): the button's `value` carries the settings as edited, so the box is read from there. The button answers at once ("started") and the pass logs how many calls it queued when it ends.
- **Live settings:** engine, gate, prepare, prompt, filter, archive, store and per-system settings apply at once. `engine.concurrency`, `queue.max` and `http.*` apply when recording next starts; a change to them logs a warning saying so and the rest is still taken.
- **Metrics every 10 s** from the plugin's own upkeep thread, built from the queue's snapshot: `queued`, `retrying`, `in_flight`, `retries`, `latency_ms`, `last_ok`, `last_error`, `last_error_text`, one `Endpoint` (the engine's host, state Up / Degraded / Down), and `extra`: `transcribed` (rows written this run), `archive_queued`, `model`.
- **`start_ms` of a call** is `record.start_time_ms` when it is above 0, else `record.start_time × 1000`; `length_ms` likewise from `call_length_ms` else `call_length × 1000`.

## Review Focus

Input classes the spec implies but no task's tests would otherwise exercise, most likely to bite first:

1. A WAV that is not 8 kHz 16-bit mono (a conventional system recorded differently, a hand-copied file): it must still transcribe. Pinned in Task 2 (`reads_a_16khz_stereo_wav_and_downmixes`).
2. A search string with FTS5 syntax in it (`"`, `AND`, `*`, a lone `(`): the route must answer 200 with hits or none, never 500. Pinned in Task 6 (`search_finds_text_with_a_snippet_and_survives_fts_syntax`) and Task 10 (`search_hits_carry_a_snippet_and_survive_fts_syntax`).
3. A `Last-Event-ID` that is not a number: treated as absent, the stream starts live. Pinned in Task 10 (`events_replay_then_live_and_a_bad_last_event_id_starts_live`).
4. A call record without `start_time_ms` (older Trunk Recorder files have only `start_time`): the row's `start_ms` is seconds × 1000, not 0. Pinned in Task 8 (`a_record_without_ms_fields_gets_seconds_times_1000`).
5. `paths=` with empty entries, duplicates, or more than 200 paths, and `limit` outside 1–500: trimmed, deduplicated, truncated to 200, clamped. Pinned in Task 10 (`by_paths_in_order_trimmed_deduplicated_and_capped`, `a_page_and_limit_is_clamped`).

---

## File structure

```
Cargo.toml                 crate `transcribe`, deps above, release profile strip+lto
rustfmt.toml, .gitignore, LICENSE (MIT), README.md, CHANGELOG.md
.github/workflows/ci.yml, release.yml, .github/check_manifest.py   (the template's, verbatim)
src/main.rs        trunk_recorder_plugin::run::<plugin::Transcribe>()
src/lib.rs         pub mod {config, audio, gate, filter, prompt, segment, engine, store, queue, work, archive, http, plugin}
src/config.rs      Config, SystemConfig, their defaults and schema extensions; validation
src/audio.rs       WAV in, RMS, resample to 16 kHz, P25 filter, WAV out, the test tone
src/gate.rs        the pre-network gate
src/filter.rs      the post-engine text filters and the default hallucination list
src/prompt.rs      the prompt string
src/segment.rs     words → segments at srcList boundaries
src/engine.rs      the OpenAI-compatible client: transcribe, models, error classes
src/store.rs       transcripts.sqlite: schema, open/recover, rows, queries, FTS, retention; the Transcript JSON type
src/queue.rs       the priority work queue and its workers
src/work.rs        one call through the pipeline; Ctx (settings, systems, store, engine, announcer)
src/archive.rs     the archive pass and re-transcribe pass over calls.query pages
src/http/mod.rs    router, AppState, start/stop on a thread
src/http/cors.rs   the allowlist middleware
src/http/transcripts.rs  /api/v1/transcripts (list, page) and /api/v1/transcripts/{*path}
src/http/search.rs       /api/v1/search
src/http/events.rs       /api/v1/events (SSE)
src/http/health.rs       /api/v1/health
src/plugin.rs      Transcribe: Plugin — start, events, action, settings, shutdown, upkeep thread
```

Tests live in each module (`#[cfg(test)] mod tests`), as the template and trunk-reporter do.

---

### Task 1: Repository scaffold, settings and manifest

**Files:**
- Create: `Cargo.toml`, `rustfmt.toml`, `.gitignore`, `LICENSE`, `README.md`, `CHANGELOG.md`, `.github/workflows/ci.yml`, `.github/workflows/release.yml`, `.github/check_manifest.py`, `src/main.rs`, `src/lib.rs`, `src/config.rs`, `src/plugin.rs` (a stub that only starts)
- Test: `src/config.rs`, `src/plugin.rs`

**Interfaces:**
- Produces: `config::Config` (`#[serde(rename_all = "camelCase", default)]`, `Clone + Debug + Serialize + Deserialize + JsonSchema`) with nested structs `EngineConfig { url: String, model: String, api_key: Option<String>, language: String, timeout_seconds: u64, concurrency: usize }`, `GateConfig { rms: f32, min_seconds: f64, skip_encrypted: bool }`, `PrepareConfig { p25_filter: bool }`, `PromptConfig { enabled: bool, extra: String }`, `FilterConfig { max_words_per_second: f64, max_repeat: usize, hallucinations: Vec<String> }`, `QueueConfig { max: usize }`, `ArchiveConfig { days: u32, every_minutes: u32, retranscribe_everything: bool }`, `StoreConfig { retention_days: u32 }`, `HttpConfig { port: Option<u16>, bind: String, origins: Vec<String> }`; fields `engine, gate, prepare, prompt, filter, queue, archive, store, http`.
- Produces: `config::SystemConfig { enabled: bool }` (default true).
- Produces: `Config::validate(&self) -> Result<(), String>`: `engine.url` non-empty and starting with `http://` or `https://`; `engine.concurrency` 1–8; `engine.timeoutSeconds` 5–600; `queue.max` ≥ 10; `archive.days` ≥ 1; the message names the field as the form shows it (`Engine URL`, …).
- Produces: `config::ACTION_TEST = "test"`, `config::ACTION_RETRANSCRIBE = "retranscribe"` and the `x-actions` extension on `Config`.
- Produces: `plugin::Transcribe` with `manifest()` = `Manifest { name: "Transcribe", subscribe: [topic::CALL_CONCLUDED], audio_formats: [], live_settings: true, ..manifest!() }`.

- [ ] **Step 1: Write the scaffold**

`Cargo.toml` as the Global Constraints say, plus `[dev-dependencies] http-body-util = "0.1"`, `tower = "0.5"` (route tests), `tokio = { features = ["io-util"] }` (SSE socket tests). `.gitignore`, `rustfmt.toml`, `ci.yml`, `release.yml`, `check_manifest.py` copied from `https://github.com/TrunkRecorder/trunk-plugin-template` (fetch with `gh api repos/TrunkRecorder/trunk-plugin-template/contents/<path> --jq .content | base64 -d`). `README.md`: title, one paragraph, "Settings" and "HTTP API" headings left as "see the design document" for now. `CHANGELOG.md`: `## [0.1.0]` with one line.

- [ ] **Step 2: Write the failing tests** in `src/config.rs` and `src/plugin.rs`

```rust
#[test]
fn defaults_are_the_specs() {
    let c = Config::default();
    assert_eq!((c.engine.model.as_str(), c.engine.language.as_str(), c.engine.timeout_seconds, c.engine.concurrency), ("default", "en", 60, 2));
    assert_eq!((c.gate.rms, c.gate.min_seconds, c.gate.skip_encrypted), (0.01, 1.0, true));
    assert!(!c.prepare.p25_filter && c.prompt.enabled);
    assert_eq!((c.filter.max_words_per_second, c.filter.max_repeat), (8.0, 4));
    assert!(c.filter.hallucinations.contains(&"Thank you for watching".to_string()));
    assert_eq!((c.queue.max, c.archive.days, c.archive.every_minutes, c.store.retention_days), (2000, 7, 10, 0));
    assert_eq!((c.http.port, c.http.bind.as_str()), (None, "127.0.0.1"));
}

#[test]
fn an_empty_url_is_refused_by_name() {
    let err = Config::default().validate().unwrap_err();
    assert!(err.contains("Engine URL"), "{err}");
    let mut c = Config::default();
    c.engine.url = "http://e:8766".into();
    assert!(c.validate().is_ok());
    c.engine.concurrency = 9;
    assert!(c.validate().unwrap_err().contains("Concurrency"));
}

#[test]
fn the_schema_has_the_buttons_and_the_secret() {
    let s = trunk_recorder_plugin::schema::schema_for::<Config>();
    let ids: Vec<&str> = s["x-actions"].as_array().unwrap().iter().map(|a| a["id"].as_str().unwrap()).collect();
    assert_eq!(ids, ["test", "retranscribe"]);
    assert_eq!(s["properties"]["engine"]["properties"]["apiKey"]["x-secret"], true);
    assert_eq!(s["properties"]["engine"]["required"], serde_json::json!(["url"]));
}
```

```rust
// src/plugin.rs
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
```

- [ ] **Step 3: Run the tests to see them fail**

Run: `cargo test`
Expected: compile errors for the missing types.

- [ ] **Step 4: Implement `config.rs` and the stub plugin**

Doc comments on every field in the form's style (first line the label, a blank line, then help), e.g. `/// Engine URL\n///\n/// Where the engine answers, e.g. http://eddie:8766. The plugin posts to /v1/audio/transcriptions under it.` `url` carries `#[schemars(extend("x-required" = true))]`, `api_key` `#[schemars(extend("x-secret" = true))]`. `Config` carries `#[schemars(extend("x-actions" = [{"id": "test", "label": "Test the engine", "hint": "Posts a one-second sample and reports the model and the latency"}, {"id": "retranscribe", "label": "Re-transcribe with the current model", "hint": "Queues calls whose transcript came from another model"}]))]`. `impl Default for Config` writes the spec's defaults by hand (the derive would give zeros). The default hallucination list, exactly: `Thank you for watching`, `Thanks for watching`, `Thank you for listening`, `Thank you`, `Thanks`, `Please subscribe`, `Subtitles by the Amara.org community`, `Subscribe to my channel`, `Bye`, `Goodbye`, `you`, `The end`, `Music`, `Applause`.

`plugin.rs` for now: `struct Transcribe { host: Host }`, `start` validates `setup.config` and returns `Ok`, nothing else.

- [ ] **Step 5: Run the tests, clippy and fmt**

Run: `cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check && cargo run --quiet -- --describe > manifest.json && python .github/check_manifest.py manifest.json`
Expected: all pass; the manifest check prints no error.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "Scaffold: the transcribe plugin's settings, manifest and release workflow"
```

---

### Task 2: Audio — WAV in, RMS, resampling, the P25 filter, WAV out

**Files:**
- Create: `src/audio.rs`
- Test: `src/audio.rs`

**Interfaces:**
- Produces: `pub struct Pcm { pub rate: u32, pub samples: Vec<f32> }` (mono, −1.0..1.0).
- Produces: `pub fn read_wav(path: &Path) -> Result<Pcm, String>` (any rate, 8/16/24/32-bit int or float, channels averaged to mono; the error names the file).
- Produces: `pub fn rms(samples: &[f32]) -> f32`.
- Produces: `pub fn resample_to_16k(pcm: &Pcm) -> Vec<f32>` (returns `samples` cloned when `rate == 16000`).
- Produces: `pub fn p25_filter(samples: &mut [f32])` (at 16 kHz; the biquad chain and −3 dBFS peak normalisation from the plan's decisions).
- Produces: `pub fn wav_16k(samples: &[f32]) -> Vec<u8>` (RIFF/WAVE, 16-bit PCM, mono, 16 000 Hz, 44-byte header).
- Produces: `pub fn test_tone() -> Vec<u8>` (one second, 440 Hz, −20 dBFS, through `wav_16k`).

- [ ] **Step 1: Write the failing tests**

```rust
fn sine(rate: u32, hz: f32, secs: f32, amp: f32) -> Vec<f32> { (0..(rate as f32 * secs) as usize).map(|n| amp * (2.0 * PI * hz * n as f32 / rate as f32).sin()).collect() }

#[test]
fn reads_an_8khz_wav_and_measures_it() {
    let dir = testing::temp_dir("audio");
    let call = testing::call(&dir, "sys1", 1);          // 3 s of silence at 8 kHz
    let pcm = read_wav(&call.files.wav).unwrap();
    assert_eq!((pcm.rate, pcm.samples.len()), (8000, 24000));
    assert_eq!(rms(&pcm.samples), 0.0);
    assert!((rms(&sine(8000, 1000.0, 1.0, 0.5)) - 0.3536).abs() < 0.01);
}

#[test]
fn reads_a_16khz_stereo_wav_and_downmixes() {
    // Written with hound: 16 kHz, 2 channels, left = 0.5 tone, right = silence.
    // Expect rate 16000, samples.len() == 16000, rms ≈ 0.177 (half of 0.3536).
}

#[test]
fn resampling_a_tone_keeps_its_frequency() {
    let out = resample_to_16k(&Pcm { rate: 8000, samples: sine(8000, 1000.0, 1.0, 0.5) });
    assert_eq!(out.len(), 16000);
    let ideal = sine(16000, 1000.0, 1.0, 0.5);
    let err = out[800..15200].iter().zip(&ideal[800..15200]).map(|(a, b)| (a - b).powi(2)).sum::<f32>() / 14400.0;
    assert!(err.sqrt() < 0.02, "rms error {}", err.sqrt());
}

#[test]
fn the_p25_filter_notches_1200_and_keeps_600() {
    let mut keep = sine(16000, 600.0, 1.0, 0.5);
    let mut notch = sine(16000, 1200.0, 1.0, 0.5);
    p25_filter(&mut keep); p25_filter(&mut notch);
    assert!(rms(&keep[4000..]) > 0.2, "600 Hz kept");
    assert!(rms(&notch[4000..]) < 0.05, "1200 Hz notched");
    assert!(keep.iter().map(|s| s.abs()).fold(0.0, f32::max) <= 0.71);   // −3 dBFS
}

#[test]
fn wav_out_reads_back() {
    let bytes = wav_16k(&sine(16000, 440.0, 0.5, 0.1));
    assert_eq!(&bytes[..4], b"RIFF"); assert_eq!(bytes.len(), 44 + 8000 * 2);
    let p = read_wav(&write_temp(&bytes)).unwrap();
    assert_eq!((p.rate, p.samples.len()), (16000, 8000));
    assert_eq!(test_tone().len(), 44 + 16000 * 2);
}
```

- [ ] **Step 2: Run them to see them fail** — `cargo test audio` → unresolved module.

- [ ] **Step 3: Implement `audio.rs`** with `hound` for reading (match `spec.sample_format` and `bits_per_sample` to scale to f32), the resampler and the filter as the decisions section specifies. Biquad coefficients from the Audio EQ Cookbook (high-pass, low-pass, notch); apply each twice-cascaded Butterworth as one biquad with Q = 0.7071.

- [ ] **Step 4: Run the tests** — `cargo test audio` → pass.

- [ ] **Step 5: Commit** — `git commit -am "Audio: WAV in and out, RMS, 16 kHz resampling, the P25 filter chain"`

---

### Task 3: The gate and the text filters

**Files:**
- Create: `src/gate.rs`, `src/filter.rs`
- Test: both

**Interfaces:**
- Consumes: `audio::Pcm`, `audio::rms`, `config::GateConfig`, `config::FilterConfig`.
- Produces: `gate::check(record: &CallRecord, pcm: Option<&Pcm>, cfg: &GateConfig) -> Option<&'static str>`: `Some("encrypted")` when `record.encrypted && cfg.skip_encrypted`; `Some("too short")` when `record.call_length < cfg.min_seconds`; `Some("silent")` when `pcm` is given and `rms < cfg.rms`; else `None`. Order: encrypted, too short, silent.
- Produces: `filter::Verdict { Ok, Empty, Rejected(&'static str) }` and `filter::check(text: &str, length_s: f64, cfg: &FilterConfig) -> Verdict` with reasons `"implausible rate"`, `"repetition"`, `"hallucination"`. Words are whitespace tokens; comparison is lowercase with ASCII punctuation stripped from both ends of each token; the hallucination match is the whole normalised text against each normalised list entry; the rate check is skipped when `length_s <= 0`.

- [ ] **Step 1: Write the failing tests**

```rust
// gate.rs
#[test] fn encrypted_then_short_then_silent() {
    let cfg = GateConfig::default();
    let mut r = CallRecord { call_length: 5.0, encrypted: true, ..Default::default() };
    assert_eq!(check(&r, None, &cfg), Some("encrypted"));
    r.encrypted = false; r.call_length = 0.5;
    assert_eq!(check(&r, None, &cfg), Some("too short"));
    r.call_length = 5.0;
    let quiet = Pcm { rate: 8000, samples: vec![0.002; 8000] };
    assert_eq!(check(&r, Some(&quiet), &cfg), Some("silent"));
    let loud = Pcm { rate: 8000, samples: sine(8000, 1000.0, 1.0, 0.3) };
    assert_eq!(check(&r, Some(&loud), &cfg), None);
    assert_eq!(check(&r, None, &cfg), None, "an M4A has no RMS gate");
    let keep = GateConfig { skip_encrypted: false, ..cfg };
    r.encrypted = true;
    assert_eq!(check(&r, None, &keep), None);
}

// filter.rs
#[test] fn the_four_verdicts() {
    let cfg = FilterConfig::default();
    assert_eq!(check("", 5.0, &cfg), Verdict::Empty);
    assert_eq!(check("   \n", 5.0, &cfg), Verdict::Empty);
    assert_eq!(check("Tower 73 go ahead", 4.0, &cfg), Verdict::Ok);
    let fast = "one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen";
    assert_eq!(check(fast, 2.0, &cfg), Verdict::Rejected("implausible rate"));   // 17 words in 2 s
    assert_eq!(check(fast, 0.0, &cfg), Verdict::Ok, "no length, no rate check");
    assert_eq!(check("ok ok ok ok ok done", 3.0, &cfg), Verdict::Rejected("repetition"));   // 5 in a row > 4
    assert_eq!(check("ok ok ok ok done", 3.0, &cfg), Verdict::Ok);                           // 4 is allowed
    assert_eq!(check("Thank you for watching.", 3.0, &cfg), Verdict::Rejected("hallucination"));
    assert_eq!(check("thank you for watching!", 3.0, &cfg), Verdict::Rejected("hallucination"));
    assert_eq!(check("Thank you for watching the scene", 3.0, &cfg), Verdict::Ok, "only the whole text");
}
```

- [ ] **Step 2: Run them to see them fail.**
- [ ] **Step 3: Implement both modules.**
- [ ] **Step 4: `cargo test gate filter` → pass.**
- [ ] **Step 5: Commit** — `git commit -am "The gate before the network and the filters after the engine"`

---

### Task 4: The prompt and the segments

**Files:**
- Create: `src/prompt.rs`, `src/segment.rs`
- Test: both

**Interfaces:**
- Consumes: `CallRecord` (`talkgroup_tag`, `talkgroup_description`, `src_list[].tag`, `src_list[].src`, `src_list[].pos`, `call_length`), `config::PromptConfig`.
- Produces: `prompt::build(record: &CallRecord, system_name: &str, cfg: &PromptConfig) -> String`: empty when `!cfg.enabled`; else the non-empty parts of `[talkgroup_tag, talkgroup_description, system_name, "Units: " + unique non-empty src tags joined by ", ", cfg.extra]` joined by `". "`, then cut to at most 200 characters at the last space before the limit (no partial word, no trailing space).
- Produces: `engine::Word { word: String, start: f64, end: f64 }` is defined in Task 5; this task defines it in `segment.rs` as `pub struct Word` and Task 5 re-exports it (`pub use crate::segment::Word`).
- Produces: `segment::Segment { unit: Option<i64>, start_s: f64, end_s: f64, text: String }` and `segment::split(words: &[Word], text: &str, src_list: &[SrcEntry], length_s: f64) -> Vec<Segment>`:
  - no words → one segment `{ unit: None, start_s: 0.0, end_s: length_s, text }` (empty `text` → empty vec);
  - with words: boundaries are the src entries sorted by `pos`; segment `i` spans `[pos_i, pos_{i+1})`, the last to `length_s.max(last word end)`; a word belongs to the segment containing its midpoint `(start + end) / 2`, words before the first `pos` to the first segment; a segment with no words is dropped; `unit = Some(src)`; `start_s` / `end_s` are its first word's `start` and last word's `end`; `text` its words joined by one space;
  - with words and an empty `src_list` → one segment, `unit: None`, spanning the words.

- [ ] **Step 1: Write the failing tests**

```rust
// prompt.rs
#[test] fn the_prompt_is_the_vocabulary_capped_at_200() {
    let r = CallRecord { talkgroup_tag: "Fire Dispatch".into(), talkgroup_description: "County fire main".into(),
        src_list: vec![src(101, "Engine 3"), src(102, ""), src(101, "Engine 3")], ..Default::default() };
    let cfg = PromptConfig { enabled: true, extra: "Middletown".into() };
    assert_eq!(build(&r, "OHMARCS", &cfg), "Fire Dispatch. County fire main. OHMARCS. Units: Engine 3. Middletown");
    assert_eq!(build(&r, "", &PromptConfig { enabled: false, extra: "x".into() }), "");
    let long = CallRecord { talkgroup_description: "word ".repeat(60), ..Default::default() };
    let p = build(&long, "", &cfg);
    assert!(p.len() <= 200 && !p.ends_with(' ') && p.ends_with("word"));
}

// segment.rs
fn w(t: &str, s: f64, e: f64) -> Word { Word { word: t.into(), start: s, end: e } }
fn src(id: i64, pos: f64) -> SrcEntry { SrcEntry { src: id, pos, ..Default::default() } }

#[test] fn words_fall_under_the_radio_talking() {
    let words = [w("tower", 0.2, 0.5), w("seventy", 0.6, 0.9), w("three", 1.0, 1.3), w("go", 4.1, 4.3), w("ahead", 4.4, 4.8)];
    let segs = split(&words, "tower seventy three go ahead", &[src(1001, 0.0), src(1002, 4.0)], 6.0);
    assert_eq!(segs.len(), 2);
    assert_eq!((segs[0].unit, segs[0].start_s, segs[0].end_s, segs[0].text.as_str()), (Some(1001), 0.2, 1.3, "tower seventy three"));
    assert_eq!((segs[1].unit, segs[1].text.as_str()), (Some(1002), "go ahead"));
}

#[test] fn one_radio_no_timestamps_and_no_radios() {
    let one = split(&[], "all of it", &[src(7, 0.0)], 3.0);
    assert_eq!((one[0].unit, one[0].start_s, one[0].end_s, one[0].text.as_str()), (None, 0.0, 3.0, "all of it"));
    assert!(split(&[], "", &[src(7, 0.0)], 3.0).is_empty());
    let none = split(&[w("hi", 0.1, 0.4)], "hi", &[], 2.0);
    assert_eq!((none[0].unit, none[0].text.as_str()), (None, "hi"));
    // Overlapping radios: a word's midpoint decides; a radio that said nothing has no segment.
    let segs = split(&[w("a", 0.0, 1.0), w("b", 1.2, 1.4)], "a b", &[src(1, 0.0), src(2, 0.9), src(3, 1.1)], 2.0);
    assert_eq!(segs.iter().map(|s| s.unit).collect::<Vec<_>>(), [Some(1), Some(3)]);
}
```

- [ ] **Step 2: Run them to see them fail.**
- [ ] **Step 3: Implement both modules.**
- [ ] **Step 4: `cargo test prompt segment` → pass.**
- [ ] **Step 5: Commit** — `git commit -am "The prompt from the call's vocabulary, and segments at the radios' boundaries"`

---

### Task 5: The engine client

**Files:**
- Create: `src/engine.rs`
- Test: `src/engine.rs` (against `testing::MockServer`)

**Interfaces:**
- Consumes: `trunk_recorder_plugin::Multipart`, `config::EngineConfig`, `segment::Word`.
- Produces: `pub struct Engine { client: reqwest::blocking::Client }` with `Engine::new(timeout: Duration) -> Engine`.
- Produces: `pub struct Audio<'a> { pub bytes: &'a [u8], pub file_name: &'a str, pub content_type: &'a str }` (`"call.wav"`/`"audio/wav"` or `"call.m4a"`/`"audio/mp4"`).
- Produces: `pub struct Answer { pub text: String, pub language: Option<String>, pub words: Vec<Word>, pub model: Option<String>, pub raw: String }`.
- Produces: `pub enum EngineError { Retry(String), Fail(String) }` — `Retry` for connection errors, timeouts, 5xx and 429; `Fail` for any other 4xx (message: `"HTTP <code>: <first 200 chars of body>"`) and for an unreadable 200 body (`"unreadable answer: <serde error>"`).
- Produces: `Engine::transcribe(&self, cfg: &EngineConfig, audio: Audio, language: &str, prompt: &str) -> Result<Answer, EngineError>`: `POST {url}/v1/audio/transcriptions` (one `/` between, whatever `url` ends with), multipart fields in this order: `file`, `model` (`cfg.model`), `language`, `response_format` = `verbose_json`, `timestamp_granularities[]` = `word`, `prompt` only when non-empty; header `Authorization: Bearer <key>` only when `api_key` is set and non-empty.
- Produces: `Engine::models(&self, cfg: &EngineConfig) -> Result<Option<String>, EngineError>`: `GET {url}/v1/models`, the first `data[].id`; `Ok(None)` on 404 or an answer without `data`.
- Produces: `pub fn host_of(url: &str) -> String` (`"eddie:8766"` from `"http://eddie:8766/"`; the whole string when it does not parse) for the endpoint name and the `engine` column.

- [ ] **Step 1: Write the failing tests**

```rust
fn cfg(url: &str) -> EngineConfig { EngineConfig { url: url.into(), model: "default".into(), api_key: Some("k1".into()), ..Default::default() } }
const OK: &str = r#"{"task":"transcribe","language":"en","text":"Tower 73 go ahead","model":"Qwen3-ASR-1.7B","words":[{"word":"Tower","start":0.1,"end":0.4},{"word":"73","start":0.5,"end":0.9}]}"#;

#[test] fn posts_the_openai_form_and_reads_the_answer() {
    let s = MockServer::start(|_| (200, OK.into()));
    let a = Engine::new(Duration::from_secs(5)).transcribe(&cfg(&format!("{}/", s.url())), Audio { bytes: b"RIFF", file_name: "call.wav", content_type: "audio/wav" }, "en", "Fire Dispatch").unwrap();
    assert_eq!((a.text.as_str(), a.language.as_deref(), a.model.as_deref(), a.words.len()), ("Tower 73 go ahead", Some("en"), Some("Qwen3-ASR-1.7B"), 2));
    assert_eq!(a.words[1], Word { word: "73".into(), start: 0.5, end: 0.9 });
    let r = &s.requests()[0];
    assert_eq!((r.method.as_str(), r.path.as_str()), ("POST", "/v1/audio/transcriptions"));
    assert_eq!(r.header("authorization"), Some("Bearer k1"));
    assert_eq!(r.form_field("file").unwrap(), b"RIFF"); assert_eq!(r.form_file_name("file").unwrap(), "call.wav");
    for (k, v) in [("model", "default"), ("language", "en"), ("response_format", "verbose_json"), ("timestamp_granularities[]", "word"), ("prompt", "Fire Dispatch")] {
        assert_eq!(r.form_field(k).unwrap(), v.as_bytes(), "{k}");
    }
}

#[test] fn no_key_no_header_no_prompt_no_field() { /* api_key None, prompt "" → header absent, form_field("prompt") None */ }

#[test] fn errors_are_classed_for_retry_or_not() {
    for (code, retry) in [(500, true), (503, true), (429, true), (400, false), (401, false), (413, false)] {
        let s = MockServer::start(move |_| (code, "nope".into()));
        let e = Engine::new(Duration::from_secs(5)).transcribe(&cfg(s.url()), wav(), "en", "").unwrap_err();
        assert_eq!(matches!(e, EngineError::Retry(_)), retry, "{code}");
        if !retry { assert!(matches!(&e, EngineError::Fail(m) if m.starts_with(&format!("HTTP {code}")))); }
    }
    let s = MockServer::start(|_| (200, "not json".into()));
    assert!(matches!(Engine::new(Duration::from_secs(5)).transcribe(&cfg(s.url()), wav(), "en", "").unwrap_err(), EngineError::Fail(m) if m.starts_with("unreadable answer")));
    assert!(matches!(Engine::new(Duration::from_secs(5)).transcribe(&cfg("http://127.0.0.1:1"), wav(), "en", "").unwrap_err(), EngineError::Retry(_)));
}

#[test] fn a_hang_past_the_timeout_is_a_retry() {
    let s = MockServer::start(|_| { std::thread::sleep(Duration::from_millis(800)); (200, OK.into()) });
    let t0 = Instant::now();
    assert!(matches!(Engine::new(Duration::from_millis(200)).transcribe(&cfg(s.url()), wav(), "en", "").unwrap_err(), EngineError::Retry(_)));
    assert!(t0.elapsed() < Duration::from_millis(700));
}

#[test] fn models_names_the_first_or_none() {
    let s = MockServer::start(|r| if r.path == "/v1/models" { (200, r#"{"object":"list","data":[{"id":"Qwen3-ASR-1.7B"},{"id":"other"}]}"#.into()) } else { (404, "".into()) });
    assert_eq!(Engine::new(Duration::from_secs(5)).models(&cfg(s.url())).unwrap(), Some("Qwen3-ASR-1.7B".into()));
    let s = MockServer::start(|_| (404, "".into()));
    assert_eq!(Engine::new(Duration::from_secs(5)).models(&cfg(s.url())).unwrap(), None);
    assert_eq!(host_of("http://eddie:8766/"), "eddie:8766");
}
```

- [ ] **Step 2: Run them to see them fail.**
- [ ] **Step 3: Implement `engine.rs`** — `reqwest::blocking::Client::builder().timeout(timeout).build()`; the body from `Multipart`; `Answer` parsed with a private `#[derive(Deserialize)]` struct where every field but `text` is `#[serde(default)]`; `raw` is the body as received.
- [ ] **Step 4: `cargo test engine` → pass.**
- [ ] **Step 5: Commit** — `git commit -am "The engine client: the OpenAI transcription route, error classes, the model list"`

---

### Task 6: The store

**Files:**
- Create: `src/store.rs`
- Test: `src/store.rs`

**Interfaces:**
- Produces: `pub struct Store(Arc<Mutex<rusqlite::Connection>>)` (`Clone`), `Store::open(path: &Path, warn: &dyn Fn(String)) -> Result<Store, String>`: opens, `journal_mode = WAL`, `synchronous = NORMAL`, `foreign_keys = ON`, `busy_timeout` 5 s; runs `PRAGMA quick_check` first and, when it is not `ok` or the open fails, renames `<path>`, `<path>-wal` and `<path>-shm` to `<path>.corrupt-<unix seconds>` and starts afresh, calling `warn` with what it did; creates the schema when `user_version` is 0 and sets it to 1; refuses a `user_version` above 1 (`Err`). `Store::in_memory()` for tests.
- Produces: `pub const SCHEMA: &str` — the spec's two tables and two indexes, plus `CREATE VIRTUAL TABLE transcripts_fts USING fts5(text, content='transcripts', content_rowid='rowid')` and the three external-content triggers (after insert / after delete / after update of text on `transcripts`).
- Produces: `pub struct Row { pub path: String, pub short_name: String, pub talkgroup: u32, pub start_ms: i64, pub length_ms: i64, pub status: Status, pub reason: Option<String>, pub text: Option<String>, pub language: Option<String>, pub engine: String, pub model_asked: String, pub model_reported: Option<String>, pub prompt: Option<String>, pub took_ms: Option<u64>, pub attempts: u32, pub transcribed_ms: i64, pub raw: Option<String> }` and `pub enum Status { Ok, Skipped, Empty, Rejected, Failed }` with `as_str()` (`"ok"`, …) and `parse`.
- Produces: `pub struct SegmentRow { pub seq: u32, pub unit: Option<i64>, pub start_s: f64, pub end_s: f64, pub text: String }`.
- Produces: `Store::put(&self, row: &Row, segments: &[SegmentRow]) -> Result<(), String>` (delete then insert, one transaction).
- Produces: `Store::get(&self, path: &str) -> Result<Option<(Row, Vec<SegmentRow>)>, String>`; `Store::get_many(&self, paths: &[String]) -> Result<Vec<(Row, Vec<SegmentRow>)>, String>` (in the order given, absent ones left out).
- Produces: `pub struct Query { pub system: Option<String>, pub talkgroup: Option<u32>, pub from: Option<i64>, pub to: Option<i64>, pub before: Option<i64>, pub limit: usize }` (`Default` with `limit: 100`) and `pub struct Page { pub rows: Vec<(Row, Vec<SegmentRow>)>, pub more: bool, pub before: i64 }`; `Store::page(&self, q: &Query) -> Result<Page, String>` (newest first by `start_ms`; `before` = the oldest row's `start_ms` on the page; `limit` already clamped by the caller).
- Produces: `Store::search(&self, text: &str, q: &Query) -> Result<(Vec<(Row, Vec<SegmentRow>, String)>, bool, i64), String>` (the snippet is `snippet(transcripts_fts, 0, '<b>', '</b>', '…', 12)`; only `status = 'ok'` rows match, since only they have text).
- Produces: `Store::settled(&self, paths: &[String]) -> Result<HashMap<String, (Status, u32)>, String>` (status and attempts for the paths that have rows).
- Produces: `Store::retranscribe_candidates(&self, model: &str, since_ms: Option<i64>) -> Result<Vec<String>, String>` (paths with status in ok/empty/rejected whose `model_reported` is null or differs from `model`, oldest first; `since_ms` filters `start_ms >= since_ms`).
- Produces: `Store::since(&self, transcribed_ms: i64, limit: usize) -> Result<Vec<(Row, Vec<SegmentRow>)>, String>` (rows with `transcribed_ms > ?`, ascending, for SSE replay).
- Produces: `Store::retain(&self, days: u32, now_ms: i64) -> Result<usize, String>` (0 days → nothing; deletes in batches of 500 by `start_ms`, returns the count); `Store::transcribed_since(&self, ms: i64) -> Result<u64, String>` (count of `status = 'ok'` rows with `transcribed_ms >= ms`).
- Produces: `pub struct Transcript` (the spec's JSON shape; `#[serde(rename_all = "camelCase")]`, `segments: Vec<TranscriptSegment { unit: Option<i64>, start: f64, end: f64, text: String }>`) and `Transcript::from_row(row: &Row, segments: &[SegmentRow]) -> Transcript` (`model` = `model_reported` else `model_asked`; `system` = `short_name`).
- Produces: `pub fn fts_query(q: &str) -> String` (the bag-of-quoted-words form; `""` for an input with no tokens).

- [ ] **Step 1: Write the failing tests**

```rust
fn row(path: &str, start_ms: i64, status: Status, text: Option<&str>) -> Row { Row { path: path.into(), short_name: "sys1".into(), talkgroup: 101, start_ms, length_ms: 4000, status, text: text.map(String::from), engine: "e".into(), model_asked: "default".into(), model_reported: Some("m1".into()), attempts: 1, transcribed_ms: start_ms + 10, ..Default::default() } }

#[test] fn put_get_and_replace_keep_segments_in_step() {
    let s = Store::in_memory();
    s.put(&row("a", 1000, Status::Ok, Some("tower seventy three")), &[SegmentRow { seq: 0, unit: Some(1001), start_s: 0.2, end_s: 1.3, text: "tower seventy three".into() }]).unwrap();
    let (r, segs) = s.get("a").unwrap().unwrap();
    assert_eq!((r.status, segs.len(), segs[0].unit), (Status::Ok, 1, Some(1001)));
    s.put(&row("a", 1000, Status::Empty, None), &[]).unwrap();
    let (r, segs) = s.get("a").unwrap().unwrap();
    assert_eq!((r.status, segs.len()), (Status::Empty, 0));
    assert!(s.get("b").unwrap().is_none());
    let t = Transcript::from_row(&row("a", 1000, Status::Ok, Some("x")), &[]);
    assert_eq!(serde_json::to_value(&t).unwrap(), json!({ "path": "a", "system": "sys1", "talkgroup": 101, "startMs": 1000, "lengthMs": 4000, "status": "ok", "reason": null, "text": "x", "language": null, "model": "m1", "transcribedMs": 1010, "segments": [] }));
}

#[test] fn pages_newest_first_with_a_cursor() {
    let s = Store::in_memory();
    for i in 0..7 { s.put(&row(&format!("p{i}"), 1000 * i, Status::Ok, Some("t")), &[]).unwrap(); }
    let p = s.page(&Query { limit: 3, ..Default::default() }).unwrap();
    assert_eq!((p.rows.iter().map(|(r, _)| r.path.as_str()).collect::<Vec<_>>(), p.more, p.before), (vec!["p6", "p5", "p4"], true, 4000));
    let p = s.page(&Query { limit: 3, before: Some(4000), ..Default::default() }).unwrap();
    assert_eq!(p.rows[0].0.path, "p3");
    let p = s.page(&Query { limit: 10, from: Some(2000), to: Some(5000), ..Default::default() }).unwrap();
    assert_eq!((p.rows.len(), p.more), (3, false));   // 2000, 3000, 4000
    let p = s.page(&Query { limit: 10, talkgroup: Some(5), ..Default::default() }).unwrap();
    assert!(p.rows.is_empty());
}

#[test] fn search_finds_text_with_a_snippet_and_survives_fts_syntax() {
    let s = Store::in_memory();
    s.put(&row("a", 1000, Status::Ok, Some("Engine 3 responding to the structure fire")), &[]).unwrap();
    s.put(&row("b", 2000, Status::Rejected, Some("structure structure structure")), &[]).unwrap();
    let (hits, more, _) = s.search("structure", &Query { limit: 10, ..Default::default() }).unwrap();
    assert_eq!((hits.len(), more), (1, false));
    assert!(hits[0].2.contains("<b>structure</b>"), "{}", hits[0].2);
    for q in ["\"", "AND", "fire*", "(", "a \"b", "NOT fire"] {
        assert!(s.search(q, &Query { limit: 10, ..Default::default() }).is_ok(), "{q}");
    }
    assert_eq!(fts_query("a \"b\" c"), r#""a" "\"b\"" "c""#.replace("\\\"", "\"\""));
    assert_eq!(fts_query("  "), "");
}

#[test] fn settled_candidates_since_and_retention() {
    let s = Store::in_memory();
    s.put(&row("a", 1000, Status::Ok, Some("t")), &[]).unwrap();
    s.put(&Row { model_reported: Some("m2".into()), ..row("b", 2000, Status::Empty, None) }, &[]).unwrap();
    s.put(&row("c", 3000, Status::Skipped, None), &[]).unwrap();
    s.put(&Row { attempts: 3, ..row("d", 4000, Status::Failed, None) }, &[]).unwrap();
    let got = s.settled(&["a".into(), "d".into(), "zz".into()]).unwrap();
    assert_eq!((got["a"], got["d"], got.get("zz")), ((Status::Ok, 1), (Status::Failed, 3), None));
    assert_eq!(s.retranscribe_candidates("m2", None).unwrap(), ["a"]);          // b already m2; c skipped; d failed
    assert_eq!(s.retranscribe_candidates("m3", Some(1500)).unwrap(), ["b"]);
    assert_eq!(s.since(1010, 10).unwrap().iter().map(|(r, _)| r.path.as_str()).collect::<Vec<_>>(), ["b", "c", "d"]);
    assert_eq!(s.transcribed_since(0).unwrap(), 1);
    assert_eq!(s.retain(0, 1_000_000).unwrap(), 0);
    assert_eq!(s.retain(1, 2500 + 86_400_000).unwrap(), 2);                      // a and b are older than a day
    assert!(s.get("a").unwrap().is_none() && s.get("c").unwrap().is_some());
}

#[test] fn a_corrupt_file_is_moved_aside_and_a_newer_version_refused() {
    let dir = testing::temp_dir("store");
    let p = dir.join("transcripts.sqlite");
    std::fs::write(&p, b"this is not a database").unwrap();
    let warned = std::cell::RefCell::new(Vec::new());
    let s = Store::open(&p, &|m| warned.borrow_mut().push(m)).unwrap();
    assert!(warned.borrow()[0].contains("corrupt"), "{:?}", warned.borrow());
    assert!(std::fs::read_dir(&dir).unwrap().flatten().any(|e| e.file_name().to_string_lossy().contains(".corrupt-")));
    s.put(&row("a", 1, Status::Ok, Some("t")), &[]).unwrap();
    drop(s);
    let s = Store::open(&p, &|_| {}).unwrap();
    assert!(s.get("a").unwrap().is_some(), "reopening keeps data");
    s.0.lock().unwrap().pragma_update(None, "user_version", 2).unwrap();
    drop(s);
    assert!(Store::open(&p, &|_| {}).unwrap_err().contains("newer"));
}
```

- [ ] **Step 2: Run them to see them fail.**
- [ ] **Step 3: Implement `store.rs`.** `Row` derives `Default` (with `Status::Failed` as the default status is wrong; derive `Default` on `Status` with `#[default] Skipped`, tests set it anyway). `page` uses `LIMIT limit + 1` to learn `more`. `search` joins `transcripts_fts` to `transcripts` on `rowid`, with the same `WHERE` filters as `page` plus `transcripts_fts MATCH ?`, `ORDER BY start_ms DESC`.
- [ ] **Step 4: `cargo test store` → pass.**
- [ ] **Step 5: Commit** — `git commit -am "The transcript store: schema, recovery, pages, full-text search, retention"`

---

### Task 7: The work queue

**Files:**
- Create: `src/queue.rs`
- Test: `src/queue.rs`

**Interfaces:**
- Produces: `pub enum Kind { Live, Archive, Retranscribe }`, `pub struct Job { pub call: ConcludedCall, pub kind: Kind, pub enqueued: Instant, pub tries: u32 }`.
- Produces: `pub enum Done { Ok { took: Duration }, Skipped(String), Failed(String), Retry(String) }`.
- Produces: `pub struct QueueOptions { pub workers: usize, pub max: usize, pub retry_after: Vec<Duration> /* 2 s, 8 s, 30 s */, pub max_age: Duration /* 50 min */ }`.
- Produces: `pub struct Queue` with `Queue::start(host: Host, opts: QueueOptions, work: impl Fn(&Job) -> Done + Send + Sync + 'static, failed: impl Fn(&Job, &str) + Send + Sync + 'static) -> Queue`:
  - `push(&self, call: ConcludedCall, kind: Kind) -> bool`: false (and nothing happens) when a job with that `path` is queued or in flight; when the queue holds `max` jobs, the oldest archive job is dropped, else the oldest live job, with `failed(&job, "queue full")` (and `call.result: failed, "queue full"` when it was live) and a `warn` log at most once a minute; after `closing`, a pushed job is answered at once with `failed, "shutting down"`.
  - workers take live jobs first (oldest first), then retries that are due, then archive jobs only if no other worker is on one (`archive_busy`); a job older than `max_age` when taken is failed with `"engine down"` without calling `work`.
  - `Done::Retry` with `tries < retry_after.len()` waits `retry_after[tries]` and logs `warn` "retry <n>/3 for <path>: <message>"; past that it is failed with `"gave up: <message>"`. `Done::Failed(m)` calls `failed(job, m)`. For `Kind::Live` jobs only, every final outcome sends `host.call_result[_took]` with the spec's outcomes: `Ok` with `took`, `Skipped(m)`, `Failed(m)`.
  - "`max` jobs" counts jobs waiting (ready or due for retry), not those in flight.
  - `snapshot(&self) -> Snapshot { queued, live, archive, retrying, in_flight, retries, ok, failed, latency_ms: Option<f64> (EWMA 0.2 of Ok took), last_ok_s, last_error_s, last_error: String, in_a_row: u32, down_since: Option<Instant> }` — `in_a_row` counts Retry/Failed from the engine in a row (reset on Ok; Skipped does not touch it); `down_since` is set when `in_a_row` reaches 3 and cleared on Ok.
  - `shutdown(&mut self, grace: Duration)`: marks closing, answers every queued job with `failed, "shutting down"`, waits for in-flight workers up to `0.8 × grace`, returns.
  - `len(&self) -> usize` (queued + in flight).

- [ ] **Step 1: Write the failing tests** (each with `testing::capture()`, a 1-worker queue, `retry_after` of 20 ms, and a `work` closure keyed on `call.call.talkgroup`)

```rust
#[test] fn live_goes_before_archive_and_archive_takes_one_worker() {
    // 2 workers; work sleeps 100 ms and records the order of (kind, tg) it started; push archive 1, archive 2, live 3.
    // Expect: live 3 starts before archive 2; archive 1 and archive 2 never overlap (their start times differ by ≥ 100 ms).
}
#[test] fn results_go_back_for_live_only() {
    // work: tg 1 → Ok, tg 2 → Skipped("silent"), tg 3 → Failed("HTTP 400"); push each as Live, then tg 4 as Archive → Ok.
    // out.results() == [(p1, Ok, "", ""), (p2, Skipped, "silent", ""), (p3, Failed, "HTTP 400", "")]; failed() was called once, with (p3, "HTTP 400"); the Ok result has took_ms.
}
#[test] fn retries_then_gives_up() {
    // work counts calls and always returns Retry("down"): expect 4 calls (1 + 3 retries), result Failed with message starting "gave up: down", failed() called once, snapshot.retries == 3, in_a_row == 4, down_since.is_some().
}
#[test] fn the_queue_full_rule_drops_the_oldest_archive_first() {
    // max 3, work blocks on a channel so nothing completes; push archive a1, live l1, live l2, then live l3 → a1 is failed "queue full" (failed() called, no call.result since it was archive); push live l4 → l1 failed "queue full" with call.result.
}
#[test] fn a_duplicate_path_is_queued_once() { /* push the same call twice → second push returns false; work runs once */ }
#[test] fn an_old_job_is_failed_without_work() { /* max_age 10 ms, work blocks the single worker on job 1 for 50 ms; job 2 pushed at once is later failed "engine down" and work saw only job 1 */ }
#[test] fn shutdown_fails_what_waits() { /* block the worker; push 2 more; shutdown(100 ms) → the two get Failed "shutting down"; shutdown returns within 150 ms */ }
```

- [ ] **Step 2: Run them to see them fail.**
- [ ] **Step 3: Implement `queue.rs`** with `Mutex<Inner> + Condvar`, `workers` threads named `transcribe-<i>`, the stats in `Inner`. The `failed` callback runs on the worker thread before the `call.result` is sent.
- [ ] **Step 4: `cargo test queue` → pass (run three times: they must not flake).**
- [ ] **Step 5: Commit** — `git commit -am "The work queue: live first, archive on one worker, retries, the full and age rules, the shutdown drain"`

---

### Task 8: The pipeline — one call from audio to row

**Files:**
- Create: `src/work.rs`
- Test: `src/work.rs` (MockServer + in-memory store)

**Interfaces:**
- Consumes: everything from Tasks 2–7.
- Produces: `pub struct SystemView { pub short_name: String, pub name: String, pub enabled: bool, pub settings: Config }` and `pub fn systems_of(setup: &Setup<Config, SystemConfig>) -> Vec<SystemView>`.
- Produces: `pub struct Ctx { pub config: Arc<RwLock<Config>>, pub systems: Arc<RwLock<Vec<SystemView>>>, pub store: Store, pub engine: Engine, pub reported_model: Arc<RwLock<Option<String>>>, pub announce: tokio::sync::broadcast::Sender<Transcript>, pub capture_dir: PathBuf, pub transcribed: Arc<AtomicU64> }` (`Clone`).
- Produces: `pub fn process(ctx: &Ctx, job: &Job) -> Done` — the spec's call path, in this order:
  1. the system's `SystemView` by `job.call.call.short_name` (unknown → the root config, enabled); disabled → row `skipped, "system disabled"`, `Done::Skipped`;
  2. audio: `job.call.files.wav` if it exists, else `files.m4a` if set and exists, else `<capture_dir>/<path>.wav`, else `<capture_dir>/<path>.m4a`, else row `skipped, "no audio"`;
  3. a WAV is read (`read_wav`; an error → `Done::Failed("unreadable audio: …")`, no row — the queue's `failed` writes it); `gate::check` with the PCM for a WAV, `None` for an M4A → row `skipped, <reason>`;
  4. WAV: resample, `p25_filter` when on, `wav_16k`; M4A: the file's bytes as they are;
  5. `prompt::build` with the system's `name` and its `prompt` settings;
  6. `engine.transcribe` with the system's `engine` settings and `language`: `Retry` → `Done::Retry`; `Fail` → `Done::Failed` (no row);
  7. `filter::check(text, call_length)` → status `ok` / `empty` / `rejected` with reason; a rejection logs `warn` "rejected <path>: <reason>" (never the text);
  8. `segment::split` for `ok` only;
  9. one `Row` (`engine` = `host_of(url)`, `model_asked` = the setting, `model_reported` = the answer's `model` else the cached reported model, `attempts` = `job.tries + 1`, `took_ms` the engine round trip, `raw` the body, `prompt` what was sent) and the segments through `store.put`; a store error → `Done::Failed("store: …")`;
  10. `announce.send(Transcript)` (a send error, no listeners, is ignored); `transcribed += 1` for `ok`; `Done::Ok { took }`.
- Produces: `pub fn record_failed(ctx: &Ctx, job: &Job, reason: &str)` — a `failed` row (`attempts = job.tries + 1`, the existing row's `attempts + 1` when there is one), then `announce`.
- Produces: `pub fn start_ms(r: &CallRecord) -> i64` and `pub fn length_ms(r: &CallRecord) -> i64` per the decisions.

- [ ] **Step 1: Write the failing tests**

```rust
fn ctx(url: &str) -> Ctx { /* Config with engine.url = url, in-memory store, Engine 5 s, one system "sys1" named "System One" */ }
fn speech_call(dir: &Path, tg: u32) -> ConcludedCall { /* testing::call then overwrite files.wav with 3 s of a 0.3-amplitude 1 kHz tone at 8 kHz, files.m4a = None */ }
fn job(c: ConcludedCall) -> Job { Job { call: c, kind: Kind::Live, enqueued: Instant::now(), tries: 0 } }

#[test] fn a_call_ends_as_an_ok_row_with_segments_and_an_announcement() {
    let s = MockServer::start(|_| (200, OK.into()));   // Task 5's OK body
    let ctx = ctx(s.url()); let mut rx = ctx.announce.subscribe();
    let dir = testing::temp_dir("work"); let mut c = speech_call(&dir, 101);
    c.call.src_list = vec![SrcEntry { src: 1001, pos: 0.0, tag: "Engine 3".into(), ..Default::default() }];
    c.call.talkgroup_tag = "Fire".into();
    assert!(matches!(process(&ctx, &job(c.clone())), Done::Ok { .. }));
    let (row, segs) = ctx.store.get(&c.path).unwrap().unwrap();
    assert_eq!((row.status, row.text.as_deref(), row.model_reported.as_deref(), row.attempts), (Status::Ok, Some("Tower 73 go ahead"), Some("Qwen3-ASR-1.7B"), 1));
    assert_eq!((segs.len(), segs[0].unit, segs[0].text.as_str()), (1, Some(1001), "Tower 73"));
    assert_eq!(row.prompt.as_deref(), Some("Fire. System One. Units: Engine 3"));   // tag set above; testing::call leaves the description empty
    let r = &s.requests()[0];
    assert_eq!(r.form_file_name("file").unwrap(), "call.wav");
    assert_eq!(read_wav(&write_temp(&r.form_field("file").unwrap())).unwrap().rate, 16000, "posted at 16 kHz");
    assert_eq!(rx.try_recv().unwrap().path, c.path);
    assert_eq!(ctx.transcribed.load(Ordering::Relaxed), 1);
}

#[test] fn the_gate_skips_before_the_network() {
    let s = MockServer::start(|_| (200, OK.into())); let ctx = ctx(s.url()); let dir = testing::temp_dir("work");
    let silent = testing::call(&dir, "sys1", 1);                         // silence
    assert!(matches!(process(&ctx, &job(silent.clone())), Done::Skipped(m) if m == "silent"));
    let mut enc = speech_call(&dir, 2); enc.call.encrypted = true;
    assert!(matches!(process(&ctx, &job(enc)), Done::Skipped(m) if m == "encrypted"));
    let mut short = speech_call(&dir, 3); short.call.call_length = 0.4;
    assert!(matches!(process(&ctx, &job(short)), Done::Skipped(m) if m == "too short"));
    assert!(s.requests().is_empty());
    assert_eq!(ctx.store.get(&silent.path).unwrap().unwrap().0.reason.as_deref(), Some("silent"));
}

#[test] fn a_disabled_system_and_a_missing_file() {
    // systems[0].enabled = false → Skipped("system disabled") and a row; enabled again with files.wav and files.m4a pointing nowhere and no file under capture_dir → Skipped("no audio").
}

#[test] fn an_m4a_goes_as_it_is() {
    // files.wav deleted, files.m4a = the placeholder bytes testing::call wrote: posted with file name "call.m4a", content type audio/mp4, the same bytes; no gate on RMS.
}

#[test] fn empty_and_rejected_keep_the_raw_and_no_text() {
    // 200 {"text":""} → Done::Ok, row status Empty, text None, raw == body; 200 {"text":"Thank you for watching"} → Rejected, reason "hallucination".
}

#[test] fn engine_failures_write_no_row_and_record_failed_does() {
    let s = MockServer::start(|_| (503, "".into())); let ctx = ctx(s.url()); let dir = testing::temp_dir("work");
    let c = speech_call(&dir, 9);
    assert!(matches!(process(&ctx, &job(c.clone())), Done::Retry(_)));
    assert!(ctx.store.get(&c.path).unwrap().is_none());
    record_failed(&ctx, &Job { tries: 3, ..job(c.clone()) }, "gave up: 503");
    let (row, _) = ctx.store.get(&c.path).unwrap().unwrap();
    assert_eq!((row.status, row.reason.as_deref(), row.attempts), (Status::Failed, Some("gave up: 503"), 4));
    record_failed(&ctx, &job(c.clone()), "again");
    assert_eq!(ctx.store.get(&c.path).unwrap().unwrap().0.attempts, 5, "attempts accumulate across passes");
}

#[test] fn a_record_without_ms_fields_gets_seconds_times_1000() {
    let r = CallRecord { start_time: 1_700_000_000, call_length: 4.5, ..Default::default() };
    assert_eq!((start_ms(&r), length_ms(&r)), (1_700_000_000_000, 4500));
    let r = CallRecord { start_time: 1, start_time_ms: 1_700_000_000_123, call_length_ms: 4512, ..Default::default() };
    assert_eq!((start_ms(&r), length_ms(&r)), (1_700_000_000_123, 4512));
}
```

- [ ] **Step 2: Run them to see them fail.**
- [ ] **Step 3: Implement `work.rs`.** The engine is called with the system's effective `engine` settings; `took` is measured around `engine.transcribe` only.
- [ ] **Step 4: `cargo test work` → pass.**
- [ ] **Step 5: Commit** — `git commit -am "The pipeline: gate, prepare, prompt, post, filter, segment, write, announce"`

---

### Task 9: The archive pass and re-transcribe

**Files:**
- Create: `src/archive.rs`
- Test: `src/archive.rs` (scripted `CallsPage`s, a `testing::capture()` host, a real `Queue` whose `work` records paths)

**Interfaces:**
- Consumes: `Host::calls`, `CallsQuery`, `CallsPage`, `StoredCall`, `Store::settled`, `Store::retranscribe_candidates`, `Queue::push`, `work::systems_of` (for the system index).
- Produces: `pub enum Mode { Start, Periodic, Retranscribe { everything: bool, model: String } }`.
- Produces: `pub struct Archive { … }` with `Archive::new(host: Host, store: Store, queue: Arc<Queue>, systems: Arc<RwLock<Vec<SystemView>>>, days: Arc<AtomicU32>, capture_dir: PathBuf) -> Archive` and:
  - `begin(&mut self, mode: Mode, now_ms: i64) -> bool`: false if a pass is running; else sends `CallsQuery { from: Some(now_ms − days × 86 400 000) (None when Retranscribe { everything: true }), limit: Some(500), ..Default::default() }` via `host.calls`, remembers the request number, logs `info` "archive pass started (…)", returns true. For `Retranscribe`, first loads the candidate set from `store.retranscribe_candidates(model, since)`; an empty set ends the pass at once with an `info` log and no query.
  - `on_page(&mut self, page: CallsPage) -> bool` (true when it was this pass's request): `page.error` → `warn` once per pass, pass ends; else for each `StoredCall` with `audio == true` whose path is not settled (no row; or a `failed` row with `attempts < 10`; in Retranscribe mode: in the candidate set), builds a `ConcludedCall { path, system: the index of its `record.short_name`, call: record, files: CallFiles { json: <capture>/<path>.json, wav: <capture>/<path>.wav, m4a: None } }` and pushes it as `Kind::Archive` (or `Kind::Retranscribe`); a `StoredCall` with `audio == false` and no row gets a row `skipped, "no audio"` written directly (in `Retranscribe` mode a candidate without audio is left as it is); then asks the next page with `before: Some(page.before)` when `page.more` and the stop rule allows: `Mode::Periodic` stops when the page queued nothing and wrote nothing; `Start` and `Retranscribe` continue to the end. When the pass ends: `info` "archive pass: N queued, M skipped, P pages".
  - `running(&self) -> bool`.

- [ ] **Step 1: Write the failing tests**

```rust
fn stored(path: &str, audio: bool) -> StoredCall { StoredCall { path: path.into(), record: CallRecord { short_name: "sys1".into(), talkgroup: 1, call_length: 3.0, ..Default::default() }, audio, json: true } }

#[test] fn a_start_pass_queues_every_unsettled_call_and_pages_to_the_end() {
    // store has rows: "a" ok, "f" failed attempts 2, "g" failed attempts 10
    // begin(Start, now) → the captured calls.query has from == now − 7 days, limit 500
    // on_page(page 1: a, b, f, g, more true, before 500) → queued b and f (not a, not g); on_page asked page 2 with before 500
    // on_page(page 2: c (audio false), d; more false) → d queued, c gets a row skipped "no audio"; pass over; running() false; log says "2 pages"
}
#[test] fn a_periodic_pass_stops_at_the_first_settled_page() {
    // rows for a, b; page 1: a, b, more true → no second query sent; running() false
}
#[test] fn the_store_being_off_is_logged_once_and_the_pass_ends() { /* page.error Some → warn, no query, running false; begin again → a new query */ }
#[test] fn retranscribe_queues_only_the_candidates() {
    // rows: a ok m1, b ok m2, c empty m1, d skipped; begin(Retranscribe { everything: false, model: "m2" }) → candidates {a, c}; a page with a, b, c, d queues a and c as Kind::Retranscribe (the work closure records kinds)
    // everything: true → the query has no `from`
    // no candidates (model "m1" with only a, c) → no query, info log
}
#[test] fn a_second_begin_while_running_is_refused() { /* begin → true; begin → false; after the last page, begin → true */ }
```

- [ ] **Step 2: Run them to see them fail.**
- [ ] **Step 3: Implement `archive.rs`.** `Archive` owns no thread: `begin` is called from the upkeep thread and `on_page` from the protocol thread, so it lives in an `Arc<Mutex<Archive>>` in the plugin; keep `on_page` quick (the `settled` query is one `IN` list of ≤ 500).
- [ ] **Step 4: `cargo test archive` → pass.**
- [ ] **Step 5: Commit** — `git commit -am "The archive pass over calls.query, and re-transcribe by the engine's reported model"`

---

### Task 10: The HTTP side

**Files:**
- Create: `src/http/mod.rs`, `src/http/cors.rs`, `src/http/transcripts.rs`, `src/http/search.rs`, `src/http/events.rs`, `src/http/health.rs`
- Test: each module (axum `Router::oneshot` via `tower::ServiceExt` for routes; a raw `TcpStream` for the SSE test)

**Interfaces:**
- Consumes: `Store`, `Transcript`, `tokio::sync::broadcast::Sender<Transcript>`.
- Produces: `pub struct Health { pub engine_url: String, pub model: String, pub up: bool, pub last_ok_ms: Option<i64>, pub last_error: String, pub queue: u64, pub in_flight: u64, pub transcribed_24h: u64 }` (`Serialize` by hand to the spec's `{ ok, engine: { url, model, up, lastOkMs, lastError }, queue, inFlight, transcribed24h }`; `ok` = `up`).
- Produces: `pub struct AppState { pub store: Store, pub events: broadcast::Sender<Transcript>, pub health: Arc<RwLock<Health>>, pub origins: Vec<String> }` (`Clone`) and `pub fn router(state: AppState) -> Router`.
- Produces: `pub struct ServerHandle` with `stop(self, within: Duration)` and `pub fn start(state: AppState, bind: &str, port: u16) -> Result<(ServerHandle, SocketAddr), String>` — a current-thread tokio runtime on a thread named `http`, graceful shutdown through a oneshot (as trunk-reporter's `server.rs`).
- Routes, exactly:
  - `GET /api/v1/transcripts?paths=a,b,c` → `{ "transcripts": [Transcript…] }` in the order given; paths trimmed, empty ones dropped, duplicates dropped, at most the first 200 used.
  - `GET /api/v1/transcripts?system&talkgroup&from&to&before&limit` (no `paths`) → `{ "transcripts": [...], "more": bool, "before": i64 }`; `limit` default 100, clamped to 1–500; an unparsable number → 400 `{ "error": "bad <name>" }`.
  - `GET /api/v1/transcripts/{*path}` → `Transcript` or 404 `{ "error": "no transcript" }`.
  - `GET /api/v1/search?q&system&talkgroup&from&to&before&limit` → `{ "hits": [ { …Transcript, "snippet": "…" } ], "more", "before" }`; a missing or blank `q` → 400 `{ "error": "bad q" }`.
  - `GET /api/v1/events` → `text/event-stream`; each row as `id: <transcribedMs>\nevent: transcript\ndata: <Transcript JSON>`; `event: ping` with data `{}` every 15 s (`KeepAlive::new().interval(15 s).event(Event::default().event("ping").data("{}"))`); with `Last-Event-ID: <ms>` (header, or `?lastEventId=`), `store.since(ms, 1000)` replayed first, then live rows with `transcribedMs` above the last replayed; a non-numeric id is ignored. Headers `Cache-Control: no-cache`, `X-Accel-Buffering: no`.
  - `GET /api/v1/health` → the `Health` JSON, always 200.
  - CORS: `cors.rs` is trunk-reporter's middleware (`src/api/cors.rs`) with `GET, OPTIONS` as the methods and `Last-Event-ID, Content-Type` as the headers; an `Origin` in `origins` (or `origins` containing `*`) gets `Access-Control-Allow-Origin: <origin>` and `Vary: Origin`; `OPTIONS` from it → 204; an unlisted origin gets no CORS headers.

- [ ] **Step 1: Write the failing tests**

```rust
async fn get(app: Router, uri: &str, origin: Option<&str>) -> (StatusCode, HeaderMap, Value) { /* oneshot, read body with http_body_util */ }

#[tokio::test] async fn by_paths_in_order_trimmed_deduplicated_and_capped() {
    // store rows a, b, c; GET /api/v1/transcripts?paths=%20c%20,,a,c,zz → transcripts [c, a]
    // 250 distinct paths, the first 200 seeded: 200 come back
}
#[tokio::test] async fn one_by_path_and_404() { /* /api/v1/transcripts/sys1/2026/10/27/101-1 → status ok, json.path == ...; /api/v1/transcripts/nope → 404 {"error":"no transcript"} */ }
#[tokio::test] async fn a_page_and_limit_is_clamped() { /* 7 rows; ?limit=3 → 3, more true, before = 4th's start_ms; ?limit=0 → 1 row; ?limit=9999 → all 7, more false; ?limit=x → 400 {"error":"bad limit"} */ }
#[tokio::test] async fn search_hits_carry_a_snippet_and_survive_fts_syntax() { /* ?q=structure → one hit with "<b>structure</b>"; ?q= → 400; ?q=%22 → 200 */ }
#[tokio::test] async fn health_is_the_specs_shape() { /* json == {"ok":true,"engine":{"url":"http://e:8766","model":"m","up":true,"lastOkMs":5,"lastError":""},"queue":2,"inFlight":1,"transcribed24h":40} */ }
#[tokio::test] async fn cors_answers_listed_origins_only() {
    // origins ["http://a:1"]; GET with Origin http://a:1 → ACAO http://a:1, Vary Origin; OPTIONS → 204; Origin http://b:2 → no ACAO; origins ["*"] → ACAO echoes http://b:2
}
#[tokio::test] async fn events_replay_then_live_and_a_bad_last_event_id_starts_live() {
    // start() on 127.0.0.1:0; rows transcribed_ms 10, 20 seeded; connect with a TcpStream, send GET /api/v1/events with Last-Event-ID: 10
    // read: "id: 20\nevent: transcript\ndata: {...}" then after events.send(row 30): "id: 30 …"; a second connection with Last-Event-ID: abc gets nothing until events.send(row 40), then "id: 40"
    // stop(1 s) returns
}
```

- [ ] **Step 2: Run them to see them fail.**
- [ ] **Step 3: Implement the modules.** One handler for `/api/v1/transcripts` that branches on `paths`. The SSE stream is `stream::iter(replayed).chain(BroadcastStream)` filtered by `transcribedMs > floor` (`tokio-stream` is not a dependency: unfold over `rx.recv()` as trunk-reporter's `sse.rs` does, dropping `Lagged` errors).
- [ ] **Step 4: `cargo test http` → pass.**
- [ ] **Step 5: Commit** — `git commit -am "The HTTP side: transcripts by path and by page, search, events, health, CORS"`

---

### Task 11: The plugin — wiring, actions, live settings, upkeep, shutdown

**Files:**
- Modify: `src/plugin.rs` (replace the stub), `src/main.rs`
- Test: `src/plugin.rs` (`testing::run` with a `MockServer`)

**Interfaces:**
- Consumes: everything above.
- Produces: `pub struct Transcribe { host, ctx: Ctx, queue: Arc<Queue>, archive: Arc<Mutex<Archive>>, http: Option<ServerHandle>, upkeep: Option<(mpsc::Sender<()>, JoinHandle<()>)>, started: Instant }` and `impl Plugin for Transcribe`:
  - `start`: `config.validate()` (Err → the message); `Store::open(data_dir/transcripts.sqlite, host.warn)`; `Engine::new(timeout)`; `Ctx`; `Queue::start` with `workers = engine.concurrency`, `max = queue.max`, the spec's retry schedule and 50 min, `work = process`, `failed = record_failed`; `Archive`; the HTTP server when `http.port` is set (a bind failure is `Err` — shown as the status, the plugin does not start); the upkeep thread; `info` "transcribe <version>: engine <host>, model <model>, serving http://<bind>:<port> | not serving"; then `archive.begin(Mode::Start)`.
  - `call_concluded`: `queue.push(call, Kind::Live)`; a `false` (duplicate) sends `call.result: skipped, "already queued"`.
  - `calls(page)`: `archive.lock().on_page(page)`.
  - `action`: `"test"` → a thread: `Engine::transcribe` with the `engine` block of `a.value` (parsed into `EngineConfig`; falls back to the running one) and `audio::test_tone()`; `reply.ok(format!("{model} answered in {ms} ms"))` (`model` = the answer's `model`, else the reported one, else "the engine") or `reply.fail("<error>")`. `"retranscribe"` → `archive.begin(Mode::Retranscribe { everything: value.archive.retranscribeEverything, model: reported else engine.model })`: `reply.ok("Re-transcription started; the log says how many calls were queued")`, or `reply.fail("An archive pass is already running")`. Anything else → `reply.fail`.
  - `settings(setup)`: validate; swap `ctx.config` and `ctx.systems`; `warn` "Concurrency, Queue size and HTTP settings take effect when recording next starts" when any of those differ; `Ok(())`.
  - `shutdown(grace)`: stop the upkeep thread; wait up to `0.5 × grace` for `queue.len() == 0` (a call in flight at stop gets its answer); `queue.shutdown(grace / 2)` (what is still queued is failed "shutting down", in-flight work gets the rest); `http.stop(min(2 s, grace / 4))`.
  - the upkeep thread (`upkeep`, every 10 s via `recv_timeout`): `host.metrics` from `queue.snapshot()` as the decisions section lists; `host.status`: `Error` "engine down since <m> min: <last error>" when `down_since` ≥ 5 min; `Warning` "<n> calls queued" when `queued > max / 2`, or "engine degraded: <last error>" when `in_a_row > 0`; else `Ok` "<model>"; `health` written for the HTTP side; every `archive.everyMinutes`: `archive.begin(Mode::Periodic)`; every 10 min (and at the first tick): `engine.models()` → `reported_model` (an `info` log when it changes: "engine model: <name>"); once a day (first time 60 s after start): `store.retain(retentionDays)` with an `info` log when rows went.

- [ ] **Step 1: Write the failing tests**

```rust
fn hello_with(dir: &Path, url: &str, extra: Value) -> Hello { /* testing::hello with {"engine":{"url":url}} merged with extra */ }

#[test] fn a_live_call_is_transcribed_and_answered() {
    let s = MockServer::start(|r| if r.path == "/v1/models" { (200, MODELS.into()) } else { (200, OK.into()) });
    let dir = testing::temp_dir("plugin"); let c = speech_call(&dir, 101);
    let out = testing::run::<Transcribe>([HostMessage::Hello(hello_with(&dir, s.url(), json!({}))), HostMessage::CallConcluded(c.clone())]);
    assert!(out.ready());
    assert_eq!(out.results().iter().map(|r| (r.0.as_str(), r.1)).collect::<Vec<_>>(), [(c.path.as_str(), Outcome::Ok)]);
    assert!(Store::open(&dir.join("data/transcripts.sqlite"), &|_| {}).unwrap().get(&c.path).unwrap().is_some());
    let q = out.messages.iter().filter(|m| matches!(m, PluginMessage::CallsQuery(_))).count();
    assert_eq!(q, 1, "the start pass asked for calls once");
}
#[test] fn a_page_from_the_recorder_feeds_the_archive() {
    // Hello, then HostMessage::Calls(CallsPage { request: 1, entries: [stored call whose files exist under dir], more: false, .. }) → after run, the store has a row for it and no call.result was sent for it
}
#[test] fn the_test_button_reports_model_and_latency() {
    // HostMessage::Action(Action { request: 7, action: "test", value: json!({"engine": {"url": s.url()}}) }) → an ActionResult { request: 7, ok: true, message } with message containing "Qwen3-ASR-1.7B" and " ms"
    // an unreachable url in value → ok false
}
#[test] fn new_settings_are_taken_and_some_wait_for_a_restart() {
    // HostMessage::Settings(hello with engine.url = s2.url(), engine.concurrency = 4) then a call → the call went to s2; a warn log mentions "Concurrency"; status Ok
    // Settings with an empty url → status Error "new settings not taken: …"
}
#[test] fn shutdown_fails_what_is_still_queued() {
    // engine hangs 4 s per request; engine.concurrency 1; three calls; the input ends at once (grace 10 s from testing::run):
    // the idle wait (5 s) lets call 1 finish at 4 s and call 2 start; the drain fails call 3 "shutting down"; call 2 finishes inside the in-flight wait.
    // Expect results: call 1 Ok, call 3 Failed "shutting down", call 2 Ok; run returns within 12 s.
}
#[test] fn metrics_and_status_carry_the_engine() {
    // after a run with one Ok call: the last Metrics has endpoints[0].name == host_of(url), state Up, extra["transcribed"] == 1, extra["model"] == "Qwen3-ASR-1.7B"; status is (Ok, "Qwen3-ASR-1.7B")
}
```

`testing::run` ends the input right after the last message, so `shutdown` runs while calls are still queued. The idle wait in `shutdown` (up to `0.5 × grace` for the queue to empty) is what lets these tests see their results; it is also right for the recorder, where a call in flight at stop gets its answer.

- [ ] **Step 2: Run them to see them fail.**
- [ ] **Step 3: Implement `plugin.rs`** and `main.rs` (`fn main() { trunk_recorder_plugin::run::<transcribe::plugin::Transcribe>() }`).
- [ ] **Step 4: `cargo test` (all), `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check` → pass.**
- [ ] **Step 5: Commit** — `git commit -am "The plugin: start, live calls, archive pages, the two buttons, live settings, upkeep, shutdown"`

---

### Task 12: Documentation and release readiness

**Files:**
- Modify: `README.md`, `CHANGELOG.md`, `Cargo.toml` (version stays 0.1.0)
- Create: `examples/settings.json` (a full settings object with the defaults and `engine.url` filled in, as the template has)

- [ ] **Step 1: Write the README** for a user: what it does (one paragraph), the engines known to work (`qwen3-asr-server`, Speaches, LocalAI, OpenAI — with the caveat that only the first was run against it), installing from the registry or a release, the settings table (every key, its default, one line each), the two buttons, what the statuses on a transcript mean, the HTTP API (the spec's route table verbatim, plus "no authentication: keep it on the LAN or behind TRP's interface port"), CORS, re-transcribing after a model change, and "Where the files are" (`plugin-data/transcribe/transcripts.sqlite`).
- [ ] **Step 2: `CHANGELOG.md`** `## [0.1.0]` — the features in five lines.
- [ ] **Step 3: Verify the manifest and a describe run**

Run: `cargo build --release && ./target/release/transcribe --describe > manifest.json && python .github/check_manifest.py manifest.json --release && ./target/release/transcribe --version`
Expected: no error; `0.1.0`.

- [ ] **Step 4: Commit** — `git commit -am "README, changelog and an example settings file for 0.1.0"`

---

## After the plan (not tasks for the implementer)

- **Live run** under the user's recorder against the engine on Eddie (`http://eddie:8766`): install the build through **Install from file**, set the engine URL and `http.port` 8767, watch the start pass transcribe the last 7 days, press **Test the engine**, open `/api/v1/events` in a browser. Then the P25 fine-tune on the same server, **Re-transcribe**, and compare the two on the same hundred calls.
- Push to `trunk-reporter/trunk-plugin-transcribe`, tag `v0.1.0`, submit to the registry once the SDK 0.9.0 recorder is released.
