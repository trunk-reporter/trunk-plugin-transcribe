# Transcribe — a Trunk Recorder Pro plugin — design

**Date:** 2026-10-27
**Status:** draft for review
**Companions:** `2026-10-26-trp-call-store-design.md` (the store this plugin reads back from),
`2026-10-25-trunk-reporter-ui-design.md` (the web app that will show the transcripts). This plugin
is its own project; it never depends on trunk-reporter and trunk-reporter never depends on it.

## Intent

Every recorded call gets a transcript, written by whatever speech-to-text engine the user already
runs, and the transcripts are there for an interface to read beside the calls. The plugin itself
holds no model. It is the glue: it hands each call's audio to an OpenAI-compatible transcription
endpoint, keeps what comes back, and serves it.

Why a plugin and not a server mode: Trunk Recorder Pro (TRP) hands plugins each finished call with
its files on disk, restarts them when they crash, and shows their health, settings and log in its
interface. That is exactly the operating envelope a transcriber needs, and it comes free.

Why an API and not a bundled model: engines differ by machine. The reference setup is the Qwen3-ASR
family behind `qwen3-asr-server` on a GPU box; another user runs Whisper through Speaches or LocalAI
on the recorder; a third pays OpenAI for a quiet system. All speak `POST /v1/audio/transcriptions`.
A plugin that speaks that and nothing else is a small Rust binary that ships through the registry on
every platform, including a Raspberry Pi whose GPU is elsewhere on the network. The user's P25
fine-tune (`trunk-reporter/qwen3-asr-p25-0.6B`) is then a drop-in: the same server with a different
model folder.

Measured 2026-10-27 as the spike behind this design: vanilla Qwen3-ASR-1.7B behind
`qwen3-asr-server` on an RTX A5000, reached over Tailscale from the recorder, transcribed 13 to 20
second P25 calls in 0.65 to 0.75 s each round trip (3 s cold), with word timestamps when asked for
in the OpenAI way, and readable dispatch text.

## Goals

1. **Every concluded call on an enabled system is transcribed** once, within seconds of concluding
   on a healthy engine, and eventually when the engine was down.
2. **Nothing engine-specific in the plugin.** One endpoint, one model name, one optional key.
   Vanilla Whisper, Qwen3-ASR, a fine-tune and a hosted API all work with only settings changed.
3. **No garbage in the record.** Silence, encrypted bursts and hallucinations are caught before
   they reach the model or before they are kept.
4. **Readable from an interface**: by call path, in batches, and as they arrive, from a page on
   another origin (TRP serves interfaces on their own ports).
5. **Re-transcribable.** Every transcript records which engine and model produced it, so a better
   model can redo the archive without touching the rest.
6. **Runs on a Pi.** No model, no Python, no GPU on the recorder.

## Non-goals

- Running a model. Not now, not as an option.
- Speaker identification beyond what the call record already says (the `srcList`).
- Writing into TRP's call store. The plan for TRP names an `annotations` table as a later step;
  when it exists, this plugin writes through it and its own HTTP side can go. Until then it keeps
  its own file.
- Live, in-call transcription. Calls are transcribed when concluded. Streaming is a later plugin
  or a later version.
- Translation, diarisation, summarisation.

## Anti-goals

- A transcript that reads as confident text for a call that was silence or encryption. That is
  worse than no transcript, because an interface will show it as what was said.
- Falling behind silently. A queue that grows without a figure on the Health tab is a failure.
- Taking the recorder down. The plugin's inference path must never block the protocol thread.

## What a call's result means

TRP keeps a call's files until every plugin has reported on it, and keeps them longer only when
the plugin says `failed` and the user has **Keep when a plugin fails** on. The plugin uses that
honestly, and the user's TRP setting decides what happens to the files:

- `ok`: transcribed and stored.
- `skipped`: the plugin chose not to (encrypted, too short, silent, no audio, the system is
  disabled). Nothing will change by trying again.
- `failed`: the plugin could not (engine down after the retries, queue full, store write failed,
  shutting down with the call still queued). Trying again later might work, and the archive pass
  will.

A call never waits in the queue past **50 minutes**: it is answered `failed, "engine down"` then,
inside TRP's one-hour limit, so the user's setting applies rather than TRP's default. With **Keep
when a plugin fails** on, the archive pass finds the files later; off, the call's row records that
it was never transcribed and why.

## Constraints

- **Plugin SDK:** `trunk-recorder-plugin` 0.9.0 (`calls.query` / `Host::calls`; in TRP PR #104,
  not yet on crates.io, where 0.8.0 is the latest). Developed against a path dependency on the TRP
  checkout; switched to the crates.io version when it is published. API version 1.
- **Language:** Rust, edition 2024, like trunk-reporter. The HTTP side is axum 0.8 and tower-http,
  the store rusqlite with `bundled`, the client `reqwest` with rustls (no OpenSSL on the Pi).
- **TRP contract** (`docs/plugins/writing-plugins.md`): `--describe` prints the manifest; `hello`
  on stdin; `ready` within a moment; a `call.result` for **every** `call.concluded` or TRP counts
  it failed after an hour; `shutdown` with a grace period (10 s when recording stops); a crash is
  restarted with backoff; the queue to the plugin holds 1,024 events.
- **Only while recording.** TRP runs plugins while it records. Archive work (below) runs inside
  that window.
- **The audio TRP hands over** is 8 kHz 16-bit mono WAV (an M4A only if asked for). Engines expect
  16 kHz; the server used in the spike resamples itself, but the plugin does not rely on that.
- **Interfaces are on other origins.** The HTTP side must answer CORS preflights for an allowlist
  of origins.

## The parts

Four, in one binary:

1. **The protocol side.** The SDK's `run` loop: `hello`, events, `shutdown`, `action`, `calls`
   answers. It never does I/O that can stall. It pushes work to the queue and answers TRP at once.
2. **The worker.** A bounded pool (`concurrency`, default 2) that takes calls from the queue,
   gates them, posts them to the engine, filters what comes back, and writes the transcript. It
   sends `call.result` when it is done with a call, or `skipped` when the gate stops it.
3. **The store.** One SQLite file in the plugin's data folder, `transcripts.sqlite`. WAL mode,
   one writer.
4. **The HTTP side.** A small axum server on a port of the plugin's own, answering transcripts by
   path and streaming new ones. Off by default until `http.port` is set.

## The call path

For each `call.concluded` on a system where the plugin is enabled:

1. **Enqueue.** The path key, the record and the audio path go on the queue. Queue depth is a
   metric. If the queue is past `queue.max` (default 2,000) the oldest waiting call is dropped
   with `call.result: failed, "queue full"`, and a warning is logged once a minute at most.
   The audio is the WAV `call.concluded` names; for an archive call it is
   `<capture_dir>/<path>.wav` if that exists, else `<path>.m4a`, else the call gets a row
   `skipped, "no audio"` and no request. An M4A is posted as it is, with its content type: the
   OpenAI route accepts it and every engine in scope decodes it. The plugin decodes no AAC.
2. **Gate.** Before any network:
   - a call with `encrypted: true` in its record is skipped;
   - a call shorter than `gate.minSeconds` (default 1.0, from the record's length) is skipped;
   - a WAV whose RMS over the whole file is under `gate.rms` (default 0.01, from the P25
     fine-tune's measurements: blank audio under 0.003, speech over 0.03) is skipped; an M4A is
     not measured (no decoder), so the record's flags are its whole gate;
   - a skipped call gets a row with `status: skipped` and the reason, so the interface can show
     why there is no text, and a `call.result: skipped`.
3. **Prepare.** A WAV is resampled to 16 kHz mono in the plugin (a windowed-sinc resampler on
   8 kHz input is a few lines; no ffmpeg dependency). An M4A goes as it is. The P25 bandpass and notch chain from the
   fine-tune's training is **off by default** (`prepare.p25Filter`); it was tuned for that model and
   hurts others.
4. **Prompt.** The request's `prompt` field carries the vocabulary the model will not know: the
   talkgroup's tag and description from TRP's talkgroup file, the system's name, and the unit tags
   of the radios in `srcList` when TRP knows them. Capped at 200 characters. Off with
   `prompt.enabled: false`. Engines that ignore `prompt` lose nothing.
5. **Post.** `POST {endpoint}/v1/audio/transcriptions`, multipart: `file`, `model`,
   `language`, `response_format=verbose_json`, `timestamp_granularities[]=word`, `prompt`, and
   `Authorization: Bearer` when a key is set. Timeout `engine.timeoutSeconds` (default 60).
   Retries: 3, with backoff 2 s, 8 s, 30 s, on connection errors, 5xx and 429. A 4xx other than
   429 is a failure without retry (the request is wrong, retrying will not fix it).
6. **Filter.** From the fine-tune's label-quality rules, applied to every engine's output:
   - empty text → `status: empty`;
   - more than `filter.maxWordsPerSecond` (default 8) → `status: rejected, "implausible rate"`;
   - the same token repeated more than `filter.maxRepeat` (default 4) times in a row →
     `status: rejected, "repetition"`;
   - a phrase from `filter.hallucinations` (default: the dozen Whisper is known for, "Thank you
     for watching" and kin) as the whole text → `status: rejected, "hallucination"`.
   Rejected transcripts are kept with their status and the raw text, never shown as text.
7. **Segment.** When the engine returned word timestamps, words are grouped into segments at the
   boundaries of the record's `srcList` (each entry has `pos` seconds and the next entry's `pos`
   ends it). Each segment carries the radio id and its words. Without timestamps there is one
   segment, radio unknown. This is what lets an IRC-style view put each sentence under the unit
   that said it; the old tr-engine page matches segments to transmissions by time overlap and
   expects exactly this shape.
8. **Write.** One row in `transcripts`, the segments in `segments`, in one transaction. Then
   `call.result: ok` with `took_ms`. A write failure is `call.result: failed`.
9. **Announce.** The HTTP side's event stream gets `{ path, status, text }`.

## The archive

Transcribing calls recorded before the plugin existed, or while the engine was down, or with an
old model:

- **On start, and every `archive.everyMinutes` (default 10) while recording:** the plugin pages
  TRP's call store through `calls.query`, newest first, within `archive.days` (default 7), and
  queues every call it has no row for. Paging stops at the first page whose calls all have rows,
  which keeps a steady state cheap. Every call the pass looks at ends with a row, whatever its
  outcome (`skipped, "no audio"` and `skipped, "system disabled"` included), or the stop rule
  would never fire. The store's answer includes calls not yet committed, so this also catches a
  call missed during a restart. A call that is both live (in the queue) and on a page is queued
  once, by path.
- **Priority.** Live calls go ahead of archive calls in the queue. Archive work uses at most one
  worker, so a backlog never starves the present.
- **The engine's model name.** The plugin asks `GET /v1/models` at start and every ten minutes
  and keeps the name reported (or, when the route is missing, the `model` field of the last
  transcription answer, or failing that the `engine.model` setting). Every row stores both the
  name asked for (`model_asked`) and the name reported (`model_reported`).
- **Re-transcribe** (`action: "retranscribe"`, a button in the plugin's settings): rows whose
  `model_reported` differs from the name the engine reports now are queued again, oldest last,
  live calls still first. It takes `ok`, `empty` and `rejected` rows (a better model may turn an
  empty or rejected call into text); never `skipped` ones, which no model changes. Within
  `archive.days` by default, the whole store with the button's **everything** box ticked. A call
  whose audio is gone is left as it is. The old row stays until the new one is written, so
  nothing disappears.
- **The store being off** (`calls` answers with `error`): the archive pass logs it once and tries
  again next interval. Live transcription is unaffected.

## The store

`transcripts.sqlite` in the plugin's data folder. Schema version in `user_version`.

```
transcripts (
  path          text primary key,         -- TRP's call path key, capture-relative, no extension
  short_name    text not null,
  talkgroup     integer not null,
  start_ms      integer not null,
  length_ms     integer not null,
  status        text not null,            -- ok | skipped | empty | rejected | failed
  reason        text,                     -- for skipped / rejected / failed
  text          text,                     -- the transcript (null unless ok)
  language      text,
  engine        text not null,            -- endpoint host
  model_asked   text not null,            -- the engine.model setting at the time
  model_reported text,                    -- what the engine said it ran, when it said
  prompt        text,                     -- what was sent, for debugging
  took_ms       integer,
  attempts      integer not null,
  transcribed_ms integer not null,        -- when the row was written, Unix ms
  raw           text                      -- the engine's JSON, verbatim
)
segments (
  path    text not null references transcripts(path) on delete cascade,
  seq     integer not null,
  unit    integer,                         -- radio id from srcList, null if unknown
  start_s real not null,
  end_s   real not null,
  text    text not null,
  primary key (path, seq)
) without rowid
index transcripts_start on transcripts(start_ms)
index transcripts_tg_start on transcripts(short_name, talkgroup, start_ms)
```

Retention: rows older than `store.retentionDays` (default 0, keep everything) are deleted in
time-ordered batches in the worker thread. Transcripts are small (a few hundred bytes a call), so
the default is to keep them; the setting exists for a Pi's SD card.

## The HTTP side

Off until `http.port` is set. Binds `http.bind` (default `127.0.0.1`; `0.0.0.0` to serve other
machines). CORS: `http.origins`, a list of origins allowed (`*` allowed but discouraged), answering
preflights with the usual headers. No authentication in this version; the docs say to keep it on
the LAN or behind TRP's own interface port.

One object shape, `Transcript`, everywhere a transcript appears:

```
{ path, system, talkgroup, startMs, lengthMs,
  status,                       -- ok | skipped | empty | rejected | failed
  reason,                       -- null unless skipped / rejected / failed
  text,                         -- null unless ok
  language, model,              -- model = model_reported, else model_asked
  transcribedMs,
  segments: [ { unit, start, end, text } ] }   -- unit null if unknown; start/end seconds
```

| Route | Answers |
|---|---|
| `GET /api/v1/transcripts?paths=a,b,c` | `{ transcripts: [Transcript…] }` for the paths given (up to 200), in the order given; a path with no row is absent |
| `GET /api/v1/transcripts/{*path}` | one `Transcript`, or 404 `{ error: "no transcript" }` |
| `GET /api/v1/transcripts?system=&talkgroup=&from=&to=&before=&limit=` | `{ transcripts, more, before }`: a page newest first; `from` / `to` / `before` are Unix ms as in TRP's own calls query, `before` is the next page's cursor, `limit` 1 to 500 (100) |
| `GET /api/v1/search?q=&system=&talkgroup=&from=&to=&before=&limit=` | `{ hits: [ { …Transcript, snippet } ], more, before }`: full-text search over `text` (SQLite FTS5), newest first; `snippet` is FTS5's, the match wrapped in `<b>` |
| `GET /api/v1/events` | server-sent events: `event: transcript`, `data: Transcript` as each row is written or replaced; `event: ping` every 15 s; `Last-Event-ID` is the row's `transcribedMs`, and a reconnect with it replays rows written since |
| `GET /api/v1/health` | `{ ok, engine: { url, model, up, lastOkMs, lastError }, queue, inFlight, transcribed24h }` |

These shapes are a guide to what the old tr-engine IRC page consumes (`web/irc-radio-live.html`
in the trunk-reporter repo: a batch fetch by call id, a transcription event, and segments matched
to transmissions by time overlap), not a contract to match byte for byte. That page is being
ported to TRP's protocol anyway, and its transcript code is adapted to these shapes as part of
that port. The contract this document fixes is the table above.

## Settings

The manifest's JSON Schema, in the SDK's way (`schemars`). Live settings on (`live_settings:
true`), so a changed endpoint takes effect without a restart. Per-system settings: `enabled`
(default true), `language`, `prompt.extra` (text added to every prompt for that system).

```
engine:   url (required, e.g. http://eddie:8766), model ("default"), apiKey (secret, optional),
          language ("en"), timeoutSeconds (60), concurrency (2)
gate:     rms (0.01), minSeconds (1.0), skipEncrypted (true)
prepare:  p25Filter (false)
prompt:   enabled (true), extra ("")
filter:   maxWordsPerSecond (8), maxRepeat (4), hallucinations ([...])
queue:    max (2000)
archive:  days (7), everyMinutes (10)
store:    retentionDays (0)
http:     port (unset = off), bind ("127.0.0.1"), origins ([])
```

Buttons (`x-actions`): **Test the engine** (posts a one-second built-in sample, reports model and
latency), **Re-transcribe with the current model**.

## Health and logging

`metrics` to TRP every 10 s: queue depth, in flight, retries, failures, the engine as a service
(`up` / `degraded` after a retry / `down` after three failures in a row), mean latency, calls
transcribed. `status` on the card: `ok` with the model name; `warning` when degraded or the queue
is over half full; `error` when the engine has been down for five minutes, with the last error.

Logs at `info`: start, engine model confirmed, archive pass summary. `warn`: each retry, a dropped
call, a rejected transcript. Never the transcript text itself above `debug`.

## Failure modes, by design

| What | Then |
|---|---|
| Engine down at start | `ready` anyway; status `error` after five minutes; calls wait in the queue up to `queue.max`; the archive pass picks up the rest later |
| Engine slow | Latency metric rises; concurrency is the only throttle; nothing is dropped until the queue is full |
| Engine returns garbage | The filter keeps it with a status; the interface shows no text |
| Plugin crashes mid-call | TRP restarts it; the call has no `call.result` yet; the archive pass on start finds it without a row and queues it; TRP keeps the files an hour either way |
| Shutdown with work in flight | In-flight requests get up to the grace period; every call still queued gets `call.result: failed, "shutting down"` (the user's keep-on-failure setting decides its files) and is left for the next archive pass |
| Store file corrupt | Moved aside with a warning, a new one made, the archive pass refills it |
| Two recorders, one engine | Fine: the engine serialises or batches; nothing in the plugin is shared |

## Testing

- **Unit:** the gate on synthetic audio (silence, tone, speech-shaped noise); the resampler against
  a reference; the filters on crafted texts; segmenting words against `srcList` boundaries
  including a call with one radio, overlapping radios, and no timestamps.
- **A fake engine** in the tests: an axum server that answers the OpenAI route with scripted
  responses (text, words, 429 then 200, 500 ×3, a hang past the timeout). The worker is tested
  against it end to end, including retries, the queue cap and the shutdown drain.
- **Protocol:** the SDK's `testing::run` with a scripted `hello`, concluded calls and a `calls`
  page, asserting the `call.result` for each and the `calls.query` the archive pass sends.
- **HTTP:** each route against a seeded store, CORS preflight included.
- **Live:** under the user's recorder against the engine on Eddie, then the same with the P25
  fine-tune, comparing the two on the same hundred calls.

## Left to the builder

- Exact resampler (a polyphase or windowed-sinc; quality matters less than not adding a
  dependency).
- The default hallucination list.
- Whether the FTS5 table is kept in sync by triggers or by the writer.
- The built-in test sample for the **Test the engine** button.
- The SSE keep-alive and reconnect details.

## Open questions (for the owner)

Settled by the conversation, listed so they can be checked while reading:

- **Name and id:** `transcribe`. The registry's ids are flat (`notify`, `mqtt`, `openmhz`), a
  maintainer checks that "the id is fitting", and nothing transcription-shaped is listed yet
  (checked 2026-10-28). The generic name is honest here: this is the engine-agnostic plugin, so an
  engine-specific one would be the one that needs a qualifier. The repo follows the registry
  convention, `trunk-reporter/trunk-plugin-transcribe`, and lives locally at
  `D:\trunk-plugin-transcribe`. The executable inside the release archive is named after the id.
- **Engine, not model:** the user's decision (OpenAI-compatible API, user's choice of engine).
- **Reference engine:** the user's own `qwen3-asr-server`, running now on Eddie on port 8766 with
  vanilla Qwen3-ASR-1.7B, started by hand; its systemd unit points at a folder that does not exist
  and port 8765 is held by tailscaled. Making it a service there is the user's to do, or a one-line
  task if asked.
- **Own HTTP side until `annotations` exists upstream:** my call, following the pattern
  trunk-reporter already uses.
- **`failed` when the engine is the reason, `skipped` when the call is:** my call, after the
  reviewer found the first draft said both. It hands the files question to TRP's own
  **Keep when a plugin fails** switch instead of deciding it here. If you would rather the plugin
  never caused files to be kept, every `failed` in the call path becomes `skipped` and the
  archive can only reach calls whose files TRP keeps anyway.
- **M4A posted undecoded, no AAC in the plugin:** my call, keeping the binary small; the cost is
  no RMS gate on M4A-only archive calls.
- **Re-transcribe compares the engine's reported model name:** my call; both names are stored.
- **The HTTP shapes are fixed here, not by the old IRC page:** my call; the page is being ported
  anyway.
