# Transcribe

A Trunk Recorder Pro plugin that sends every recorded call to a speech-to-text engine, keeps the transcripts in a
searchable store and serves them over HTTP, as a list, a full-text search and a live event stream. When the engine gives
word times, the transcript is split by radio.

## Engines

The plugin speaks the OpenAI audio transcription API: `POST /v1/audio/transcriptions`, multipart, with `file`, `model`,
`language`, `response_format=verbose_json`, `timestamp_granularities[]=word` and, if a prompt is sent, `prompt`. It sends
`Authorization: Bearer` when `engine.apiKey` is set.

| Engine | State |
| --- | --- |
| [trunk-reporter/qwen3-asr-server](https://github.com/trunk-reporter/qwen3-asr-server) | Works. The plugin was developed against it. |
| Speaches, LocalAI, faster-whisper-server | Expected to work. Not tested. |
| OpenAI's hosted API (model `whisper-1`) | Expected to work. Not tested. It costs money, and your calls leave the LAN. |

Only the first was run against the plugin.

## Installing

From the Trunk Recorder Pro plugin registry, once the plugin is listed there. Or download a release archive,
`transcribe-<version>-<target>.tar.gz` (`.zip` on Windows), and install it through the recorder's **Install from file**.

Then open the plugin's settings and set the engine URL; nothing runs without it.

## Settings

Settings are applied while the recorder runs, except the five marked (restart) below: `engine.concurrency`, `queue.max`,
`http.port`, `http.bind` and `http.origins` take effect when recording next starts (the plugin logs a warning). Keys are nested under the group names below;
[`examples/settings.json`](examples/settings.json) is a complete settings object.

| Key | Default | |
| --- | --- | --- |
| `engine.url` | empty | Required. Where the engine answers, `http://` or `https://`, e.g. `http://eddie:8766`. |
| `engine.model` | `default` | The model to ask for; `default` lets the engine choose. |
| `engine.apiKey` | none | Sent as a bearer token, if the engine wants one. |
| `engine.language` | `en` | The language spoken, as a two-letter code. |
| `engine.timeoutSeconds` | `60` | How long to wait for the engine to answer one call (5 to 600). |
| `engine.concurrency` (restart) | `2` | How many calls to send at once (1 to 8). |
| `gate.rms` | `0.01` | Calls quieter than this are skipped as silence. |
| `gate.minSeconds` | `1.0` | Calls shorter than this are skipped. |
| `gate.skipEncrypted` | `true` | Leave encrypted calls out. |
| `prepare.p25Filter` | `false` | Filter the audio for P25 vocoder artifacts before sending it. |
| `prompt.enabled` | `true` | Prime the engine with the talkgroup's name and tag. |
| `prompt.extra` | empty | Added to every prompt, e.g. local place names and codes. |
| `filter.maxWordsPerSecond` | `8.0` | A transcript with more words per second of audio than this is discarded. |
| `filter.maxRepeat` | `4` | A word repeated more times than this in a row discards the transcript. |
| `filter.hallucinations` | 14 phrases | Transcripts that are only one of these phrases are discarded ("Thank you for watching", "Bye", "Music", ...). |
| `queue.max` (restart) | `2000` | How many calls may wait for the engine (at least 10). The oldest are dropped beyond it. |
| `archive.days` | `7` | How far back to look for calls without a transcript (at least 1). |
| `archive.everyMinutes` | `10` | How often to look. |
| `archive.retranscribeEverything` | `false` | Make the re-transcribe button ignore `archive.days`. Read only by the button. |
| `store.retentionDays` | `0` | Older transcripts are deleted. 0 keeps them for ever. |
| `http.port` (restart) | none | Serve the transcripts on this port. Empty for no server. |
| `http.bind` (restart) | `127.0.0.1` | The address to listen on; `127.0.0.1` keeps it to this machine. |
| `http.origins` (restart) | none | Web pages from these origins may call the API (see CORS). |

Two buttons sit on the settings form:

- **Test the engine** posts a one-second sample and reports the model and the latency.
- **Re-transcribe with the current model** queues calls whose transcript came from another model.

Per system: `enabled` (default on) turns transcribing off for one system; its calls are recorded as skipped.

## What happens to a call

When a call concludes, the plugin:

1. Gates it: encrypted (if `gate.skipEncrypted`), shorter than `gate.minSeconds` or quieter than `gate.rms` is skipped.
2. Converts the recorder's WAV to 16 kHz mono WAV, through the P25 filter chain if `prepare.p25Filter` is on. An M4A is
   posted as it is.
3. Posts it to `/v1/audio/transcriptions` with `verbose_json` and word timestamps, and the prompt if enabled.
4. Filters the answer: too many words per second, a word repeated too often, or a known hallucination is rejected.
5. Stores the transcript and announces it on `/api/v1/events`.

The engine's model is asked from `GET /v1/models` (the one named by `engine.model` when the engine lists it, else the
engine's only model) and stored on each transcript; changing `engine.url` or `engine.model` asks again.

## The archive pass and re-transcribing

When the plugin starts, and then every `archive.everyMinutes`, it checks the last `archive.days` days of the recorder's
call store and queues every call that has no transcript. Archive work never delays live calls. A pass queues at most
half of `queue.max`; past that it pauses, and the next pass goes on with the rest. A pass whose page does not come from
the recorder within 60 seconds ends with a warning.

After you change the engine's model, press **Re-transcribe with the current model**. It queues the transcripts with status
`ok`, `empty` or `rejected` whose recorded model differs from the engine's current model (the one named by
`engine.model` when the engine lists it, else the engine's only model, else the configured name). A transcript with no
model reported by the engine counts as made by the model it asked for. Failed and skipped calls are never selected, and
a re-transcribe that fails or is skipped leaves the old transcript as it was (the log says so). Only calls within the last `archive.days` days
are considered; tick `archive.retranscribeEverything` to drop that window. The box is read only by the button, not by the
periodic pass.

## Statuses

Each call has one transcript record, with a `status` and sometimes a `reason`:

| Status | Meaning |
| --- | --- |
| `ok` | Text is stored, with segments per radio when the engine gave word times. |
| `skipped` | Not sent. Reason: `encrypted`, `too short`, `silent`, `system disabled` or `no audio`. |
| `empty` | The engine returned no words. |
| `rejected` | Thrown out by a filter; the raw answer is kept, the text is not. Reason: `implausible rate`, `repetition` or `hallucination`. |
| `failed` | Not transcribed. Reason: the engine's error, `gave up: ...`, `engine down`, `queue full` or `shutting down`. The archive pass retries it, up to 10 attempts. A call the archive pass queued but that was never tried gets no record and counts no attempt. |

On the recorder's plugin page the plugin shows **Ok** with the model name; **Warning** when the engine is degraded or the
queue is half full of live calls and retries (archive work queuing up is expected); and **Error** once the engine has been down for 5 minutes.

## HTTP API

Set `http.port` to turn the server on. All routes are `GET` and return JSON, except the event stream.

| Route | Returns |
| --- | --- |
| `/api/v1/transcripts?paths=a,b,c` | `{transcripts}` for up to 200 paths, in the order given. |
| `/api/v1/transcripts?system&talkgroup&from&to&before&limit` | `{transcripts, more, before}`, newest first. `limit` is 1 to 500 (default 100); `before` is a start time in ms, and the `before` returned is the cursor for the next page. |
| `/api/v1/transcripts/{path}` | One transcript; 404 `{"error":"no transcript"}` if there is none. |
| `/api/v1/search?q&...` | `{hits, more, before}`; each hit is a transcript plus a `snippet` with matches marked `<b>`. Takes the same filters as the list. |
| `/api/v1/events` | Server-sent events: `event: transcript` with the transcript as data and `id` its `transcribedMs`; `event: ping` every 15 s. Send `Last-Event-ID` (or `?lastEventId=`) to replay what you missed. |
| `/api/v1/health` | `{ok, engine:{url, model, up, lastOkMs, lastError}, queue, inFlight, transcribed24h}` |

A transcript:

```json
{
  "path": "sys1/2026/10/27/101-1761600000",
  "system": "sys1",
  "talkgroup": 101,
  "startMs": 1761600000000,
  "lengthMs": 4000,
  "status": "ok",
  "reason": null,
  "text": "Engine 3 responding to the structure fire",
  "language": "en",
  "model": "qwen3-asr",
  "transcribedMs": 1761600012345,
  "segments": [{ "unit": 4521, "start": 0.0, "end": 3.2, "text": "Engine 3 responding to the structure fire" }]
}
```

`start` and `end` are seconds into the call; `unit` is the radio's id, or null when unknown.

```js
const events = new EventSource('http://localhost:8767/api/v1/events');
events.addEventListener('transcript', (e) => console.log(JSON.parse(e.data)));
// the browser resends the last id on reconnect, so nothing is missed
```

**CORS.** `http.origins` lists the origins whose pages may call the API; `*` allows any. Methods are `GET` and `OPTIONS`.

**There is no authentication.** Keep the port on the LAN or behind Trunk Recorder Pro's interface port.

## Where the files are

`plugin-data/transcribe/transcripts.sqlite` under the recorder's data folder. If the file is corrupt, it is moved aside as
`transcripts.sqlite.corrupt-<time>` and a fresh one is started.

## Building

```
cargo build --release
cargo test
```

Needs Rust 1.89 or later.

## License

MIT
