## Unreleased

- Choosing talkgroups: `gate.talkgroups` and `gate.categories` (each an allowlist or a denylist) decide which calls are
  transcribed, by talkgroup number or by the talkgroup's tag or group. Checked before the audio is read; calls left out
  are recorded as skipped with the reason. Per system through the usual overrides. ([#1](https://github.com/trunk-reporter/trunk-plugin-transcribe/issues/1))

## [0.1.0]

- The live path: each concluded call is gated, prepared (optional P25 filter) and sent to an OpenAI-style transcription engine.
- The archive pass catches up on calls without a transcript, and re-transcribes calls made by another model.
- A SQLite store of transcripts with per-radio segments, full-text search and retention.
- An HTTP API with server-sent events, health and CORS for web interfaces.
- Live settings, with the buttons "Test the engine" and "Re-transcribe with the current model".
