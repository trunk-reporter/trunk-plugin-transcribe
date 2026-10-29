## [0.1.0]

- The live path: each concluded call is gated, prepared (optional P25 filter) and sent to an OpenAI-style transcription engine.
- The archive pass catches up on calls without a transcript, and re-transcribes calls made by another model.
- A SQLite store of transcripts with per-radio segments, full-text search and retention.
- An HTTP API with server-sent events, health and CORS for web interfaces.
- Live settings, with the buttons "Test the engine" and "Re-transcribe with the current model".
