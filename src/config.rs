//! The plugin's settings: the form is drawn from these structs' schema.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The settings-form button that posts a sample to the engine.
pub const ACTION_TEST: &str = "test";
/// The settings-form button that queues calls transcribed by another model.
pub const ACTION_RETRANSCRIBE: &str = "retranscribe";

/// The plugin's settings.
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
#[schemars(extend("x-actions" = [
    {"id": "test", "label": "Test the engine", "hint": "Posts a one-second sample and reports the model and the latency"},
    {"id": "retranscribe", "label": "Re-transcribe with the current model", "hint": "Queues calls whose transcript came from another model"}
]))]
pub struct Config {
    /// Engine
    pub engine: EngineConfig,
    /// Gate
    pub gate: GateConfig,
    /// Audio preparation
    pub prepare: PrepareConfig,
    /// Prompt
    pub prompt: PromptConfig,
    /// Filter
    pub filter: FilterConfig,
    /// Queue
    pub queue: QueueConfig,
    /// Archive
    pub archive: ArchiveConfig,
    /// Store
    pub store: StoreConfig,
    /// HTTP
    pub http: HttpConfig,
}

/// Settings for each system.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct SystemConfig {
    /// Transcribe this system
    ///
    /// Turn off to leave this system's calls untranscribed.
    pub enabled: bool,
}

impl Default for SystemConfig {
    fn default() -> Self {
        SystemConfig { enabled: true }
    }
}

/// The speech-to-text engine.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct EngineConfig {
    /// Engine URL
    ///
    /// Where the engine answers, e.g. http://eddie:8766. The plugin posts to /v1/audio/transcriptions under it.
    #[schemars(extend("x-required" = true))]
    pub url: String,
    /// Model
    ///
    /// The model to ask for; "default" lets the engine choose.
    pub model: String,
    /// API key
    ///
    /// Sent as a bearer token, if the engine wants one.
    #[schemars(extend("x-secret" = true))]
    pub api_key: Option<String>,
    /// Language
    ///
    /// The language spoken, as a two-letter code.
    pub language: String,
    /// Timeout (seconds)
    ///
    /// How long to wait for the engine to answer one call (5 to 600).
    pub timeout_seconds: u64,
    /// Concurrency
    ///
    /// How many calls to send at once (1 to 8).
    pub concurrency: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig { url: String::new(), model: "default".into(), api_key: None, language: "en".into(), timeout_seconds: 60, concurrency: 2 }
    }
}

/// Which calls are worth sending.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct GateConfig {
    /// Minimum loudness (RMS)
    ///
    /// Calls quieter than this are skipped as silence.
    pub rms: f32,
    /// Minimum length (seconds)
    ///
    /// Calls shorter than this are skipped.
    pub min_seconds: f64,
    /// Skip encrypted calls
    ///
    /// Encrypted calls are noise; leave them out.
    pub skip_encrypted: bool,
}

impl Default for GateConfig {
    fn default() -> Self {
        GateConfig { rms: 0.01, min_seconds: 1.0, skip_encrypted: true }
    }
}

/// How the audio is prepared.
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct PrepareConfig {
    /// P25 filter
    ///
    /// Filter the audio for P25 vocoder artifacts before sending it.
    pub p25_filter: bool,
}

/// The prompt that primes the engine.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct PromptConfig {
    /// Send a prompt
    ///
    /// Prime the engine with the talkgroup's name and tag.
    pub enabled: bool,
    /// Extra prompt text
    ///
    /// Added to every prompt, e.g. local place names and codes.
    #[schemars(extend("x-multiline" = true))]
    pub extra: String,
}

impl Default for PromptConfig {
    fn default() -> Self {
        PromptConfig { enabled: true, extra: String::new() }
    }
}

/// Throwing out what the engine makes up.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct FilterConfig {
    /// Most words per second
    ///
    /// A transcript with more words than this per second of audio is discarded.
    pub max_words_per_second: f64,
    /// Most repeats
    ///
    /// A word repeated more times than this in a row discards the transcript.
    pub max_repeat: usize,
    /// Known hallucinations
    ///
    /// Transcripts that are only one of these phrases are discarded.
    pub hallucinations: Vec<String>,
}

impl Default for FilterConfig {
    fn default() -> Self {
        let hallucinations = [
            "Thank you for watching",
            "Thanks for watching",
            "Thank you for listening",
            "Thank you",
            "Thanks",
            "Please subscribe",
            "Subtitles by the Amara.org community",
            "Subscribe to my channel",
            "Bye",
            "Goodbye",
            "you",
            "The end",
            "Music",
            "Applause",
        ];
        FilterConfig { max_words_per_second: 8.0, max_repeat: 4, hallucinations: hallucinations.map(String::from).to_vec() }
    }
}

/// The queue of calls waiting for the engine.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct QueueConfig {
    /// Queue size
    ///
    /// How many calls may wait for the engine (at least 10). The oldest are dropped beyond it.
    pub max: usize,
}

impl Default for QueueConfig {
    fn default() -> Self {
        QueueConfig { max: 2000 }
    }
}

/// Catching up on calls recorded while the plugin was off.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct ArchiveConfig {
    /// Archive days
    ///
    /// How far back to look for calls without a transcript (at least 1).
    pub days: u32,
    /// Scan every (minutes)
    ///
    /// How often to look.
    pub every_minutes: u32,
    /// Re-transcribe everything
    ///
    /// Also queue calls that already have a transcript.
    pub retranscribe_everything: bool,
}

impl Default for ArchiveConfig {
    fn default() -> Self {
        ArchiveConfig { days: 7, every_minutes: 10, retranscribe_everything: false }
    }
}

/// The transcript store.
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct StoreConfig {
    /// Keep transcripts for (days)
    ///
    /// Older transcripts are deleted. 0 keeps them for ever.
    pub retention_days: u32,
}

/// The plugin's own web server.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct HttpConfig {
    /// Port
    ///
    /// Serve the transcripts on this port. Empty for no server.
    pub port: Option<u16>,
    /// Bind address
    ///
    /// The address to listen on; 127.0.0.1 keeps it to this machine.
    pub bind: String,
    /// Allowed origins
    ///
    /// Web pages from these origins may call the API.
    pub origins: Vec<String>,
}

impl Default for HttpConfig {
    fn default() -> Self {
        HttpConfig { port: None, bind: "127.0.0.1".into(), origins: Vec::new() }
    }
}

impl Config {
    /// Whether the settings can be run with; the message names the field as the form shows it.
    pub fn validate(&self) -> Result<(), String> {
        let e = &self.engine;
        if e.url.is_empty() {
            return Err("Engine URL is required".into());
        }
        if !(e.url.starts_with("http://") || e.url.starts_with("https://")) {
            return Err(format!("Engine URL must start with http:// or https:// (it is {})", e.url));
        }
        if !(1..=8).contains(&e.concurrency) {
            return Err(format!("Concurrency must be 1 to 8 (it is {})", e.concurrency));
        }
        if !(5..=600).contains(&e.timeout_seconds) {
            return Err(format!("Timeout (seconds) must be 5 to 600 (it is {})", e.timeout_seconds));
        }
        if self.queue.max < 10 {
            return Err(format!("Queue size must be at least 10 (it is {})", self.queue.max));
        }
        if self.archive.days < 1 {
            return Err("Archive days must be at least 1".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
