//! The engine client: the OpenAI transcription route over HTTP.

use std::time::Duration;

use serde::Deserialize;
use trunk_recorder_plugin::Multipart;

use crate::config::EngineConfig;
pub use crate::segment::Word;

/// A blocking client for an OpenAI-style speech-to-text engine.
#[derive(Clone)]
pub struct Engine {
    client: reqwest::blocking::Client,
}

/// The audio to send, with the file name and type the engine sees.
pub struct Audio<'a> {
    pub bytes: &'a [u8],
    pub file_name: &'a str,
    pub content_type: &'a str,
}

/// What the engine said.
#[derive(Debug)]
pub struct Answer {
    pub text: String,
    pub language: Option<String>,
    pub words: Vec<Word>,
    pub model: Option<String>,
    /// The body as received.
    pub raw: String,
}

/// Why a request failed: worth trying again, or not.
#[derive(Debug)]
pub enum EngineError {
    Retry(String),
    Fail(String),
}

#[derive(Deserialize)]
struct Wire {
    text: String,
    #[serde(default)]
    language: Option<String>,
    #[serde(default)]
    words: Vec<Word>,
    #[serde(default)]
    model: Option<String>,
}

#[derive(Deserialize)]
struct Models {
    #[serde(default)]
    data: Vec<ModelEntry>,
}

#[derive(Deserialize)]
struct ModelEntry {
    id: String,
}

impl Engine {
    pub fn new(timeout: Duration) -> Engine {
        let client = reqwest::blocking::Client::builder().timeout(timeout).build().expect("an HTTP client");
        Engine { client }
    }

    /// `POST {url}/v1/audio/transcriptions` with `audio`.
    pub fn transcribe(&self, cfg: &EngineConfig, audio: Audio, language: &str, prompt: &str) -> Result<Answer, EngineError> {
        let mut form = Multipart::new()
            .file("file", audio.file_name, audio.content_type, audio.bytes)
            .text("model", &cfg.model)
            .text("language", language)
            .text("response_format", "verbose_json")
            .text("timestamp_granularities[]", "word");
        if !prompt.is_empty() {
            form = form.text("prompt", prompt);
        }
        let (body, content_type) = form.finish();
        let request = self.client.post(route(cfg, "/v1/audio/transcriptions")).header("Content-Type", content_type).body(body);
        let raw = read(authorize(request, cfg))?;
        let wire: Wire = serde_json::from_str(&raw).map_err(|e| EngineError::Fail(format!("unreadable answer: {e}")))?;
        Ok(Answer { text: wire.text, language: wire.language, words: wire.words, model: wire.model, raw })
    }

    /// `GET {url}/v1/models`: the first model's id, or `None` when the engine has no list.
    pub fn models(&self, cfg: &EngineConfig) -> Result<Option<String>, EngineError> {
        let request = authorize(self.client.get(route(cfg, "/v1/models")), cfg);
        let raw = match read(request) {
            Ok(raw) => raw,
            Err(EngineError::Fail(m)) if m.starts_with("HTTP 404") => return Ok(None),
            Err(e) => return Err(e),
        };
        let models: Models = serde_json::from_str(&raw).map_err(|e| EngineError::Fail(format!("unreadable answer: {e}")))?;
        Ok(models.data.into_iter().next().map(|m| m.id))
    }
}

/// `host` or `host:port` of `url`; the whole string when it does not parse.
pub fn host_of(url: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(u) => match (u.host_str(), u.port()) {
            (Some(h), Some(p)) => format!("{h}:{p}"),
            (Some(h), None) => h.to_string(),
            _ => url.to_string(),
        },
        Err(_) => url.to_string(),
    }
}

fn route(cfg: &EngineConfig, path: &str) -> String {
    format!("{}{path}", cfg.url.trim_end_matches('/'))
}

fn authorize(request: reqwest::blocking::RequestBuilder, cfg: &EngineConfig) -> reqwest::blocking::RequestBuilder {
    match cfg.api_key.as_deref() {
        Some(key) if !key.is_empty() => request.bearer_auth(key),
        _ => request,
    }
}

/// Send, and return the body of a 2xx answer.
fn read(request: reqwest::blocking::RequestBuilder) -> Result<String, EngineError> {
    let response = request.send().map_err(failed)?;
    let status = response.status();
    let body = response.text().map_err(failed)?;
    if status.is_success() {
        Ok(body)
    } else if status.is_server_error() || status.as_u16() == 429 {
        Err(EngineError::Retry(format!("HTTP {}: {}", status.as_u16(), clip(&body))))
    } else {
        Err(EngineError::Fail(format!("HTTP {}: {}", status.as_u16(), clip(&body))))
    }
}

fn failed(e: reqwest::Error) -> EngineError {
    if e.is_builder() { EngineError::Fail(e.to_string()) } else { EngineError::Retry(e.to_string()) }
}

fn clip(s: &str) -> String {
    s.chars().take(200).collect()
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use trunk_recorder_plugin::testing::MockServer;

    use super::*;
    use crate::config::EngineConfig;

    fn cfg(url: &str) -> EngineConfig {
        EngineConfig { url: url.into(), model: "default".into(), api_key: Some("k1".into()), ..Default::default() }
    }

    const OK: &str = r#"{"task":"transcribe","language":"en","text":"Tower 73 go ahead","model":"Qwen3-ASR-1.7B","words":[{"word":"Tower","start":0.1,"end":0.4},{"word":"73","start":0.5,"end":0.9}]}"#;

    fn wav() -> Audio<'static> {
        const BYTES: &[u8] = b"RIFF";
        Audio { bytes: BYTES, file_name: "call.wav", content_type: "audio/wav" }
    }

    #[test]
    fn posts_the_openai_form_and_reads_the_answer() {
        let s = MockServer::start(|_| (200, OK.into()));
        let a = Engine::new(Duration::from_secs(5)).transcribe(&cfg(&format!("{}/", s.url())), wav(), "en", "Fire Dispatch").unwrap();
        assert_eq!((a.text.as_str(), a.language.as_deref(), a.model.as_deref(), a.words.len()), ("Tower 73 go ahead", Some("en"), Some("Qwen3-ASR-1.7B"), 2));
        assert_eq!(a.words[1], Word { word: "73".into(), start: 0.5, end: 0.9 });
        assert_eq!(a.raw, OK);
        let r = &s.requests()[0];
        assert_eq!((r.method.as_str(), r.path.as_str()), ("POST", "/v1/audio/transcriptions"));
        assert_eq!(r.header("authorization"), Some("Bearer k1"));
        assert_eq!(r.form_field("file").unwrap(), b"RIFF");
        assert_eq!(r.form_file_name("file").unwrap(), "call.wav");
        for (k, v) in
            [("model", "default"), ("language", "en"), ("response_format", "verbose_json"), ("timestamp_granularities[]", "word"), ("prompt", "Fire Dispatch")]
        {
            assert_eq!(r.form_field(k).unwrap(), v.as_bytes(), "{k}");
        }
    }

    #[test]
    fn no_key_no_header_no_prompt_no_field() {
        let s = MockServer::start(|_| (200, OK.into()));
        let c = EngineConfig { api_key: None, ..cfg(s.url()) };
        Engine::new(Duration::from_secs(5)).transcribe(&c, wav(), "en", "").unwrap();
        let r = &s.requests()[0];
        assert_eq!(r.header("authorization"), None);
        assert_eq!(r.form_field("prompt"), None);
        let c = EngineConfig { api_key: Some(String::new()), ..cfg(s.url()) };
        Engine::new(Duration::from_secs(5)).transcribe(&c, wav(), "en", "").unwrap();
        assert_eq!(s.requests()[1].header("authorization"), None);
    }

    #[test]
    fn errors_are_classed_for_retry_or_not() {
        for (code, retry) in [(500, true), (503, true), (429, true), (400, false), (401, false), (413, false)] {
            let s = MockServer::start(move |_| (code, "nope".into()));
            let e = Engine::new(Duration::from_secs(5)).transcribe(&cfg(s.url()), wav(), "en", "").unwrap_err();
            assert_eq!(matches!(e, EngineError::Retry(_)), retry, "{code}");
            if !retry {
                assert!(matches!(&e, EngineError::Fail(m) if m.starts_with(&format!("HTTP {code}"))));
            }
        }
        let s = MockServer::start(|_| (200, "not json".into()));
        assert!(
            matches!(Engine::new(Duration::from_secs(5)).transcribe(&cfg(s.url()), wav(), "en", "").unwrap_err(), EngineError::Fail(m) if m.starts_with("unreadable answer"))
        );
        assert!(matches!(Engine::new(Duration::from_secs(5)).transcribe(&cfg("http://127.0.0.1:1"), wav(), "en", "").unwrap_err(), EngineError::Retry(_)));
    }

    #[test]
    fn a_hang_past_the_timeout_is_a_retry() {
        let s = MockServer::start(|_| {
            std::thread::sleep(Duration::from_millis(800));
            (200, OK.into())
        });
        let t0 = Instant::now();
        assert!(matches!(Engine::new(Duration::from_millis(200)).transcribe(&cfg(s.url()), wav(), "en", "").unwrap_err(), EngineError::Retry(_)));
        assert!(t0.elapsed() < Duration::from_millis(700));
    }

    #[test]
    fn models_names_the_first_or_none() {
        let s = MockServer::start(|r| {
            if r.path == "/v1/models" { (200, r#"{"object":"list","data":[{"id":"Qwen3-ASR-1.7B"},{"id":"other"}]}"#.into()) } else { (404, "".into()) }
        });
        assert_eq!(Engine::new(Duration::from_secs(5)).models(&cfg(s.url())).unwrap(), Some("Qwen3-ASR-1.7B".into()));
        let s = MockServer::start(|_| (404, "".into()));
        assert_eq!(Engine::new(Duration::from_secs(5)).models(&cfg(s.url())).unwrap(), None);
        let s = MockServer::start(|_| (200, r#"{"object":"list"}"#.into()));
        assert_eq!(Engine::new(Duration::from_secs(5)).models(&cfg(s.url())).unwrap(), None);
        assert_eq!(host_of("http://eddie:8766/"), "eddie:8766");
        assert_eq!(host_of("http://eddie/x"), "eddie");
        assert_eq!(host_of("not a url"), "not a url");
    }
}
