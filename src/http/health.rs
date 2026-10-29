//! `/api/v1/health`: the engine, the queue, and the day's count.

use axum::Json;
use axum::extract::State;
use serde_json::{Value, json};

use super::AppState;

/// The [`super::Health`] the plugin keeps, always 200.
pub async fn health(State(state): State<AppState>) -> Json<Value> {
    let h = state.health.read().unwrap_or_else(|poisoned| poisoned.into_inner()).clone();
    Json(json!({
        "ok": h.up,
        "engine": { "url": h.engine_url, "model": h.model, "up": h.up, "lastOkMs": h.last_ok_ms, "lastError": h.last_error },
        "queue": h.queue,
        "inFlight": h.in_flight,
        "transcribed24h": h.transcribed_24h,
    }))
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use serde_json::json;

    use super::super::testing::{get, state};
    use super::super::{Health, router};

    #[tokio::test]
    async fn health_is_the_specs_shape() {
        let s = state();
        *s.health.write().unwrap() = Health {
            engine_url: "http://e:8766".into(),
            model: "m".into(),
            up: true,
            last_ok_ms: Some(5),
            last_error: String::new(),
            queue: 2,
            in_flight: 1,
            transcribed_24h: 40,
        };
        let (status, _, json) = get(router(s), "/api/v1/health", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            json,
            json!({"ok":true,"engine":{"url":"http://e:8766","model":"m","up":true,"lastOkMs":5,"lastError":""},"queue":2,"inFlight":1,"transcribed24h":40})
        );
    }
}
