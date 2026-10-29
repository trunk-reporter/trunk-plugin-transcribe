//! `/api/v1/search`: full-text search over the transcripts.

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

use super::AppState;
use super::transcripts::{Params, error, failed, param, query};
use crate::store::Transcript;

/// `?q=` plus the page's filters: each hit is a transcript with its `snippet`.
pub async fn search(State(state): State<AppState>, Query(q): Query<Params>) -> Response {
    let Some(text) = param(&q, "q") else { return error(StatusCode::BAD_REQUEST, "bad q") };
    let query = match query(&q) {
        Ok(query) => query,
        Err(why) => return error(StatusCode::BAD_REQUEST, &why),
    };
    match state.store.search(text, &query) {
        Ok((found, more, before)) => {
            let hits: Vec<Value> = found
                .iter()
                .map(|(row, segments, snippet)| {
                    let mut hit = serde_json::to_value(Transcript::from_row(row, segments)).unwrap_or_default();
                    if let Value::Object(fields) = &mut hit {
                        fields.insert("snippet".into(), Value::String(snippet.clone()));
                    }
                    hit
                })
                .collect();
            Json(json!({ "hits": hits, "more": more, "before": before })).into_response()
        }
        Err(e) => failed(e),
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use serde_json::json;

    use super::super::router;
    use super::super::testing::{get, row, state};

    #[tokio::test]
    async fn search_hits_carry_a_snippet_and_survive_fts_syntax() {
        let s = state();
        s.store.put(&row("a", 1000, "Engine 3 responding to the structure fire"), &[]).unwrap();
        s.store.put(&row("b", 2000, "all quiet"), &[]).unwrap();
        let app = router(s);
        let (status, _, json) = get(app.clone(), "/api/v1/search?q=structure", None).await;
        assert_eq!((status, json["more"].clone(), json["before"].clone()), (StatusCode::OK, json!(false), json!(1000)));
        let hits = json["hits"].as_array().unwrap();
        assert_eq!((hits.len(), hits[0]["path"].clone(), hits[0]["talkgroup"].clone()), (1, json!("a"), json!(101)));
        assert!(hits[0]["snippet"].as_str().unwrap().contains("<b>structure</b>"), "{json}");
        for uri in ["/api/v1/search?q=", "/api/v1/search?q=%20%20", "/api/v1/search"] {
            let (status, _, json) = get(app.clone(), uri, None).await;
            assert_eq!((status, json), (StatusCode::BAD_REQUEST, json!({ "error": "bad q" })), "{uri}");
        }
        let (status, _, _) = get(app.clone(), "/api/v1/search?q=%22", None).await;
        assert_eq!(status, StatusCode::OK);
        let (status, _, json) = get(app, "/api/v1/search?q=fire&from=x", None).await;
        assert_eq!((status, json), (StatusCode::BAD_REQUEST, json!({ "error": "bad from" })));
    }
}
