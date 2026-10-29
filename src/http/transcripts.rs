//! `/api/v1/transcripts`: by paths, by page, and one by path; and the query
//! parsing the search shares.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

use super::AppState;
use crate::store::{self, Transcript};

/// The most paths one `?paths=` looks up.
const MAX_PATHS: usize = 200;
/// A page's `limit` when none is given, and the bounds it is clamped to.
const DEFAULT_LIMIT: i64 = 100;
const MAX_LIMIT: i64 = 500;

pub(crate) type Params = HashMap<String, String>;

/// `{ "error": why }` with `status`.
pub(crate) fn error(status: StatusCode, why: &str) -> Response {
    (status, Json(json!({ "error": why }))).into_response()
}

/// A store failure, as a 500.
pub(crate) fn failed(why: String) -> Response {
    error(StatusCode::INTERNAL_SERVER_ERROR, &why)
}

/// A parameter's trimmed value; an empty one counts as not given.
pub(crate) fn param<'a>(q: &'a Params, name: &str) -> Option<&'a str> {
    q.get(name).map(|v| v.trim()).filter(|v| !v.is_empty())
}

/// A numeric parameter; one that does not parse is `Err("bad <name>")`, for a 400.
pub(crate) fn number<T: FromStr>(q: &Params, name: &str) -> Result<Option<T>, String> {
    param(q, name).map(|v| v.parse().map_err(|_| format!("bad {name}"))).transpose()
}

/// `system`, `talkgroup`, `from`, `to`, `before` and `limit` (default 100,
/// clamped to 1–500); `Err` is the 400's message.
pub(crate) fn query(q: &Params) -> Result<store::Query, String> {
    Ok(store::Query {
        system: param(q, "system").map(String::from),
        talkgroup: number(q, "talkgroup")?,
        from: number(q, "from")?,
        to: number(q, "to")?,
        before: number(q, "before")?,
        limit: number::<i64>(q, "limit")?.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT) as usize,
    })
}

/// `?paths=a,b,c` (in the order given), or a page newest first.
pub async fn list(State(state): State<AppState>, Query(q): Query<Params>) -> Response {
    if let Some(paths) = param(&q, "paths") {
        let mut seen = HashSet::new();
        let paths: Vec<String> = paths.split(',').map(str::trim).filter(|p| !p.is_empty() && seen.insert(*p)).take(MAX_PATHS).map(String::from).collect();
        return match state.store.get_many(&paths) {
            Ok(rows) => {
                let transcripts: Vec<Transcript> = rows.iter().map(|(r, s)| Transcript::from_row(r, s)).collect();
                Json(json!({ "transcripts": transcripts })).into_response()
            }
            Err(e) => failed(e),
        };
    }
    let query = match query(&q) {
        Ok(query) => query,
        Err(why) => return error(StatusCode::BAD_REQUEST, &why),
    };
    match state.store.page(&query) {
        Ok(page) => {
            let transcripts: Vec<Transcript> = page.rows.iter().map(|(r, s)| Transcript::from_row(r, s)).collect();
            Json(json!({ "transcripts": transcripts, "more": page.more, "before": page.before })).into_response()
        }
        Err(e) => failed(e),
    }
}

/// One transcript by its path, or 404.
pub async fn one(State(state): State<AppState>, Path(path): Path<String>) -> Response {
    match state.store.get(&path) {
        Ok(Some((row, segments))) => Json(Transcript::from_row(&row, &segments)).into_response(),
        Ok(None) => error(StatusCode::NOT_FOUND, "no transcript"),
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
    async fn by_paths_in_order_trimmed_deduplicated_and_capped() {
        let s = state();
        for (i, p) in ["a", "b", "c"].iter().enumerate() {
            s.store.put(&row(p, 1000 * i as i64, "t"), &[]).unwrap();
        }
        let (status, _, json) = get(router(s.clone()), "/api/v1/transcripts?paths=%20c%20,,a,c,zz", None).await;
        assert_eq!(status, StatusCode::OK);
        let paths: Vec<&str> = json["transcripts"].as_array().unwrap().iter().map(|t| t["path"].as_str().unwrap()).collect();
        assert_eq!(paths, ["c", "a"]);
        assert_eq!(json.as_object().unwrap().len(), 1, "{json}");

        // 250 distinct paths, all stored: only the first 200 are looked up.
        let s = state();
        let many: Vec<String> = (0..250).map(|i| format!("p{i}")).collect();
        for p in &many {
            s.store.put(&row(p, 1000, "t"), &[]).unwrap();
        }
        let (_, _, json) = get(router(s), &format!("/api/v1/transcripts?paths={}", many.join(",")), None).await;
        let got = json["transcripts"].as_array().unwrap();
        assert_eq!((got.len(), got[0]["path"].clone(), got[199]["path"].clone()), (200, json!("p0"), json!("p199")));
    }

    #[tokio::test]
    async fn one_by_path_and_404() {
        let s = state();
        s.store.put(&row("sys1/2026/10/27/101-1", 1000, "t"), &[]).unwrap();
        let (status, _, json) = get(router(s.clone()), "/api/v1/transcripts/sys1/2026/10/27/101-1", None).await;
        assert_eq!((status, json["path"].clone(), json["text"].clone()), (StatusCode::OK, json!("sys1/2026/10/27/101-1"), json!("t")));
        let (status, _, json) = get(router(s), "/api/v1/transcripts/nope", None).await;
        assert_eq!((status, json), (StatusCode::NOT_FOUND, json!({ "error": "no transcript" })));
    }

    #[tokio::test]
    async fn a_page_and_limit_is_clamped() {
        let s = state();
        for i in 0..7 {
            s.store.put(&row(&format!("p{i}"), 1000 * i, "t"), &[]).unwrap();
        }
        let app = router(s);
        let (status, _, json) = get(app.clone(), "/api/v1/transcripts?limit=3", None).await;
        let paths: Vec<&str> = json["transcripts"].as_array().unwrap().iter().map(|t| t["path"].as_str().unwrap()).collect();
        assert_eq!((status, paths, json["more"].clone(), json["before"].clone()), (StatusCode::OK, vec!["p6", "p5", "p4"], json!(true), json!(4000)));
        let (_, _, json) = get(app.clone(), "/api/v1/transcripts?limit=0", None).await;
        assert_eq!(json["transcripts"].as_array().unwrap().len(), 1);
        let (_, _, json) = get(app.clone(), "/api/v1/transcripts?limit=9999", None).await;
        assert_eq!((json["transcripts"].as_array().unwrap().len(), json["more"].clone()), (7, json!(false)));
        let (_, _, json) = get(app.clone(), "/api/v1/transcripts?limit=&before=4000&system=sys1", None).await;
        assert_eq!(json["transcripts"][0]["path"], "p3");
        let (status, _, json) = get(app.clone(), "/api/v1/transcripts?limit=x", None).await;
        assert_eq!((status, json), (StatusCode::BAD_REQUEST, json!({ "error": "bad limit" })));
        let (status, _, json) = get(app, "/api/v1/transcripts?talkgroup=-1", None).await;
        assert_eq!((status, json), (StatusCode::BAD_REQUEST, json!({ "error": "bad talkgroup" })));
    }
}
