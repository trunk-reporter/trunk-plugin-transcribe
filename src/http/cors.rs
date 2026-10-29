//! CORS for the origins an operator lists.

use axum::extract::{Request, State};
use axum::http::header::{
    ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_MAX_AGE, HeaderValue, ORIGIN, VARY,
};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::middleware::Next;
use axum::response::Response;

use super::AppState;

fn add_headers(headers: &mut HeaderMap, origin: HeaderValue) {
    headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    headers.append(VARY, HeaderValue::from_static("Origin"));
    headers.insert(ACCESS_CONTROL_ALLOW_HEADERS, HeaderValue::from_static("Last-Event-ID, Content-Type"));
    headers.insert(ACCESS_CONTROL_ALLOW_METHODS, HeaderValue::from_static("GET, OPTIONS"));
    headers.insert(ACCESS_CONTROL_MAX_AGE, HeaderValue::from_static("600"));
}

/// Adds CORS headers when the request's `Origin` is listed (`*` lists every
/// origin, echoed, never sent as `*`); answers a listed origin's `OPTIONS` with 204.
pub async fn cors(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let origin = req.headers().get(ORIGIN).filter(|o| o.to_str().is_ok_and(|o| state.origins.iter().any(|allowed| allowed == "*" || allowed == o))).cloned();
    let Some(origin) = origin else { return next.run(req).await };
    let mut res = if req.method() == Method::OPTIONS {
        let mut res = Response::new(axum::body::Body::empty());
        *res.status_mut() = StatusCode::NO_CONTENT;
        res
    } else {
        next.run(req).await
    };
    add_headers(res.headers_mut(), origin);
    res
}

#[cfg(test)]
mod tests {
    use axum::http::{Method, StatusCode};

    use super::super::router;
    use super::super::testing::{get, send, state};

    #[tokio::test]
    async fn cors_answers_listed_origins_only() {
        let mut s = state();
        s.origins = vec!["http://a:1".into()];
        let app = router(s.clone());
        let (status, h, _) = get(app.clone(), "/api/v1/health", Some("http://a:1")).await;
        assert_eq!((status, h["access-control-allow-origin"].to_str().unwrap(), h["vary"].to_str().unwrap()), (StatusCode::OK, "http://a:1", "Origin"));
        assert_eq!(h["access-control-allow-headers"], "Last-Event-ID, Content-Type");
        assert_eq!(h["access-control-allow-methods"], "GET, OPTIONS");
        let (status, h, _) = send(app.clone(), Method::OPTIONS, "/api/v1/events", Some("http://a:1")).await;
        assert_eq!((status, h["access-control-allow-origin"].to_str().unwrap()), (StatusCode::NO_CONTENT, "http://a:1"));
        let (status, h, _) = get(app.clone(), "/api/v1/health", Some("http://b:2")).await;
        assert_eq!((status, h.get("access-control-allow-origin")), (StatusCode::OK, None));
        let (_, h, _) = get(app, "/api/v1/health", None).await;
        assert_eq!(h.get("access-control-allow-origin"), None);
        s.origins = vec!["*".into()];
        let (_, h, _) = get(router(s), "/api/v1/health", Some("http://b:2")).await;
        assert_eq!(h["access-control-allow-origin"], "http://b:2");
    }
}
