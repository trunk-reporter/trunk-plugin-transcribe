//! The HTTP side: transcripts for web interfaces.

pub mod cors;
pub mod events;
pub mod health;
pub mod search;
pub mod transcripts;

use std::net::{IpAddr, SocketAddr};
use std::sync::mpsc;
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;
use std::time::Duration;

use axum::Router;
use axum::routing::get;
use tokio::sync::{broadcast, oneshot, watch};

use crate::store::{Store, Transcript};

/// What `/api/v1/health` reports; the plugin keeps it current.
#[derive(Clone, Debug, Default)]
pub struct Health {
    pub engine_url: String,
    pub model: String,
    pub up: bool,
    pub last_ok_ms: Option<i64>,
    pub last_error: String,
    pub queue: u64,
    pub in_flight: u64,
    pub transcribed_24h: u64,
}

/// What every handler shares.
#[derive(Clone)]
pub struct AppState {
    pub store: Store,
    /// Each stored transcript, announced.
    pub events: broadcast::Sender<Transcript>,
    pub health: Arc<RwLock<Health>>,
    /// Origins allowed by CORS; `*` allows any.
    pub origins: Vec<String>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/v1/transcripts", get(transcripts::list))
        .route("/api/v1/transcripts/{*path}", get(transcripts::one))
        .route("/api/v1/search", get(search::search))
        .route("/api/v1/events", get(events::events))
        .route("/api/v1/health", get(health::health))
        .layer(axum::middleware::from_fn_with_state(state.clone(), cors::cors))
        .with_state(state)
}

/// A running server.
pub struct ServerHandle {
    /// Ends the open event streams, which graceful shutdown would otherwise wait on forever.
    shutdown: watch::Sender<bool>,
    stop: oneshot::Sender<()>,
    done: mpsc::Receiver<()>,
    thread: JoinHandle<()>,
}

impl ServerHandle {
    /// Ends the event streams, stops taking connections and lets open requests
    /// finish for up to `within`; one still open then is left to the process's exit.
    pub fn stop(self, within: Duration) {
        let _ = self.shutdown.send(true);
        let _ = self.stop.send(());
        if self.done.recv_timeout(within).is_ok() {
            let _ = self.thread.join();
        }
    }
}

/// Listens on `bind:port` and serves `state` on a thread named `http`. The
/// socket is bound before this returns, so a taken port is the `Err`.
pub fn start(state: AppState, bind: &str, port: u16) -> Result<(ServerHandle, SocketAddr), String> {
    let ip: IpAddr = bind.trim().parse().map_err(|_| format!("the bind address {bind} is not an IP address"))?;
    let listener = std::net::TcpListener::bind((ip, port)).map_err(|e| format!("cannot listen on {ip}:{port}: {e}"))?;
    listener.set_nonblocking(true).map_err(|e| format!("cannot listen on {ip}:{port}: {e}"))?;
    let addr = listener.local_addr().map_err(|e| format!("cannot listen on {ip}:{port}: {e}"))?;
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().map_err(|e| format!("cannot start the HTTP server: {e}"))?;
    let (stop, stopped) = oneshot::channel::<()>();
    let (done_tx, done) = mpsc::channel();
    let (shutdown, shutdown_rx) = watch::channel(false);
    let app = router(state).layer(axum::Extension(shutdown_rx));
    let thread = std::thread::Builder::new()
        .name("http".into())
        .spawn(move || {
            runtime.block_on(async move {
                // Errors after the bind have no one to tell: the plugin sees the server gone.
                if let Ok(listener) = tokio::net::TcpListener::from_std(listener) {
                    let _ = axum::serve(listener, app)
                        .with_graceful_shutdown(async move {
                            stopped.await.ok();
                        })
                        .await;
                }
            });
            let _ = done_tx.send(());
        })
        .map_err(|e| format!("cannot start the HTTP server: {e}"))?;
    Ok((ServerHandle { shutdown, stop, done, thread }, addr))
}

#[cfg(test)]
pub(crate) mod testing {
    use axum::Router;
    use axum::body::Body;
    use axum::http::{HeaderMap, Method, Request, StatusCode};
    use http_body_util::BodyExt;
    use serde_json::Value;
    use std::sync::{Arc, RwLock};
    use tower::ServiceExt;

    use super::{AppState, Health};
    use crate::store::{Row, Status, Store};

    pub fn state() -> AppState {
        AppState {
            store: Store::in_memory(),
            events: tokio::sync::broadcast::channel(16).0,
            health: Arc::new(RwLock::new(Health::default())),
            origins: Vec::new(),
        }
    }

    pub fn row(path: &str, start_ms: i64, text: &str) -> Row {
        Row {
            path: path.into(),
            short_name: "sys1".into(),
            talkgroup: 101,
            start_ms,
            length_ms: 4000,
            status: Status::Ok,
            text: Some(text.into()),
            engine: "e".into(),
            model_asked: "default".into(),
            model_reported: Some("m1".into()),
            attempts: 1,
            transcribed_ms: start_ms + 10,
            ..Default::default()
        }
    }

    pub async fn send(app: Router, method: Method, uri: &str, origin: Option<&str>) -> (StatusCode, HeaderMap, Value) {
        let mut req = Request::builder().method(method).uri(uri);
        if let Some(o) = origin {
            req = req.header("origin", o);
        }
        let res = app.oneshot(req.body(Body::empty()).unwrap()).await.unwrap();
        let (status, headers) = (res.status(), res.headers().clone());
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let json = if bytes.is_empty() { Value::Null } else { serde_json::from_slice(&bytes).unwrap() };
        (status, headers, json)
    }

    pub async fn get(app: Router, uri: &str, origin: Option<&str>) -> (StatusCode, HeaderMap, Value) {
        send(app, Method::GET, uri, origin).await
    }
}
