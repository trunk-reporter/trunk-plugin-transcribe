//! `/api/v1/events`: transcripts as they are made, as server-sent events.

use std::convert::Infallible;
use std::time::Duration;

use axum::Extension;
use axum::extract::{Query, State};
use axum::http::header::{CACHE_CONTROL, HeaderName};
use axum::http::{HeaderMap, HeaderValue};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use futures_util::StreamExt;
use futures_util::stream;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::watch;

use super::AppState;
use super::transcripts::{Params, failed, param};
use crate::store::Transcript;

/// How often an idle stream gets an `event: ping`.
const KEEPALIVE: Duration = Duration::from_secs(15);
/// The most rows replayed to a client that reconnects.
const REPLAY: usize = 1000;

fn frame(t: &Transcript) -> Event {
    Event::default().id(t.transcribed_ms.to_string()).event("transcript").data(serde_json::to_string(t).unwrap_or_default())
}

/// Replays what was transcribed after `Last-Event-ID` (the header, or
/// `?lastEventId=`), then streams live. A non-numeric id is ignored and the
/// stream starts live (an `EventSource` does not reconnect after an error).
/// The stream ends when the server's shutdown signal (from [`super::start`]) fires.
pub async fn events(
    State(state): State<AppState>,
    shutdown: Option<Extension<watch::Receiver<bool>>>,
    headers: HeaderMap,
    Query(q): Query<Params>,
) -> Response {
    let header = headers.get("last-event-id").and_then(|v| v.to_str().ok()).map(str::trim).filter(|v| !v.is_empty());
    let last: Option<i64> = header.or_else(|| param(&q, "lastEventId")).and_then(|id| id.parse().ok());
    // Subscribe before replaying so nothing stored in between is lost; the
    // overlap is dropped by the floor below.
    let rx = state.events.subscribe();
    let replayed: Vec<Transcript> = match last {
        Some(ms) => match state.store.since(ms, REPLAY) {
            Ok(rows) => rows.iter().map(|(r, s)| Transcript::from_row(r, s)).collect(),
            Err(e) => return failed(e),
        },
        None => Vec::new(),
    };
    let floor = replayed.last().map(|t| t.transcribed_ms).or(last);
    let live = stream::unfold(rx, |mut rx| async move {
        loop {
            match rx.recv().await {
                Ok(t) => return Some((t, rx)),
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return None,
            }
        }
    })
    .filter(move |t| std::future::ready(floor.is_none_or(|f| t.transcribed_ms > f)));
    let stopping = async move {
        match shutdown {
            Some(Extension(mut rx)) => {
                let _ = rx.wait_for(|stop| *stop).await;
            }
            None => std::future::pending().await,
        }
    };
    let frames = stream::iter(replayed).chain(live).take_until(stopping).map(|t| Ok::<_, Infallible>(frame(&t)));
    (
        [(CACHE_CONTROL, HeaderValue::from_static("no-cache")), (HeaderName::from_static("x-accel-buffering"), HeaderValue::from_static("no"))],
        Sse::new(frames).keep_alive(KeepAlive::new().interval(KEEPALIVE).event(Event::default().event("ping").data("{}"))),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use std::io::{ErrorKind, Read, Write};
    use std::net::{SocketAddr, TcpStream};
    use std::time::{Duration, Instant};

    use super::super::start;
    use super::super::testing::{row, state};
    use crate::store::{Row, Transcript};

    fn transcribed(path: &str, ms: i64) -> Row {
        Row { transcribed_ms: ms, ..row(path, 1000, "t") }
    }

    /// `GET target` with `headers` (each ending in `\r\n`).
    fn connect(addr: SocketAddr, target: &str, headers: &str) -> TcpStream {
        let mut s = TcpStream::connect(addr).unwrap();
        s.set_read_timeout(Some(Duration::from_millis(100))).unwrap();
        write!(s, "GET {target} HTTP/1.1\r\nHost: x\r\n{headers}\r\n").unwrap();
        s
    }

    /// Reads once (at most 100 ms) into `buf`; false when the peer has closed.
    fn read_some(s: &mut TcpStream, buf: &mut String) -> bool {
        let mut chunk = [0u8; 4096];
        match s.read(&mut chunk) {
            Ok(0) => false,
            Ok(n) => {
                buf.push_str(&String::from_utf8_lossy(&chunk[..n]));
                true
            }
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => true,
            Err(e) if matches!(e.kind(), ErrorKind::ConnectionReset | ErrorKind::ConnectionAborted) => false,
            Err(e) => panic!("{e}"),
        }
    }

    /// Reads into `buf` until it holds `needle`; panics after 5 s.
    fn read_until(s: &mut TcpStream, buf: &mut String, needle: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !buf.contains(needle) {
            assert!(Instant::now() < deadline, "no {needle:?} in {buf:?}");
            assert!(read_some(s, buf), "closed before {needle:?}: {buf:?}");
        }
    }

    #[test]
    fn events_replay_then_live_and_a_bad_last_event_id_starts_live() {
        let s = state();
        s.store.put(&transcribed("a", 10), &[]).unwrap();
        s.store.put(&transcribed("b", 20), &[]).unwrap();
        let events = s.events.clone();
        let (server, addr) = start(s, "127.0.0.1", 0).unwrap();

        let mut first = connect(addr, "/api/v1/events", "Last-Event-ID: 10\r\n");
        let mut buf = String::new();
        read_until(&mut first, &mut buf, "id: 20\nevent: transcript\ndata: {\"path\":\"b\"");
        assert!(buf.starts_with("HTTP/1.1 200"), "{buf}");
        let head = buf.to_ascii_lowercase();
        assert!(head.contains("content-type: text/event-stream"), "{buf}");
        assert!(head.contains("cache-control: no-cache") && head.contains("x-accel-buffering: no"), "{buf}");
        // Live, and the overlap with the replay (an old row announced again) is dropped.
        events.send(Transcript::from_row(&transcribed("b", 20), &[])).unwrap();
        events.send(Transcript::from_row(&transcribed("c", 30), &[])).unwrap();
        read_until(&mut first, &mut buf, "id: 30\nevent: transcript\ndata: {\"path\":\"c\"");
        assert!(!buf.contains("id: 10\n") && buf.matches("id: 20\n").count() == 1, "{buf}");

        // A non-numeric id, as the header or as the query, starts live: 200, nothing replayed.
        let mut by_header = connect(addr, "/api/v1/events", "Last-Event-ID: abc\r\n");
        let mut by_query = connect(addr, "/api/v1/events?lastEventId=abc", "");
        let (mut a, mut b) = (String::new(), String::new());
        read_until(&mut by_header, &mut a, "\r\n\r\n");
        read_until(&mut by_query, &mut b, "\r\n\r\n");
        events.send(Transcript::from_row(&transcribed("d", 40), &[])).unwrap();
        for (s, buf) in [(&mut by_header, &mut a), (&mut by_query, &mut b)] {
            read_until(s, buf, "id: 40\nevent: transcript\ndata: {\"path\":\"d\"");
            assert!(buf.starts_with("HTTP/1.1 200") && buf.to_ascii_lowercase().contains("content-type: text/event-stream"), "{buf}");
            assert!(!buf.contains("id: 10\n") && !buf.contains("id: 20\n") && !buf.contains("id: 30\n"), "{buf}");
        }

        drop((first, by_header, by_query));
        let t = Instant::now();
        server.stop(Duration::from_secs(1));
        assert!(t.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn stop_ends_an_open_event_stream() {
        let (server, addr) = start(state(), "127.0.0.1", 0).unwrap();
        let mut client = connect(addr, "/api/v1/events", "");
        let mut buf = String::new();
        read_until(&mut client, &mut buf, "\r\n\r\n");
        let t = Instant::now();
        server.stop(Duration::from_secs(2));
        assert!(t.elapsed() < Duration::from_secs(1), "stop took {:?}", t.elapsed());
        let deadline = Instant::now() + Duration::from_secs(5);
        while read_some(&mut client, &mut buf) {
            assert!(Instant::now() < deadline, "still open: {buf:?}");
        }
    }
}
