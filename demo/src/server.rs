//! Web layer: serves the UI and streams frames over a WebSocket.
//!
//! It never touches actor state. It reads frames published by the gateway actor, samples the
//! metrics counters, and turns browser commands into supervisor messages.

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::{Html, IntoResponse},
    routing::get,
};
use tokio::{net::TcpListener, sync::watch};

use crate::{
    protocol::{Bridge, hello},
    sim::Sim,
};

const INDEX_HTML: &str = include_str!("../web/index.html");
const FRAME_PERIOD: Duration = Duration::from_millis(100);

struct AppState {
    bridge: Mutex<Bridge>,
    frames: watch::Receiver<Arc<str>>,
    hello: String,
}

/// Runs the demo server until Ctrl-C.
///
/// # Errors
///
/// Returns an error message if the simulation fails to start, `addr`
/// cannot be bound, or the server fails.
pub async fn serve(addr: SocketAddr, vehicles: usize) -> Result<(), String> {
    let sim = Sim::start(vehicles)?;
    let (tx, frames) = watch::channel::<Arc<str>>(Arc::from("{}"));
    let state = Arc::new(AppState {
        bridge: Mutex::new(Bridge::new(sim)),
        frames,
        hello: hello(),
    });

    tokio::spawn(broadcaster(state.clone(), tx));

    let app = Router::new()
        .route("/", get(|| async { Html(INDEX_HTML) }))
        .route("/ws", get(ws_handler))
        .route("/api/metrics", get(metrics_handler))
        .with_state(state);

    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| format!("cannot bind {addr}: {e}"))?;
    println!("Living City running at http://{addr}");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .map_err(|e| e.to_string())
}

async fn metrics_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let frame = state.frames.borrow().clone();
    ([("content-type", "application/json")], frame.to_string())
}

async fn broadcaster(state: Arc<AppState>, tx: watch::Sender<Arc<str>>) {
    let mut interval = tokio::time::interval(FRAME_PERIOD);
    loop {
        interval.tick().await;
        let frame = state.bridge.lock().map(|mut b| b.step());
        if let Ok(frame) = frame {
            let _ = tx.send(Arc::from(frame));
        }
    }
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| client(socket, state))
}

async fn client(mut socket: WebSocket, state: Arc<AppState>) {
    if socket
        .send(Message::text(state.hello.clone()))
        .await
        .is_err()
    {
        return;
    }
    let mut frames = state.frames.clone();
    loop {
        tokio::select! {
            changed = frames.changed() => {
                if changed.is_err() {
                    break;
                }
                let frame = frames.borrow_and_update().clone();
                if socket.send(Message::text(frame.to_string())).await.is_err() {
                    break;
                }
            }
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Text(text))) => {
                    if let Ok(mut bridge) = state.bridge.lock() {
                        bridge.command(&text);
                    }
                }
                Some(Ok(_)) => {}
                _ => break,
            },
        }
    }
}
