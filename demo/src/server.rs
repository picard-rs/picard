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
    serve_sim(addr, sim).await
}

/// Runs the web server using an existing simulation, until Ctrl-C.
pub async fn serve_sim(addr: SocketAddr, sim: Sim) -> Result<(), String> {
    serve_with(addr, sim, Router::new(), false).await
}

/// Like [`serve_sim`], and also routes authenticated WebSocket connections at `/console`
/// to the simulation's Picard console.
///
/// Clients send `Authorization: Bearer <token>`; browser upgrades (with an `Origin`
/// header) are rejected.
///
/// # Errors
///
/// Returns an error message if `addr` is not a loopback address, the token is weak,
/// or the server fails.
#[cfg(feature = "console")]
pub async fn serve_sim_with_console(addr: SocketAddr, sim: Sim, token: &str) -> Result<(), String> {
    if !addr.ip().is_loopback() {
        return Err(
            "the console route requires a loopback --host; use a secure tunnel for remote access"
                .into(),
        );
    }
    if !picard::console::is_valid_token(token) {
        return Err(
            "console token must contain at least 32 ASCII letters, digits, '-' or '_'".into(),
        );
    }
    let routes = console_route::router(sim.system.console(), token);
    serve_with(addr, sim, routes, true).await
}

async fn serve_with(
    addr: SocketAddr,
    sim: Sim,
    routes: Router,
    console: bool,
) -> Result<(), String> {
    let (tx, frames) = watch::channel::<Arc<str>>(Arc::from("{}"));
    let state = Arc::new(AppState {
        bridge: Mutex::new(Bridge::new(sim)),
        frames,
        hello: hello(),
    });

    let app = Router::new()
        .route("/", get(|| async { Html(INDEX_HTML) }))
        .route("/ws", get(ws_handler))
        .route("/api/metrics", get(metrics_handler))
        .with_state(state.clone())
        .merge(routes);

    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| format!("cannot bind {addr}: {e}"))?;
    let bound_address = listener.local_addr().map_err(|err| err.to_string())?;
    if console {
        println!("Living City console at ws://{bound_address}/console");
    }
    println!("Living City running at http://{bound_address}");
    let broadcaster = tokio::spawn(broadcaster(state.clone(), tx));
    let result = axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .map_err(|e| e.to_string());
    broadcaster.abort();
    result
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

#[cfg(feature = "console")]
mod console_route {
    use std::sync::Arc;

    use axum::{
        Router,
        extract::{
            State,
            ws::{Message, WebSocket, WebSocketUpgrade},
        },
        http::{HeaderMap, StatusCode, header},
        response::{IntoResponse, Response},
        routing::get,
    };
    use picard::console::{Console, ConsoleError, MAX_REQUEST_BYTES, Response as ConsoleResponse};
    use tokio::sync::Semaphore;

    const MAX_CLIENTS: usize = 16;

    struct RouteState {
        console: Console,
        authorization: String,
        clients: Arc<Semaphore>,
    }

    pub(super) fn router(console: Console, token: &str) -> Router {
        Router::new()
            .route("/console", get(upgrade))
            .with_state(Arc::new(RouteState {
                console,
                authorization: format!("Bearer {token}"),
                clients: Arc::new(Semaphore::new(MAX_CLIENTS)),
            }))
    }

    async fn upgrade(
        ws: WebSocketUpgrade,
        headers: HeaderMap,
        State(state): State<Arc<RouteState>>,
    ) -> Response {
        let authorized = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            == Some(state.authorization.as_str());
        if !authorized || headers.contains_key(header::ORIGIN) {
            return (StatusCode::UNAUTHORIZED, "Console upgrade rejected").into_response();
        }
        let Ok(permit) = state.clients.clone().try_acquire_owned() else {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Console connection limit reached",
            )
                .into_response();
        };
        let console = state.console.clone();
        ws.max_message_size(MAX_REQUEST_BYTES)
            .max_frame_size(MAX_REQUEST_BYTES)
            .on_upgrade(move |socket| async move {
                let _permit = permit;
                session(socket, console).await;
            })
    }

    async fn session(mut socket: WebSocket, console: Console) {
        while let Some(Ok(message)) = socket.recv().await {
            let reply = match message {
                Message::Text(text) => {
                    let console = console.clone();
                    // Requests may run user serializers; keep them off the async workers.
                    match tokio::task::spawn_blocking(move || console.handle_text(text.as_str()))
                        .await
                    {
                        Ok(reply) => reply,
                        Err(_) => break,
                    }
                }
                Message::Binary(_) => ConsoleResponse::from_result(
                    None,
                    Err(ConsoleError {
                        code: "invalid_request".into(),
                        message: "Requests must be JSON text frames".into(),
                    }),
                )
                .to_json(),
                Message::Close(_) => break,
                Message::Ping(_) | Message::Pong(_) => continue,
            };
            if socket.send(Message::text(reply)).await.is_err() {
                break;
            }
        }
    }
}
