//! Authenticated, transport-pluggable remote message exchange.
//!
//! The default transport is WebSocket over TCP. Its listener is intentionally
//! plain HTTP: applications may bind it to loopback/a private interface and
//! terminate TLS at a trusted reverse proxy. Applications that own a server
//! can implement [`RemoteTransport`] and pass authenticated frames to the same
//! [`RemoteHandler`].

use std::{
    collections::HashMap,
    fmt,
    future::Future,
    io,
    net::{SocketAddr, TcpListener, TcpStream},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use subtle::ConstantTimeEq;
use thiserror::Error;
use tungstenite::{
    Message, accept_hdr_with_config,
    client::IntoClientRequest,
    http::{
        HeaderValue, Response, StatusCode,
        header::{AUTHORIZATION, SEC_WEBSOCKET_PROTOCOL},
    },
    protocol::WebSocketConfig,
};
use uuid::Uuid;

/// Maximum JSON frame accepted by the built-in transport.
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;
/// Protocol version used by the first remoting implementation.
pub const PROTOCOL_VERSION: u16 = 1;

/// Actor location carried over a remote connection.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct ActorAddress {
    /// Stable identity of the actor system that owns the actor.
    pub system_id: Uuid,
    /// WebSocket endpoint of the owning system, when the destination is remote.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// Absolute actor path, such as `/user/orders`.
    pub path: String,
}

impl ActorAddress {
    /// Construct a local address.
    pub fn local(system_id: Uuid, path: impl Into<String>) -> Self {
        Self {
            system_id,
            endpoint: None,
            path: path.into(),
        }
    }

    /// Construct a remote address.
    pub fn remote(system_id: Uuid, endpoint: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            system_id,
            endpoint: Some(endpoint.into()),
            path: path.into(),
        }
    }

    /// Whether this address belongs to a remote endpoint.
    #[must_use]
    pub fn is_remote(&self) -> bool {
        self.endpoint.is_some()
    }
}

/// A versioned actor-message frame.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteRequest {
    pub version: u16,
    pub id: Uuid,
    pub destination: ActorAddress,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sender: Option<ActorAddress>,
    pub payload: Value,
}

impl RemoteRequest {
    /// Construct a request with the current protocol version.
    #[must_use]
    pub fn new(destination: ActorAddress, sender: Option<ActorAddress>, payload: Value) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            id: Uuid::new_v4(),
            destination,
            sender,
            payload,
        }
    }
}

/// A transport-level acknowledgment. `accepted` means the receiving runtime
/// accepted the message into the local actor mailbox, not that it processed it.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteResponse {
    pub version: u16,
    pub id: Uuid,
    pub accepted: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RemoteError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
}

impl RemoteResponse {
    /// Acknowledge mailbox acceptance.
    #[must_use]
    pub fn accepted(id: Uuid) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            id,
            accepted: true,
            error: None,
            payload: None,
        }
    }

    /// Acknowledge acceptance and return an application-defined reply payload.
    #[must_use]
    pub fn replied(id: Uuid, payload: Value) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            id,
            accepted: true,
            error: None,
            payload: Some(payload),
        }
    }

    /// Reject a request with a protocol-safe error.
    #[must_use]
    pub fn rejected(id: Uuid, error: RemoteError) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            id,
            accepted: false,
            error: Some(error),
            payload: None,
        }
    }
}

/// Failures visible to a remoting caller.
#[derive(Clone, Debug, Deserialize, Error, Serialize)]
#[serde(tag = "code", content = "message", rename_all = "snake_case")]
pub enum RemoteError {
    #[error("remote transport failed: {0}")]
    Transport(String),
    #[error("peer authentication failed")]
    Authentication,
    #[error("unsupported protocol version {0}")]
    UnsupportedVersion(u16),
    #[error("message frame exceeds the {MAX_FRAME_BYTES}-byte limit")]
    FrameTooLarge,
    #[error("destination actor was not found: {0}")]
    ActorNotFound(String),
    #[error("remote sender addresses are not supported by this transport API")]
    UnsupportedSender,
    #[error("message could not be decoded: {0}")]
    InvalidMessage(String),
    #[error("remote peer rejected the message: {0}")]
    Rejected(String),
}

/// Peer connection information. The bearer token is redacted from debug output
/// and is only sent in the HTTP upgrade authorization header.
#[derive(Clone)]
pub struct RemotePeer {
    pub id: String,
    pub endpoint: String,
    token: Arc<str>,
}

impl RemotePeer {
    /// Configure one trusted remote peer.
    ///
    /// Endpoints must use `ws://` for private/trusted networks or `wss://`
    /// when TLS is terminated by the remote listener or a trusted proxy.
    pub fn new(
        id: impl Into<String>,
        endpoint: impl Into<String>,
        bearer_token: impl Into<Arc<str>>,
    ) -> Result<Self, RemoteError> {
        let id = id.into();
        let endpoint = endpoint.into();
        let token = bearer_token.into();
        if id.is_empty() || token.is_empty() {
            return Err(RemoteError::Transport(
                "peer ID and bearer token must not be empty".into(),
            ));
        }
        if !(endpoint.starts_with("ws://") || endpoint.starts_with("wss://")) {
            return Err(RemoteError::Transport(
                "remote endpoint must use ws:// or wss://".into(),
            ));
        }
        Ok(Self {
            id,
            endpoint,
            token,
        })
    }

    fn authorization(&self) -> String {
        format!("Bearer {}", self.token)
    }
}

impl fmt::Debug for RemotePeer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemotePeer")
            .field("id", &self.id)
            .field("endpoint", &self.endpoint)
            .field("token", &"[REDACTED]")
            .finish()
    }
}

/// Authenticates a peer from its HTTP upgrade `Authorization` header.
#[derive(Clone, Default)]
pub struct PeerAuthenticator {
    peers: Arc<HashMap<String, Arc<str>>>,
}

impl PeerAuthenticator {
    /// Create an allow-list mapping peer IDs to independently generated tokens.
    #[must_use]
    pub fn new(peers: impl IntoIterator<Item = (String, String)>) -> Self {
        Self {
            peers: Arc::new(
                peers
                    .into_iter()
                    .map(|(id, token)| (id, Arc::<str>::from(token)))
                    .collect(),
            ),
        }
    }

    /// Authenticate a bearer token and return its configured peer identity.
    pub fn authenticate(&self, bearer_token: &str) -> Result<&str, RemoteError> {
        if bearer_token.is_empty() {
            return Err(RemoteError::Authentication);
        }
        self.peers
            .iter()
            .find(|(_, expected)| bool::from(expected.as_bytes().ct_eq(bearer_token.as_bytes())))
            .map(|(id, _)| id.as_str())
            .ok_or(RemoteError::Authentication)
    }
}

/// Boxed future returned by a transport send operation.
pub type RemoteFuture<T> = Pin<Box<dyn Future<Output = Result<T, RemoteError>> + Send + 'static>>;

/// A message handler invoked after the transport authenticates its peer.
pub type RemoteHandler = Arc<dyn Fn(&str, RemoteRequest) -> RemoteResponse + Send + Sync + 'static>;

/// Handle returned by a listening transport.
pub trait RemoteListener: Send + Sync {
    /// Return the actual local address, including an assigned port when binding
    /// to port zero. Custom transports may return `None` when unavailable.
    fn local_addr(&self) -> Option<SocketAddr> {
        None
    }

    /// Stop accepting new connections and close the listener.
    fn shutdown(&self);
}

/// Pluggable bidirectional transport for remote actor messages.
///
/// Implementations must authenticate the peer before invoking `handler`,
/// enforce [`MAX_FRAME_BYTES`], and preserve each request's correlation ID.
pub trait RemoteTransport: Send + Sync {
    /// Send one message and wait for the receiver's mailbox-acceptance response.
    fn send(&self, peer: RemotePeer, request: RemoteRequest) -> RemoteFuture<RemoteResponse>;

    /// Accept incoming messages and invoke `handler` for authenticated peers.
    fn listen(
        &self,
        bind: SocketAddr,
        authenticator: PeerAuthenticator,
        handler: RemoteHandler,
    ) -> Result<Box<dyn RemoteListener>, RemoteError>;
}

/// An authenticated outbound connection using the configured transport.
#[derive(Clone)]
pub struct RemoteConnection {
    peer: RemotePeer,
    transport: Arc<dyn RemoteTransport>,
}

impl RemoteConnection {
    /// Create a connection with a caller-supplied bidirectional transport.
    #[must_use]
    pub fn with_transport(peer: RemotePeer, transport: Arc<dyn RemoteTransport>) -> Self {
        Self { peer, transport }
    }

    /// The configured peer identity and endpoint.
    #[must_use]
    pub fn peer(&self) -> &RemotePeer {
        &self.peer
    }

    /// Send a request and await mailbox-acceptance acknowledgment.
    pub fn send(
        &self,
        destination: ActorAddress,
        sender: Option<ActorAddress>,
        payload: Value,
    ) -> RemoteFuture<RemoteResponse> {
        self.transport.send(
            self.peer.clone(),
            RemoteRequest::new(destination, sender, payload),
        )
    }
}

/// Built-in WebSocket-over-TCP implementation of [`RemoteTransport`].
#[derive(Clone, Copy, Debug, Default)]
pub struct WebSocketTransport;

impl RemoteTransport for WebSocketTransport {
    fn send(&self, peer: RemotePeer, request: RemoteRequest) -> RemoteFuture<RemoteResponse> {
        Box::pin(async move {
            let (tx, rx) = futures::channel::oneshot::channel();
            thread::Builder::new()
                .name("picard-remoting-send".into())
                .spawn(move || {
                    let result = websocket_send(&peer, &request);
                    let _ = tx.send(result);
                })
                .map_err(|err| RemoteError::Transport(err.to_string()))?;
            rx.await
                .map_err(|err| RemoteError::Transport(err.to_string()))?
        })
    }

    fn listen(
        &self,
        bind: SocketAddr,
        authenticator: PeerAuthenticator,
        handler: RemoteHandler,
    ) -> Result<Box<dyn RemoteListener>, RemoteError> {
        let listener =
            TcpListener::bind(bind).map_err(|err| RemoteError::Transport(err.to_string()))?;
        let local_addr = listener
            .local_addr()
            .map_err(|err| RemoteError::Transport(err.to_string()))?;
        listener
            .set_nonblocking(true)
            .map_err(|err| RemoteError::Transport(err.to_string()))?;
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = stop.clone();
        let thread_authenticator = authenticator.clone();
        let thread = thread::Builder::new()
            .name("picard-remoting-listener".into())
            .spawn(move || {
                while !thread_stop.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let auth = thread_authenticator.clone();
                            let handler = handler.clone();
                            let _ = thread::Builder::new()
                                .name("picard-remoting-peer".into())
                                .spawn(move || {
                                    serve_peer(stream, auth, handler);
                                });
                        }
                        Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(10));
                        }
                        Err(_) => break,
                    }
                }
            })
            .map_err(|err| RemoteError::Transport(err.to_string()))?;
        Ok(Box::new(WebSocketListener {
            stop,
            thread: Mutex::new(Some(thread)),
            local_addr,
        }))
    }
}

struct WebSocketListener {
    stop: Arc<AtomicBool>,
    thread: Mutex<Option<JoinHandle<()>>>,
    local_addr: SocketAddr,
}

impl RemoteListener for WebSocketListener {
    fn local_addr(&self) -> Option<SocketAddr> {
        Some(self.local_addr)
    }

    fn shutdown(&self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.lock().ok().and_then(|mut thread| thread.take()) {
            let _ = thread.join();
        }
    }
}

impl Drop for WebSocketListener {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn websocket_send(
    peer: &RemotePeer,
    request: &RemoteRequest,
) -> Result<RemoteResponse, RemoteError> {
    let mut http_request = peer
        .endpoint
        .as_str()
        .into_client_request()
        .map_err(|err| RemoteError::Transport(err.to_string()))?;
    let authorization = HeaderValue::from_str(&peer.authorization())
        .map_err(|err| RemoteError::Transport(err.to_string()))?;
    http_request
        .headers_mut()
        .insert(AUTHORIZATION, authorization);
    http_request.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static("picard.remoting.v1"),
    );

    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_FRAME_BYTES))
        .max_frame_size(Some(MAX_FRAME_BYTES));
    let (mut socket, _) = tungstenite::client::connect_with_config(http_request, Some(config), 0)
        .map_err(|err| RemoteError::Transport(err.to_string()))?;
    let frame = serde_json::to_string(request)
        .map_err(|err| RemoteError::InvalidMessage(err.to_string()))?;
    if frame.len() > MAX_FRAME_BYTES {
        return Err(RemoteError::FrameTooLarge);
    }
    socket
        .send(Message::Text(frame.into()))
        .map_err(|err| RemoteError::Transport(err.to_string()))?;
    let response = socket
        .read()
        .map_err(|err| RemoteError::Transport(err.to_string()))?;
    let Message::Text(response) = response else {
        return Err(RemoteError::Transport(
            "remote peer returned a non-text response".into(),
        ));
    };
    let response: RemoteResponse = serde_json::from_str(&response)
        .map_err(|err| RemoteError::InvalidMessage(err.to_string()))?;
    if response.version != PROTOCOL_VERSION {
        return Err(RemoteError::UnsupportedVersion(response.version));
    }
    if response.id != request.id {
        return Err(RemoteError::Transport(
            "response correlation ID does not match request".into(),
        ));
    }
    if !response.accepted {
        return Err(response
            .error
            .clone()
            .unwrap_or_else(|| RemoteError::Rejected("unspecified rejection".into())));
    }
    Ok(response)
}

#[allow(clippy::result_large_err)]
fn serve_peer(stream: TcpStream, authenticator: PeerAuthenticator, handler: RemoteHandler) {
    let mut peer_id = None;
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_FRAME_BYTES))
        .max_frame_size(Some(MAX_FRAME_BYTES));
    let Ok(mut socket) = accept_hdr_with_config(
        stream,
        |request: &tungstenite::handshake::server::Request, mut response: Response<()>| {
            let authorization = request
                .headers()
                .get(AUTHORIZATION)
                .and_then(|header| header.to_str().ok())
                .and_then(|header| header.strip_prefix("Bearer "));
            let authenticated = authorization
                .and_then(|token| authenticator.authenticate(token).ok())
                .map(str::to_string);
            let Some(authenticated) = authenticated else {
                *response.status_mut() = StatusCode::UNAUTHORIZED;
                return Err(response.map(|()| Some("peer authentication failed".into())));
            };
            if request
                .headers()
                .get(SEC_WEBSOCKET_PROTOCOL)
                .and_then(|value| value.to_str().ok())
                != Some("picard.remoting.v1")
            {
                *response.status_mut() = StatusCode::BAD_REQUEST;
                return Err(response.map(|()| Some("unsupported WebSocket subprotocol".into())));
            }
            peer_id = Some(authenticated);
            response.headers_mut().insert(
                SEC_WEBSOCKET_PROTOCOL,
                HeaderValue::from_static("picard.remoting.v1"),
            );
            Ok(response)
        },
        Some(config),
    ) else {
        return;
    };
    let Some(peer_id) = peer_id else {
        return;
    };
    while let Ok(message) = socket.read() {
        match message {
            Message::Text(frame) => {
                let response = handle_request(&peer_id, &frame, &handler);
                let Ok(encoded) = serde_json::to_string(&response) else {
                    break;
                };
                if encoded.len() > MAX_FRAME_BYTES
                    || socket.send(Message::Text(encoded.into())).is_err()
                {
                    break;
                }
            }
            Message::Ping(payload) => {
                if socket.send(Message::Pong(payload)).is_err() {
                    break;
                }
            }
            Message::Close(_) => break,
            _ => {
                let response = RemoteResponse::rejected(
                    Uuid::nil(),
                    RemoteError::InvalidMessage("only text frames are supported".into()),
                );
                let Ok(encoded) = serde_json::to_string(&response) else {
                    break;
                };
                if socket.send(Message::Text(encoded.into())).is_err() {
                    break;
                }
            }
        }
    }
}

fn handle_request(peer_id: &str, frame: &str, handler: &RemoteHandler) -> RemoteResponse {
    if frame.len() > MAX_FRAME_BYTES {
        return RemoteResponse::rejected(Uuid::nil(), RemoteError::FrameTooLarge);
    }
    let request = match serde_json::from_str::<RemoteRequest>(frame) {
        Ok(request) => request,
        Err(err) => {
            return RemoteResponse::rejected(
                Uuid::nil(),
                RemoteError::InvalidMessage(err.to_string()),
            );
        }
    };
    if request.version != PROTOCOL_VERSION {
        return RemoteResponse::rejected(
            request.id,
            RemoteError::UnsupportedVersion(request.version),
        );
    }
    let id = request.id;
    let mut response = handler(peer_id, request);
    if response.id != id || response.version != PROTOCOL_VERSION {
        response = RemoteResponse::rejected(
            id,
            RemoteError::InvalidMessage("handler returned an invalid acknowledgment".into()),
        );
    }
    response
}

/// Build an authenticated HTTP header for an application-owned WebSocket
/// server integration. Keep bearer tokens out of URLs and log output.
#[must_use]
pub fn bearer_authorization(token: &str) -> String {
    format!("Bearer {token}")
}

/// Spawn a blocking transport operation and expose its result as a future.
///
/// Intended for custom transports that use a blocking client library.
pub fn spawn_blocking<T, F>(operation: F) -> RemoteFuture<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, RemoteError> + Send + 'static,
{
    Box::pin(async move {
        let (tx, rx) = futures::channel::oneshot::channel();
        thread::Builder::new()
            .name("picard-custom-transport".into())
            .spawn(move || {
                let _ = tx.send(operation());
            })
            .map_err(|err| RemoteError::Transport(err.to_string()))?;
        rx.await
            .map_err(|err| RemoteError::Transport(err.to_string()))?
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn bearer_credentials_are_not_in_debug_output() {
        let peer = RemotePeer::new("orders", "ws://127.0.0.1:8080/remoting", "top-secret").unwrap();

        assert_eq!(bearer_authorization("top-secret"), "Bearer top-secret");
        assert!(!format!("{peer:?}").contains("top-secret"));
    }

    #[test]
    fn authenticator_rejects_unknown_tokens() {
        let authenticator = PeerAuthenticator::new([("orders".into(), "expected".into())]);

        assert!(matches!(
            authenticator.authenticate("expected"),
            Ok("orders")
        ));
        assert!(matches!(
            authenticator.authenticate("unexpected"),
            Err(RemoteError::Authentication)
        ));
        assert!(matches!(
            authenticator.authenticate(""),
            Err(RemoteError::Authentication)
        ));
    }

    #[test]
    fn websocket_transport_delivers_and_acknowledges_a_request() {
        let transport = WebSocketTransport;
        let handler: RemoteHandler = Arc::new(|peer_id, request| {
            assert_eq!(peer_id, "sender");
            assert_eq!(request.destination.path, "/user/receiver");
            assert_eq!(request.payload, json!({"value": 7}));
            RemoteResponse::replied(request.id, json!({"received": true}))
        });
        let listener = transport
            .listen(
                "127.0.0.1:0".parse().unwrap(),
                PeerAuthenticator::new([("sender".into(), "shared-secret".into())]),
                handler,
            )
            .unwrap();
        let endpoint = format!(
            "ws://{}/remoting",
            listener.local_addr().expect("WebSocket listener address")
        );
        let peer = RemotePeer::new("receiver", endpoint, "shared-secret").unwrap();
        let request = RemoteRequest::new(
            ActorAddress::local(Uuid::new_v4(), "/user/receiver"),
            None,
            json!({"value": 7}),
        );

        let response = futures::executor::block_on(transport.send(peer, request.clone())).unwrap();

        assert!(response.accepted);
        assert_eq!(response.id, request.id);
        assert_eq!(response.payload, Some(json!({"received": true})));
        listener.shutdown();
    }

    #[test]
    fn websocket_transport_reports_authentication_failure() {
        let transport = WebSocketTransport;
        let listener = transport
            .listen(
                "127.0.0.1:0".parse().unwrap(),
                PeerAuthenticator::new([("sender".into(), "expected".into())]),
                Arc::new(|_, request| RemoteResponse::accepted(request.id)),
            )
            .unwrap();
        let endpoint = format!(
            "ws://{}/remoting",
            listener.local_addr().expect("WebSocket listener address")
        );
        let peer = RemotePeer::new("receiver", endpoint, "wrong").unwrap();
        let request = RemoteRequest::new(
            ActorAddress::local(Uuid::new_v4(), "/user/receiver"),
            None,
            Value::Null,
        );

        let result = futures::executor::block_on(transport.send(peer, request));

        assert!(matches!(result, Err(RemoteError::Transport(_))));
        listener.shutdown();
    }

    #[test]
    fn handler_rejects_invalid_protocol_versions() {
        let mut request = RemoteRequest::new(
            ActorAddress::local(Uuid::new_v4(), "/user/receiver"),
            None,
            Value::Null,
        );
        request.version += 1;

        let handler: RemoteHandler = Arc::new(|_, request| RemoteResponse::accepted(request.id));
        let response = handle_request(
            "sender",
            &serde_json::to_string(&request).unwrap(),
            &handler,
        );

        assert!(!response.accepted);
        assert!(matches!(
            response.error,
            Some(RemoteError::UnsupportedVersion(_))
        ));
    }
}
