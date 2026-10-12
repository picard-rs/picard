use std::time::Duration;
use tungstenite::{Message, client::IntoClientRequest, http::header::AUTHORIZATION};

/// A client for connecting to a Picard console WebSocket server.
#[derive(Clone, Debug)]
pub struct ConsolePeerClient {
    endpoint: String,
    token: String,
    timeout: Duration,
}

impl ConsolePeerClient {
    /// Create a new client targeting a console endpoint with a bearer token.
    pub fn new(endpoint: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            token: token.into(),
            timeout: Duration::from_secs(5),
        }
    }

    /// Set the request timeout.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Send a console request (JSON string) to the remote console server and return the response JSON string.
    pub fn query(&self, request_text: &str) -> Result<String, String> {
        let mut request = self
            .endpoint
            .as_str()
            .into_client_request()
            .map_err(|e| format!("Invalid console endpoint: {e}"))?;
        request.headers_mut().insert(
            AUTHORIZATION,
            format!("Bearer {}", self.token)
                .parse()
                .map_err(|e| format!("Invalid authorization header: {e}"))?,
        );
        let (mut socket, _) = tungstenite::connect(request)
            .map_err(|e| format!("Failed to connect to console at {}: {e}", self.endpoint))?;

        if let tungstenite::stream::MaybeTlsStream::Plain(s) = socket.get_ref() {
            let _ = s.set_read_timeout(Some(self.timeout));
            let _ = s.set_write_timeout(Some(self.timeout));
        }

        socket
            .send(Message::Text(request_text.to_owned().into()))
            .map_err(|e| format!("Failed to send console request: {e}"))?;

        let reply = loop {
            match socket
                .read()
                .map_err(|e| format!("Failed to read console response: {e}"))?
            {
                Message::Text(text) => break text.to_string(),
                Message::Binary(bin) => {
                    if let Ok(text) = String::from_utf8(bin.to_vec()) {
                        break text;
                    }
                }
                Message::Ping(payload) => {
                    let _ = socket.send(Message::Pong(payload));
                }
                Message::Close(_) => return Err("Connection closed by console peer".to_string()),
                _ => {}
            }
        };
        let _ = socket.close(None);
        Ok(reply)
    }
}
