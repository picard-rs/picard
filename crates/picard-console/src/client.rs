//! Blocking WebSocket client for the console protocol.

use std::{error::Error, time::Duration};

use picard::console::{Command, Request, Response};
use serde_json::Value;
use tungstenite::{Message, WebSocket, client::IntoClientRequest, connect, stream::MaybeTlsStream};

pub(crate) struct Client {
    socket: WebSocket<MaybeTlsStream<std::net::TcpStream>>,
    next_id: u64,
}

impl Client {
    pub(crate) fn connect(url: &str, token: &str) -> Result<Self, Box<dyn Error>> {
        let mut request = url.into_client_request()?;
        if request.uri().scheme_str() != Some("ws") {
            return Err("Use a ws:// URL through loopback or a secure tunnel".into());
        }
        let host = request.uri().host().ok_or("Missing console hostname")?;
        let host = host.trim_start_matches('[').trim_end_matches(']');
        if host != "localhost"
            && !host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
        {
            return Err("Console connections must use loopback; use a secure tunnel for remote applications".into());
        }
        request
            .headers_mut()
            .insert("Authorization", format!("Bearer {token}").parse()?);
        let (socket, _) = connect(request).map_err(|err| connect_error(&err, url))?;
        if let MaybeTlsStream::Plain(stream) = socket.get_ref() {
            stream.set_read_timeout(Some(Duration::from_secs(5)))?;
            stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        }
        Ok(Self { socket, next_id: 0 })
    }

    pub(crate) fn execute(&mut self, command: Command) -> Result<Response, Box<dyn Error>> {
        self.next_id = self.next_id.checked_add(1).ok_or("Request ID exhausted")?;
        let request = Request {
            id: self.next_id,
            command,
        };
        self.socket
            .send(Message::Text(serde_json::to_string(&request)?.into()))?;
        loop {
            match self.socket.read()? {
                Message::Text(text) => {
                    let response: Response = serde_json::from_str(&text)?;
                    if response.id != Some(request.id)
                        || response.result.is_some() == response.error.is_some()
                    {
                        return Err("Invalid console response or mismatched request ID".into());
                    }
                    return Ok(response);
                }
                Message::Ping(_) | Message::Pong(_) => self.socket.flush()?,
                Message::Close(_) => return Err("Server closed the console connection".into()),
                _ => return Err("Expected a JSON text response".into()),
            }
        }
    }

    pub(crate) fn actors(&mut self) -> Result<Vec<Value>, Box<dyn Error>> {
        let mut actors = Vec::new();
        let mut offset = 0;
        loop {
            let response = self.execute(Command::Actors { offset, limit: 100 })?;
            if let Some(error) = response.error {
                return Err(format!("{}: {}", error.code, error.message).into());
            }
            let result = response.result.ok_or("Missing actor list")?;
            actors.extend(
                result["actors"]
                    .as_array()
                    .ok_or("Invalid actor list")?
                    .iter()
                    .cloned(),
            );
            let Some(next) = result["next_offset"].as_u64() else {
                break;
            };
            let next = usize::try_from(next)?;
            if next <= offset {
                return Err("Actor pagination did not advance".into());
            }
            offset = next;
        }
        Ok(actors)
    }

    pub(crate) fn close(mut self) -> Result<(), Box<dyn Error>> {
        self.socket.close(None)?;
        loop {
            match self.socket.read() {
                Ok(_) => {}
                Err(tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed) => {
                    return Ok(());
                }
                Err(err) => return Err(err.into()),
            }
        }
    }
}

fn connect_error(error: &tungstenite::Error, url: &str) -> String {
    use tungstenite::{Error, error::UrlError, http::StatusCode};
    let hint = match error {
        Error::Url(UrlError::UnableToConnect(_)) | Error::Io(_) => {
            "Nothing accepted the connection. Check that the application is running with the \
             `console` feature and that its console endpoint is started (Living City: \
             `cargo run -p living-city --features console -- --console-listen 127.0.0.1:9000` \
             with PICARD_CONSOLE_TOKEN set)."
        }
        Error::Http(response) if response.status() == StatusCode::UNAUTHORIZED => {
            "The server rejected the token. Set PICARD_CONSOLE_TOKEN to the application's token."
        }
        Error::Http(response) if response.status() == StatusCode::NOT_FOUND => {
            "The server has no console at this path; check the URL's path."
        }
        _ => return format!("Cannot connect to {url}: {error}"),
    };
    format!("Cannot connect to {url}: {error}\n{hint}")
}
