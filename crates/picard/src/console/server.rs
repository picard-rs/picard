use std::{
    io,
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use slog::warn;
use tungstenite::{
    Error, Message, WebSocket, accept_hdr_with_config,
    handshake::server::{Request as UpgradeRequest, Response as UpgradeResponse},
    http::{StatusCode, header},
    protocol::WebSocketConfig,
};

use super::{ConsoleError, MAX_REQUEST_BYTES, Response, is_valid_token};
use crate::system::{ActorSystem, LoggingSystem};

const MAX_CONNECTIONS: usize = 16;
const IO_TIMEOUT: Duration = Duration::from_millis(100);

/// Options for an explicitly started console. Only loopback addresses are accepted.
///
/// Supply a strong, application-generated token (at least 32 ASCII characters).
/// Clients authenticate the HTTP upgrade using `Authorization: Bearer <token>`.
pub struct ConsoleOptions {
    pub bind: SocketAddr,
    pub token: String,
}

/// Owns the listener and its client threads. Dropping it stops and joins them.
pub struct ConsoleHandle {
    address: SocketAddr,
    stopping: Arc<AtomicBool>,
    thread: Option<JoinHandle<io::Result<()>>>,
    log: LoggingSystem,
}

impl ConsoleHandle {
    /// The bound address, including the selected port when `bind` used port zero.
    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }

    /// Stop the listener and all clients, reporting server or thread failures.
    pub fn stop(mut self) -> io::Result<()> {
        self.join()
    }

    fn join(&mut self) -> io::Result<()> {
        self.stopping.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .map_err(|_| io::Error::other("Console server panicked"))??;
        }
        Ok(())
    }
}

impl Drop for ConsoleHandle {
    fn drop(&mut self) {
        if let Err(err) = self.join() {
            warn!(self.log, "Console shutdown failed"; "error" => err.to_string());
        }
    }
}

impl ActorSystem {
    /// Start an authenticated WebSocket endpoint at `/console`.
    ///
    /// The feature does not open a port until this method is called. The returned
    /// handle must be kept alive. Binding outside loopback or providing an invalid
    /// token is rejected; use a secure tunnel for remote access.
    pub fn start_console(&self, options: ConsoleOptions) -> io::Result<ConsoleHandle> {
        if !options.bind.ip().is_loopback() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Console must bind to loopback",
            ));
        }
        if !is_valid_token(&options.token) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Console token must contain at least 32 ASCII letters, digits, '-' or '_'",
            ));
        }
        let listener = TcpListener::bind(options.bind)?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let stopping = Arc::new(AtomicBool::new(false));
        let stop = stopping.clone();
        let system = self.clone();
        let log = self.log();
        let thread = thread::Builder::new()
            .name("picard-console".into())
            .spawn(move || serve(&listener, &system, &options.token, &stop))?;
        Ok(ConsoleHandle {
            address,
            stopping,
            thread: Some(thread),
            log,
        })
    }
}

fn serve(
    listener: &TcpListener,
    system: &ActorSystem,
    token: &str,
    stop: &Arc<AtomicBool>,
) -> io::Result<()> {
    let mut clients: Vec<JoinHandle<()>> = Vec::new();
    let result = (|| {
        while !stop.load(Ordering::Acquire) {
            let mut index = 0;
            while index < clients.len() {
                if clients[index].is_finished() {
                    clients
                        .swap_remove(index)
                        .join()
                        .map_err(|_| io::Error::other("Console client thread panicked"))?;
                } else {
                    index += 1;
                }
            }
            match listener.accept() {
                Ok((stream, _)) if clients.len() < MAX_CONNECTIONS => {
                    let system = system.clone();
                    let token = token.to_owned();
                    let stop = stop.clone();
                    clients.push(thread::Builder::new().name("picard-console-client".into()).spawn(move || {
                        if let Err(err) = client(stream, &system, &token, &stop) {
                            warn!(system.log(), "Console client disconnected"; "error" => err.to_string());
                        }
                    })?);
                }
                Ok((stream, _)) => {
                    drop(stream);
                    warn!(system.log(), "Console connection limit reached");
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(err) => return Err(err),
            }
        }
        Ok(())
    })();
    stop.store(true, Ordering::Release);
    let mut result = result;
    for client in clients {
        if client.join().is_err() {
            result = Err(io::Error::other("Console client thread panicked"));
        }
    }
    result
}

fn client(
    stream: TcpStream,
    system: &ActorSystem,
    token: &str,
    stop: &AtomicBool,
) -> Result<(), Box<dyn std::error::Error>> {
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(IO_TIMEOUT))?;
    let console = system.console();
    let authorization = format!("Bearer {token}");
    // Tungstenite fixes the callback's error type to an HTTP response.
    #[allow(clippy::result_large_err)]
    let callback = move |request: &UpgradeRequest, response: UpgradeResponse| {
        let status = if request.uri().path() != "/console" || request.uri().query().is_some() {
            Some(StatusCode::NOT_FOUND)
        } else if request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            != Some(authorization.as_str())
            || request.headers().contains_key(header::ORIGIN)
        {
            Some(StatusCode::UNAUTHORIZED)
        } else {
            None
        };
        if let Some(status) = status {
            let mut rejection =
                tungstenite::http::Response::new(Some("Console upgrade rejected".into()));
            *rejection.status_mut() = status;
            Err(rejection)
        } else {
            Ok(response)
        }
    };
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_REQUEST_BYTES))
        .max_frame_size(Some(MAX_REQUEST_BYTES))
        .write_buffer_size(0)
        .max_write_buffer_size(1024 * 1024);
    let mut socket = accept_hdr_with_config(stream, callback, Some(config))?;
    socket.get_mut().set_read_timeout(Some(IO_TIMEOUT))?;
    while !stop.load(Ordering::Acquire) {
        match socket.read() {
            Ok(Message::Text(text)) => {
                write_response(&mut socket, console.handle_text(&text))?;
            }
            Ok(Message::Binary(_)) => write_response(
                &mut socket,
                Response::from_result(
                    None,
                    Err(ConsoleError::new(
                        "invalid_request",
                        "Requests must be JSON text frames",
                    )),
                )
                .to_json(),
            )?,
            Ok(Message::Close(_)) => {
                // Sends the close reply that tungstenite queued.
                return match socket.flush() {
                    Ok(()) | Err(Error::ConnectionClosed) => Ok(()),
                    Err(err) => Err(err.into()),
                };
            }
            Err(Error::ConnectionClosed | Error::AlreadyClosed) => return Ok(()),
            Ok(Message::Ping(_) | Message::Pong(_)) => socket.flush()?,
            Ok(Message::Frame(_)) => {}
            Err(Error::Io(err))
                if matches!(
                    err.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
            Err(err) => return Err(err.into()),
        }
    }
    socket.close(None)?;
    Ok(())
}

fn write_response(
    socket: &mut WebSocket<TcpStream>,
    text: String,
) -> Result<(), Box<dyn std::error::Error>> {
    socket.send(Message::Text(text.into()))?;
    Ok(())
}
