//! Runtime inspection and control protocol for the `console` feature.
//!
//! The WebSocket server is available on native targets and is never started implicitly.
//! Applications that own their transport can route JSON requests through
//! [`ActorSystem::console`] instead; they are then responsible for authentication.

use std::{
    any::Any,
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    Envelope, LocalMessage,
    actor::{ActorRef, ActorReference, BasicActorRef},
    kernel::mailbox::{Mailbox, MailboxSchedule},
    system::{ActorSystem, SystemCmd, SystemMsg},
};

mod server;
pub use server::{ConsoleHandle, ConsoleOptions};

/// Largest request accepted by [`Console::handle_text`] and the built-in endpoint.
pub const MAX_REQUEST_BYTES: usize = 64 * 1024;
/// Serialized responses above this size are replaced by a `response_too_large` error.
pub const MAX_RESPONSE_BYTES: usize = 1024 * 1024 - 1024;

const HISTORY_LIMIT: usize = 64;
#[cfg(feature = "serde")]
const PAYLOAD_LIMIT: usize = 4096;

/// One request and its client-selected correlation ID.
#[derive(Debug, Deserialize, Serialize)]
pub struct Request {
    pub id: u64,
    #[serde(flatten)]
    pub command: Command,
}

/// Console operations. Paths are absolute, such as `/user/worker`.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    Actors {
        #[serde(default)]
        offset: usize,
        #[serde(default = "default_page_limit")]
        limit: usize,
    },
    Inspect {
        path: String,
    },
    Messages {
        path: String,
    },
    Pause {
        path: String,
    },
    Resume {
        path: String,
    },
    Clear {
        path: String,
    },
    Stop {
        path: String,
    },
    Restart {
        path: String,
    },
    /// Send a JSON message to a local actor, or to an actor of the named remote `peer`
    /// registered with [`ActorSystem::register_console_peer`].
    Send {
        path: String,
        message: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        peer: Option<String>,
    },
    /// List the remote peers that [`Command::Send`] and [`Command::Forward`] can target.
    Peers,
    /// Run `command` on the console of the remote `peer`, registered with
    /// [`ActorSystem::register_console_peer_query`], and return its result.
    Forward {
        peer: String,
        command: Box<Command>,
    },
}

fn default_page_limit() -> usize {
    100
}

/// Exactly one of `result` and `error` is present in a response.
#[derive(Debug, Deserialize, Serialize)]
pub struct Response {
    pub id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ConsoleError>,
}

/// A protocol error, also used for failed management operations.
#[derive(Debug, Deserialize, Serialize)]
pub struct ConsoleError {
    pub code: String,
    pub message: String,
}

impl ConsoleError {
    pub(crate) fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

/// Transport-agnostic console service, obtained with [`ActorSystem::console`].
///
/// Use it to serve the console protocol over an application-owned channel, such as a route
/// on an existing web server. The service performs no authentication: only route requests
/// from trusted, authenticated clients, and apply [`MAX_REQUEST_BYTES`] before buffering.
#[derive(Clone)]
pub struct Console {
    system: ActorSystem,
}

impl Console {
    /// Execute one typed request.
    pub fn handle(&self, request: Request) -> Response {
        Response::from_result(
            Some(request.id),
            self.system.provider.console().execute(request.command),
        )
    }

    /// Execute one JSON request and return the JSON response text.
    ///
    /// Malformed or oversized requests produce an `invalid_request` error response, and
    /// responses larger than [`MAX_RESPONSE_BYTES`] are replaced by `response_too_large`.
    pub fn handle_text(&self, text: &str) -> String {
        let response = if text.len() > MAX_REQUEST_BYTES {
            Response::from_result(
                None,
                Err(ConsoleError::new(
                    "invalid_request",
                    format!("Request exceeds {MAX_REQUEST_BYTES} bytes"),
                )),
            )
        } else {
            let parsed = serde_json::from_str::<Value>(text);
            let id = parsed
                .as_ref()
                .ok()
                .and_then(|value| value.get("id"))
                .and_then(Value::as_u64);
            match parsed.and_then(serde_json::from_value::<Request>) {
                Ok(request) => self.handle(request),
                Err(err) => Response::from_result(
                    id,
                    Err(ConsoleError::new("invalid_request", err.to_string())),
                ),
            }
        };
        response.to_json()
    }
}

/// Delivers a JSON message to the actor at the given path on a remote system.
///
/// Returns once the message is accepted for delivery; the error text is shown to the console
/// user, so it must not contain secrets.
pub type ConsolePeerSender = Arc<dyn Fn(&str, Value) -> Result<(), String> + Send + Sync>;

/// Runs one console request, given as JSON text, on a remote system and returns the
/// JSON text of its response. The error text is shown to the console user.
pub type ConsolePeerQuery = Arc<dyn Fn(&str) -> Result<String, String> + Send + Sync>;

impl ActorSystem {
    /// Let the console run commands on the console of a remote system: `forward` requests
    /// naming `name` are serialized as a [`Request`] and passed to `query`. The remote
    /// side should answer with [`Console::handle_text`] and must not accept
    /// [`Command::Forward`] itself.
    pub fn register_console_peer_query(&self, name: impl Into<String>, query: ConsolePeerQuery) {
        self.provider.console().register_query(name.into(), query);
    }

    /// Make a remote system reachable from the console: `send` requests naming `name` as
    /// their `peer` are passed to `sender`. Replaces an earlier peer of the same name.
    pub fn register_console_peer(&self, name: impl Into<String>, sender: ConsolePeerSender) {
        self.provider.console().register_peer(name.into(), sender);
    }

    /// A console service for routing requests over an application-owned transport.
    pub fn console(&self) -> Console {
        Console {
            system: self.clone(),
        }
    }
}

impl Response {
    /// Build a response from an operation result.
    pub fn from_result(id: Option<u64>, result: Result<Value, ConsoleError>) -> Self {
        match result {
            Ok(result) => Self {
                id,
                result: Some(result),
                error: None,
            },
            Err(error) => Self {
                id,
                result: None,
                error: Some(error),
            },
        }
    }

    /// Serialize the response, enforcing [`MAX_RESPONSE_BYTES`].
    pub fn to_json(&self) -> String {
        match serde_json::to_string(self) {
            Ok(text) if text.len() <= MAX_RESPONSE_BYTES => text,
            Ok(_) => Self::from_result(
                self.id,
                Err(ConsoleError::new(
                    "response_too_large",
                    "Response exceeds one MiB; request a smaller actor page",
                )),
            )
            .to_json(),
            Err(err) => Self::from_result(
                self.id,
                Err(ConsoleError::new("serialization", err.to_string())),
            )
            .to_json(),
        }
    }
}

/// Whether `token` meets the console's bearer token requirements: at least 32 ASCII
/// letters, digits, `-` or `_`. Custom transports can use it to apply the same policy.
pub fn is_valid_token(token: &str) -> bool {
    token.len() >= 32
        && token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

#[derive(Default)]
pub(crate) struct Registry {
    actors: DashMap<String, Arc<dyn ConsoleActor>>,
    peers: DashMap<String, ConsolePeerSender>,
    queries: DashMap<String, ConsolePeerQuery>,
}

impl Registry {
    pub(crate) fn register_query(&self, name: String, query: ConsolePeerQuery) {
        self.queries.insert(name, query);
    }

    fn forward(&self, peer: &str, command: Command) -> Result<Value, ConsoleError> {
        if matches!(command, Command::Forward { .. } | Command::Send { .. }) {
            return Err(ConsoleError::new(
                "invalid_request",
                "Only inspection and management commands can be forwarded; use send with a peer",
            ));
        }
        let query = self
            .queries
            .get(peer)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or_else(|| ConsoleError::new("not_found", format!("No remote peer {peer}")))?;
        let text = serde_json::to_string(&Request { id: 0, command })
            .map_err(|err| ConsoleError::new("serialization", err.to_string()))?;
        let reply = query(&text).map_err(|err| ConsoleError::new("remote_query", err))?;
        let response: Response = serde_json::from_str(&reply)
            .map_err(|err| ConsoleError::new("remote_query", format!("Invalid reply: {err}")))?;
        match (response.result, response.error) {
            (Some(result), None) => Ok(result),
            (None, Some(error)) => Err(error),
            _ => Err(ConsoleError::new("remote_query", "Invalid remote response")),
        }
    }

    pub(crate) fn register_peer(&self, name: String, sender: ConsolePeerSender) {
        self.peers.insert(name, sender);
    }

    fn send_to_peer(&self, peer: &str, path: &str, message: Value) -> Result<Value, ConsoleError> {
        let sender = self
            .peers
            .get(peer)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or_else(|| ConsoleError::new("not_found", format!("No remote peer {peer}")))?;
        sender(path, message).map_err(|err| ConsoleError::new("remote_send", err))?;
        Ok(json!({ "queued": true, "peer": peer, "path": path }))
    }

    pub(crate) fn register<Msg: LocalMessage>(&self, actor: ActorRef<Msg>, mailbox: Mailbox<Msg>) {
        self.actors.insert(
            actor.path().to_string(),
            Arc::new(TypedActor {
                actor: actor.into(),
                mailbox,
            }),
        );
    }

    pub(crate) fn remove(&self, path: &str) {
        self.actors.remove(path);
    }

    pub(crate) fn enable_management(&self, path: &str) {
        if let Some(actor) = self.actors.get(path) {
            actor.enable_management();
        }
    }

    #[cfg(feature = "serde")]
    pub(crate) fn enable_json<Msg: crate::Message>(&self, path: &str) {
        if let Some(actor) = self.actors.get(path) {
            actor.configure_json(JsonCodec {
                encode: |message| {
                    let message = message.downcast_ref::<Msg>().ok_or_else(|| {
                        ConsoleError::new("message_type", "Message type did not match the actor")
                    })?;
                    let mut writer = PayloadWriter(Vec::new());
                    serde_json::to_writer(&mut writer, message)
                        .map_err(|err| ConsoleError::new("serialization", err.to_string()))?;
                    serde_json::from_slice(&writer.0)
                        .map_err(|err| ConsoleError::new("serialization", err.to_string()))
                },
                decode: |value| {
                    serde_json::from_value::<Msg>(value)
                        .map(crate::AnyMessage::new)
                        .map_err(|err| ConsoleError::new("invalid_message", err.to_string()))
                },
            });
        }
    }

    pub(crate) fn execute(&self, command: Command) -> Result<Value, ConsoleError> {
        let command = match command {
            Command::Forward { peer, command } => return self.forward(&peer, *command),
            command => command,
        };
        let path = match &command {
            Command::Forward { .. } => unreachable!("forwarded above"),
            Command::Peers => {
                let mut peers: Vec<String> = self
                    .peers
                    .iter()
                    .map(|entry| entry.key().clone())
                    .chain(self.queries.iter().map(|entry| entry.key().clone()))
                    .collect();
                peers.sort();
                peers.dedup();
                return Ok(json!({ "peers": peers }));
            }
            Command::Send {
                path,
                message,
                peer: Some(peer),
            } => {
                if !path.starts_with('/') {
                    return Err(ConsoleError::new(
                        "invalid_request",
                        "Actor path must be absolute",
                    ));
                }
                return self.send_to_peer(peer, path, message.clone());
            }
            Command::Actors { offset, limit } => {
                if !(1..=100).contains(limit) {
                    return Err(ConsoleError::new(
                        "invalid_request",
                        "Actor page limit must be between 1 and 100",
                    ));
                }
                let mut actors: Vec<_> = self
                    .actors
                    .iter()
                    .map(|entry| (entry.key().clone(), Arc::clone(entry.value())))
                    .collect();
                actors.sort_by(|a, b| a.0.cmp(&b.0));
                let total = actors.len();
                let page: Vec<_> = actors
                    .into_iter()
                    .skip(*offset)
                    .take(*limit)
                    .map(|(_, actor)| actor.snapshot())
                    .collect();
                let end = offset.saturating_add(page.len());
                let next_offset = (end < total).then_some(end);
                return Ok(json!({ "actors": page, "total": total, "next_offset": next_offset }));
            }
            Command::Inspect { path }
            | Command::Messages { path }
            | Command::Pause { path }
            | Command::Resume { path }
            | Command::Clear { path }
            | Command::Stop { path }
            | Command::Restart { path }
            | Command::Send { path, .. } => path,
        };
        let actor = self
            .actors
            .get(path)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or_else(|| ConsoleError::new("not_found", format!("No actor at {path}")))?;
        match command {
            Command::Inspect { .. } => Ok(actor.snapshot()),
            Command::Messages { .. } => Ok(actor.messages()),
            Command::Pause { .. } => actor.pause(true),
            Command::Resume { .. } => actor.pause(false),
            Command::Clear { .. } => actor.clear(),
            Command::Stop { .. } => actor.lifecycle(SystemCmd::Stop),
            Command::Restart { .. } => actor.lifecycle(SystemCmd::Restart),
            Command::Send { message, .. } => actor.send(message),
            Command::Actors { .. } | Command::Peers | Command::Forward { .. } => unreachable!(),
        }
    }
}

trait ConsoleActor: Send + Sync {
    fn snapshot(&self) -> Value;
    fn messages(&self) -> Value;
    fn pause(&self, paused: bool) -> Result<Value, ConsoleError>;
    fn clear(&self) -> Result<Value, ConsoleError>;
    fn lifecycle(&self, command: SystemCmd) -> Result<Value, ConsoleError>;
    fn send(&self, message: Value) -> Result<Value, ConsoleError>;
    fn enable_management(&self);
    #[cfg(feature = "serde")]
    fn configure_json(&self, codec: JsonCodec);
}

struct TypedActor<Msg: LocalMessage> {
    actor: BasicActorRef,
    mailbox: Mailbox<Msg>,
}

impl<Msg: LocalMessage> TypedActor<Msg> {
    fn check_management(&self) -> Result<(), ConsoleError> {
        if !self.mailbox.diagnostics().lock().unwrap().managed {
            return Err(ConsoleError::new(
                "protected_actor",
                "Runtime actors can be inspected but cannot be managed through the console",
            ));
        }
        Ok(())
    }
}

impl<Msg: LocalMessage> ConsoleActor for TypedActor<Msg> {
    fn snapshot(&self) -> Value {
        let state = self.mailbox.diagnostics().lock().unwrap();
        json!({
            "path": self.actor.path().as_str(),
            "parent": self.actor.parent().path().as_str(),
            "message_type": std::any::type_name::<Msg>(),
            "json_send": state.codec.is_some(),
            "manageable": state.managed,
            "mailbox": {
                "user_messages": self.mailbox.user_queue_len(),
                "system_messages": self.mailbox.system_queue_len(),
                "paused": state.paused,
                "suspended": self.mailbox.is_suspended(),
                "scheduled": self.mailbox.is_scheduled(),
            },
        })
    }

    fn messages(&self) -> Value {
        let state = self.mailbox.diagnostics().lock().unwrap();
        json!({ "limit": HISTORY_LIMIT, "events": state.history })
    }

    fn pause(&self, paused: bool) -> Result<Value, ConsoleError> {
        self.check_management()?;
        self.mailbox.diagnostics().lock().unwrap().paused = paused;
        if !paused {
            self.actor.cell.kernel().schedule();
        }
        Ok(json!({ "paused": paused }))
    }

    fn clear(&self) -> Result<Value, ConsoleError> {
        self.check_management()?;
        let mut state = self.mailbox.diagnostics().lock().unwrap();
        if !state.paused {
            return Err(ConsoleError::new(
                "not_paused",
                "Pause the mailbox before clearing it",
            ));
        }
        let mut cleared = 0;
        let mut discarded = Vec::new();
        while let Ok(envelope) = self.mailbox.try_dequeue() {
            state.push(metadata(&envelope, "cleared"));
            discarded.push(envelope);
            cleared += 1;
        }
        // Message destructors may send messages; never run them under the mailbox lock.
        drop(state);
        drop(discarded);
        Ok(json!({ "cleared": cleared }))
    }

    fn lifecycle(&self, command: SystemCmd) -> Result<Value, ConsoleError> {
        self.check_management()?;
        self.actor
            .cell
            .send_sys_msg(Envelope {
                msg: SystemMsg::Command(command),
                sender: None,
            })
            .map_err(|_| ConsoleError::new("delivery_failed", "Actor has terminated"))?;
        Ok(json!({ "accepted": true }))
    }

    fn send(&self, message: Value) -> Result<Value, ConsoleError> {
        self.check_management()?;
        let codec = self.mailbox.diagnostics().lock().unwrap().codec;
        let codec = codec.ok_or_else(|| {
            ConsoleError::new(
                "json_unavailable",
                "JSON sends require console + serde and an actor created with a Message bound",
            )
        })?;
        let mut message = (codec.decode)(message)?;
        self.actor
            .cell
            .send_any_msg(&mut message, None)
            .map_err(|_| ConsoleError::new("delivery_failed", "Actor has terminated"))?;
        Ok(json!({ "accepted": true }))
    }

    fn enable_management(&self) {
        self.mailbox.diagnostics().lock().unwrap().managed = true;
    }

    #[cfg(feature = "serde")]
    fn configure_json(&self, codec: JsonCodec) {
        self.mailbox.diagnostics().lock().unwrap().codec = Some(codec);
    }
}

#[derive(Clone, Copy)]
pub(crate) struct JsonCodec {
    encode: fn(&dyn Any) -> Result<Value, ConsoleError>,
    decode: fn(Value) -> Result<crate::AnyMessage, ConsoleError>,
}

#[derive(Default)]
pub(crate) struct MailboxDiagnostics {
    pub(crate) paused: bool,
    history: VecDeque<Value>,
    codec: Option<JsonCodec>,
    managed: bool,
}

impl MailboxDiagnostics {
    pub(crate) fn push(&mut self, entry: Value) {
        if self.history.len() == HISTORY_LIMIT {
            self.history.pop_front();
        }
        self.history.push_back(entry);
    }
}

pub(crate) type SharedDiagnostics = Arc<Mutex<MailboxDiagnostics>>;

fn metadata<Msg: LocalMessage>(envelope: &Envelope<Msg>, event: &str) -> Value {
    json!({
        "event": event,
        "message_type": std::any::type_name::<Msg>(),
        "sender": envelope.sender.as_ref().map(|sender| sender.path().as_str()),
    })
}

pub(crate) fn message_event<Msg: LocalMessage>(
    diagnostics: &SharedDiagnostics,
    envelope: &Envelope<Msg>,
    event: &str,
) -> Value {
    let mut entry = metadata(envelope, event);
    let codec = diagnostics.lock().unwrap().codec;
    // User-defined serializers must not execute under a runtime lock.
    if let Some(codec) = codec {
        match (codec.encode)(&envelope.msg) {
            Ok(payload) => entry["message"] = payload,
            Err(err) => {
                entry["payload_error"] = json!(err.message.chars().take(256).collect::<String>());
            }
        }
    }
    entry
}

#[cfg(feature = "serde")]
struct PayloadWriter(Vec<u8>);

#[cfg(feature = "serde")]
impl std::io::Write for PayloadWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > PAYLOAD_LIMIT - self.0.len() {
            return Err(std::io::Error::other("Payload exceeds 4096 bytes"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
