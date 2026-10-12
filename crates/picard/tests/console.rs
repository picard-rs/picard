#![cfg(all(feature = "console", not(target_arch = "wasm32")))]

use std::{
    net::{SocketAddr, TcpStream},
    sync::mpsc::{self, Receiver, Sender as ChannelSender},
    time::Duration,
};

use picard::{
    actors::*,
    console::{ConsoleHandle, ConsoleOptions, Response},
};
use serde_json::{Value, json};
use tungstenite::{
    Message as Frame, WebSocket, client::IntoClientRequest, connect, stream::MaybeTlsStream,
};

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

#[derive(Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
enum Msg {
    Count(u32),
    Greet {
        name: String,
    },
    #[cfg(feature = "serde")]
    Unserializable(#[serde(serialize_with = "reject_serialization")] u32),
}

#[cfg(feature = "serde")]
fn reject_serialization<T: ?Sized, S: serde::Serializer>(_: &T, _: S) -> Result<S::Ok, S::Error> {
    Err(serde::ser::Error::custom(
        "intentional serialization failure",
    ))
}

#[derive(Debug, PartialEq)]
enum Event {
    Started,
    Received(Msg),
    Stopped,
}

struct Recorder(ChannelSender<Event>);

impl ActorFactoryArgs<ChannelSender<Event>> for Recorder {
    fn create_args(tx: ChannelSender<Event>) -> Self {
        Self(tx)
    }
}

impl Actor for Recorder {
    type Msg = Msg;

    fn pre_start(&mut self, _: &Context<Msg>) {
        self.0.send(Event::Started).unwrap();
    }

    fn recv(&mut self, _: &Context<Msg>, msg: Msg, _: Sender) {
        self.0.send(Event::Received(msg)).unwrap();
    }

    fn post_stop(&mut self) {
        self.0.send(Event::Stopped).unwrap();
    }
}

type Socket = WebSocket<MaybeTlsStream<TcpStream>>;

fn connect_to(
    address: SocketAddr,
    token: Option<&str>,
    path: &str,
) -> Result<Socket, tungstenite::Error> {
    let mut request = format!("ws://{address}{path}").into_client_request()?;
    if let Some(token) = token {
        request
            .headers_mut()
            .insert("Authorization", format!("Bearer {token}").parse().unwrap());
    }
    connect(request).map(|(socket, _)| socket)
}

struct Fixture {
    system: ActorSystem,
    actor: ActorRef<Msg>,
    events: Receiver<Event>,
    handle: Option<ConsoleHandle>,
    socket: Socket,
    id: u64,
}

impl Fixture {
    fn new() -> Self {
        let system = ActorSystem::new().unwrap();
        let (tx, events) = mpsc::channel();
        let actor = system.actor_of_args::<Recorder, _>("worker", tx).unwrap();
        assert_eq!(
            events.recv_timeout(Duration::from_secs(2)).unwrap(),
            Event::Started
        );
        let handle = system
            .start_console(ConsoleOptions {
                bind: "127.0.0.1:0".parse().unwrap(),
                token: TOKEN.into(),
            })
            .unwrap();
        let socket = connect_to(handle.local_addr(), Some(TOKEN), "/console").unwrap();
        if let MaybeTlsStream::Plain(stream) = socket.get_ref() {
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
        }
        Self {
            system,
            actor,
            events,
            handle: Some(handle),
            socket,
            id: 0,
        }
    }

    fn request(&mut self, mut command: Value) -> Response {
        self.id += 1;
        command["id"] = json!(self.id);
        self.socket
            .send(Frame::Text(command.to_string().into()))
            .unwrap();
        let Frame::Text(text) = self.socket.read().unwrap() else {
            panic!("Expected text response")
        };
        let response: Response = serde_json::from_str(&text).unwrap();
        assert_eq!(response.id, Some(self.id));
        assert_ne!(response.result.is_some(), response.error.is_some());
        response
    }

    fn command(&mut self, op: &str) -> Value {
        let response = self.request(json!({ "op": op, "path": "/user/worker" }));
        assert!(response.error.is_none(), "{:?}", response.error);
        response.result.unwrap()
    }

    fn event(&self) -> Event {
        self.events.recv_timeout(Duration::from_secs(2)).unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.stop().unwrap();
        }
        futures::executor::block_on(self.system.shutdown()).unwrap();
    }
}

#[test]
fn authenticates_upgrade_and_restricts_startup() {
    let fixture = Fixture::new();
    let address = fixture.handle.as_ref().unwrap().local_addr();
    for token in [None, Some("wrong")] {
        let Err(tungstenite::Error::Http(response)) = connect_to(address, token, "/console") else {
            panic!("Expected authentication failure");
        };
        assert_eq!(response.status(), 401);
    }
    assert!(connect_to(address, Some(TOKEN), "/wrong").is_err());
    assert!(connect_to(address, Some(TOKEN), "/console?token=secret").is_err());
    let mut request = format!("ws://{address}/console")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("Authorization", format!("Bearer {TOKEN}").parse().unwrap());
    request
        .headers_mut()
        .insert("Origin", "https://example.org".parse().unwrap());
    assert!(connect(request).is_err());
    for (bind, token) in [("0.0.0.0:0", TOKEN), ("127.0.0.1:0", "short")] {
        assert!(
            fixture
                .system
                .start_console(ConsoleOptions {
                    bind: bind.parse().unwrap(),
                    token: token.into()
                })
                .is_err()
        );
    }
    assert!(
        fixture
            .system
            .start_console(ConsoleOptions {
                bind: address,
                token: TOKEN.into()
            })
            .is_err()
    );
}

#[test]
fn lists_inspects_and_reports_protocol_errors() {
    let mut fixture = Fixture::new();
    let first = fixture.request(json!({ "op": "actors", "limit": 2 }));
    let page = first.result.unwrap();
    assert_eq!(page["actors"].as_array().unwrap().len(), 2);
    assert_eq!(page["next_offset"], 2);
    let response = fixture.request(json!({ "op": "actors" }));
    assert!(
        response.result.unwrap()["actors"]
            .as_array()
            .unwrap()
            .iter()
            .any(|actor| actor["path"] == "/user/worker")
    );
    let actor = fixture.command("inspect");
    assert_eq!(actor["parent"], "/user");
    assert_eq!(actor["mailbox"]["paused"], false);
    assert_eq!(actor["manageable"], true);
    assert_eq!(actor["json_send"], cfg!(feature = "serde"));
    for command in [
        json!({ "op": "inspect", "path": "/missing" }),
        json!({ "op": "unknown" }),
        json!({ "op": "actors", "limit": 0 }),
        json!({ "op": "actors", "limit": 101 }),
        json!({ "op": "pause", "path": "/system" }),
        json!({ "op": "pause", "path": "/user/worker", "extra": true }),
    ] {
        assert!(fixture.request(command).error.is_some());
    }
    fixture
        .socket
        .send(Frame::Text("{not json".into()))
        .unwrap();
    let Frame::Text(text) = fixture.socket.read().unwrap() else {
        panic!("Expected response")
    };
    let response: Response = serde_json::from_str(&text).unwrap();
    assert_eq!(response.id, None);
    assert_eq!(response.error.unwrap().code, "invalid_request");
    assert!(fixture.command("inspect").is_object());
    fixture
        .socket
        .send(Frame::Binary(vec![1, 2].into()))
        .unwrap();
    let Frame::Text(text) = fixture.socket.read().unwrap() else {
        panic!("Expected response")
    };
    let response: Response = serde_json::from_str(&text).unwrap();
    assert_eq!(response.error.unwrap().code, "invalid_request");
}

#[test]
fn pauses_clears_and_resumes_with_bounded_message_history() {
    let mut fixture = Fixture::new();
    assert_eq!(
        fixture
            .request(json!({ "op": "clear", "path": "/user/worker" }))
            .error
            .unwrap()
            .code,
        "not_paused"
    );
    fixture.command("pause");
    for number in 0..200 {
        fixture.actor.tell(Msg::Count(number), None);
    }
    assert!(
        fixture
            .events
            .recv_timeout(Duration::from_millis(50))
            .is_err()
    );
    assert_eq!(fixture.command("inspect")["mailbox"]["user_messages"], 200);
    let history = fixture.command("messages");
    let events = history["events"].as_array().unwrap();
    assert_eq!(events.len(), 64);
    assert_eq!(events[63]["event"], "enqueued");
    assert_eq!(
        events[63]["message_type"].as_str().unwrap(),
        std::any::type_name::<Msg>()
    );
    #[cfg(feature = "serde")]
    assert_eq!(events[63]["message"], json!({ "Count": 199 }));
    #[cfg(not(feature = "serde"))]
    assert!(events[63].get("message").is_none());
    assert_eq!(fixture.command("clear")["cleared"], 200);
    assert_eq!(fixture.command("inspect")["mailbox"]["user_messages"], 0);
    fixture.actor.tell(Msg::Count(201), None);
    fixture.command("resume");
    assert_eq!(fixture.event(), Event::Received(Msg::Count(201)));
    assert!(
        fixture
            .events
            .recv_timeout(Duration::from_millis(50))
            .is_err()
    );
}

#[test]
fn restart_preserves_pause_and_stop_removes_actor() {
    let mut fixture = Fixture::new();
    fixture.command("pause");
    fixture.actor.tell(Msg::Count(1), None);
    assert_eq!(fixture.command("restart")["accepted"], true);
    assert_eq!(fixture.event(), Event::Started);
    assert!(
        fixture
            .events
            .recv_timeout(Duration::from_millis(50))
            .is_err()
    );
    assert_eq!(fixture.command("inspect")["mailbox"]["paused"], true);
    fixture.command("resume");
    assert_eq!(fixture.event(), Event::Received(Msg::Count(1)));
    fixture.command("pause");
    assert_eq!(fixture.command("stop")["accepted"], true);
    assert_eq!(fixture.event(), Event::Stopped);
    for _ in 0..100 {
        let response = fixture.request(json!({ "op": "inspect", "path": "/user/worker" }));
        if response
            .error
            .as_ref()
            .is_some_and(|error| error.code == "not_found")
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("Terminated actor remained registered");
}

#[test]
fn json_send_obeys_actor_message_type_and_feature_gate() {
    let mut fixture = Fixture::new();
    fixture.actor.tell(
        Msg::Greet {
            name: "Typed".into(),
        },
        None,
    );
    assert_eq!(
        fixture.event(),
        Event::Received(Msg::Greet {
            name: "Typed".into()
        })
    );
    let response = fixture.request(json!({
        "op": "send", "path": "/user/worker", "message": { "Greet": { "name": "Picard" } },
    }));
    #[cfg(feature = "serde")]
    {
        assert!(response.error.is_none());
        assert_eq!(
            fixture.event(),
            Event::Received(Msg::Greet {
                name: "Picard".into()
            })
        );
        let response = fixture.request(
            json!({ "op": "send", "path": "/user/worker", "message": { "Count": "wrong type" } }),
        );
        assert_eq!(response.error.unwrap().code, "invalid_message");
        assert!(
            fixture
                .events
                .recv_timeout(Duration::from_millis(50))
                .is_err()
        );
        fixture.command("pause");
        let response = fixture
            .request(json!({ "op": "send", "path": "/user/worker", "message": { "Count": 42 } }));
        assert_eq!(response.result.unwrap()["accepted"], true);
        assert_eq!(fixture.command("inspect")["mailbox"]["user_messages"], 1);
        fixture.command("resume");
        assert_eq!(fixture.event(), Event::Received(Msg::Count(42)));
    }
    #[cfg(not(feature = "serde"))]
    assert_eq!(response.error.unwrap().code, "json_unavailable");
}

#[test]
fn shutdown_closes_listener_and_connected_clients() {
    let mut fixture = Fixture::new();
    let handle = fixture.handle.take().unwrap();
    let address = handle.local_addr();
    handle.stop().unwrap();
    assert!(TcpStream::connect(address).is_err());
    assert!(matches!(
        fixture.socket.read(),
        Ok(Frame::Close(_)) | Err(_)
    ));
}

#[test]
fn limits_concurrent_clients_and_handles_ping() {
    let fixture = Fixture::new();
    let address = fixture.handle.as_ref().unwrap().local_addr();
    let mut clients: Vec<_> = (0..15)
        .map(|_| connect_to(address, Some(TOKEN), "/console").unwrap())
        .collect();
    assert!(connect_to(address, Some(TOKEN), "/console").is_err());
    let socket = &mut clients[0];
    socket.send(Frame::Ping(vec![1, 2, 3].into())).unwrap();
    assert_eq!(socket.read().unwrap(), Frame::Pong(vec![1, 2, 3].into()));
}

#[cfg(feature = "serde")]
#[test]
fn large_or_unserializable_payloads_do_not_prevent_delivery() {
    let mut fixture = Fixture::new();
    fixture.command("pause");
    let name = "x".repeat(8192);
    fixture.actor.tell(Msg::Greet { name: name.clone() }, None);
    fixture.actor.tell(Msg::Unserializable(7), None);
    let history = fixture.command("messages");
    let events = history["events"].as_array().unwrap();
    assert!(
        events[0]["payload_error"]
            .as_str()
            .unwrap()
            .contains("4096")
    );
    assert!(events[0].get("message").is_none());
    assert!(
        events[1]["payload_error"]
            .as_str()
            .unwrap()
            .contains("intentional serialization failure")
    );
    fixture.command("resume");
    assert_eq!(fixture.event(), Event::Received(Msg::Greet { name }));
    assert_eq!(fixture.event(), Event::Received(Msg::Unserializable(7)));
}

struct Parent {
    events: ChannelSender<Event>,
    child: ChannelSender<ActorRef<Msg>>,
}

impl ActorFactoryArgs<(ChannelSender<Event>, ChannelSender<ActorRef<Msg>>)> for Parent {
    fn create_args((events, child): (ChannelSender<Event>, ChannelSender<ActorRef<Msg>>)) -> Self {
        Self { events, child }
    }
}

impl Actor for Parent {
    type Msg = ();

    fn pre_start(&mut self, ctx: &Context<()>) {
        let child = ctx
            .actor_of_args::<Recorder, _>("child", self.events.clone())
            .unwrap();
        self.child.send(child).unwrap();
    }

    fn recv(&mut self, _: &Context<()>, (): (), _: Sender) {}
}

#[test]
fn registers_public_system_temporary_props_and_child_actors() {
    let mut fixture = Fixture::new();
    let (tx, events) = mpsc::channel();
    let (child_tx, child_rx) = mpsc::channel();
    let _parent = fixture
        .system
        .actor_of_args::<Parent, _>("parent", (tx.clone(), child_tx))
        .unwrap();
    let from_props = fixture
        .system
        .actor_of_props("from-props", Props::new_args::<Recorder, _>(tx.clone()))
        .unwrap();
    let system_actor = fixture
        .system
        .sys_actor_of_args::<Recorder, _>("custom", tx.clone())
        .unwrap();
    let temporary_actor = fixture.system.tmp_actor_of_args::<Recorder, _>(tx).unwrap();
    let child = child_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let actors = [from_props, system_actor, temporary_actor, child];
    for _ in &actors {
        assert_eq!(
            events.recv_timeout(Duration::from_secs(2)).unwrap(),
            Event::Started
        );
    }

    for actor in actors {
        let path = actor.path().to_string();
        let response = fixture.request(json!({ "op": "inspect", "path": path }));
        let snapshot = response.result.unwrap();
        assert_eq!(snapshot["manageable"], true);
        assert_eq!(snapshot["json_send"], cfg!(feature = "serde"));
        assert!(
            fixture
                .request(json!({ "op": "pause", "path": path }))
                .error
                .is_none()
        );
        let response =
            fixture.request(json!({ "op": "send", "path": path, "message": { "Count": 42 } }));
        #[cfg(feature = "serde")]
        assert!(response.error.is_none());
        #[cfg(not(feature = "serde"))]
        {
            assert_eq!(response.error.unwrap().code, "json_unavailable");
            actor.tell(Msg::Count(42), None);
        }
        assert!(
            fixture
                .request(json!({ "op": "resume", "path": path }))
                .error
                .is_none()
        );
        assert_eq!(
            events.recv_timeout(Duration::from_secs(2)).unwrap(),
            Event::Received(Msg::Count(42))
        );
        assert!(
            fixture
                .request(json!({ "op": "stop", "path": path }))
                .error
                .is_none()
        );
        assert_eq!(
            events.recv_timeout(Duration::from_secs(2)).unwrap(),
            Event::Stopped
        );
    }
}

#[cfg(feature = "serde")]
struct StartupRecorder(ChannelSender<String>);

#[cfg(feature = "serde")]
impl ActorFactoryArgs<ChannelSender<String>> for StartupRecorder {
    fn create_args(tx: ChannelSender<String>) -> Self {
        Self(tx)
    }
}

#[cfg(feature = "serde")]
impl Actor for StartupRecorder {
    type Msg = String;

    fn pre_start(&mut self, ctx: &Context<String>) {
        ctx.myself().tell("startup".to_owned(), None);
    }

    fn recv(&mut self, _: &Context<String>, message: String, _: Sender) {
        self.0.send(message).unwrap();
    }
}

#[cfg(feature = "serde")]
#[test]
fn json_codec_is_installed_before_actor_startup() {
    let mut fixture = Fixture::new();
    let (tx, rx) = mpsc::channel();
    let _actor = fixture
        .system
        .actor_of_args::<StartupRecorder, _>("startup", tx)
        .unwrap();
    assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), "startup");
    let response = fixture.request(json!({ "op": "messages", "path": "/user/startup" }));
    let result = response.result.unwrap();
    let events = result["events"].as_array().unwrap();
    assert_eq!(events.len(), 2);
    assert!(events.iter().all(|event| event["message"] == "startup"));
}

#[test]
fn routes_requests_through_application_owned_transport() {
    use picard::console::{
        Command, MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES, Request, is_valid_token,
    };

    let system = ActorSystem::new().unwrap();
    let (tx, events) = mpsc::channel();
    let actor = system.actor_of_args::<Recorder, _>("worker", tx).unwrap();
    assert_eq!(
        events.recv_timeout(Duration::from_secs(2)).unwrap(),
        Event::Started
    );
    let console = system.console();

    let response: Response = serde_json::from_str(
        &console.handle_text(r#"{"id":7,"op":"inspect","path":"/user/worker"}"#),
    )
    .unwrap();
    assert_eq!(response.id, Some(7));
    assert_eq!(response.result.unwrap()["path"], "/user/worker");

    let response = console.clone().handle(Request {
        id: 8,
        command: Command::Pause {
            path: "/user/worker".into(),
        },
    });
    assert_eq!(response.id, Some(8));
    assert!(response.error.is_none(), "{:?}", response.error);
    actor.tell(Msg::Count(1), None);
    assert!(events.recv_timeout(Duration::from_millis(200)).is_err());
    console.handle(Request {
        id: 9,
        command: Command::Resume {
            path: "/user/worker".into(),
        },
    });
    assert_eq!(
        events.recv_timeout(Duration::from_secs(2)).unwrap(),
        Event::Received(Msg::Count(1))
    );

    for (text, id) in [
        (r#"{"id":3,"op":"unknown"}"#.to_owned(), Some(3)),
        ("not json".to_owned(), None),
        (" ".repeat(MAX_REQUEST_BYTES + 1), None),
    ] {
        let response: Response = serde_json::from_str(&console.handle_text(&text)).unwrap();
        assert_eq!(response.id, id);
        assert_eq!(response.error.unwrap().code, "invalid_request");
    }

    let oversized = Response::from_result(Some(1), Ok(json!("x".repeat(MAX_RESPONSE_BYTES))));
    let response: Response = serde_json::from_str(&oversized.to_json()).unwrap();
    assert_eq!(response.error.unwrap().code, "response_too_large");

    assert!(is_valid_token(TOKEN));
    assert!(!is_valid_token("short"));
    assert!(!is_valid_token(&format!("{TOKEN} ")));
    futures::executor::block_on(system.shutdown()).unwrap();
}

#[test]
fn send_can_target_a_registered_remote_peer() {
    let mut fixture = Fixture::new();
    let (tx, rx) = mpsc::channel();
    fixture.system.register_console_peer(
        "district-1",
        std::sync::Arc::new(move |path, message| {
            if path == "/user/refused" {
                return Err("peer refused".into());
            }
            tx.send((path.to_owned(), message)).unwrap();
            Ok(())
        }),
    );

    let peers = fixture.request(json!({ "op": "peers" })).result.unwrap();
    assert_eq!(peers["peers"], json!(["district-1"]));

    let response = fixture.request(json!({
        "op": "send", "peer": "district-1", "path": "/user/gateway", "message": { "Ping": 1 }
    }));
    assert!(response.error.is_none(), "{:?}", response.error);
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        ("/user/gateway".to_owned(), json!({ "Ping": 1 }))
    );

    for (peer, path, code) in [
        ("missing", "/user/gateway", "not_found"),
        ("district-1", "/user/refused", "remote_send"),
        ("district-1", "relative", "invalid_request"),
    ] {
        let response =
            fixture.request(json!({ "op": "send", "peer": peer, "path": path, "message": 1 }));
        assert_eq!(response.error.unwrap().code, code);
    }
}

#[test]
fn forward_runs_commands_on_a_registered_peer_console() {
    let mut fixture = Fixture::new();
    let remote = ActorSystem::new().unwrap();
    let remote_console = remote.console();
    fixture.system.register_console_peer_query(
        "remote",
        std::sync::Arc::new(move |request| {
            assert!(!request.contains("forward"), "{request}");
            Ok(remote_console.handle_text(request))
        }),
    );
    let peers = fixture.request(json!({ "op": "peers" })).result.unwrap();
    assert_eq!(peers["peers"], json!(["remote"]));

    let response = fixture
        .request(json!({ "op": "forward", "peer": "remote", "command": { "op": "actors" } }));
    let actors = response.result.expect("remote actor list");
    let paths: Vec<_> = actors["actors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|actor| actor["path"].as_str().unwrap().to_owned())
        .collect();
    assert!(!paths.contains(&"/user/worker".to_owned()), "{paths:?}");

    for (command, code) in [
        (
            json!({ "op": "inspect", "path": "/user/worker" }),
            "not_found",
        ),
        (
            json!({ "op": "send", "peer": "other", "path": "/user/x", "message": 1 }),
            "invalid_request",
        ),
        (
            json!({ "op": "forward", "peer": "remote", "command": { "op": "actors" } }),
            "invalid_request",
        ),
    ] {
        let response =
            fixture.request(json!({ "op": "forward", "peer": "remote", "command": command }));
        assert_eq!(response.error.unwrap().code, code);
    }
    let missing =
        fixture.request(json!({ "op": "forward", "peer": "nope", "command": { "op": "actors" } }));
    assert_eq!(missing.error.unwrap().code, "not_found");
    futures::executor::block_on(remote.shutdown()).unwrap();
}
