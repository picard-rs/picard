#![cfg(feature = "remoting")]

use picard::{
    actors::{
        Actor, ActorFactoryArgs, ActorRefFactory, ActorReference, ActorSystem, BasicActorRef,
        Context, Sender,
    },
    remoting::{
        ActorAddress, ClusterTransport, ClusterTransportOptions, PeerAuthenticator, RemotePeer,
    },
};
use serde::{Deserialize, Serialize};
use std::{
    sync::mpsc::{self, Sender as ChannelSender},
    time::{Duration, Instant},
};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct RemoteMessage(String);

struct Recorder(ChannelSender<RemoteMessage>);

impl ActorFactoryArgs<ChannelSender<RemoteMessage>> for Recorder {
    fn create_args(tx: ChannelSender<RemoteMessage>) -> Self {
        Self(tx)
    }
}

impl Actor for Recorder {
    type Msg = RemoteMessage;

    fn recv(&mut self, _: &Context<Self::Msg>, msg: Self::Msg, _: Sender) {
        let _ = self.0.send(msg);
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct AskMessage(String);

#[derive(Default)]
struct Echo;

impl Actor for Echo {
    type Msg = AskMessage;

    fn recv(&mut self, _: &Context<Self::Msg>, msg: Self::Msg, sender: Sender) {
        sender
            .expect("remote ask sender")
            .try_tell(format!("reply: {}", msg.0), None)
            .expect("reply should be accepted");
    }
}

#[derive(Default)]
struct Silent(Option<BasicActorRef>);

impl Actor for Silent {
    type Msg = AskMessage;

    fn recv(&mut self, _: &Context<Self::Msg>, _: Self::Msg, sender: Sender) {
        self.0 = sender;
    }
}

#[derive(Default)]
struct DropsReply;

impl Actor for DropsReply {
    type Msg = AskMessage;

    fn recv(&mut self, _: &Context<Self::Msg>, _: Self::Msg, _: Sender) {}
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ProcessingDelay(u64);

#[derive(Default)]
struct DelayedProcessor;

impl Actor for DelayedProcessor {
    type Msg = ProcessingDelay;

    fn recv(&mut self, _: &Context<Self::Msg>, msg: Self::Msg, _: Sender) {
        std::thread::sleep(Duration::from_millis(msg.0));
    }
}

#[test]
fn websocket_remoting_routes_a_serialized_message_to_an_actor() {
    let sender_system = ActorSystem::new().unwrap();
    let receiver_system = ActorSystem::new().unwrap();
    let (tx, rx) = mpsc::channel();
    let actor = receiver_system
        .actor_of_args::<Recorder, _>("remote-recorder", tx)
        .unwrap();
    let listener = receiver_system
        .listen_remote_actors(
            "127.0.0.1:0".parse().unwrap(),
            PeerAuthenticator::new([("sender".into(), "shared-token".into())]),
        )
        .unwrap();
    let endpoint = format!(
        "ws://{}/remoting",
        listener.local_addr().expect("WebSocket listener address")
    );
    let connection = sender_system
        .connect_remote(RemotePeer::new("receiver", endpoint.clone(), "shared-token").unwrap());
    let sent = RemoteMessage("hello over WebSocket".into());
    let destination =
        ActorAddress::remote(receiver_system.id(), endpoint, actor.path().to_string());

    let response = futures::executor::block_on(connection.send(
        destination,
        None,
        serde_json::to_value(&sent).unwrap(),
    ))
    .unwrap();

    assert!(response.accepted);
    assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), sent);
    listener.shutdown();
}

#[test]
fn websocket_remoting_can_wait_until_the_actor_finishes_processing() {
    let caller = ActorSystem::new().unwrap();
    let receiver = ActorSystem::new().unwrap();
    let actor = receiver.actor_of::<DelayedProcessor>("delayed").unwrap();
    let listener = receiver
        .listen_remote_actors(
            "127.0.0.1:0".parse().unwrap(),
            PeerAuthenticator::new([("caller".into(), "processed-token".into())]),
        )
        .unwrap();
    let endpoint = format!(
        "ws://{}/remoting",
        listener.local_addr().expect("WebSocket listener address")
    );
    let remote = caller
        .connect_remote(RemotePeer::new("receiver", endpoint.clone(), "processed-token").unwrap());
    let destination = ActorAddress::remote(receiver.id(), endpoint, actor.path().to_string());

    let started = Instant::now();
    futures::executor::block_on(remote.send_processed(
        destination,
        ProcessingDelay(60),
        Duration::from_secs(1),
    ))
    .unwrap();

    assert!(started.elapsed() >= Duration::from_millis(50));
    listener.shutdown();
}

#[test]
fn websocket_remoting_reports_a_processed_delivery_timeout() {
    let caller = ActorSystem::new().unwrap();
    let receiver = ActorSystem::new().unwrap();
    let actor = receiver.actor_of::<DelayedProcessor>("delayed").unwrap();
    let listener = receiver
        .listen_remote_actors(
            "127.0.0.1:0".parse().unwrap(),
            PeerAuthenticator::new([("caller".into(), "processed-token".into())]),
        )
        .unwrap();
    let endpoint = format!(
        "ws://{}/remoting",
        listener.local_addr().expect("WebSocket listener address")
    );
    let remote = caller
        .connect_remote(RemotePeer::new("receiver", endpoint.clone(), "processed-token").unwrap());
    let destination = ActorAddress::remote(receiver.id(), endpoint, actor.path().to_string());

    let result = futures::executor::block_on(remote.send_processed(
        destination,
        ProcessingDelay(80),
        Duration::from_millis(5),
    ));

    assert!(matches!(
        result,
        Err(picard::remoting::RemoteError::ProcessedTimeout)
    ));
    listener.shutdown();
}

#[test]
fn websocket_remoting_supports_typed_remote_ask() {
    let caller = ActorSystem::new().unwrap();
    let receiver = ActorSystem::new().unwrap();
    let actor = receiver.actor_of::<Echo>("echo").unwrap();
    let listener = receiver
        .listen_remote_actors(
            "127.0.0.1:0".parse().unwrap(),
            PeerAuthenticator::new([("caller".into(), "ask-token".into())]),
        )
        .unwrap();
    let endpoint = format!(
        "ws://{}/remoting",
        listener.local_addr().expect("WebSocket listener address")
    );
    let remote =
        caller.connect_remote(RemotePeer::new("receiver", endpoint.clone(), "ask-token").unwrap());
    let destination = ActorAddress::remote(receiver.id(), endpoint, actor.path().to_string());

    let reply = futures::executor::block_on(remote.ask::<_, String>(
        destination,
        AskMessage("hello".into()),
        Duration::from_secs(1),
    ))
    .unwrap();

    assert_eq!(reply, "reply: hello");
    listener.shutdown();
}

#[test]
fn websocket_remoting_times_out_when_remote_actor_does_not_reply() {
    let caller = ActorSystem::new().unwrap();
    let receiver = ActorSystem::new().unwrap();
    let actor = receiver.actor_of::<Silent>("silent").unwrap();
    let listener = receiver
        .listen_remote_actors(
            "127.0.0.1:0".parse().unwrap(),
            PeerAuthenticator::new([("caller".into(), "ask-token".into())]),
        )
        .unwrap();
    let endpoint = format!(
        "ws://{}/remoting",
        listener.local_addr().expect("WebSocket listener address")
    );
    let remote =
        caller.connect_remote(RemotePeer::new("receiver", endpoint.clone(), "ask-token").unwrap());
    let destination = ActorAddress::remote(receiver.id(), endpoint, actor.path().to_string());

    let started = Instant::now();
    let result = futures::executor::block_on(remote.ask::<_, String>(
        destination,
        AskMessage("no reply".into()),
        Duration::from_millis(20),
    ));
    let elapsed = started.elapsed();

    assert!(
        matches!(result, Err(picard::remoting::RemoteError::Timeout)),
        "unexpected result: {result:?}"
    );
    assert!(elapsed >= Duration::from_millis(15), "elapsed: {elapsed:?}");
    listener.shutdown();
}

#[test]
fn websocket_remoting_reports_a_dropped_reply_sender() {
    let caller = ActorSystem::new().unwrap();
    let receiver = ActorSystem::new().unwrap();
    let actor = receiver.actor_of::<DropsReply>("drops-reply").unwrap();
    let listener = receiver
        .listen_remote_actors(
            "127.0.0.1:0".parse().unwrap(),
            PeerAuthenticator::new([("caller".into(), "ask-token".into())]),
        )
        .unwrap();
    let endpoint = format!(
        "ws://{}/remoting",
        listener.local_addr().expect("WebSocket listener address")
    );
    let remote =
        caller.connect_remote(RemotePeer::new("receiver", endpoint.clone(), "ask-token").unwrap());
    let destination = ActorAddress::remote(receiver.id(), endpoint, actor.path().to_string());

    let result = futures::executor::block_on(remote.ask::<_, String>(
        destination,
        AskMessage("drop sender".into()),
        Duration::from_secs(1),
    ));

    assert!(matches!(
        result,
        Err(picard::remoting::RemoteError::NoReply)
    ));
    listener.shutdown();
}

#[test]
fn websocket_remoting_returns_a_transport_error_for_an_unreachable_peer() {
    let caller = ActorSystem::new().unwrap();
    let unavailable = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = unavailable.local_addr().unwrap();
    drop(unavailable);
    let endpoint = format!("ws://{address}/remoting");
    let remote = caller
        .connect_remote(RemotePeer::new("unreachable", endpoint.clone(), "unused-token").unwrap());

    let result = futures::executor::block_on(remote.ask::<_, String>(
        ActorAddress::remote(caller.id(), endpoint, "/user/missing"),
        AskMessage("unreachable".into()),
        Duration::from_secs(1),
    ));

    assert!(matches!(
        result,
        Err(picard::remoting::RemoteError::Transport(_))
    ));
}

#[test]
fn websocket_remoting_rejects_timeouts_over_sixty_seconds() {
    let caller = ActorSystem::new().unwrap();
    let remote = caller.connect_remote(
        RemotePeer::new("unreachable", "ws://127.0.0.1:1/remoting", "unused-token").unwrap(),
    );

    let result = futures::executor::block_on(remote.ask::<_, String>(
        ActorAddress::local(caller.id(), "/user/missing"),
        AskMessage("too long".into()),
        Duration::from_secs(61),
    ));

    assert!(matches!(
        result,
        Err(picard::remoting::RemoteError::TimeoutTooLong)
    ));
}

#[test]
fn cluster_transport_reuses_persistent_connection_and_batches() {
    use std::sync::Arc;

    let sender_system = ActorSystem::new().unwrap();
    let receiver_system = ActorSystem::new().unwrap();
    let (tx, rx) = mpsc::channel();
    let actor = receiver_system
        .actor_of_args::<Recorder, _>("cluster-recorder", tx)
        .unwrap();

    let transport = Arc::new(ClusterTransport::with_options(ClusterTransportOptions {
        max_batch_size: 16,
        batch_timeout: Duration::from_millis(5),
        ..ClusterTransportOptions::default()
    }));

    let listener = receiver_system
        .listen_remote_with(
            "127.0.0.1:0".parse().unwrap(),
            PeerAuthenticator::new([("sender".into(), "shared-token".into())]),
            receiver_system.remote_handler(),
            transport.clone(),
        )
        .unwrap();
    let endpoint = format!(
        "ws://{}/remoting",
        listener.local_addr().expect("WebSocket listener address")
    );

    let connection = sender_system.connect_remote_with(
        RemotePeer::new("receiver", endpoint.clone(), "shared-token").unwrap(),
        transport.clone(),
    );

    let destination =
        ActorAddress::remote(receiver_system.id(), endpoint, actor.path().to_string());

    // Send multiple messages; all should be accepted over the persistent connection
    for i in 0..10 {
        let sent = RemoteMessage(format!("msg-{i}"));
        let response = futures::executor::block_on(connection.send(
            destination.clone(),
            None,
            serde_json::to_value(&sent).unwrap(),
        ))
        .unwrap();
        assert!(response.accepted);
        assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), sent);
    }

    listener.shutdown();
    transport.close();
}

#[test]
fn cluster_transport_applies_backpressure_on_saturated_queue() {
    use std::sync::Arc;

    let sender_system = ActorSystem::new().unwrap();
    let transport = Arc::new(ClusterTransport::with_options(ClusterTransportOptions {
        queue_capacity: 1,
        max_batch_size: 1,
        backpressure_timeout: Duration::from_millis(10),
        reconnect_backoff: Duration::from_secs(1),
        request_timeout: Duration::from_secs(5),
        ..ClusterTransportOptions::default()
    }));

    // Connect to an unavailable endpoint so messages sit in queue while connecting
    let unavailable = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = unavailable.local_addr().unwrap();
    drop(unavailable);

    let connection = sender_system.connect_remote_with(
        RemotePeer::new("peer", format!("ws://{address}/remoting"), "token").unwrap(),
        transport.clone(),
    );

    let destination = ActorAddress::remote(
        sender_system.id(),
        format!("ws://{address}/remoting"),
        "/user/test",
    );

    // Launch multiple sends concurrently without awaiting
    let mut futures = Vec::new();
    for _ in 0..20 {
        futures.push(connection.send(destination.clone(), None, serde_json::json!({})));
    }

    // At least one send should fail with backpressure
    let mut backpressured = false;
    for f in futures {
        if let Err(err) = futures::executor::block_on(f)
            && err.to_string().contains("backpressure")
        {
            backpressured = true;
            break;
        }
    }
    assert!(
        backpressured,
        "expected backpressure error when queue was flooded"
    );
    transport.close();
}

#[cfg(feature = "console")]
#[test]
fn register_console_remote_peer_returns_peer_replies() {
    use std::sync::Arc;

    let sys_a = ActorSystem::new().unwrap();
    let sys_b = ActorSystem::new().unwrap();
    let (tx, rx) = mpsc::channel();
    let _actor = sys_b.actor_of_args::<Recorder, _>("b-worker", tx).unwrap();

    let transport = Arc::new(ClusterTransport::new());
    let listener = sys_b
        .listen_remote_with(
            "127.0.0.1:0".parse().unwrap(),
            PeerAuthenticator::new([("node-a".into(), "test-token-1234567890123456789012".into())]),
            sys_b.remote_handler(),
            transport.clone(),
        )
        .unwrap();
    let address = listener.local_addr().unwrap();

    let peer = RemotePeer::new(
        "node-b",
        format!("ws://{address}/remoting"),
        "test-token-1234567890123456789012",
    )
    .unwrap();
    let connection = sys_a.connect_remote_with(peer, transport.clone());

    sys_a.register_console_remote_peer("peer-b", connection);

    let console_a = sys_a.console();
    let reply = console_a.handle_text(
        r#"{"id":1,"op":"send","peer":"peer-b","path":"/user/b-worker","message":"hello from a"}"#,
    );
    let resp: picard::console::Response = serde_json::from_str(&reply).unwrap();
    assert_eq!(resp.id, Some(1));
    assert_eq!(resp.result.unwrap()["accepted"], true);
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        RemoteMessage("hello from a".into())
    );

    let reply = console_a.handle_text(
        r#"{"id":2,"op":"forward","peer":"peer-b","command":{"op":"inspect","path":"/user/b-worker"}}"#,
    );
    let resp: picard::console::Response = serde_json::from_str(&reply).unwrap();
    assert_eq!(resp.id, Some(2));
    assert_eq!(resp.result.unwrap()["path"], "/user/b-worker");

    let reply = console_a.handle_text(
        r#"{"id":3,"op":"send","peer":"peer-b","path":"/user/nope","message":"fail"}"#,
    );
    let resp: picard::console::Response = serde_json::from_str(&reply).unwrap();
    assert_eq!(resp.error.unwrap().code, "not_found");

    transport.close();
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
struct CounterMsg(i32);

struct MovableCounter {
    value: i32,
    tx: Option<ChannelSender<i32>>,
}

impl ActorFactoryArgs<(i32, Option<ChannelSender<i32>>)> for MovableCounter {
    fn create_args((value, tx): (i32, Option<ChannelSender<i32>>)) -> Self {
        Self { value, tx }
    }
}

impl Actor for MovableCounter {
    type Msg = CounterMsg;

    fn recv(&mut self, _: &Context<Self::Msg>, msg: Self::Msg, _: Sender) {
        self.value += msg.0;
        if let Some(ref tx) = self.tx {
            let _ = tx.send(self.value);
        }
    }
}

#[test]
fn migrate_actor_moves_state_and_forwards_messages_transparently() {
    use std::sync::Arc;

    let sys_a = ActorSystem::new().unwrap();
    let sys_b = ActorSystem::new().unwrap();

    let transport = Arc::new(ClusterTransport::new());

    let listener = sys_b
        .listen_remote_with(
            "127.0.0.1:0".parse().unwrap(),
            PeerAuthenticator::new([("node-a".into(), "test-token-1234567890123456789012".into())]),
            sys_b.remote_handler(),
            transport.clone(),
        )
        .unwrap();
    let address = listener.local_addr().unwrap();

    let (tx_b, rx_b) = mpsc::channel();
    let tx_b_clone = tx_b.clone();

    // Register migration handler on sys_b for MovableCounter
    sys_b.register_migration_handler(
        "MovableCounter",
        Arc::new(move |sys, payload| {
            let initial_val: i32 =
                serde_json::from_value(payload.state.clone()).map_err(|e| e.to_string())?;
            let name = payload
                .destination_path
                .split('/')
                .next_back()
                .unwrap_or("migrated");
            sys.actor_of_args::<MovableCounter, _>(name, (initial_val, Some(tx_b_clone.clone())))
                .map_err(|e| e.to_string())?;
            Ok(())
        }),
    );

    // Create counter on sys_a
    let actor_a = sys_a
        .actor_of_args::<MovableCounter, _>("counter", (10, None))
        .unwrap();

    let peer = RemotePeer::new(
        "node-b",
        format!("ws://{address}/remoting"),
        "test-token-1234567890123456789012",
    )
    .unwrap();
    let connection = sys_a.connect_remote_with(peer, transport.clone());

    // Migrate the actor from sys_a to sys_b
    sys_a
        .migrate_actor(
            "/user/counter",
            sys_b.id(),
            Some(format!("ws://{address}/remoting")),
            Some("/user/counter"),
            "MovableCounter",
            serde_json::to_value(10).unwrap(),
            &connection,
        )
        .expect("migration should succeed");

    // Existing actor_a reference on sys_a sends messages, which should be forwarded to sys_b!
    actor_a.send_msg(CounterMsg(5), None);
    actor_a.send_msg(CounterMsg(2), None);

    assert_eq!(
        rx_b.recv_timeout(Duration::from_secs(3)).unwrap(),
        15,
        "first message forwarded to node-b"
    );
    assert_eq!(
        rx_b.recv_timeout(Duration::from_secs(3)).unwrap(),
        17,
        "second message forwarded to node-b"
    );

    // Also verify remote tell sent to sys_a at /user/counter gets forwarded to sys_b
    let handler_a = sys_a.remote_handler();
    let req = picard::remoting::RemoteRequest::new(
        ActorAddress::local(sys_a.id(), "/user/counter"),
        None,
        serde_json::to_value(CounterMsg(10)).unwrap(),
    );
    let resp = handler_a("peer", req);
    assert!(resp.accepted);

    assert_eq!(
        rx_b.recv_timeout(Duration::from_secs(3)).unwrap(),
        27,
        "remote message to sys_a forwarded to sys_b"
    );

    listener.shutdown();
    transport.close();
}
