#![cfg(feature = "remoting")]

use picard::{
    actors::{
        Actor, ActorFactoryArgs, ActorRefFactory, ActorReference, ActorSystem, BasicActorRef,
        Context, Sender,
    },
    remoting::{ActorAddress, PeerAuthenticator, RemotePeer},
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
