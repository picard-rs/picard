#![cfg(feature = "remoting")]

use picard::{
    actors::{
        Actor, ActorFactoryArgs, ActorRefFactory, ActorReference, ActorSystem, Context, Sender,
    },
    remoting::{ActorAddress, PeerAuthenticator, RemotePeer},
};
use serde::{Deserialize, Serialize};
use std::{
    sync::mpsc::{self, Sender as ChannelSender},
    time::Duration,
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
