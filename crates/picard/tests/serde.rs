#![cfg(feature = "serde")]

use picard::actors::*;
use serde::{Deserialize, Serialize};
use std::{
    sync::mpsc::{self, Sender as ChannelSender},
    time::Duration,
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum Command {
    Greet { name: String },
    Count(u32),
}

struct Recorder(ChannelSender<Command>);

impl ActorFactoryArgs<ChannelSender<Command>> for Recorder {
    fn create_args(tx: ChannelSender<Command>) -> Self {
        Self(tx)
    }
}

impl Actor for Recorder {
    type Msg = Command;

    fn recv(&mut self, _: &Context<Self::Msg>, msg: Self::Msg, _: Sender) {
        let _ = self.0.send(msg);
    }
}

fn assert_message<T: Message>() {}

#[test]
fn messages_are_serializable() {
    assert_message::<Command>();
    assert_message::<String>();
    assert_message::<u32>();
    assert_message::<()>();
}

#[test]
fn message_survives_a_wire_round_trip() {
    let sys = ActorSystem::new().unwrap();
    let (tx, rx) = mpsc::channel();
    let actor = sys
        .actor_of_args::<Recorder, _>("serde-recorder", tx)
        .unwrap();

    let sent = Command::Greet {
        name: "Picard".into(),
    };
    let wire = serde_json::to_string(&sent).unwrap();
    let received: Command = serde_json::from_str(&wire).unwrap();
    actor.tell(received, None);

    assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), sent);
}

#[test]
fn runtime_actors_still_work() {
    let sys = ActorSystem::new().unwrap();
    let (tx, rx) = mpsc::channel();
    let actor = sys
        .actor_of_args::<Recorder, _>("serde-subscriber", tx)
        .unwrap();

    let chan: ChannelRef<Command> = channel("serde-channel", &sys).unwrap();
    chan.tell(
        Subscribe {
            actor: Box::new(actor.clone()),
            topic: "numbers".into(),
        },
        None,
    );
    sys.schedule_once(Duration::from_millis(50), actor, None, Command::Count(1));
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        Command::Count(1)
    );

    chan.tell(
        Publish {
            msg: Command::Count(2),
            topic: "numbers".into(),
        },
        None,
    );
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        Command::Count(2)
    );
}
