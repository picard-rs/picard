use picard::actors::*;
use std::{
    sync::mpsc::{self, Sender as StdSender},
    time::Duration,
};

struct Subscriber(StdSender<String>);

impl ActorFactoryArgs<StdSender<String>> for Subscriber {
    fn create_args(sender: StdSender<String>) -> Self {
        Self(sender)
    }
}

impl Actor for Subscriber {
    type Msg = String;

    fn recv(&mut self, _: &Context<Self::Msg>, msg: Self::Msg, _: Sender) {
        self.0.send(msg).unwrap();
    }
}

#[test]
fn publishes_to_matching_and_wildcard_subscribers() {
    let system = ActorSystem::new().unwrap();
    let channel: ChannelRef<String> = channel("test-channel", &system).unwrap();

    let (news_tx, news_rx) = mpsc::channel();
    let news_subscriber = system
        .actor_of_args::<Subscriber, _>("news-subscriber", news_tx)
        .unwrap();
    let (all_tx, all_rx) = mpsc::channel();
    let all_subscriber = system
        .actor_of_args::<Subscriber, _>("all-subscriber", all_tx)
        .unwrap();

    channel.tell(
        Subscribe {
            actor: Box::new(news_subscriber),
            topic: "news".into(),
        },
        None,
    );
    channel.tell(
        Subscribe {
            actor: Box::new(all_subscriber),
            topic: All.into(),
        },
        None,
    );
    channel.tell(
        Publish {
            msg: "headline".to_string(),
            topic: "news".into(),
        },
        None,
    );
    channel.tell(
        Publish {
            msg: "forecast".to_string(),
            topic: "weather".into(),
        },
        None,
    );

    let timeout = Duration::from_secs(2);
    assert_eq!(news_rx.recv_timeout(timeout).unwrap(), "headline");
    assert_eq!(all_rx.recv_timeout(timeout).unwrap(), "headline");
    assert_eq!(all_rx.recv_timeout(timeout).unwrap(), "forecast");
    assert!(news_rx.try_recv().is_err());
}
