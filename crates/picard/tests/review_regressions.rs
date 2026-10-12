use chrono::{TimeZone, Utc};
use picard::actors::*;
use serde::{Deserialize, Serialize};
use std::{
    sync::mpsc::{self, Sender as ChannelSender},
    time::{Duration, Instant},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ProbeMessage;

#[derive(Default)]
struct StringActor;

impl Actor for StringActor {
    type Msg = String;

    fn recv(&mut self, _: &Context<Self::Msg>, _: Self::Msg, _: Sender) {}
}

#[test]
fn named_selection_drops_unsupported_message_without_panicking() {
    let system = ActorSystem::new().unwrap();
    system.actor_of::<StringActor>("string-actor").unwrap();
    let selection = system.select("string-actor").unwrap();

    assert_eq!(
        selection.try_tell(ProbeMessage, None),
        Err(SelectionSendError::DeliveryFailed {
            delivered: 0,
            failed: 1
        })
    );
}

#[test]
fn named_selection_reports_missing_recipients() {
    let system = ActorSystem::new().unwrap();
    let selection = system.select("missing-actor").unwrap();

    assert_eq!(
        selection.try_tell(ProbeMessage, None),
        Err(SelectionSendError::NoRecipients)
    );
}

#[derive(Default)]
struct OtherMessageActor;

impl Actor for OtherMessageActor {
    type Msg = u8;

    fn recv(&mut self, _: &Context<Self::Msg>, _: Self::Msg, _: Sender) {}
}

struct RecordingStringActor(ChannelSender<String>);

impl ActorFactoryArgs<ChannelSender<String>> for RecordingStringActor {
    fn create_args(sender: ChannelSender<String>) -> Self {
        Self(sender)
    }
}

impl Actor for RecordingStringActor {
    type Msg = String;

    fn recv(&mut self, _: &Context<Self::Msg>, msg: Self::Msg, _: Sender) {
        let _ = self.0.send(msg);
    }
}

#[test]
fn selection_reports_partial_delivery_failures() {
    let system = ActorSystem::new().unwrap();
    let (tx, rx) = mpsc::channel();
    system
        .actor_of_args::<RecordingStringActor, _>("selection-recorder", tx)
        .unwrap();
    system
        .actor_of::<OtherMessageActor>("selection-other")
        .unwrap();
    let selection = system.select("*").unwrap();

    assert_eq!(
        selection.try_tell("delivered".to_owned(), None),
        Err(SelectionSendError::DeliveryFailed {
            delivered: 1,
            failed: 1
        })
    );
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "delivered"
    );
}

struct SelectionParent(ChannelSender<()>);

impl ActorFactoryArgs<ChannelSender<()>> for SelectionParent {
    fn create_args(ready: ChannelSender<()>) -> Self {
        Self(ready)
    }
}

impl Actor for SelectionParent {
    type Msg = String;

    fn pre_start(&mut self, ctx: &Context<Self::Msg>) {
        ctx.actor_of::<StringActor>("child-a").unwrap();
        ctx.actor_of::<StringActor>("child-b").unwrap();
        let _ = self.0.send(());
    }

    fn recv(&mut self, _: &Context<Self::Msg>, _: Self::Msg, _: Sender) {
        let _ = self.0.send(());
    }
}

#[test]
fn selection_deduplicates_recipients_reached_through_parent_navigation() {
    let system = ActorSystem::new().unwrap();
    let (tx, rx) = mpsc::channel();
    system
        .actor_of_args::<SelectionParent, _>("selection-parent", tx)
        .unwrap();
    rx.recv_timeout(Duration::from_secs(1))
        .expect("parent should finish creating children");

    let selection = system.select("selection-parent/*/..").unwrap();
    assert_eq!(selection.try_tell("once".to_owned(), None), Ok(1));
    rx.recv_timeout(Duration::from_secs(1))
        .expect("selection should deliver to its unique parent");
    assert!(
        rx.recv_timeout(Duration::from_millis(200)).is_err(),
        "duplicate delivery to parent"
    );
}

#[derive(Default)]
struct RetryActor;

impl Actor for RetryActor {
    type Msg = ();

    fn recv(&mut self, _: &Context<Self::Msg>, (): Self::Msg, _: Sender) {}
}

#[test]
fn failed_actor_creation_releases_reserved_path() {
    let system = ActorSystem::new().unwrap();
    let props = Props::new_from(|| -> RetryActor {
        panic!("expected actor factory failure");
    });

    assert!(matches!(
        system.actor_of_props("retry-after-failure", props),
        Err(CreateError::Panicked)
    ));
    assert!(system.actor_of::<RetryActor>("retry-after-failure").is_ok());
}

#[derive(Debug, Serialize, Deserialize)]
struct MoveOnlyMessage(String);

struct MoveOnlyActor(ChannelSender<String>);

impl ActorFactoryArgs<ChannelSender<String>> for MoveOnlyActor {
    fn create_args(sender: ChannelSender<String>) -> Self {
        Self(sender)
    }
}

impl Actor for MoveOnlyActor {
    type Msg = MoveOnlyMessage;

    fn recv(&mut self, _: &Context<Self::Msg>, msg: Self::Msg, _: Sender) {
        let _ = self.0.send(msg.0);
    }
}

#[test]
fn actor_messages_do_not_need_to_be_cloneable() {
    let system = ActorSystem::new().unwrap();
    let (tx, rx) = mpsc::channel();
    let actor = system
        .actor_of_args::<MoveOnlyActor, _>("move-only", tx)
        .unwrap();

    actor.tell(MoveOnlyMessage("received without cloning".into()), None);

    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "received without cloning"
    );
}

#[test]
fn repeatable_any_message_survives_mismatched_and_repeated_downcasts() {
    let mut message = picard::AnyMessage::new_repeatable(String::from("repeat"));

    assert!(message.take::<u8>().is_err());
    let mut first = message
        .take::<String>()
        .ok()
        .expect("matching message type");
    first.push_str("-changed");
    assert_eq!(
        message.take::<String>().ok().expect("repeatable message"),
        "repeat"
    );
}

#[test]
fn repeating_timer_delivers_message_on_multiple_intervals() {
    let system = ActorSystem::new().unwrap();
    let (tx, rx) = mpsc::channel();
    let actor = system
        .actor_of_args::<RecordingStringActor, _>("repeating-timer-recorder", tx)
        .unwrap();
    let id = system.schedule(
        Duration::from_millis(10),
        Duration::from_millis(60),
        actor,
        None,
        String::from("timer tick"),
    );

    for _ in 0..3 {
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            "timer tick"
        );
    }
    system.cancel_schedule(id);
}

#[test]
fn invalid_custom_config_returns_an_error() {
    let mut config = PicardConfig::default();
    config.dispatcher.pool_size = 0;

    assert!(matches!(
        ActorSystem::with_config("invalid-config", config),
        Err(picard::system::SystemError::Configuration(_))
    ));
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum ContextTimerMessage {
    Schedule,
    Tick,
}

struct ContextTimerActor {
    event_tx: ChannelSender<()>,
    scheduled_tx: ChannelSender<()>,
}

impl ActorFactoryArgs<(ChannelSender<()>, ChannelSender<()>)> for ContextTimerActor {
    fn create_args((event_tx, scheduled_tx): (ChannelSender<()>, ChannelSender<()>)) -> Self {
        Self {
            event_tx,
            scheduled_tx,
        }
    }
}

impl Actor for ContextTimerActor {
    type Msg = ContextTimerMessage;

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, _: Sender) {
        match msg {
            ContextTimerMessage::Schedule => {
                let now = Utc::now();
                let deadline = Utc
                    .timestamp_opt(now.timestamp() + 1, 150_000_000)
                    .single()
                    .unwrap();
                ctx.schedule_at_time(deadline, ctx.myself(), None, ContextTimerMessage::Tick);
                let _ = self.scheduled_tx.send(());
            }
            ContextTimerMessage::Tick => {
                let _ = self.event_tx.send(());
            }
        }
    }
}

#[test]
fn context_schedule_at_time_preserves_subsecond_precision() {
    let system = ActorSystem::new().unwrap();
    let (event_tx, event_rx) = mpsc::channel();
    let (scheduled_tx, scheduled_rx) = mpsc::channel();
    let actor = system
        .actor_of_args::<ContextTimerActor, _>("context-timer", (event_tx, scheduled_tx))
        .unwrap();

    let wait_deadline = Instant::now() + Duration::from_secs(3);
    while Utc::now().timestamp_subsec_millis() < 800 && Instant::now() < wait_deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(Utc::now().timestamp_subsec_millis() >= 800);

    actor.tell(ContextTimerMessage::Schedule, None);
    scheduled_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    assert!(
        event_rx.recv_timeout(Duration::from_millis(800)).is_ok(),
        "context scheduled message missed its sub-second deadline"
    );
}

type Gate = (
    ChannelSender<()>,
    std::sync::Arc<std::sync::Mutex<mpsc::Receiver<()>>>,
);

struct BlockingStop(Gate);

impl ActorFactoryArgs<Gate> for BlockingStop {
    fn create_args(gate: Gate) -> Self {
        Self(gate)
    }
}

impl Actor for BlockingStop {
    type Msg = String;

    fn post_stop(&mut self) {
        self.0.0.send(()).unwrap();
        self.0.1.lock().unwrap().recv().unwrap();
    }

    fn recv(&mut self, _: &Context<Self::Msg>, _: Self::Msg, _: Sender) {}
}

#[test]
fn shutdown_stops_children_created_while_parent_terminates() {
    let system = ActorSystem::new().unwrap();
    let (stopping_tx, stopping) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    let gate = (
        stopping_tx,
        std::sync::Arc::new(std::sync::Mutex::new(release_rx)),
    );
    system
        .actor_of_args::<BlockingStop, _>("blocking", gate)
        .unwrap();
    let shutdown = system.shutdown();
    // `/user` is now terminating; it must also stop a child added after it stopped the others.
    stopping.recv_timeout(Duration::from_secs(5)).unwrap();
    system.actor_of::<StringActor>("late").unwrap();
    release.send(()).unwrap();

    let (done_tx, done) = mpsc::channel();
    std::thread::spawn(move || done_tx.send(futures::executor::block_on(shutdown)));
    assert!(
        done.recv_timeout(Duration::from_secs(5)).unwrap().is_ok(),
        "Shutdown did not complete"
    );
}
