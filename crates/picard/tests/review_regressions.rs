use chrono::{TimeZone, Utc};
use picard::actors::*;
use std::{
    sync::mpsc::{self, Sender as ChannelSender},
    time::{Duration, Instant},
};

#[derive(Clone, Debug)]
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

    selection.try_tell(ProbeMessage, None);
}

#[derive(Default)]
struct EmptyActor;

impl Actor for EmptyActor {
    type Msg = ();

    fn recv(&mut self, _: &Context<Self::Msg>, _: Self::Msg, _: Sender) {}
}

#[test]
fn named_selection_sys_tell_sends_system_message() {
    let system = ActorSystem::new().unwrap();
    system
        .actor_of::<EmptyActor>("stopped-by-selection")
        .unwrap();
    let selection = system.select("stopped-by-selection").unwrap();

    selection.sys_tell(SystemMsg::Command(picard::system::SystemCmd::Stop), None);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while system
        .user_root()
        .children()
        .any(|child| child.name() == "stopped-by-selection")
        && std::time::Instant::now() < deadline
    {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    assert!(
        !system
            .user_root()
            .children()
            .any(|child| child.name() == "stopped-by-selection")
    );
}

#[derive(Default)]
struct RetryActor;

impl Actor for RetryActor {
    type Msg = ();

    fn recv(&mut self, _: &Context<Self::Msg>, _: Self::Msg, _: Sender) {}
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

#[derive(Clone, Debug)]
enum ContextTimerMessage {
    Schedule(ChannelSender<()>, ChannelSender<()>),
    Tick(ChannelSender<()>),
}

#[derive(Default)]
struct ContextTimerActor;

impl Actor for ContextTimerActor {
    type Msg = ContextTimerMessage;

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, _: Sender) {
        match msg {
            ContextTimerMessage::Schedule(event_tx, scheduled_tx) => {
                let now = Utc::now();
                let deadline = Utc
                    .timestamp_opt(now.timestamp() + 1, 150_000_000)
                    .single()
                    .unwrap();
                ctx.schedule_at_time(
                    deadline,
                    ctx.myself(),
                    None,
                    ContextTimerMessage::Tick(event_tx),
                );
                let _ = scheduled_tx.send(());
            }
            ContextTimerMessage::Tick(event_tx) => {
                let _ = event_tx.send(());
            }
        }
    }
}

#[test]
fn context_schedule_at_time_preserves_subsecond_precision() {
    let system = ActorSystem::new().unwrap();
    let actor = system
        .actor_of::<ContextTimerActor>("context-timer")
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel();
    let (scheduled_tx, scheduled_rx) = mpsc::channel();

    let wait_deadline = Instant::now() + Duration::from_secs(3);
    while Utc::now().timestamp_subsec_millis() < 800 && Instant::now() < wait_deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(Utc::now().timestamp_subsec_millis() >= 800);

    actor.tell(ContextTimerMessage::Schedule(event_tx, scheduled_tx), None);
    scheduled_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    assert!(
        event_rx.recv_timeout(Duration::from_millis(800)).is_ok(),
        "context scheduled message missed its sub-second deadline"
    );
}
