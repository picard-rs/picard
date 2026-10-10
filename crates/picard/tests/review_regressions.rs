use picard::actors::*;

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

    assert!(!system
        .user_root()
        .children()
        .any(|child| child.name() == "stopped-by-selection"));
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
