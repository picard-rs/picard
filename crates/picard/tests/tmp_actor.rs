use picard::actors::*;
use picard_testkit::probe::{
    Probe, ProbeReceive,
    channel::{ChannelProbe, probe},
};

#[derive(Default)]
struct DefaultActor;

impl Actor for DefaultActor {
    type Msg = ();

    fn recv(&mut self, _: &Context<Self::Msg>, (): Self::Msg, _: Sender) {}
}

struct ArgsActor;

impl ActorFactoryArgs<String> for ArgsActor {
    fn create_args(_: String) -> Self {
        Self
    }
}

impl Actor for ArgsActor {
    type Msg = ();

    fn recv(&mut self, _: &Context<Self::Msg>, (): Self::Msg, _: Sender) {}
}

struct CheckTemporaryActors(ChannelProbe<(), Vec<String>>);

impl ActorFactoryArgs<ChannelProbe<(), Vec<String>>> for CheckTemporaryActors {
    fn create_args(probe: ChannelProbe<(), Vec<String>>) -> Self {
        Self(probe)
    }
}

impl Actor for CheckTemporaryActors {
    type Msg = String;

    fn recv(&mut self, ctx: &Context<Self::Msg>, _: Self::Msg, _: Sender) {
        let from_props = ctx
            .tmp_actor_of_props::<ArgsActor>(Props::new_args::<ArgsActor, _>("props".to_string()))
            .unwrap();
        let from_factory = ctx.tmp_actor_of::<DefaultActor>().unwrap();
        let from_args = ctx
            .tmp_actor_of_args::<ArgsActor, _>("args".to_string())
            .unwrap();

        self.0.event(vec![
            from_props.path().to_string(),
            from_factory.path().to_string(),
            from_args.path().to_string(),
        ]);
    }
}

#[test]
fn context_creates_temporary_actors_under_temp_guardian() {
    let system = ActorSystem::new().unwrap();
    let (probe, listen) = probe();
    let actor = system
        .actor_of_args::<CheckTemporaryActors, _>("checker", probe)
        .unwrap();

    actor.tell("create".to_string(), None);

    let paths = listen.recv();
    assert_eq!(paths.len(), 3);
    assert!(paths.iter().all(|path| path.starts_with("/temp/")));
    assert_eq!(
        paths.iter().collect::<std::collections::HashSet<_>>().len(),
        3
    );
}
