use picard::actors::*;
use std::{
    sync::mpsc::{self, Sender as StdSender},
    time::Duration,
};

struct RestartingChild(StdSender<()>);

impl ActorFactoryArgs<StdSender<()>> for RestartingChild {
    fn create_args(started: StdSender<()>) -> Self {
        Self(started)
    }
}

impl Actor for RestartingChild {
    type Msg = String;

    fn pre_start(&mut self, _: &Context<Self::Msg>) {
        self.0.send(()).unwrap();
    }

    fn recv(&mut self, _: &Context<Self::Msg>, msg: Self::Msg, _: Sender) {
        assert_ne!(msg, "panic", "intentional supervision test failure");
    }
}

struct RestartSupervisor {
    child: Option<ActorRef<String>>,
    started: StdSender<()>,
}

impl ActorFactoryArgs<StdSender<()>> for RestartSupervisor {
    fn create_args(started: StdSender<()>) -> Self {
        Self {
            child: None,
            started,
        }
    }
}

impl Actor for RestartSupervisor {
    type Msg = String;

    fn pre_start(&mut self, ctx: &Context<Self::Msg>) {
        self.child = Some(
            ctx.actor_of_args::<RestartingChild, _>("child", self.started.clone())
                .unwrap(),
        );
    }

    fn supervisor_strategy(&self) -> Strategy {
        Strategy::Restart
    }

    fn recv(&mut self, _: &Context<Self::Msg>, msg: Self::Msg, _: Sender) {
        if msg == "panic" {
            self.child.as_ref().unwrap().tell(msg, None);
        }
    }
}

#[test]
fn restart_strategy_restarts_failed_child() {
    let system = ActorSystem::new().unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let supervisor = system
        .actor_of_args::<RestartSupervisor, _>("supervisor", started_tx)
        .unwrap();

    let timeout = Duration::from_secs(2);
    started_rx.recv_timeout(timeout).unwrap();
    supervisor.tell("panic".to_string(), None);
    started_rx.recv_timeout(timeout).unwrap();
}
