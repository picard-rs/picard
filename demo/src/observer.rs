//! Runtime observer: turns Picard's own lifecycle events and dead letters into counters.
//!
//! These numbers come from the framework (system event stream and dead-letter channel), not
//! from application-level bookkeeping.

use std::sync::Arc;

use picard::actors::{
    Actor, ActorFactoryArgs, Context, DeadLetter, Sender, Subscribe, SysTopic, SystemEvent,
    SystemMsg, Tell,
};

use crate::metrics::Metrics;

#[derive(Clone, Debug)]
pub enum ObserverMsg {
    SystemEvent(SystemEvent),
    DeadLetter(DeadLetter),
}

impl From<SystemEvent> for ObserverMsg {
    fn from(event: SystemEvent) -> Self {
        Self::SystemEvent(event)
    }
}

impl From<DeadLetter> for ObserverMsg {
    fn from(letter: DeadLetter) -> Self {
        Self::DeadLetter(letter)
    }
}

pub struct Observer {
    metrics: Arc<Metrics>,
}

impl ActorFactoryArgs<Arc<Metrics>> for Observer {
    fn create_args(metrics: Arc<Metrics>) -> Self {
        Self { metrics }
    }
}

impl Actor for Observer {
    type Msg = ObserverMsg;

    fn pre_start(&mut self, ctx: &Context<Self::Msg>) {
        for topic in [SysTopic::ActorTerminated, SysTopic::ActorRestarted] {
            ctx.system.sys_events().tell(
                Subscribe {
                    topic: topic.into(),
                    actor: Box::new(ctx.myself()),
                },
                None,
            );
        }
        ctx.system.dead_letters().tell(
            Subscribe {
                topic: "dead_letter".into(),
                actor: Box::new(ctx.myself()),
            },
            None,
        );
    }

    fn sys_recv(&mut self, _: &Context<Self::Msg>, msg: SystemMsg, _: Sender) {
        if let SystemMsg::Event(event) = msg {
            let m = &self.metrics;
            match event {
                SystemEvent::ActorCreated(_) => {}
                SystemEvent::ActorTerminated(_) => m.inc(&m.terminated),
                SystemEvent::ActorRestarted(_) => m.inc(&m.restarted),
            }
        }
    }

    fn recv(&mut self, _: &Context<Self::Msg>, msg: Self::Msg, _: Sender) {
        if matches!(msg, ObserverMsg::DeadLetter(_)) {
            self.metrics.inc(&self.metrics.dead_letters);
        }
    }
}
