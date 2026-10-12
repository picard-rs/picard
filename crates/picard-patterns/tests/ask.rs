use futures::executor::block_on;
use futures::future::RemoteHandle;
use picard::actors::*;
use picard_patterns::ask::ask;
use serde::{Deserialize, Serialize};
use std::sync::mpsc::{Sender as StdSender, channel};

#[derive(Default)]
struct EchoActor;

impl Actor for EchoActor {
    type Msg = String;

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, sender: Sender) {
        sender
            .unwrap()
            .try_tell(msg, Some(ctx.myself().into()))
            .unwrap();
    }
}

#[test]
fn ask_actor() {
    let sys = ActorSystem::new().unwrap();

    let actor = sys.actor_of::<EchoActor>("me").unwrap();

    let msg = "hello".to_string();

    let res: RemoteHandle<String> = ask(&sys, &actor, msg.clone());
    let res = block_on(res);

    assert_eq!(res, msg);
}

struct ContextAsker {
    receiver: ActorRef<String>,
    reply_tx: StdSender<RemoteHandle<String>>,
}

impl ActorFactoryArgs<(ActorRef<String>, StdSender<RemoteHandle<String>>)> for ContextAsker {
    fn create_args(
        (receiver, reply_tx): (ActorRef<String>, StdSender<RemoteHandle<String>>),
    ) -> Self {
        Self { receiver, reply_tx }
    }
}

impl Actor for ContextAsker {
    type Msg = String;

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, _: Sender) {
        self.reply_tx.send(ask(ctx, &self.receiver, msg)).unwrap();
    }
}

#[test]
fn ask_from_actor_context() {
    let system = ActorSystem::new().unwrap();
    let (reply_tx, reply_rx) = channel();
    let receiver = system.actor_of::<EchoActor>("echo").unwrap();
    let asker = system
        .actor_of_args::<ContextAsker, _>("asker", (receiver, reply_tx))
        .unwrap();

    asker.tell("from context".to_string(), None);
    let reply = reply_rx.recv().unwrap();
    assert_eq!(block_on(reply), "from context");
}

#[test]
fn stress_test() {
    #[derive(Debug, Clone, Serialize, Deserialize)]
    enum Protocol {
        Foo,
        FooResult,
    }

    #[derive(Default)]
    struct FooActor;

    impl Actor for FooActor {
        type Msg = Protocol;

        fn recv(&mut self, context: &Context<Self::Msg>, _: Self::Msg, sender: Sender) {
            sender
                .unwrap()
                .try_tell(Protocol::FooResult, Some(context.myself().into()))
                .unwrap();
        }
    }

    let system: ActorSystem = ActorSystem::new().unwrap();
    let actor = system.actor_of::<FooActor>("foo").unwrap();

    for _i in 1..10_000 {
        let a: RemoteHandle<Protocol> = ask(&system, &actor, Protocol::Foo);
        block_on(a);
    }
}
