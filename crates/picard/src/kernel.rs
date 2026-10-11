pub(crate) mod kernel_ref;
pub(crate) mod mailbox;
pub(crate) mod provider;
pub(crate) mod queue;

use crate::system::ActorSystem;

#[derive(Debug)]
pub enum KernelMsg {
    TerminateActor,
    RestartActor,
    RunActor,
    Sys(ActorSystem),
}
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
};

use futures::{StreamExt, channel::mpsc::unbounded, task::SpawnExt};
use slog::warn;

use crate::{
    Message,
    actor::actor_cell::ExtendedCell,
    actor::actor_ref::SystemTell,
    actor::{
        Actor, ActorProducer, ActorRef, ActorReference, BasicActorRef, BoxActorProd, Context,
        CreateError,
    },
    kernel::{
        kernel_ref::KernelRef,
        mailbox::{Mailbox, flush_to_deadletters, run_mailbox},
    },
    system::{ActorRestarted, ActorTerminated, SystemMsg},
};

pub struct Dock<A: Actor> {
    pub actor: Arc<Mutex<Option<A>>>,
    pub cell: ExtendedCell<A::Msg>,
}

impl<A: Actor> Clone for Dock<A> {
    fn clone(&self) -> Self {
        Self {
            actor: self.actor.clone(),
            cell: self.cell.clone(),
        }
    }
}

/// Starts the kernel task that drives an actor's lifecycle and mailbox.
///
/// # Errors
///
/// Returns [`CreateError::Panicked`] if the actor factory panics.
///
/// # Panics
///
/// Panics if the kernel task cannot be spawned on the system's executor.
pub fn kernel<A>(
    props: BoxActorProd<A>,
    cell: ExtendedCell<A::Msg>,
    mailbox: Mailbox<A::Msg>,
    sys: &ActorSystem,
) -> Result<KernelRef, CreateError>
where
    A: Actor + 'static,
{
    let (tx, mut rx) = unbounded::<KernelMsg>();
    let kr = KernelRef { tx };

    let mut asys = sys.clone();
    let akr = kr.clone();
    let actor = start_actor(&props)?;
    let cell = cell.init(&kr);

    let mut dock = Dock {
        actor: Arc::new(Mutex::new(Some(actor))),
        cell: cell.clone(),
    };

    let actor_ref = ActorRef::new(cell);

    #[cfg(feature = "stats")]
    sys.stats_recorder().actor_created();
    #[cfg(feature = "tracing")]
    tracing::debug!(
        actor = %actor_ref.path(),
        actor_type = std::any::type_name::<A>(),
        "actor created"
    );

    let f = async move {
        while let Some(msg) = rx.next().await {
            match msg {
                KernelMsg::RunActor => {
                    let ctx = Context {
                        myself: actor_ref.clone(),
                        system: asys.clone(),
                        kernel: akr.clone(),
                    };

                    // A panic is reported to the parent by the mailbox sentinel.
                    let _ =
                        catch_unwind(AssertUnwindSafe(|| run_mailbox(&mailbox, &ctx, &mut dock)));
                }
                KernelMsg::RestartActor => {
                    restart_actor(&dock, actor_ref.clone().into(), &props, &asys);
                }
                KernelMsg::TerminateActor => {
                    terminate_actor(&mailbox, actor_ref.clone().into(), &asys);
                    break;
                }
                KernelMsg::Sys(s) => {
                    asys = s;
                }
            }
        }
    };

    sys.exec.spawn(f).unwrap();
    Ok(kr)
}

fn restart_actor<A>(
    dock: &Dock<A>,
    actor_ref: BasicActorRef,
    props: &BoxActorProd<A>,
    sys: &ActorSystem,
) where
    A: Actor,
{
    let mut a = dock.actor.lock().unwrap();
    if let Ok(actor) = start_actor(props) {
        *a = Some(actor);
        #[cfg(feature = "stats")]
        sys.stats_recorder().actor_restarted();
        #[cfg(feature = "tracing")]
        tracing::info!(actor = %actor_ref.path(), "actor restarted");
        actor_ref.send_system_message(SystemMsg::ActorInit);
        sys.publish_event(ActorRestarted { actor: actor_ref }.into());
    } else {
        #[cfg(feature = "tracing")]
        tracing::warn!(actor = %actor_ref.path(), "actor failed to restart");
        warn!(sys.log(), "Actor failed to restart: {:?}", actor_ref);
    }
}

fn terminate_actor<Msg>(mbox: &Mailbox<Msg>, actor_ref: BasicActorRef, sys: &ActorSystem)
where
    Msg: Message,
{
    sys.provider.unregister(actor_ref.path());
    #[cfg(feature = "stats")]
    sys.stats_recorder().actor_terminated();
    #[cfg(feature = "tracing")]
    tracing::debug!(actor = %actor_ref.path(), "actor terminated");
    flush_to_deadletters(mbox, &actor_ref, sys);
    sys.publish_event(
        ActorTerminated {
            actor: actor_ref.clone(),
        }
        .into(),
    );

    let parent = actor_ref.parent();
    if !parent.is_root() {
        parent.send_system_message(ActorTerminated { actor: actor_ref }.into());
    }
}

fn start_actor<A>(props: &BoxActorProd<A>) -> Result<A, CreateError>
where
    A: Actor,
{
    let actor = catch_unwind(|| props.produce()).map_err(|_| CreateError::Panicked)?;

    Ok(actor)
}
