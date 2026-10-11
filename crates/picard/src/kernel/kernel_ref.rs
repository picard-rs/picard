use std::sync::Arc;

use futures::channel::mpsc::UnboundedSender;

use crate::{
    AnyMessage, Envelope, Message,
    actor::{MsgError, MsgResult},
    kernel::{
        KernelMsg,
        mailbox::{AnyEnqueueError, AnySender, MailboxSchedule, MailboxSender},
    },
    system::ActorSystem,
};

#[derive(Clone)]
pub struct KernelRef {
    pub tx: UnboundedSender<KernelMsg>,
}

impl KernelRef {
    pub(crate) fn schedule(&self) {
        self.send(KernelMsg::RunActor);
    }

    pub(crate) fn restart(&self) {
        self.send(KernelMsg::RestartActor);
    }

    pub(crate) fn terminate(&self) {
        self.send(KernelMsg::TerminateActor);
    }

    pub(crate) fn sys_init(&self, sys: &ActorSystem) {
        self.send(KernelMsg::Sys(sys.clone()));
    }

    fn send(&self, msg: KernelMsg) {
        // Fails only once the kernel task has terminated, so there is nothing left to notify.
        let _ = self.tx.unbounded_send(msg);
    }
}

pub fn dispatch<Msg>(
    msg: Envelope<Msg>,
    mbox: &MailboxSender<Msg>,
    kernel: &KernelRef,
) -> MsgResult<Envelope<Msg>>
where
    Msg: Message,
{
    match mbox.try_enqueue(msg) {
        Ok(()) => {
            if !mbox.is_scheduled() {
                mbox.set_scheduled(true);
                kernel.schedule();
            }

            Ok(())
        }
        Err(e) => Err(MsgError::new(e.msg)),
    }
}

pub fn dispatch_any(
    msg: &mut AnyMessage,
    sender: crate::actor::Sender,
    mbox: &Arc<dyn AnySender>,
    kernel: &KernelRef,
) -> Result<(), AnyEnqueueError> {
    mbox.try_any_enqueue(msg, sender).map(|()| {
        if !mbox.is_sched() {
            mbox.set_sched(true);
            kernel.schedule();
        }
    })
}
