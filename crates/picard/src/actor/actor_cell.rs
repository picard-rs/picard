use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc, Mutex, MutexGuard, OnceLock, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use chrono::prelude::*;
use futures::{Future, future::RemoteHandle, task::SpawnError};
use uuid::Uuid;

#[cfg(feature = "remoting")]
use crate::kernel::mailbox::ProcessedAck;
use crate::{
    AnyMessage, Envelope, LocalMessage, Message,
    actor::actor_ref::SystemTell,
    actor::{
        Actor, ActorArgs, ActorFactoryArgs, ActorRef, ActorRefFactory, ActorReference,
        ActorSelection, ActorSelectionFactory, ActorUri, BasicActorRef, BoxActorProd, CreateError,
        DeadLetter, MsgResult, Props, Publish, Sender, Strategy, Tell, TmpActorRefFactory,
        props::ActorFactory,
    },
    kernel::{
        kernel_ref::{KernelRef, dispatch, dispatch_any},
        mailbox::{AnyEnqueueError, AnySender, MailboxSender},
    },
    platform::Instant,
    system::{
        ActorSystem, Run, SystemCmd, SystemMsg,
        timer::{Job, OnceJob, RepeatJob, ScheduleId, Timer},
    },
    validate::InvalidPath,
};

#[derive(Clone)]
pub struct ActorCell {
    inner: Arc<ActorCellInner>,
}

struct ActorCellInner {
    uri: ActorUri,
    parent: Option<BasicActorRef>,
    children: Children,
    is_terminating: AtomicBool,
    is_restarting: AtomicBool,
    kernel: OnceLock<KernelRef>,
    system: ActorSystem,
    mailbox: Arc<dyn AnySender>,
    sys_mailbox: MailboxSender<SystemMsg>,
    #[cfg(feature = "remoting")]
    remote_reply: Option<Arc<std::sync::mpsc::SyncSender<serde_json::Value>>>,
}

impl ActorCell {
    /// Constructs a new `ActorCell`
    pub(crate) fn new(
        uri: ActorUri,
        parent: Option<BasicActorRef>,
        system: &ActorSystem,
        mailbox: Arc<dyn AnySender>,
        sys_mailbox: MailboxSender<SystemMsg>,
    ) -> Self {
        Self {
            inner: Arc::new(ActorCellInner {
                uri,
                parent,
                children: Children::new(),
                is_terminating: AtomicBool::new(false),
                is_restarting: AtomicBool::new(false),
                kernel: OnceLock::new(),
                system: system.clone(),
                mailbox,
                sys_mailbox,
                #[cfg(feature = "remoting")]
                remote_reply: None,
            }),
        }
    }

    #[cfg(feature = "remoting")]
    pub(crate) fn with_remote_reply(
        &self,
        uri: ActorUri,
        sender: std::sync::mpsc::SyncSender<serde_json::Value>,
    ) -> Self {
        Self {
            inner: Arc::new(ActorCellInner {
                uri,
                parent: Some(self.myself()),
                children: Children::new(),
                is_terminating: AtomicBool::new(false),
                is_restarting: AtomicBool::new(false),
                kernel: OnceLock::new(),
                system: self.inner.system.clone(),
                mailbox: self.inner.mailbox.clone(),
                sys_mailbox: self.inner.sys_mailbox.clone(),
                remote_reply: Some(Arc::new(sender)),
            }),
        }
    }

    #[cfg(feature = "remoting")]
    pub(crate) fn send_remote_reply<Msg: Message>(
        &self,
        msg: &Msg,
    ) -> Option<Result<(), AnyEnqueueError>> {
        self.inner.remote_reply.as_ref().map(|sender| {
            serde_json::to_value(msg)
                .map_err(|_| AnyEnqueueError)
                .and_then(|payload| sender.send(payload).map_err(|_| AnyEnqueueError))
        })
    }

    #[cfg(feature = "remoting")]
    pub(crate) fn is_remote_reply(&self) -> bool {
        self.inner.remote_reply.is_some()
    }

    pub(crate) fn init(self, kernel: &KernelRef) -> Self {
        // Every cell handed a kernel is given the same one, so a repeated init is a no-op.
        let _ = self.inner.kernel.set(kernel.clone());
        self
    }

    pub(crate) fn kernel(&self) -> &KernelRef {
        self.inner.kernel.get().unwrap()
    }

    pub(crate) fn myself(&self) -> BasicActorRef {
        BasicActorRef::new(self.clone())
    }

    pub(crate) fn uri(&self) -> &ActorUri {
        &self.inner.uri
    }

    pub(crate) fn parent(&self) -> BasicActorRef {
        self.inner.parent.as_ref().unwrap().clone()
    }

    pub fn has_children(&self) -> bool {
        self.inner.children.len() > 0
    }

    pub(crate) fn children<'a>(&'a self) -> Box<dyn Iterator<Item = BasicActorRef> + 'a> {
        Box::new(self.inner.children.iter())
    }

    pub(crate) fn user_root(&self) -> BasicActorRef {
        self.inner.system.user_root().clone()
    }

    pub(crate) fn is_root(&self) -> bool {
        self.myself().path() == "/"
    }

    pub fn is_user(&self) -> bool {
        self.inner.system.user_root().is_child(&self.myself())
    }

    pub(crate) fn send_any_msg(
        &self,
        msg: &mut AnyMessage,
        sender: crate::actor::Sender,
    ) -> Result<(), AnyEnqueueError> {
        #[cfg(feature = "remoting")]
        if self.is_remote_reply() {
            return Err(AnyEnqueueError);
        }
        let mb = &self.inner.mailbox;
        let k = self.kernel();

        let res = dispatch_any(msg, sender, mb, k);
        #[cfg(feature = "stats")]
        self.inner.system.stats_recorder().message_sent(res.is_ok());
        #[cfg(feature = "tracing")]
        if res.is_err() {
            tracing::debug!(target: "picard::messaging", actor = %self.uri().path, "message could not be delivered");
        }
        res
    }

    pub(crate) fn send_sys_msg(&self, msg: Envelope<SystemMsg>) -> MsgResult<Envelope<SystemMsg>> {
        #[cfg(feature = "remoting")]
        if self.is_remote_reply() {
            return Err(crate::actor::MsgError::new(msg));
        }
        let mb = &self.inner.sys_mailbox;

        let k = self.kernel();
        let res = dispatch(msg, mb, k);
        #[cfg(feature = "stats")]
        self.inner
            .system
            .stats_recorder()
            .system_message_sent(res.is_ok());
        res
    }

    pub(crate) fn is_child(&self, actor: &BasicActorRef) -> bool {
        self.inner.children.contains(actor)
    }

    pub(crate) fn stop(actor: &BasicActorRef) {
        actor.send_system_message(SystemCmd::Stop.into());
    }

    /// Adds a child and returns whether this actor is already terminating, in which case
    /// the caller must stop the child once it is initialized.
    #[must_use]
    pub fn add_child(&self, actor: BasicActorRef) -> bool {
        self.inner.children.add(actor)
    }

    pub fn remove_child(&self, actor: &BasicActorRef) {
        self.inner.children.remove(actor);
    }

    pub fn receive_cmd<A: Actor>(&self, cmd: SystemCmd, actor: &mut Option<A>) {
        match cmd {
            SystemCmd::Stop => self.terminate(actor),
            SystemCmd::Restart => self.restart(),
        }
    }

    pub fn terminate<A: Actor>(&self, actor: &mut Option<A>) {
        // *1. Suspend non-system mailbox messages
        // *2. Iterate all children and send Stop to each
        // *3. Wait for ActorTerminated from each child

        self.inner.is_terminating.store(true, Ordering::Relaxed);

        // Children added after this snapshot are stopped by their creator (see `add_child`).
        let children = self.inner.children.terminate();
        if children.is_empty() {
            self.kernel().terminate();
            post_stop(actor);
        } else {
            for child in children {
                Self::stop(&child);
            }
        }
    }

    pub fn restart(&self) {
        if self.has_children() {
            self.inner.is_restarting.store(true, Ordering::Relaxed);
            for child in self.inner.children.iter() {
                Self::stop(&child);
            }
        } else {
            self.kernel().restart();
        }
    }

    pub fn death_watch<A: Actor>(&self, terminated: &BasicActorRef, actor: &mut Option<A>) {
        if self.is_child(terminated) {
            self.remove_child(terminated);

            if !self.has_children() {
                // No children exist. Stop this actor's kernel.
                if self.inner.is_terminating.load(Ordering::Relaxed) {
                    self.kernel().terminate();
                    post_stop(actor);
                }

                // No children exist. Restart the actor.
                if self.inner.is_restarting.load(Ordering::Relaxed) {
                    self.inner.is_restarting.store(false, Ordering::Relaxed);
                    self.kernel().restart();
                }
            }
        }
    }

    pub fn handle_failure(&self, failed: &BasicActorRef, strategy: Strategy) {
        #[cfg(feature = "tracing")]
        tracing::warn!(
            target: "picard::actor::supervision",
            supervisor = %self.uri().path,
            actor = %failed.path(),
            strategy = match strategy {
                Strategy::Stop => "stop",
                Strategy::Restart => "restart",
                Strategy::Escalate => "escalate",
            },
            "supervising failed child"
        );
        match strategy {
            Strategy::Stop => Self::stop(failed),
            Strategy::Restart => Self::restart_child(failed),
            Strategy::Escalate => self.escalate_failure(),
        }
    }

    pub fn restart_child(actor: &BasicActorRef) {
        actor.send_system_message(SystemCmd::Restart.into());
    }

    pub fn escalate_failure(&self) {
        self.inner
            .parent
            .as_ref()
            .unwrap()
            .send_system_message(SystemMsg::Failed(self.myself()));
    }
}

impl<Msg: LocalMessage> From<ExtendedCell<Msg>> for ActorCell {
    fn from(cell: ExtendedCell<Msg>) -> Self {
        cell.cell
    }
}

impl fmt::Debug for ActorCell {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ActorCell[{:?}]", self.uri())
    }
}

impl TmpActorRefFactory for ActorCell {
    fn tmp_actor_of_props<A: Actor>(
        &self,
        props: BoxActorProd<A>,
    ) -> Result<ActorRef<A::Msg>, CreateError>
    where
        <A as Actor>::Msg: Message,
    {
        self.inner.system.tmp_actor_of_props(props)
    }

    fn tmp_actor_of<A: ActorFactory>(&self) -> Result<ActorRef<<A as Actor>::Msg>, CreateError>
    where
        <A as Actor>::Msg: Message,
    {
        self.inner.system.tmp_actor_of::<A>()
    }

    fn tmp_actor_of_args<A, Args>(
        &self,
        args: Args,
    ) -> Result<ActorRef<<A as Actor>::Msg>, CreateError>
    where
        Args: ActorArgs,
        A: ActorFactoryArgs<Args>,
        <A as Actor>::Msg: Message,
    {
        self.inner.system.tmp_actor_of_args::<A, _>(args)
    }
}

pub struct ExtendedCell<Msg: LocalMessage> {
    cell: ActorCell,
    mailbox: MailboxSender<Msg>,
}

impl<Msg: LocalMessage> Clone for ExtendedCell<Msg> {
    fn clone(&self) -> Self {
        Self {
            cell: self.cell.clone(),
            mailbox: self.mailbox.clone(),
        }
    }
}

impl<Msg> ExtendedCell<Msg>
where
    Msg: LocalMessage,
{
    pub(crate) fn new(
        uri: ActorUri,
        parent: Option<BasicActorRef>,
        system: &ActorSystem,
        any_mailbox: Arc<dyn AnySender>,
        sys_mailbox: MailboxSender<SystemMsg>,
        mailbox: MailboxSender<Msg>,
    ) -> Self {
        let cell = ActorCell {
            inner: Arc::new(ActorCellInner {
                uri,
                parent,
                children: Children::new(),
                is_terminating: AtomicBool::new(false),
                is_restarting: AtomicBool::new(false),
                kernel: OnceLock::new(),
                system: system.clone(),
                mailbox: any_mailbox,
                sys_mailbox,
                #[cfg(feature = "remoting")]
                remote_reply: None,
            }),
        };

        Self { cell, mailbox }
    }

    pub(crate) fn init(self, kernel: &KernelRef) -> Self {
        let cell = self.cell.init(kernel);

        Self { cell, ..self }
    }

    pub fn myself(&self) -> ActorRef<Msg> {
        self.cell.myself().typed(self.clone())
    }

    pub fn uri(&self) -> &ActorUri {
        self.cell.uri()
    }

    pub fn parent(&self) -> BasicActorRef {
        self.cell.parent()
    }

    pub fn has_children(&self) -> bool {
        self.cell.has_children()
    }

    pub(crate) fn is_child(&self, actor: &BasicActorRef) -> bool {
        self.cell.is_child(actor)
    }

    pub fn children<'a>(&'a self) -> Box<dyn Iterator<Item = BasicActorRef> + 'a> {
        self.cell.children()
    }

    pub fn user_root(&self) -> BasicActorRef {
        self.cell.user_root()
    }

    pub fn is_root(&self) -> bool {
        self.cell.is_root()
    }

    pub fn is_user(&self) -> bool {
        self.cell.is_user()
    }

    pub(crate) fn send_msg(&self, msg: Envelope<Msg>) -> MsgResult<Envelope<Msg>> {
        let mb = &self.mailbox;
        let k = self.cell.kernel();

        let res = dispatch(msg, mb, k);
        #[cfg(feature = "stats")]
        self.cell
            .inner
            .system
            .stats_recorder()
            .message_sent(res.is_ok());
        res.inspect_err(|e| {
            #[cfg(feature = "tracing")]
            tracing::debug!(
                target: "picard::messaging",
                actor = %self.cell.uri().path,
                msg_type = std::any::type_name::<Msg>(),
                "message could not be delivered; sending to dead letters"
            );
            let dl = DeadLetter {
                msg: format!("{:?}", e.msg.msg),
                sender: e.msg.sender.clone(),
                recipient: self.cell.myself(),
            };

            self.cell.inner.system.dead_letters().tell(
                Publish {
                    topic: "dead_letter".into(),
                    msg: dl,
                },
                None,
            );
        })
    }

    #[cfg(feature = "remoting")]
    pub(crate) fn send_msg_with_ack(
        &self,
        msg: Envelope<Msg>,
        ack: ProcessedAck,
    ) -> MsgResult<Envelope<Msg>> {
        let res = crate::kernel::kernel_ref::dispatch_with_ack(
            msg,
            ack,
            &self.mailbox,
            self.cell.kernel(),
        );
        #[cfg(feature = "stats")]
        self.cell
            .inner
            .system
            .stats_recorder()
            .message_sent(res.is_ok());
        res
    }

    pub(crate) fn send_sys_msg(&self, msg: Envelope<SystemMsg>) -> MsgResult<Envelope<SystemMsg>> {
        self.cell.send_sys_msg(msg)
    }

    pub fn system(&self) -> &ActorSystem {
        &self.cell.inner.system
    }

    pub(crate) fn handle_failure(&self, failed: &BasicActorRef, strategy: Strategy) {
        self.cell.handle_failure(failed, strategy);
    }

    pub(crate) fn receive_cmd<A: Actor>(&self, cmd: SystemCmd, actor: &mut Option<A>) {
        self.cell.receive_cmd(cmd, actor);
    }

    pub(crate) fn death_watch<A: Actor>(&self, terminated: &BasicActorRef, actor: &mut Option<A>) {
        self.cell.death_watch(terminated, actor);
    }
}

impl<Msg: LocalMessage> fmt::Debug for ExtendedCell<Msg> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ExtendedCell[{:?}]", self.uri())
    }
}

fn post_stop<A: Actor>(actor: &mut Option<A>) {
    // If the actor instance exists we can execute post_stop.
    // The instance will be None if this is an actor that has failed
    // and is being terminated by an escalated supervisor.
    if let Some(act) = actor.as_mut() {
        act.post_stop();
    }
}

/// Provides context, including the actor system during actor execution.
///
/// `Context` is passed to an actor's functions, such as
/// `receive`.
///
/// Operations performed are in most cases done so from the
/// actor's perspective. For example, creating a child actor
/// using `ctx.actor_of` will create the child under the current
/// actor within the hierarchy.
///
/// Since `Context` is specific to an actor and its functions
/// it is not cloneable.
pub struct Context<Msg: LocalMessage> {
    pub myself: ActorRef<Msg>,
    pub system: ActorSystem,
    pub(crate) kernel: KernelRef,
}

impl<Msg> Context<Msg>
where
    Msg: LocalMessage,
{
    /// Returns the `ActorRef` of the current actor.
    #[must_use]
    pub fn myself(&self) -> ActorRef<Msg> {
        self.myself.clone()
    }

    /// Reports this actor as failed to its parent, which applies its supervision strategy.
    ///
    /// This is what a panic does on targets with unwinding. Targets that abort on panic, such
    /// as `wasm32-unknown-unknown`, can use it to get the same supervision without crashing.
    pub fn fail(&self) {
        let me: BasicActorRef = self.myself.clone().into();
        me.parent()
            .send_system_message(SystemMsg::Failed(me.clone()));
    }
}

impl<Msg: LocalMessage> ActorRefFactory for Context<Msg> {
    fn actor_of_props<A>(
        &self,
        name: &str,
        props: BoxActorProd<A>,
    ) -> Result<ActorRef<A::Msg>, CreateError>
    where
        A: Actor,
        <A as Actor>::Msg: Message,
    {
        self.system
            .provider
            .create_actor(props, name, &self.myself().into(), &self.system)
    }

    fn actor_of<A>(&self, name: &str) -> Result<ActorRef<<A as Actor>::Msg>, CreateError>
    where
        A: ActorFactory,
        <A as Actor>::Msg: Message,
    {
        self.system.provider.create_actor(
            Props::produce::<A>(),
            name,
            &self.myself().into(),
            &self.system,
        )
    }

    fn actor_of_args<A, Args>(
        &self,
        name: &str,
        args: Args,
    ) -> Result<ActorRef<<A as Actor>::Msg>, CreateError>
    where
        Args: ActorArgs,
        A: ActorFactoryArgs<Args>,
        <A as Actor>::Msg: Message,
    {
        self.system.provider.create_actor(
            Props::new_args::<A, _>(args),
            name,
            &self.myself().into(),
            &self.system,
        )
    }

    fn local_actor_of_props<A>(
        &self,
        name: &str,
        props: BoxActorProd<A>,
    ) -> Result<ActorRef<A::Msg>, CreateError>
    where
        A: Actor,
    {
        self.system
            .provider
            .create_local_actor(props, name, &self.myself().into(), &self.system)
    }

    fn stop(&self, actor: impl ActorReference) {
        actor.stop();
    }
}

impl<Msg: LocalMessage> TmpActorRefFactory for Context<Msg> {
    fn tmp_actor_of_props<A>(&self, props: BoxActorProd<A>) -> Result<ActorRef<A::Msg>, CreateError>
    where
        A: Actor,
        <A as Actor>::Msg: Message,
    {
        self.system.tmp_actor_of_props(props)
    }

    fn tmp_actor_of<A>(&self) -> Result<ActorRef<<A as Actor>::Msg>, CreateError>
    where
        A: ActorFactory + Actor,
        <A as Actor>::Msg: Message,
    {
        self.system.tmp_actor_of::<A>()
    }

    fn tmp_actor_of_args<A, Args>(
        &self,
        args: Args,
    ) -> Result<ActorRef<<A as Actor>::Msg>, CreateError>
    where
        Args: ActorArgs,
        A: ActorFactoryArgs<Args>,
        <A as Actor>::Msg: Message,
    {
        self.system.tmp_actor_of_args::<A, _>(args)
    }
}

impl<Msg> ActorSelectionFactory for Context<Msg>
where
    Msg: LocalMessage,
{
    fn select(&self, path: &str) -> Result<ActorSelection, InvalidPath> {
        let (anchor, path_str) = if path.starts_with('/') {
            let anchor = self.system.user_root().clone();
            let anchor_path = format!("{}/", anchor.path());
            let path = path.to_string().replace(&anchor_path, "");

            (anchor, path)
        } else {
            (self.myself.clone().into(), path.to_string())
        };

        ActorSelection::new(
            anchor, // self.system.dead_letters(),
            path_str,
        )
    }
}

impl<Msg> Run for Context<Msg>
where
    Msg: LocalMessage,
{
    fn run<Fut>(&self, future: Fut) -> Result<RemoteHandle<<Fut as Future>::Output>, SpawnError>
    where
        Fut: Future + Send + 'static,
        <Fut as Future>::Output: Send,
    {
        self.system.run(future)
    }
}

impl<Msg> Timer for Context<Msg>
where
    Msg: LocalMessage,
{
    fn schedule<T, M>(
        &self,
        initial_delay: Duration,
        interval: Duration,
        receiver: ActorRef<M>,
        sender: Sender,
        msg: T,
    ) -> ScheduleId
    where
        T: LocalMessage + Into<M>,
        M: LocalMessage + Clone,
    {
        let id = Uuid::new_v4();
        let msg: M = msg.into();

        let job = RepeatJob {
            id,
            send_at: Instant::now() + initial_delay,
            interval,
            receiver: receiver.into(),
            sender,
            msg: AnyMessage::new_repeatable(msg),
        };

        self.system.timer.send(Job::Repeat(job)).unwrap();
        id
    }

    fn schedule_once<T, M>(
        &self,
        delay: Duration,
        receiver: ActorRef<M>,
        sender: Sender,
        msg: T,
    ) -> ScheduleId
    where
        T: LocalMessage + Into<M>,
        M: LocalMessage,
    {
        let id = Uuid::new_v4();
        let msg: M = msg.into();

        let job = OnceJob {
            id,
            send_at: Instant::now() + delay,
            receiver: receiver.into(),
            sender,
            msg: AnyMessage::new(msg),
        };

        self.system.timer.send(Job::Once(job)).unwrap();
        id
    }

    fn schedule_at_time<T, M>(
        &self,
        time: DateTime<Utc>,
        receiver: ActorRef<M>,
        sender: Sender,
        msg: T,
    ) -> ScheduleId
    where
        T: LocalMessage + Into<M>,
        M: LocalMessage,
    {
        let delay = crate::system::delay_until(time, Utc::now());

        let id = Uuid::new_v4();
        let msg: M = msg.into();

        let job = OnceJob {
            id,
            send_at: Instant::now() + delay,
            receiver: receiver.into(),
            sender,
            msg: AnyMessage::new(msg),
        };

        self.system.timer.send(Job::Once(job)).unwrap();
        id
    }

    fn cancel_schedule(&self, id: Uuid) {
        let _ = self.system.timer.send(Job::Cancel(id));
    }
}

/// The children of an actor, keyed by name.
///
/// A plain `Mutex<HashMap>` is used because it allocates nothing until the first child is
/// added, unlike a sharded map, which allocates all of its shards for every actor.
#[derive(Default)]
pub struct Children {
    actors: Mutex<ChildMap>,
}

#[derive(Default)]
struct ChildMap {
    actors: HashMap<String, BasicActorRef>,
    terminating: bool,
}

impl Children {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, ChildMap> {
        self.actors.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Adds a child and returns whether the parent was already terminating.
    pub fn add(&self, actor: BasicActorRef) -> bool {
        let mut children = self.lock();
        children.actors.insert(actor.name().to_string(), actor);
        children.terminating
    }

    pub fn remove(&self, actor: &BasicActorRef) {
        self.lock().actors.remove(actor.name());
    }

    pub fn len(&self) -> usize {
        self.lock().actors.len()
    }

    pub fn contains(&self, actor: &BasicActorRef) -> bool {
        self.lock().actors.values().any(|child| child == actor)
    }

    /// Marks the parent as terminating and returns the children to stop. Taken under the
    /// same lock as `add`, so each child is stopped either here or by its creator.
    fn terminate(&self) -> Vec<BasicActorRef> {
        let mut children = self.lock();
        children.terminating = true;
        children.actors.values().cloned().collect()
    }

    /// Returns a snapshot, so the lock is not held while the caller messages the children.
    pub fn iter(&self) -> std::vec::IntoIter<BasicActorRef> {
        self.lock()
            .actors
            .values()
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
    }
}
