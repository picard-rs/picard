use std::fmt;

use crate::{
    AnyMessage, Envelope, LocalMessage, Message,
    actor::{
        Actor, ActorPath, ActorUri, BoxActorProd, CreateError,
        actor_cell::{ActorCell, ExtendedCell},
        props::{ActorArgs, ActorFactory, ActorFactoryArgs},
    },
    kernel::mailbox::AnyEnqueueError,
    system::{ActorSystem, SystemCmd, SystemMsg},
};

pub trait ActorReference {
    /// Actor name.
    ///
    /// Unique among siblings.
    fn name(&self) -> &str;

    /// Actor URI.
    ///
    /// Returns the URI for this actor.
    fn uri(&self) -> &ActorUri;

    /// Actor path.
    ///
    /// e.g. `/user/actor_a/actor_b`
    fn path(&self) -> &ActorPath;

    /// True if this actor is the top level root
    ///
    /// I.e. `/root`
    fn is_root(&self) -> bool;

    /// User root reference
    ///
    /// I.e. `/root/user`
    fn user_root(&self) -> BasicActorRef;

    /// Parent reference
    ///
    /// Returns the `BasicActorRef` of this actor's parent actor
    fn parent(&self) -> BasicActorRef;

    /// True is this actor has any children actors
    fn has_children(&self) -> bool;

    /// True if the given actor is a child of this actor
    fn is_child(&self, actor: &BasicActorRef) -> bool;

    /// Iterator over children references.
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = BasicActorRef> + 'a>;

    /// Stop this actor and its children.
    fn stop(&self);
}

pub trait SystemTell {
    fn send_system_message(&self, msg: SystemMsg);
}

pub type BoxedTell<T> = Box<dyn Tell<T> + Send + 'static>;

#[allow(private_bounds)]
pub trait Tell<T>: ActorReference + SystemTell + Send + 'static {
    fn tell(&self, msg: T, sender: Sender);
    fn box_clone(&self) -> BoxedTell<T>;
}

impl<T, M> Tell<T> for ActorRef<M>
where
    T: LocalMessage + Into<M>,
    M: LocalMessage,
{
    fn tell(&self, msg: T, sender: Sender) {
        self.send_msg(msg.into(), sender);
    }

    fn box_clone(&self) -> BoxedTell<T> {
        Box::new((*self).clone())
    }
}

impl<T> ActorReference for BoxedTell<T>
where
    T: LocalMessage,
{
    /// Actor name.
    ///
    /// Unique among siblings.
    fn name(&self) -> &str {
        (**self).name()
    }

    fn uri(&self) -> &ActorUri {
        (**self).uri()
    }

    /// Actor path.
    ///
    /// e.g. `/user/actor_a/actor_b`
    fn path(&self) -> &ActorPath {
        (**self).path()
    }

    fn is_root(&self) -> bool {
        (**self).is_root()
    }

    /// Parent reference.
    fn parent(&self) -> BasicActorRef {
        (**self).parent()
    }

    fn user_root(&self) -> BasicActorRef {
        (**self).user_root()
    }

    fn has_children(&self) -> bool {
        (**self).has_children()
    }

    fn is_child(&self, actor: &BasicActorRef) -> bool {
        (**self).is_child(actor)
    }

    /// Iterator over children references.
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = BasicActorRef> + 'a> {
        (**self).children()
    }

    fn stop(&self) {
        (**self).stop();
    }
}

impl<T> SystemTell for BoxedTell<T>
where
    T: LocalMessage,
{
    fn send_system_message(&self, msg: SystemMsg) {
        (**self).send_system_message(msg);
    }
}

impl<T> PartialEq for BoxedTell<T> {
    fn eq(&self, other: &Self) -> bool {
        self.path() == other.path()
    }
}

impl<T> fmt::Debug for BoxedTell<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Tell[{:?}]", self.uri())
    }
}

impl<T> fmt::Display for BoxedTell<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Tell[{}]", self.uri())
    }
}

impl<T: 'static> Clone for BoxedTell<T> {
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

/// A lightweight, un-typed reference to interact with its underlying
/// actor instance through concurrent messaging.
///
/// `BasicActorRef` can be derived from an original `ActorRef<Msg>`.
///
/// `BasicActorRef` allows for un-typed messaging using `try_tell`,
/// that will return a `Result`. If the message type was not supported,
/// the result will contain an `Error`.
///
/// `BasicActorRef` can be used when the original `ActorRef` isn't available,
/// when you need to use collections to store references from different actor
/// types, or when using actor selections to message parts of the actor hierarchy.
///
/// In general, it is better to use `ActorRef` where possible.
#[derive(Clone)]
pub struct BasicActorRef {
    pub(crate) cell: ActorCell,
}

impl BasicActorRef {
    #[doc(hidden)]
    #[must_use]
    pub fn new(cell: ActorCell) -> Self {
        Self { cell }
    }

    #[must_use]
    pub fn typed<Msg: LocalMessage>(&self, cell: ExtendedCell<Msg>) -> ActorRef<Msg> {
        ActorRef { cell }
    }

    pub(crate) fn sys_init(&self, sys: &ActorSystem) {
        self.cell.kernel().sys_init(sys);
    }

    /// Send a message to this actor
    ///
    /// Returns a result. If the message type is not supported Error is returned.
    ///
    /// # Errors
    ///
    /// Returns `AnyEnqueueError` if the actor does not accept `Msg`, the
    /// message cannot be serialized for a remote reply reference, or the
    /// recipient no longer accepts messages.
    pub fn try_tell<Msg>(
        &self,
        msg: Msg,
        sender: impl Into<Option<Self>>,
    ) -> Result<(), AnyEnqueueError>
    where
        Msg: Message + Send,
    {
        #[cfg(feature = "remoting")]
        if let Some(result) = self.cell.send_remote_reply(&msg) {
            return result;
        }
        self.try_tell_any(&mut AnyMessage::new(msg), sender)
    }

    /// Send a type-erased message to this actor.
    ///
    /// # Errors
    ///
    /// Returns `AnyEnqueueError` if the message type does not match the
    /// actor's message type or its mailbox no longer accepts messages.
    pub fn try_tell_any(
        &self,
        msg: &mut AnyMessage,
        sender: impl Into<Option<Self>>,
    ) -> Result<(), AnyEnqueueError> {
        #[cfg(feature = "remoting")]
        if self.cell.is_remote_reply() {
            return Err(AnyEnqueueError);
        }
        self.cell.send_any_msg(msg, sender.into())
    }
}

impl ActorReference for BasicActorRef {
    /// Actor name.
    ///
    /// Unique among siblings.
    fn name(&self) -> &str {
        self.cell.uri().name.as_ref()
    }

    fn uri(&self) -> &ActorUri {
        self.cell.uri()
    }

    /// Actor path.
    ///
    /// e.g. `/user/actor_a/actor_b`
    fn path(&self) -> &ActorPath {
        &self.cell.uri().path
    }

    fn is_root(&self) -> bool {
        self.cell.is_root()
    }

    /// Parent reference.
    fn parent(&self) -> BasicActorRef {
        self.cell.parent()
    }

    fn user_root(&self) -> BasicActorRef {
        self.cell.user_root()
    }

    fn has_children(&self) -> bool {
        self.cell.has_children()
    }

    fn is_child(&self, actor: &BasicActorRef) -> bool {
        self.cell.is_child(actor)
    }

    /// Iterator over children references.
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = BasicActorRef> + 'a> {
        self.cell.children()
    }

    fn stop(&self) {
        self.send_system_message(SystemCmd::Stop.into());
    }
}

impl SystemTell for BasicActorRef {
    fn send_system_message(&self, msg: SystemMsg) {
        #[cfg(feature = "remoting")]
        if self.cell.is_remote_reply() {
            return;
        }
        let envelope = Envelope { msg, sender: None };
        let _ = self.cell.send_sys_msg(envelope);
    }
}

impl ActorReference for &BasicActorRef {
    /// Actor name.
    ///
    /// Unique among siblings.
    fn name(&self) -> &str {
        (**self).name()
    }

    fn uri(&self) -> &ActorUri {
        (**self).uri()
    }

    /// Actor path.
    ///
    /// e.g. `/user/actor_a/actor_b`
    fn path(&self) -> &ActorPath {
        (**self).path()
    }

    fn is_root(&self) -> bool {
        (**self).is_root()
    }

    /// Parent reference.
    fn parent(&self) -> BasicActorRef {
        self.cell.parent()
    }

    fn user_root(&self) -> BasicActorRef {
        self.cell.user_root()
    }

    fn has_children(&self) -> bool {
        (**self).has_children()
    }

    fn is_child(&self, actor: &BasicActorRef) -> bool {
        (**self).is_child(actor)
    }

    /// Iterator over children references.
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = BasicActorRef> + 'a> {
        (**self).children()
    }

    fn stop(&self) {
        self.send_system_message(SystemCmd::Stop.into());
    }
}

impl SystemTell for &BasicActorRef {
    fn send_system_message(&self, msg: SystemMsg) {
        (**self).send_system_message(msg);
    }
}

impl fmt::Debug for BasicActorRef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BasicActorRef[{:?}]", self.uri())
    }
}

impl fmt::Display for BasicActorRef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BasicActorRef[{}]", self.uri())
    }
}

impl PartialEq for BasicActorRef {
    fn eq(&self, other: &Self) -> bool {
        self.path() == other.path()
    }
}

impl<Msg> From<ActorRef<Msg>> for BasicActorRef
where
    Msg: LocalMessage,
{
    fn from(actor: ActorRef<Msg>) -> Self {
        Self::new(ActorCell::from(actor.cell))
    }
}

impl<Msg> From<ActorRef<Msg>> for Option<BasicActorRef>
where
    Msg: LocalMessage,
{
    fn from(actor: ActorRef<Msg>) -> Self {
        Some(BasicActorRef::new(ActorCell::from(actor.cell)))
    }
}

pub type Sender = Option<BasicActorRef>;

/// A lightweight, typed reference to interact with its underlying
/// actor instance through concurrent messaging.
///
/// All `ActorRefs` are products of `system.actor_of`
/// or `context.actor_of`. When an actor is created using `actor_of`
/// an `ActorRef<Msg>` is returned, where `Msg` is the mailbox
/// message type for the actor.
///
/// Actor references are lightweight and can be cloned without concern
/// for memory use.
///
/// Messages sent to an actor are added to the actor's mailbox.
///
/// In the event that the underlying actor is terminated messages sent
/// to the actor will be routed to dead letters.
///
/// If an actor is restarted all existing references continue to
/// be valid.
pub struct ActorRef<Msg: LocalMessage> {
    pub(crate) cell: ExtendedCell<Msg>,
}

impl<Msg: LocalMessage> Clone for ActorRef<Msg> {
    fn clone(&self) -> Self {
        Self {
            cell: self.cell.clone(),
        }
    }
}

impl<Msg: LocalMessage> ActorRef<Msg> {
    #[doc(hidden)]
    #[must_use]
    pub fn new(cell: ExtendedCell<Msg>) -> Self {
        Self { cell }
    }

    pub fn send_msg(&self, msg: Msg, sender: impl Into<Option<BasicActorRef>>) {
        let envelope = Envelope {
            msg,
            sender: sender.into(),
        };
        // consume the result (we don't return it to user)
        let _ = self.cell.send_msg(envelope);
    }
}

impl<Msg: LocalMessage> ActorReference for ActorRef<Msg> {
    /// Actor name.
    ///
    /// Unique among siblings.
    fn name(&self) -> &str {
        self.cell.uri().name.as_ref()
    }

    fn uri(&self) -> &ActorUri {
        self.cell.uri()
    }

    /// Actor path.
    ///
    /// e.g. `/user/actor_a/actor_b`
    fn path(&self) -> &ActorPath {
        &self.cell.uri().path
    }

    fn is_root(&self) -> bool {
        self.cell.is_root()
    }

    /// Parent reference.
    fn parent(&self) -> BasicActorRef {
        self.cell.parent()
    }

    fn user_root(&self) -> BasicActorRef {
        self.cell.user_root()
    }

    fn has_children(&self) -> bool {
        self.cell.has_children()
    }

    fn is_child(&self, actor: &BasicActorRef) -> bool {
        self.cell.is_child(actor)
    }

    /// Iterator over children references.
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = BasicActorRef> + 'a> {
        self.cell.children()
    }

    fn stop(&self) {
        self.send_system_message(SystemCmd::Stop.into());
    }
}

impl<Msg: LocalMessage> SystemTell for &ActorRef<Msg> {
    fn send_system_message(&self, msg: SystemMsg) {
        let envelope = Envelope { msg, sender: None };
        let _ = self.cell.send_sys_msg(envelope);
    }
}

impl<Msg: LocalMessage> ActorReference for &ActorRef<Msg> {
    /// Actor name.
    ///
    /// Unique among siblings.
    fn name(&self) -> &str {
        self.cell.uri().name.as_ref()
    }

    fn uri(&self) -> &ActorUri {
        self.cell.uri()
    }

    /// Actor path.
    ///
    /// e.g. `/user/actor_a/actor_b`
    fn path(&self) -> &ActorPath {
        &self.cell.uri().path
    }

    fn is_root(&self) -> bool {
        self.cell.is_root()
    }

    /// Parent reference.
    fn parent(&self) -> BasicActorRef {
        self.cell.parent()
    }

    fn user_root(&self) -> BasicActorRef {
        self.cell.user_root()
    }

    fn has_children(&self) -> bool {
        self.cell.has_children()
    }

    fn is_child(&self, actor: &BasicActorRef) -> bool {
        self.cell.is_child(actor)
    }

    /// Iterator over children references.
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = BasicActorRef> + 'a> {
        self.cell.children()
    }

    fn stop(&self) {
        self.send_system_message(SystemCmd::Stop.into());
    }
}

impl<Msg: LocalMessage> SystemTell for ActorRef<Msg> {
    fn send_system_message(&self, msg: SystemMsg) {
        let envelope = Envelope { msg, sender: None };
        let _ = self.cell.send_sys_msg(envelope);
    }
}

impl<Msg: LocalMessage> fmt::Debug for ActorRef<Msg> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ActorRef[{:?}]", self.uri())
    }
}

impl<Msg: LocalMessage> fmt::Display for ActorRef<Msg> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ActorRef[{}]", self.uri())
    }
}

impl<Msg: LocalMessage> PartialEq for ActorRef<Msg> {
    fn eq(&self, other: &Self) -> bool {
        self.uri().path == other.uri().path
    }
}

/// Produces `ActorRef`s. Actor creation runs the actor factory synchronously,
/// then returns the reference after scheduling the actor's initialization.
///
/// `pre_start` and `post_start` may not have run when this method returns.
/// Failures in those hooks are handled by the actor runtime and are not
/// reported as errors from actor creation.
///
/// # Errors
///
/// The `actor_of*` methods return [`CreateError::InvalidName`] if `name` is
/// not a valid actor name, [`CreateError::AlreadyExists`] if an actor with
/// the same path exists, [`CreateError::Panicked`] if the actor factory
/// panics, and [`CreateError::System`] if the actor system cannot start it.
pub trait ActorRefFactory {
    /// Create an actor from `props`.
    ///
    /// # Errors
    ///
    /// See [`ActorRefFactory`].
    fn actor_of_props<A>(
        &self,
        name: &str,
        props: BoxActorProd<A>,
    ) -> Result<ActorRef<A::Msg>, CreateError>
    where
        A: Actor,
        <A as Actor>::Msg: Message;

    /// Create an actor using its [`ActorFactory`] implementation.
    ///
    /// # Errors
    ///
    /// See [`ActorRefFactory`].
    fn actor_of<A>(&self, name: &str) -> Result<ActorRef<<A as Actor>::Msg>, CreateError>
    where
        A: ActorFactory + Actor,
        <A as Actor>::Msg: Message;

    /// Create an actor using its [`ActorFactoryArgs`] implementation.
    ///
    /// # Errors
    ///
    /// See [`ActorRefFactory`].
    fn actor_of_args<A, Args>(
        &self,
        name: &str,
        args: Args,
    ) -> Result<ActorRef<<A as Actor>::Msg>, CreateError>
    where
        Args: ActorArgs,
        A: ActorFactoryArgs<Args>,
        <A as Actor>::Msg: Message;

    /// Create an actor without requiring its message type to implement
    /// [`Message`]. Picard uses it for its own actors, such as channels,
    /// whose messages hold actor references.
    ///
    /// # Errors
    ///
    /// See [`ActorRefFactory`].
    #[doc(hidden)]
    fn local_actor_of_props<A>(
        &self,
        name: &str,
        props: BoxActorProd<A>,
    ) -> Result<ActorRef<A::Msg>, CreateError>
    where
        A: Actor;

    fn stop(&self, actor: impl ActorReference);
}

/// Produces `ActorRef`s under the `temp` guardian actor.
pub trait TmpActorRefFactory {
    /// Create a temporary actor from `props`.
    ///
    /// # Errors
    ///
    /// Returns [`CreateError::Panicked`] if the actor factory panics, or
    /// [`CreateError::System`] if the actor system cannot start the actor.
    fn tmp_actor_of_props<A>(
        &self,
        props: BoxActorProd<A>,
    ) -> Result<ActorRef<A::Msg>, CreateError>
    where
        A: Actor,
        <A as Actor>::Msg: Message;

    /// Create a temporary actor using its [`ActorFactory`] implementation.
    ///
    /// # Errors
    ///
    /// See [`TmpActorRefFactory::tmp_actor_of_props`].
    fn tmp_actor_of<A>(&self) -> Result<ActorRef<<A as Actor>::Msg>, CreateError>
    where
        A: ActorFactory + Actor,
        <A as Actor>::Msg: Message;

    /// Create a temporary actor using its [`ActorFactoryArgs`] implementation.
    ///
    /// # Errors
    ///
    /// See [`TmpActorRefFactory::tmp_actor_of_props`].
    fn tmp_actor_of_args<A, Args>(
        &self,
        args: Args,
    ) -> Result<ActorRef<<A as Actor>::Msg>, CreateError>
    where
        Args: ActorArgs,
        A: ActorFactoryArgs<Args>,
        <A as Actor>::Msg: Message;
}
