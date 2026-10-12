#[cfg(feature = "remoting")]
use dashmap::DashMap;
use dashmap::DashSet;
use slog::trace;

use std::sync::Arc;

use crate::system::LoggingSystem;
use crate::{
    actor::actor_cell::{ActorCell, ExtendedCell},
    actor::actor_ref::SystemTell,
    actor::{
        Actor, ActorFactoryArgs, ActorPath, ActorRef, ActorReference, ActorUri, BasicActorRef,
        BoxActorProd, Context, CreateError, Props,
    },
    kernel::kernel,
    kernel::mailbox::mailbox,
    system::{ActorSystem, SysActors, SystemCmd, SystemMsg},
    validate::validate_name,
};

#[derive(Clone)]
pub struct Provider {
    inner: Arc<ProviderInner>,
}

struct ProviderInner {
    paths: DashSet<ActorPath>,
    #[cfg(feature = "remoting")]
    remote_dispatchers: DashMap<ActorPath, RemoteDispatcher>,
    #[cfg(all(feature = "console", not(target_arch = "wasm32")))]
    console: crate::console::Registry,
}

#[cfg(feature = "remoting")]
type RemoteDispatcher = Arc<
    dyn Fn(serde_json::Value, Option<BasicActorRef>) -> Result<(), crate::remoting::RemoteError>
        + Send
        + Sync,
>;

impl Provider {
    pub fn new() -> Self {
        let inner = ProviderInner {
            paths: DashSet::new(),
            #[cfg(feature = "remoting")]
            remote_dispatchers: DashMap::new(),
            #[cfg(all(feature = "console", not(target_arch = "wasm32")))]
            console: crate::console::Registry::default(),
        };

        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn create_actor<A>(
        &self,
        props: BoxActorProd<A>,
        name: &str,
        parent: &BasicActorRef,
        sys: &ActorSystem,
    ) -> Result<ActorRef<A::Msg>, CreateError>
    where
        A: Actor + 'static,
        A::Msg: crate::Message,
    {
        let (actor, stop) = self.create_uninitialized_actor(props, name, parent, sys)?;
        #[cfg(feature = "remoting")]
        self.register_remote_actor(actor.clone());
        #[cfg(all(feature = "console", not(target_arch = "wasm32")))]
        self.console().enable_management(actor.path().as_str());
        #[cfg(all(feature = "console", feature = "serde", not(target_arch = "wasm32")))]
        self.console().enable_json::<A::Msg>(actor.path().as_str());
        Self::init(&actor, stop);
        Ok(actor)
    }

    pub fn create_local_actor<A>(
        &self,
        props: BoxActorProd<A>,
        name: &str,
        parent: &BasicActorRef,
        sys: &ActorSystem,
    ) -> Result<ActorRef<A::Msg>, CreateError>
    where
        A: Actor + 'static,
    {
        let (actor, stop) = self.create_uninitialized_actor(props, name, parent, sys)?;
        Self::init(&actor, stop);
        Ok(actor)
    }

    /// Starts the actor, stopping it straight away when its parent is already terminating
    /// so the parent's termination is not blocked by a child it never told to stop.
    fn init<Msg: crate::LocalMessage>(actor: &ActorRef<Msg>, stop: bool) {
        actor.send_system_message(SystemMsg::ActorInit);
        if stop {
            actor.send_system_message(SystemCmd::Stop.into());
        }
    }

    fn create_uninitialized_actor<A>(
        &self,
        props: BoxActorProd<A>,
        name: &str,
        parent: &BasicActorRef,
        sys: &ActorSystem,
    ) -> Result<(ActorRef<A::Msg>, bool), CreateError>
    where
        A: Actor + 'static,
    {
        validate_name(name)?;

        let path = ActorPath::new(&format!("{}/{}", parent.path(), name));
        trace!(sys.log(), "Attempting to create actor at: {}", path);

        self.register(&path)?;

        let uri = ActorUri {
            path: path.clone(),
            name: Arc::from(name),
            host: sys.host(),
        };

        let (sender, sys_sender, mb) = mailbox::<A::Msg>(sys.sys_settings().msg_process_limit);

        let cell = ExtendedCell::new(
            uri,
            Some(parent.clone()),
            sys,
            Arc::new(sender.clone()),
            sys_sender,
            sender,
        );

        let k = match kernel(props, cell.clone(), mb, sys) {
            Ok(kernel) => kernel,
            Err(err) => {
                self.unregister(&path);
                return Err(err);
            }
        };
        let cell = cell.init(&k);

        let actor = ActorRef::new(cell);
        let child = BasicActorRef::from(actor.clone());
        let stop = parent.cell.add_child(child);

        Ok((actor, stop))
    }

    fn register(&self, path: &ActorPath) -> Result<(), CreateError> {
        if self.inner.paths.insert(path.clone()) {
            Ok(())
        } else {
            Err(CreateError::AlreadyExists(path.clone()))
        }
    }

    pub fn unregister(&self, path: &ActorPath) {
        self.inner.paths.remove(path);
        #[cfg(feature = "remoting")]
        self.inner.remote_dispatchers.remove(path);
        #[cfg(all(feature = "console", not(target_arch = "wasm32")))]
        self.inner.console.remove(path.as_str());
    }

    #[cfg(feature = "remoting")]
    fn register_remote_actor<Msg>(&self, actor: ActorRef<Msg>)
    where
        Msg: crate::Message,
    {
        let path = actor.path().clone();
        let target = BasicActorRef::from(actor);
        self.inner.remote_dispatchers.insert(
            path,
            Arc::new(move |payload, sender| {
                let msg = serde_json::from_value::<Msg>(payload)
                    .map_err(|err| crate::remoting::RemoteError::InvalidMessage(err.to_string()))?;
                target
                    .try_tell(msg, sender)
                    .map_err(|err| crate::remoting::RemoteError::Rejected(format!("{err:?}")))
            }),
        );
    }

    #[cfg(feature = "remoting")]
    pub(crate) fn dispatch_remote(
        &self,
        system: &ActorSystem,
        request: &crate::remoting::RemoteRequest,
    ) -> Result<Option<serde_json::Value>, crate::remoting::RemoteError> {
        if request.destination.system_id != system.id() {
            return Err(crate::remoting::RemoteError::Rejected(
                "destination system ID does not match this system".into(),
            ));
        }
        if request.sender.is_some() != request.reply_timeout_ms.is_some() {
            return Err(crate::remoting::RemoteError::UnsupportedSender);
        }
        if request
            .reply_timeout_ms
            .is_some_and(|timeout_ms| timeout_ms > crate::remoting::MAX_REMOTE_ASK_TIMEOUT_MS)
        {
            return Err(crate::remoting::RemoteError::TimeoutTooLong);
        }

        let (reply_sender, reply_receiver) = if let Some(address) = &request.sender {
            if address.endpoint.is_some() {
                return Err(crate::remoting::RemoteError::UnsupportedSender);
            }
            let (sender, receiver) = std::sync::mpsc::sync_channel(1);
            let name = address.path.rsplit('/').next().unwrap_or("remote-ask");
            let uri = ActorUri {
                name: Arc::from(name),
                path: ActorPath::new(&address.path),
                host: Arc::from(address.system_id.to_string()),
            };
            (
                Some(BasicActorRef::new(
                    system.temp_root().cell.with_remote_reply(uri, sender),
                )),
                Some(receiver),
            )
        } else {
            (None, None)
        };
        let path = ActorPath::new(&request.destination.path);
        let dispatcher = self
            .inner
            .remote_dispatchers
            .get(&path)
            .map(|dispatcher| dispatcher.clone())
            .ok_or_else(|| crate::remoting::RemoteError::ActorNotFound(path.to_string()))?;
        dispatcher(request.payload.clone(), reply_sender)?;
        match (reply_receiver, request.reply_timeout_ms) {
            (Some(receiver), Some(timeout_ms)) => receiver
                .recv_timeout(std::time::Duration::from_millis(timeout_ms))
                .map(Some)
                .map_err(|error| match error {
                    std::sync::mpsc::RecvTimeoutError::Timeout => {
                        crate::remoting::RemoteError::Timeout
                    }
                    std::sync::mpsc::RecvTimeoutError::Disconnected => {
                        crate::remoting::RemoteError::NoReply
                    }
                }),
            (None, None) => Ok(None),
            _ => Err(crate::remoting::RemoteError::UnsupportedSender),
        }
    }

    #[cfg(all(feature = "console", not(target_arch = "wasm32")))]
    pub(crate) fn console(&self) -> &crate::console::Registry {
        &self.inner.console
    }
}

pub fn create_root(sys: &ActorSystem) -> SysActors {
    let root = root(sys);

    SysActors {
        user: guardian("user", "/user", &root, sys),
        sysm: guardian("system", "/system", &root, sys),
        temp: guardian("temp", "/temp", &root, sys),
        root,
    }
}

fn root(sys: &ActorSystem) -> BasicActorRef {
    let uri = ActorUri {
        name: Arc::from("root"),
        path: ActorPath::new("/"),
        host: Arc::from("localhost"),
    };
    let (sender, sys_sender, _mb) = mailbox::<SystemMsg>(100);

    // Big bang: all actors have a parent.
    // This means root also needs a parent.
    // An ActorCell, ActorRef and KernelRef are created
    // independently without an actor being created.
    // The big bang cell has no kernel: it is just a channel to nowhere.

    let bb_cell = ActorCell::new(uri.clone(), None, sys, Arc::new(sender), sys_sender);

    let bigbang = BasicActorRef::new(bb_cell);

    // root
    let props: BoxActorProd<Guardian> =
        Props::new_args::<Guardian, _>(("root".to_string(), sys.log()));
    let (sender, sys_sender, mb) = mailbox::<SystemMsg>(100);

    let cell = ExtendedCell::new(
        uri,
        Some(bigbang),
        sys,
        Arc::new(sender.clone()),
        sys_sender,
        sender,
    );

    let k = kernel(props, cell.clone(), mb, sys).unwrap();
    let cell = cell.init(&k);
    let actor_ref = ActorRef::new(cell);

    BasicActorRef::from(actor_ref)
}

fn guardian(name: &str, path: &str, root: &BasicActorRef, sys: &ActorSystem) -> BasicActorRef {
    let uri = ActorUri {
        name: Arc::from(name),
        path: ActorPath::new(path),
        host: Arc::from("localhost"),
    };

    let props: BoxActorProd<Guardian> =
        Props::new_args::<Guardian, _>((name.to_string(), sys.log()));
    let (sender, sys_sender, mb) = mailbox::<SystemMsg>(100);

    let cell = ExtendedCell::new(
        uri,
        Some(root.clone()),
        sys,
        Arc::new(sender.clone()),
        sys_sender,
        sender,
    );

    let k = kernel(props, cell.clone(), mb, sys).unwrap();
    let cell = cell.init(&k);
    let actor_ref = ActorRef::new(cell);

    let actor = BasicActorRef::from(actor_ref);
    let _ = root.cell.add_child(actor.clone());
    actor
}

struct Guardian {
    name: String,
    log: LoggingSystem,
}

impl ActorFactoryArgs<(String, LoggingSystem)> for Guardian {
    fn create_args((name, log): (String, LoggingSystem)) -> Self {
        Self { name, log }
    }
}

impl Actor for Guardian {
    type Msg = SystemMsg;

    fn recv(&mut self, _: &Context<Self::Msg>, _: Self::Msg, _: Option<BasicActorRef>) {}

    fn post_stop(&mut self) {
        trace!(self.log, "{} guardian stopped", self.name);
    }
}
