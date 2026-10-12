#[cfg(feature = "remoting")]
use dashmap::DashMap;
use dashmap::DashSet;
use slog::trace;

use std::sync::Arc;

use crate::system::LoggingSystem;
#[cfg(feature = "remoting")]
use crate::{Envelope, kernel::mailbox::ProcessedAck};
use crate::{
    actor::actor_cell::{ActorCell, ExtendedCell},
    actor::actor_ref::SystemTell,
    actor::{
        Actor, ActorFactoryArgs, ActorPath, ActorRef, ActorReference, ActorUri, BasicActorRef,
        BoxActorProd, Context, CreateError, Props,
    },
    kernel::kernel,
    kernel::mailbox::{Mailbox, mailbox},
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
    #[cfg(feature = "remoting")]
    forwarded_paths: DashSet<ActorPath>,
    #[cfg(feature = "remoting")]
    drain_handlers: DashMap<ActorPath, Arc<dyn Fn() -> Vec<serde_json::Value> + Send + Sync>>,
    #[cfg(feature = "remoting")]
    cell_forwarders: DashMap<ActorPath, ActorCell>,
    #[cfg(feature = "remoting")]
    migration_handlers: DashMap<String, crate::remoting::MigrationHandler>,
    #[cfg(all(feature = "console", not(target_arch = "wasm32")))]
    console: crate::console::Registry,
}

#[cfg(feature = "remoting")]
type RemoteDispatcher = Arc<
    dyn Fn(
            serde_json::Value,
            Option<BasicActorRef>,
            Option<ProcessedAck>,
        ) -> Result<(), crate::remoting::RemoteError>
        + Send
        + Sync,
>;

impl Provider {
    pub fn new() -> Self {
        let inner = ProviderInner {
            paths: DashSet::new(),
            #[cfg(feature = "remoting")]
            remote_dispatchers: DashMap::new(),
            #[cfg(feature = "remoting")]
            forwarded_paths: DashSet::new(),
            #[cfg(feature = "remoting")]
            drain_handlers: DashMap::new(),
            #[cfg(feature = "remoting")]
            cell_forwarders: DashMap::new(),
            #[cfg(feature = "remoting")]
            migration_handlers: DashMap::new(),
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
        let (actor, mb, stop) = self.create_uninitialized_actor(props, name, parent, sys)?;
        #[cfg(feature = "remoting")]
        {
            let encoder: crate::actor::actor_cell::EncoderFn = Arc::new(|any| {
                any.downcast_ref::<A::Msg>()
                    .and_then(|m| serde_json::to_value(m).ok())
            });
            actor.cell.set_encoder(encoder);
            self.register_remote_actor(actor.clone(), actor.cell.to_actor_cell(), mb);
        }
        #[cfg(not(feature = "remoting"))]
        let _ = mb;
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
        let (actor, _mb, stop) = self.create_uninitialized_actor(props, name, parent, sys)?;
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

    #[allow(clippy::type_complexity)]
    fn create_uninitialized_actor<A>(
        &self,
        props: BoxActorProd<A>,
        name: &str,
        parent: &BasicActorRef,
        sys: &ActorSystem,
    ) -> Result<(ActorRef<A::Msg>, Mailbox<A::Msg>, bool), CreateError>
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
        let mb_clone = mb.clone();

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

        Ok((actor, mb_clone, stop))
    }

    fn register(&self, path: &ActorPath) -> Result<(), CreateError> {
        if self.inner.paths.insert(path.clone()) {
            Ok(())
        } else {
            Err(CreateError::AlreadyExists(path.clone()))
        }
    }

    pub fn unregister(&self, path: &ActorPath) {
        #[cfg(feature = "remoting")]
        if self.inner.forwarded_paths.contains(path) {
            return;
        }
        self.inner.paths.remove(path);
        #[cfg(feature = "remoting")]
        {
            self.inner.remote_dispatchers.remove(path);
            self.inner.drain_handlers.remove(path);
            self.inner.cell_forwarders.remove(path);
        }
        #[cfg(all(feature = "console", not(target_arch = "wasm32")))]
        self.inner.console.remove(path.as_str());
    }

    #[cfg(feature = "remoting")]
    pub fn has_actor(&self, path: &ActorPath) -> bool {
        self.inner.paths.contains(path)
    }

    #[cfg(feature = "remoting")]
    pub fn get_cell(&self, path: &ActorPath) -> Option<ActorCell> {
        self.inner.cell_forwarders.get(path).map(|c| c.clone())
    }

    #[cfg(feature = "remoting")]
    pub fn drain_mailbox(&self, path: &ActorPath) -> Vec<serde_json::Value> {
        self.inner
            .drain_handlers
            .get(path)
            .map(|drain| drain())
            .unwrap_or_default()
    }

    #[cfg(feature = "remoting")]
    pub fn register_migration_handler(
        &self,
        type_name: String,
        handler: crate::remoting::MigrationHandler,
    ) {
        self.inner.migration_handlers.insert(type_name, handler);
    }

    #[cfg(feature = "remoting")]
    pub fn install_forwarder(
        &self,
        path: &ActorPath,
        forward_fn: Arc<
            dyn Fn(serde_json::Value) -> Result<(), crate::kernel::mailbox::AnyEnqueueError>
                + Send
                + Sync,
        >,
        target_system_id: uuid::Uuid,
        target_endpoint: Option<String>,
        target_path: String,
        connection: crate::remoting::RemoteConnection,
    ) {
        self.inner.forwarded_paths.insert(path.clone());
        if let Some(cell) = self.inner.cell_forwarders.get(path) {
            cell.set_forwarder(forward_fn);
        }
        let fwd_conn = connection;
        let fwd_path = target_path;
        let fwd_endpoint = target_endpoint;
        self.inner.remote_dispatchers.insert(
            path.clone(),
            Arc::new(move |payload, _sender, _| {
                let address = crate::remoting::ActorAddress {
                    system_id: target_system_id,
                    endpoint: fwd_endpoint.clone(),
                    path: fwd_path.clone(),
                };
                let fut = fwd_conn.send(address, None, payload);
                crate::remoting::block_on(fut).map_err(|e| {
                    crate::remoting::RemoteError::Rejected(format!("forwarding failed: {e}"))
                })?;
                Ok(())
            }),
        );
    }

    #[cfg(feature = "remoting")]
    pub fn handle_migration(
        &self,
        system: &ActorSystem,
        payload: &crate::remoting::MigrationPayload,
    ) -> Result<(), String> {
        let handler = self
            .inner
            .migration_handlers
            .get(&payload.type_name)
            .map(|h| Arc::clone(h.value()))
            .ok_or_else(|| {
                format!(
                    "No migration handler registered for '{}'",
                    payload.type_name
                )
            })?;

        handler(system, payload)?;

        if !payload.in_flight.is_empty() {
            let dest_path = ActorPath::new(&payload.destination_path);
            if let Some(dispatcher) = self.inner.remote_dispatchers.get(&dest_path) {
                for msg in &payload.in_flight {
                    dispatcher(msg.clone(), None, None)
                        .map_err(|e| format!("in-flight delivery error: {e}"))?;
                }
            }
        }

        Ok(())
    }

    #[cfg(feature = "remoting")]
    fn register_remote_actor<Msg>(
        &self,
        actor: ActorRef<Msg>,
        cell: ActorCell,
        mailbox: Mailbox<Msg>,
    ) where
        Msg: crate::Message,
    {
        let path = actor.path().clone();
        let target = actor;
        self.inner.remote_dispatchers.insert(
            path.clone(),
            Arc::new(move |payload, sender, processed_ack| {
                let msg = serde_json::from_value::<Msg>(payload)
                    .map_err(|err| crate::remoting::RemoteError::InvalidMessage(err.to_string()))?;
                let envelope = Envelope { sender, msg };
                let result = if let Some(ack) = processed_ack {
                    target.cell.send_msg_with_ack(envelope, ack)
                } else {
                    target.cell.send_msg(envelope)
                };
                result.map_err(|err| crate::remoting::RemoteError::Rejected(format!("{err:?}")))
            }),
        );
        self.inner.cell_forwarders.insert(path.clone(), cell);
        let mb_drain = mailbox;
        self.inner.drain_handlers.insert(
            path,
            Arc::new(move || {
                let mut drained = Vec::new();
                while let Ok(envelope) = mb_drain.try_dequeue() {
                    if let Ok(val) = serde_json::to_value(&envelope.msg) {
                        drained.push(val);
                    }
                }
                drained
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
        if request.processed_timeout_ms.is_some()
            && (request.sender.is_some() || request.reply_timeout_ms.is_some())
        {
            return Err(crate::remoting::RemoteError::UnsupportedSender);
        }
        if request
            .processed_timeout_ms
            .is_some_and(|timeout_ms| timeout_ms > crate::remoting::MAX_REMOTE_ASK_TIMEOUT_MS)
        {
            return Err(crate::remoting::RemoteError::TimeoutTooLong);
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
        let (processed_ack, processed_receiver) = if request.processed_timeout_ms.is_some() {
            let (sender, receiver) = std::sync::mpsc::sync_channel(1);
            (Some(ProcessedAck::new(sender)), Some(receiver))
        } else {
            (None, None)
        };
        dispatcher(request.payload.clone(), reply_sender, processed_ack)?;
        if let (Some(receiver), Some(timeout_ms)) =
            (processed_receiver, request.processed_timeout_ms)
        {
            receiver
                .recv_timeout(std::time::Duration::from_millis(timeout_ms))
                .map_err(|error| match error {
                    std::sync::mpsc::RecvTimeoutError::Timeout
                    | std::sync::mpsc::RecvTimeoutError::Disconnected => {
                        crate::remoting::RemoteError::ProcessedTimeout
                    }
                })?;
        }
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
