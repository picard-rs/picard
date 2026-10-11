pub(crate) mod logger;
pub(crate) mod timer;

use std::{collections::HashMap, fmt};

use better_default::Default;
use config::ConfigError;

use crate::actor::BasicActorRef;
use serde::Deserialize;
use thiserror::Error;

// Public picard::system API (plus the pub data types in this file)
pub use self::timer::{BasicTimer, ScheduleId, Timer};

#[derive(Clone, Debug)]
pub enum SystemMsg {
    ActorInit,
    Command(SystemCmd),
    Event(SystemEvent),
    Failed(BasicActorRef),
}

unsafe impl Send for SystemMsg {}

#[derive(Clone, Debug)]
pub enum SystemCmd {
    Stop,
    Restart,
}

impl From<SystemCmd> for SystemMsg {
    fn from(val: SystemCmd) -> Self {
        SystemMsg::Command(val)
    }
}

#[derive(Clone, Debug)]
pub enum SystemEvent {
    /// An actor was terminated
    ActorCreated(ActorCreated),

    /// An actor was restarted
    ActorRestarted(ActorRestarted),

    /// An actor was started
    ActorTerminated(ActorTerminated),
}

impl From<SystemEvent> for SystemMsg {
    fn from(val: SystemEvent) -> Self {
        SystemMsg::Event(val)
    }
}

#[derive(Clone, Debug)]
pub struct ActorCreated {
    pub actor: BasicActorRef,
}

#[derive(Clone, Debug)]
pub struct ActorRestarted {
    pub actor: BasicActorRef,
}

#[derive(Clone, Debug)]
pub struct ActorTerminated {
    pub actor: BasicActorRef,
}

impl From<ActorCreated> for SystemEvent {
    fn from(val: ActorCreated) -> Self {
        SystemEvent::ActorCreated(val)
    }
}

impl From<ActorRestarted> for SystemEvent {
    fn from(val: ActorRestarted) -> Self {
        SystemEvent::ActorRestarted(val)
    }
}

impl From<ActorTerminated> for SystemEvent {
    fn from(val: ActorTerminated) -> Self {
        SystemEvent::ActorTerminated(val)
    }
}

impl From<ActorCreated> for SystemMsg {
    fn from(val: ActorCreated) -> Self {
        SystemMsg::Event(SystemEvent::ActorCreated(val))
    }
}

impl From<ActorRestarted> for SystemMsg {
    fn from(val: ActorRestarted) -> Self {
        SystemMsg::Event(SystemEvent::ActorRestarted(val))
    }
}

impl From<ActorTerminated> for SystemMsg {
    fn from(val: ActorTerminated) -> Self {
        SystemMsg::Event(SystemEvent::ActorTerminated(val))
    }
}

#[derive(Clone, Debug)]
pub enum SystemEventType {
    ActorTerminated,
    ActorRestarted,
    ActorCreated,
}

#[derive(Error)]
pub enum SystemError {
    #[error("Failed to create actor system. Cause: Sub module failed to start ({0})")]
    ModuleFailed(String),
    #[error("Failed to create actor system. Cause: Sub module failed to start ({0})")]
    InvalidName(String),
    #[error("Invalid actor system configuration: {0}")]
    Configuration(#[from] config::ConfigError),
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
}

impl fmt::Debug for SystemError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.to_string())
    }
}
use std::{
    ops::Deref,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use chrono::prelude::*;
use futures::{
    Future,
    channel::oneshot,
    executor::{ThreadPool, ThreadPoolBuilder},
    future::RemoteHandle,
    task::{SpawnError, SpawnExt},
};

use uuid::Uuid;

use crate::{
    AnyMessage, Message,
    actor::{props::ActorFactory, *},
    kernel::provider::{Provider, create_root},
    load_config,
    system::logger::*,
    system::timer::*,
    validate::{InvalidPath, validate_name},
};
use slog::{Logger, debug};

// 0. error results on any
// 1. visibility

pub struct ProtoSystem {
    id: Uuid,
    name: String,
    pub host: Arc<str>,
    config: PicardConfig,
    pub(crate) sys_settings: SystemSettings,
    started_at: DateTime<Utc>,
    started_instant: Instant,
}

#[derive(Default)]
pub struct SystemBuilder {
    name: Option<String>,
    cfg: Option<PicardConfig>,
    log: Option<Logger>,
    exec: Option<ThreadPool>,
}

impl SystemBuilder {
    pub fn new() -> Self {
        SystemBuilder::default()
    }

    pub fn create(self) -> Result<ActorSystem, SystemError> {
        let name = self.name.unwrap_or_else(|| "picard".to_string());
        let cfg = match self.cfg {
            Some(cfg) => cfg,
            None => load_config()?,
        };
        let exec = match self.exec {
            Some(exec) => exec,
            None => default_exec(&cfg)?,
        };
        let log = self
            .log
            .map(|log| LoggingSystem::new(log, None))
            .unwrap_or_else(|| default_log(&cfg));

        ActorSystem::create(name.as_ref(), exec, log, cfg)
    }

    pub fn name(self, name: &str) -> Self {
        SystemBuilder {
            name: Some(name.to_string()),
            ..self
        }
    }

    pub fn cfg(self, cfg: PicardConfig) -> Self {
        SystemBuilder {
            cfg: Some(cfg),
            ..self
        }
    }

    pub fn exec(self, exec: ThreadPool) -> Self {
        SystemBuilder {
            exec: Some(exec),
            ..self
        }
    }

    pub fn log(self, log: Logger) -> Self {
        SystemBuilder {
            log: Some(log),
            ..self
        }
    }
}

/// Holds fields related to logging system.
#[derive(Clone)]
pub struct LoggingSystem {
    /// Logger
    log: Logger,
    /// Global logger guard
    _global_logger_guard: Option<GlobalLoggerGuard>,
}

impl LoggingSystem {
    pub(crate) fn new(log: Logger, global_logger_guard: Option<GlobalLoggerGuard>) -> Self {
        Self {
            log,
            _global_logger_guard: global_logger_guard,
        }
    }
}

impl Deref for LoggingSystem {
    type Target = Logger;

    fn deref(&self) -> &Self::Target {
        &self.log
    }
}

/// The actor runtime and common services coordinator
///
/// The `ActorSystem` provides a runtime on which actors are executed.
/// It also provides common services such as channels and scheduling.
/// The `ActorSystem` is the heart of a Picard application,
/// starting several threads when it is created. Create only one instance
/// of `ActorSystem` per application.
#[allow(dead_code)]
#[derive(Clone)]
pub struct ActorSystem {
    proto: Arc<ProtoSystem>,
    sys_actors: Option<SysActors>,
    log: LoggingSystem,
    debug: bool,
    pub exec: ThreadPool,
    pub timer: TimerRef,
    pub sys_channels: Option<SysChannels>,
    pub(crate) provider: Provider,
}

impl ActorSystem {
    /// Create a new `ActorSystem` instance
    ///
    /// Requires a type that implements the `Model` trait.
    pub fn new() -> Result<ActorSystem, SystemError> {
        let cfg = load_config()?;
        validate_config(&cfg)?;
        let exec = default_exec(&cfg)?;
        let log = default_log(&cfg);

        ActorSystem::create("picard", exec, log, cfg)
    }

    /// Create a new `ActorSystem` instance with provided name
    ///
    /// Requires a type that implements the `Model` trait.
    pub fn with_name(name: &str) -> Result<ActorSystem, SystemError> {
        let cfg = load_config()?;
        validate_config(&cfg)?;
        let exec = default_exec(&cfg)?;
        let log = default_log(&cfg);

        ActorSystem::create(name, exec, log, cfg)
    }

    /// Create a new `ActorSystem` instance bypassing default config behavior
    pub fn with_config(name: &str, cfg: PicardConfig) -> Result<ActorSystem, SystemError> {
        validate_config(&cfg)?;
        let exec = default_exec(&cfg)?;
        let log = default_log(&cfg);

        ActorSystem::create(name, exec, log, cfg)
    }

    fn create(
        name: &str,
        exec: ThreadPool,
        log: LoggingSystem,
        cfg: PicardConfig,
    ) -> Result<ActorSystem, SystemError> {
        validate_name(name).map_err(|_| SystemError::InvalidName(name.into()))?;
        // Process Configuration
        let debug = cfg.debug;

        // Until the logger has started, use println
        if debug {
            debug!(log, "Starting actor system: System[{}]", name);
        }

        let prov = Provider::new();
        let timer = BasicTimer::start(&cfg);

        // 1. create proto system
        let proto = ProtoSystem {
            id: Uuid::new_v4(),
            name: name.to_string(),
            host: Arc::from("localhost"),
            config: cfg.clone(),
            sys_settings: SystemSettings::from(&cfg),
            started_at: Utc::now(),
            started_instant: Instant::now(),
        };

        // 2. create uninitialized system
        let mut sys = ActorSystem {
            proto: Arc::new(proto),
            debug,
            exec,
            log,
            // event_store: None,
            timer,
            sys_channels: None,
            sys_actors: None,
            provider: prov.clone(),
        };

        // 3. create initial actor hierarchy
        let sys_actors = create_root(&sys);
        sys.sys_actors = Some(sys_actors);

        // 4. start system channels
        sys.sys_channels = Some(sys_channels(&prov, &sys)?);

        // 5. start dead letter logger
        let _dl_logger = sys_actor_of_args::<DeadLetterLogger, _>(
            &prov,
            &sys,
            "dl_logger",
            (sys.dead_letters().clone(), sys.log()),
        )?;

        sys.complete_start();

        debug!(sys.log, "Actor system [{}] [{}] started", sys.id(), name);

        Ok(sys)
    }

    fn complete_start(&self) {
        self.sys_actors.as_ref().unwrap().user.sys_init(self);
    }

    /// Returns the system start date
    pub fn start_date(&self) -> &DateTime<Utc> {
        &self.proto.started_at
    }

    /// Returns the number of seconds since the system started
    pub fn uptime(&self) -> u64 {
        self.proto.started_instant.elapsed().as_secs()
    }

    /// Returns the hostname used when the system started
    ///
    /// The host is used in actor addressing.
    ///
    /// Currently not used, but will be once system clustering is introduced.
    pub fn host(&self) -> Arc<str> {
        self.proto.host.clone()
    }

    /// Returns the UUID assigned to the system
    pub fn id(&self) -> Uuid {
        self.proto.id
    }

    /// Returns the name of the system
    pub fn name(&self) -> String {
        self.proto.name.clone()
    }

    pub fn print_tree(&self) {
        fn print_node(sys: &ActorSystem, node: &BasicActorRef, indent: &str) {
            if node.is_root() {
                println!("{}", sys.name());

                for actor in node.children() {
                    print_node(sys, &actor, "");
                }
            } else {
                println!("{}└─ {}", indent, node.name());

                for actor in node.children() {
                    print_node(sys, &actor, &(indent.to_string() + "   "));
                }
            }
        }

        let root = &self.sys_actors.as_ref().unwrap().root;
        print_node(self, root, "");
    }

    /// Returns the system root's actor reference
    #[allow(dead_code)]
    fn root(&self) -> &BasicActorRef {
        &self.sys_actors.as_ref().unwrap().root
    }

    /// Returns the user root actor reference
    pub fn user_root(&self) -> &BasicActorRef {
        &self.sys_actors.as_ref().unwrap().user
    }

    /// Returns the system root actor reference
    pub fn sys_root(&self) -> &BasicActorRef {
        &self.sys_actors.as_ref().unwrap().sysm
    }

    /// Reutrns the temp root actor reference
    pub fn temp_root(&self) -> &BasicActorRef {
        &self.sys_actors.as_ref().unwrap().temp
    }

    /// Returns a reference to the system events channel
    pub fn sys_events(&self) -> &ActorRef<ChannelMsg<SystemEvent>> {
        &self.sys_channels.as_ref().unwrap().sys_events
    }

    /// Returns a reference to the dead letters channel
    pub fn dead_letters(&self) -> &ActorRef<DLChannelMsg> {
        &self.sys_channels.as_ref().unwrap().dead_letters
    }

    pub fn publish_event(&self, evt: SystemEvent) {
        let topic = Topic::from(&evt);
        self.sys_events().tell(Publish { topic, msg: evt }, None);
    }

    /// Returns the `PicardConfig` used by the system
    pub fn config(&self) -> &PicardConfig {
        &self.proto.config
    }

    pub(crate) fn sys_settings(&self) -> &SystemSettings {
        &self.proto.sys_settings
    }

    /// Create an actor under the system root
    pub fn sys_actor_of_props<A>(
        &self,
        name: &str,
        props: BoxActorProd<A>,
    ) -> Result<ActorRef<A::Msg>, CreateError>
    where
        A: Actor,
    {
        self.provider
            .create_actor(props, name, self.sys_root(), self)
    }

    pub fn sys_actor_of<A>(&self, name: &str) -> Result<ActorRef<<A as Actor>::Msg>, CreateError>
    where
        A: ActorFactory,
    {
        self.provider
            .create_actor(Props::new::<A>(), name, self.sys_root(), self)
    }

    pub fn sys_actor_of_args<A, Args>(
        &self,
        name: &str,
        args: Args,
    ) -> Result<ActorRef<<A as Actor>::Msg>, CreateError>
    where
        Args: ActorArgs,
        A: ActorFactoryArgs<Args>,
    {
        self.provider
            .create_actor(Props::new_args::<A, _>(args), name, self.sys_root(), self)
    }

    #[inline]
    pub fn log(&self) -> LoggingSystem {
        self.log.clone()
    }

    /// Shutdown the actor system
    ///
    /// Attempts a graceful shutdown of the system and all actors.
    /// Actors will receive a stop message, executing `actor.post_stop`.
    ///
    /// Does not block. Returns a future which is completed when all
    /// actors have successfully stopped.
    pub fn shutdown(&self) -> Shutdown {
        let (tx, rx) = oneshot::channel::<()>();
        let tx = Arc::new(Mutex::new(Some(tx)));

        self.tmp_actor_of_args::<ShutdownActor, _>(tx).unwrap();

        rx
    }
}

unsafe impl Send for ActorSystem {}
unsafe impl Sync for ActorSystem {}

impl ActorRefFactory for ActorSystem {
    fn actor_of_props<A>(
        &self,
        name: &str,
        props: BoxActorProd<A>,
    ) -> Result<ActorRef<A::Msg>, CreateError>
    where
        A: Actor,
    {
        self.provider
            .create_actor(props, name, self.user_root(), self)
    }

    fn actor_of<A>(&self, name: &str) -> Result<ActorRef<<A as Actor>::Msg>, CreateError>
    where
        A: ActorFactory,
    {
        self.provider
            .create_actor(Props::new::<A>(), name, self.user_root(), self)
    }

    fn actor_of_args<A, Args>(
        &self,
        name: &str,
        args: Args,
    ) -> Result<ActorRef<<A as Actor>::Msg>, CreateError>
    where
        Args: ActorArgs,
        A: ActorFactoryArgs<Args>,
    {
        self.provider
            .create_actor(Props::new_args::<A, _>(args), name, self.user_root(), self)
    }

    fn stop(&self, actor: impl ActorReference) {
        actor.stop();
    }
}

impl ActorRefFactory for &ActorSystem {
    fn actor_of_props<A>(
        &self,
        name: &str,
        props: BoxActorProd<A>,
    ) -> Result<ActorRef<A::Msg>, CreateError>
    where
        A: Actor,
    {
        self.provider
            .create_actor(props, name, self.user_root(), self)
    }

    fn actor_of<A>(&self, name: &str) -> Result<ActorRef<<A as Actor>::Msg>, CreateError>
    where
        A: ActorFactory,
    {
        self.provider
            .create_actor(Props::new::<A>(), name, self.user_root(), self)
    }

    fn actor_of_args<A, Args>(
        &self,
        name: &str,
        args: Args,
    ) -> Result<ActorRef<<A as Actor>::Msg>, CreateError>
    where
        Args: ActorArgs,
        A: ActorFactoryArgs<Args>,
    {
        self.provider
            .create_actor(Props::new_args::<A, _>(args), name, self.user_root(), self)
    }

    fn stop(&self, actor: impl ActorReference) {
        actor.stop();
    }
}

impl TmpActorRefFactory for ActorSystem {
    fn tmp_actor_of_props<A>(&self, props: BoxActorProd<A>) -> Result<ActorRef<A::Msg>, CreateError>
    where
        A: Actor,
    {
        let name = rand::random::<u64>().to_string();
        self.provider
            .create_actor(props, &name, self.temp_root(), self)
    }

    fn tmp_actor_of<A>(&self) -> Result<ActorRef<<A as Actor>::Msg>, CreateError>
    where
        A: ActorFactory,
    {
        let name = rand::random::<u64>().to_string();
        self.provider
            .create_actor(Props::new::<A>(), &name, self.temp_root(), self)
    }

    fn tmp_actor_of_args<A, Args>(
        &self,
        args: Args,
    ) -> Result<ActorRef<<A as Actor>::Msg>, CreateError>
    where
        Args: ActorArgs,
        A: ActorFactoryArgs<Args>,
    {
        let name = rand::random::<u64>().to_string();
        self.provider
            .create_actor(Props::new_args::<A, _>(args), &name, self.temp_root(), self)
    }
}

impl ActorSelectionFactory for ActorSystem {
    fn select(&self, path: &str) -> Result<ActorSelection, InvalidPath> {
        let anchor = self.user_root();
        let (anchor, path_str) = if path.starts_with('/') {
            let anchor = self.user_root();
            let anchor_path = format!("{}/", anchor.path());
            let path = path.to_string().replace(&anchor_path, "");

            (anchor, path)
        } else {
            (anchor, path.to_string())
        };

        ActorSelection::new(
            anchor.clone(),
            // self.dead_letters(),
            path_str,
        )
    }
}

// futures::task::Spawn::spawn requires &mut self so
// we'll create a wrapper trait that requires only &self.
pub trait Run {
    fn run<Fut>(&self, future: Fut) -> Result<RemoteHandle<<Fut as Future>::Output>, SpawnError>
    where
        Fut: Future + Send + 'static,
        <Fut as Future>::Output: Send;
}

impl Run for ActorSystem {
    fn run<Fut>(&self, future: Fut) -> Result<RemoteHandle<<Fut as Future>::Output>, SpawnError>
    where
        Fut: Future + Send + 'static,
        <Fut as Future>::Output: Send,
    {
        self.exec.spawn_with_handle(future)
    }
}

impl fmt::Debug for ActorSystem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ActorSystem[Name: {}, Start Time: {}, Uptime: {} seconds]",
            self.name(),
            self.start_date(),
            self.uptime()
        )
    }
}

impl Timer for ActorSystem {
    fn schedule<T, M>(
        &self,
        initial_delay: Duration,
        interval: Duration,
        receiver: ActorRef<M>,
        sender: Sender,
        msg: T,
    ) -> ScheduleId
    where
        T: Message + Into<M>,
        M: Message + Clone,
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

        let _ = self.timer.send(Job::Repeat(job));
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
        T: Message + Into<M>,
        M: Message,
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

        let _ = self.timer.send(Job::Once(job));
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
        T: Message + Into<M>,
        M: Message,
    {
        let delay = delay_until(time, Utc::now());

        let id = Uuid::new_v4();
        let msg: M = msg.into();

        let job = OnceJob {
            id,
            send_at: Instant::now() + delay,
            receiver: receiver.into(),
            sender,
            msg: AnyMessage::new(msg),
        };

        let _ = self.timer.send(Job::Once(job));
        id
    }

    fn cancel_schedule(&self, id: Uuid) {
        let _ = self.timer.send(Job::Cancel(id));
    }
}

pub(crate) fn delay_until(time: DateTime<Utc>, now: DateTime<Utc>) -> Duration {
    time.signed_duration_since(now)
        .to_std()
        .unwrap_or(Duration::ZERO)
}

#[cfg(test)]
mod tests {
    use super::delay_until;
    use chrono::{Duration as ChronoDuration, Utc};
    use std::time::Duration;

    #[test]
    fn schedule_delay_preserves_subsecond_precision() {
        let now = Utc::now();
        assert_eq!(
            delay_until(now + ChronoDuration::milliseconds(250), now),
            Duration::from_millis(250)
        );
    }

    #[test]
    fn schedule_delay_is_zero_for_past_times() {
        let now = Utc::now();
        assert_eq!(
            delay_until(now - ChronoDuration::milliseconds(250), now),
            Duration::ZERO
        );
    }
}

// helper functions
#[allow(unused)]
fn sys_actor_of_props<A>(
    prov: &Provider,
    sys: &ActorSystem,
    name: &str,
    props: BoxActorProd<A>,
) -> Result<ActorRef<A::Msg>, SystemError>
where
    A: Actor,
{
    prov.create_actor(props, name, sys.sys_root(), sys)
        .map_err(|_| SystemError::ModuleFailed(name.into()))
}

fn sys_actor_of<A>(
    prov: &Provider,
    sys: &ActorSystem,
    name: &str,
) -> Result<ActorRef<<A as Actor>::Msg>, SystemError>
where
    A: ActorFactory,
{
    prov.create_actor(Props::new::<A>(), name, sys.sys_root(), sys)
        .map_err(|_| SystemError::ModuleFailed(name.into()))
}

#[allow(dead_code)]
fn sys_actor_of_args<A, Args>(
    prov: &Provider,
    sys: &ActorSystem,
    name: &str,
    args: Args,
) -> Result<ActorRef<<A as Actor>::Msg>, SystemError>
where
    Args: ActorArgs,
    A: ActorFactoryArgs<Args>,
{
    prov.create_actor(Props::new_args::<A, _>(args), name, sys.sys_root(), sys)
        .map_err(|_| SystemError::ModuleFailed(name.into()))
}

fn sys_channels(prov: &Provider, sys: &ActorSystem) -> Result<SysChannels, SystemError> {
    let sys_events = sys_actor_of::<EventsChannel>(prov, sys, "sys_events")?;
    let dead_letters = sys_actor_of::<Channel<DeadLetter>>(prov, sys, "dead_letters")?;

    // subscribe the dead_letters channel to actor terminated events
    // so that any future subscribed actors that terminate are automatically
    // unsubscribed from the dead_letters channel
    // let msg = ChannelMsg::Subscribe(SysTopic::ActorTerminated.into(), dl.clone());
    // es.tell(msg, None);

    Ok(SysChannels {
        sys_events,
        dead_letters,
    })
}

pub struct SystemSettings {
    pub msg_process_limit: u32,
}

impl From<&PicardConfig> for SystemSettings {
    fn from(config: &PicardConfig) -> Self {
        SystemSettings {
            msg_process_limit: config.mailbox.msg_process_limit,
        }
    }
}

struct ThreadPoolConfig {
    pool_size: usize,
    stack_size: usize,
}

impl From<&PicardConfig> for ThreadPoolConfig {
    fn from(config: &PicardConfig) -> Self {
        ThreadPoolConfig {
            pool_size: config.dispatcher.pool_size,
            stack_size: config.dispatcher.stack_size,
        }
    }
}

/// Typed system configuration. Missing settings fall back to the defaults below.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct PicardConfig {
    #[default(true)]
    pub debug: bool,
    pub log: LogConfig,
    pub mailbox: MailboxConfig,
    pub dispatcher: DispatcherConfig,
    pub scheduler: SchedulerConfig,
    pub cqrs: CqrsConfig,
    /// Free-form application settings (the `[app]` section)
    pub app: HashMap<String, config::Value>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct LogConfig {
    #[default("debug".to_string())]
    pub level: String,
    /// Reserved: the default logger does not currently apply this format.
    #[default("{date} {time} {level} [{module}] {body}".to_string())]
    pub log_format: String,
    #[default("%Y-%m-%d".to_string())]
    pub date_format: String,
    #[default("%H:%M:%S%:z".to_string())]
    pub time_format: String,
    pub filter: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct MailboxConfig {
    #[default(1000)]
    pub msg_process_limit: u32,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct DispatcherConfig {
    #[default(std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get) * 2)]
    pub pool_size: usize,
    pub stack_size: usize,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct SchedulerConfig {
    #[default(50)]
    pub frequency_millis: u64,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct CqrsConfig {
    #[default(120)]
    pub sleep_after_secs: u64,
}

fn validate_config(cfg: &PicardConfig) -> Result<(), SystemError> {
    let invalid = |msg: &str| Err(SystemError::Configuration(ConfigError::Message(msg.into())));
    if cfg.mailbox.msg_process_limit == 0 {
        return invalid("`mailbox.msg_process_limit` must be between 1 and u32::MAX");
    }
    if cfg.dispatcher.pool_size == 0 {
        return invalid("`dispatcher.pool_size` must be a positive usize");
    }
    if cfg.scheduler.frequency_millis == 0 {
        return invalid("`scheduler.frequency_millis` must be a positive u64");
    }
    Ok(())
}

fn default_exec(cfg: &PicardConfig) -> Result<ThreadPool, SystemError> {
    let exec_cfg = ThreadPoolConfig::from(cfg);
    ThreadPoolBuilder::new()
        .pool_size(exec_cfg.pool_size)
        .stack_size(exec_cfg.stack_size)
        .name_prefix("pool-thread-#")
        .create()
        .map_err(|err| err.into())
}

#[derive(Clone)]
pub struct SysActors {
    pub root: BasicActorRef,
    pub user: BasicActorRef,
    pub sysm: BasicActorRef,
    pub temp: BasicActorRef,
}

#[derive(Clone)]
pub struct SysChannels {
    pub sys_events: ActorRef<ChannelMsg<SystemEvent>>,
    pub dead_letters: ActorRef<DLChannelMsg>,
}

pub type Shutdown = oneshot::Receiver<()>;

#[derive(Clone)]
struct ShutdownActor {
    tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

impl ActorFactoryArgs<Arc<Mutex<Option<oneshot::Sender<()>>>>> for ShutdownActor {
    fn create_args(tx: Arc<Mutex<Option<oneshot::Sender<()>>>>) -> Self {
        ShutdownActor::new(tx)
    }
}

impl ShutdownActor {
    fn new(tx: Arc<Mutex<Option<oneshot::Sender<()>>>>) -> Self {
        ShutdownActor { tx }
    }
}

impl Actor for ShutdownActor {
    type Msg = SystemEvent;

    fn pre_start(&mut self, ctx: &Context<Self::Msg>) {
        let sub = Subscribe {
            topic: SysTopic::ActorTerminated.into(),
            actor: Box::new(ctx.myself.clone()),
        };
        ctx.system.sys_events().tell(sub, None);

        // todo this is prone to failing since there is no
        // confirmation that ShutdownActor has subscribed to
        // the ActorTerminated events yet.
        // It may be that the user root actor is Sterminated
        // before the subscription is complete.

        // std::thread::sleep_ms(1000);
        // send stop to all /user children
        ctx.system.stop(ctx.system.user_root());
    }

    fn sys_recv(
        &mut self,
        ctx: &Context<Self::Msg>,
        msg: SystemMsg,
        sender: Option<BasicActorRef>,
    ) {
        if let SystemMsg::Event(evt) = msg
            && let SystemEvent::ActorTerminated(terminated) = evt
        {
            self.receive(ctx, terminated, sender);
        }
    }

    fn recv(&mut self, _: &Context<Self::Msg>, _: Self::Msg, _: Option<BasicActorRef>) {}
}

impl Receive<ActorTerminated> for ShutdownActor {
    type Msg = SystemEvent;

    fn receive(
        &mut self,
        ctx: &Context<Self::Msg>,
        msg: ActorTerminated,
        _sender: Option<BasicActorRef>,
    ) {
        if &msg.actor == ctx.system.user_root()
            && let Ok(ref mut tx) = self.tx.lock()
            && let Some(tx) = tx.take()
        {
            tx.send(()).unwrap();
        }
    }
}
