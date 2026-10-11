#![crate_name = "picard"]
#![deny(clippy::all)]
// #![deny(clippy::pedantic)]
// #![deny(clippy::nursery)]
#![allow(clippy::new_ret_no_self)]
#![allow(clippy::large_enum_variant)]

mod validate;

pub mod actor;
pub mod kernel;
pub mod system;

use std::any::Any;
use std::env;
use std::fmt;
use std::fmt::Debug;

use config::{Config, File};

use crate::actor::BasicActorRef;
use crate::system::SystemError;

pub fn load_config() -> Result<Config, SystemError> {
    let mut cfg = Config::new();
    set_config_defaults(&mut cfg).map_err(SystemError::Configuration)?;

    // load the system config
    // picard.toml contains settings for anything related to the actor framework and its modules
    let path = env::var("PICARD_CONF").unwrap_or_else(|_| "config/picard.toml".into());
    cfg.merge(File::with_name(&path).required(false))
        .map_err(|err| SystemError::Configuration(err.to_string()))?;

    // load the user application config
    // app.toml or app.yaml contains settings specific to the user application
    let path = env::var("APP_CONF").unwrap_or_else(|_| "config/app".into());
    cfg.merge(File::with_name(&path).required(false))
        .map_err(|err| SystemError::Configuration(err.to_string()))?;
    Ok(cfg)
}

pub(crate) fn set_config_defaults(cfg: &mut Config) -> Result<(), String> {
    cfg.set_default("debug", true)
        .map_err(|err| err.to_string())?;
    cfg.set_default("log.level", "debug")
        .map_err(|err| err.to_string())?;
    cfg.set_default("log.log_format", "{date} {time} {level} [{module}] {body}")
        .map_err(|err| err.to_string())?;
    cfg.set_default("log.date_format", "%Y-%m-%d")
        .map_err(|err| err.to_string())?;
    cfg.set_default("log.time_format", "%H:%M:%S%:z")
        .map_err(|err| err.to_string())?;
    cfg.set_default("mailbox.msg_process_limit", 1000)
        .map_err(|err| err.to_string())?;
    cfg.set_default("dispatcher.pool_size", (num_cpus::get() * 2) as i64)
        .map_err(|err| err.to_string())?;
    cfg.set_default("dispatcher.stack_size", 0)
        .map_err(|err| err.to_string())?;
    cfg.set_default("scheduler.frequency_millis", 50)
        .map_err(|err| err.to_string())?;
    Ok(())
}

/// Wraps message and sender
#[derive(Debug, Clone)]
pub struct Envelope<T: Message> {
    pub sender: Option<BasicActorRef>,
    pub msg: T,
}

/// A value that can be sent to an actor.
///
/// Messages need not be cloneable. APIs that fan out or repeat a message
/// require `Clone` locally.
pub trait Message: Debug + Send + 'static {}
impl<T: Debug + Send + 'static> Message for T {}

type CloneMessage = fn(&(dyn Any + Send)) -> Box<dyn Any + Send>;

pub struct AnyMessage {
    msg: Option<Box<dyn Any + Send>>,
    clone_message: Option<CloneMessage>,
}

pub struct DowncastAnyMessageError;

impl AnyMessage {
    pub fn new<T>(msg: T) -> Self
    where
        T: Any + Message,
    {
        Self {
            msg: Some(Box::new(msg)),
            clone_message: None,
        }
    }

    pub fn new_repeatable<T>(msg: T) -> Self
    where
        T: Any + Message + Clone,
    {
        fn clone_message<T: Any + Send + Clone>(msg: &(dyn Any + Send)) -> Box<dyn Any + Send> {
            Box::new(
                msg.downcast_ref::<T>()
                    .expect("stored message type must match its clone function")
                    .clone(),
            )
        }

        Self {
            msg: Some(Box::new(msg)),
            clone_message: Some(clone_message::<T>),
        }
    }

    pub fn take<T>(&mut self) -> Result<T, DowncastAnyMessageError>
    where
        T: Any + Message,
    {
        let Some(stored) = self.msg.as_ref() else {
            return Err(DowncastAnyMessageError);
        };
        if !stored.is::<T>() {
            return Err(DowncastAnyMessageError);
        }

        let msg = if let Some(clone_message) = self.clone_message {
            clone_message(stored.as_ref())
        } else {
            self.msg.take().ok_or(DowncastAnyMessageError)?
        };
        msg.downcast::<T>()
            .map(|msg| *msg)
            .map_err(|_| DowncastAnyMessageError)
    }
}

impl Debug for AnyMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("AnyMessage")
    }
}

pub mod actors {
    pub use crate::actor::*;
    pub use crate::system::{
        ActorSystem, Run, ScheduleId, SystemBuilder, SystemEvent, SystemMsg, Timer,
    };
    pub use crate::{AnyMessage, Message};
}
