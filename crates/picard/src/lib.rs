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

use config::Config;

use crate::actor::BasicActorRef;
use crate::system::{PicardConfig, SystemError};

pub fn load_config() -> Result<PicardConfig, SystemError> {
    // picard.toml contains settings for the actor framework and its modules
    let picard_path = env::var("PICARD_CONF").unwrap_or_else(|_| "config/picard".into());
    // app.toml or app.yaml contains settings specific to the user application
    let app_path = env::var("APP_CONF").unwrap_or_else(|_| "config/app".into());

    Ok(Config::builder()
        .add_source(config::File::with_name(&picard_path).required(false))
        .add_source(config::File::with_name(&app_path).required(false))
        .build()?
        .try_deserialize()?)
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
        ActorSystem, DispatcherConfig, LogConfig, PicardConfig, Run, ScheduleId, SchedulerConfig,
        SystemBuilder, SystemEvent, SystemMsg, Timer,
    };
    pub use crate::{AnyMessage, Message};
}
