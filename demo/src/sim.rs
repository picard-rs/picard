//! Assembles the actor system: observer, gateway and supervisor.

use std::sync::Arc;

use picard::actors::{ActorRef, ActorRefFactory, ActorSystem, PicardConfig, Tell};
use tokio::sync::watch;

use crate::{
    gateway::{Frame, Gateway, GatewayArgs},
    metrics::Metrics,
    msg::{GatewayMsg, SupervisorMsg},
    observer::Observer,
    supervisor::{Supervisor, SupervisorArgs},
};

pub struct Sim {
    pub system: ActorSystem,
    pub supervisor: ActorRef<SupervisorMsg>,
    pub gateway: ActorRef<GatewayMsg>,
    pub metrics: Arc<Metrics>,
    pub frames: watch::Receiver<Arc<Frame>>,
}

impl Sim {
    /// Starts the actor system with the default districts and `vehicles` vehicles.
    ///
    /// # Errors
    ///
    /// Returns an error message if the actor system or one of the top-level
    /// actors cannot be created.
    pub fn start(vehicles: usize) -> Result<Self, String> {
        let mut config = PicardConfig::default();
        config.log.level = "warn".to_string();
        let system = ActorSystem::with_config("living-city", config).map_err(|e| e.to_string())?;
        let metrics = Arc::new(Metrics::default());

        // The observer starts first so the counters include every simulation actor.
        system
            .actor_of_args::<Observer, _>("observer", metrics.clone())
            .map_err(|e| e.to_string())?;

        let (tx, frames) = watch::channel(Arc::new(Frame::default()));
        let gateway = system
            .actor_of_args::<Gateway, _>(
                "gateway",
                GatewayArgs {
                    tx,
                    metrics: metrics.clone(),
                },
            )
            .map_err(|e| e.to_string())?;
        let supervisor = system
            .actor_of_args::<Supervisor, _>(
                "supervisor",
                SupervisorArgs {
                    gateway: gateway.clone(),
                    metrics: metrics.clone(),
                    initial_vehicles: vehicles,
                },
            )
            .map_err(|e| e.to_string())?;
        metrics.add(&metrics.created, 3);
        Ok(Self {
            system,
            supervisor,
            gateway,
            metrics,
            frames,
        })
    }

    pub fn tell(&self, msg: SupervisorMsg) {
        self.supervisor.tell(msg, None);
    }
}
