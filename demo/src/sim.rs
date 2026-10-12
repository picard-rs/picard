//! Assembles the actor system: observer, gateway and supervisor.

use std::sync::Arc;

use picard::actors::{ActorRef, ActorRefFactory, ActorSystem, PicardConfig, Tell};
use tokio::sync::watch;

#[cfg(feature = "remoting")]
use crate::remoting::{PeerLink, PeerSpec, SUPERVISOR_PATH};
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
    #[cfg(feature = "remoting")]
    remote_peers: Vec<PeerLink>,
    #[cfg(feature = "remoting")]
    #[allow(dead_code)] // keeps the listener alive
    remote_listener: Option<Box<dyn picard::remoting::RemoteListener>>,
}

impl Sim {
    /// Starts the actor system with the default districts and `vehicles` vehicles.
    ///
    /// # Errors
    ///
    /// Returns an error message if the actor system or one of the top-level
    /// actors cannot be created.
    pub fn start(vehicles: usize) -> Result<Self, String> {
        Self::start_inner(vehicles, None, None)
    }

    fn start_inner(
        vehicles: usize,
        district: Option<usize>,
        district_count: Option<usize>,
    ) -> Result<Self, String> {
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
                    #[cfg(feature = "remoting")]
                    district,
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
                    district,
                    district_count,
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
            #[cfg(feature = "remoting")]
            remote_peers: Vec::new(),
            #[cfg(feature = "remoting")]
            remote_listener: None,
        })
    }

    #[cfg(feature = "remoting")]
    pub fn start_remote(
        vehicles: usize,
        district: usize,
        listen: std::net::SocketAddr,
        token: String,
        peers: &[PeerSpec],
    ) -> Result<Self, String> {
        let mut sim = Self::start_inner(vehicles, Some(district), Some(peers.len() + 1))?;
        let (listener, links) =
            crate::remoting::start(&sim.system, district, listen, token, peers)?;
        sim.gateway
            .tell(crate::msg::GatewayMsg::SetRemotePeers(links.clone()), None);
        sim.remote_peers = links;
        sim.remote_listener = Some(listener);
        Ok(sim)
    }

    pub fn tell(&self, msg: SupervisorMsg) {
        self.supervisor.tell(msg.clone(), None);
        #[cfg(feature = "remoting")]
        for peer in &self.remote_peers {
            if let Err(err) = peer.send(SUPERVISOR_PATH, &msg) {
                eprintln!("cannot queue command for district {}: {err}", peer.district);
            }
        }
    }
}
