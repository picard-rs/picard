//! Messages exchanged between the simulation actors.

use std::{collections::HashMap, fmt, sync::Arc};

use picard::platform::Instant;

use picard::actors::{ActorRef, Context, Message, SystemEvent};

use crate::city::{Axis, Node};

pub type VehicleId = u64;

/// Simulation clock pulse. `sent` lets receivers measure how long the message waited.
#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Tick {
    pub dt: f32,
    #[cfg_attr(feature = "serde", serde(skip, default = "Instant::now"))]
    pub sent: Instant,
    /// True on ticks where actors should send a state report towards the browser.
    pub report: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Fault {
    /// The actor stops itself: nothing restarts it.
    Terminate,
    /// The actor panics: its parent's supervision strategy decides what happens.
    Crash,
}

/// Makes the calling actor fail so that its parent's supervision strategy takes over.
///
/// Natively the actor panics. `wasm32-unknown-unknown` aborts on panic, so there the actor
/// reports its own failure to the parent instead, which restarts it the same way.
///
/// # Panics
///
/// Always panics on native targets; that is how the failure is injected.
#[cfg(not(target_arch = "wasm32"))]
pub fn crash<M: Message>(_: &Context<M>, name: &str) {
    panic!("fault injected into {name}");
}

#[cfg(target_arch = "wasm32")]
pub fn crash<M: Message>(ctx: &Context<M>, _: &str) {
    ctx.fail();
}

/// What every inspectable actor needs to answer an inspector request.
#[derive(Clone, Debug)]
pub struct InspectReq {
    pub reply: ActorRef<GatewayMsg>,
}

/// Immutable address book of the junctions that currently exist.
///
/// The supervisor builds a fresh snapshot whenever a district appears or disappears and hands it
/// to the districts, which forward it to their vehicles. Nobody mutates a snapshot.
#[derive(Default)]
pub struct Directory {
    pub version: u64,
    pub junctions: HashMap<Node, ActorRef<JunctionMsg>>,
}

impl fmt::Debug for Directory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Directory(v{}, {} junctions)",
            self.version,
            self.junctions.len()
        )
    }
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum VehicleMsg {
    Tick(Tick),
    EntryGranted {
        node: Node,
    },
    EntryDenied {
        node: Node,
        queue_pos: u32,
    },
    TrafficSignalChanged {
        node: Node,
        green: Option<Axis>,
    },
    #[cfg_attr(feature = "serde", serde(skip))]
    Directory(Arc<Directory>),
    Fault(Fault),
    #[cfg_attr(feature = "serde", serde(skip))]
    Inspect(InspectReq),
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum JunctionMsg {
    Tick(Tick),
    #[cfg_attr(feature = "serde", serde(skip))]
    RequestIntersectionEntry {
        vehicle: ActorRef<VehicleMsg>,
        id: VehicleId,
        axis: Axis,
    },
    VehicleExitedIntersection {
        id: VehicleId,
    },
    TrafficSignalChanged(Signal),
    Fault(Fault),
    #[cfg_attr(feature = "serde", serde(skip))]
    Inspect(InspectReq),
    #[cfg_attr(feature = "serde", serde(skip))]
    InspectLight(InspectReq),
    FaultLight(Fault),
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum LightMsg {
    Tick(Tick),
    Demand {
        ns: u32,
        ew: u32,
    },
    Fault(Fault),
    #[cfg_attr(feature = "serde", serde(skip))]
    Inspect(InspectReq),
}

/// Which axis (if any) may currently enter the junction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Signal {
    pub green: Option<Axis>,
    pub yellow: bool,
}

impl Signal {
    pub const ALL_RED: Self = Self {
        green: None,
        yellow: false,
    };

    #[must_use]
    pub fn permits(self, axis: Axis) -> bool {
        self.green == Some(axis) && !self.yellow
    }
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum DistrictMsg {
    Tick(Tick),
    #[cfg_attr(feature = "serde", serde(skip))]
    Directory(Arc<Directory>),
    #[cfg_attr(feature = "serde", serde(skip))]
    NeedDirectory(ActorRef<VehicleMsg>),
    SetTarget(usize),
    Surge(usize),
    VehicleReport(VehicleView),
    JunctionReport(JunctionView),
    SpawnBatch,
    Fault {
        id: String,
        fault: Fault,
    },
    #[cfg_attr(feature = "serde", serde(skip))]
    Inspect {
        id: String,
        req: InspectReq,
    },
    /// Only needed so the district can subscribe to system events; they arrive via `sys_recv`.
    #[cfg_attr(feature = "serde", serde(skip))]
    SystemEvent(SystemEvent),
}

impl From<SystemEvent> for DistrictMsg {
    fn from(event: SystemEvent) -> Self {
        Self::SystemEvent(event)
    }
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SupervisorMsg {
    Tick,
    Pause(bool),
    Reset,
    SetVehicles(usize),
    Surge(usize),
    AddDistrict(usize),
    RemoveDistrict(usize),
    #[cfg_attr(feature = "serde", serde(skip))]
    DistrictReady {
        district: usize,
        junctions: Vec<(Node, ActorRef<JunctionMsg>)>,
    },
    Fault {
        id: String,
        fault: Fault,
    },
    Inspect {
        id: String,
    },
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum GatewayMsg {
    Flush,
    Snapshot(Arc<DistrictSnapshot>),
    DistrictStarted {
        district: usize,
        generation: u64,
    },
    DistrictGone(usize),
    InspectReply {
        id: String,
        json: String,
    },
    Status {
        paused: bool,
        target: usize,
    },
    #[cfg(feature = "remoting")]
    Heartbeat,
    #[cfg(feature = "remoting")]
    RemoteUpdate(RemoteUpdate),
    #[cfg(feature = "remoting")]
    #[cfg_attr(feature = "serde", serde(skip))]
    SetRemotePeers(Vec<crate::remoting::PeerLink>),
}

#[cfg(feature = "remoting")]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub enum RemoteUpdate {
    Snapshot(Arc<DistrictSnapshot>),
    DistrictStarted {
        district: usize,
        generation: u64,
    },
    DistrictGone(usize),
    InspectReply {
        id: String,
        json: String,
    },
    Status {
        district: usize,
        paused: bool,
        target: usize,
    },
    Metrics {
        district: usize,
        sample: crate::sampler::Sample,
    },
}

/// Vehicle driving states as shown in the UI.
pub mod state {
    pub const MOVING: u8 = 0;
    pub const WAITING: u8 = 1;
    pub const CROSSING: u8 = 2;
    pub const PARKED: u8 = 3;
}

#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct VehicleView {
    pub id: VehicleId,
    pub from: Node,
    pub to: Node,
    pub s: f32,
    pub state: u8,
}

#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct JunctionView {
    pub node: Node,
    pub signal: Signal,
    pub queued: [u32; 2],
    pub inside: u32,
    pub degraded: bool,
}

#[derive(Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DistrictSnapshot {
    pub district: usize,
    pub generation: u64,
    pub vehicles: Vec<VehicleView>,
    pub junctions: Vec<JunctionView>,
    pub target: usize,
    /// Messages the district handled since its previous snapshot.
    pub handled: u64,
}
