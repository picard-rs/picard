//! Messages exchanged between the simulation actors.

use std::{collections::HashMap, fmt, sync::Arc};

use picard::platform::Instant;

use picard::actors::{ActorRef, Context, Message, SystemEvent};

use crate::city::{Axis, Node};

pub type VehicleId = u64;

/// Simulation clock pulse. `sent` lets receivers measure how long the message waited.
#[derive(Clone, Copy, Debug)]
pub struct Tick {
    pub dt: f32,
    pub sent: Instant,
    /// True on ticks where actors should send a state report towards the browser.
    pub report: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
pub enum VehicleMsg {
    Tick(Tick),
    EntryGranted { node: Node },
    EntryDenied { node: Node, queue_pos: u32 },
    TrafficSignalChanged { node: Node, green: Option<Axis> },
    Directory(Arc<Directory>),
    Fault(Fault),
    Inspect(InspectReq),
}

#[derive(Clone, Debug)]
pub enum JunctionMsg {
    Tick(Tick),
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
    Inspect(InspectReq),
    InspectLight(InspectReq),
    FaultLight(Fault),
}

#[derive(Clone, Debug)]
pub enum LightMsg {
    Tick(Tick),
    Demand { ns: u32, ew: u32 },
    Fault(Fault),
    Inspect(InspectReq),
}

/// Which axis (if any) may currently enter the junction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Signal {
    pub green: Option<Axis>,
    pub yellow: bool,
}

impl Signal {
    pub const ALL_RED: Self = Self {
        green: None,
        yellow: false,
    };

    pub fn permits(self, axis: Axis) -> bool {
        self.green == Some(axis) && !self.yellow
    }
}

#[derive(Clone, Debug)]
pub enum DistrictMsg {
    Tick(Tick),
    Directory(Arc<Directory>),
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
    Inspect {
        id: String,
        req: InspectReq,
    },
    /// Only needed so the district can subscribe to system events; they arrive via `sys_recv`.
    SystemEvent(SystemEvent),
}

impl From<SystemEvent> for DistrictMsg {
    fn from(event: SystemEvent) -> Self {
        Self::SystemEvent(event)
    }
}

#[derive(Clone, Debug)]
pub enum SupervisorMsg {
    Tick,
    Pause(bool),
    Reset,
    SetVehicles(usize),
    Surge(usize),
    AddDistrict(usize),
    RemoveDistrict(usize),
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
pub enum GatewayMsg {
    Flush,
    Snapshot(Arc<DistrictSnapshot>),
    DistrictGone(usize),
    InspectReply { id: String, json: String },
    Status { paused: bool, target: usize },
}

/// Vehicle driving states as shown in the UI.
pub mod state {
    pub const MOVING: u8 = 0;
    pub const WAITING: u8 = 1;
    pub const CROSSING: u8 = 2;
    pub const PARKED: u8 = 3;
}

#[derive(Clone, Copy, Debug)]
pub struct VehicleView {
    pub id: VehicleId,
    pub from: Node,
    pub to: Node,
    pub s: f32,
    pub state: u8,
}

#[derive(Clone, Copy, Debug)]
pub struct JunctionView {
    pub node: Node,
    pub signal: Signal,
    pub queued: [u32; 2],
    pub inside: u32,
    pub degraded: bool,
}

#[derive(Debug, Default)]
pub struct DistrictSnapshot {
    pub district: usize,
    pub vehicles: Vec<VehicleView>,
    pub junctions: Vec<JunctionView>,
    pub target: usize,
    /// Messages the district handled since its previous snapshot.
    pub handled: u64,
}
