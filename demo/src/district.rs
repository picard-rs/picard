//! District actor: parent of the junctions and vehicles of one part of the city.
//!
//! It fans the clock out to its children, owns their address book, and aggregates their reports
//! into one snapshot per frame so that individual vehicles never talk to the browser.

use std::{collections::HashMap, sync::Arc};

use picard::platform::Instant;

use picard::actors::{
    Actor, ActorFactoryArgs, ActorRef, ActorRefFactory, ActorReference, Context, Sender, Subscribe,
    SysTopic, SystemEvent, SystemMsg, Tell,
};
use rand::Rng;

use crate::{
    city::{Node, district_center, district_nodes},
    junction::{Junction, JunctionArgs},
    metrics::Metrics,
    msg::{
        Directory, DistrictMsg, DistrictSnapshot, Fault, GatewayMsg, JunctionMsg, JunctionView,
        SupervisorMsg, Tick, VehicleId, VehicleMsg, VehicleView,
    },
    vehicle::{Vehicle, VehicleArgs, VehicleSeed},
};

const SPAWN_BATCH: usize = 250;

#[derive(Clone)]
pub struct DistrictArgs {
    pub index: usize,
    /// Distinguishes vehicle ids across district generations.
    pub id_base: u64,
    pub supervisor: ActorRef<SupervisorMsg>,
    pub gateway: ActorRef<GatewayMsg>,
    pub metrics: Arc<Metrics>,
}

pub struct District {
    args: DistrictArgs,
    dir: Arc<Directory>,
    junctions: HashMap<Node, ActorRef<JunctionMsg>>,
    vehicles: HashMap<VehicleId, ActorRef<VehicleMsg>>,
    vehicle_views: HashMap<VehicleId, VehicleView>,
    junction_views: HashMap<Node, JunctionView>,
    target: usize,
    to_spawn: usize,
    surge_left: usize,
    counter: u64,
    handled: u64,
}

impl ActorFactoryArgs<DistrictArgs> for District {
    fn create_args(args: DistrictArgs) -> Self {
        Self {
            args,
            dir: Arc::default(),
            junctions: HashMap::new(),
            vehicles: HashMap::new(),
            vehicle_views: HashMap::new(),
            junction_views: HashMap::new(),
            target: 0,
            to_spawn: 0,
            surge_left: 0,
            counter: 0,
            handled: 0,
        }
    }
}

impl District {
    fn spawn_vehicles(&mut self, ctx: &Context<DistrictMsg>) {
        let batch = self.to_spawn.min(SPAWN_BATCH);
        let nodes: Vec<Node> = district_nodes(self.args.index).collect();
        let hotspot = district_center(self.args.index);
        let mut rng = rand::rng();
        for _ in 0..batch {
            let id = self.args.id_base + self.counter;
            self.counter += 1;
            let surge = self.surge_left > 0;
            self.surge_left = self.surge_left.saturating_sub(1);
            let seed = VehicleSeed {
                id,
                spawn: nodes[rng.random_range(0..nodes.len())],
                hotspot: surge.then_some(hotspot),
                speed: rng.random_range(1.4..2.4),
            };
            let args = VehicleArgs {
                seed,
                district: ctx.myself(),
                metrics: self.args.metrics.clone(),
            };
            if let Ok(vehicle) = ctx.actor_of_args::<Vehicle, _>(&format!("v-{id}"), args) {
                self.args.metrics.inc(&self.args.metrics.created);
                self.vehicles.insert(id, vehicle);
            }
        }
        self.to_spawn -= batch;
        if self.to_spawn > 0 {
            // Yield to the mailbox between batches so ticks and inspector requests keep flowing.
            ctx.myself().tell(DistrictMsg::SpawnBatch, None);
        }
    }

    fn set_target(&mut self, ctx: &Context<DistrictMsg>, target: usize) {
        self.target = target;
        let current = self.vehicles.len();
        self.surge_left = 0;
        if target >= current {
            self.to_spawn = target - current;
            if self.to_spawn > 0 {
                ctx.myself().tell(DistrictMsg::SpawnBatch, None);
            }
        } else {
            self.to_spawn = 0;
            let doomed: Vec<VehicleId> = self
                .vehicles
                .keys()
                .copied()
                .take(current - target)
                .collect();
            for id in doomed {
                if let Some(vehicle) = self.vehicles.remove(&id) {
                    self.vehicle_views.remove(&id);
                    ctx.stop(vehicle);
                }
            }
        }
    }

    fn publish(&mut self) {
        let snapshot = DistrictSnapshot {
            district: self.args.index,
            vehicles: self.vehicle_views.values().copied().collect(),
            junctions: self.junction_views.values().copied().collect(),
            target: self.target,
            handled: std::mem::take(&mut self.handled),
        };
        self.args
            .gateway
            .tell(GatewayMsg::Snapshot(Arc::new(snapshot)), None);
        self.args.metrics.inc(&self.args.metrics.sent);
    }

    fn tick(&mut self, tick: Tick) {
        if tick.report {
            self.publish();
        }
        let tick = Tick {
            sent: Instant::now(),
            ..tick
        };
        for junction in self.junctions.values() {
            junction.tell(JunctionMsg::Tick(tick), None);
        }
        for vehicle in self.vehicles.values() {
            vehicle.tell(VehicleMsg::Tick(tick), None);
        }
        let fanout = self.junctions.len() + self.vehicles.len();
        self.args
            .metrics
            .add(&self.args.metrics.sent, fanout as u64);
    }

    fn route_fault(&self, id: &str, fault: Fault) {
        if let Some(raw) = id.strip_prefix("v-") {
            if let Some(v) = raw
                .parse::<VehicleId>()
                .ok()
                .and_then(|id| self.vehicles.get(&id))
            {
                v.tell(VehicleMsg::Fault(fault), None);
            }
        } else if let Some((kind, node)) = Node::parse(id)
            && let Some(j) = self.junctions.get(&node)
        {
            let msg = if kind == 'l' {
                JunctionMsg::FaultLight(fault)
            } else {
                JunctionMsg::Fault(fault)
            };
            j.tell(msg, None);
        }
    }

    fn route_inspect(&self, id: &str, req: crate::msg::InspectReq) {
        if let Some(raw) = id.strip_prefix("v-") {
            if let Some(v) = raw
                .parse::<VehicleId>()
                .ok()
                .and_then(|id| self.vehicles.get(&id))
            {
                v.tell(VehicleMsg::Inspect(req), None);
            }
        } else if let Some((kind, node)) = Node::parse(id)
            && let Some(j) = self.junctions.get(&node)
        {
            let msg = if kind == 'l' {
                JunctionMsg::InspectLight(req)
            } else {
                JunctionMsg::Inspect(req)
            };
            j.tell(msg, None);
        }
    }
}

impl Actor for District {
    type Msg = DistrictMsg;

    fn pre_start(&mut self, ctx: &Context<Self::Msg>) {
        for node in district_nodes(self.args.index) {
            let args = JunctionArgs {
                node,
                district: ctx.myself(),
                metrics: self.args.metrics.clone(),
            };
            if let Ok(j) = ctx.actor_of_args::<Junction, _>(&node.junction_name(), args) {
                self.args.metrics.inc(&self.args.metrics.created);
                self.junctions.insert(node, j);
            }
        }
        ctx.system.sys_events().tell(
            Subscribe {
                topic: SysTopic::ActorTerminated.into(),
                actor: Box::new(ctx.myself()),
            },
            None,
        );
        let junctions = self
            .junctions
            .iter()
            .map(|(n, j)| (*n, j.clone()))
            .collect();
        self.args.supervisor.tell(
            SupervisorMsg::DistrictReady {
                district: self.args.index,
                junctions,
            },
            None,
        );
    }

    fn post_stop(&mut self) {
        // Sent from the district itself so it cannot be overtaken by a late snapshot.
        self.args
            .gateway
            .tell(GatewayMsg::DistrictGone(self.args.index), None);
    }

    fn sys_recv(&mut self, _: &Context<Self::Msg>, msg: SystemMsg, _: Sender) {
        let SystemMsg::Event(SystemEvent::ActorTerminated(t)) = msg else {
            return;
        };
        let name = t.actor.name();
        if let Some(id) = name
            .strip_prefix("v-")
            .and_then(|n| n.parse::<VehicleId>().ok())
        {
            self.vehicles.remove(&id);
            self.vehicle_views.remove(&id);
        } else if let Some(('i', node)) = Node::parse(name)
            && self
                .junctions
                .get(&node)
                .is_some_and(|j| j.path() == t.actor.path())
        {
            self.junctions.remove(&node);
            self.junction_views.remove(&node);
        }
    }

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, _: Sender) {
        self.args.metrics.inc(&self.args.metrics.processed);
        self.handled += 1;
        match msg {
            DistrictMsg::Tick(t) => self.tick(t),
            DistrictMsg::Directory(dir) => {
                for vehicle in self.vehicles.values() {
                    vehicle.tell(VehicleMsg::Directory(dir.clone()), None);
                }
                self.args
                    .metrics
                    .add(&self.args.metrics.sent, self.vehicles.len() as u64);
                self.dir = dir;
            }
            DistrictMsg::NeedDirectory(vehicle) => {
                if self.dir.version > 0 {
                    vehicle.tell(VehicleMsg::Directory(self.dir.clone()), None);
                    self.args.metrics.inc(&self.args.metrics.sent);
                }
            }
            DistrictMsg::SetTarget(n) => self.set_target(ctx, n),
            DistrictMsg::Surge(n) => {
                self.target += n;
                self.to_spawn += n;
                self.surge_left += n;
                ctx.myself().tell(DistrictMsg::SpawnBatch, None);
            }
            DistrictMsg::SpawnBatch => self.spawn_vehicles(ctx),
            DistrictMsg::VehicleReport(view) => {
                if self.vehicles.contains_key(&view.id) {
                    self.vehicle_views.insert(view.id, view);
                }
            }
            DistrictMsg::JunctionReport(view) => {
                self.junction_views.insert(view.node, view);
            }
            DistrictMsg::Fault { id, fault } => self.route_fault(&id, fault),
            DistrictMsg::Inspect { id, req } => self.route_inspect(&id, req),
            DistrictMsg::SystemEvent(_) => {}
        }
    }
}
