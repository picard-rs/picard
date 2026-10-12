//! Vehicle actor: owns its position, route and driving state and negotiates every junction
//! crossing through messages.

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use picard::actors::{
    Actor, ActorFactoryArgs, ActorRef, ActorRefFactory, ActorReference, Context, Sender, Tell,
};
use picard::platform::Instant;
use rand::Rng;
use serde_json::json;

use crate::{
    city::{GRID_NODES, Node, plan_route},
    metrics::Metrics,
    msg::{
        Directory, DistrictMsg, Fault, GatewayMsg, InspectReq, JunctionMsg, Tick, VehicleId,
        VehicleMsg, VehicleTransfer, VehicleView, crash, state,
    },
    trace::Trace,
};

const CROSS_TIME: f32 = 0.6;
const RETRY_AFTER: f32 = 1.5;
const MAX_UNANSWERED: u32 = 2;
const BLOCK_FOR: f32 = 30.0;

/// Actor name of a vehicle: `v-<id>` at first, `v-<id>h<hop>` after hand-overs.
#[must_use]
pub fn actor_name(id: VehicleId, hop: u32) -> String {
    if hop == 0 {
        format!("v-{id}")
    } else {
        format!("v-{id}h{hop}")
    }
}

#[must_use]
pub fn parse_actor_name(name: &str) -> Option<VehicleId> {
    let rest = name.strip_prefix("v-")?;
    rest.split_once('h').map_or(rest, |(id, _)| id).parse().ok()
}

#[derive(Clone, Copy, Debug)]
pub struct VehicleSeed {
    pub id: VehicleId,
    pub spawn: Node,
    /// First destination for surge traffic, which all heads for the same place.
    pub hotspot: Option<Node>,
    pub speed: f32,
}

#[derive(Clone)]
pub struct VehicleArgs {
    pub seed: VehicleSeed,
    pub district: ActorRef<DistrictMsg>,
    pub gateway: ActorRef<GatewayMsg>,
    /// Set when the vehicle was handed over by a district of another process.
    pub arrival: Option<VehicleTransfer>,
    pub metrics: Arc<Metrics>,
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    Driving,
    Queued { pos: u32 },
    Crossing { left: f32 },
    Parked { left: f32 },
}

pub struct Vehicle {
    args: VehicleArgs,
    dir: Arc<Directory>,
    from: Node,
    to: Node,
    s: f32,
    dest: Node,
    route: VecDeque<Node>,
    mode: Mode,
    blocked: HashMap<Node, f32>,
    clock: f32,
    request_age: f32,
    unanswered: u32,
    trips: u32,
    hop: u32,
    trace: Trace,
}

impl ActorFactoryArgs<VehicleArgs> for Vehicle {
    fn create_args(args: VehicleArgs) -> Self {
        let spawn = args.seed.spawn;
        let arrival = args.arrival.clone();
        let mut vehicle = Self {
            dest: args.seed.hotspot.unwrap_or(spawn),
            args,
            dir: Arc::default(),
            from: spawn,
            to: spawn,
            s: 1.0,
            route: VecDeque::new(),
            mode: Mode::Parked { left: 0.0 },
            blocked: HashMap::new(),
            clock: 0.0,
            request_age: 0.0,
            unanswered: 0,
            trips: 0,
            hop: 0,
            trace: Trace::default(),
        };
        if let Some(transfer) = arrival {
            vehicle.from = transfer.from;
            vehicle.to = transfer.to;
            vehicle.dest = transfer.dest;
            vehicle.route = transfer.route;
            vehicle.trips = transfer.trips;
            vehicle.hop = transfer.hop;
            vehicle.s = 0.0;
            vehicle.mode = Mode::Driving;
        }
        vehicle
    }
}

impl Vehicle {
    fn name(&self) -> String {
        format!("v-{}", self.args.seed.id)
    }

    fn passable(&self, node: Node) -> bool {
        self.dir.has(node)
            && self
                .blocked
                .get(&node)
                .is_none_or(|until| *until <= self.clock)
    }

    fn pick_destination(&self, rng: &mut impl Rng, avoid: Node) -> Option<Node> {
        for _ in 0..32 {
            let node = Node::new(
                rng.random_range(0..GRID_NODES),
                rng.random_range(0..GRID_NODES),
            );
            if node != avoid && self.dir.has(node) {
                return Some(node);
            }
        }
        None
    }

    fn plan_trip(&mut self, ctx: &Context<VehicleMsg>) {
        let mut rng = rand::rng();
        let at = self.to;
        let hotspot = self
            .args
            .seed
            .hotspot
            .filter(|_| self.trips == 0 && self.dest != at);
        let Some(dest) = hotspot.or_else(|| self.pick_destination(&mut rng, at)) else {
            self.mode = Mode::Parked { left: 1.0 };
            return;
        };
        match plan_route(at, dest, |n| self.passable(n), &mut rng) {
            Some(mut route) if !route.is_empty() => {
                self.dest = dest;
                self.from = at;
                self.to = route.pop_front().expect("route is not empty");
                self.route = route;
                self.s = 0.0;
                self.mode = Mode::Driving;
                self.hand_over_if_remote(ctx);
            }
            _ => self.mode = Mode::Parked { left: 1.0 },
        }
    }

    fn arrive(&mut self, ctx: &Context<VehicleMsg>) {
        if self.to == self.dest {
            self.trips += 1;
            self.args.metrics.inc(&self.args.metrics.trips);
            self.mode = Mode::Parked {
                left: rand::rng().random_range(0.3..1.5),
            };
            return;
        }
        self.request_entry(ctx);
    }

    fn request_entry(&mut self, ctx: &Context<VehicleMsg>) {
        let Some(junction) = self.dir.junctions.get(&self.to).cloned() else {
            self.u_turn(ctx);
            return;
        };
        let id = self.args.seed.id;
        let axis = self.from.axis_to(self.to);
        junction.tell(
            JunctionMsg::RequestIntersectionEntry {
                vehicle: ctx.myself(),
                id,
                axis,
            },
            None,
        );
        self.args.metrics.inc(&self.args.metrics.sent);
        let (ms, to) = (self.args.metrics.elapsed_ms(), self.to);
        self.trace
            .sent(ms, "RequestIntersectionEntry", || to.junction_name());
        self.request_age = 0.0;
        if !matches!(self.mode, Mode::Queued { .. }) {
            self.mode = Mode::Queued { pos: 0 };
        }
    }

    /// The junction ahead cannot be reached: remember that and drive back to plan another way.
    fn u_turn(&mut self, ctx: &Context<VehicleMsg>) {
        self.args.metrics.inc(&self.args.metrics.reroutes);
        self.blocked.insert(self.to, self.clock + BLOCK_FOR);
        if self.from == self.to {
            self.mode = Mode::Parked { left: 1.0 };
            return;
        }
        let back = self.from;
        let mut rng = rand::rng();
        let route =
            plan_route(back, self.dest, |n| self.passable(n), &mut rng).unwrap_or_else(|| {
                self.dest = back;
                VecDeque::new()
            });
        self.from = self.to;
        self.to = back;
        self.route = route;
        self.s = 0.0;
        self.unanswered = 0;
        self.mode = Mode::Driving;
        self.hand_over_if_remote(ctx);
    }

    /// Drives into a district of another process: the vehicle leaves this actor system and
    /// continues as a new actor in the remote district, carrying its route and statistics.
    fn hand_over_if_remote(&mut self, ctx: &Context<VehicleMsg>) {
        if !self.dir.is_remote(self.to) {
            return;
        }
        let transfer = VehicleTransfer {
            id: self.args.seed.id,
            hop: self.hop + 1,
            from: self.from,
            to: self.to,
            dest: self.dest,
            route: std::mem::take(&mut self.route),
            speed: self.args.seed.speed,
            trips: self.trips,
        };
        self.args.gateway.tell(
            GatewayMsg::Migrate {
                district: self.to.district(),
                transfer,
            },
            None,
        );
        self.args.metrics.inc(&self.args.metrics.sent);
        self.mode = Mode::Parked { left: f32::MAX };
        ctx.stop(ctx.myself());
    }

    fn replan_after_directory_change(&mut self, ctx: &Context<VehicleMsg>) {
        if matches!(self.mode, Mode::Parked { .. }) {
            return;
        }
        if !self.dir.has(self.to) {
            self.u_turn(ctx);
        } else if self.route.iter().any(|n| !self.dir.has(*n)) {
            let mut rng = rand::rng();
            self.route = plan_route(self.to, self.dest, |n| self.passable(n), &mut rng)
                .unwrap_or_else(|| {
                    self.dest = self.to;
                    VecDeque::new()
                });
        }
    }

    fn finish_crossing(&mut self, ctx: &Context<VehicleMsg>) {
        if let Some(junction) = self.dir.junctions.get(&self.to) {
            junction.tell(
                JunctionMsg::VehicleExitedIntersection {
                    id: self.args.seed.id,
                },
                None,
            );
            self.args.metrics.inc(&self.args.metrics.sent);
            let (ms, to) = (self.args.metrics.elapsed_ms(), self.to);
            self.trace
                .sent(ms, "VehicleExitedIntersection", || to.junction_name());
        }
        match self.route.pop_front() {
            Some(next) => {
                self.from = self.to;
                self.to = next;
                self.s = 0.0;
                self.mode = Mode::Driving;
                self.hand_over_if_remote(ctx);
            }
            None => self.mode = Mode::Parked { left: 0.0 },
        }
    }

    fn view(&self) -> VehicleView {
        let (s, state) = match self.mode {
            Mode::Driving => (self.s, state::MOVING),
            Mode::Queued { pos } => (
                1.0 - 0.07 * f32::from(u8::try_from(pos.min(8)).unwrap_or(8)),
                state::WAITING,
            ),
            Mode::Crossing { .. } => (1.0, state::CROSSING),
            Mode::Parked { .. } => (self.s, state::PARKED),
        };
        VehicleView {
            id: self.args.seed.id,
            from: self.from,
            to: self.to,
            s,
            state,
        }
    }

    fn tick(&mut self, ctx: &Context<VehicleMsg>, tick: Tick) {
        let dt = tick.dt;
        self.clock += dt;
        self.trace.expire();
        self.args.metrics.inc(&self.args.metrics.vehicle_ticks);

        match self.mode {
            Mode::Parked { left } => {
                let left = left - dt;
                self.mode = Mode::Parked { left };
                if left <= 0.0 {
                    self.plan_trip(ctx);
                }
            }
            // A handed-over vehicle waits for its address book before reaching a junction.
            Mode::Driving if self.dir.version == 0 => {}
            Mode::Driving => {
                self.s += self.args.seed.speed * dt;
                if self.s >= 1.0 {
                    self.s = 1.0;
                    self.arrive(ctx);
                }
            }
            Mode::Queued { .. } => {
                self.request_age += dt;
                if self.request_age >= RETRY_AFTER {
                    self.unanswered += 1;
                    self.args.metrics.inc(&self.args.metrics.retries);
                    if self.unanswered > MAX_UNANSWERED {
                        self.u_turn(ctx);
                    } else {
                        self.request_entry(ctx);
                    }
                }
            }
            Mode::Crossing { left } => {
                let left = left - dt;
                if left <= 0.0 {
                    self.finish_crossing(ctx);
                } else {
                    self.mode = Mode::Crossing { left };
                }
            }
        }

        if tick.report {
            self.args
                .district
                .tell(DistrictMsg::VehicleReport(self.view()), None);
            self.args.metrics.inc(&self.args.metrics.sent);
        }
    }

    fn answered(&mut self) {
        self.request_age = 0.0;
        self.unanswered = 0;
    }

    fn inspect(&mut self, ctx: &Context<VehicleMsg>, req: &InspectReq) {
        self.trace.watch();
        let id = self.name();
        let state = match self.mode {
            Mode::Driving => "moving",
            Mode::Queued { .. } => "waiting for entry",
            Mode::Crossing { .. } => "turning / crossing",
            Mode::Parked { .. } => "parked at destination",
        };
        let body = json!({
            "id": id,
            "type": "vehicle",
            "state": state,
            "lifecycle": "running",
            "age_s": self.clock,
            "path": ctx.myself().path().to_string(),
            "details": {
                "position": [self.from.x, self.from.y, self.to.x, self.to.y, self.s],
                "speed": self.args.seed.speed,
                "from": self.from.junction_name(),
                "heading_to": self.to.junction_name(),
                "destination": self.dest.junction_name(),
                "route_hops_left": self.route.len(),
                "trips_completed": self.trips,
                "blocked_junctions": self.blocked.len(),
            },
            "trace": self.trace.to_json(&id),
        });
        req.reply.tell(
            GatewayMsg::InspectReply {
                id,
                json: body.to_string(),
            },
            None,
        );
    }
}

impl Actor for Vehicle {
    type Msg = VehicleMsg;

    fn pre_start(&mut self, ctx: &Context<Self::Msg>) {
        // A freshly started (or restarted) vehicle has no address book yet.
        self.args
            .district
            .tell(DistrictMsg::NeedDirectory(ctx.myself()), None);
        self.args.metrics.inc(&self.args.metrics.sent);
    }

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, _: Sender) {
        let sampled = self.args.seed.id.trailing_zeros() >= 4;
        let started = sampled.then(Instant::now);
        self.args.metrics.inc(&self.args.metrics.processed);

        let ms = self.args.metrics.elapsed_ms();
        match msg {
            VehicleMsg::Tick(t) => {
                if sampled {
                    self.args.metrics.tick_delay.record(t.sent.elapsed());
                }
                self.tick(ctx, t);
            }
            VehicleMsg::EntryGranted { node } => {
                self.trace
                    .received(ms, "EntryGranted", || node.junction_name());
                if node == self.to && !matches!(self.mode, Mode::Crossing { .. }) {
                    self.answered();
                    self.mode = Mode::Crossing { left: CROSS_TIME };
                }
            }
            VehicleMsg::EntryDenied { node, queue_pos } => {
                self.trace
                    .received(ms, "EntryDenied", || node.junction_name());
                if node == self.to && matches!(self.mode, Mode::Queued { .. }) {
                    self.answered();
                    self.mode = Mode::Queued { pos: queue_pos };
                }
            }
            VehicleMsg::TrafficSignalChanged { node, green } => {
                self.trace.received(ms, "TrafficSignalChanged", || {
                    format!(
                        "{} ({})",
                        node.light_name(),
                        green.map_or("red".into(), |a| format!("{a:?} green"))
                    )
                });
                if node == self.to {
                    self.answered();
                }
            }
            VehicleMsg::Directory(dir) => {
                self.dir = dir;
                self.replan_after_directory_change(ctx);
            }
            VehicleMsg::Fault(Fault::Crash) => crash(ctx, &self.name()),
            VehicleMsg::Fault(Fault::Terminate) => ctx.stop(ctx.myself()),
            VehicleMsg::Inspect(req) => self.inspect(ctx, &req),
        }

        if let Some(t0) = started {
            self.args.metrics.handler.record(t0.elapsed());
        }
    }
}
