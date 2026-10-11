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
        VehicleMsg, VehicleView, crash, state,
    },
    trace::Trace,
};

const CROSS_TIME: f32 = 0.6;
const RETRY_AFTER: f32 = 1.5;
const MAX_UNANSWERED: u32 = 2;
const BLOCK_FOR: f32 = 30.0;

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
    trace: Trace,
}

impl ActorFactoryArgs<VehicleArgs> for Vehicle {
    fn create_args(args: VehicleArgs) -> Self {
        let spawn = args.seed.spawn;
        Self {
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
            trace: Trace::default(),
        }
    }
}

impl Vehicle {
    fn name(&self) -> String {
        format!("v-{}", self.args.seed.id)
    }

    fn passable(&self, node: Node) -> bool {
        self.dir.junctions.contains_key(&node)
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
            if node != avoid && self.dir.junctions.contains_key(&node) {
                return Some(node);
            }
        }
        None
    }

    fn plan_trip(&mut self) {
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
            self.u_turn();
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
    fn u_turn(&mut self) {
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
    }

    fn replan_after_directory_change(&mut self) {
        if matches!(self.mode, Mode::Parked { .. }) {
            return;
        }
        if !self.dir.junctions.contains_key(&self.to) {
            self.u_turn();
        } else if self
            .route
            .iter()
            .any(|n| !self.dir.junctions.contains_key(n))
        {
            let mut rng = rand::rng();
            self.route = plan_route(self.to, self.dest, |n| self.passable(n), &mut rng)
                .unwrap_or_else(|| {
                    self.dest = self.to;
                    VecDeque::new()
                });
        }
    }

    fn finish_crossing(&mut self) {
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
                    self.plan_trip();
                }
            }
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
                        self.u_turn();
                    } else {
                        self.request_entry(ctx);
                    }
                }
            }
            Mode::Crossing { left } => {
                let left = left - dt;
                if left <= 0.0 {
                    self.finish_crossing();
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
                self.replan_after_directory_change();
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
