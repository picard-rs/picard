//! Intersection actor: owns the queues and occupancy of one junction and grants safe passage.
//!
//! The safety rule lives in [`Core`], a pure data structure that is unit-tested on its own:
//! vehicles from conflicting axes are never inside the junction at the same time.

use std::{collections::VecDeque, sync::Arc};

use picard::actors::{
    Actor, ActorFactoryArgs, ActorRef, ActorRefFactory, ActorReference, Context, Sender, Tell,
};
use serde_json::json;

use crate::{
    city::{Axis, Node},
    light::{Light, LightArgs},
    metrics::Metrics,
    msg::{
        DistrictMsg, Fault, GatewayMsg, JunctionMsg, JunctionView, LightMsg, Signal, VehicleId,
        VehicleMsg, crash,
    },
    trace::Trace,
};

/// Vehicles of one axis that may be inside the junction together.
pub const CAPACITY: usize = 3;
/// A grant is released automatically if the vehicle never reports leaving (it may have died).
pub const LEASE: f32 = 2.0;
const LIGHT_TIMEOUT: f32 = 3.0;
const RESPAWN_AFTER: f32 = 8.0;
const DEMAND_EVERY: f32 = 1.0;

struct Inside {
    id: VehicleId,
    axis: Axis,
    lease: f32,
}

pub enum Outcome {
    Granted,
    Queued(u32),
}

/// Queues and occupancy of one junction, generic over the handle used to answer a vehicle.
pub struct Core<H> {
    queues: [VecDeque<(VehicleId, H)>; 2],
    inside: Vec<Inside>,
}

impl<H> Default for Core<H> {
    fn default() -> Self {
        Self {
            queues: [VecDeque::new(), VecDeque::new()],
            inside: Vec::new(),
        }
    }
}

impl<H> Core<H> {
    fn allowed(&self, axis: Axis, signal: Signal) -> bool {
        signal.permits(axis)
            && self.inside.len() < CAPACITY
            && self.inside.iter().all(|i| i.axis == axis)
    }

    fn admit(&mut self, id: VehicleId, axis: Axis) {
        self.inside.push(Inside {
            id,
            axis,
            lease: LEASE,
        });
    }

    /// Requests are idempotent: a retry from a vehicle that already holds a grant or a queue
    /// position does not create a duplicate.
    pub fn request(&mut self, id: VehicleId, axis: Axis, handle: H, signal: Signal) -> Outcome {
        if let Some(inside) = self.inside.iter_mut().find(|i| i.id == id) {
            inside.lease = LEASE;
            return Outcome::Granted;
        }
        let q = axis.index();
        if let Some(pos) = self.queues[q].iter().position(|(v, _)| *v == id) {
            self.queues[q][pos].1 = handle;
            return Outcome::Queued(u32::try_from(pos).unwrap_or(u32::MAX));
        }
        if self.queues[q].is_empty() && self.allowed(axis, signal) {
            self.admit(id, axis);
            return Outcome::Granted;
        }
        self.queues[q].push_back((id, handle));
        Outcome::Queued(u32::try_from(self.queues[q].len() - 1).unwrap_or(u32::MAX))
    }

    /// Admits queued vehicles that may now enter, oldest first.
    pub fn drain(&mut self, signal: Signal) -> Vec<(VehicleId, H)> {
        let mut granted = Vec::new();
        for axis in [Axis::Ns, Axis::Ew] {
            while self.allowed(axis, signal) {
                let Some((id, handle)) = self.queues[axis.index()].pop_front() else {
                    break;
                };
                self.admit(id, axis);
                granted.push((id, handle));
            }
        }
        granted
    }

    pub fn exit(&mut self, id: VehicleId) {
        self.inside.retain(|i| i.id != id);
    }

    pub fn expire(&mut self, dt: f32) {
        for i in &mut self.inside {
            i.lease -= dt;
        }
        self.inside.retain(|i| i.lease > 0.0);
    }

    #[must_use]
    pub fn queued(&self, axis: Axis) -> usize {
        self.queues[axis.index()].len()
    }

    #[must_use]
    pub fn inside(&self) -> usize {
        self.inside.len()
    }

    pub fn handles(&self) -> impl Iterator<Item = &H> {
        self.queues.iter().flatten().map(|(_, h)| h)
    }

    /// The safety invariant: everybody inside travels on the same axis and capacity is respected.
    #[must_use]
    pub fn is_safe(&self) -> bool {
        self.inside.len() <= CAPACITY && self.inside.windows(2).all(|w| w[0].axis == w[1].axis)
    }
}

#[derive(Clone)]
pub struct JunctionArgs {
    pub node: Node,
    pub district: ActorRef<DistrictMsg>,
    pub metrics: Arc<Metrics>,
}

pub struct Junction {
    args: JunctionArgs,
    core: Core<ActorRef<VehicleMsg>>,
    signal: Signal,
    light: Option<ActorRef<LightMsg>>,
    since_signal: f32,
    degraded: Option<f32>,
    last_respawn: f32,
    since_demand: f32,
    clock: f32,
    granted: u64,
    denied: u64,
    trace: Trace,
}

impl ActorFactoryArgs<JunctionArgs> for Junction {
    fn create_args(args: JunctionArgs) -> Self {
        Self {
            args,
            core: Core::default(),
            signal: Signal::ALL_RED,
            light: None,
            since_signal: 0.0,
            degraded: None,
            last_respawn: 0.0,
            since_demand: 0.0,
            clock: 0.0,
            granted: 0,
            denied: 0,
            trace: Trace::default(),
        }
    }
}

/// Fixed-time plan used while the traffic light actor is unresponsive: 3s per axis, 1s all red.
fn fallback_signal(degraded_for: f32) -> Signal {
    let t = degraded_for % 8.0;
    match t {
        t if t < 3.0 => Signal {
            green: Some(Axis::Ns),
            yellow: false,
        },
        t if (4.0..7.0).contains(&t) => Signal {
            green: Some(Axis::Ew),
            yellow: false,
        },
        _ => Signal::ALL_RED,
    }
}

impl Junction {
    fn me(&self) -> String {
        self.args.node.junction_name()
    }

    fn ms(&self) -> u32 {
        self.args.metrics.elapsed_ms()
    }

    fn spawn_light(&mut self, ctx: &Context<JunctionMsg>) {
        let args = LightArgs {
            node: self.args.node,
            junction: ctx.myself(),
            metrics: self.args.metrics.clone(),
        };
        if let Ok(light) = ctx.actor_of_args::<Light, _>("light", args) {
            self.args.metrics.inc(&self.args.metrics.created);
            self.light = Some(light);
            self.last_respawn = self.clock;
        }
    }

    fn send_vehicle(
        &mut self,
        to: &ActorRef<VehicleMsg>,
        msg: VehicleMsg,
        kind: &'static str,
        id: VehicleId,
    ) {
        to.tell(msg, None);
        self.args.metrics.inc(&self.args.metrics.sent);
        let ms = self.ms();
        self.trace.sent(ms, kind, || format!("v-{id}"));
    }

    fn grant_all(&mut self, granted: Vec<(VehicleId, ActorRef<VehicleMsg>)>) {
        for (id, vehicle) in granted {
            self.granted += 1;
            self.args.metrics.inc(&self.args.metrics.granted);
            let node = self.args.node;
            self.send_vehicle(
                &vehicle,
                VehicleMsg::EntryGranted { node },
                "EntryGranted",
                id,
            );
        }
    }

    fn set_signal(&mut self, signal: Signal) {
        if signal == self.signal {
            return;
        }
        self.signal = signal;
        let node = self.args.node;
        let waiting: Vec<_> = self.core.handles().cloned().collect();
        for v in waiting {
            v.tell(
                VehicleMsg::TrafficSignalChanged {
                    node,
                    green: signal.green,
                },
                None,
            );
            self.args.metrics.inc(&self.args.metrics.sent);
            let ms = self.ms();
            self.trace
                .sent(ms, "TrafficSignalChanged", || "queued vehicles".to_string());
        }
        let granted = self.core.drain(self.signal);
        self.grant_all(granted);
    }

    fn tick(&mut self, ctx: &Context<JunctionMsg>, tick: crate::msg::Tick) {
        let dt = tick.dt;
        self.clock += dt;
        self.since_signal += dt;
        self.since_demand += dt;
        self.trace.expire();
        self.core.expire(dt);

        if self.since_signal > LIGHT_TIMEOUT && self.degraded.is_none() {
            self.degraded = Some(0.0);
        }
        if let Some(d) = self.degraded.as_mut() {
            *d += dt;
            let signal = fallback_signal(*d);
            let due = self.clock - self.last_respawn > RESPAWN_AFTER;
            self.set_signal(signal);
            if due {
                self.spawn_light(ctx);
            }
        }

        let granted = self.core.drain(self.signal);
        self.grant_all(granted);

        if !self.core.is_safe() {
            self.args.metrics.inc(&self.args.metrics.violations);
        }

        if self.since_demand >= DEMAND_EVERY {
            self.since_demand = 0.0;
            if let Some(light) = &self.light {
                let demand = LightMsg::Demand {
                    ns: u32::try_from(self.core.queued(Axis::Ns)).unwrap_or(u32::MAX),
                    ew: u32::try_from(self.core.queued(Axis::Ew)).unwrap_or(u32::MAX),
                };
                light.tell(demand, None);
                self.args.metrics.inc(&self.args.metrics.sent);
            }
        }

        if tick.report {
            let view = JunctionView {
                node: self.args.node,
                signal: self.signal,
                queued: [self.core.queued(Axis::Ns), self.core.queued(Axis::Ew)]
                    .map(|n| u32::try_from(n).unwrap_or(u32::MAX)),
                inside: u32::try_from(self.core.inside()).unwrap_or(u32::MAX),
                degraded: self.degraded.is_some(),
            };
            self.args
                .district
                .tell(DistrictMsg::JunctionReport(view), None);
            self.args.metrics.inc(&self.args.metrics.sent);
        }
    }

    fn inspect(&mut self, ctx: &Context<JunctionMsg>, req: &crate::msg::InspectReq) {
        self.trace.watch();
        let id = self.me();
        let state = match (self.degraded, self.signal.green, self.signal.yellow) {
            (Some(_), ..) => "degraded: light unresponsive, fixed-time fallback".to_string(),
            (None, Some(a), false) => format!("{a:?} green"),
            (None, Some(a), true) => format!("{a:?} yellow"),
            (None, None, _) => "all red".to_string(),
        };
        let body = json!({
            "id": id,
            "type": "intersection",
            "state": state,
            "lifecycle": "running",
            "age_s": self.clock,
            "path": ctx.myself().path().to_string(),
            "details": {
                "queue_ns": self.core.queued(Axis::Ns),
                "queue_ew": self.core.queued(Axis::Ew),
                "inside": self.core.inside(),
                "granted": self.granted,
                "denied": self.denied,
                "light": if self.degraded.is_some() { "unresponsive" } else { "ok" },
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

impl Actor for Junction {
    type Msg = JunctionMsg;

    fn pre_start(&mut self, ctx: &Context<Self::Msg>) {
        self.spawn_light(ctx);
    }

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, _: Sender) {
        let metrics = self.args.metrics.clone();
        metrics.inc(&metrics.processed);
        match msg {
            JunctionMsg::Tick(t) => self.tick(ctx, t),
            JunctionMsg::RequestIntersectionEntry { vehicle, id, axis } => {
                let ms = self.ms();
                self.trace
                    .received(ms, "RequestIntersectionEntry", || format!("v-{id}"));
                match self.core.request(id, axis, vehicle.clone(), self.signal) {
                    Outcome::Granted => {
                        self.granted += 1;
                        metrics.inc(&metrics.granted);
                        let node = self.args.node;
                        self.send_vehicle(
                            &vehicle,
                            VehicleMsg::EntryGranted { node },
                            "EntryGranted",
                            id,
                        );
                    }
                    Outcome::Queued(queue_pos) => {
                        self.denied += 1;
                        metrics.inc(&metrics.denied);
                        let node = self.args.node;
                        self.send_vehicle(
                            &vehicle,
                            VehicleMsg::EntryDenied { node, queue_pos },
                            "EntryDenied",
                            id,
                        );
                    }
                }
            }
            JunctionMsg::VehicleExitedIntersection { id } => {
                let ms = self.ms();
                self.trace
                    .received(ms, "VehicleExitedIntersection", || format!("v-{id}"));
                self.core.exit(id);
                let granted = self.core.drain(self.signal);
                self.grant_all(granted);
            }
            JunctionMsg::TrafficSignalChanged(signal) => {
                let ms = self.ms();
                let light = self.args.node.light_name();
                self.trace.received(ms, "TrafficSignalChanged", || light);
                self.since_signal = 0.0;
                self.degraded = None;
                self.set_signal(signal);
            }
            JunctionMsg::Fault(Fault::Crash) => crash(ctx, &self.me()),
            JunctionMsg::Fault(Fault::Terminate) => ctx.stop(ctx.myself()),
            JunctionMsg::FaultLight(fault) => {
                if let Some(light) = &self.light {
                    light.tell(LightMsg::Fault(fault), None);
                }
            }
            JunctionMsg::Inspect(req) => self.inspect(ctx, &req),
            JunctionMsg::InspectLight(req) => {
                if let (Some(light), None) = (&self.light, self.degraded) {
                    light.tell(LightMsg::Inspect(req), None);
                } else {
                    let id = self.args.node.light_name();
                    let body = json!({
                        "id": id,
                        "type": "traffic light",
                        "state": "unresponsive or terminated",
                        "lifecycle": "terminated",
                        "details": { "note": "the junction detected missing heartbeats and runs a fixed-time fallback" },
                        "trace": { "sent": 0, "received": 0, "recent": [] },
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NS_GREEN: Signal = Signal {
        green: Some(Axis::Ns),
        yellow: false,
    };
    const EW_GREEN: Signal = Signal {
        green: Some(Axis::Ew),
        yellow: false,
    };

    fn granted(o: &Outcome) -> bool {
        matches!(o, Outcome::Granted)
    }

    #[test]
    fn red_axis_waits_and_green_axis_enters() {
        let mut core: Core<()> = Core::default();
        assert!(granted(&core.request(1, Axis::Ns, (), NS_GREEN)));
        assert!(matches!(
            core.request(2, Axis::Ew, (), NS_GREEN),
            Outcome::Queued(0)
        ));
        assert!(core.is_safe());
    }

    #[test]
    fn conflicting_axis_is_held_until_the_junction_is_clear() {
        let mut core: Core<()> = Core::default();
        assert!(granted(&core.request(1, Axis::Ns, (), NS_GREEN)));
        core.request(2, Axis::Ew, (), NS_GREEN);
        assert!(core.drain(EW_GREEN).is_empty(), "vehicle 1 is still inside");
        core.exit(1);
        assert_eq!(core.drain(EW_GREEN).len(), 1);
        assert!(core.is_safe());
    }

    #[test]
    fn capacity_is_enforced_and_queue_is_fifo() {
        let mut core: Core<()> = Core::default();
        for id in 0..CAPACITY as u64 {
            assert!(granted(&core.request(id, Axis::Ns, (), NS_GREEN)));
        }
        assert!(matches!(
            core.request(10, Axis::Ns, (), NS_GREEN),
            Outcome::Queued(0)
        ));
        assert!(matches!(
            core.request(11, Axis::Ns, (), NS_GREEN),
            Outcome::Queued(1)
        ));
        core.exit(0);
        let admitted = core.drain(NS_GREEN);
        assert_eq!(
            admitted.iter().map(|(id, ())| *id).collect::<Vec<_>>(),
            vec![10]
        );
    }

    #[test]
    fn retries_are_idempotent() {
        let mut core: Core<()> = Core::default();
        core.request(1, Axis::Ew, (), NS_GREEN);
        core.request(1, Axis::Ew, (), NS_GREEN);
        assert_eq!(core.queued(Axis::Ew), 1);
        assert!(granted(&core.request(2, Axis::Ns, (), NS_GREEN)));
        assert!(granted(&core.request(2, Axis::Ns, (), NS_GREEN)));
        assert_eq!(core.inside(), 1);
    }

    #[test]
    fn yellow_and_red_admit_nobody() {
        let mut core: Core<()> = Core::default();
        let yellow = Signal {
            green: Some(Axis::Ns),
            yellow: true,
        };
        assert!(!granted(&core.request(1, Axis::Ns, (), yellow)));
        assert!(!granted(&core.request(2, Axis::Ew, (), Signal::ALL_RED)));
        assert_eq!(core.inside(), 0);
    }

    #[test]
    fn leases_release_vehicles_that_never_report_back() {
        let mut core: Core<()> = Core::default();
        core.request(1, Axis::Ns, (), NS_GREEN);
        core.expire(LEASE + 0.1);
        assert_eq!(core.inside(), 0);
    }

    #[test]
    fn random_traffic_never_breaks_the_invariant() {
        use rand::Rng;
        let mut rng = rand::rng();
        let mut core: Core<()> = Core::default();
        let signals = [
            NS_GREEN,
            EW_GREEN,
            Signal::ALL_RED,
            Signal {
                green: Some(Axis::Ns),
                yellow: true,
            },
        ];
        let mut signal = signals[0];
        for step in 0..20_000_u64 {
            match rng.random_range(0..6) {
                0 | 1 => {
                    let axis = if rng.random_bool(0.5) {
                        Axis::Ns
                    } else {
                        Axis::Ew
                    };
                    core.request(step % 40, axis, (), signal);
                }
                2 => core.exit(rng.random_range(0..40)),
                3 => signal = signals[rng.random_range(0..signals.len())],
                4 => core.expire(0.05),
                _ => {
                    core.drain(signal);
                }
            }
            assert!(core.is_safe(), "unsafe state after step {step}");
        }
    }
}
