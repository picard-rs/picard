//! Traffic light actor: cycles through its phases and tells its junction about every change.

use std::sync::Arc;

use picard::actors::{
    Actor, ActorFactoryArgs, ActorRef, ActorRefFactory, ActorReference, Context, Sender, Tell,
};
use serde_json::json;

use crate::{
    city::{Axis, Node},
    metrics::Metrics,
    msg::{Fault, JunctionMsg, LightMsg, Signal, crash},
    trace::Trace,
};

const PHASES: usize = 6;
const MIN_GREEN: f32 = 3.0;
const MAX_GREEN: f32 = 12.0;
const BASE_GREEN: f32 = 7.0;
const YELLOW: f32 = 1.5;
const ALL_RED: f32 = 1.0;
const HEARTBEAT: f32 = 1.0;

#[derive(Clone)]
pub struct LightArgs {
    pub node: Node,
    pub junction: ActorRef<JunctionMsg>,
    pub metrics: Arc<Metrics>,
}

pub struct Light {
    args: LightArgs,
    phase: usize,
    phase_age: f32,
    left: f32,
    demand: [u32; 2],
    since_heartbeat: f32,
    clock: f32,
    trace: Trace,
}

impl ActorFactoryArgs<LightArgs> for Light {
    fn create_args(args: LightArgs) -> Self {
        // Neighbouring junctions start on opposite axes so the grid does not pulse in unison.
        let phase = if (args.node.x + args.node.y) % 2 == 0 {
            0
        } else {
            3
        };
        Self {
            args,
            phase,
            phase_age: 0.0,
            left: phase_len(phase),
            demand: [0; 2],
            since_heartbeat: 0.0,
            clock: 0.0,
            trace: Trace::default(),
        }
    }
}

const fn phase_axis(phase: usize) -> Axis {
    if phase < 3 { Axis::Ns } else { Axis::Ew }
}

const fn is_green(phase: usize) -> bool {
    phase.is_multiple_of(3)
}

fn phase_len(phase: usize) -> f32 {
    match phase % 3 {
        0 => BASE_GREEN,
        1 => YELLOW,
        _ => ALL_RED,
    }
}

fn phase_signal(phase: usize) -> Signal {
    match phase % 3 {
        0 => Signal {
            green: Some(phase_axis(phase)),
            yellow: false,
        },
        1 => Signal {
            green: Some(phase_axis(phase)),
            yellow: true,
        },
        _ => Signal::ALL_RED,
    }
}

impl Light {
    fn announce(&mut self) {
        self.args.junction.tell(
            JunctionMsg::TrafficSignalChanged(phase_signal(self.phase)),
            None,
        );
        self.args.metrics.inc(&self.args.metrics.sent);
        let (ms, node) = (self.args.metrics.elapsed_ms(), self.args.node);
        self.trace
            .sent(ms, "TrafficSignalChanged", || node.junction_name());
        self.since_heartbeat = 0.0;
    }

    fn advance_phase(&mut self) {
        self.phase = (self.phase + 1) % PHASES;
        self.phase_age = 0.0;
        self.left = phase_len(self.phase);
        self.announce();
    }

    /// Demand-responsive timing: extend a busy green, cut short a green nobody is using.
    fn adapt(&mut self) {
        if !is_green(self.phase) {
            return;
        }
        let axis = phase_axis(self.phase);
        let (mine, other) = (self.demand[axis.index()], self.demand[axis.other().index()]);
        if self.phase_age >= MIN_GREEN && mine == 0 && other > 0 {
            self.left = 0.0;
        } else if self.left <= 0.0 && self.phase_age < MAX_GREEN && mine > other {
            self.left = 1.0;
        }
    }

    fn tick(&mut self, dt: f32) {
        self.clock += dt;
        self.phase_age += dt;
        self.left -= dt;
        self.since_heartbeat += dt;
        self.trace.expire();
        self.adapt();
        if self.left <= 0.0 {
            self.advance_phase();
        } else if self.since_heartbeat >= HEARTBEAT {
            self.announce();
        }
    }
}

impl Actor for Light {
    type Msg = LightMsg;

    fn post_start(&mut self, _ctx: &Context<Self::Msg>) {
        self.announce();
    }

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, _: Sender) {
        self.args.metrics.inc(&self.args.metrics.processed);
        match msg {
            LightMsg::Tick(t) => self.tick(t.dt),
            LightMsg::Demand { ns, ew } => {
                self.demand = [ns, ew];
                let ms = self.args.metrics.elapsed_ms();
                let node = self.args.node;
                self.trace.received(ms, "Demand", || node.junction_name());
            }
            LightMsg::Fault(Fault::Crash) => crash(ctx, &self.args.node.light_name()),
            LightMsg::Fault(Fault::Terminate) => ctx.stop(ctx.myself()),
            LightMsg::Inspect(req) => {
                self.trace.watch();
                let id = self.args.node.light_name();
                let signal = phase_signal(self.phase);
                let state = match (signal.green, signal.yellow) {
                    (Some(a), false) => format!("green {a:?}"),
                    (Some(a), true) => format!("yellow {a:?}"),
                    (None, _) => "all red".to_string(),
                };
                let body = json!({
                    "id": id,
                    "type": "traffic light",
                    "state": state,
                    "lifecycle": "running",
                    "age_s": self.clock,
                    "path": ctx.myself().path().to_string(),
                    "details": {
                        "phase": self.phase,
                        "phase_age_s": self.phase_age,
                        "demand_ns": self.demand[0],
                        "demand_ew": self.demand[1],
                    },
                    "trace": self.trace.to_json(&id),
                });
                req.reply.tell(
                    crate::msg::GatewayMsg::InspectReply {
                        id,
                        json: body.to_string(),
                    },
                    None,
                );
            }
        }
    }
}
