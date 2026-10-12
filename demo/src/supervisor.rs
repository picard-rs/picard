//! Simulation supervisor: owns the districts, the clock and the simulation lifecycle.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use picard::actors::{
    Actor, ActorFactoryArgs, ActorRef, ActorRefFactory, Context, Sender, Tell, Timer,
};
use picard::platform::Instant;

use crate::{
    city::{DEFAULT_ACTIVE, DISTRICT_COUNT, Node, district_nodes},
    district::{District, DistrictArgs},
    metrics::Metrics,
    msg::{Directory, DistrictMsg, GatewayMsg, InspectReq, JunctionMsg, SupervisorMsg, Tick},
};

pub const TICK_DT: f32 = 0.05;
pub const SURGE_SIZE: usize = 800;

#[derive(Clone)]
pub struct SupervisorArgs {
    pub gateway: ActorRef<GatewayMsg>,
    pub metrics: Arc<Metrics>,
    pub initial_vehicles: usize,
    pub district: Option<usize>,
    pub district_count: Option<usize>,
}

pub struct Supervisor {
    args: SupervisorArgs,
    districts: BTreeMap<usize, ActorRef<DistrictMsg>>,
    junctions: std::collections::HashMap<Node, ActorRef<JunctionMsg>>,
    version: u64,
    generation: u64,
    per_district: usize,
    paused: bool,
    tick_no: u64,
}

impl ActorFactoryArgs<SupervisorArgs> for Supervisor {
    fn create_args(args: SupervisorArgs) -> Self {
        Self {
            per_district: if args.district.is_some() {
                args.initial_vehicles
            } else {
                args.initial_vehicles / DEFAULT_ACTIVE.len()
            },
            args,
            districts: BTreeMap::new(),
            junctions: std::collections::HashMap::new(),
            version: 0,
            generation: 0,
            paused: false,
            tick_no: 0,
        }
    }
}

impl Supervisor {
    fn status(&self) {
        self.args.gateway.tell(
            GatewayMsg::Status {
                paused: self.paused,
                target: self.per_district * self.districts.len(),
            },
            None,
        );
    }

    fn start_district(&mut self, ctx: &Context<SupervisorMsg>, index: usize) {
        if self.districts.contains_key(&index) {
            return;
        }
        self.generation += 1;
        let args = DistrictArgs {
            index,
            generation: self.generation,
            id_base: (self.generation << 40) | ((index as u64) << 32),
            supervisor: ctx.myself(),
            gateway: self.args.gateway.clone(),
            metrics: self.args.metrics.clone(),
        };
        let name = format!("district-{index}-g{}", self.generation);
        if let Ok(district) = ctx.actor_of_args::<District, _>(&name, args) {
            self.args.metrics.inc(&self.args.metrics.created);
            self.args.gateway.tell(
                GatewayMsg::DistrictStarted {
                    district: index,
                    generation: self.generation,
                },
                None,
            );
            self.districts.insert(index, district);
        }
    }

    fn stop_district(&mut self, ctx: &Context<SupervisorMsg>, index: usize) {
        if let Some(district) = self.districts.remove(&index) {
            ctx.stop(district);
            for node in district_nodes(index) {
                self.junctions.remove(&node);
            }
            self.args
                .gateway
                .tell(GatewayMsg::DistrictGone(index), None);
            self.publish_directory();
        }
    }

    fn publish_directory(&mut self) {
        self.version += 1;
        let dir = Arc::new(Directory {
            version: self.version,
            junctions: self.junctions.clone(),
        });
        for district in self.districts.values() {
            district.tell(DistrictMsg::Directory(dir.clone()), None);
        }
    }

    fn broadcast(&self, msg: &DistrictMsg) {
        for district in self.districts.values() {
            district.tell(msg.clone(), None);
        }
    }

    fn reset(&mut self, ctx: &Context<SupervisorMsg>) {
        let indices: Vec<usize> = self.districts.keys().copied().collect();
        for index in indices {
            self.stop_district(ctx, index);
        }
        self.junctions.clear();
        self.per_district = if self.args.district.is_some() {
            self.args.initial_vehicles
        } else {
            self.args.initial_vehicles / DEFAULT_ACTIVE.len()
        };
        self.paused = false;
        if let Some(index) = self.args.district {
            self.start_district(ctx, index);
        } else {
            for index in DEFAULT_ACTIVE {
                self.start_district(ctx, index);
            }
        }
        self.status();
    }
}

impl Actor for Supervisor {
    type Msg = SupervisorMsg;

    fn pre_start(&mut self, ctx: &Context<Self::Msg>) {
        let period = Duration::from_secs_f32(TICK_DT);
        ctx.schedule(period, period, ctx.myself(), None, SupervisorMsg::Tick);
        if let Some(index) = self.args.district {
            self.start_district(ctx, index);
        } else {
            for index in DEFAULT_ACTIVE {
                self.start_district(ctx, index);
            }
        }
        self.status();
    }

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, _: Sender) {
        self.args.metrics.inc(&self.args.metrics.processed);
        match msg {
            SupervisorMsg::Tick => {
                if self.paused {
                    return;
                }
                self.tick_no += 1;
                self.args.metrics.inc(&self.args.metrics.sim_ticks);
                self.broadcast(&DistrictMsg::Tick(Tick {
                    dt: TICK_DT,
                    sent: Instant::now(),
                    report: self.tick_no.is_multiple_of(2),
                }));
                self.args
                    .metrics
                    .add(&self.args.metrics.sent, self.districts.len() as u64);
            }
            SupervisorMsg::Pause(paused) => {
                self.paused = paused;
                self.status();
            }
            SupervisorMsg::Reset => self.reset(ctx),
            SupervisorMsg::SetVehicles(total) => {
                if !self.districts.is_empty() {
                    self.per_district = self
                        .args
                        .district_count
                        .map_or_else(|| total / self.districts.len(), |count| total / count);
                    self.broadcast(&DistrictMsg::SetTarget(self.per_district));
                    self.status();
                }
            }
            SupervisorMsg::Surge(index) => {
                if let Some(district) = self.districts.get(&index) {
                    district.tell(DistrictMsg::Surge(SURGE_SIZE), None);
                }
            }
            SupervisorMsg::AddDistrict(index) => {
                if index < DISTRICT_COUNT
                    && self.args.district.is_none_or(|district| district == index)
                {
                    self.start_district(ctx, index);
                    self.status();
                }
            }
            SupervisorMsg::RemoveDistrict(index) => {
                if (self.args.district.is_some() || self.districts.len() > 1)
                    && self.args.district.is_none_or(|district| district == index)
                {
                    self.stop_district(ctx, index);
                    self.status();
                }
            }
            SupervisorMsg::DistrictReady {
                district,
                junctions,
            } => {
                if let Some(actor) = self.districts.get(&district).cloned() {
                    self.junctions.extend(junctions);
                    self.publish_directory();
                    actor.tell(DistrictMsg::SetTarget(self.per_district), None);
                }
            }
            SupervisorMsg::Fault { id, fault } => self.broadcast(&DistrictMsg::Fault { id, fault }),
            SupervisorMsg::Inspect { id } => {
                // The gateway is the reply address; it forwards the answer to whoever asked.
                let req = InspectReq {
                    reply: self.args.gateway.clone(),
                };
                self.broadcast(&DistrictMsg::Inspect { id, req });
            }
        }
    }
}
