//! Visualization gateway: merges per-district snapshots into frames for the browser layer.
//!
//! The web server only ever reads the latest frame from a watch channel. It has no access to
//! actor state and cannot influence the simulation except by sending supervisor messages.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use picard::actors::{Actor, ActorFactoryArgs, Context, Sender, Timer};
use tokio::sync::watch;

use crate::{
    metrics::Metrics,
    msg::{DistrictSnapshot, GatewayMsg},
};

pub const FRAME_PERIOD: Duration = Duration::from_millis(100);

#[derive(Default)]
pub struct Frame {
    pub seq: u64,
    pub districts: Vec<Arc<DistrictSnapshot>>,
    pub paused: bool,
    pub target: usize,
    pub inspect: Option<(String, String)>,
}

#[derive(Clone)]
pub struct GatewayArgs {
    pub tx: watch::Sender<Arc<Frame>>,
    pub metrics: Arc<Metrics>,
}

pub struct Gateway {
    args: GatewayArgs,
    snapshots: BTreeMap<usize, Arc<DistrictSnapshot>>,
    generations: BTreeMap<usize, u64>,
    paused: bool,
    target: usize,
    inspect: Option<(String, String)>,
    seq: u64,
}

impl ActorFactoryArgs<GatewayArgs> for Gateway {
    fn create_args(args: GatewayArgs) -> Self {
        Self {
            args,
            snapshots: BTreeMap::new(),
            generations: BTreeMap::new(),
            paused: false,
            target: 0,
            inspect: None,
            seq: 0,
        }
    }
}

impl Actor for Gateway {
    type Msg = GatewayMsg;

    fn pre_start(&mut self, ctx: &Context<Self::Msg>) {
        ctx.schedule(
            FRAME_PERIOD,
            FRAME_PERIOD,
            ctx.myself(),
            None,
            GatewayMsg::Flush,
        );
    }

    fn recv(&mut self, _: &Context<Self::Msg>, msg: Self::Msg, _: Sender) {
        self.handle(msg);
    }
}

impl Gateway {
    fn handle(&mut self, msg: GatewayMsg) {
        self.args.metrics.inc(&self.args.metrics.processed);
        match msg {
            GatewayMsg::Flush => {
                self.seq += 1;
                let frame = Frame {
                    seq: self.seq,
                    districts: self.snapshots.values().cloned().collect(),
                    paused: self.paused,
                    target: self.target,
                    inspect: self.inspect.clone(),
                };
                let _ = self.args.tx.send(Arc::new(frame));
            }
            GatewayMsg::Snapshot(snapshot) => {
                if self.generations.get(&snapshot.district) == Some(&snapshot.generation) {
                    self.snapshots.insert(snapshot.district, snapshot);
                }
            }
            GatewayMsg::DistrictStarted {
                district,
                generation,
            } => {
                self.generations.insert(district, generation);
                self.snapshots.remove(&district);
            }
            GatewayMsg::DistrictGone(index) => {
                self.generations.remove(&index);
                self.snapshots.remove(&index);
            }
            GatewayMsg::InspectReply { id, json } => self.inspect = Some((id, json)),
            GatewayMsg::Status { paused, target } => {
                self.paused = paused;
                self.target = target;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn late_snapshots_cannot_restore_removed_or_replaced_districts() {
        let (tx, frames) = watch::channel(Arc::new(Frame::default()));
        let mut gateway = Gateway::create_args(GatewayArgs {
            tx,
            metrics: Arc::new(Metrics::default()),
        });
        let old = Arc::new(DistrictSnapshot {
            district: 4,
            generation: 1,
            ..DistrictSnapshot::default()
        });
        gateway.handle(GatewayMsg::DistrictStarted {
            district: 4,
            generation: 1,
        });
        gateway.handle(GatewayMsg::Snapshot(old.clone()));
        gateway.handle(GatewayMsg::Flush);
        assert_eq!(frames.borrow().districts.len(), 1);
        gateway.handle(GatewayMsg::DistrictGone(4));
        gateway.handle(GatewayMsg::Snapshot(old.clone()));
        gateway.handle(GatewayMsg::Flush);
        assert!(frames.borrow().districts.is_empty());
        gateway.handle(GatewayMsg::DistrictStarted {
            district: 4,
            generation: 2,
        });
        let new = Arc::new(DistrictSnapshot {
            district: 4,
            generation: 2,
            ..DistrictSnapshot::default()
        });
        gateway.handle(GatewayMsg::Snapshot(new));
        gateway.handle(GatewayMsg::Snapshot(old));
        gateway.handle(GatewayMsg::Flush);
        assert_eq!(frames.borrow().districts.len(), 1);
        assert_eq!(frames.borrow().districts[0].generation, 2);
    }
}
