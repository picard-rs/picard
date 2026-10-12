//! Visualization gateway: merges per-district snapshots into frames for the browser layer.
//!
//! The web server only ever reads the latest frame from a watch channel. It has no access to
//! actor state and cannot influence the simulation except by sending supervisor messages.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

#[cfg(feature = "remoting")]
use picard::platform::Instant;

use picard::actors::{Actor, ActorFactoryArgs, Context, Sender, Timer};
use tokio::sync::watch;

use crate::{
    metrics::Metrics,
    msg::{DistrictSnapshot, GatewayMsg},
    sampler::Sample,
};
#[cfg(feature = "remoting")]
use crate::{
    msg::RemoteUpdate,
    remoting::{GATEWAY_PATH, PeerLink},
    sampler::Sampler,
};

pub const FRAME_PERIOD: Duration = Duration::from_millis(100);
#[cfg(feature = "remoting")]
const HEARTBEAT_PERIOD: Duration = Duration::from_secs(1);
/// A remote district that has been silent this long is treated as dead.
#[cfg(feature = "remoting")]
const PEER_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Default)]
pub struct Frame {
    pub seq: u64,
    pub districts: Vec<Arc<DistrictSnapshot>>,
    pub paused: bool,
    pub target: usize,
    pub inspect: Option<(String, String)>,
    pub remote_samples: Vec<Sample>,
}

#[derive(Clone)]
pub struct GatewayArgs {
    pub tx: watch::Sender<Arc<Frame>>,
    pub metrics: Arc<Metrics>,
    #[cfg(feature = "remoting")]
    pub district: Option<usize>,
}

pub struct Gateway {
    args: GatewayArgs,
    snapshots: BTreeMap<usize, Arc<DistrictSnapshot>>,
    generations: BTreeMap<usize, u64>,
    paused: bool,
    target: usize,
    inspect: Option<(String, String)>,
    seq: u64,
    #[cfg(feature = "remoting")]
    remote_peers: Vec<PeerLink>,
    #[cfg(feature = "remoting")]
    local_paused: bool,
    #[cfg(feature = "remoting")]
    local_target: usize,
    #[cfg(feature = "remoting")]
    remote_status: BTreeMap<usize, (bool, usize)>,
    #[cfg(feature = "remoting")]
    remote_samples: BTreeMap<usize, Sample>,
    #[cfg(feature = "remoting")]
    last_seen: BTreeMap<usize, Instant>,
    #[cfg(feature = "remoting")]
    sampler: Option<Sampler>,
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
            #[cfg(feature = "remoting")]
            remote_peers: Vec::new(),
            #[cfg(feature = "remoting")]
            local_paused: false,
            #[cfg(feature = "remoting")]
            local_target: 0,
            #[cfg(feature = "remoting")]
            remote_status: BTreeMap::new(),
            #[cfg(feature = "remoting")]
            remote_samples: BTreeMap::new(),
            #[cfg(feature = "remoting")]
            last_seen: BTreeMap::new(),
            #[cfg(feature = "remoting")]
            sampler: None,
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
        #[cfg(feature = "remoting")]
        if self.args.district.is_some() {
            self.sampler = Some(Sampler::new(self.args.metrics.clone()));
            ctx.schedule(
                HEARTBEAT_PERIOD,
                HEARTBEAT_PERIOD,
                ctx.myself(),
                None,
                GatewayMsg::Heartbeat,
            );
        }
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
                #[cfg(feature = "remoting")]
                self.drop_silent_peers();
                self.seq += 1;
                let frame = Frame {
                    seq: self.seq,
                    districts: self.snapshots.values().cloned().collect(),
                    paused: self.paused,
                    target: self.target,
                    inspect: self.inspect.clone(),
                    remote_samples: self.collect_remote_samples(),
                };
                let _ = self.args.tx.send(Arc::new(frame));
            }
            GatewayMsg::Snapshot(snapshot) => {
                #[cfg(feature = "remoting")]
                self.relay(RemoteUpdate::Snapshot(snapshot.clone()));
                self.apply_snapshot(snapshot);
            }
            GatewayMsg::DistrictStarted {
                district,
                generation,
            } => {
                #[cfg(feature = "remoting")]
                self.relay(RemoteUpdate::DistrictStarted {
                    district,
                    generation,
                });
                self.apply_district_started(district, generation);
            }
            GatewayMsg::DistrictGone(index) => {
                #[cfg(feature = "remoting")]
                self.relay(RemoteUpdate::DistrictGone(index));
                self.apply_district_gone(index);
            }
            GatewayMsg::InspectReply { id, json } => {
                #[cfg(feature = "remoting")]
                self.relay(RemoteUpdate::InspectReply {
                    id: id.clone(),
                    json: json.clone(),
                });
                self.inspect = Some((id, json));
            }
            GatewayMsg::Status { paused, target } => {
                #[cfg(feature = "remoting")]
                {
                    self.local_paused = paused;
                    self.local_target = target;
                    self.recompute_status();
                    if let Some(district) = self.args.district {
                        self.relay(RemoteUpdate::Status {
                            district,
                            paused,
                            target,
                        });
                    }
                }
                #[cfg(not(feature = "remoting"))]
                {
                    self.paused = paused;
                    self.target = target;
                }
            }
            #[cfg(feature = "remoting")]
            GatewayMsg::SetRemotePeers(peers) => {
                self.remote_peers = peers;
                if let Some(district) = self.args.district {
                    self.relay(RemoteUpdate::Status {
                        district,
                        paused: self.local_paused,
                        target: self.local_target,
                    });
                }
            }
            #[cfg(feature = "remoting")]
            GatewayMsg::Heartbeat => {
                if let (Some(district), Some(sampler)) = (self.args.district, &mut self.sampler) {
                    let sample = sampler.sample();
                    self.relay(RemoteUpdate::Metrics { district, sample });
                }
            }
            #[cfg(feature = "remoting")]
            GatewayMsg::RemoteUpdate(update) => self.apply_remote_update(update),
        }
    }
}

impl Gateway {
    fn apply_snapshot(&mut self, snapshot: Arc<DistrictSnapshot>) {
        if self.generations.get(&snapshot.district) == Some(&snapshot.generation) {
            self.snapshots.insert(snapshot.district, snapshot);
        }
    }

    fn apply_district_started(&mut self, district: usize, generation: u64) {
        self.generations.insert(district, generation);
        self.snapshots.remove(&district);
    }

    fn apply_district_gone(&mut self, district: usize) {
        self.generations.remove(&district);
        self.snapshots.remove(&district);
    }

    #[cfg(feature = "remoting")]
    fn apply_remote_update(&mut self, update: RemoteUpdate) {
        let sender = match &update {
            RemoteUpdate::Snapshot(snapshot) => Some(snapshot.district),
            RemoteUpdate::Status { district, .. } | RemoteUpdate::Metrics { district, .. } => {
                Some(*district)
            }
            _ => None,
        };
        if let Some(district) = sender {
            self.last_seen.insert(district, Instant::now());
        }
        match update {
            RemoteUpdate::Metrics { district, sample } => {
                self.remote_samples.insert(district, sample);
            }
            RemoteUpdate::Snapshot(snapshot) => {
                self.generations
                    .entry(snapshot.district)
                    .or_insert(snapshot.generation);
                self.apply_snapshot(snapshot);
            }
            RemoteUpdate::DistrictStarted {
                district,
                generation,
            } => self.apply_district_started(district, generation),
            RemoteUpdate::DistrictGone(district) => self.apply_district_gone(district),
            RemoteUpdate::InspectReply { id, json } => self.inspect = Some((id, json)),
            RemoteUpdate::Status {
                district,
                paused,
                target,
            } => {
                self.remote_status.insert(district, (paused, target));
                self.recompute_status();
            }
        }
    }

    #[cfg(feature = "remoting")]
    fn drop_silent_peers(&mut self) {
        let now = Instant::now();
        let silent: Vec<usize> = self
            .last_seen
            .iter()
            .filter(|(_, seen)| now.duration_since(**seen) > PEER_TIMEOUT)
            .map(|(district, _)| *district)
            .collect();
        for district in silent {
            self.last_seen.remove(&district);
            self.remote_samples.remove(&district);
            self.remote_status.remove(&district);
            self.apply_district_gone(district);
        }
        self.recompute_status();
    }

    #[cfg(feature = "remoting")]
    fn collect_remote_samples(&self) -> Vec<Sample> {
        self.remote_samples.values().cloned().collect()
    }

    #[cfg(not(feature = "remoting"))]
    fn collect_remote_samples(&self) -> Vec<Sample> {
        Vec::new()
    }

    #[cfg(feature = "remoting")]
    fn recompute_status(&mut self) {
        self.paused = self.local_paused
            && self.remote_peers.iter().all(|peer| {
                self.remote_status
                    .get(&peer.district)
                    .is_some_and(|(paused, _)| *paused)
            });
        self.target = self.local_target
            + self
                .remote_status
                .values()
                .map(|(_, target)| *target)
                .sum::<usize>();
    }

    #[cfg(feature = "remoting")]
    fn relay(&self, update: RemoteUpdate) {
        let msg = GatewayMsg::RemoteUpdate(update);
        for peer in &self.remote_peers {
            if let Err(err) = peer.send(GATEWAY_PATH, &msg) {
                eprintln!(
                    "cannot queue gateway update for district {}: {err}",
                    peer.district
                );
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
            #[cfg(feature = "remoting")]
            district: None,
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
