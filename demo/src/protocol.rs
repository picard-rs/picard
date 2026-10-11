//! Transport-independent half of the web layer.
//!
//! A [`Bridge`] owns the simulation, samples its metrics, turns browser commands (JSON) into
//! supervisor messages and encodes the gateway's frames as JSON. The native server pushes the
//! result over a WebSocket; the WebAssembly build hands it to the page through a worker.

use std::fmt::Write;

use serde_json::Value;

use crate::{
    city::{Axis, BLOCK, DEFAULT_ACTIVE, DISTRICT_COUNT, GRID_DISTRICTS, district_name},
    gateway::Frame,
    msg::{Fault, SupervisorMsg, state},
    sampler::Sampler,
    sim::Sim,
};

const INSPECT_EVERY: u32 = 3;
const SAMPLE_EVERY: u32 = 10;

pub struct Bridge {
    sim: Sim,
    sampler: Sampler,
    sample_json: String,
    selected: Option<String>,
    n: u32,
}

pub fn hello() -> String {
    let names: Vec<String> = (0..DISTRICT_COUNT)
        .map(|d| format!("\"{}\"", district_name(d)))
        .collect();
    format!(
        "{{\"hello\":{{\"block\":{BLOCK},\"grid\":{GRID_DISTRICTS},\"names\":[{}],\"default\":{:?}}}}}",
        names.join(","),
        DEFAULT_ACTIVE
    )
}

impl Bridge {
    pub fn new(sim: Sim) -> Self {
        let mut sampler = Sampler::new(sim.metrics.clone());
        let sample_json = sampler.sample().to_json();
        Self {
            sim,
            sampler,
            sample_json,
            selected: None,
            n: 0,
        }
    }

    pub fn sim(&self) -> &Sim {
        &self.sim
    }

    /// Produces the next frame as JSON. Call it once per frame period.
    pub fn step(&mut self) -> String {
        self.n = self.n.wrapping_add(1);
        if self.n.is_multiple_of(SAMPLE_EVERY) {
            self.sample_json = self.sampler.sample().to_json();
        }
        if let Some(id) = &self.selected
            && self.n.is_multiple_of(INSPECT_EVERY)
        {
            self.sim.tell(SupervisorMsg::Inspect { id: id.clone() });
        }
        let frame = self.sim.frames.borrow().clone();
        encode(&frame, &self.sample_json, self.selected.as_deref())
    }

    pub fn command(&mut self, text: &str) {
        let Ok(cmd) = serde_json::from_str::<Value>(text) else {
            return;
        };
        let num = |k: &str| {
            cmd.get(k)
                .and_then(Value::as_u64)
                .and_then(|n| usize::try_from(n).ok())
        };
        let district = || num("d").filter(|d| *d < DISTRICT_COUNT);
        let sim = &self.sim;
        match cmd.get("cmd").and_then(Value::as_str) {
            Some("set_vehicles") => {
                if let Some(n) = num("n") {
                    sim.tell(SupervisorMsg::SetVehicles(n.min(25_000)));
                }
            }
            Some("pause") => sim.tell(SupervisorMsg::Pause(
                cmd.get("on").and_then(Value::as_bool).unwrap_or(true),
            )),
            Some("reset") => sim.tell(SupervisorMsg::Reset),
            Some("surge") => {
                if let Some(d) = district() {
                    sim.tell(SupervisorMsg::Surge(d));
                }
            }
            Some("add_district") => {
                if let Some(d) = district() {
                    sim.tell(SupervisorMsg::AddDistrict(d));
                }
            }
            Some("remove_district") => {
                if let Some(d) = district() {
                    sim.tell(SupervisorMsg::RemoveDistrict(d));
                }
            }
            Some("kill") => {
                if let Some(id) = cmd.get("id").and_then(Value::as_str) {
                    let fault = match cmd.get("mode").and_then(Value::as_str) {
                        Some("crash") => Fault::Crash,
                        _ => Fault::Terminate,
                    };
                    sim.tell(SupervisorMsg::Fault {
                        id: id.to_string(),
                        fault,
                    });
                }
            }
            Some("select") => {
                let id = cmd.get("id").and_then(Value::as_str).map(str::to_string);
                if let Some(id) = &id {
                    sim.tell(SupervisorMsg::Inspect { id: id.clone() });
                }
                self.selected = id;
            }
            _ => {}
        }
    }
}

pub fn encode(frame: &Frame, sample: &str, selected: Option<&str>) -> String {
    let vehicles: usize = frame.districts.iter().map(|d| d.vehicles.len()).sum();
    let mut out = String::with_capacity(64 + vehicles * 40);
    let _ = write!(
        out,
        "{{\"seq\":{},\"paused\":{},\"target\":{},\"m\":{sample},\"districts\":[",
        frame.seq, frame.paused, frame.target
    );
    for (i, d) in frame.districts.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let _ = write!(
            out,
            "{{\"d\":{},\"target\":{},\"handled\":{},\"v\":[",
            d.district, d.target, d.handled
        );
        for (j, v) in d.vehicles.iter().enumerate() {
            if j > 0 {
                out.push(',');
            }
            let _ = write!(
                out,
                "[{},{},{},{},{},{},{}]",
                v.id,
                v.from.x,
                v.from.y,
                v.to.x,
                v.to.y,
                (v.s.clamp(0.0, 1.0) * 1000.0) as u32,
                v.state.min(state::PARKED)
            );
        }
        out.push_str("],\"j\":[");
        for (j, v) in d.junctions.iter().enumerate() {
            if j > 0 {
                out.push(',');
            }
            let green = match v.signal.green {
                None => 0,
                Some(Axis::Ns) => 1,
                Some(Axis::Ew) => 2,
            };
            let _ = write!(
                out,
                "[{},{},{green},{},{},{},{},{}]",
                v.node.x,
                v.node.y,
                u8::from(v.signal.yellow),
                v.queued[0],
                v.queued[1],
                v.inside,
                u8::from(v.degraded)
            );
        }
        out.push_str("]}");
    }
    out.push(']');
    if let (Some(sel), Some((id, json))) = (selected, &frame.inspect)
        && sel == id
    {
        let _ = write!(out, ",\"inspect\":{json}");
    }
    out.push('}');
    out
}
