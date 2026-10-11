//! End-to-end tests: real actors, real scheduler, observed through the gateway frames and the
//! framework-fed metrics.

use std::{
    sync::{Arc, Once},
    time::{Duration, Instant},
};

use living_city::{
    gateway::Frame,
    metrics::Metrics,
    msg::{Fault, SupervisorMsg},
    sim::Sim,
};

fn quiet_panics() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let default = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let msg = info
                .payload()
                .downcast_ref::<String>()
                .cloned()
                .unwrap_or_default();
            if !msg.starts_with("fault injected") {
                default(info);
            }
        }));
    });
}

fn start(vehicles: usize) -> Sim {
    quiet_panics();
    Sim::start(vehicles).expect("start simulation")
}

fn frame(sim: &Sim) -> Arc<Frame> {
    sim.frames.borrow().clone()
}

fn vehicle_count(sim: &Sim) -> usize {
    frame(sim).districts.iter().map(|d| d.vehicles.len()).sum()
}

fn wait_until(what: &str, secs: u64, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        if cond() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("timed out waiting for: {what}");
}

fn get(c: &std::sync::atomic::AtomicU64) -> u64 {
    Metrics::get(c)
}

#[test]
fn vehicles_complete_trips_without_safety_violations() {
    let sim = start(200);
    wait_until("200 vehicles visible", 20, || vehicle_count(&sim) == 200);
    wait_until("trips completed", 40, || get(&sim.metrics.trips) >= 20);
    assert_eq!(
        get(&sim.metrics.violations),
        0,
        "two conflicting axes shared a junction"
    );
    assert!(get(&sim.metrics.granted) > 0);
    assert!(get(&sim.metrics.sent) > 0 && get(&sim.metrics.processed) > 0);
    assert!(sim.metrics.alive_actors() >= 200 + 4 * 36);
}

#[test]
fn killing_a_vehicle_does_not_disturb_the_others() {
    let sim = start(120);
    wait_until("vehicles visible", 20, || vehicle_count(&sim) == 120);
    let victim = frame(&sim).districts[0].vehicles[0].id;
    let terminated_before = get(&sim.metrics.terminated);

    sim.tell(SupervisorMsg::Fault {
        id: format!("v-{victim}"),
        fault: Fault::Terminate,
    });

    wait_until("victim gone", 20, || vehicle_count(&sim) == 119);
    assert!(get(&sim.metrics.terminated) > terminated_before);
    let trips = get(&sim.metrics.trips);
    wait_until("others keep driving", 40, || {
        get(&sim.metrics.trips) > trips + 5
    });
    assert_eq!(
        vehicle_count(&sim),
        119,
        "a terminated vehicle is not restarted"
    );
}

#[test]
fn a_crashing_vehicle_is_restarted_by_its_supervisor() {
    let sim = start(80);
    wait_until("vehicles visible", 20, || vehicle_count(&sim) == 80);
    let victim = frame(&sim).districts[1].vehicles[0].id;
    let restarted_before = get(&sim.metrics.restarted);

    sim.tell(SupervisorMsg::Fault {
        id: format!("v-{victim}"),
        fault: Fault::Crash,
    });

    wait_until("restart event", 20, || {
        get(&sim.metrics.restarted) > restarted_before
    });
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(
        vehicle_count(&sim),
        80,
        "the restarted vehicle keeps its identity"
    );
}

fn degraded(sim: &Sim, x: i16, y: i16) -> Option<bool> {
    frame(sim)
        .districts
        .iter()
        .flat_map(|d| d.junctions.iter())
        .find(|j| j.node.x == x && j.node.y == y)
        .map(|j| j.degraded)
}

#[test]
fn a_stopped_light_degrades_its_junction_and_is_respawned() {
    let sim = start(40);
    wait_until("junction reports", 20, || degraded(&sim, 2, 2).is_some());

    sim.tell(SupervisorMsg::Fault {
        id: "l-2-2".into(),
        fault: Fault::Terminate,
    });
    wait_until("junction notices missing heartbeats", 20, || {
        degraded(&sim, 2, 2) == Some(true)
    });
    wait_until("junction respawns its light", 40, || {
        degraded(&sim, 2, 2) == Some(false)
    });
    assert_eq!(get(&sim.metrics.violations), 0);
}

#[test]
fn a_crashing_junction_recovers_and_traffic_continues() {
    let sim = start(150);
    wait_until("junction reports", 20, || degraded(&sim, 3, 3).is_some());
    let restarted_before = get(&sim.metrics.restarted);

    sim.tell(SupervisorMsg::Fault {
        id: "i-3-3".into(),
        fault: Fault::Crash,
    });
    wait_until("junction restarted", 20, || {
        get(&sim.metrics.restarted) > restarted_before
    });

    let trips = get(&sim.metrics.trips);
    wait_until("traffic keeps flowing", 40, || {
        get(&sim.metrics.trips) > trips + 10
    });
    assert_eq!(get(&sim.metrics.violations), 0);
}

#[test]
fn vehicles_route_around_a_terminated_junction() {
    let sim = start(200);
    wait_until("junction reports", 20, || degraded(&sim, 3, 3).is_some());

    sim.tell(SupervisorMsg::Fault {
        id: "i-3-3".into(),
        fault: Fault::Terminate,
    });
    wait_until("junction disappears", 20, || degraded(&sim, 3, 3).is_none());

    let trips = get(&sim.metrics.trips);
    wait_until("vehicles keep completing trips", 60, || {
        get(&sim.metrics.trips) > trips + 10
    });
    assert_eq!(vehicle_count(&sim), 200);
}

#[test]
fn districts_can_be_added_and_removed_at_runtime() {
    let sim = start(200);
    wait_until("vehicles visible", 20, || vehicle_count(&sim) == 200);
    assert_eq!(frame(&sim).districts.len(), 4);
    let alive = sim.metrics.alive_actors();

    sim.tell(SupervisorMsg::RemoveDistrict(4));
    wait_until("district gone", 20, || {
        frame(&sim).districts.len() == 3 && vehicle_count(&sim) == 150
    });
    wait_until("actors cleaned up", 20, || {
        sim.metrics.alive_actors() + 50 < alive
    });

    sim.tell(SupervisorMsg::AddDistrict(8));
    wait_until("district added", 20, || {
        frame(&sim).districts.len() == 4 && vehicle_count(&sim) == 200
    });
}

#[test]
fn vehicle_target_scales_up_and_down() {
    let sim = start(100);
    wait_until("100 vehicles", 20, || vehicle_count(&sim) == 100);
    sim.tell(SupervisorMsg::SetVehicles(1000));
    wait_until("1000 vehicles", 30, || vehicle_count(&sim) == 1000);
    sim.tell(SupervisorMsg::SetVehicles(100));
    wait_until("back to 100", 30, || vehicle_count(&sim) == 100);
}

#[test]
fn a_surge_only_adds_vehicles_to_the_chosen_district() {
    let sim = start(100);
    wait_until("100 vehicles", 20, || vehicle_count(&sim) == 100);
    sim.tell(SupervisorMsg::Surge(0));
    wait_until("surge spawned", 30, || vehicle_count(&sim) == 900);
    let f = frame(&sim);
    let others: usize = f
        .districts
        .iter()
        .filter(|d| d.district != 0)
        .map(|d| d.vehicles.len())
        .sum();
    assert_eq!(others, 75, "other districts are untouched");
}

#[test]
fn inspector_returns_state_and_a_message_timeline() {
    let sim = start(60);
    wait_until("vehicles visible", 20, || vehicle_count(&sim) == 60);
    let id = format!("v-{}", frame(&sim).districts[0].vehicles[0].id);

    let mut json = None;
    wait_until("inspector reply with traffic", 30, || {
        sim.tell(SupervisorMsg::Inspect { id: id.clone() });
        std::thread::sleep(Duration::from_millis(250));
        let reply = frame(&sim).inspect.clone();
        json = reply.filter(|(rid, _)| *rid == id).map(|(_, j)| j);
        json.as_ref()
            .is_some_and(|j| j.contains("RequestIntersectionEntry"))
    });
    let body: serde_json::Value = serde_json::from_str(&json.unwrap()).unwrap();
    assert_eq!(body["type"], "vehicle");
    assert!(body["trace"]["sent"].as_u64().unwrap() > 0);
}

#[test]
fn pause_stops_the_clock() {
    let sim = start(50);
    wait_until("clock running", 20, || get(&sim.metrics.sim_ticks) > 10);
    sim.tell(SupervisorMsg::Pause(true));
    std::thread::sleep(Duration::from_millis(300));
    let ticks = get(&sim.metrics.sim_ticks);
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(get(&sim.metrics.sim_ticks), ticks);
    sim.tell(SupervisorMsg::Pause(false));
    wait_until("clock resumes", 10, || get(&sim.metrics.sim_ticks) > ticks);
}
