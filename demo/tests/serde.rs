#![cfg(feature = "serde")]

use living_city::{
    city::{Axis, Node},
    msg::{
        DistrictMsg, DistrictSnapshot, Fault, GatewayMsg, JunctionMsg, JunctionView, LightMsg,
        Signal, SupervisorMsg, Tick, VehicleMsg, VehicleView,
    },
    observer::ObserverMsg,
};
use picard::{Message, platform::Instant};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

fn roundtrip<T: Serialize + DeserializeOwned>(message: &T) -> Value {
    let value = serde_json::to_value(message).unwrap();
    let decoded: T = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), value);
    value
}

#[test]
fn all_actor_message_types_satisfy_picard_message() {
    fn assert_message<T: Message>() {}
    assert_message::<VehicleMsg>();
    assert_message::<JunctionMsg>();
    assert_message::<LightMsg>();
    assert_message::<DistrictMsg>();
    assert_message::<SupervisorMsg>();
    assert_message::<GatewayMsg>();
    assert_message::<ObserverMsg>();
}

#[test]
fn controls_and_simulation_data_roundtrip() {
    assert_eq!(
        roundtrip(&SupervisorMsg::Pause(true)),
        json!({"Pause": true})
    );
    assert_eq!(
        roundtrip(&SupervisorMsg::SetVehicles(42)),
        json!({"SetVehicles": 42})
    );
    roundtrip(&SupervisorMsg::Fault {
        id: "v-1".into(),
        fault: Fault::Crash,
    });
    roundtrip(&VehicleMsg::EntryGranted {
        node: Node::new(1, 2),
    });
    roundtrip(&JunctionMsg::TrafficSignalChanged(Signal {
        green: Some(Axis::Ns),
        yellow: false,
    }));
    roundtrip(&LightMsg::Demand { ns: 5, ew: 3 });
    roundtrip(&DistrictMsg::SetTarget(12));
    roundtrip(&GatewayMsg::Snapshot(std::sync::Arc::new(
        DistrictSnapshot {
            district: 1,
            generation: 1,
            vehicles: vec![VehicleView {
                id: 5,
                from: Node::new(1, 2),
                to: Node::new(1, 3),
                s: 0.5,
                state: 0,
            }],
            junctions: vec![JunctionView {
                node: Node::new(1, 2),
                signal: Signal::ALL_RED,
                queued: [1, 2],
                inside: 0,
                degraded: false,
            }],
            target: 12,
            handled: 7,
        },
    )));
}

#[test]
fn json_ticks_restore_local_time_without_a_wire_timestamp() {
    let before = Instant::now();
    let tick: Tick = serde_json::from_value(json!({"dt": 0.05, "report": true})).unwrap();
    assert!(tick.sent >= before && tick.sent <= Instant::now());
    assert_eq!(
        roundtrip(&VehicleMsg::Tick(tick)),
        json!({"Tick": {"dt": tick.dt, "report": true}})
    );
}

#[test]
fn internal_reference_bearing_variants_are_rejected() {
    for value in [json!({"Directory": {}}), json!({"Inspect": {}})] {
        assert!(serde_json::from_value::<VehicleMsg>(value).is_err());
    }
    for value in [
        json!({"RequestIntersectionEntry": {"vehicle": "/user/v-1", "id": 1, "axis": "Ns"}}),
        json!({"Inspect": {}}),
        json!({"InspectLight": {}}),
    ] {
        assert!(serde_json::from_value::<JunctionMsg>(value).is_err());
    }
    assert!(serde_json::from_value::<LightMsg>(json!({"Inspect": {}})).is_err());
    for value in [
        json!({"Directory": {}}),
        json!({"NeedDirectory": "/user/v-1"}),
        json!({"Inspect": {}}),
        json!({"SystemEvent": {}}),
    ] {
        assert!(serde_json::from_value::<DistrictMsg>(value).is_err());
    }
    assert!(serde_json::from_value::<SupervisorMsg>(json!({"DistrictReady": {}})).is_err());
    for value in [json!({"SystemEvent": {}}), json!({"DeadLetter": {}})] {
        assert!(serde_json::from_value::<ObserverMsg>(value).is_err());
    }
}
