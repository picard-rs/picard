#![cfg(feature = "remoting")]

use std::{
    net::{SocketAddr, TcpListener},
    thread,
    time::{Duration, Instant},
};

use living_city::{remoting::PeerSpec, sim::Sim};

fn available_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

#[tokio::test]
async fn remote_district_snapshots_reach_the_ui_gateway() {
    let address_zero = available_address();
    let address_one = available_address();
    let endpoint_zero = format!("ws://{address_zero}/remoting");
    let endpoint_one = format!("ws://{address_one}/remoting");
    let peers_zero = vec![PeerSpec {
        district: 1,
        endpoint: endpoint_one,
    }];
    let peers_one = vec![PeerSpec {
        district: 0,
        endpoint: endpoint_zero,
    }];
    let first = thread::spawn(move || {
        Sim::start_remote(1, 0, address_zero, "test-shared-token".into(), &peers_zero)
    });
    let second = thread::spawn(move || {
        Sim::start_remote(1, 1, address_one, "test-shared-token".into(), &peers_one)
    });
    let first = first.join().unwrap().unwrap();
    let second = second.join().unwrap().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);

    loop {
        let snapshots = first
            .frames
            .borrow()
            .districts
            .iter()
            .map(|snapshot| (snapshot.district, snapshot.target))
            .collect::<Vec<_>>();
        if snapshots.contains(&(0, 1))
            && snapshots.contains(&(1, 1))
            && first.frames.borrow().target == 2
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "remote snapshots did not reach the gateway: {snapshots:?}"
        );
        thread::sleep(Duration::from_millis(20));
    }

    first.tell(living_city::msg::SupervisorMsg::SetVehicles(4));
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (targets, total_target) = {
            let frame = first.frames.borrow();
            let targets = frame
                .districts
                .iter()
                .map(|snapshot| (snapshot.district, snapshot.target))
                .collect::<Vec<_>>();
            (targets, frame.target)
        };
        if targets.contains(&(0, 2)) && targets.contains(&(1, 2)) && total_target == 4 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "remote command did not reach both districts: {targets:?}"
        );
        thread::sleep(Duration::from_millis(20));
    }

    first.tell(living_city::msg::SupervisorMsg::Pause(true));
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if first.frames.borrow().paused {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "pause state was not aggregated across districts"
        );
        thread::sleep(Duration::from_millis(20));
    }

    first.system.shutdown().await.unwrap();
    second.system.shutdown().await.unwrap();
}
