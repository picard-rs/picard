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

fn visitors_from(frame: &living_city::gateway::Frame, home: u64, host: usize) -> usize {
    frame
        .districts
        .iter()
        .filter(|snapshot| snapshot.district == host)
        .flat_map(|snapshot| &snapshot.vehicles)
        .filter(|vehicle| (vehicle.id >> 32) & 0xff == home)
        .count()
}

#[tokio::test]
async fn vehicles_drive_between_districts_of_different_processes() {
    let address_zero = available_address();
    let address_one = available_address();
    let peers_zero = vec![PeerSpec {
        district: 1,
        endpoint: format!("ws://{address_one}/remoting"),
    }];
    let peers_one = vec![PeerSpec {
        district: 0,
        endpoint: format!("ws://{address_zero}/remoting"),
    }];
    let first = thread::spawn(move || {
        Sim::start_remote(
            200,
            0,
            address_zero,
            "test-shared-token".into(),
            &peers_zero,
        )
    });
    let second = thread::spawn(move || {
        Sim::start_remote(200, 1, address_one, "test-shared-token".into(), &peers_one)
    });
    let first = first.join().unwrap().unwrap();
    let second = second.join().unwrap().unwrap();

    let deadline = Instant::now() + Duration::from_secs(60);
    let (mut into_zero, mut into_one) = (0, 0);
    while into_zero == 0 || into_one == 0 {
        {
            let frame = first.frames.borrow();
            into_zero = into_zero.max(visitors_from(&frame, 1, 0));
            into_one = into_one.max(visitors_from(&frame, 0, 1));
        }
        assert!(
            Instant::now() < deadline,
            "no vehicle crossed the process boundary: into 0 = {into_zero}, into 1 = {into_one}"
        );
        thread::sleep(Duration::from_millis(50));
    }

    first.system.shutdown().await.unwrap();
    second.system.shutdown().await.unwrap();
}
