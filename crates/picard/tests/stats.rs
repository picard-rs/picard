#![cfg(feature = "stats")]

use picard::actors::*;
use std::{
    sync::mpsc::{self, Sender as ChannelSender},
    time::{Duration, Instant},
};

struct Echo(ChannelSender<u32>);

impl ActorFactoryArgs<ChannelSender<u32>> for Echo {
    fn create_args(tx: ChannelSender<u32>) -> Self {
        Self(tx)
    }
}

impl Actor for Echo {
    type Msg = u32;

    fn recv(&mut self, _: &Context<Self::Msg>, msg: Self::Msg, _: Sender) {
        assert!(msg != 0, "zero is fatal");
        let _ = self.0.send(msg);
    }
}

fn wait_for(sys: &ActorSystem, cond: impl Fn(&RuntimeStats) -> bool) -> RuntimeStats {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let s = sys.stats();
        if cond(&s) {
            return s;
        }
        assert!(Instant::now() < deadline, "stats condition not met: {s:?}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn tracks_actor_lifecycle_and_messages() {
    let sys = ActorSystem::new().unwrap();
    let before = sys.stats();
    assert!(before.actors_created > 0, "system actors are counted");

    let (tx, rx) = mpsc::channel();
    let echo = sys.actor_of_args::<Echo, _>("echo", tx).unwrap();
    for i in 1..=10_u32 {
        echo.tell(i, None);
    }
    for _ in 0..10 {
        rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }

    let after = sys.stats();
    assert_eq!(after.actors_created, before.actors_created + 1);
    assert!(after.messages_sent >= before.messages_sent + 10);
    assert!(after.system_messages_sent > before.system_messages_sent);

    // A panic triggers the default restart strategy.
    echo.tell(0_u32, None);
    wait_for(&sys, |s| s.actors_restarted == before.actors_restarted + 1);
    echo.tell(7_u32, None);
    assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap(), 7);

    sys.stop(echo.clone());
    let stopped = wait_for(&sys, |s| s.actors_terminated > before.actors_terminated);
    assert_eq!(stopped.live_actors(), before.live_actors());

    // Sending to a stopped actor fails and is counted.
    echo.tell(1_u32, None);
    wait_for(&sys, |s| s.messages_failed > before.messages_failed);
}
