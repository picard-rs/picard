use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Sender as ChannelSender},
    },
    time::{Duration, Instant},
};

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use futures::executor::block_on;
use picard::actors::*;

#[derive(Clone, Debug)]
enum BenchMessage {
    Work,
    Barrier(ChannelSender<()>),
    RoundTrip(ChannelSender<()>),
}

#[derive(Default)]
struct BenchActor;

impl Actor for BenchActor {
    type Msg = BenchMessage;

    fn recv(&mut self, _: &Context<Self::Msg>, msg: Self::Msg, _: Sender) {
        match msg {
            BenchMessage::Work => black_box(()),
            BenchMessage::Barrier(done) | BenchMessage::RoundTrip(done) => {
                let _ = done.send(());
            }
        }
    }
}

fn benchmark_system() -> ActorSystem {
    let cfg = PicardConfig {
        debug: false,
        log: LogConfig {
            level: "error".into(),
            ..Default::default()
        },
        dispatcher: DispatcherConfig {
            pool_size: 2,
            ..Default::default()
        },
        ..Default::default()
    };

    ActorSystem::with_config("bench", cfg).unwrap()
}

fn stop_and_wait(system: &ActorSystem, actors: Vec<ActorRef<BenchMessage>>) {
    let names: HashSet<String> = actors
        .iter()
        .map(|actor| actor.name().to_string())
        .collect();
    for actor in actors {
        system.stop(&actor);
    }

    let deadline = Instant::now() + Duration::from_secs(30);
    while system
        .user_root()
        .children()
        .any(|child| names.contains(child.name()))
    {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for benchmark actors to stop"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn bench_runtime(c: &mut Criterion) {
    let system = benchmark_system();
    let typed_actor = system.actor_of::<BenchActor>("typed-messages").unwrap();
    let untyped_actor = system.actor_of::<BenchActor>("untyped-messages").unwrap();
    let untyped_actor: BasicActorRef = untyped_actor.into();
    let (typed_done_tx, typed_done_rx) = mpsc::channel();
    let (untyped_done_tx, untyped_done_rx) = mpsc::channel();

    let mut messages = c.benchmark_group("actor_message_throughput");
    messages.throughput(Throughput::Elements(1));
    messages.bench_function("typed_tell", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                typed_actor.tell(BenchMessage::Work, None);
            }
            typed_actor.tell(BenchMessage::Barrier(typed_done_tx.clone()), None);
            typed_done_rx
                .recv()
                .expect("actor stopped before processing typed messages");
            start.elapsed()
        });
    });
    messages.bench_function("untyped_try_tell", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                untyped_actor
                    .try_tell(BenchMessage::Work, None)
                    .expect("actor rejected benchmark message");
            }
            untyped_actor
                .try_tell(BenchMessage::Barrier(untyped_done_tx.clone()), None)
                .expect("actor rejected benchmark barrier");
            untyped_done_rx
                .recv()
                .expect("actor stopped before processing untyped messages");
            start.elapsed()
        });
    });
    messages.finish();

    let (round_trip_tx, round_trip_rx) = mpsc::channel();
    c.bench_function("actor_round_trip", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                typed_actor.tell(BenchMessage::RoundTrip(round_trip_tx.clone()), None);
                round_trip_rx
                    .recv()
                    .expect("actor stopped before replying to round trip");
            }
            start.elapsed()
        });
    });

    let next_actor_id = AtomicUsize::new(0);
    c.bench_function("actor_creation", |b| {
        b.iter_custom(|iters| {
            let mut actors = Vec::new();
            let mut measured = Duration::ZERO;
            for _ in 0..iters {
                let id = next_actor_id.fetch_add(1, Ordering::Relaxed);
                let name = format!("created-{id}");
                let start = Instant::now();
                actors.push(system.actor_of::<BenchActor>(&name).unwrap());
                measured += start.elapsed();
            }
            stop_and_wait(&system, actors);
            measured
        });
    });

    block_on(system.shutdown()).expect("benchmark actor system failed to shut down");
}

criterion_group!(benches, bench_runtime);
criterion_main!(benches);
