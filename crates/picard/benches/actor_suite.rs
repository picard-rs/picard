use std::{
    hint::black_box,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Receiver, Sender as ChannelSender},
    },
    time::{Duration, Instant},
};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use picard::actors::{
    Actor, ActorFactoryArgs, ActorRef, ActorRefFactory, ActorReference, ActorSystem, BasicActorRef,
    Context, DispatcherConfig, LocalMessage, LogConfig, PicardConfig, Sender, Tell, Timer,
};
use serde::{Deserialize, Serialize};

fn benchmark_system() -> ActorSystem {
    let config = PicardConfig {
        debug: false,
        log: LogConfig {
            level: "error".into(),
            ..Default::default()
        },
        dispatcher: DispatcherConfig {
            pool_size: 4,
            ..Default::default()
        },
        ..Default::default()
    };
    ActorSystem::with_config("actor-suite-bench", config).unwrap()
}

fn stop_actor<Msg: LocalMessage>(system: &ActorSystem, actor: &ActorRef<Msg>) {
    let name = actor.name().to_owned();
    actor.stop();
    let deadline = Instant::now() + Duration::from_secs(10);
    while system
        .user_root()
        .children()
        .any(|child| child.name() == name)
    {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for benchmark actor {name} to stop"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn wait_for_count(rx: &Receiver<()>, count: usize) {
    for _ in 0..count {
        rx.recv_timeout(Duration::from_secs(30))
            .expect("actor benchmark timed out waiting for completion");
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum TreeMessage {
    Grow(u32),
    Result(usize),
}

/// The root reports to `done`; every other node reports to its parent actor.
struct TreeActor {
    done: Option<ChannelSender<usize>>,
    pending: u8,
    total: usize,
}

impl ActorFactoryArgs<Option<ChannelSender<usize>>> for TreeActor {
    fn create_args(done: Option<ChannelSender<usize>>) -> Self {
        Self {
            done,
            pending: 0,
            total: 0,
        }
    }
}

impl TreeActor {
    fn reply(&self, ctx: &Context<TreeMessage>, count: usize) {
        match &self.done {
            Some(done) => {
                let _ = done.send(count);
            }
            None => {
                let _ = ctx
                    .myself()
                    .parent()
                    .try_tell(TreeMessage::Result(count), None);
            }
        }
    }
}

impl Actor for TreeActor {
    type Msg = TreeMessage;

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, _: Sender) {
        match msg {
            TreeMessage::Grow(level) if level <= 1 => {
                self.reply(ctx, 1);
                ctx.myself().stop();
            }
            TreeMessage::Grow(level) => {
                self.pending = 2;
                self.total = 1;
                for name in ["left", "right"] {
                    ctx.actor_of_args::<TreeActor, _>(name, None)
                        .unwrap()
                        .tell(TreeMessage::Grow(level - 1), None);
                }
            }
            TreeMessage::Result(count) => {
                self.total += count;
                self.pending -= 1;
                if self.pending == 0 {
                    self.reply(ctx, self.total);
                    ctx.myself().stop();
                }
            }
        }
    }
}

fn bench_actor_creation(c: &mut Criterion, system: &ActorSystem) {
    let next_id = AtomicUsize::new(0);
    let (done_tx, done_rx) = mpsc::channel();
    c.bench_function("actor_creation/binary_tree_31_actors", |b| {
        b.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                let id = next_id.fetch_add(1, Ordering::Relaxed);
                let start = Instant::now();
                let root = system
                    .actor_of_args::<TreeActor, _>(&format!("tree-{id}"), Some(done_tx.clone()))
                    .unwrap();
                root.tell(TreeMessage::Grow(5), None);
                let total = done_rx
                    .recv_timeout(Duration::from_secs(30))
                    .expect("actor benchmark timed out waiting for completion");
                assert_eq!(total, 31);
                elapsed += start.elapsed();
            }
            elapsed
        });
    });
}

#[derive(Debug, Serialize, Deserialize)]
enum MailboxMessage {
    Data,
    Barrier,
}

struct MailboxReceiver(ChannelSender<()>);

impl ActorFactoryArgs<ChannelSender<()>> for MailboxReceiver {
    fn create_args(done: ChannelSender<()>) -> Self {
        Self(done)
    }
}

impl Actor for MailboxReceiver {
    type Msg = MailboxMessage;

    fn recv(&mut self, _: &Context<Self::Msg>, message: Self::Msg, _: Sender) {
        match message {
            MailboxMessage::Data => {
                black_box(());
            }
            MailboxMessage::Barrier => {
                let _ = self.0.send(());
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum ProducerMessage {
    SendBatch { count: usize },
}

struct MailboxProducer {
    receiver: ActorRef<MailboxMessage>,
    done: ChannelSender<()>,
}

impl ActorFactoryArgs<(ActorRef<MailboxMessage>, ChannelSender<()>)> for MailboxProducer {
    fn create_args((receiver, done): (ActorRef<MailboxMessage>, ChannelSender<()>)) -> Self {
        Self { receiver, done }
    }
}

impl Actor for MailboxProducer {
    type Msg = ProducerMessage;

    fn recv(&mut self, _: &Context<Self::Msg>, message: Self::Msg, _: Sender) {
        match message {
            ProducerMessage::SendBatch { count } => {
                for _ in 0..count {
                    self.receiver.tell(MailboxMessage::Data, None);
                }
                let _ = self.done.send(());
            }
        }
    }
}

fn bench_mailbox_performance(c: &mut Criterion, system: &ActorSystem) {
    const PRODUCERS: usize = 4;
    const MESSAGES_PER_PRODUCER: usize = 256;

    let (producer_tx, producer_rx) = mpsc::channel();
    let (barrier_tx, barrier_rx) = mpsc::channel();
    let receiver = system
        .actor_of_args::<MailboxReceiver, _>("mailbox-suite-receiver", barrier_tx)
        .unwrap();
    let producers: Vec<_> = (0..PRODUCERS)
        .map(|index| {
            system
                .actor_of_args::<MailboxProducer, _>(
                    &format!("mailbox-suite-producer-{index}"),
                    (receiver.clone(), producer_tx.clone()),
                )
                .unwrap()
        })
        .collect();

    let mut group = c.benchmark_group("mailbox_performance");
    group.throughput(Throughput::Elements(
        (PRODUCERS * MESSAGES_PER_PRODUCER) as u64,
    ));
    group.bench_function("4_senders_x_256_messages", |b| {
        b.iter_custom(|iterations| {
            let start = Instant::now();
            for _ in 0..iterations {
                for producer in &producers {
                    producer.tell(
                        ProducerMessage::SendBatch {
                            count: MESSAGES_PER_PRODUCER,
                        },
                        None,
                    );
                }
                wait_for_count(&producer_rx, PRODUCERS);
                receiver.tell(MailboxMessage::Barrier, None);
                barrier_rx
                    .recv_timeout(Duration::from_secs(30))
                    .expect("mailbox receiver stopped before barrier");
            }
            start.elapsed()
        });
    });
    group.finish();

    for producer in &producers {
        stop_actor(system, producer);
    }
    stop_actor(system, &receiver);
}

#[derive(Debug, Serialize, Deserialize)]
struct MandelbrotRow {
    y: usize,
    width: usize,
}

struct MandelbrotWorker(ChannelSender<u64>);

impl ActorFactoryArgs<ChannelSender<u64>> for MandelbrotWorker {
    fn create_args(tx: ChannelSender<u64>) -> Self {
        Self(tx)
    }
}

impl Actor for MandelbrotWorker {
    type Msg = MandelbrotRow;

    fn recv(&mut self, _: &Context<Self::Msg>, row: Self::Msg, _: Sender) {
        let checksum = mandelbrot_row(row.width, row.y);
        let _ = self.0.send(checksum);
    }
}

// Row widths are small, so converting coordinates to `f64` is exact.
#[allow(clippy::cast_precision_loss)]
fn mandelbrot_row(width: usize, y: usize) -> u64 {
    const MAX_ITERATIONS: usize = 250;
    let ci = 2.0 * y as f64 / width as f64 - 1.0;
    let mut checksum = 0_u64;
    for x in 0..width {
        let cr0 = 2.0 * x as f64 / width as f64 - 1.5;
        let (mut cr, mut ci_value) = (0.0, 0.0);
        let mut iterations = 0;
        while cr * cr + ci_value * ci_value <= 4.0 && iterations < MAX_ITERATIONS {
            let next_cr = cr * cr - ci_value * ci_value + cr0;
            ci_value = 2.0 * cr * ci_value + ci;
            cr = next_cr;
            iterations += 1;
        }
        checksum = checksum.wrapping_add(iterations as u64);
    }
    checksum
}

fn bench_mandelbrot(c: &mut Criterion, system: &ActorSystem) {
    const WIDTH: usize = 32;
    const WORKERS: usize = 4;
    let (result_tx, result_rx) = mpsc::channel();
    let workers: Vec<_> = (0..WORKERS)
        .map(|index| {
            system
                .actor_of_args::<MandelbrotWorker, _>(
                    &format!("mandelbrot-worker-{index}"),
                    result_tx.clone(),
                )
                .unwrap()
        })
        .collect();

    let mut group = c.benchmark_group("mandelbrot");
    group.throughput(Throughput::Elements((WIDTH * WIDTH) as u64));
    group.bench_function("32x32_four_actor_workers", |b| {
        b.iter_custom(|iterations| {
            let start = Instant::now();
            for _ in 0..iterations {
                for y in 0..WIDTH {
                    workers[y % WORKERS].tell(MandelbrotRow { y, width: WIDTH }, None);
                }
                let mut checksum = 0_u64;
                for _ in 0..WIDTH {
                    checksum = checksum.wrapping_add(
                        result_rx
                            .recv_timeout(Duration::from_secs(30))
                            .expect("Mandelbrot worker stopped before returning a row"),
                    );
                }
                assert_ne!(checksum, 0);
                black_box(checksum);
            }
            start.elapsed()
        });
    });
    group.finish();

    for worker in &workers {
        stop_actor(system, worker);
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum MatchMessage {
    One(i32),
    Two(f64),
    Three([i32; 1]),
    Four(i32, String),
    Five(i32, i32, i32),
    Six(i32, f64, String),
    Barrier,
}

struct MatchingActor(ChannelSender<()>);

impl ActorFactoryArgs<ChannelSender<()>> for MatchingActor {
    fn create_args(done: ChannelSender<()>) -> Self {
        Self(done)
    }
}

impl Actor for MatchingActor {
    type Msg = MatchMessage;

    fn recv(&mut self, _: &Context<Self::Msg>, message: Self::Msg, _: Sender) {
        match message {
            MatchMessage::One(a) => {
                black_box(a);
            }
            MatchMessage::Two(a) => {
                black_box(a);
            }
            MatchMessage::Three(a) => {
                black_box(a);
            }
            MatchMessage::Four(a, b) => {
                black_box((a, b));
            }
            MatchMessage::Five(a, b, c) => {
                black_box((a, b, c));
            }
            MatchMessage::Six(a, b, c) => {
                black_box((a, b, c));
            }
            MatchMessage::Barrier => {
                let _ = self.0.send(());
            }
        }
    }
}

fn bench_matching(c: &mut Criterion, system: &ActorSystem) {
    let (done_tx, done_rx) = mpsc::channel();
    let actor = system
        .actor_of_args::<MatchingActor, _>("matching-suite-actor", done_tx)
        .unwrap();
    let mut group = c.benchmark_group("message_matching");
    group.throughput(Throughput::Elements(6));
    group.bench_function("six_message_shapes", |b| {
        b.iter_custom(|iterations| {
            let start = Instant::now();
            for _ in 0..iterations {
                actor.tell(MatchMessage::One(1), None);
                actor.tell(MatchMessage::Two(2.0), None);
                actor.tell(MatchMessage::Three([3]), None);
                actor.tell(MatchMessage::Four(4, "four".to_owned()), None);
                actor.tell(MatchMessage::Five(5, 5, 5), None);
                actor.tell(MatchMessage::Six(6, 6.0, "six".to_owned()), None);
                actor.tell(MatchMessage::Barrier, None);
                done_rx
                    .recv_timeout(Duration::from_secs(30))
                    .expect("matching actor stopped before processing messages");
            }
            start.elapsed()
        });
    });
    group.finish();
    stop_actor(system, &actor);
}

#[derive(Debug, Serialize, Deserialize)]
enum RingMessage {
    /// Links this ring member to the sibling actor with the given name.
    SetNext(String),
    Token {
        remaining: usize,
    },
}

struct RingActor {
    next: Option<BasicActorRef>,
    done: ChannelSender<()>,
}

impl ActorFactoryArgs<ChannelSender<()>> for RingActor {
    fn create_args(done: ChannelSender<()>) -> Self {
        Self { next: None, done }
    }
}

impl Actor for RingActor {
    type Msg = RingMessage;

    fn recv(&mut self, ctx: &Context<Self::Msg>, message: Self::Msg, _: Sender) {
        match message {
            RingMessage::SetNext(name) => {
                self.next = ctx
                    .myself()
                    .parent()
                    .children()
                    .find(|child| child.name() == name);
                assert!(self.next.is_some(), "ring actor {name} not found");
            }
            RingMessage::Token { remaining: 0 } => {
                let _ = self.done.send(());
            }
            RingMessage::Token { remaining } => {
                self.next
                    .as_ref()
                    .expect("ring actor not linked")
                    .try_tell(
                        RingMessage::Token {
                            remaining: remaining - 1,
                        },
                        None,
                    )
                    .expect("ring actor rejected token");
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum FactorMessage {
    Factorize(u64),
}

struct FactorActor(ChannelSender<Vec<u64>>);

impl ActorFactoryArgs<ChannelSender<Vec<u64>>> for FactorActor {
    fn create_args(tx: ChannelSender<Vec<u64>>) -> Self {
        Self(tx)
    }
}

impl Actor for FactorActor {
    type Msg = FactorMessage;

    fn recv(&mut self, _: &Context<Self::Msg>, message: Self::Msg, _: Sender) {
        match message {
            FactorMessage::Factorize(number) => {
                let _ = self.0.send(factorize(number));
            }
        }
    }
}

fn factorize(mut number: u64) -> Vec<u64> {
    let mut factors = Vec::new();
    let mut divisor = 2;
    while divisor <= number / divisor {
        while number.is_multiple_of(divisor) {
            factors.push(divisor);
            number /= divisor;
        }
        divisor = if divisor == 2 { 3 } else { divisor + 2 };
    }
    if number > 1 {
        factors.push(number);
    }
    factors
}

fn bench_mixed_case(c: &mut Criterion, system: &ActorSystem) {
    const RING_SIZE: usize = 8;
    const TOKEN_HOPS: usize = 64;
    const FACTORIZATION_INPUT: u64 = 1009 * 1013;

    let (ring_tx, ring_rx) = mpsc::channel();
    let ring: Vec<_> = (0..RING_SIZE)
        .map(|index| {
            system
                .actor_of_args::<RingActor, _>(&format!("mixed-ring-{index}"), ring_tx.clone())
                .unwrap()
        })
        .collect();
    for index in 0..RING_SIZE {
        ring[index].tell(
            RingMessage::SetNext(ring[(index + 1) % RING_SIZE].name().to_owned()),
            None,
        );
    }
    let (factor_tx, factor_rx) = mpsc::channel();
    let factor_actor = system
        .actor_of_args::<FactorActor, _>("mixed-factor-actor", factor_tx)
        .unwrap();

    c.bench_function("mixed_case/factorization_and_64_hop_ring", |b| {
        b.iter_custom(|iterations| {
            let start = Instant::now();
            for _ in 0..iterations {
                ring[0].tell(
                    RingMessage::Token {
                        remaining: TOKEN_HOPS,
                    },
                    None,
                );
                factor_actor.tell(FactorMessage::Factorize(FACTORIZATION_INPUT), None);
                ring_rx
                    .recv_timeout(Duration::from_secs(30))
                    .expect("ring stopped before returning its token");
                assert_eq!(
                    factor_rx
                        .recv_timeout(Duration::from_secs(30))
                        .expect("factor actor stopped before returning factors"),
                    vec![1009, 1013]
                );
            }
            start.elapsed()
        });
    });

    stop_actor(system, &factor_actor);
    for actor in &ring {
        stop_actor(system, actor);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ScheduledMessage;

struct ScheduledActor(ChannelSender<()>);

impl ActorFactoryArgs<ChannelSender<()>> for ScheduledActor {
    fn create_args(done: ChannelSender<()>) -> Self {
        Self(done)
    }
}

impl Actor for ScheduledActor {
    type Msg = ScheduledMessage;

    fn recv(&mut self, _: &Context<Self::Msg>, _: Self::Msg, _: Sender) {
        let _ = self.0.send(());
    }
}

fn bench_scheduling(c: &mut Criterion, system: &ActorSystem) {
    const JOBS: usize = 8;
    let (done_tx, done_rx) = mpsc::channel();
    let actor = system
        .actor_of_args::<ScheduledActor, _>("scheduler-suite-actor", done_tx)
        .unwrap();

    let mut group = c.benchmark_group("scheduling");
    group.throughput(Throughput::Elements(JOBS as u64));
    group.bench_function("eight_one_shot_jobs", |b| {
        b.iter_custom(|iterations| {
            let start = Instant::now();
            for _ in 0..iterations {
                for _ in 0..JOBS {
                    system.schedule_once(
                        Duration::from_millis(2),
                        actor.clone(),
                        None,
                        ScheduledMessage,
                    );
                }
                wait_for_count(&done_rx, JOBS);
            }
            start.elapsed()
        });
    });
    group.finish();
    stop_actor(system, &actor);
}

fn bench_actor_suite(c: &mut Criterion) {
    let system = benchmark_system();
    bench_actor_creation(c, &system);
    bench_mailbox_performance(c, &system);
    bench_mandelbrot(c, &system);
    bench_matching(c, &system);
    bench_mixed_case(c, &system);
    bench_scheduling(c, &system);
    futures::executor::block_on(system.shutdown())
        .expect("benchmark actor system failed to shut down");
}

criterion_group!(benches, bench_actor_suite);
criterion_main!(benches);
