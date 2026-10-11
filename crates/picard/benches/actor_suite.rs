use std::{
    hint::black_box,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Receiver, Sender as ChannelSender},
    },
    time::{Duration, Instant},
};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use picard::actors::*;

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

fn stop_actor<Msg: Message>(system: &ActorSystem, actor: &ActorRef<Msg>) {
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

#[derive(Debug)]
enum TreeMessage {
    Grow(u32, TreeReply),
    Result(usize),
}

#[derive(Clone, Debug)]
enum TreeReply {
    Parent(BasicActorRef),
    Done(ChannelSender<usize>),
}

impl TreeReply {
    fn send(&self, count: usize) {
        match self {
            TreeReply::Parent(parent) => {
                let _ = parent.try_tell(TreeMessage::Result(count), None);
            }
            TreeReply::Done(done) => {
                let _ = done.send(count);
            }
        }
    }
}

#[derive(Default)]
struct TreeActor {
    reply: Option<TreeReply>,
    pending: u8,
    total: usize,
}

impl Actor for TreeActor {
    type Msg = TreeMessage;

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, _: Sender) {
        match msg {
            TreeMessage::Grow(level, reply) if level <= 1 => {
                reply.send(1);
                ctx.myself().stop();
            }
            TreeMessage::Grow(level, reply) => {
                self.reply = Some(reply);
                self.pending = 2;
                self.total = 1;
                for name in ["left", "right"] {
                    ctx.actor_of::<TreeActor>(name).unwrap().tell(
                        TreeMessage::Grow(level - 1, TreeReply::Parent(ctx.myself().into())),
                        None,
                    );
                }
            }
            TreeMessage::Result(count) => {
                self.total += count;
                self.pending -= 1;
                if self.pending == 0 {
                    if let Some(reply) = &self.reply {
                        reply.send(self.total);
                    }
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
                let root = system.actor_of::<TreeActor>(&format!("tree-{id}")).unwrap();
                root.tell(TreeMessage::Grow(5, TreeReply::Done(done_tx.clone())), None);
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

#[derive(Debug)]
enum MailboxMessage {
    Data,
    Barrier(ChannelSender<()>),
}

#[derive(Default)]
struct MailboxReceiver;

impl Actor for MailboxReceiver {
    type Msg = MailboxMessage;

    fn recv(&mut self, _: &Context<Self::Msg>, message: Self::Msg, _: Sender) {
        match message {
            MailboxMessage::Data => {
                black_box(());
            }
            MailboxMessage::Barrier(done) => {
                let _ = done.send(());
            }
        }
    }
}

#[derive(Debug)]
enum ProducerMessage {
    SendBatch {
        receiver: ActorRef<MailboxMessage>,
        count: usize,
        done: ChannelSender<()>,
    },
}

#[derive(Default)]
struct MailboxProducer;

impl Actor for MailboxProducer {
    type Msg = ProducerMessage;

    fn recv(&mut self, _: &Context<Self::Msg>, message: Self::Msg, _: Sender) {
        match message {
            ProducerMessage::SendBatch {
                receiver,
                count,
                done,
            } => {
                for _ in 0..count {
                    receiver.tell(MailboxMessage::Data, None);
                }
                let _ = done.send(());
            }
        }
    }
}

fn bench_mailbox_performance(c: &mut Criterion, system: &ActorSystem) {
    const PRODUCERS: usize = 4;
    const MESSAGES_PER_PRODUCER: usize = 256;

    let receiver = system
        .actor_of::<MailboxReceiver>("mailbox-suite-receiver")
        .unwrap();
    let producers: Vec<_> = (0..PRODUCERS)
        .map(|index| {
            system
                .actor_of::<MailboxProducer>(&format!("mailbox-suite-producer-{index}"))
                .unwrap()
        })
        .collect();
    let (producer_tx, producer_rx) = mpsc::channel();
    let (barrier_tx, barrier_rx) = mpsc::channel();

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
                            receiver: receiver.clone(),
                            count: MESSAGES_PER_PRODUCER,
                            done: producer_tx.clone(),
                        },
                        None,
                    );
                }
                wait_for_count(&producer_rx, PRODUCERS);
                receiver.tell(MailboxMessage::Barrier(barrier_tx.clone()), None);
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

#[derive(Debug)]
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

#[derive(Debug)]
enum MatchMessage {
    One(i32),
    Two(f64),
    Three([i32; 1]),
    Four(i32, &'static str),
    Five(i32, i32, i32),
    Six(i32, f64, &'static str),
    Barrier(ChannelSender<()>),
}

#[derive(Default)]
struct MatchingActor;

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
            MatchMessage::Barrier(done) => {
                let _ = done.send(());
            }
        }
    }
}

fn bench_matching(c: &mut Criterion, system: &ActorSystem) {
    let actor = system
        .actor_of::<MatchingActor>("matching-suite-actor")
        .unwrap();
    let (done_tx, done_rx) = mpsc::channel();
    let mut group = c.benchmark_group("message_matching");
    group.throughput(Throughput::Elements(6));
    group.bench_function("six_message_shapes", |b| {
        b.iter_custom(|iterations| {
            let start = Instant::now();
            for _ in 0..iterations {
                actor.tell(MatchMessage::One(1), None);
                actor.tell(MatchMessage::Two(2.0), None);
                actor.tell(MatchMessage::Three([3]), None);
                actor.tell(MatchMessage::Four(4, "four"), None);
                actor.tell(MatchMessage::Five(5, 5, 5), None);
                actor.tell(MatchMessage::Six(6, 6.0, "six"), None);
                actor.tell(MatchMessage::Barrier(done_tx.clone()), None);
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

#[derive(Debug)]
enum RingMessage {
    SetNext(BasicActorRef),
    Token {
        remaining: usize,
        done: ChannelSender<()>,
    },
}

#[derive(Default)]
struct RingActor {
    next: Option<BasicActorRef>,
}

impl Actor for RingActor {
    type Msg = RingMessage;

    fn recv(&mut self, _: &Context<Self::Msg>, message: Self::Msg, _: Sender) {
        match message {
            RingMessage::SetNext(next) => self.next = Some(next),
            RingMessage::Token { remaining: 0, done } => {
                let _ = done.send(());
            }
            RingMessage::Token { remaining, done } => {
                self.next
                    .as_ref()
                    .expect("ring actor not linked")
                    .try_tell(
                        RingMessage::Token {
                            remaining: remaining - 1,
                            done,
                        },
                        None,
                    )
                    .expect("ring actor rejected token");
            }
        }
    }
}

#[derive(Debug)]
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

    let ring: Vec<_> = (0..RING_SIZE)
        .map(|index| {
            system
                .actor_of::<RingActor>(&format!("mixed-ring-{index}"))
                .unwrap()
        })
        .collect();
    for index in 0..RING_SIZE {
        ring[index].tell(
            RingMessage::SetNext(ring[(index + 1) % RING_SIZE].clone().into()),
            None,
        );
    }
    let (ring_tx, ring_rx) = mpsc::channel();
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
                        done: ring_tx.clone(),
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

#[derive(Debug)]
struct ScheduledMessage(ChannelSender<()>);

#[derive(Default)]
struct ScheduledActor;

impl Actor for ScheduledActor {
    type Msg = ScheduledMessage;

    fn recv(&mut self, _: &Context<Self::Msg>, message: Self::Msg, _: Sender) {
        let _ = message.0.send(());
    }
}

fn bench_scheduling(c: &mut Criterion, system: &ActorSystem) {
    const JOBS: usize = 8;
    let actor = system
        .actor_of::<ScheduledActor>("scheduler-suite-actor")
        .unwrap();
    let (done_tx, done_rx) = mpsc::channel();

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
                        ScheduledMessage(done_tx.clone()),
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
