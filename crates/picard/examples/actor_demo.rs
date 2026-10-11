use std::{
    hint::black_box,
    sync::mpsc,
    time::{Duration, Instant},
};

use picard::actors::{Actor, ActorRefFactory, ActorSystem, Context, Sender, Tell};

const ITEMS: u64 = 100_000;
const WORK_PER_ITEM: u32 = 100;
const MAX_WORKERS: usize = 4;

#[derive(Clone, Debug)]
struct Work {
    start: u64,
    end: u64,
    reply_to: mpsc::Sender<u64>,
}

#[derive(Default)]
struct Worker;

impl Actor for Worker {
    type Msg = Work;

    fn recv(&mut self, _: &Context<Self::Msg>, work: Self::Msg, _: Sender) {
        let result = calculate(work.start, work.end);
        work.reply_to
            .send(result)
            .expect("demo receiver should stay connected");
    }
}

fn calculate(start: u64, end: u64) -> u64 {
    (start..end).fold(0_u64, |sum, item| {
        let mut value = black_box(item);
        for _ in 0..WORK_PER_ITEM {
            value = value
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            value ^= value >> 29;
        }
        sum.wrapping_add(value)
    })
}

fn main() {
    let workers = std::thread::available_parallelism()
        .map_or(1, usize::from)
        .min(MAX_WORKERS)
        .min(ITEMS as usize);
    let system = ActorSystem::with_name("efficiency-demo").expect("start actor system");
    let actors: Vec<_> = (0..workers)
        .map(|index| {
            system
                .actor_of::<Worker>(&format!("worker-{index}"))
                .expect("create worker actor")
        })
        .collect();

    let sequential_start = Instant::now();
    let sequential_result = calculate(0, ITEMS);
    let sequential_time = sequential_start.elapsed();

    let parallel_start = Instant::now();
    let mut replies = Vec::with_capacity(workers);
    for (index, actor) in actors.iter().enumerate() {
        let start = ITEMS * index as u64 / workers as u64;
        let end = ITEMS * (index + 1) as u64 / workers as u64;
        let (reply_to, reply_from) = mpsc::channel();
        actor.tell(
            Work {
                start,
                end,
                reply_to,
            },
            None,
        );
        replies.push(reply_from);
    }

    let parallel_result = replies.into_iter().fold(0_u64, |sum, reply| {
        sum.wrapping_add(reply.recv().expect("receive worker result"))
    });
    let parallel_time = parallel_start.elapsed();

    assert_eq!(sequential_result, parallel_result);
    println!("Work items:       {ITEMS}");
    println!("Workers:          {workers}");
    println!("Sequential time:  {}", format_duration(sequential_time));
    println!("Actor time:       {}", format_duration(parallel_time));
    println!(
        "Speedup:          {:.2}x",
        sequential_time.as_secs_f64() / parallel_time.as_secs_f64()
    );
    println!("Result checksum:  {parallel_result}");
}

fn format_duration(duration: Duration) -> String {
    format!("{duration:.2?}")
}
