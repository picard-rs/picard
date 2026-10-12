//! Turns the raw counters into per-second rates plus process CPU and memory.

use std::{fmt::Write, fs, sync::Arc};

use picard::platform::Instant;

use crate::metrics::{BUCKETS, Metrics, summarize};

const CLOCK_TICKS_PER_SEC: f64 = 100.0;
const PAGE_SIZE: f64 = 4096.0;

#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Sample {
    pub uptime_s: f64,
    pub actors: u64,
    pub msgs_sent_rate: f64,
    pub msgs_processed_rate: f64,
    pub sim_ticks_rate: f64,
    pub tick_delay_mean_us: f64,
    pub tick_delay_p95_us: f64,
    pub handler_mean_us: f64,
    pub handler_p95_us: f64,
    pub grants_rate: f64,
    pub denies_rate: f64,
    pub trips_rate: f64,
    pub trips: u64,
    pub retries: u64,
    pub reroutes: u64,
    pub violations: u64,
    pub created: u64,
    pub terminated: u64,
    pub restarted: u64,
    pub dead_letters: u64,
    pub cpu_pct: f64,
    pub rss_mb: f64,
}

impl Sample {
    /// Folds another process's sample into this one: counters, rates and resources add up,
    /// latencies are averaged by tick rate, and the p95 is the worst of the two.
    pub fn merge(&mut self, other: &Self) {
        let weight = self.sim_ticks_rate + other.sim_ticks_rate;
        let blend = |mine: f64, theirs: f64| {
            if weight > 0.0 {
                (mine * self.sim_ticks_rate + theirs * other.sim_ticks_rate) / weight
            } else {
                mine.max(theirs)
            }
        };
        self.tick_delay_mean_us = blend(self.tick_delay_mean_us, other.tick_delay_mean_us);
        self.handler_mean_us = blend(self.handler_mean_us, other.handler_mean_us);
        self.tick_delay_p95_us = self.tick_delay_p95_us.max(other.tick_delay_p95_us);
        self.handler_p95_us = self.handler_p95_us.max(other.handler_p95_us);
        self.uptime_s = self.uptime_s.max(other.uptime_s);
        self.actors += other.actors;
        self.msgs_sent_rate += other.msgs_sent_rate;
        self.msgs_processed_rate += other.msgs_processed_rate;
        self.sim_ticks_rate += other.sim_ticks_rate;
        self.grants_rate += other.grants_rate;
        self.denies_rate += other.denies_rate;
        self.trips_rate += other.trips_rate;
        self.trips += other.trips;
        self.retries += other.retries;
        self.reroutes += other.reroutes;
        self.violations += other.violations;
        self.created += other.created;
        self.terminated += other.terminated;
        self.restarted += other.restarted;
        self.dead_letters += other.dead_letters;
        self.cpu_pct += other.cpu_pct;
        self.rss_mb += other.rss_mb;
    }

    #[must_use]
    pub fn to_json(&self) -> String {
        let mut s = String::with_capacity(512);
        let _ = write!(
            s,
            "{{\"uptime\":{:.1},\"actors\":{},\"sent\":{:.0},\"processed\":{:.0},\"ticks\":{:.1},\
             \"delay_mean\":{:.0},\"delay_p95\":{:.0},\"handler_mean\":{:.0},\"handler_p95\":{:.0},\
             \"grants\":{:.1},\"denies\":{:.1},\"trips_rate\":{:.1},\"trips\":{},\"retries\":{},\
             \"reroutes\":{},\"violations\":{},\"created\":{},\"terminated\":{},\"restarted\":{},\
             \"dead_letters\":{},\"cpu\":{:.1},\"rss\":{:.1}}}",
            self.uptime_s,
            self.actors,
            self.msgs_sent_rate,
            self.msgs_processed_rate,
            self.sim_ticks_rate,
            self.tick_delay_mean_us,
            self.tick_delay_p95_us,
            self.handler_mean_us,
            self.handler_p95_us,
            self.grants_rate,
            self.denies_rate,
            self.trips_rate,
            self.trips,
            self.retries,
            self.reroutes,
            self.violations,
            self.created,
            self.terminated,
            self.restarted,
            self.dead_letters,
            self.cpu_pct,
            self.rss_mb,
        );
        s
    }
}

pub struct Sampler {
    metrics: Arc<Metrics>,
    at: Instant,
    counters: [u64; 7],
    delay: [u64; BUCKETS],
    handler: [u64; BUCKETS],
    cpu_ticks: f64,
}

impl Sampler {
    pub fn new(metrics: Arc<Metrics>) -> Self {
        let counters = Self::counters(&metrics);
        let delay = metrics.tick_delay.snapshot();
        let handler = metrics.handler.snapshot();
        Self {
            metrics,
            at: Instant::now(),
            counters,
            delay,
            handler,
            cpu_ticks: cpu_ticks(),
        }
    }

    fn counters(m: &Metrics) -> [u64; 7] {
        [
            Metrics::get(&m.sent),
            Metrics::get(&m.processed),
            Metrics::get(&m.sim_ticks),
            Metrics::get(&m.granted),
            Metrics::get(&m.denied),
            Metrics::get(&m.trips),
            Metrics::get(&m.vehicle_ticks),
        ]
    }

    #[allow(clippy::cast_precision_loss)]
    pub fn sample(&mut self) -> Sample {
        let m = &self.metrics;
        let dt = self.at.elapsed().as_secs_f64().max(1e-3);
        self.at = Instant::now();

        let now = Self::counters(m);
        let rate = |i: usize| now[i].saturating_sub(self.counters[i]) as f64 / dt;
        let delay_now = m.tick_delay.snapshot();
        let handler_now = m.handler.snapshot();
        let (delay_mean, delay_p95) = summarize(&self.delay, &delay_now);
        let (handler_mean, handler_p95) = summarize(&self.handler, &handler_now);
        let cpu = cpu_ticks();

        let sample = Sample {
            uptime_s: f64::from(m.elapsed_ms()) / 1000.0,
            actors: m.alive_actors(),
            msgs_sent_rate: rate(0),
            msgs_processed_rate: rate(1),
            sim_ticks_rate: rate(2),
            tick_delay_mean_us: delay_mean,
            tick_delay_p95_us: delay_p95,
            handler_mean_us: handler_mean,
            handler_p95_us: handler_p95,
            grants_rate: rate(3),
            denies_rate: rate(4),
            trips_rate: rate(5),
            trips: now[5],
            retries: Metrics::get(&m.retries),
            reroutes: Metrics::get(&m.reroutes),
            violations: Metrics::get(&m.violations),
            created: Metrics::get(&m.created),
            terminated: Metrics::get(&m.terminated),
            restarted: Metrics::get(&m.restarted),
            dead_letters: Metrics::get(&m.dead_letters),
            cpu_pct: (cpu - self.cpu_ticks) / CLOCK_TICKS_PER_SEC / dt * 100.0,
            rss_mb: rss_bytes() / 1_048_576.0,
        };
        self.counters = now;
        self.delay = delay_now;
        self.handler = handler_now;
        self.cpu_ticks = cpu;
        sample
    }
}

/// Total user + system CPU time of this process in clock ticks (Linux only; 0 elsewhere).
#[allow(clippy::cast_precision_loss)]
fn cpu_ticks() -> f64 {
    let Ok(stat) = fs::read_to_string("/proc/self/stat") else {
        return 0.0;
    };
    // The command name may contain spaces; fields after the closing parenthesis are stable.
    let Some(rest) = stat.rsplit_once(')').map(|(_, r)| r) else {
        return 0.0;
    };
    let f: Vec<&str> = rest.split_whitespace().collect();
    let get = |i: usize| f.get(i).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0) as f64;
    get(11) + get(12)
}

#[allow(clippy::cast_precision_loss)]
fn rss_bytes() -> f64 {
    let Ok(statm) = fs::read_to_string("/proc/self/statm") else {
        return 0.0;
    };
    let pages = statm
        .split_whitespace()
        .nth(1)
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    pages as f64 * PAGE_SIZE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_sums_counters_and_weights_latency() {
        let mut a = Sample {
            actors: 10,
            trips: 5,
            sim_ticks_rate: 10.0,
            tick_delay_mean_us: 100.0,
            tick_delay_p95_us: 200.0,
            ..Sample::default()
        };
        let b = Sample {
            actors: 20,
            trips: 7,
            sim_ticks_rate: 30.0,
            tick_delay_mean_us: 200.0,
            tick_delay_p95_us: 800.0,
            ..Sample::default()
        };
        a.merge(&b);
        assert_eq!((a.actors, a.trips), (30, 12));
        assert!((a.tick_delay_mean_us - 175.0).abs() < 1e-9);
        assert!((a.tick_delay_p95_us - 800.0).abs() < 1e-9);
    }
}
