//! Shared instrumentation counters.
//!
//! These are write-only for actors (relaxed atomic increments) and read by the metrics sampler.
//! They never feed back into simulation decisions, so they are not simulation state.
//!
//! Provenance matters for an honest demo:
//! - `terminated`, `restarted` and `dead_letters` come from Picard's own system event stream and
//!   dead-letter channel (see `observer.rs`). Picard only publishes `ActorCreated` for direct
//!   children of `/user`, so `created` is counted at this application's spawn sites.
//! - Everything else is counted by this application at its own send/receive call sites.

use std::{
    sync::atomic::{AtomicU64, Ordering::Relaxed},
    time::Duration,
};

use picard::platform::Instant;

pub const BUCKETS: usize = 24;

/// Log2 histogram of durations in microseconds.
#[derive(Default)]
pub struct Histogram {
    buckets: [AtomicU64; BUCKETS],
}

impl Histogram {
    pub fn record(&self, d: Duration) {
        let us = u64::try_from(d.as_micros()).unwrap_or(u64::MAX);
        let idx = (64 - us.leading_zeros() as usize).min(BUCKETS - 1);
        self.buckets[idx].fetch_add(1, Relaxed);
    }

    pub fn snapshot(&self) -> [u64; BUCKETS] {
        std::array::from_fn(|i| self.buckets[i].load(Relaxed))
    }
}

/// Mean and p95 (in microseconds) of the samples recorded between two snapshots.
#[must_use]
pub fn summarize(prev: &[u64; BUCKETS], now: &[u64; BUCKETS]) -> (f64, f64) {
    let delta: Vec<u64> = now
        .iter()
        .zip(prev)
        .map(|(n, p)| n.saturating_sub(*p))
        .collect();
    let total: u64 = delta.iter().sum();
    if total == 0 {
        return (0.0, 0.0);
    }
    let upper = |i: usize| {
        if i == 0 {
            1.0
        } else {
            f64::from(1_u32 << i.min(31))
        }
    };
    let mid = |i: usize| if i == 0 { 0.5 } else { upper(i) * 0.75 };
    #[allow(clippy::cast_precision_loss)]
    let mean = delta
        .iter()
        .enumerate()
        .map(|(i, c)| mid(i) * *c as f64)
        .sum::<f64>()
        / total as f64;
    // ceil(0.95 * total) without floating point.
    let target = total - total / 20;
    let mut acc = 0;
    for (i, c) in delta.iter().enumerate() {
        acc += c;
        if acc >= target {
            return (mean, upper(i));
        }
    }
    (mean, upper(BUCKETS - 1))
}

pub struct Metrics {
    pub start: Instant,
    pub sent: AtomicU64,
    pub processed: AtomicU64,
    pub sim_ticks: AtomicU64,
    pub vehicle_ticks: AtomicU64,
    pub trips: AtomicU64,
    pub granted: AtomicU64,
    pub denied: AtomicU64,
    pub retries: AtomicU64,
    pub reroutes: AtomicU64,
    pub violations: AtomicU64,
    pub created: AtomicU64,
    pub terminated: AtomicU64,
    pub restarted: AtomicU64,
    pub dead_letters: AtomicU64,
    /// Time a tick message spent between being sent and being handled by a vehicle.
    pub tick_delay: Histogram,
    /// Time a vehicle spent inside its `recv` handler.
    pub handler: Histogram,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            start: Instant::now(),
            sent: AtomicU64::default(),
            processed: AtomicU64::default(),
            sim_ticks: AtomicU64::default(),
            vehicle_ticks: AtomicU64::default(),
            trips: AtomicU64::default(),
            granted: AtomicU64::default(),
            denied: AtomicU64::default(),
            retries: AtomicU64::default(),
            reroutes: AtomicU64::default(),
            violations: AtomicU64::default(),
            created: AtomicU64::default(),
            terminated: AtomicU64::default(),
            restarted: AtomicU64::default(),
            dead_letters: AtomicU64::default(),
            tick_delay: Histogram::default(),
            handler: Histogram::default(),
        }
    }
}

impl Metrics {
    pub fn inc(&self, counter: &AtomicU64) {
        counter.fetch_add(1, Relaxed);
    }

    pub fn add(&self, counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Relaxed);
    }

    pub fn get(counter: &AtomicU64) -> u64 {
        counter.load(Relaxed)
    }

    #[allow(clippy::cast_possible_truncation)]
    pub fn elapsed_ms(&self) -> u32 {
        self.start.elapsed().as_millis() as u32
    }

    pub fn alive_actors(&self) -> u64 {
        Self::get(&self.created).saturating_sub(Self::get(&self.terminated))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn p95_tracks_the_tail() {
        let h = Histogram::default();
        for _ in 0..95 {
            h.record(Duration::from_micros(10));
        }
        for _ in 0..5 {
            h.record(Duration::from_millis(10));
        }
        let (mean, p95) = summarize(&[0; BUCKETS], &h.snapshot());
        assert!(p95 <= 16.0, "p95 should still be in the fast bucket: {p95}");
        assert!(mean > 100.0, "mean should include the slow tail: {mean}");
    }
}
