//! Reproducible headless load test: ramps the vehicle count and reports a markdown table.

use std::{fmt::Write, time::Duration};

use crate::{gateway::Frame, msg::SupervisorMsg, sampler::Sampler, sim::Sim};

const WARMUP: Duration = Duration::from_secs(5);

fn vehicles(frame: &Frame) -> usize {
    frame.districts.iter().map(|d| d.vehicles.len()).sum()
}

/// Runs every size for `secs` (after a warm-up) and returns the results as a markdown table.
///
/// # Panics
///
/// Panics if the simulation cannot be started.
pub async fn run(sizes: &[usize], secs: Duration) -> String {
    let sim = Sim::start(0).expect("start simulation");
    let mut sampler = Sampler::new(sim.metrics.clone());
    let mut out = String::from(
        "| vehicles | actors | msgs sent/s | msgs processed/s | tick delay mean (µs) | tick delay p95 (µs) \
         | handler p95 (µs) | ticks/s (target 20) | CPU % | RSS MB | violations |\n\
         |---|---|---|---|---|---|---|---|---|---|---|\n",
    );
    for &n in sizes {
        sim.tell(SupervisorMsg::SetVehicles(n));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        while vehicles(&sim.frames.borrow()) != n && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        tokio::time::sleep(WARMUP).await;
        sampler.sample();
        tokio::time::sleep(secs).await;
        let s = sampler.sample();
        let _ = writeln!(
            out,
            "| {} | {} | {:.0} | {:.0} | {:.0} | {:.0} | {:.0} | {:.1} | {:.0} | {:.0} | {} |",
            vehicles(&sim.frames.borrow()),
            s.actors,
            s.msgs_sent_rate,
            s.msgs_processed_rate,
            s.tick_delay_mean_us,
            s.tick_delay_p95_us,
            s.handler_p95_us,
            s.sim_ticks_rate,
            s.cpu_pct,
            s.rss_mb,
            s.violations,
        );
    }
    out
}
