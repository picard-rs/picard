//! Runtime statistics, compiled in with the `stats` feature.

use std::sync::atomic::{AtomicU64, Ordering};

/// A snapshot of an [`ActorSystem`](crate::system::ActorSystem)'s runtime counters.
///
/// Counters are cumulative since the system started and include Picard's own
/// actors (root, guardians, channels, the dead letter logger) as well as user actors.
/// Each counter is read independently, so a snapshot taken while the system is busy
/// is not an atomic view across all fields.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct RuntimeStats {
    /// Actors whose instance was produced and whose kernel was started.
    pub actors_created: u64,
    /// Actors that were stopped and removed from the system.
    pub actors_terminated: u64,
    /// Actor restarts after a failure. A restart does not count as a creation.
    pub actors_restarted: u64,
    /// Messages successfully enqueued in an actor's user mailbox.
    pub messages_sent: u64,
    /// Messages that could not be enqueued, for example because the recipient has stopped.
    pub messages_failed: u64,
    /// System messages (lifecycle commands, events, failures) successfully enqueued.
    pub system_messages_sent: u64,
}

impl RuntimeStats {
    /// Actors created but not yet terminated.
    pub fn live_actors(&self) -> u64 {
        self.actors_created.saturating_sub(self.actors_terminated)
    }
}

// Each counter sits on its own cache line so threads bumping different counters
// do not contend on the same line.
#[derive(Default)]
#[repr(align(128))]
struct Counter(AtomicU64);

impl Counter {
    #[inline]
    fn incr(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

#[derive(Default)]
pub(crate) struct StatsRecorder {
    actors_created: Counter,
    actors_terminated: Counter,
    actors_restarted: Counter,
    messages_sent: Counter,
    messages_failed: Counter,
    system_messages_sent: Counter,
}

impl StatsRecorder {
    pub(crate) fn actor_created(&self) {
        self.actors_created.incr();
    }

    pub(crate) fn actor_terminated(&self) {
        self.actors_terminated.incr();
    }

    pub(crate) fn actor_restarted(&self) {
        self.actors_restarted.incr();
    }

    #[inline]
    pub(crate) fn message_sent(&self, ok: bool) {
        if ok {
            self.messages_sent.incr();
        } else {
            self.messages_failed.incr();
        }
    }

    #[inline]
    pub(crate) fn system_message_sent(&self, ok: bool) {
        if ok {
            self.system_messages_sent.incr();
        } else {
            self.messages_failed.incr();
        }
    }

    pub(crate) fn snapshot(&self) -> RuntimeStats {
        // Read terminations before creations so `live_actors` never underflows in practice.
        let actors_terminated = self.actors_terminated.get();
        RuntimeStats {
            actors_created: self.actors_created.get(),
            actors_terminated,
            actors_restarted: self.actors_restarted.get(),
            messages_sent: self.messages_sent.get(),
            messages_failed: self.messages_failed.get(),
            system_messages_sent: self.system_messages_sent.get(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_reflects_recorded_events() {
        let rec = StatsRecorder::default();
        rec.actor_created();
        rec.actor_created();
        rec.actor_terminated();
        rec.actor_restarted();
        rec.message_sent(true);
        rec.message_sent(false);
        rec.system_message_sent(true);

        let s = rec.snapshot();
        assert_eq!(s.actors_created, 2);
        assert_eq!(s.actors_terminated, 1);
        assert_eq!(s.actors_restarted, 1);
        assert_eq!(s.live_actors(), 1);
        assert_eq!(s.messages_sent, 1);
        assert_eq!(s.messages_failed, 1);
        assert_eq!(s.system_messages_sent, 1);
    }
}
