//! Per-actor message counters and an opt-in, bounded message log for the inspector.

use std::{collections::VecDeque, time::Duration};

use picard::platform::Instant;

use serde_json::{Value, json};

const RING: usize = 48;
const WATCH_FOR: Duration = Duration::from_secs(4);

#[derive(Default)]
pub struct Trace {
    pub sent: u64,
    pub received: u64,
    watched_until: Option<Instant>,
    ring: VecDeque<(u32, bool, &'static str, String)>,
}

impl Trace {
    pub fn sent(&mut self, ms: u32, kind: &'static str, peer: impl FnOnce() -> String) {
        self.sent += 1;
        self.record(ms, false, kind, peer);
    }

    pub fn received(&mut self, ms: u32, kind: &'static str, peer: impl FnOnce() -> String) {
        self.received += 1;
        self.record(ms, true, kind, peer);
    }

    fn record(
        &mut self,
        ms: u32,
        inbound: bool,
        kind: &'static str,
        peer: impl FnOnce() -> String,
    ) {
        if self.watched_until.is_none() {
            return;
        }
        if self.ring.len() == RING {
            self.ring.pop_front();
        }
        self.ring.push_back((ms, inbound, kind, peer()));
    }

    /// The inspector polls regularly; the log is only kept while somebody is watching.
    pub fn watch(&mut self) {
        self.watched_until = Some(Instant::now() + WATCH_FOR);
    }

    pub fn expire(&mut self) {
        if self.watched_until.is_some_and(|t| Instant::now() > t) {
            self.watched_until = None;
            self.ring.clear();
        }
    }

    #[must_use]
    pub fn is_watched(&self) -> bool {
        self.watched_until.is_some()
    }

    #[must_use]
    pub fn to_json(&self, me: &str) -> Value {
        let recent: Vec<Value> = self
            .ring
            .iter()
            .rev()
            .map(|(ms, inbound, kind, peer)| {
                let (from, to) = if *inbound {
                    (peer.as_str(), me)
                } else {
                    (me, peer.as_str())
                };
                json!({ "t": ms, "from": from, "to": to, "kind": kind })
            })
            .collect();
        json!({ "sent": self.sent, "received": self.received, "recent": recent })
    }
}
