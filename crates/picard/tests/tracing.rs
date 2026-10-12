#![cfg(feature = "tracing")]

use picard::actors::*;
use std::{
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tracing::{
    Event, Metadata, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
    subscriber::Interest,
};

#[derive(Clone, Default)]
struct Recorder {
    events: Arc<Mutex<Vec<String>>>,
    targets: Arc<Mutex<Vec<(&'static str, String)>>>,
    spans: Arc<Mutex<Vec<&'static str>>>,
    next_id: Arc<AtomicU64>,
}

struct MessageVisitor<'a>(&'a mut String);

impl Visit for MessageVisitor<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            *self.0 = format!("{value:?}");
        }
    }
}

impl Subscriber for Recorder {
    fn register_callsite(&self, _: &'static Metadata<'static>) -> Interest {
        Interest::always()
    }

    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, span: &Attributes<'_>) -> Id {
        self.spans.lock().unwrap().push(span.metadata().name());
        Id::from_u64(self.next_id.fetch_add(1, Ordering::Relaxed) + 1)
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut msg = String::new();
        event.record(&mut MessageVisitor(&mut msg));
        self.targets
            .lock()
            .unwrap()
            .push((event.metadata().target(), msg.clone()));
        self.events.lock().unwrap().push(msg);
    }

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}

#[derive(Default)]
struct Worker;

impl Actor for Worker {
    type Msg = u32;

    fn recv(&mut self, _: &Context<Self::Msg>, msg: Self::Msg, _: Sender) {
        assert!(msg != 0, "zero is fatal");
    }
}

#[test]
fn emits_lifecycle_events_and_recv_spans() {
    let rec = Recorder::default();
    tracing::subscriber::set_global_default(rec.clone()).unwrap();

    let sys = ActorSystem::new().unwrap();
    let worker = sys.actor_of::<Worker>("worker").unwrap();
    worker.tell(1_u32, None);
    worker.tell(0_u32, None);

    let expected = [
        "actor system started",
        "actor created",
        "actor panicked; reporting failure to parent",
        "supervising failed child",
        "actor restarted",
    ];
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let events = rec.events.lock().unwrap().clone();
        if expected.iter().all(|e| events.iter().any(|m| m == e)) {
            break;
        }
        assert!(Instant::now() < deadline, "missing events, got {events:?}");
        std::thread::sleep(Duration::from_millis(5));
    }

    assert!(rec.spans.lock().unwrap().contains(&"recv"));
    let targets = rec.targets.lock().unwrap();
    assert!(targets.contains(&("picard::system", "actor system started".into())));
    assert!(targets.contains(&("picard::actor::lifecycle", "actor created".into())));
    assert!(targets.contains(&(
        "picard::actor::supervision",
        "actor panicked; reporting failure to parent".into()
    )));
    assert!(targets.contains(&(
        "picard::actor::supervision",
        "supervising failed child".into()
    )));
}
