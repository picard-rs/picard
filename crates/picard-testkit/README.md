# picard-testkit

`picard-testkit` provides probes for coordinating actor tests. A probe is a
cloneable handle that an actor can use to report test events; the corresponding
receiver lets the test wait for those events.

```rust
use picard_testkit::probe::{
    Probe, ProbeReceive,
    channel::probe,
};

let (probe, received) = probe::<String>();
probe.event("actor handled the message".to_string());

assert_eq!(received.recv(), "actor handled the message");
```

The channel probe is intended for tests running in one process and uses an
`std::sync::mpsc` channel. It is not serializable; pass it to an actor through
`actor_of_args` rather than including it in an actor message when testing with
Picard's `serde` feature.
