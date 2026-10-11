# Diagnostics

Picard has two optional Cargo features for observing a running system. Both are off by default and add no code when disabled.

```toml
[dependencies]
picard = { version = "0.4", features = ["tracing", "stats"] }
```

## Tracing

The `tracing` feature emits [`tracing`](https://docs.rs/tracing) spans and events. Install any subscriber, for example `tracing-subscriber`, to collect them:

```rust
tracing_subscriber::fmt().with_max_level(tracing::Level::DEBUG).init();
let sys = ActorSystem::new().unwrap();
```

| Level   | Kind  | Name / message                                  | Fields                         |
| ------- | ----- | ----------------------------------------------- | ------------------------------ |
| `INFO`  | event | `actor system started`, `actor system shutting down` | `system`, `id`            |
| `DEBUG` | event | `actor created`                                 | `actor`, `actor_type`          |
| `DEBUG` | event | `actor terminated`                              | `actor`                        |
| `INFO`  | event | `actor restarted`                               | `actor`                        |
| `WARN`  | event | `actor failed to restart`                       | `actor`                        |
| `ERROR` | event | `actor panicked; reporting failure to parent`   | `actor`                        |
| `WARN`  | event | `supervising failed child`                      | `supervisor`, `actor`, `strategy` |
| `DEBUG` | event | `message could not be delivered...`             | `actor`, `msg_type` (typed sends) |
| `TRACE` | span  | `recv`, entered while an actor handles a message | `actor`, `msg_type`           |

Events raised inside an actor's `recv` (including your own `tracing` calls) are nested in its `recv` span. The `recv` span is at `TRACE` level, so filter it out in production if its per-message cost matters.

The `tracing` feature is independent of Picard's [slog-based logger](logging.md); both can be used at the same time.

## Runtime statistics

The `stats` feature keeps cumulative counters and exposes them through `ActorSystem::stats()`:

```rust
let stats = sys.stats();
println!(
    "live actors: {}, messages sent: {}, failed: {}",
    stats.live_actors(),
    stats.messages_sent,
    stats.messages_failed,
);
```

`RuntimeStats` contains:

| Field                  | Counts                                                              |
| ---------------------- | ------------------------------------------------------------------- |
| `actors_created`       | Actors whose instance was produced and kernel started               |
| `actors_terminated`    | Actors that were stopped and removed                                |
| `actors_restarted`     | Restarts after a failure (not counted as creations)                 |
| `messages_sent`        | Messages enqueued in a user mailbox                                 |
| `messages_failed`      | Messages (user or system) that could not be enqueued, e.g. the recipient had stopped |
| `system_messages_sent` | Lifecycle commands, events and failure notifications enqueued      |

`live_actors()` returns `actors_created - actors_terminated`.

Counts include Picard's own actors (root, guardians, system channels and the dead letter logger) and messages sent to them, such as channel publishes. Each field is read separately, so a snapshot of a busy system is not atomic across fields. Counters are relaxed atomics on separate cache lines; each send costs one extra atomic increment.
