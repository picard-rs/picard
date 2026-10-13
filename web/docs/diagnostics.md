# Diagnostics

Picard has optional Cargo features for observing a running system. They are off by default and add no code when disabled. The [runtime console](console.md) provides authenticated WebSocket inspection and management; `tracing` and `stats` are described below.

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

Events have separate targets so subscriber filters can silence frequent lifecycle and
message diagnostics independently of supervision events. For example, with an
`EnvFilter`-enabled subscriber, `RUST_LOG=picard::actor::lifecycle=warn,picard::messaging=warn,picard::actor::supervision=info`
keeps supervision warnings while reducing routine event volume.

| Level   | Target                       | Kind  | Name / message                                       | Fields                            |
| ------- | ---------------------------- | ----- | ---------------------------------------------------- | --------------------------------- |
| `INFO`  | `picard::system`             | event | `actor system started`, `actor system shutting down` | `system`, `id`                    |
| `DEBUG` | `picard::actor::lifecycle`   | event | `actor created`                                      | `actor`, `actor_type`             |
| `DEBUG` | `picard::actor::lifecycle`   | event | `actor terminated`                                   | `actor`                           |
| `INFO`  | `picard::actor::lifecycle`   | event | `actor restarted`                                    | `actor`                           |
| `WARN`  | `picard::actor::lifecycle`   | event | `actor failed to restart`                            | `actor`                           |
| `ERROR` | `picard::actor::supervision` | event | `actor panicked; reporting failure to parent`        | `actor`                           |
| `WARN`  | `picard::actor::supervision` | event | `supervising failed child`                           | `supervisor`, `actor`, `strategy` |
| `DEBUG` | `picard::messaging`          | event | `message could not be delivered...`                  | `actor`, `msg_type` (typed sends) |
| `TRACE` | `picard::messaging`          | span  | `recv`, entered while an actor handles a message     | `actor`, `msg_type`               |

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

| Field                  | Counts                                                                               |
| ---------------------- | ------------------------------------------------------------------------------------ |
| `actors_created`       | Actors whose instance was produced and kernel started                                |
| `actors_terminated`    | Actors that were stopped and removed                                                 |
| `actors_restarted`     | Restarts after a failure (not counted as creations)                                  |
| `messages_sent`        | Messages enqueued in a user mailbox                                                  |
| `messages_failed`      | Messages (user or system) that could not be enqueued, e.g. the recipient had stopped |
| `system_messages_sent` | Lifecycle commands, events and failure notifications enqueued                        |

`live_actors()` returns `actors_created - actors_terminated`.

Counts include Picard's own actors (root, guardians, system channels and the dead letter logger) and messages sent to them, such as channel publishes. Each field is read separately, so a snapshot of a busy system is not atomic across fields. Counters are relaxed atomics on separate cache lines; each send costs one extra atomic increment.
