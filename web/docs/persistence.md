# Persistence and CQRS

Picard does not provide a built-in event store or persistent actor lifecycle.
Actor state lives in memory; when an actor is restarted or recreated, the
application is responsible for restoring any state that must survive. This can
be done by loading state from an application-owned database or by implementing
an event-sourcing design in the application.

## CQRS entity management

The separate `picard-cqrs` crate provides an entity manager that routes
commands by entity ID and starts an actor for an entity when needed. After an
entity actor has been idle for the configured interval, the manager stops it;
a later command creates a fresh actor instance. The crate does not store or
replay events, and the application must arrange any required state recovery.

The default idle interval is 120 seconds. Configure it under `[cqrs]` in
`picard.toml`:

```toml
[cqrs]
sleep_after_secs = 120
```

See the [`picard-cqrs` README](https://github.com/picard-rs/picard/tree/main/crates/picard-cqrs#readme)
for an entity manager example. For messaging and actor lifecycle, see
[Actors](actors.md) and [Message Types](messaging.md).
