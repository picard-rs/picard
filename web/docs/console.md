# Runtime console

Enable `console` to inspect and manage a native Picard application over a
WebSocket connection. The feature alone does **not** open a port. It is
independent of `stats` and `tracing`.

```toml
[dependencies]
picard = { version = "0.4", features = ["console"] }
```

Also enable `serde` to send JSON messages and inspect their JSON payloads:

```toml
picard = { version = "0.4", features = ["console", "serde"] }
```

## Starting an endpoint

```rust,no_run
use picard::actors::*;
use picard::console::ConsoleOptions;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sys = ActorSystem::new()?;
    let console = sys.start_console(ConsoleOptions {
        bind: "127.0.0.1:9000".parse()?,
        token: std::env::var("PICARD_CONSOLE_TOKEN")?,
    })?;
    println!("Console listening at ws://{}/console", console.local_addr());

    // Create actors and run the application here, keeping `console` alive.

    console.stop()?;
    futures::executor::block_on(sys.shutdown())?;
    Ok(())
}
```

Use port `0` to request an available port, then read it with `local_addr()`.
Only loopback addresses are accepted. The token must contain at least 32 ASCII
letters, digits, `-` or `_`; generate an unpredictable token in your application
or deployment environment. Do not put it in source control.

The handle owns the listener and client threads. `stop()` reports shutdown
errors; dropping the handle also stops and joins them, logging failures.
Stopping the actor system does not automatically drop the console handle.
There is no console listener or terminal REPL on WebAssembly.

The endpoint exposes application internals and destructive controls. Treat
the token as an administrative credential. Remote use should go through a
secure tunnel to the loopback port; the endpoint does not provide TLS.

## Interactive REPL

The `picard-console` workspace crate provides a terminal client:

```sh
cargo run -p picard-console -- ws://127.0.0.1:9000/console
```

Set `PICARD_CONSOLE_TOKEN` in the client's environment to the same token used
by the server. The client does not accept tokens as command-line arguments
or URL parameters.

Commands:

```text
actors
inspect /user/worker
messages /user/worker
pause /user/worker
clear /user/worker
resume /user/worker
restart /user/worker
send /user/worker {"Greet":{"name":"Picard"}}
stop /user/worker
help
quit
```

Tab completes commands and actor paths. Up/Down recall history, Ctrl-C cancels
input, and Ctrl-D disconnects. Prompts, hints, results, and errors use terminal
colors; `NO_COLOR` disables them, and redirected output is uncolored.

History is stored at `$XDG_STATE_HOME/picard-console/history`, or
`$HOME/.local/state/picard-console/history`. `send` commands are never saved
because payloads may be sensitive. Connection and protocol errors are explicit;
there is no automatic reconnect or retry of destructive operations.

### Living City

The Living City demo forwards its `console` and `serde` features to Picard.
Start its optional endpoint with `--console-listen 127.0.0.1:9000` and set
`PICARD_CONSOLE_TOKEN` in the environment:

```sh
cargo run -p living-city --release --features console,serde -- \
  --console-listen 127.0.0.1:9000
```

The same option works with `loadtest`. See the [Living City guide](../../demo/GUIDE.md)
for Clap environment options and its JSON message compatibility.

## Inspection and management semantics

- `actors` lists registered actors, including runtime actors. Each entry has
  its absolute path, parent path, Rust message type, JSON-send capability,
  management capability, and mailbox state. The REPL follows actor-list pages
  automatically and refreshes path completion before each prompt.
- `inspect` returns one actor's metadata, queued user/system message counts,
  and paused, suspended, and scheduled flags. Snapshots are best-effort views
  of a concurrent runtime, not an atomic snapshot of the whole application.
- `messages` returns at most 64 recent user-message events per actor:
  `enqueued`, `received`, `cleared`, and `delivery_failed`. Each event includes
  the message type and optional sender path. This is a bounded history, not a
  nondestructive copy of the current queue. `received` means dequeued for
  processing, not that the handler successfully completed.
- With `serde`, enqueue/receive events include a JSON `message` payload for
  actors created through APIs with a `Message` bound. Captured payloads are
  limited to 4096 serialized bytes. Oversized or unserializable payloads produce
  `payload_error` metadata without preventing normal delivery. Clear events
  contain metadata only. Runtime message types need not implement serde.
- `pause` prevents further user-message dequeues. An already dequeued message
  can still finish. System messages continue, so supervision, restart, stop,
  and system shutdown remain available. Pausing through the console is separate
  from failure-induced suspension and persists across restarts.
- `resume` removes the console pause and wakes the actor. It does not bypass
  suspension caused by actor failure.
- `clear` requires a paused mailbox. It discards its queued user messages,
  returning a count; it does not clear system messages or an in-flight message.
  Explicitly cleared messages are not sent to dead letters.
- `stop` and `restart` enqueue normal lifecycle commands; `accepted: true`
  means the command was queued, not that the lifecycle operation completed.
  Normal descendant termination and supervision semantics apply.
- Runtime actors created through internal/local APIs (guardians, system
  channels, and the dead-letter logger) are inspectable but cannot be managed.
  Actors created through public user, child, system, or temporary actor
  factories can be managed. An actor disappears from the registry on termination.

With `console` disabled, the registry, message history, and mailbox control
locks are not compiled. Enabling it adds per-message inspection overhead and
bounded history storage even before an endpoint is started. JSON serialization
adds further overhead when both `console` and `serde` are enabled.

## Typed JSON sends

`send` looks up the target actor's registered decoder and deserializes the JSON
into **that actor's own message type**, then uses the normal message-send path.
Messages do not need to implement `Clone`. The JSON representation follows
the message type's serde derives or custom implementation.

For example, an enum declared as:

```rust
#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum WorkerMsg {
    Greet { name: String },
    Count(u32),
}
```

accepts `{"Greet":{"name":"Picard"}}` or `{"Count":42}`. A `u32` actor accepts
`42`, and a `String` actor accepts `"hello"`. Messages queued while paused are
processed only after resume. A missing actor, unavailable decoder, invalid JSON
message type, or failed delivery produces an error response. `serde` is not
required merely to inspect or manage actors.

## WebSocket protocol

Connect to `ws://HOST:PORT/console` with the HTTP upgrade header:

```text
Authorization: Bearer TOKEN
```

The listener accepts at most 16 concurrent clients. Upgrade authentication
has a two-second read timeout. Requests must be text frames containing JSON
and are limited to 64 KiB. Responses are limited to one MiB.
Binary requests receive an error. Oversized WebSocket messages disconnect
the client. Ping/pong and close frames follow the normal WebSocket protocol.

Unknown paths, query parameters, missing/incorrect credentials, and upgrade
requests with an `Origin` header are rejected. This is a native administrative
client protocol, not a directly browser-accessible API.

Every request has an unsigned integer `id` and an `op`:

```json
{"id":1,"op":"actors","offset":0,"limit":100}
{"id":2,"op":"inspect","path":"/user/worker"}
{"id":3,"op":"send","path":"/user/worker","message":{"Count":42}}
```

Supported operations are `actors`, `inspect`, `messages`, `pause`, `resume`,
`clear`, `stop`, `restart`, and `send`. All except `actors` require `path`;
`send` additionally requires `message`.

`actors` defaults to `offset: 0` and `limit: 100`. Limits must be in `1..=100`.
Its result contains `actors`, `total`, and `next_offset` (null for the final
page). Actor creation/termination during paging can change the result.

Responses echo the request ID and contain exactly one of `result` and `error`:

```json
{"id":3,"result":{"accepted":true}}
{"id":4,"error":{"code":"invalid_message","message":"..."}}
```

Malformed requests use `id: null` if a valid ID cannot be recovered.
Error codes include `invalid_request`, `not_found`, `protected_actor`,
`not_paused`, `json_unavailable`, `invalid_message`, `delivery_failed`,
and `response_too_large`.
