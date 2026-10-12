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
There is no console listener or terminal client on WebAssembly.

The endpoint exposes application internals and destructive controls. Treat
the token as an administrative credential. Remote use should go through a
secure tunnel to the loopback port; the endpoint does not provide TLS.

## Custom transports

Applications that already run a server can expose the console through their own
endpoint instead of calling `start_console`. `ActorSystem::console()` returns a
cloneable, transport-agnostic `Console` service that executes protocol requests:

```rust
use picard::{actors::ActorSystem, console::Console};

fn console_service(sys: &ActorSystem) -> Console {
    sys.console()
}

fn on_console_text(console: &Console, text: &str) -> String {
    // `text` comes from an authenticated client of your own server.
    console.handle_text(text)
}
```

- `handle_text(&str) -> String` parses one JSON request and returns the JSON
  response. Malformed requests and requests larger than `MAX_REQUEST_BYTES`
  (64 KiB) produce `invalid_request`; responses above `MAX_RESPONSE_BYTES` are
  replaced by `response_too_large`.
- `handle(Request) -> Response` is the typed equivalent. `Response::to_json`
  applies the same response size limit.
- The service is synchronous and may run user serializers, so async servers
  should call it from a blocking task (for example `tokio::task::spawn_blocking`).

The service performs no authentication. Your transport must authenticate clients
before routing requests, cap request sizes, and limit concurrent clients.
`console::is_valid_token` checks a bearer token against the built-in endpoint's
policy. To stay compatible with `picard-console`, serve WebSocket JSON text frames,
require `Authorization: Bearer <token>`, and reject browser upgrades that carry
an `Origin` header. Living City's `--console-route` (in `demo/src/server.rs`) is
an axum example.

## Terminal client

The `picard-console` workspace crate provides a terminal client:

```sh
cargo run -p picard-console -- ws://127.0.0.1:9000/console
```

Set `PICARD_CONSOLE_TOKEN` in the client's environment to the same token used
by the server. The client does not accept tokens as command-line arguments
or URL parameters.

When stdin and stdout are terminals, the client opens a full-screen TUI. Pass
`--repl` for the line-oriented REPL, which is also used automatically when input
or output is redirected (for example in scripts).

### TUI

The TUI shows the actor tree with queue lengths and mailbox state, the selected
actor's details, and its recent message events (newest first). It refreshes
every second. The tree starts collapsed below the root; filtering shows all matches.

```text
Up/Down, j/k   Select an actor (PgUp/PgDn, Home/End jump)
Right/l, Left/h  Expand / collapse the actor's children (Left on a leaf selects its parent)
/              Filter actors by path (Enter keeps, Esc clears)
:              Enter a command (Tab completes, Up/Down history)
p              Pause or resume the selected actor's mailbox
c              Clear the paused mailbox (asks for confirmation)
s              Send a JSON message to the selected actor
r              Restart the selected actor (asks for confirmation)
x              Stop the selected actor (asks for confirmation)
F5, Ctrl-L     Refresh now
?              Toggle help
q, Ctrl-C      Quit
```

`s` opens the command bar prefilled with `send PATH `; type the JSON payload and
press Enter. The command bar accepts the REPL commands below. Its history is
kept in memory only and never includes `send` commands. Colors follow
`NO_COLOR`.

### REPL

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
peers
send @district-1 /user/supervisor {"SetVehicles":100}
stop /user/worker
help
quit
```

Tab completes commands and actor paths. Up/Down recall history, Ctrl-C cancels
input, and Ctrl-D disconnects. Prompts, hints, results, and errors use terminal
colors; `NO_COLOR` disables them, and redirected output is uncolored.

History is stored at `$XDG_STATE_HOME/picard-console/history`, or
`$HOME/.local/state/picard-console/history`. `send` commands are never saved
because payloads may be sensitive.

### Errors

Connection and protocol errors are explicit; there is no automatic reconnect
or retry of destructive operations. If no endpoint is listening, the client
reports `Unable to connect` with a hint: check that the application was built
with `console` and that it started an endpoint (the feature alone opens no port).

### Living City

The Living City demo forwards its `console` and `serde` features to Picard.
Start its optional endpoint with `--console-listen 127.0.0.1:9000` and set
`PICARD_CONSOLE_TOKEN` in the environment:

```sh
cargo run -p living-city --release --features console,serde -- \
  --console-listen 127.0.0.1:9000
```

The same option works with `loadtest`. Alternatively, `--console-route` serves
the console at `/console` on the demo's own web server (loopback `--host` only):

```sh
cargo run -p living-city --release --features console,serde -- --console-route
cargo run -p picard-console -- ws://127.0.0.1:8080/console
```

See the [Living City guide](../../demo/GUIDE.md)
for Clap environment options and its JSON message compatibility.

## Inspection and management semantics

- `actors` lists registered actors, including runtime actors. Each entry has
  its absolute path, parent path, Rust message type, JSON-send capability,
  management capability, and mailbox state. The client follows actor-list pages
  automatically and refreshes path completion before each prompt (REPL) or on
  every refresh (TUI).
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

## Sending to remote actors

`send @PEER PATH JSON` forwards the JSON message to the actor at `PATH` on a remote
system; `peers` lists the available `@PEER` names. The console only reaches peers the
application registers with `ActorSystem::register_console_peer(name, sender)`, where
`sender` hands the message to your remoting layer (for example a `RemoteConnection`).
The remote actor must be registered for remote delivery, and the response only says
the message was queued, not that the actor accepted it. In the wire protocol this is
`{"op":"send","peer":"district-1","path":"/user/gateway","message":{...}}` and
`{"op":"peers"}`. The Living City demo registers every `--connect` peer as `district-N`.

### Querying and managing remote actors

`@PEER COMMAND` runs any inspection or management command (`actors`, `inspect`, `messages`,
`pause`, `resume`, `clear`, `stop`, `restart`) on the console of a remote system, through the
system you are connected to: `@district-1 inspect /user/supervisor`. The application
registers the peer with `ActorSystem::register_console_peer_query(name, query)`, where
`query` carries a request as JSON text to the remote system and returns the text of
`Console::handle_text` there. Forwarded requests cannot be forwarded again, and the remote
console applies its own rules (for example, runtime actors stay protected). The wire form is
`{"op":"forward","peer":"district-1","command":{"op":"inspect","path":"/user/x"}}`.
Remote `actors` returns the first page only. Whoever holds a peer's shared token can use
this path, so it grants the same trust as sending messages to that peer.

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
`clear`, `stop`, `restart`, `send`, `forward`, and `peers`. All except `actors` and `peers`
require `path`; `send` additionally requires `message` and accepts an optional `peer`.

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
