# Living City

An interactive traffic simulation where every car, junction, traffic light and district is a
[Picard](../README.md) actor. The brief this implements is in [README.md](README.md).

## Run it

```sh
cargo run -p living-city --release            # then open http://127.0.0.1:8080
cargo run -p living-city --release -- --port 9000 --vehicles 1000 --host 0.0.0.0
```

Arguments are parsed with Clap. `--help` and `loadtest --help` describe each
mode; malformed values and unknown arguments return an error instead of falling
back to defaults. Server options can also be set with `LIVING_CITY_HOST`,
`LIVING_CITY_PORT`, and `LIVING_CITY_VEHICLES`. Load-test options use
`LIVING_CITY_LOADTEST_VEHICLES` (comma-separated counts) and
`LIVING_CITY_LOADTEST_SECS`. Command-line options override environment variables.
IPv6 hosts are supported, for example `--host ::1`.

### Distributed districts

Remote district support is opt-in and is not part of the default build. It
uses Picard's authenticated WebSocket remoting:

```sh
LIVING_CITY_REMOTE_TOKEN="$(openssl rand -hex 32)" \
  cargo run -p living-city --release --features remoting -- \
  --district 0 --web-ui --host 0.0.0.0 \
  --connect 1=ws://127.0.0.1:9101/remoting

LIVING_CITY_REMOTE_TOKEN="<same-token>" \
  cargo run -p living-city --release --features remoting -- \
  --district 1 \
  --connect 0=ws://127.0.0.1:9100/remoting
```

Each process runs only the district given by `--district` (0 through 8).
`--connect DISTRICT=ENDPOINT` may be repeated to connect to each other process;
connections discover the remote actor-system ID over the authenticated
connection. `--web-ui` chooses which process serves the browser UI; without it,
a distributed process runs headless until Ctrl-C. The bearer token is required
and should be provided with `LIVING_CITY_REMOTE_TOKEN`, not a command-line
argument. The listener defaults to `0.0.0.0:9100` and can be changed with
`--remote-listen` or `LIVING_CITY_REMOTE_LISTEN`. Set `--vehicles` per district
in distributed mode. Browser controls are forwarded to connected supervisors,
and district snapshots are relayed to peers for aggregation at the UI gateway.

The transport is plain WebSocket by default. Use a private/trusted network or
terminate TLS at a trusted reverse proxy and secure the proxy-to-process path.
All peers configured as outgoing connections are allow-listed by the same
shared bearer token. A process needs a peer entry for every other process for
the mesh to exchange commands and snapshots. Each district
owns and simulates its local junctions and vehicles. When a vehicle's next
hop lies in a district of another process, it migrates: it sends a
`VehicleTransfer` (route, speed, trip count) to the remote supervisor and
stops, and the remote district spawns a continuing vehicle on that edge.
Vehicles in flight to a district that goes down are lost, and district
populations drift from their target as vehicles migrate. Transparent remote
`ActorRef`s are not implemented.

With [just](https://just.systems), run these from `demo/`: `just up` creates a
git-ignored `.env` with a random shared token (if missing) and starts the
nine containers; `just down`, `just restart`, `just logs [service]` and
`just ps` manage them. The UI is published on host port 8080; set
`LIVING_CITY_PORT` (for example in `.env`) if that port is taken. `just console [district]` opens the Picard console of that district
(default 0): each container runs the console on its own loopback address, which
is never published, and `picard-console` ships in the image and is run via
`docker compose exec` using `LIVING_CITY_CONSOLE_TOKEN` from `.env`. `just run` and `just loadtest` start the single-process
demo. Without `just`, build and run the nine-container example manually:

```sh
export LIVING_CITY_REMOTE_TOKEN="$(openssl rand -hex 32)"
docker compose -f demo/compose.yml up --build
```

The compose file starts one container per district, connects them over the
private Compose network, and publishes only the district-0 web UI on port 8080.
For production, supply a secret from a secret manager rather than a shell
environment file.

### Runtime console

Living City's `console` and `serde` features forward to `picard/console` and
`picard/serde`. Enable `console` for inspection and management, and additionally
enable `serde` for typed JSON sends:

```sh
# Set PICARD_CONSOLE_TOKEN to the same strong, unpredictable token in both terminals.
cargo run -p living-city --release --features console,serde -- \
  --console-listen 127.0.0.1:9000
cargo run -p picard-console -- ws://127.0.0.1:9000/console
```

The endpoint is opt-in: enabling the feature alone does not open a console port.
`--console-listen IP:PORT` (or `PICARD_CONSOLE_LISTEN`) accepts only loopback
addresses. Port `0` selects an available port, printed at startup. A bearer token
is required when a listen address is supplied. Set `PICARD_CONSOLE_TOKEN` rather
than using `--console-token`, which can expose the token in shell history and
process listings. Tokens must contain at least 32 ASCII letters, digits, `-` or
`_`; environment token values are hidden in Clap help.

Both options are available only with `console` and work in either mode, including:

```sh
cargo run -p living-city --release --features console,serde -- \
  loadtest --vehicles 1000,5000 --secs 20 --console-listen 127.0.0.1:9000
```

Instead of a separate port, `--console-route` (or `PICARD_CONSOLE_ROUTE=true`)
serves the console at `/console` on the web server itself, routing requests
through Picard's transport-agnostic `ActorSystem::console()` service:

```sh
cargo run -p living-city --release --features console,serde -- --console-route
cargo run -p picard-console -- ws://127.0.0.1:8080/console
```

The route requires the same token, a loopback `--host`, and the web server (it
cannot be combined with `loadtest`). It rejects browser upgrades carrying an
`Origin` header, limits request sizes, and allows 16 concurrent clients. Both
endpoints can be enabled together.

If `picard-console` reports `Unable to connect`, no endpoint is listening: build
with `--features console` and pass `--console-listen` or `--console-route`.

The console handle remains alive for the whole server or load-test run. Startup
and shutdown errors are reported, and the listener and actor system are stopped
when the run finishes. The runtime console is separate from the browser's `/ws`
visualization endpoint. Use a secure tunnel for remote access.

`picard-console` opens a full-screen TUI: filter with `/` (for example
`/supervisor`), select an actor, and press `s` to send it JSON, `p` to pause or
resume it, or `?` for all keys. Pass `--repl` for the line-oriented REPL.

Example console commands (REPL prompt or the TUI's `:` command bar):

```text
inspect /user/supervisor
send /user/supervisor {"Pause":true}
send /user/supervisor {"SetVehicles":1000}
send /user/supervisor {"Inspect":{"id":"v-1"}}
send /user/supervisor {"Pause":false}
```

Data/control message variants support their normal serde JSON representation.
Variants carrying actor references are deliberately excluded from serialization
and deserialization: vehicle `Directory`/`Inspect`; junction
`RequestIntersectionEntry`/`Inspect`/`InspectLight`; light `Inspect`; district
`Directory`/`NeedDirectory`/`Inspect`/`SystemEvent`; supervisor `DistrictReady`;
and both observer variants. They continue to work as ordinary in-process messages.
JSON sends of these variants return `invalid_message`; console history records a
`payload_error` for them.

Tick messages serialize `dt` and `report`, not the process-local monotonic
`sent` timestamp. JSON deserialization initializes `sent` to the current local
time. Normal typed tick delivery keeps its original timestamp and latency
measurements unchanged. Console history/serialization adds overhead; compare
load tests with the same feature configuration.

See the [runtime console documentation](../web/docs/console.md) for protocol,
limits, completion, and mailbox control semantics.

### In the browser (WebAssembly)

The same simulation also runs entirely in the browser, which is how it is published on GitHub
Pages (`/demo/`). Picard is compiled to `wasm32-unknown-unknown` and runs in a Web Worker:

```sh
wasm-pack build demo --release --target web --out-dir web/pkg --no-default-features
python3 -m http.server -d demo/web 8000       # then open http://localhost:8000
```

Differences from the native server: there are no threads, so all actors share one thread
(about 150k messages/s with 5,000 vehicles in headless Chromium); CPU and memory are not
available; **Crash actor** reports the failure with `Context::fail()` instead of panicking
(wasm aborts on panic), and the parent restarts the actor as usual.

Click **Demo Tour** for a guided walk through concurrency, message passing, fault isolation,
scaling and a traffic surge.

## Architecture

```text
/user
├── observer                       subscribes to system events and dead letters
├── gateway                        merges snapshots into frames (10 Hz)
└── supervisor                     20 Hz clock, scenarios, address book
    └── district-N-gG              one per active district (3x3 grid, 6x6 junctions each)
        ├── i-X-Y                  junction: capacity 3, one axis at a time, FIFO queues
        │   └── light              traffic light, adaptive green, heartbeat to the junction
        └── v-ID                   vehicle: route planning, retries, rerouting
```

- **Time** is driven by a timer in the supervisor, which fans `Tick` out to districts, which fan
  out to junctions and vehicles. Vehicles never share state; they ask a junction for entry and
  wait for `EntryGranted` or `EntryDenied`.
- **Safety**: a junction admits at most 3 vehicles and never mixes axes. Requests are
  idempotent and grants carry a lease, so a restarted junction or a dead vehicle cannot wedge
  it. `violations` must stay 0 (a unit test fuzzes this and the integration tests assert it).
- **Addresses**: an immutable directory of junction refs is rebuilt by the supervisor when the
  topology changes and pushed to districts and vehicles.
- **Snapshots** carry a district generation. The gateway accepts only snapshots from
  active generations, and districts ignore reports from removed junctions, so delayed
  reports cannot bring terminated actors back into the visualization.
- **Web layer** (`src/server.rs`) is thin: it reads the latest frame published by the gateway
  actor through a `tokio::sync::watch` channel and turns browser commands into supervisor
  messages. It cannot touch actor state, and rendering is fully decoupled from simulation.

## Selecting actors

Click a vehicle or junction to inspect it. **Ctrl-click** (or Cmd-click) adds or removes actors
from a multi-selection, and dragging a box over the map selects everything inside it (hold Ctrl to
add to the current selection). With several actors selected, **Kill** and **Crash** apply to all
of them at once. Pan with Shift-drag or the right mouse button.

## Scenarios

| Scenario              | UI                                   | What happens                                                               |
| --------------------- | ------------------------------------ | -------------------------------------------------------------------------- |
| A Massive concurrency | 100 / 1,000 / 5,000 / 10,000 buttons | Vehicle actors are spawned in batches of 250 per district                  |
| B Message passing     | Click a vehicle                      | Inspector shows live sent/received messages with sender, receiver and time |
| C Fault injection     | **Kill actor** / **Crash actor** (also for a multi-selection) | See below                                                                  |
| D Traffic surge       | **Surge in selected district**       | 800 vehicles head for that district's centre                               |
| E Dynamic lifecycle   | **Add / Remove selected** district   | A whole subtree is spawned or stopped; actor count and message rate follow |

### Failure semantics (what Picard really does)

| Target   | Kill (actor stops itself)                                                                      | Crash (panic, parent's strategy `Restart`)                           |
| -------- | ---------------------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| Vehicle  | Gone. The district removes it on the termination event. Others unaffected.                     | Restarted by its district with the same id at its spawn node.        |
| Light    | Junction notices missing heartbeats after 3 s, runs a fixed-time fallback, respawns it in 8 s. | Restarted by the junction.                                           |
| Junction | Not restarted. Vehicles time out, block the node and reroute around it.                        | Restarted by its district; queues are lost and retries rebuild them. |

Removing a district stops all of its descendants through Picard's hierarchical stop.

## Metrics: what is real

- **Runtime**: terminated/restarted counts and dead letters come from Picard's system event
  stream and dead-letter channel; CPU and memory come from `/proc`.
- **App-level**: everything else is counted by the demo at its own send/receive sites. Picard
  only publishes `ActorCreated` for direct children of `/user`, so the created count is also
  taken at the spawn sites.
- **Latency**: tick delay is the real time a tick message waits between being sent and being
  handled, sampled on about 1 in 16 vehicles; handler time is measured inside `recv`.
- Both are labelled in the UI.

## Tests

```sh
cargo test -p living-city              # unit tests + end-to-end scenarios with real actors
cargo test -p living-city --all-features # also CLI/console and serde tests
```

`demo/tests/simulation.rs` covers: trips complete with zero safety violations, killing and
crashing vehicles, light and junction faults (including rerouting around a dead junction),
adding and removing districts, scaling up and down, surge isolation, the inspector and pause.

## Load test

```sh
cargo run -p living-city --release -- loadtest --vehicles 1000,5000,10000 --secs 20
```

Reference run on a 12-thread Linux machine (10 s windows, after a 5 s warm-up). Your numbers will differ.

| vehicles | actors | msgs sent/s | msgs processed/s | tick delay mean (µs) | tick delay p95 (µs) | ticks/s | CPU % | RSS MB |
| -------- | ------ | ----------- | ---------------- | -------------------- | ------------------- | ------- | ----- | ------ |
| 1,000    | 1,295  | 36,317      | 36,347           | 526                  | 1,024               | 20.0    | 18    | 17     |
| 5,000    | 5,295  | 160,626     | 160,656          | 2,068                | 4,096               | 20.0    | 78    | 52     |
| 10,000   | 10,295 | 317,030     | 317,060          | 4,127                | 8,192               | 20.0    | 156   | 87     |

## Limitations

- Vehicles move along edges without lane-level car following; congestion shows as junction queues.
- Picard exposes no mailbox depth and has no backpressure, so neither is shown or simulated.
  A surge shows up as tick delay and junction queues instead.
- Single process; no distributed execution.
- A terminated junction is not respawned automatically (that is the point of the demo: see
  vehicles route around it). Use **Reset** to rebuild the city.
- Tick-delay percentiles are log2 histogram buckets, so p95 is an upper bound within a factor of 2.
