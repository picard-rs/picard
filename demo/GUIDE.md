# Living City

An interactive traffic simulation where every car, junction, traffic light and district is a
[Picard](../README.md) actor. The brief this implements is in [README.md](README.md).

## Run it

```sh
cargo run -p living-city --release            # then open http://127.0.0.1:8080
cargo run -p living-city --release -- --port 9000 --vehicles 1000 --host 0.0.0.0
```

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
- **Web layer** (`src/server.rs`) is thin: it reads the latest frame published by the gateway
  actor through a `tokio::sync::watch` channel and turns browser commands into supervisor
  messages. It cannot touch actor state, and rendering is fully decoupled from simulation.

## Scenarios

| Scenario                | UI                                       | What happens                                                                    |
| ----------------------- | ---------------------------------------- | ------------------------------------------------------------------------------- |
| A Massive concurrency   | 100 / 1,000 / 5,000 / 10,000 buttons     | Vehicle actors are spawned in batches of 250 per district                       |
| B Message passing       | Click a vehicle                          | Inspector shows live sent/received messages with sender, receiver and time      |
| C Fault injection       | **Kill actor** / **Crash actor**         | See below                                                                       |
| D Traffic surge         | **Surge in selected district**           | 800 vehicles head for that district's centre                                    |
| E Dynamic lifecycle     | **Add / Remove selected** district       | A whole subtree is spawned or stopped; actor count and message rate follow      |

### Failure semantics (what Picard really does)

| Target     | Kill (actor stops itself)                                                                  | Crash (panic, parent's strategy `Restart`)                       |
| ---------- | ------------------------------------------------------------------------------------------ | ---------------------------------------------------------------- |
| Vehicle    | Gone. The district removes it on the termination event. Others unaffected.                 | Restarted by its district with the same id at its spawn node.    |
| Light      | Junction notices missing heartbeats after 3 s, runs a fixed-time fallback, respawns it in 8 s. | Restarted by the junction.                                       |
| Junction   | Not restarted. Vehicles time out, block the node and reroute around it.                    | Restarted by its district; queues are lost and retries rebuild them. |

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
