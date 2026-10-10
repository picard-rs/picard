# Picard

[![Build status](https://github.com/picard-rs/picard/workflows/Build%20and%20run%20tests/badge.svg)](https://github.com/picard-rs/picard/actions?query=workflow%3A%22Build+and+run+tests%22)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![crates.io](https://meritbadge.herokuapp.com/picard)](https://crates.io/crates/picard)
[![Released API docs](https://docs.rs/picard/badge.svg)](https://docs.rs/picard)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![code style: prettier](https://img.shields.io/badge/code_style-prettier-ff69b4.svg?style=flat-square)](https://github.com/prettier/prettier)

## Overview

Picard is a framework for building modern, concurrent and resilient systems using the Rust language. Picard aims to make working with state and behavior in concurrent systems as easy and scalable as possible. The Actor Model has been chosen to realize this because of the familiar and inherent simplicity it provides while also providing strong guarantees that are easy to reason about. The Actor Model also provides a firm foundation for resilient systems through the use of the actor hierarchy and actor supervision.

Picard provides:

- An Actor based execution runtime
- Actor supervision to isolate and recover from failures
- A modular system
- Concurrency built on `futures::execution::ThreadPool`
- Publish/Subscribe messaging via actor channels
- Message scheduling
- Out-of-the-box, configurable, non-blocking logging
- Command Query Responsibility Segregation (CQRS)
- Easily run futures

[Website](https://picard.rs) | [API Docs](https://docs.rs/picard)

## Example

`Cargo.toml`:

```toml
[dependencies]
picard = "0.4.1"
```

`main.rs`:

```rust
use std::time::Duration;
use picard::actors::*;

#[derive(Default)]
struct MyActor;

// implement the Actor trait
impl Actor for MyActor {
    type Msg = String;

    fn recv(&mut self,
                _ctx: &Context<String>,
                msg: String,
                _sender: Sender) {

        println!("Received: {}", msg);
    }
}

// start the system and create an actor
fn main() {
    let sys = ActorSystem::new().unwrap();

    let my_actor = sys.actor_of::<MyActor>("my-actor").unwrap();

    my_actor.tell("Hello my actor!".to_string(), None);

    std::thread::sleep(Duration::from_millis(500));
}
```

## Associated Projects

Official crates that provide additional functionality:

- [picard-cqrs](https://github.com/picard-rs/picard-cqrs): Command Query Responsibility Separation support
- [picard-testkit](https://github.com/picard-rs/picard-testkit): Tools to make testing easier
- [picard-patterns](https://github.com/picard-rs/picard-patterns): Common actor patterns, including `transform!` and 'ask'

## Benchmarks

Run the Picard runtime benchmarks in release mode with:

```bash
cargo bench -p picard --bench runtime
```

The benchmark target measures typed and untyped message throughput (including
mailbox processing), actor round-trip latency, and actor creation.

## Actor Demo

Run the actor efficiency demo in release mode for a more representative
comparison:

```bash
cargo run --release -p picard --example actor_demo
```

It computes the same CPU-heavy workload once on the calling thread and once
split across worker actors, then prints elapsed time and speedup. Actor startup
is excluded from the timings, and the results are checked for equality. This
illustrates how actors can improve throughput for independent, substantial
work; message-passing overhead means smaller tasks may not be faster.

## Roadmap & Currently in Development

The next major theme on the project roadmap is clustering and location transparency:

- Remote actors
- Support for TCP and UDP
- Clustering (using vector clocks)
- Distributed data (CRDTs)

## Why Picard

Picard is a full-featured actor model implementation that scales to hundreds or thousands of microservices and that equally can run exceptionally well on resource limited hardware to drive drones, IoT and robotics. The Rust language makes this possible.

Rust empowers developers with control over memory management, requiring no garbage collection and runtime overhead, while also providing modern semantics and expressive syntax such as the trait system. The result is a language that can solve problems equally for Web and IoT.

Picard adds to this by providing a familiar actor model API which in turn makes concurrent, resilient systems programming easy.

## Rust Version

Picard is currently built using the **latest Rust Nightly**.

## Contributing

Picard is looking for contributors - join the project! You don't need to be an expert in actors, concurrent systems, or even Rust. Great ideas come from everyone.

There are multiple ways to contribute:

- Documentation. Our aim is to make concurrent, resilient systems programming available to everyone and that starts with great Documentation.
- Additions to Picard code base. Whether small or big, your Pull Request could make a difference.
- Patterns, data storage and other supporting crates. We are happy to link to and provide full credit to external projects that provide support for databases in Picard's event storage model or implementations of common actor patterns.

### pre-commit

Before you commit your code [pre-commit](https://pre-commit.com/) integrates as a git hook to automatically check your code.
Please don't skip git hooks (even if you do the travis TravisCI build will still fail).

There are two different approaches you can use to run pre-commit

#### direct approach

```bash
pre-commit run -a
```

#### with yarn or npm

```bash
yarn
yarn lint
```

```bash
npm run install
npn run lint
```
