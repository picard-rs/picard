**What is Picard's goal?**

Picard's principle goal is to be a framework to make concurrent, resilient software development easy and to make the Actor Model applicable to a wide spectrum of platforms.

Picard achieves this by:

- Exposing a simple `Actor` trait
- Actor supervision based on the actor hierarchy
- Allowing Actors to own their state so `Sync` is not required
- Providing mailbox delivery within a running process (not durable or exactly-once delivery)
- Providing a configurable, modular system
- Optional CQRS entity management through the separate `picard-cqrs` crate
- Low memory footprint
- Fast execution
- No `unsafe` code (with the exception of `impl Send` and `impl Sync`)

**Is Picard a remake of Akka for Rust?**

Picard is inspired by [Akka](https://akka.io/), the JVM's popular Actor Model implementation. However it's not Picard's goal to recreate Akka for Rust. Other environments have done this, for example C#'s Akka.NET but the differences of Rust and Scala/Java and others require a fresh Rust-specific approach. And since Picard works well on resource limited hardware such as single board computers there are new challenges and problems to solve that are not applicable to Akka.

**How do actors compare to Futures?**

Both Actors and Futures in general work to solve the challenges of concurrent software design. However they are complementary since they are solving different issues.

Futures allow software developers to write concurrent code in an almost single-threaded manner. Simple, short-lived workload processing that is non-blocking can be easily written. So Futures can be seen as building blocks for non-blocking application design.

Actors on the other hand are used when state exists and must be maintained between cycles of when an actor is running and when it is idle. Actors are well suited for long running processes that can even span the lifetime of the application itself. Actors also form a hierarchy, with parent and child actors, which communicate with each other via a message based API. Since Actors are aware of other Actors in the system they can heal each other when unexpected things happen, such as panics.

Under the covers, Picard actors are futures. Native builds use a thread pool;
WebAssembly builds dispatch tasks on the browser event loop.

**How do actors compare to threads?**

Actors are scheduled when they have messages to process. On native targets,
the dispatcher runs actor tasks on its thread pool; idle actors do not occupy a
thread. The browser event loop is used on WebAssembly.

It's possible to have thousands or millions of actors that share just two CPU core/threads.

**Can Picard communicate across hosts?**

The optional [`remoting` feature](cluster.md) provides authenticated WebSocket
message delivery between Picard systems. It is not transparent clustering:
there is no discovery, failover, remote actor-reference conversion, or
cross-host coordination to ensure one active actor per entity. The separate
`picard-cqrs` crate manages entities within one actor system; applications
that distribute entity ownership must provide that coordination themselves.

**Can multiple `ActorSystem` instances run in the same app?**

It is possible to create two or more instances using `ActorSystem::new` but there is little benefit in doing so. A single instance is already optimized to run actors on the configured dispatcher which generally uses all cores of a CPU.

An exception to this advice is when an application is handling greatly varying workloads and it is not possible due to architecture constraints to break into multiple applications.

**Does Picard use `unsafe` code?**

The Picard runtime crates currently contain no `unsafe` blocks or unsafe trait
implementations. This describes the project source, not its dependencies.

**Are there any benchmarks?**

The repository includes Criterion benchmarks for message throughput, actor
round-trip latency, actor creation, mailbox workloads, scheduling, and other
scenarios. Run them with `cargo bench -p picard --bench runtime` or
`cargo bench -p picard --bench actor_suite`. Results depend on hardware and
workload; benchmark numbers should not be treated as a general throughput
guarantee.
