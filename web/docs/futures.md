# Running Futures

Picard can execute and drive futures to completion. Internally, actors are
also executed as futures by the dispatcher. `ActorSystem` and `Context` both
provide a `run` method.

On native targets, Picard uses a futures thread pool. On WebAssembly it
dispatches tasks to the browser event loop instead. This example shows the
native API:

```rust,no_run
use futures::executor::block_on;
use picard::actors::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let system = ActorSystem::new()?;
    let handle = system.run(async { "someval".to_string() })?;
    assert_eq!(block_on(handle), "someval");
    Ok(())
}
```

`run` returns a `futures::future::RemoteHandle` for the future's output, or a
`SpawnError` if the executor cannot accept the task.

In the next section we'll see how to to test Picard applications.

[Testing](testing.md)
