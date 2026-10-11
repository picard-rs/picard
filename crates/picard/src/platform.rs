//! Per-target building blocks: the clock and the executor.
//!
//! Native builds use `std::time::Instant` and a `futures` thread pool. On
//! `wasm32-unknown-unknown` threads and `std::time::Instant` are unavailable, so the
//! actors run cooperatively on the host's event loop instead.

#[cfg(not(target_arch = "wasm32"))]
pub use std::time::Instant;
#[cfg(target_arch = "wasm32")]
pub use web_time::Instant;

#[cfg(not(target_arch = "wasm32"))]
pub use futures::executor::{ThreadPool as Executor, ThreadPoolBuilder};

#[cfg(target_arch = "wasm32")]
pub use wasm::Executor;

#[cfg(target_arch = "wasm32")]
pub(crate) mod wasm {
    use futures::task::{FutureObj, Spawn, SpawnError};

    /// Single-threaded executor that hands every task to the browser's microtask queue.
    #[derive(Clone, Copy, Debug, Default)]
    pub struct Executor;

    impl Spawn for Executor {
        fn spawn_obj(&self, future: FutureObj<'static, ()>) -> Result<(), SpawnError> {
            wasm_bindgen_futures::spawn_local(future);
            Ok(())
        }
    }
}
