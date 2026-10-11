//! Living City: a traffic simulation built entirely out of Picard actors.
//!
//! Actor hierarchy: `supervisor` -> `district-N-gG` -> junctions (`i-x-y`, each with a `light`
//! child) and vehicles (`v-id`). The web layer ([`server`] natively, [`wasm`] in the browser) only reads frames from the
//! [`gateway`] and sends commands to the supervisor.

pub mod city;
pub mod district;
pub mod gateway;
pub mod junction;
pub mod light;
#[cfg(feature = "server")]
pub mod loadtest;
pub mod metrics;
pub mod msg;
pub mod observer;
pub mod protocol;
pub mod sampler;
#[cfg(feature = "server")]
pub mod server;
pub mod sim;
pub mod supervisor;
pub mod trace;
pub mod vehicle;
#[cfg(target_arch = "wasm32")]
pub mod wasm;
