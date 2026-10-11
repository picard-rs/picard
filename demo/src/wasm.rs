//! WebAssembly entry point: the same simulation, driven from a Web Worker.
//!
//! There are no threads on `wasm32-unknown-unknown`, so every actor runs cooperatively on the
//! worker's event loop. The worker calls [`Demo::frame`] once per frame period and forwards
//! browser commands to [`Demo::command`]; the JSON is identical to the native WebSocket protocol.

use wasm_bindgen::prelude::*;

use crate::{
    protocol::{Bridge, hello},
    sim::Sim,
};

#[wasm_bindgen]
pub struct Demo {
    bridge: Bridge,
}

#[wasm_bindgen]
impl Demo {
    /// Starts the actor system with `vehicles` vehicles.
    #[wasm_bindgen(constructor)]
    pub fn new(vehicles: usize) -> Result<Demo, JsError> {
        std::panic::set_hook(Box::new(|info| {
            web_sys::console::error_1(&info.to_string().into());
        }));
        let sim = Sim::start(vehicles).map_err(|e| JsError::new(&e))?;
        Ok(Self {
            bridge: Bridge::new(sim),
        })
    }

    #[must_use]
    pub fn hello() -> String {
        hello()
    }

    pub fn command(&mut self, json: &str) {
        self.bridge.command(json);
    }

    pub fn frame(&mut self) -> String {
        self.bridge.step()
    }
}
