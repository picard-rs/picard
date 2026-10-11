// Runs the Living City simulation (Picard compiled to WebAssembly) off the main thread.
import init, { Demo } from "./pkg/living_city.js";

const FRAME_PERIOD_MS = 100;
let demo = null;

self.onmessage = async (e) => {
  if (e.data.start !== undefined) {
    try {
      await init();
      demo = new Demo(e.data.start);
      self.postMessage(Demo.hello());
      setInterval(() => self.postMessage(demo.frame()), FRAME_PERIOD_MS);
    } catch (err) {
      self.postMessage({ error: String(err) });
    }
  } else if (demo) {
    demo.command(e.data.text);
  }
};
