# Project: Actor Model Showcase — Living City

## Objective

Build an interactive, real-time traffic simulation called **Living City** that demonstrates the capabilities and advantages of our custom Actor Model framework.

The application should serve as both:

1. A visually impressive demonstration of thousands of independent actors communicating concurrently.
2. A technical showcase of the framework's architecture, performance, message passing, state isolation, and fault tolerance.

The demo should feel like a living system rather than a static benchmark. Users should be able to watch actors interact, inspect their internal state, introduce failures, increase system load, and observe how the framework behaves.

**Important:** This is a showcase for our existing actor framework, not an exercise in implementing a new one.

## 1. Framework Discovery

Before implementing anything:

- Inspect the repository and understand the existing actor framework.
- Identify its actor lifecycle, spawning mechanisms, messaging API, mailbox implementation, scheduling, and state management.
- Determine whether supervision, actor monitoring, actor termination, and distributed execution are supported.
- Identify the project's language, build system, dependencies, and conventions.
- Reuse the existing framework directly.
- Do not introduce another actor framework.
- Do not implement parallel actor abstractions or bypass the framework for core simulation logic.

Adapt the implementation to the framework's actual capabilities.

If a requested feature is unsupported, either implement a minimal, clearly isolated demonstration-level extension where appropriate or document the limitation. Do not present unsupported capabilities as working features.

## 2. Simulation Concept

Create a procedurally generated city consisting of:

- Roads and intersections
- Traffic lights
- Hundreds or thousands of vehicles
- Multiple city districts
- Vehicle spawning and destination points
- Dynamic congestion and traffic patterns

Each autonomous entity should be modeled as an actor.

### Vehicle Actors

Each vehicle maintains its own:

- Unique actor ID
- Position and velocity
- Current route and destination
- Driving state (moving, waiting, turning, etc.)
- Relevant local traffic information

Vehicles communicate with intersection actors to request permission to enter intersections.

Vehicles should make independent decisions based on messages and their own state.

### Intersection Actors

Each intersection:

- Maintains its own traffic queues and occupancy state.
- Receives requests from vehicles.
- Coordinates safe passage.
- Communicates with its traffic light actor.
- Responds to congestion and traffic conditions.

### Traffic Light Actors

Traffic lights independently manage their signal states.

They should:

- Transition between red, yellow, and green.
- Communicate with intersection actors.
- Optionally adjust timing based on traffic conditions.
- Support simulated failures and recovery.

### District Actors

Divide the city into logical districts.

District actors coordinate district-level activity and aggregate statistics.

They should demonstrate hierarchical actor organization and provide natural boundaries for scaling experiments.

### Simulation Supervisor

A top-level actor manages the simulation lifecycle.

Responsibilities include:

- Initializing the city
- Spawning actors
- Managing simulation start, pause, and reset
- Coordinating shutdown
- Monitoring actor health where supported
- Initiating recovery where supported

Use the framework's native supervision and lifecycle facilities wherever possible.

## 3. Visualization

Build a polished, browser-based visualization.

The interface should have a modern, technical aesthetic suitable for a framework demonstration or conference presentation.

### Main City View

Display an animated, top-down city map.

Requirements:

- Vehicles move smoothly through roads.
- Traffic lights visibly change state.
- Intersections show traffic congestion.
- Vehicles can be selected for inspection.
- Camera supports zooming and panning.
- The simulation remains responsive under significant actor load.

Use Canvas or WebGL if necessary for rendering performance.

Rendering must be decoupled from simulation execution. The browser should consume snapshots or updates rather than directly controlling individual actor state.

### Actor Inspector

Clicking an actor should display:

- Actor ID and type
- Current state
- Mailbox depth, if available
- Recent messages
- Messages sent and received
- Actor lifecycle status
- Relevant actor-specific information

Where practical, visualize communication between selected actors using animated connections or message pulses.

### Live Metrics Dashboard

Display real-time metrics:

- Active actor count
- Actors created and terminated
- Messages sent per second
- Messages processed per second
- Average and p95 message processing latency, if measurable
- Mailbox backlog, if available
- Simulation tick/update rate
- CPU and memory usage, if available
- Vehicle throughput and congestion

Distinguish actual runtime instrumentation from application-level approximations.

Do not invent or simulate framework performance measurements.

## 4. Interactive Demonstrations

Provide controls that allow users to explore the Actor Model interactively.

### Scenario A: Massive Concurrency

Allow the user to select a target vehicle count:

- 100
- 1,000
- 5,000
- 10,000

Spawn vehicle actors dynamically and visualize how the system behaves.

Show actor count, throughput, latency, and responsiveness.

Avoid blocking the UI during large actor creation bursts.

### Scenario B: Message Passing

Allow users to select a vehicle and watch its interactions with traffic lights and intersections.

Display a live message timeline showing events such as:

- RequestIntersectionEntry
- EntryGranted
- EntryDenied
- TrafficSignalChanged
- VehicleExitedIntersection

Show the sender, receiver, message type, and timestamp.

The purpose is to make asynchronous actor communication understandable.

### Scenario C: Fault Injection

Provide a **Kill Actor** button.

Allow users to terminate a selected vehicle, intersection, or traffic light actor.

Demonstrate:

- Whether unrelated actors continue functioning.
- How failures are detected.
- Whether supervision restarts failed actors.
- How dependent actors respond.

If the framework supports supervision, demonstrate automatic recovery.

If it does not, show the failure behavior accurately rather than simulating framework-level recovery.

### Scenario D: Traffic Surge

Provide a button that generates a large traffic surge in one district.

Demonstrate:

- Increased message traffic
- Mailbox accumulation
- Congestion propagation
- Backpressure behavior, if supported
- Continued responsiveness in unaffected districts

### Scenario E: Dynamic Actor Lifecycle

Allow users to spawn and remove entire city districts at runtime.

Show the corresponding changes in actor count and message throughput.

## 5. Architecture Requirements

The simulation must genuinely use the custom actor framework.

Core principles:

**State isolation:** Each actor owns its state. Avoid shared mutable simulation state between actors.

**Message passing:** Inter-actor coordination must use the framework's messaging primitives.

**Concurrency:** Independent actors should execute through the framework's scheduler rather than a single manually implemented actor loop.

**Lifecycle:** Actor creation, termination, and cleanup should use framework facilities.

**Fault handling:** Use native supervision and monitoring where available.

**Observability:** Instrument actual actor and message activity without significantly distorting performance.

The visualization should not be responsible for the simulation's business logic.

A possible architecture:

```
┌───────────────────────────────┐
│       Browser Dashboard       │
│  City View / Metrics / Debug  │
└──────────────┬────────────────┘
               │
         WebSocket / API
               │
┌──────────────▼────────────────┐
│     Visualization Gateway     │
│  Snapshots / Events / Control │
└──────────────┬────────────────┘
               │
┌──────────────▼────────────────┐
│      Simulation Supervisor    │
└──────────────┬────────────────┘
               │
       District Actors
               │
   ┌───────────┼───────────┐
   │           │           │
Vehicles   Intersections  Traffic
                           Lights
               │
         Actor Runtime
```

This is a conceptual architecture. Adapt it to the framework rather than forcing unnecessary layers.

## 6. Performance and Correctness

Performance is an important part of this demonstration.

Design for thousands of active actors while keeping the UI usable.

Consider:

- Efficient actor scheduling
- Batched visualization updates
- Rendering only visible entities
- Avoiding excessive message logging
- Bounded history buffers
- Configurable instrumentation
- Efficient actor cleanup

Do not make every actor send high-frequency updates directly to the browser.

The simulation should maintain basic traffic invariants, such as preventing conflicting vehicle movements through the same intersection.

Avoid artificial concurrency claims. Measure and report actual behavior.

Include a reproducible load test that records actor count, message throughput, latency, CPU usage, and memory consumption.

## 7. UI and Presentation

Aim for a polished developer-tool experience.

Suggested layout:

**Center:** Large animated city simulation.

**Left panel:** Simulation controls, actor counts, and scenario selection.

**Right panel:** Selected actor inspector and message timeline.

**Bottom panel:** Live throughput, latency, mailbox, and resource charts.

Include:

- Dark theme
- Clear color-coded actor states
- Smooth animations
- Real-time charts
- Helpful tooltips
- Scenario presets
- Pause/resume and reset controls

The application should be understandable within 30 seconds of opening it.

Provide a **Demo Tour** that walks through concurrency, message passing, fault isolation, and scaling using guided scenarios.

## 8. Implementation Strategy

Implement incrementally.

### Phase 1 — Framework Integration

Inspect the framework, identify supported features, and build a minimal actor-based simulation with vehicles and intersections.

Verify that actors actually communicate using framework messages.

### Phase 2 — Working Simulation

Implement road networks, vehicle movement, intersection coordination, and traffic lights.

Ensure the simulation behaves correctly before adding extensive visualization.

### Phase 3 — Interactive UI

Build the city renderer, simulation controls, and actor inspector.

### Phase 4 — Observability

Add real runtime metrics, message tracing, and performance charts.

### Phase 5 — Advanced Scenarios

Implement load testing, traffic surges, fault injection, and lifecycle demonstrations.

### Phase 6 — Polish and Validation

Improve visual quality, performance, documentation, and demo usability.

## 9. Deliverables

Produce:

1. A runnable demo application integrated with the existing actor framework.
2. A polished interactive browser UI.
3. A set of predefined demonstration scenarios.
4. Runtime instrumentation and performance metrics.
5. Automated tests covering key simulation invariants and actor interactions.
6. A README explaining architecture, setup, and execution.
7. A short demo walkthrough suitable for presenting the framework to other developers.

Provide a straightforward development command to start the demo.

Use existing repository conventions and dependencies where practical.

## 10. Acceptance Criteria

The demo is complete when:

- A city simulation runs using actual framework actors.
- Hundreds of independent actors communicate concurrently.
- Vehicle movement and traffic coordination are visible.
- Users can inspect actors and their messages.
- Runtime metrics update live.
- Users can increase and decrease actor load.
- Fault injection demonstrates the framework's actual failure semantics.
- The interface remains responsive during normal demonstrations.
- The application can be launched locally with documented commands.
- Unsupported framework capabilities are clearly identified rather than faked.

## Final Direction

Prioritize **demonstrating the Actor Model over building a sophisticated traffic game**.

The city is a visual metaphor for a concurrent system.

Every vehicle represents isolated state.

Every intersection demonstrates message-driven coordination.

Every traffic surge demonstrates system load.

Every actor failure demonstrates isolation and, where supported, supervision.

The result should make developers immediately understand what the Actor Model provides, how this framework implements it, and why they might want to use it.

**Start by e0,**

**3**

**xploring the repository and framework API, then implement the smallest working vertical slice. Continue iteratively until the complete demo is runnable and validated.**
