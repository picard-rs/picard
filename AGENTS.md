# Project notes for agents

Picard is a Rust actor framework implementing the Actor Model, supervision,
publish/subscribe channels, scheduling, and CQRS/event-sourcing patterns. The
workspace also contains reusable patterns, test helpers, a runtime console, and
the `living-city` browser demo. Project documentation is in `web/docs`; the
website is at <https://picard.rs>.

## Agent rules

- Do not change `AGENTS.md` or `CLAUDE.md` unless explicitly asked. This request
  explicitly authorizes creating/updating `AGENTS.md`.
- Never run `git push` unless explicitly told to.
- Do not commit to `main`. If the current branch is `main`, ask the user before
  changing files whether to create a branch or make the changes without
  committing.
- Always run `cargo fmt --all` after changing file contents. Do not pass
  formatting options (`--config`, `--edition`, etc.) or invoke `rustfmt`
  directly; settings come from [rustfmt.toml](rustfmt.toml) and `Cargo.toml`.
- Use conventional commits with one of `feat`, `fix`, `perf`, `refactor`,
  `chore`, `docs`, `test`, or `dev`; use `!` and a `BREAKING CHANGE:` footer
  for API breaks.
  - `dev` is for tooling, debugging aids, and test-flakiness work that does not
    change shipped behavior.
  - `bump` and `release` are maintainer-only commit types; do not create
    release commits unless asked.
- End commit messages with this co-author trailer:
  `Co-Authored-By: Copilot <223556219+Copilot@users.noreply.github.com>`.
- Inform the user about commits and other Git changes, including the commit
  message.
- Commit changes after each step unless the user says not to or `.NOCOMMIT`
  exists in the project root. This applies to documentation, CI, and
  configuration changes as well as code.
  - Changes affecting compilation or tests (Rust code, Cargo manifests or
    lockfiles, build scripts, or test data) may only be committed after
    `cargo clippy` and `cargo test` pass.
  - Documentation, CI, editor, and deployment configuration changes do not
    require those checks before committing.
  - Check the current branch with `git branch --show-current` before every
    commit.
- Commit formatting-only changes separately as `chore: cargo fmt` when
  `cargo fmt` reformats code that was not otherwise changed.
- Add user-visible changes to the `Unreleased` section of
  [CHANGELOG.md](CHANGELOG.md), using Keep a Changelog format. Create the
  section if needed.

## Workspace layout

The Rust 2024 Cargo workspace uses resolver 3. Its members are under `crates/`:

| Crate | Purpose |
| --- | --- |
| `picard` | Runtime, actors, system, kernel, mailboxes, channels, timer, logger, configuration |
| `picard-macros` | `#[actor]` attribute macro |
| `picard-patterns` | Reusable patterns such as `ask` and `transform` |
| `picard-cqrs` | CQRS and event-sourcing support |
| `picard-testkit` | Test helpers such as probes |
| `picard-console` | Runtime console application |

Other important locations:

- `demo/` contains the `living-city` browser-based simulation.
- `config/picard.toml` and `config/app.toml` provide default configuration.
- `examples/` contains examples.
- `tests/*.rs` at the repository root are legacy tests and are not compiled;
  active tests are in crate `tests/` directories and in-source test modules.
- `web/docs/` is the mdBook documentation and should be updated with relevant
  public API or configuration changes.

## Core architecture

- `crates/picard/src/lib.rs` defines `Message`, `LocalMessage`, `AnyMessage`,
  `Envelope`, and the public actor prelude.
- `crates/picard/src/system.rs` defines `ActorSystem`, configuration, and
  validation.
- `crates/picard/src/system/timer.rs` implements the deadline-heap timer.
  It sleeps until the next deadline on native targets; on wasm, `WasmTimerRef`
  is driven by host timeouts. Use `picard::platform::Instant` in code that must
  run on WebAssembly, never `std::time::Instant`.
- `crates/picard/src/platform.rs` provides target-specific `Instant` and
  `Executor` types: native uses `std::time::Instant` and a futures thread pool;
  wasm uses `web_time::Instant` and a single-threaded browser-event-loop
  executor.
- `crates/picard/src/system/logger.rs` implements the slog-based
  non-blocking logger. `stats` adds padded atomic runtime counters, and
  `tracing` adds lifecycle, panic, supervision, and mailbox-receive events.
  Both features are documented in `web/docs/diagnostics.md`; verify them with
  `cargo test -p picard --all-features`.
- `crates/picard/src/actor/` contains actors, references, paths, channels, and
  selection.
- `crates/picard/src/kernel/` contains actor execution, mailboxes, queues, and
  the provider/path registry. Each actor has separate user and system
  crossbeam queues and a kernel task. `Dock` stores the actor behind
  `Arc<Mutex<Option<A>>>`, allowing restart to replace the actor instance.
- `crates/picard/src/console.rs` and `console/` implement the optional runtime
  console. It is distinct from actor remoting.
- The optional `remoting` feature provides native WebSocket JSON messaging,
  authenticated peers, actor-path dispatch, and custom bidirectional
  `RemoteTransport` support. It does not currently provide transparent remote
  `ActorRef`s, remote `Sender` replies, discovery, failover, or exactly-once
  delivery. See [cluster.md](web/docs/cluster.md).

With the `serde` feature, user-facing `Message` types must implement
`Serialize` and `DeserializeOwned`; the runtime itself only requires
`LocalMessage`. The bound applies when users create actors through
`ActorRefFactory`, `TmpActorRefFactory`, and public `sys_actor_of*` methods.
Picard's internal actors can carry non-serializable runtime values and are
created through internal local-actor paths. `#[actor]` and CQRS `EntityMsg`
use the hidden `__actor_msg_enum!` macro to add serde derives. Keep test,
benchmark, and example actor messages serializable under this feature; pass
channels and actor references through actor factory arguments rather than
embedding them in messages. The `living-city` demo does not support
`picard/serde`; see [messaging.md](web/docs/messaging.md).

Actor panics are caught on native targets: the mailbox is suspended by
`Sentinel`, and the parent receives `SystemMsg::Failed` for supervision.
`wasm32-unknown-unknown` aborts on panic, so `Context::fail()` reports failures
without panicking there. The repository's `.cargo/config.toml` selects the
`getrandom` wasm JS backend required to build that target.

Core runtime types including `ActorSystem`, `SystemMsg`, `KernelRef`, and
`MailboxSender` are `Send + Sync` without `unsafe`; tests assert this property.
Do not add `unsafe impl Send` or `unsafe impl Sync`.

## Configuration

`PicardConfig` is typed and includes `debug`, `log`, `mailbox`, `dispatcher`,
`cqrs`, and `app: HashMap<String, config::Value>`. Defaults are supplied by
`better_default`; `validate_config` rejects invalid values such as zero mailbox
processing limits or dispatcher pool size. `load_config()` reads `config/picard`
and `config/app` relative to the working directory (the config crate resolves
supported file extensions); `PICARD_CONF` and `APP_CONF` override those paths.
`ActorSystem::with_config` accepts a `PicardConfig` directly. There is no
scheduler configuration because the timer does not poll at a configured
interval. Keep [config.md](web/docs/config.md) in sync with configuration
changes.

## Commands

```sh
cargo build --workspace --all-targets
cargo test --workspace --no-default-features
cargo test -p picard --all-features
cargo clippy --all -- -D warnings
cargo fmt --all
```

Additional useful checks and tasks:

```sh
cargo test --workspace
cargo test --workspace --exclude living-city --features picard/serde
cargo clippy --workspace --all-targets -- -D warnings
cargo run -p living-city --release
cargo run -p living-city --release -- loadtest
wasm-pack build demo --release --target web --out-dir web/pkg --no-default-features
cargo bench -p picard --bench runtime
cargo bench -p picard --bench actor_suite
```

Run commands that load configuration from the repository root or set
`PICARD_CONF` and `APP_CONF` to explicit paths. The `living-city` demo does not
support `picard/serde`.

Workspace lint levels are configured in `[workspace.lints]` in the root
[Cargo.toml](Cargo.toml); new crates must include `[lints] workspace = true`.
Change lint levels there, not in CI or command-line overrides.

## Conventions

- Use descriptive variable and function names so code explains itself.
- Comment only code that needs clarification; keep comments short.
- Add shared dependencies to `[workspace.dependencies]` in the root
  [Cargo.toml](Cargo.toml) and use `dep.workspace = true` in member crates.
- Use `thiserror` in libraries and `anyhow` only in binaries or tools.
- Use `tracing` for logging. Add Tracy spans only in performance-critical code.
- Preserve type safety and existing patterns; surface failures explicitly.
- Do not add tests to the root `tests/` directory because Cargo does not compile
  them as part of this workspace.
- Preserve existing line endings when editing files that use CRLF.

## Code reviews

For reviews of commits, branches, diffs, or specific files, report every
finding as an entry in a list with this exact shape:

- **Location:** `file:line` (use a range such as `file:120-146` when the
  finding spans several lines)
- **Category:** one of `Blocking`, `FYI`, or `Nit` (use categories such as
  `Question` only when none of those fit)
- **Comment:** what could be improved and the concrete problem or risk,
  including the failure scenario when claiming a defect, rather than an
  abstract concern

Order findings by severity, with `Blocking` first. Use `Blocking` for defects,
unbounded resource use, and violations of these rules that must change before
merge; use `FYI` for design consequences and risks worth a conscious decision;
use `Nit` for style or cleanliness. Verify claims against the code and run
`cargo clippy --all -- -D warnings` and the relevant `cargo test` commands
before reporting findings. State when a finding is a possibility rather than a
confirmed defect.
