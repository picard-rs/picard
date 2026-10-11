# Picard: project notes for agents

Picard is an actor framework for Rust (Actor Model, supervision, pub/sub channels, scheduling, CQRS). Concurrency runs on `futures::executor::ThreadPool`. Website and docs: <https://picard.rs>.

## AGENT RULES
- you **MUST** never do `git push`, unless you're explicitly told to
- you **MUST** always run formatting tools such as `cargo fmt` after you changed the contents of a file
- you **MUST** use conventional commits when committing (`feat`, `fix`, `perf`, `refactor`, `chore`, `docs`), with `!` and a `BREAKING CHANGE:` footer for API breaks
- you **MUST** end commit messages with the trailer `Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>`
- you **MUST** inform the user about commits or any other changes to git, including the commit message
- you **SHOULD** commit your changes after every step, but only when `cargo clippy` and `cargo test` pass
- you **SHOULD** commit formatting-only changes separately from functional changes

## Workspace layout

Cargo workspace (`resolver = "3"`, `edition = "2024"`). Members under `crates/`:

| Crate             | Purpose                                                                          |
| ----------------- | -------------------------------------------------------------------------------- |
| `picard`          | Core runtime: actors, system, kernel, mailboxes, channels, timer, logger, config |
| `picard-macros`   | The `#[actor]` attribute macro; uses `syn` 2                                     |
| `picard-patterns` | Reusable patterns (`ask`, `transform`)                                           |
| `picard-cqrs`     | CQRS / event sourcing support                                                    |
| `picard-testkit`  | Test helpers (`probe`)                                                           |

Other top-level items:
- `config/picard.toml`, `config/app.toml`: default configuration files.
- `examples/*.rs`, `crates/picard/examples/actor_demo.rs`: runnable examples.
- `tests/*.rs` at the repo root are legacy integration tests and are **not part of any workspace package**, so they are not compiled. Real tests live in `crates/*/tests` and in in-source `#[cfg(test)]` modules.
- `web/`: mdBook documentation (`web/docs`) and the front page. Keep `web/docs/config.md` in sync with config changes.
- `public/benchmarks`: separate nested git repo (CMake project comparing Picard with CAF and others). It may be absent from the checkout.

## Core source map (`crates/picard/src`)

- `lib.rs`: `AnyMessage`, `Envelope`, `load_config()`, and the `actors` prelude.
- `system.rs`: `ActorSystem`, `SystemBuilder`, `SystemMsg`/`SystemEvent`, the typed config structs and `validate_config`.
- `system/timer.rs`: `BasicTimer`, a deadline-heap timer thread that sleeps until the next job and exits when its sender is dropped.
- `system/logger.rs`: slog-based non-blocking logger.
- `actor.rs`, `actor/`: `Actor` trait, `Props`, `ActorRef`/`BasicActorRef`, `ActorCell`, paths and URIs, channels, selection.
- `kernel.rs`, `kernel/`: per-actor kernel task, `mailbox.rs` (user and system queues, scheduling flags), `queue.rs` (crossbeam channel), `provider.rs` (actor creation, path registry), `kernel_ref.rs`.
- `validate.rs`: actor name and path validation (plain char checks, no regex).

## Configuration

`PicardConfig` is typed (`debug`, `log`, `mailbox`, `dispatcher`, `cqrs`, and `app: HashMap<String, config::Value>`). Defaults come from the `better_default` derive. `load_config()` reads `config/picard` and `config/app` (override with the `PICARD_CONF` and `APP_CONF` env vars). `ActorSystem::with_config` takes a `PicardConfig`; `validate_config` rejects zero or invalid values. There is no scheduler config: the timer needs no polling interval.

## Design facts worth knowing

- Each actor has a user queue and a system queue (lock-free crossbeam channels), a `Dock` that holds the actor behind `Arc<Mutex<Option<A>>>` (restart swaps the instance), and a kernel task fed by an unbounded futures channel.
- Panics inside an actor are caught, the mailbox is suspended by `Sentinel`, and the parent receives `SystemMsg::Failed` to apply its supervision strategy.
- The path registry is a `DashSet<ActorPath>`; creating an actor whose path exists returns `CreateError::AlreadyExists`.
- Core types (`ActorSystem`, `SystemMsg`, `KernelRef`, `MailboxSender`) are `Send + Sync` without `unsafe`; a test asserts this. Do not add `unsafe impl Send/Sync`.

## Commands

```sh
cargo build --workspace --all-targets
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings   # must stay clean
cargo fmt --all
cargo bench -p picard --bench runtime                   # Criterion: message throughput, round trip, creation
cargo bench -p picard --bench actor_suite               # Criterion: CAF-style benchmark suite
```

Indicative Criterion measurements (one machine, not a controlled benchmark): `actor_creation` about 4.5 µs per actor, `actor_round_trip` about 8 µs, `typed_tell` about 33 ns.

## Conventions

- Style: `rustfmt.toml` (4 spaces, max width 100). Comment only code that needs clarification.
- Warnings are treated as errors (`-D warnings`).
- For a quick check run `cargo clippy --workspace --all-targets -- -D warnings` and `cargo test -p picard`. Do not add tests to the root `tests/` directory, since it is not compiled.
- Config files are resolved relative to the current working directory (`config/picard`, `config/app`), so run examples and tests from the repository root or set `PICARD_CONF`/`APP_CONF`.
- Some files use CRLF line endings (for example `crates/picard-patterns/config/picard.toml`); preserve them when editing.

## Known gaps

- Clippy pedantic noise is untouched (`use_self`, missing `# Errors` docs, `#[must_use]`).
- Actor creation uses much more memory than CAF in the benchmark comparison, and `mixed_case` is about 2x slower than CAF.
- `.github/workflows` and `.pre-commit-config.yaml` still reflect the pre-workspace layout (`./`, `./picard-macros/`), and the workflows only trigger on `master`.
