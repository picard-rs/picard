# picard-console

An interactive terminal client for applications using Picard's `console`
feature. It opens a full-screen TUI with a live actor tree, actor and mailbox
details, recent message events, a command bar with completion, and one-key
pause, resume, clear, send, restart and stop. A line-oriented REPL with
completion, history and colored output is available with `--repl` and is used
automatically when stdin or stdout is not a terminal.

Start your application's endpoint with `ActorSystem::start_console` (or route
`ActorSystem::console()` through your own WebSocket endpoint), set
`PICARD_CONSOLE_TOKEN` to its bearer token, then connect:

```sh
cargo run -p picard-console -- ws://127.0.0.1:9000/console
```

Press `?` in the TUI or type `help` in the REPL for commands and keys, or use
`--help` for startup usage. `send PATH JSON` requires
the application's `console` and `serde` features; the client itself does not
require `serde` to be enabled on Picard.

The client and server require loopback connections. Use a secure tunnel for
remote applications. Tokens are not accepted as arguments or URL parameters.
TUI command history is kept in memory only; the REPL's persistent history
excludes message sends. Set `NO_COLOR` to disable terminal colors.

See the [runtime console documentation](../../web/docs/console.md) for setup,
control semantics, message formats, limits, and the WebSocket protocol.
