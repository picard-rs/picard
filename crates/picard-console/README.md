# picard-console

An interactive terminal REPL for applications using Picard's `console` feature.
It provides command and actor-path completion, history, colored prompts and
results, and actor/mailbox inspection and management.

Start your application's endpoint with `ActorSystem::start_console` (or route
`ActorSystem::console()` through your own WebSocket endpoint), set
`PICARD_CONSOLE_TOKEN` to its bearer token, then connect:

```sh
cargo run -p picard-console -- ws://127.0.0.1:9000/console
```

Use `help` for commands or `--help` for startup usage. `send PATH JSON` requires
the application's `console` and `serde` features; the client itself does not
require `serde` to be enabled on Picard.

The client and server require loopback connections. Use a secure tunnel for
remote applications. Tokens are not accepted as arguments or URL parameters;
message-send commands are excluded from persistent history. Set `NO_COLOR` to
disable terminal colors.

See the [runtime console documentation](../../web/docs/console.md) for setup,
control semantics, message formats, limits, and the WebSocket protocol.
