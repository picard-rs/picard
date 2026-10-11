# Configuration

## Basics

Picard makes configuration straightforward by using two config files:

- `picard.toml` can be modified to change the behavior of Picard and various modules
- `app.*`, such as `app.toml` or `app.yml` for application specific settings

Configuration is loaded with the `config` crate, so any format it supports can be used.

When the system starts `picard.toml` is loaded and if an application config file exists it will be merged and deserialized into a single typed `PicardConfig`. This keeps settings separate for the purposes of maintenance and CI/CD, but makes it uniformly available at runtime.

The `PicardConfig` is accessible via the `ActorSystem` instance. Framework settings are typed fields (e.g. `sys.config().mailbox.msg_process_limit`), and application settings from the `[app]` section are in `app`:

```rust
let myval = sys.config().app["myval"].clone().into_string().unwrap();
```

This expects the following entry in `app.*`:

```toml
[app]
myval = "five by five"
```

## File Paths

By default Picard looks for `picard.toml` and `app.*` in the `config` directory relative to the current directory of execution.

It's standard practice to place the `config` directory in the root directory of your Rust application, i.e. on the same level as `src`. This way, `cargo run` and `cargo test` will use the files located in `config`.

Configuration file paths can be overridden using environment variables:

- `PICARD_CONF` to override `picard.toml` file path
- `APP_CONF` to override `app.*` file path

## Best Practises

### Default Values

Framework settings missing from the config files fall back to the defaults of `PicardConfig`. Invalid values (e.g. a zero pool size) are rejected when the system starts. Use `ActorSystem::with_config` with a `PicardConfig` to bypass file loading.

### Using .config()

It is highly encouraged in terms of function signatures to not pass around instances of `PicardConfig`. Not only is it slower but it makes understanding what a function does difficult. It is always better to extract values from configuration early on in your application and then pass those to functions. If you have many settings to pass to a function a dedicated settings struct could be used.

### Module Settings

It is encouraged to reuse the standard section names for modules. For example, if you have a custom logging module then use `[log]` in `picard.toml`. This makes it easier for people to locate these settings based on module behavior.

Example

```toml

[persistence]
redis_url = "tcp://127.0.0.1"
resis_pwd = "password123"
``` 

Or 

```toml
[persistence]
redis = { url = "tcp://127.0.0.1", pwd = "password123" }
```
