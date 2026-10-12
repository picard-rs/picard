//! Living City entry point: web server or headless load test.

use std::{
    net::{IpAddr, SocketAddr},
    process::ExitCode,
    time::Duration,
};

use clap::{Args, Parser, Subcommand};
#[cfg(feature = "remoting")]
use living_city::remoting::PeerSpec;
use living_city::{loadtest, server, sim::Sim};

#[derive(Parser)]
#[command(version, about = "Living City actor-based traffic simulation")]
struct Cli {
    #[command(flatten)]
    server: ServerArgs,

    #[command(subcommand)]
    command: Option<Command>,

    /// Start the authenticated runtime console at this loopback IP:PORT.
    #[cfg(feature = "console")]
    #[arg(long, env = "PICARD_CONSOLE_LISTEN", global = true, value_parser = loopback_address, requires = "console_token")]
    console_listen: Option<SocketAddr>,

    /// Console bearer token; prefer `PICARD_CONSOLE_TOKEN` to avoid process-list exposure.
    #[cfg(feature = "console")]
    #[arg(
        long,
        env = "PICARD_CONSOLE_TOKEN",
        global = true,
        hide_env_values = true
    )]
    console_token: Option<String>,

    /// Also serve the console at `/console` on the HTTP server (requires a loopback --host).
    #[cfg(feature = "console")]
    #[arg(long, env = "PICARD_CONSOLE_ROUTE", requires = "console_token")]
    console_route: bool,

    /// Run only this district in a distributed simulation.
    #[cfg(feature = "remoting")]
    #[arg(long, value_parser = parse_district)]
    district: Option<usize>,

    /// Expose the browser UI from this district process.
    #[cfg(feature = "remoting")]
    #[arg(long, requires = "district")]
    web_ui: bool,

    /// Listen for authenticated remote actor messages.
    #[cfg(feature = "remoting")]
    #[arg(
        long,
        env = "LIVING_CITY_REMOTE_LISTEN",
        default_value = "0.0.0.0:9100",
        requires = "district"
    )]
    remote_listen: SocketAddr,

    /// Shared peer token; use `LIVING_CITY_REMOTE_TOKEN` rather than a CLI value in deployments.
    #[cfg(feature = "remoting")]
    #[arg(
        long,
        env = "LIVING_CITY_REMOTE_TOKEN",
        hide_env_values = true,
        requires = "district"
    )]
    remote_token: Option<String>,

    /// Connect to another district, in DISTRICT=ws://HOST:PORT/remoting form; may be repeated.
    #[cfg(feature = "remoting")]
    #[arg(long, value_name = "DISTRICT=ENDPOINT", requires = "district")]
    connect: Vec<PeerSpec>,
}

#[cfg(feature = "remoting")]
fn parse_district(value: &str) -> Result<usize, String> {
    let district = value
        .parse::<usize>()
        .map_err(|err| format!("expected a district number from 0 to 8: {err}"))?;
    (district < living_city::city::DISTRICT_COUNT)
        .then_some(district)
        .ok_or_else(|| {
            format!(
                "district must be less than {}",
                living_city::city::DISTRICT_COUNT
            )
        })
}

#[derive(Args)]
struct ServerArgs {
    /// HTTP server IP address.
    #[arg(long, env = "LIVING_CITY_HOST", default_value = "127.0.0.1")]
    host: IpAddr,

    /// HTTP server port (zero selects an available port).
    #[arg(long, env = "LIVING_CITY_PORT", default_value_t = 8080)]
    port: u16,

    /// Initial number of vehicles.
    #[arg(long, env = "LIVING_CITY_VEHICLES", default_value_t = 400)]
    vehicles: usize,
}

#[derive(Subcommand)]
enum Command {
    /// Run a headless load test and print a Markdown results table.
    Loadtest {
        /// Comma-separated vehicle counts to test.
        #[arg(
            long,
            env = "LIVING_CITY_LOADTEST_VEHICLES",
            value_delimiter = ',',
            default_value = "1000,5000,10000",
            num_args = 1
        )]
        vehicles: Vec<usize>,

        /// Measurement duration in seconds for each vehicle count.
        #[arg(long, env = "LIVING_CITY_LOADTEST_SECS", default_value_t = 20)]
        secs: u64,
    },
}

#[cfg(feature = "console")]
fn loopback_address(value: &str) -> Result<SocketAddr, String> {
    let address: SocketAddr = value
        .parse()
        .map_err(|err| format!("expected IP:PORT: {err}"))?;
    if !address.ip().is_loopback() {
        return Err(
            "console must use a loopback address; use a secure tunnel for remote access".into(),
        );
    }
    Ok(address)
}

fn install_panic_hook() {
    // Injected faults panic on purpose; keep the console readable for the demo.
    let default = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let injected = info
            .payload()
            .downcast_ref::<String>()
            .is_some_and(|m| m.starts_with("fault injected"));
        if !injected {
            default(info);
        }
    }));
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    install_panic_hook();
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<(), String> {
    #[cfg(any(feature = "console", feature = "remoting"))]
    validate_cli(&cli)?;
    let vehicles = if cli.command.is_some() {
        0
    } else {
        cli.server.vehicles
    };
    #[cfg(feature = "remoting")]
    let distributed = cli.district.is_some();
    #[cfg(feature = "remoting")]
    let sim = match cli.district {
        Some(district) => Sim::start_remote(
            vehicles,
            district,
            cli.remote_listen,
            cli.remote_token.clone().expect("validated remote token"),
            &cli.connect,
        )?,
        None => Sim::start(vehicles)?,
    };
    #[cfg(not(feature = "remoting"))]
    let sim = Sim::start(vehicles)?;
    let system = sim.system.clone();
    #[cfg(feature = "console")]
    let console = match cli.console_listen {
        Some(bind) => {
            let options = picard::console::ConsoleOptions {
                bind,
                token: cli
                    .console_token
                    .clone()
                    .expect("Clap requires a token when a listen address is supplied"),
            };
            match system.start_console(options) {
                Ok(handle) => {
                    println!(
                        "Living City console at ws://{}/console",
                        handle.local_addr()
                    );
                    Some(handle)
                }
                Err(err) => {
                    system
                        .shutdown()
                        .await
                        .map_err(|err| format!("simulation shutdown failed: {err}"))?;
                    return Err(format!("cannot start console: {err}"));
                }
            }
        }
        None => None,
    };

    let result = match cli.command {
        Some(Command::Loadtest { vehicles, secs }) => {
            println!(
                "{}",
                loadtest::run_sim(&sim, &vehicles, Duration::from_secs(secs)).await
            );
            Ok(())
        }
        None => {
            #[cfg(feature = "remoting")]
            let headless = distributed && !cli.web_ui;
            #[cfg(not(feature = "remoting"))]
            let headless = false;
            serve(sim, &cli, headless).await
        }
    };
    #[cfg(feature = "console")]
    let console_result = console
        .map_or(Ok(()), picard::console::ConsoleHandle::stop)
        .map_err(|err| format!("console shutdown failed: {err}"));
    let shutdown = system
        .shutdown()
        .await
        .map_err(|err| format!("simulation shutdown failed: {err}"));
    #[cfg(feature = "console")]
    console_result?;
    shutdown?;
    result
}

async fn serve(sim: Sim, cli: &Cli, headless: bool) -> Result<(), String> {
    if headless {
        #[cfg(feature = "remoting")]
        println!(
            "Living City district {} running headless",
            cli.district.unwrap_or_default()
        );
        #[cfg(not(feature = "remoting"))]
        println!("Living City running headless");
        tokio::signal::ctrl_c()
            .await
            .map_err(|err| format!("cannot wait for shutdown signal: {err}"))?;
        Ok(())
    } else {
        let addr = SocketAddr::new(cli.server.host, cli.server.port);
        #[cfg(feature = "console")]
        if cli.console_route {
            let token = cli
                .console_token
                .as_deref()
                .expect("Clap requires a token for the console route");
            server::serve_sim_with_console(addr, sim, token).await
        } else {
            server::serve_sim(addr, sim).await
        }
        #[cfg(not(feature = "console"))]
        server::serve_sim(addr, sim).await
    }
}

#[cfg(any(feature = "console", feature = "remoting"))]
fn validate_cli(cli: &Cli) -> Result<(), String> {
    #[cfg(feature = "console")]
    if cli.console_route && cli.command.is_some() {
        return Err(
            "--console-route requires the web server and cannot be used with loadtest".into(),
        );
    }
    #[cfg(feature = "remoting")]
    let distributed = cli.district.is_some();
    #[cfg(feature = "remoting")]
    if distributed && cli.command.is_some() {
        return Err("distributed districts cannot be combined with the loadtest command".into());
    }
    #[cfg(feature = "remoting")]
    if !distributed && (!cli.connect.is_empty() || cli.web_ui) {
        return Err("--connect and --web-ui require --district".into());
    }
    #[cfg(feature = "remoting")]
    if distributed && cli.remote_token.as_deref().is_none_or(str::is_empty) {
        return Err("distributed mode requires --remote-token or LIVING_CITY_REMOTE_TOKEN".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clap_configuration_is_valid() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
    }

    #[test]
    fn defaults_and_typed_arguments() {
        let cli = Cli::try_parse_from(["living-city"]).unwrap();
        assert_eq!(cli.server.host, "127.0.0.1".parse::<IpAddr>().unwrap());
        assert_eq!(cli.server.port, 8080);
        assert_eq!(cli.server.vehicles, 400);
        assert!(cli.command.is_none());
        let cli = Cli::try_parse_from([
            "living-city",
            "--host",
            "::1",
            "--port",
            "0",
            "--vehicles",
            "42",
        ])
        .unwrap();
        assert_eq!(cli.server.host, "::1".parse::<IpAddr>().unwrap());
        assert_eq!(cli.server.port, 0);
        assert_eq!(cli.server.vehicles, 42);
    }

    #[test]
    fn loadtest_counts_are_not_silently_filtered() {
        let cli = Cli::try_parse_from([
            "living-city",
            "loadtest",
            "--vehicles",
            "0,10,20",
            "--secs",
            "2",
        ])
        .unwrap();
        let Some(Command::Loadtest { vehicles, secs }) = cli.command else {
            panic!("Expected loadtest")
        };
        assert_eq!(vehicles, [0, 10, 20]);
        assert_eq!(secs, 2);
        for arguments in [
            vec!["living-city", "--port", "invalid"],
            vec!["living-city", "--port", "65536"],
            vec!["living-city", "--host", "invalid"],
            vec!["living-city", "--vehicles"],
            vec!["living-city", "loadtest", "--vehicles", "1,invalid"],
            vec!["living-city", "loadtest", "--vehicles", ""],
            vec!["living-city", "loadtest", "--secs", "-1"],
            vec!["living-city", "--unknown"],
        ] {
            assert!(Cli::try_parse_from(arguments).is_err());
        }
    }

    #[cfg(feature = "remoting")]
    #[test]
    fn distributed_arguments_are_feature_gated_and_validated() {
        let cli = Cli::try_parse_from([
            "living-city",
            "--district",
            "2",
            "--web-ui",
            "--connect",
            "3=ws://district-3:9100/remoting",
            "--remote-token",
            "secret",
        ])
        .unwrap();
        assert_eq!(cli.district, Some(2));
        assert!(cli.web_ui);
        assert_eq!(cli.connect[0].district, 3);
        assert_eq!(cli.connect[0].endpoint, "ws://district-3:9100/remoting");
        assert!(Cli::try_parse_from(["living-city", "--district", "9"]).is_err());
        assert!(
            Cli::try_parse_from(["living-city", "--district", "1", "--connect", "bad"]).is_err()
        );
    }

    #[cfg(not(feature = "remoting"))]
    #[test]
    fn distributed_arguments_are_unavailable_without_feature() {
        assert!(Cli::try_parse_from(["living-city", "--district", "0"]).is_err());
        assert!(Cli::try_parse_from(["living-city", "--web-ui"]).is_err());
    }

    #[cfg(feature = "console")]
    #[test]
    fn console_arguments_are_global_and_validate_loopback() {
        for arguments in [
            vec![
                "living-city",
                "--console-listen",
                "127.0.0.1:0",
                "--console-token",
                "test-token",
            ],
            vec![
                "living-city",
                "loadtest",
                "--console-listen",
                "[::1]:9000",
                "--console-token",
                "test-token",
            ],
            vec![
                "living-city",
                "--console-listen",
                "127.0.0.1:0",
                "--console-token",
                "test-token",
                "loadtest",
            ],
        ] {
            let cli = Cli::try_parse_from(arguments).unwrap();
            assert!(cli.console_listen.unwrap().ip().is_loopback());
        }
        for arguments in [
            vec!["living-city", "--console-listen"],
            vec!["living-city", "--console-listen", "invalid"],
            vec!["living-city", "--console-listen", "0.0.0.0:9000"],
        ] {
            assert!(Cli::try_parse_from(arguments).is_err());
        }
    }

    #[cfg(not(feature = "console"))]
    #[test]
    fn console_arguments_are_unavailable_without_feature() {
        assert!(Cli::try_parse_from(["living-city", "--console-listen", "127.0.0.1:0"]).is_err());
        assert!(Cli::try_parse_from(["living-city", "--console-token", "token"]).is_err());
    }
}
