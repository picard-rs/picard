//! Living City entry point.
//!
//! `living-city [--port N] [--vehicles N]` serves the demo.
//! `living-city loadtest [--vehicles 1000,5000] [--secs N]` runs headless and prints a table.

use std::{net::SocketAddr, process::ExitCode, time::Duration};

use living_city::{loadtest, server};

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

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

#[tokio::main]
async fn main() -> ExitCode {
    install_panic_hook();
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.first().is_some_and(|a| a == "loadtest") {
        let sizes: Vec<usize> = flag(&args, "--vehicles")
            .unwrap_or_else(|| "1000,5000,10000".into())
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let secs = flag(&args, "--secs")
            .and_then(|s| s.parse().ok())
            .unwrap_or(20);
        let sizes = if sizes.is_empty() { vec![1000] } else { sizes };
        println!("{}", loadtest::run(&sizes, Duration::from_secs(secs)).await);
        return ExitCode::SUCCESS;
    }

    let port: u16 = flag(&args, "--port")
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);
    let vehicles: usize = flag(&args, "--vehicles")
        .and_then(|p| p.parse().ok())
        .unwrap_or(400);
    let host = flag(&args, "--host").unwrap_or_else(|| "127.0.0.1".into());
    let Ok(addr) = format!("{host}:{port}").parse::<SocketAddr>() else {
        eprintln!("invalid address {host}:{port}");
        return ExitCode::FAILURE;
    };
    match server::serve(addr, vehicles).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
