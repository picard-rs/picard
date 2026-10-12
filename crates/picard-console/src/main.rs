#[cfg(not(target_arch = "wasm32"))]
mod client;
#[cfg(not(target_arch = "wasm32"))]
mod command;
#[cfg(not(target_arch = "wasm32"))]
mod repl;
#[cfg(not(target_arch = "wasm32"))]
mod tui;

#[cfg(not(target_arch = "wasm32"))]
#[derive(clap::Parser)]
#[command(
    version,
    about = "Inspect and manage a Picard application through its console endpoint",
    after_help = "Set PICARD_CONSOLE_TOKEN to the server's bearer token; tokens are not accepted \
                  as arguments.\nThe full-screen TUI starts when stdin and stdout are terminals; \
                  otherwise, or with --repl, the line-oriented REPL reads commands from stdin."
)]
struct Cli {
    #[arg(help = "Console WebSocket URL, such as ws://127.0.0.1:9000/console")]
    url: String,

    /// Use the line-oriented REPL instead of the full-screen TUI.
    #[arg(long)]
    repl: bool,
}

#[cfg(not(target_arch = "wasm32"))]
fn run(cli: &Cli) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::IsTerminal;

    let token = std::env::var("PICARD_CONSOLE_TOKEN")
        .map_err(|_| "Set PICARD_CONSOLE_TOKEN to the server's bearer token")?;
    let client = client::Client::connect(&cli.url, &token)?;
    let interactive = std::io::stdin().is_terminal() && std::io::stdout().is_terminal();
    let color = std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none();
    if interactive && !cli.repl {
        tui::run(client, &cli.url, color)
    } else {
        repl::run(client, &cli.url)
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn main() -> std::process::ExitCode {
    let cli = <Cli as clap::Parser>::parse();
    match run(&cli) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("picard-console: {err}");
            std::process::ExitCode::FAILURE
        }
    }
}

#[cfg(target_arch = "wasm32")]
fn main() {
    panic!("picard-console requires a native terminal");
}
