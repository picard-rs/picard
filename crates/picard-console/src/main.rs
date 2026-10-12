#[cfg(not(target_arch = "wasm32"))]
mod repl;

#[cfg(not(target_arch = "wasm32"))]
fn main() -> std::process::ExitCode {
    match repl::run() {
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
