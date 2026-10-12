#![cfg(all(feature = "server", not(target_arch = "wasm32")))]

use std::process::{Command, Output};

fn cli(arguments: &[&str], environment: &[(&str, &str)]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_living-city"));
    for variable in [
        "LIVING_CITY_HOST",
        "LIVING_CITY_PORT",
        "LIVING_CITY_VEHICLES",
        "LIVING_CITY_LOADTEST_VEHICLES",
        "LIVING_CITY_LOADTEST_SECS",
        "PICARD_CONSOLE_LISTEN",
        "PICARD_CONSOLE_TOKEN",
    ] {
        command.env_remove(variable);
    }
    command
        .args(arguments)
        .envs(environment.iter().copied())
        .output()
        .unwrap()
}

#[test]
fn help_documents_clap_arguments_and_feature_gate() {
    let output = cli(&["--help"], &[]);
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    for name in [
        "--host",
        "--port",
        "--vehicles",
        "loadtest",
        "LIVING_CITY_HOST",
    ] {
        assert!(help.contains(name), "{help}");
    }
    assert_eq!(help.contains("--console-listen"), cfg!(feature = "console"));
    let output = cli(&["loadtest", "--help"], &[]);
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    assert!(help.contains("--secs") && help.contains("1000,5000,10000"));
}

#[test]
fn invalid_numeric_and_environment_values_report_errors() {
    for (arguments, environment) in [
        (vec!["--vehicles", "bad"], vec![]),
        (vec!["loadtest", "--vehicles", "1,bad,2"], vec![]),
        (vec!["loadtest", "--secs", "bad"], vec![]),
        (vec![], vec![("LIVING_CITY_PORT", "bad")]),
        (
            vec!["loadtest"],
            vec![("LIVING_CITY_LOADTEST_VEHICLES", "1,bad")],
        ),
    ] {
        let output = cli(&arguments, &environment);
        assert!(!output.status.success());
        assert!(
            String::from_utf8(output.stderr)
                .unwrap()
                .contains("invalid value")
        );
    }
}

#[cfg(feature = "console")]
#[test]
fn environment_console_configuration_is_validated_without_exposing_token() {
    let secret = "0123456789abcdef0123456789abcdef";
    let output = cli(&["--help"], &[("PICARD_CONSOLE_TOKEN", secret)]);
    assert!(output.status.success());
    assert!(!String::from_utf8(output.stdout).unwrap().contains(secret));
    let output = cli(
        &[],
        &[
            ("PICARD_CONSOLE_LISTEN", "0.0.0.0:9000"),
            ("PICARD_CONSOLE_TOKEN", secret),
        ],
    );
    assert!(!output.status.success());
    let error = String::from_utf8(output.stderr).unwrap();
    assert!(error.contains("loopback"));
    assert!(!error.contains(secret));
    let output = cli(&["loadtest"], &[("PICARD_CONSOLE_LISTEN", "127.0.0.1:0")]);
    assert!(!output.status.success());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("--console-token")
    );
}
