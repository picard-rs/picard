#![cfg(all(feature = "server", feature = "console", not(target_arch = "wasm32")))]

use std::{
    io::{BufRead, BufReader},
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::Duration,
};
#[cfg(unix)]
use std::{net::TcpStream, time::Instant};

use serde_json::{Value, json};
use tungstenite::{Message, client::IntoClientRequest, connect, stream::MaybeTlsStream};

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

struct Application(Child);

impl Drop for Application {
    fn drop(&mut self) {
        if self.0.try_wait().is_ok_and(|status| status.is_none()) {
            self.0.kill().unwrap();
        }
        self.0.wait().unwrap();
    }
}

impl Application {
    fn stop(&mut self) {
        #[cfg(unix)]
        {
            assert!(
                Command::new("kill")
                    .args(["-s", "INT", &self.0.id().to_string()])
                    .status()
                    .unwrap()
                    .success()
            );
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                if let Some(status) = self.0.try_wait().unwrap() {
                    assert!(status.success());
                    break;
                }
                assert!(Instant::now() < deadline, "Application did not shut down");
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        #[cfg(not(unix))]
        self.0.kill().unwrap();
        self.0.wait().unwrap();
    }
}

#[test]
fn cli_rejects_invalid_console_configuration() {
    for (arguments, token, expected) in [
        (vec!["--console-listen"], None, "a value is required"),
        (
            vec!["--console-listen", "invalid"],
            None,
            "expected IP:PORT",
        ),
        (vec!["--console-listen", "0.0.0.0:9000"], None, "loopback"),
        (
            vec!["--console-listen", "127.0.0.1:0"],
            None,
            "--console-token",
        ),
        (
            vec!["--console-listen", "127.0.0.1:0"],
            Some("short"),
            "cannot start console",
        ),
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_living-city"));
        command
            .args(arguments)
            .args(["--vehicles", "0"])
            .env_remove("PICARD_CONSOLE_TOKEN");
        if let Some(token) = token {
            command.env("PICARD_CONSOLE_TOKEN", token);
        }
        let output = command.output().unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8(output.stderr).unwrap().contains(expected));
    }
}

#[test]
fn cli_starts_authenticated_console_and_obeys_serde_gate() {
    let mut app = Application(
        Command::new(env!("CARGO_BIN_EXE_living-city"))
            .args([
                "--port",
                "0",
                "--vehicles",
                "0",
                "--console-listen",
                "127.0.0.1:0",
            ])
            .env("PICARD_CONSOLE_TOKEN", TOKEN)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let output = app.0.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(output).lines() {
            let line = line.unwrap();
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    let console = rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let url = console.strip_prefix("Living City console at ").unwrap();
    assert!(!console.contains(TOKEN));
    assert!(connect(url).is_err());
    let mut request = url.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("Authorization", format!("Bearer {TOKEN}").parse().unwrap());
    let (mut socket, _) = connect(request).unwrap();
    if let MaybeTlsStream::Plain(stream) = socket.get_ref() {
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
    }
    let mut send = |id, command: Value| {
        let mut command = command;
        command["id"] = json!(id);
        socket
            .send(Message::Text(command.to_string().into()))
            .unwrap();
        let Message::Text(text) = socket.read().unwrap() else {
            panic!("Expected text")
        };
        let response: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(response["id"], id);
        response
    };
    let inspect = send(1, json!({"op": "inspect", "path": "/user/supervisor"}));
    assert_eq!(inspect["result"]["json_send"], cfg!(feature = "serde"));
    let response = send(
        2,
        json!({"op": "send", "path": "/user/supervisor", "message": {"Pause": true}}),
    );
    if cfg!(feature = "serde") {
        assert_eq!(response["result"]["accepted"], true);
        let invalid = send(
            3,
            json!({"op": "send", "path": "/user/supervisor", "message": {"DistrictReady": {}}}),
        );
        assert_eq!(invalid["error"]["code"], "invalid_message");
    } else {
        assert_eq!(response["error"]["code"], "json_unavailable");
    }
    assert!(
        rx.recv_timeout(Duration::from_secs(5))
            .unwrap()
            .starts_with("Living City running at ")
    );
    drop(socket);
    app.stop();
    #[cfg(unix)]
    assert!(
        TcpStream::connect(
            url.strip_prefix("ws://")
                .unwrap()
                .strip_suffix("/console")
                .unwrap()
        )
        .is_err()
    );
    reader.join().unwrap();
}

#[test]
fn loadtest_accepts_console_and_measurement_options_from_environment() {
    let output = Command::new(env!("CARGO_BIN_EXE_living-city"))
        .arg("loadtest")
        .env("PICARD_CONSOLE_LISTEN", "127.0.0.1:0")
        .env("PICARD_CONSOLE_TOKEN", TOKEN)
        .env("LIVING_CITY_LOADTEST_VEHICLES", "0")
        .env("LIVING_CITY_LOADTEST_SECS", "0")
        .output()
        .unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(output.status.success(), "{stderr}");
    let console = stdout.lines().next().unwrap();
    let address = console
        .strip_prefix("Living City console at ws://")
        .unwrap()
        .strip_suffix("/console")
        .unwrap();
    assert!(std::net::TcpStream::connect(address).is_err());
    assert!(stdout.contains("| vehicles | actors |"), "{stdout}");
    assert!(!stdout.contains(TOKEN));
    assert!(!stderr.contains(TOKEN));
}

#[test]
fn console_route_rejects_invalid_configuration() {
    for (arguments, token, expected) in [
        (vec!["--console-route"], None, "--console-token"),
        (
            vec!["--console-route", "--host", "0.0.0.0"],
            Some(TOKEN),
            "loopback",
        ),
        (vec!["--console-route"], Some("short"), "console token"),
        (
            vec!["--console-route", "loadtest", "--secs", "0"],
            Some(TOKEN),
            "requires the web server",
        ),
    ] {
        // Default vehicles keep the simulation busy, so startup errors also exercise shutdown
        // while actors are still being created.
        let mut command = Command::new(env!("CARGO_BIN_EXE_living-city"));
        command
            .args(["--port", "0"])
            .args(arguments)
            .env_remove("PICARD_CONSOLE_TOKEN");
        if let Some(token) = token {
            command.env("PICARD_CONSOLE_TOKEN", token);
        }
        let output = command.output().unwrap();
        assert!(!output.status.success());
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(stderr.contains(expected), "{stderr}");
    }
}

#[test]
fn console_route_shares_the_web_server_port() {
    let mut app = Application(
        Command::new(env!("CARGO_BIN_EXE_living-city"))
            .args(["--port", "0", "--vehicles", "0", "--console-route"])
            .env("PICARD_CONSOLE_TOKEN", TOKEN)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let mut lines = BufReader::new(app.0.stdout.take().unwrap()).lines();
    let console = lines.next().unwrap().unwrap();
    let web = lines.next().unwrap().unwrap();
    let url = console.strip_prefix("Living City console at ").unwrap();
    let address = url
        .strip_prefix("ws://")
        .unwrap()
        .strip_suffix("/console")
        .unwrap();
    assert_eq!(web, format!("Living City running at http://{address}"));

    let request = |token: &str, origin: bool| {
        let mut request = url.into_client_request().unwrap();
        request
            .headers_mut()
            .insert("Authorization", format!("Bearer {token}").parse().unwrap());
        if origin {
            request
                .headers_mut()
                .insert("Origin", "http://example.com".parse().unwrap());
        }
        request
    };
    assert!(connect(url).is_err());
    assert!(connect(request("wrong-token-wrong-token-wrong-token", false)).is_err());
    assert!(connect(request(TOKEN, true)).is_err());
    let (mut socket, _) = connect(request(TOKEN, false)).unwrap();
    if let MaybeTlsStream::Plain(stream) = socket.get_ref() {
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
    }
    socket
        .send(Message::Text(
            json!({"id": 1, "op": "inspect", "path": "/user/supervisor"})
                .to_string()
                .into(),
        ))
        .unwrap();
    let Message::Text(text) = socket.read().unwrap() else {
        panic!("Expected text")
    };
    let response: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(response["id"], 1);
    assert_eq!(response["result"]["path"], "/user/supervisor");
    socket.send(Message::Binary(vec![1, 2, 3].into())).unwrap();
    let Message::Text(text) = socket.read().unwrap() else {
        panic!("Expected text")
    };
    let response: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(response["error"]["code"], "invalid_request");
    drop(socket);
    app.stop();
}
