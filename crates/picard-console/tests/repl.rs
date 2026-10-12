#![cfg(not(target_arch = "wasm32"))]

use std::{
    io::Write,
    process::{Command, Stdio},
    sync::mpsc::{self, Sender as ChannelSender},
    time::Duration,
};

use picard::{actors::*, console::ConsoleOptions};

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

struct Worker(ChannelSender<u32>);

impl ActorFactoryArgs<ChannelSender<u32>> for Worker {
    fn create_args(tx: ChannelSender<u32>) -> Self {
        Self(tx)
    }
}

impl Actor for Worker {
    type Msg = u32;

    fn recv(&mut self, _: &Context<u32>, msg: u32, _: Sender) {
        self.0.send(msg).unwrap();
    }
}

#[test]
fn repl_connects_and_manages_a_live_application() {
    let sys = ActorSystem::new().unwrap();
    let (tx, rx) = mpsc::channel();
    let actor = sys.actor_of_args::<Worker, _>("worker", tx).unwrap();
    actor.tell(1_u32, None);
    assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), 1);
    let server = sys
        .start_console(ConsoleOptions {
            bind: "127.0.0.1:0".parse().unwrap(),
            token: TOKEN.into(),
        })
        .unwrap();
    let state = std::env::temp_dir().join(format!("picard-console-test-{}", sys.id()));
    let mut child = Command::new(env!("CARGO_BIN_EXE_picard-console"))
        .arg(format!("ws://{}/console", server.local_addr()))
        .env("PICARD_CONSOLE_TOKEN", TOKEN)
        .env("XDG_STATE_HOME", &state)
        .env("NO_COLOR", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let commands = "actors\ninspect /user/worker\npause /user/worker\nclear /user/worker\nresume /user/worker\nsend /user/worker 42\nunknown\nhelp\nquit\n";
    child
        .stdin
        .take()
        .unwrap()
        .write_all(commands.as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    let history = state.join("picard-console/history");
    let contents = std::fs::read_to_string(&history).unwrap();
    std::fs::remove_file(history).unwrap();
    std::fs::remove_dir(state.join("picard-console")).unwrap();
    std::fs::remove_dir(state).unwrap();
    server.stop().unwrap();
    futures::executor::block_on(sys.shutdown()).unwrap();
    assert!(output.status.success(), "{stderr}");
    assert!(stdout.contains("/user/worker"), "{stdout}");
    assert!(stdout.contains("\"paused\": true"), "{stdout}");
    assert!(stdout.contains("\"cleared\": 0"), "{stdout}");
    assert!(
        stdout.contains("Tab completes commands and actor paths"),
        "{stdout}"
    );
    assert!(stderr.contains("Unknown command"), "{stderr}");
    if stdout.contains("\"json_send\": true") {
        assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), 42);
    } else {
        assert!(stderr.contains("json_unavailable"), "{stderr}");
    }
    assert!(!stdout.contains("\x1b["));
    assert!(!stdout.contains(TOKEN));
    assert!(!stderr.contains(TOKEN));
    assert!(contents.contains("pause /user/worker"));
    assert!(!contents.contains("send /user/worker"));
}

#[test]
fn repl_help_and_missing_credentials_are_explicit() {
    let help = Command::new(env!("CARGO_BIN_EXE_picard-console"))
        .arg("--help")
        .output()
        .unwrap();
    assert!(help.status.success());
    assert!(
        String::from_utf8(help.stdout)
            .unwrap()
            .contains("PICARD_CONSOLE_TOKEN")
    );
    let result = Command::new(env!("CARGO_BIN_EXE_picard-console"))
        .arg("ws://127.0.0.1:1/console")
        .env_remove("PICARD_CONSOLE_TOKEN")
        .output()
        .unwrap();
    assert!(!result.status.success());
    assert!(
        String::from_utf8(result.stderr)
            .unwrap()
            .contains("Set PICARD_CONSOLE_TOKEN")
    );
}
