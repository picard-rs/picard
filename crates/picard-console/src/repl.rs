use std::{
    borrow::Cow,
    env,
    error::Error,
    io::{self, IsTerminal},
    path::PathBuf,
    time::Duration,
};

use picard::console::{Command, Request, Response};
use rustyline::{
    Context, Editor, Helper,
    completion::{Completer, Pair},
    error::ReadlineError,
    highlight::Highlighter,
    hint::{Hinter, HistoryHinter},
    history::DefaultHistory,
    validate::Validator,
};
use serde_json::Value;
use tungstenite::{Message, WebSocket, client::IntoClientRequest, connect, stream::MaybeTlsStream};

const COMMANDS: &[&str] = &[
    "actors", "inspect", "messages", "pause", "resume", "clear", "stop", "restart", "send", "help",
    "quit",
];
const HELP: &str = "\
actors                  List actors and mailbox state
inspect PATH            Inspect one actor
messages PATH           Show the last 64 user-message events
pause PATH              Pause user-message processing
resume PATH             Resume user-message processing
clear PATH              Discard queued user messages (requires pause)
stop PATH               Request actor termination
restart PATH            Request actor restart
send PATH JSON          Send a typed JSON message (server requires serde)
help                    Show this help
quit                    Disconnect

Tab completes commands and actor paths. Up/Down recall history.
Ctrl-C cancels input; Ctrl-D disconnects. Sends are not saved to history.";

struct ConsoleHelper {
    paths: Vec<String>,
    hints: HistoryHinter,
    color: bool,
}

impl Helper for ConsoleHelper {}
impl Validator for ConsoleHelper {}

impl Hinter for ConsoleHelper {
    type Hint = String;

    fn hint(&self, line: &str, pos: usize, ctx: &Context<'_>) -> Option<String> {
        // Message payloads may be sensitive, so do not suggest them from history.
        if line.trim_start().starts_with("send ") {
            return None;
        }
        self.hints.hint(line, pos, ctx)
    }
}

impl Highlighter for ConsoleHelper {
    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(&'s self, prompt: &'p str, _: bool) -> Cow<'b, str> {
        if self.color {
            Cow::Owned(format!("\x1b[36;1m{prompt}\x1b[0m"))
        } else {
            Cow::Borrowed(prompt)
        }
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        if self.color {
            Cow::Owned(format!("\x1b[2m{hint}\x1b[0m"))
        } else {
            Cow::Borrowed(hint)
        }
    }
}

impl Completer for ConsoleHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let (start, candidates) = complete(line, pos, &self.paths);
        Ok((
            start,
            candidates
                .into_iter()
                .map(|candidate| Pair {
                    display: candidate.clone(),
                    replacement: candidate,
                })
                .collect(),
        ))
    }
}

fn complete(line: &str, pos: usize, paths: &[String]) -> (usize, Vec<String>) {
    let prefix = &line[..pos];
    let start = prefix
        .char_indices()
        .rev()
        .find(|(_, ch)| ch.is_whitespace())
        .map_or(0, |(position, ch)| position + ch.len_utf8());
    let word = &prefix[start..];
    let before: Vec<_> = prefix[..start].split_whitespace().collect();
    let candidates = if before.is_empty() {
        COMMANDS
            .iter()
            .filter(|command| command.starts_with(word))
            .map(|command| (*command).to_owned())
            .collect()
    } else if before.len() == 1 && COMMANDS[1..9].contains(&before[0]) {
        paths
            .iter()
            .filter(|path| path.starts_with(word))
            .cloned()
            .collect()
    } else {
        Vec::new()
    };
    (start, candidates)
}

enum Input {
    Command(Command),
    Help,
    Quit,
    Empty,
}

fn parse(line: &str) -> Result<Input, String> {
    let line = line.trim();
    if line.is_empty() {
        return Ok(Input::Empty);
    }
    let (name, rest) = line
        .split_once(char::is_whitespace)
        .map_or((line, ""), |(name, rest)| (name, rest.trim()));
    if name == "send" {
        let (path, json) = rest
            .split_once(char::is_whitespace)
            .ok_or("Usage: send PATH JSON")?;
        let path = validate_path(path)?;
        let message =
            serde_json::from_str(json.trim()).map_err(|err| format!("Invalid JSON: {err}"))?;
        return Ok(Input::Command(Command::Send { path, message }));
    }
    let command = match name {
        "actors" if rest.is_empty() => Command::Actors {
            offset: 0,
            limit: 100,
        },
        "help" if rest.is_empty() => return Ok(Input::Help),
        "quit" if rest.is_empty() => return Ok(Input::Quit),
        "inspect" => Command::Inspect {
            path: validate_path(rest)?,
        },
        "messages" => Command::Messages {
            path: validate_path(rest)?,
        },
        "pause" => Command::Pause {
            path: validate_path(rest)?,
        },
        "resume" => Command::Resume {
            path: validate_path(rest)?,
        },
        "clear" => Command::Clear {
            path: validate_path(rest)?,
        },
        "stop" => Command::Stop {
            path: validate_path(rest)?,
        },
        "restart" => Command::Restart {
            path: validate_path(rest)?,
        },
        _ => {
            return Err(format!(
                "Unknown command or unexpected arguments: {name}. Type help."
            ));
        }
    };
    Ok(Input::Command(command))
}

fn validate_path(path: &str) -> Result<String, String> {
    if !path.starts_with('/') || path.chars().any(char::is_whitespace) {
        return Err("Expected one absolute actor path, such as /user/worker".into());
    }
    Ok(path.into())
}

struct Client {
    socket: WebSocket<MaybeTlsStream<std::net::TcpStream>>,
    next_id: u64,
}

impl Client {
    fn connect(url: &str, token: &str) -> Result<Self, Box<dyn Error>> {
        let mut request = url.into_client_request()?;
        if request.uri().scheme_str() != Some("ws") {
            return Err("Use a ws:// URL through loopback or a secure tunnel".into());
        }
        let host = request.uri().host().ok_or("Missing console hostname")?;
        let host = host.trim_start_matches('[').trim_end_matches(']');
        if host != "localhost"
            && !host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
        {
            return Err("Console connections must use loopback; use a secure tunnel for remote applications".into());
        }
        request
            .headers_mut()
            .insert("Authorization", format!("Bearer {token}").parse()?);
        let (socket, _) = connect(request)?;
        if let MaybeTlsStream::Plain(stream) = socket.get_ref() {
            stream.set_read_timeout(Some(Duration::from_secs(5)))?;
            stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        }
        Ok(Self { socket, next_id: 0 })
    }

    fn execute(&mut self, command: Command) -> Result<Response, Box<dyn Error>> {
        self.next_id = self.next_id.checked_add(1).ok_or("Request ID exhausted")?;
        let request = Request {
            id: self.next_id,
            command,
        };
        self.socket
            .send(Message::Text(serde_json::to_string(&request)?.into()))?;
        loop {
            match self.socket.read()? {
                Message::Text(text) => {
                    let response: Response = serde_json::from_str(&text)?;
                    if response.id != Some(request.id)
                        || response.result.is_some() == response.error.is_some()
                    {
                        return Err("Invalid console response or mismatched request ID".into());
                    }
                    return Ok(response);
                }
                Message::Ping(_) | Message::Pong(_) => self.socket.flush()?,
                Message::Close(_) => return Err("Server closed the console connection".into()),
                _ => return Err("Expected a JSON text response".into()),
            }
        }
    }

    fn actors(&mut self) -> Result<Vec<Value>, Box<dyn Error>> {
        let mut actors = Vec::new();
        let mut offset = 0;
        loop {
            let response = self.execute(Command::Actors { offset, limit: 100 })?;
            if let Some(error) = response.error {
                return Err(format!("{}: {}", error.code, error.message).into());
            }
            let result = response.result.ok_or("Missing actor list")?;
            actors.extend(
                result["actors"]
                    .as_array()
                    .ok_or("Invalid actor list")?
                    .iter()
                    .cloned(),
            );
            let Some(next) = result["next_offset"].as_u64() else {
                break;
            };
            let next = usize::try_from(next)?;
            if next <= offset {
                return Err("Actor pagination did not advance".into());
            }
            offset = next;
        }
        Ok(actors)
    }
}

fn history_path() -> Option<PathBuf> {
    env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/state")))
        .map(|root| root.join("picard-console/history"))
}

fn print_error(error: &str, color: bool) {
    let mut safe = String::with_capacity(error.len());
    for ch in error.chars() {
        if ch.is_control() && ch != '\n' && ch != '\t' {
            safe.extend(ch.escape_default());
        } else {
            safe.push(ch);
        }
    }
    if color {
        eprintln!("\x1b[31;1m{safe}\x1b[0m");
    } else {
        eprintln!("{safe}");
    }
}

pub(super) fn run() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args().skip(1).collect();
    if args.len() == 1 && matches!(args[0].as_str(), "--help" | "-h") {
        println!(
            "Usage: picard-console ws://127.0.0.1:PORT/console\nSet PICARD_CONSOLE_TOKEN to the server's bearer token.\n\n{HELP}"
        );
        return Ok(());
    }
    if args.len() != 1 {
        return Err("Usage: picard-console ws://127.0.0.1:PORT/console (see --help)".into());
    }
    let token = env::var("PICARD_CONSOLE_TOKEN")
        .map_err(|_| "Set PICARD_CONSOLE_TOKEN to the server's bearer token")?;
    let mut client = Client::connect(&args[0], &token)?;
    let color = io::stdout().is_terminal() && env::var_os("NO_COLOR").is_none();
    let mut editor = Editor::<ConsoleHelper, DefaultHistory>::new()?;
    editor.set_helper(Some(ConsoleHelper {
        paths: Vec::new(),
        hints: HistoryHinter::new(),
        color,
    }));
    let history = history_path();
    if let Some(path) = &history {
        match editor.load_history(path) {
            Ok(()) => {}
            Err(ReadlineError::Io(err)) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => print_error(&format!("Cannot load history: {err}"), color),
        }
    }
    println!(
        "Connected to {}. Type help; Tab completes commands and actor paths.",
        args[0]
    );
    let result = interact(&mut editor, &mut client, color);
    if let Some(path) = history {
        let saved = (|| -> Result<(), Box<dyn Error>> {
            std::fs::create_dir_all(path.parent().ok_or("Invalid history path")?)?;
            editor.save_history(&path)?;
            Ok(())
        })();
        if let Err(err) = saved {
            print_error(&format!("Cannot save history: {err}"), color);
        }
    }
    result?;
    client.socket.close(None)?;
    Ok(())
}

fn interact(
    editor: &mut Editor<ConsoleHelper, DefaultHistory>,
    client: &mut Client,
    color: bool,
) -> Result<(), Box<dyn Error>> {
    loop {
        let actors = client.actors()?;
        editor
            .helper_mut()
            .ok_or("Missing completion helper")?
            .paths = actors
            .iter()
            .filter_map(|actor| actor["path"].as_str().map(str::to_owned))
            .collect();
        let line = match editor.readline("picard> ") {
            Ok(line) => line,
            Err(ReadlineError::Interrupted) => continue,
            Err(ReadlineError::Eof) => break,
            Err(err) => return Err(err.into()),
        };
        let input = match parse(&line) {
            Ok(input) => input,
            Err(err) => {
                print_error(&err, color);
                continue;
            }
        };
        if !matches!(input, Input::Command(Command::Send { .. }) | Input::Empty) {
            editor.add_history_entry(line.as_str())?;
        }
        match input {
            Input::Empty => {}
            Input::Quit => break,
            Input::Help => println!("{HELP}"),
            Input::Command(Command::Actors { .. }) => {
                println!("{}", serde_json::to_string_pretty(&actors)?);
            }
            Input::Command(command) => {
                let response = client.execute(command)?;
                if let Some(err) = response.error {
                    print_error(&format!("{}: {}", err.code, err.message), color);
                } else if let Some(value) = response.result {
                    let output = serde_json::to_string_pretty(&value)?;
                    if color {
                        println!("\x1b[32m{output}\x1b[0m");
                    } else {
                        println!("{output}");
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_json_with_spaces_without_changing_it() {
        let Input::Command(Command::Send { path, message }) =
            parse(r#"send /user/worker {"Greet": {"name": "Jean Luc"}}"#).unwrap()
        else {
            panic!("Expected send")
        };
        assert_eq!(path, "/user/worker");
        assert_eq!(message["Greet"]["name"], "Jean Luc");
    }

    #[test]
    fn rejects_bad_commands_and_json() {
        for line in [
            "send /user/worker nope",
            "send /user/worker",
            "pause",
            "clear relative",
            "stop /user/a extra",
            "quit extra",
            "unknown",
        ] {
            assert!(parse(line).is_err(), "{line}");
        }
    }

    #[test]
    fn completes_commands_and_paths_but_not_json() {
        let paths = vec!["/user/worker".into(), "/user/writer".into()];
        assert_eq!(
            complete("res", 3, &paths),
            (0, vec!["resume".into(), "restart".into()])
        );
        assert_eq!(
            complete("send /user/wo", 13, &paths),
            (5, vec!["/user/worker".into()])
        );
        assert!(complete("send /user/worker {", 19, &paths).1.is_empty());
        let line = "inspect\u{2003}/user/wo";
        assert_eq!(
            complete(line, line.len(), &paths),
            (10, vec!["/user/worker".into()])
        );
    }
}
