//! Command-line syntax shared by the REPL and the TUI command bar.

use picard::console::Command;

pub(crate) const COMMANDS: &[&str] = &[
    "actors", "inspect", "messages", "pause", "resume", "clear", "stop", "restart", "send",
    "peers", "help", "quit",
];
pub(crate) const HELP: &str = "\
actors                  List actors and mailbox state
inspect PATH            Inspect one actor
messages PATH           Show the last 64 user-message events
pause PATH              Pause user-message processing
resume PATH             Resume user-message processing
clear PATH              Discard queued user messages (requires pause)
stop PATH               Request actor termination
restart PATH            Request actor restart
send [@PEER] PATH JSON  Send a typed JSON message (server requires serde)
peers                   List remote peers that send can target with @PEER
@PEER COMMAND           Run actors, inspect, messages, pause, ... on a remote peer
help                    Show this help
quit                    Disconnect";

pub(crate) fn complete(line: &str, pos: usize, paths: &[String]) -> (usize, Vec<String>) {
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

/// Whether the command carries a message payload, which is kept out of history.
pub(crate) fn is_send(command: &Command) -> bool {
    match command {
        Command::Send { .. } => true,
        Command::Forward { command, .. } => is_send(command),
        _ => false,
    }
}

pub(crate) enum Input {
    Command(Command),
    Help,
    Quit,
    Empty,
}

pub(crate) fn parse(line: &str) -> Result<Input, String> {
    let line = line.trim();
    if line.is_empty() {
        return Ok(Input::Empty);
    }
    if let Some(forwarded) = line.strip_prefix('@') {
        let (peer, command) = forwarded
            .split_once(char::is_whitespace)
            .ok_or("Usage: @PEER COMMAND")?;
        return match parse(command)? {
            Input::Command(command) => Ok(Input::Command(Command::Forward {
                peer: peer.to_owned(),
                command: Box::new(command),
            })),
            _ => Err("Only actor commands can run on a peer".into()),
        };
    }
    let (name, rest) = line
        .split_once(char::is_whitespace)
        .map_or((line, ""), |(name, rest)| (name, rest.trim()));
    if name == "send" {
        const USAGE: &str = "Usage: send [@PEER] PATH JSON";
        let (peer, rest) = match rest.strip_prefix('@') {
            Some(after) => {
                let (peer, rest) = after.split_once(char::is_whitespace).ok_or(USAGE)?;
                (Some(peer.to_owned()), rest.trim_start())
            }
            None => (None, rest),
        };
        let (path, json) = rest.split_once(char::is_whitespace).ok_or(USAGE)?;
        let path = validate_path(path)?;
        let message =
            serde_json::from_str(json.trim()).map_err(|err| format!("Invalid JSON: {err}"))?;
        return Ok(Input::Command(Command::Send {
            path,
            message,
            peer,
        }));
    }
    let command = match name {
        "actors" if rest.is_empty() => Command::Actors {
            offset: 0,
            limit: 100,
        },
        "peers" if rest.is_empty() => Command::Peers,
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

pub(crate) fn validate_path(path: &str) -> Result<String, String> {
    if !path.starts_with('/') || path.chars().any(char::is_whitespace) {
        return Err("Expected one absolute actor path, such as /user/worker".into());
    }
    Ok(path.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_json_with_spaces_without_changing_it() {
        let Input::Command(Command::Send {
            path,
            message,
            peer,
        }) = parse(r#"send /user/worker {"Greet": {"name": "Jean Luc"}}"#).unwrap()
        else {
            panic!("Expected send")
        };
        assert_eq!((path.as_str(), peer), ("/user/worker", None));
        assert_eq!(message["Greet"]["name"], "Jean Luc");
    }

    #[test]
    fn parses_a_remote_peer_target() {
        let Input::Command(Command::Send {
            path,
            message,
            peer,
        }) = parse(r#"send @district-1 /user/gateway {"a": 1}"#).unwrap()
        else {
            panic!("Expected send")
        };
        assert_eq!(peer.as_deref(), Some("district-1"));
        assert_eq!(path, "/user/gateway");
        assert_eq!(message["a"], 1);
        assert!(parse("send @district-1 /user/gateway").is_err());
        assert!(matches!(
            parse("peers").unwrap(),
            Input::Command(Command::Peers)
        ));
    }

    #[test]
    fn forwards_commands_to_a_peer() {
        let Input::Command(Command::Forward { peer, command }) =
            parse("@district-1 inspect /user/gateway").unwrap()
        else {
            panic!("Expected forward")
        };
        assert_eq!(peer, "district-1");
        assert!(matches!(*command, Command::Inspect { .. }));
        assert!(parse("@district-1").is_err());
        assert!(parse("@district-1 quit").is_err());
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
        assert_eq!(
            complete("send /user/worker {", 19, &paths).1,
            Vec::<String>::new()
        );
        let line = "inspect\u{2003}/user/wo";
        assert_eq!(
            complete(line, line.len(), &paths),
            (10, vec!["/user/worker".into()])
        );
    }
}
