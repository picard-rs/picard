//! Line-oriented REPL, used with `--repl` or when the terminal is not interactive.

use std::{
    borrow::Cow,
    env,
    error::Error,
    io::{self, IsTerminal},
    path::PathBuf,
};

use picard::console::Command;
use rustyline::{
    Context, Editor, Helper,
    completion::{Completer, Pair},
    error::ReadlineError,
    highlight::Highlighter,
    hint::{Hinter, HistoryHinter},
    history::DefaultHistory,
    validate::Validator,
};

use crate::{
    client::Client,
    command::{HELP, Input, complete, parse},
};

const REPL_KEYS: &str = "\
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

pub(crate) fn history_path() -> Option<PathBuf> {
    env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/state")))
        .map(|root| root.join("picard-console/history"))
}

pub(crate) fn print_error(error: &str, color: bool) {
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

pub(crate) fn run(mut client: Client, url: &str) -> Result<(), Box<dyn Error>> {
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
    println!("Connected to {url}. Type help; Tab completes commands and actor paths.");
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
    client.close()
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
            Input::Help => println!("{HELP}\n\n{REPL_KEYS}"),
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
