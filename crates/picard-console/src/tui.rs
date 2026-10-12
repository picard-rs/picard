//! Full-screen terminal UI: actor tree, actor details, message history and a command bar.

use std::{
    collections::HashSet,
    error::Error,
    time::{Duration, Instant},
};

use picard::console::Command;
use ratatui::{
    DefaultTerminal, Frame,
    crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    layout::{Constraint, Flex, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Cell, Clear, Paragraph, Row, Table, TableState, Wrap},
};
use serde_json::Value;

use crate::{
    client::Client,
    command::{HELP, Input, complete, parse},
};

const REFRESH: Duration = Duration::from_secs(1);
const KEYS: &str = "\
Up/Down, j/k   Select an actor (PgUp/PgDn, Home/End jump)
Right/l, Left/h  Expand / collapse the actor's children (Left on a leaf selects its parent)
/              Filter actors by path (Enter keeps, Esc clears)
:              Enter a command (Tab completes, Up/Down history)
p              Pause or resume the selected actor's mailbox
c              Clear the paused mailbox (asks for confirmation)
s              Send a JSON message to the selected actor
r              Restart the selected actor (asks for confirmation)
x              Stop the selected actor (asks for confirmation)
F5, Ctrl-L     Refresh now (the view also refreshes every second)
?              Toggle this help
q, Ctrl-C      Quit";
const HINTS: &str = " ↑↓ select  ←→ collapse/expand  / filter  : command  p pause/resume  c clear  s send  r restart  x stop  ? help  q quit";

/// Runs the TUI until the user quits. The terminal is restored on return and on panic.
pub(crate) fn run(mut client: Client, url: &str, color: bool) -> Result<(), Box<dyn Error>> {
    let mut app = App::new(url, color);
    app.refresh(&mut client)?;
    let mut terminal = ratatui::init();
    let result = event_loop(&mut terminal, &mut app, &mut client);
    ratatui::restore();
    result?;
    client.close()
}

fn event_loop(
    terminal: &mut DefaultTerminal,
    app: &mut App,
    client: &mut Client,
) -> Result<(), Box<dyn Error>> {
    while !app.quit {
        terminal.draw(|frame| app.render(frame))?;
        if event::poll(REFRESH.saturating_sub(app.last_refresh.elapsed()))?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            app.on_key(key, client)?;
        }
        if app.last_refresh.elapsed() >= REFRESH {
            app.refresh(client)?;
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct Theme {
    color: bool,
}

impl Theme {
    fn fg(self, color: Color) -> Style {
        if self.color {
            Style::new().fg(color)
        } else {
            Style::new()
        }
    }

    fn accent(self) -> Style {
        self.fg(Color::Cyan).add_modifier(Modifier::BOLD)
    }

    fn dim(self) -> Style {
        if self.color {
            Style::new().fg(Color::DarkGray)
        } else {
            Style::new()
        }
    }

    fn selected(self) -> Style {
        if self.color {
            Style::new().bg(Color::Blue).fg(Color::White)
        } else {
            Style::new().add_modifier(Modifier::REVERSED)
        }
    }
}

struct ActorRow {
    path: String,
    parent: String,
    message_type: String,
    manageable: bool,
    json_send: bool,
    mailbox: MailboxState,
}

struct MailboxState {
    user_messages: u64,
    system_messages: u64,
    paused: bool,
    suspended: bool,
    scheduled: bool,
}

impl ActorRow {
    fn from_value(value: &Value) -> Option<Self> {
        let mailbox = &value["mailbox"];
        Some(Self {
            path: value["path"].as_str()?.to_owned(),
            parent: value["parent"].as_str().unwrap_or_default().to_owned(),
            message_type: value["message_type"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            manageable: value["manageable"].as_bool().unwrap_or(false),
            json_send: value["json_send"].as_bool().unwrap_or(false),
            mailbox: MailboxState {
                user_messages: mailbox["user_messages"].as_u64().unwrap_or(0),
                system_messages: mailbox["system_messages"].as_u64().unwrap_or(0),
                paused: mailbox["paused"].as_bool().unwrap_or(false),
                suspended: mailbox["suspended"].as_bool().unwrap_or(false),
                scheduled: mailbox["scheduled"].as_bool().unwrap_or(false),
            },
        })
    }

    fn state(&self, theme: Theme) -> Span<'static> {
        if self.mailbox.paused {
            Span::styled(
                "paused",
                theme.fg(Color::Yellow).add_modifier(Modifier::BOLD),
            )
        } else if self.mailbox.suspended && self.path != "/" {
            Span::styled("suspended", theme.fg(Color::Red))
        } else if self.mailbox.scheduled {
            Span::styled("running", theme.fg(Color::Green))
        } else {
            Span::styled("idle", theme.dim())
        }
    }

    /// Indented last path segment, so the sorted list reads as a tree.
    fn tree_label(&self, marker: &str) -> String {
        let depth = self.path.matches('/').count().saturating_sub(1);
        let name = self.path.rsplit('/').next().filter(|name| !name.is_empty());
        format!("{}{marker}{}", "  ".repeat(depth), name.unwrap_or("/"))
    }
}

/// The parent of `path`, or `None` for the root.
fn parent_path(path: &str) -> Option<&str> {
    match path.rfind('/')? {
        _ if path == "/" => None,
        0 => Some("/"),
        index => Some(&path[..index]),
    }
}

/// Single-line text editor used by the command bar and the filter.
#[derive(Default)]
struct LineEdit {
    text: String,
    /// Byte offset, always on a character boundary.
    cursor: usize,
}

impl LineEdit {
    fn with_text(text: String) -> Self {
        Self {
            cursor: text.len(),
            text,
        }
    }

    fn clear(&mut self) {
        self.text.clear();
        self.cursor = 0;
    }

    fn previous(&self) -> Option<usize> {
        self.text[..self.cursor]
            .chars()
            .next_back()
            .map(|ch| self.cursor - ch.len_utf8())
    }

    fn next(&self) -> Option<usize> {
        self.text[self.cursor..]
            .chars()
            .next()
            .map(|ch| self.cursor + ch.len_utf8())
    }

    /// Applies an editing key and returns whether it was handled.
    fn edit(&mut self, key: KeyEvent) -> bool {
        match key.code {
            KeyCode::Char(ch) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.text.insert(self.cursor, ch);
                self.cursor += ch.len_utf8();
            }
            KeyCode::Backspace => {
                if let Some(previous) = self.previous() {
                    self.text.replace_range(previous..self.cursor, "");
                    self.cursor = previous;
                }
            }
            KeyCode::Delete => {
                if let Some(next) = self.next() {
                    self.text.replace_range(self.cursor..next, "");
                }
            }
            KeyCode::Left => self.cursor = self.previous().unwrap_or(self.cursor),
            KeyCode::Right => self.cursor = self.next().unwrap_or(self.cursor),
            KeyCode::Home => self.cursor = 0,
            KeyCode::End => self.cursor = self.text.len(),
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.text.replace_range(..self.cursor, "");
                self.cursor = 0;
            }
            _ => return false,
        }
        true
    }

    /// Completes the word before the cursor, returning the candidates when ambiguous.
    fn complete(&mut self, paths: &[String]) -> Vec<String> {
        let (start, candidates) = complete(&self.text, self.cursor, paths);
        let Some(first) = candidates.first() else {
            return candidates;
        };
        let mut common = first.as_str();
        for candidate in &candidates[1..] {
            let shared = common
                .char_indices()
                .zip(candidate.chars())
                .take_while(|((_, a), b)| a == b)
                .last()
                .map_or(0, |((index, ch), _)| index + ch.len_utf8());
            common = &common[..shared];
        }
        let mut replacement = common.to_owned();
        let is_command = !self.text[..start].contains(|ch: char| !ch.is_whitespace());
        if candidates.len() == 1 && (is_command || self.cursor == self.text.len()) {
            replacement.push(' ');
        }
        self.text.replace_range(start..self.cursor, &replacement);
        self.cursor = start + replacement.len();
        if candidates.len() == 1 {
            Vec::new()
        } else {
            candidates
        }
    }
}

enum Mode {
    Normal,
    Filter,
    Command(LineEdit),
    Confirm(Command),
    Help,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Severity {
    Info,
    Success,
    Error,
}

struct App {
    url: String,
    theme: Theme,
    actors: Vec<ActorRow>,
    selected: Option<String>,
    expanded: HashSet<String>,
    table: TableState,
    filter: LineEdit,
    messages: Vec<Value>,
    mode: Mode,
    status: Option<(Severity, String)>,
    history: Vec<String>,
    history_index: Option<usize>,
    last_refresh: Instant,
    quit: bool,
}

impl App {
    fn new(url: &str, color: bool) -> Self {
        Self {
            url: url.to_owned(),
            theme: Theme { color },
            actors: Vec::new(),
            selected: None,
            expanded: HashSet::from(["/".to_owned()]),
            table: TableState::default(),
            filter: LineEdit::default(),
            messages: Vec::new(),
            mode: Mode::Normal,
            status: None,
            history: Vec::new(),
            history_index: None,
            last_refresh: Instant::now(),
            quit: false,
        }
    }

    fn visible(&self) -> Vec<&ActorRow> {
        let filter = self.filter.text.to_lowercase();
        self.actors
            .iter()
            .filter(|actor| actor.path.to_lowercase().contains(&filter))
            .filter(|actor| !filter.is_empty() || self.ancestors_expanded(&actor.path))
            .collect()
    }

    fn ancestors_expanded(&self, path: &str) -> bool {
        let mut current = path;
        while let Some(parent) = parent_path(current) {
            if !self.expanded.contains(parent) {
                return false;
            }
            current = parent;
        }
        true
    }

    fn has_children(&self, path: &str) -> bool {
        self.actors
            .iter()
            .any(|actor| parent_path(&actor.path) == Some(path))
    }

    /// Expands the selected actor, or moves to its first child when already expanded.
    fn expand_selected(&mut self) {
        let Some(path) = self.selected.clone() else {
            return;
        };
        if !self.filter.text.is_empty() || !self.has_children(&path) {
            return;
        }
        if self.expanded.insert(path) {
            return;
        }
        let index = self.table.selected().unwrap_or(0) + 1;
        self.select_index(index);
    }

    /// Collapses the selected actor, or moves to its parent when it is already collapsed.
    fn collapse_selected(&mut self) {
        let Some(path) = self.selected.clone() else {
            return;
        };
        if !self.filter.text.is_empty() {
            return;
        }
        if self.expanded.remove(&path) {
            return;
        }
        let parent = parent_path(&path).map(str::to_owned);
        let index =
            parent.and_then(|parent| self.visible().iter().position(|actor| actor.path == parent));
        if let Some(index) = index {
            self.select_index(index);
        }
    }

    fn selected_row(&self) -> Option<&ActorRow> {
        let path = self.selected.as_deref()?;
        self.actors.iter().find(|actor| actor.path == path)
    }

    fn paths(&self) -> Vec<String> {
        self.actors.iter().map(|actor| actor.path.clone()).collect()
    }

    /// Keeps the selection on the same actor, or the same row if the actor disappeared.
    fn reselect(&mut self) {
        let visible = self.visible();
        let index = self
            .selected
            .as_deref()
            .and_then(|path| visible.iter().position(|actor| actor.path == path))
            .or_else(|| {
                (!visible.is_empty())
                    .then(|| self.table.selected().unwrap_or(0).min(visible.len() - 1))
            });
        let path = index.map(|index| visible[index].path.clone());
        self.table.select(index);
        if path != self.selected {
            self.selected = path;
            self.selection_changed();
        }
    }

    fn selection_changed(&mut self) {
        self.messages.clear();
        // Fetch the new actor's history on the next loop iteration.
        self.last_refresh = Instant::now()
            .checked_sub(REFRESH)
            .unwrap_or(self.last_refresh);
    }

    fn select_offset(&mut self, offset: isize) {
        let len = self.visible().len();
        if len == 0 {
            return;
        }
        let current = self.table.selected().unwrap_or(0);
        let index = current.saturating_add_signed(offset).min(len - 1);
        self.select_index(index);
    }

    fn select_index(&mut self, index: usize) {
        let path = self.visible().get(index).map(|actor| actor.path.clone());
        if path.is_some() {
            self.table.select(Some(index));
            if path != self.selected {
                self.selected = path;
                self.selection_changed();
            }
        }
    }

    fn select_path(&mut self, path: &str) {
        let mut current = path;
        while let Some(parent) = parent_path(current) {
            self.expanded.insert(parent.to_owned());
            current = parent;
        }
        if !self.visible().iter().any(|actor| actor.path == path) {
            self.filter.clear();
        }
        if self.selected.as_deref() != Some(path) {
            self.selected = Some(path.to_owned());
            self.selection_changed();
        }
        self.table.select(None);
        self.reselect();
    }

    fn set_status(&mut self, severity: Severity, text: impl Into<String>) {
        self.status = Some((severity, text.into()));
    }

    fn refresh(&mut self, client: &mut Client) -> Result<(), Box<dyn Error>> {
        self.actors = client
            .actors()?
            .iter()
            .filter_map(ActorRow::from_value)
            .collect();
        self.reselect();
        self.messages = match self.selected.clone() {
            Some(path) => {
                let response = client.execute(Command::Messages { path })?;
                response
                    .result
                    .and_then(|result| result["events"].as_array().cloned())
                    .unwrap_or_default()
            }
            None => Vec::new(),
        };
        self.last_refresh = Instant::now();
        Ok(())
    }

    fn execute(&mut self, client: &mut Client, command: Command) -> Result<(), Box<dyn Error>> {
        let request = serde_json::to_value(&command)?;
        let label = format!(
            "{} {}",
            request["op"].as_str().unwrap_or_default(),
            request["path"].as_str().unwrap_or_default()
        );
        let response = client.execute(command)?;
        match (response.result, response.error) {
            (_, Some(error)) => {
                self.set_status(
                    Severity::Error,
                    format!("{label}: {} ({})", error.message, error.code),
                );
            }
            (Some(result), None) => {
                self.set_status(Severity::Success, format!("{label}: {result}"));
            }
            (None, None) => self.set_status(Severity::Error, format!("{label}: empty response")),
        }
        self.refresh(client)
    }

    fn on_key(&mut self, key: KeyEvent, client: &mut Client) -> Result<(), Box<dyn Error>> {
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            self.quit = true;
            return Ok(());
        }
        match std::mem::replace(&mut self.mode, Mode::Normal) {
            Mode::Normal => self.on_normal_key(key, client)?,
            Mode::Help => {}
            Mode::Filter => match key.code {
                KeyCode::Enter => {}
                KeyCode::Esc => {
                    self.filter.clear();
                    self.reselect();
                }
                _ => {
                    self.filter.edit(key);
                    self.reselect();
                    self.mode = Mode::Filter;
                }
            },
            Mode::Confirm(command) => {
                if matches!(key.code, KeyCode::Char('y' | 'Y')) {
                    self.execute(client, command)?;
                } else {
                    self.set_status(Severity::Info, "Cancelled");
                }
            }
            Mode::Command(edit) => self.on_command_key(key, edit, client)?,
        }
        Ok(())
    }

    fn on_normal_key(&mut self, key: KeyEvent, client: &mut Client) -> Result<(), Box<dyn Error>> {
        let page = 10;
        match key.code {
            KeyCode::Char('q') => self.quit = true,
            KeyCode::Esc => {
                self.filter.clear();
                self.status = None;
                self.reselect();
            }
            KeyCode::Up | KeyCode::Char('k') => self.select_offset(-1),
            KeyCode::Down | KeyCode::Char('j') => self.select_offset(1),
            KeyCode::PageUp => self.select_offset(-page),
            KeyCode::PageDown => self.select_offset(page),
            KeyCode::Home | KeyCode::Char('g') => self.select_index(0),
            KeyCode::End | KeyCode::Char('G') => {
                self.select_index(self.visible().len().saturating_sub(1));
            }
            KeyCode::Char('/') => self.mode = Mode::Filter,
            KeyCode::Char(':') => self.mode = Mode::Command(LineEdit::default()),
            KeyCode::Char('?') => self.mode = Mode::Help,
            KeyCode::F(5) => self.refresh(client)?,
            KeyCode::Char('l') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.refresh(client)?;
            }
            KeyCode::Right | KeyCode::Char('l') => self.expand_selected(),
            KeyCode::Left | KeyCode::Char('h') => self.collapse_selected(),
            KeyCode::Char(action @ ('p' | 'c' | 's' | 'r' | 'x')) => {
                let Some(actor) = self.selected_row() else {
                    self.set_status(Severity::Error, "No actor selected");
                    return Ok(());
                };
                let path = actor.path.clone();
                match action {
                    'p' if actor.mailbox.paused => {
                        self.execute(client, Command::Resume { path })?;
                    }
                    'p' => self.execute(client, Command::Pause { path })?,
                    's' => {
                        self.mode = Mode::Command(LineEdit::with_text(format!("send {path} ")));
                    }
                    'c' => self.confirm(Command::Clear { path }),
                    'r' => self.confirm(Command::Restart { path }),
                    _ => self.confirm(Command::Stop { path }),
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn confirm(&mut self, command: Command) {
        self.mode = Mode::Confirm(command);
    }

    fn on_command_key(
        &mut self,
        key: KeyEvent,
        mut edit: LineEdit,
        client: &mut Client,
    ) -> Result<(), Box<dyn Error>> {
        match key.code {
            KeyCode::Esc => {
                self.history_index = None;
                return Ok(());
            }
            KeyCode::Enter => {
                self.history_index = None;
                return self.run_line(&edit.text, client);
            }
            KeyCode::Tab => {
                let candidates = edit.complete(&self.paths());
                if candidates.is_empty() {
                    self.status = None;
                } else {
                    self.set_status(Severity::Info, candidates.join("  "));
                }
            }
            KeyCode::Up if !self.history.is_empty() => {
                let index = self
                    .history_index
                    .map_or(self.history.len() - 1, |index| index.saturating_sub(1));
                self.history_index = Some(index);
                edit = LineEdit::with_text(self.history[index].clone());
            }
            KeyCode::Down => {
                if let Some(index) = self.history_index {
                    if index + 1 < self.history.len() {
                        self.history_index = Some(index + 1);
                        edit = LineEdit::with_text(self.history[index + 1].clone());
                    } else {
                        self.history_index = None;
                        edit = LineEdit::default();
                    }
                }
            }
            _ => {
                edit.edit(key);
            }
        }
        self.mode = Mode::Command(edit);
        Ok(())
    }

    fn run_line(&mut self, line: &str, client: &mut Client) -> Result<(), Box<dyn Error>> {
        let input = match parse(line) {
            Ok(input) => input,
            Err(err) => {
                self.set_status(Severity::Error, err);
                return Ok(());
            }
        };
        // Message payloads may be sensitive, so sends are not kept in history.
        if !matches!(input, Input::Empty | Input::Command(Command::Send { .. }))
            && self.history.last().is_none_or(|last| last != line.trim())
        {
            self.history.push(line.trim().to_owned());
        }
        match input {
            Input::Empty => {}
            Input::Quit => self.quit = true,
            Input::Help => self.mode = Mode::Help,
            Input::Command(Command::Actors { .. }) => {
                self.refresh(client)?;
                self.set_status(Severity::Info, format!("{} actors", self.actors.len()));
            }
            Input::Command(Command::Inspect { path } | Command::Messages { path }) => {
                if self.actors.iter().any(|actor| actor.path == path) {
                    self.select_path(&path);
                    self.refresh(client)?;
                } else {
                    self.set_status(Severity::Error, format!("No actor at {path}"));
                }
            }
            Input::Command(command) => self.execute(client, command)?,
        }
        Ok(())
    }

    fn render(&mut self, frame: &mut Frame) {
        let [title, body, status, input] = Layout::vertical([
            Constraint::Length(1),
            Constraint::Min(5),
            Constraint::Length(1),
            Constraint::Length(1),
        ])
        .areas(frame.area());
        let [left, right] =
            Layout::horizontal([Constraint::Percentage(45), Constraint::Percentage(55)])
                .areas(body);
        let [details, messages] =
            Layout::vertical([Constraint::Length(10), Constraint::Min(3)]).areas(right);
        self.render_title(frame, title);
        self.render_actors(frame, left);
        self.render_details(frame, details);
        self.render_messages(frame, messages);
        self.render_status(frame, status);
        self.render_input(frame, input);
        if matches!(self.mode, Mode::Help) {
            self.render_help(frame);
        }
    }

    fn render_title(&self, frame: &mut Frame, area: Rect) {
        let theme = self.theme;
        let mut spans = vec![
            Span::styled(
                " Picard console ",
                theme.accent().add_modifier(Modifier::REVERSED),
            ),
            Span::raw(" "),
            Span::styled(sanitize(&self.url), theme.fg(Color::Green)),
            Span::styled(format!("  {} actors", self.actors.len()), theme.dim()),
        ];
        if !self.filter.text.is_empty() {
            spans.push(Span::styled(
                format!("  filter: {}", sanitize(&self.filter.text)),
                theme.fg(Color::Yellow),
            ));
        }
        frame.render_widget(Line::from(spans), area);
    }

    fn render_actors(&mut self, frame: &mut Frame, area: Rect) {
        let theme = self.theme;
        let filtered = !self.filter.text.is_empty();
        let rows: Vec<Row> = self
            .visible()
            .into_iter()
            .map(|actor| {
                let label = if filtered {
                    actor.path.clone()
                } else {
                    let marker = if !self.has_children(&actor.path) {
                        "  "
                    } else if self.expanded.contains(&actor.path) {
                        "▾ "
                    } else {
                        "▸ "
                    };
                    actor.tree_label(marker)
                };
                let name_style = if actor.manageable {
                    Style::new()
                } else {
                    theme.dim()
                };
                let queued = if actor.mailbox.user_messages > 0 {
                    theme.fg(Color::Yellow)
                } else {
                    theme.dim()
                };
                Row::new([
                    Cell::from(Span::styled(sanitize(&label), name_style)),
                    Cell::from(Span::styled(
                        actor.mailbox.user_messages.to_string(),
                        queued,
                    )),
                    Cell::from(Span::styled(
                        actor.mailbox.system_messages.to_string(),
                        theme.dim(),
                    )),
                    Cell::from(actor.state(theme)),
                ])
            })
            .collect();
        let count = rows.len();
        let table = Table::new(
            rows,
            [
                Constraint::Min(10),
                Constraint::Length(6),
                Constraint::Length(4),
                Constraint::Length(9),
            ],
        )
        .header(Row::new(["Actor", "Queue", "Sys", "State"]).style(theme.accent()))
        .row_highlight_style(theme.selected())
        .highlight_symbol("▶ ")
        .block(
            Block::bordered()
                .title(format!(" Actors ({count}) "))
                .border_style(self.border(matches!(self.mode, Mode::Normal | Mode::Filter))),
        );
        frame.render_stateful_widget(table, area, &mut self.table);
    }

    fn border(&self, focused: bool) -> Style {
        if focused {
            self.theme.fg(Color::Cyan)
        } else {
            self.theme.dim()
        }
    }

    fn render_details(&self, frame: &mut Frame, area: Rect) {
        let theme = self.theme;
        let label = |text: &'static str| Span::styled(format!("{text:<12}"), theme.accent());
        let yes_no = |value: bool| {
            if value {
                Span::styled("yes", theme.fg(Color::Green))
            } else {
                Span::styled("no", theme.dim())
            }
        };
        let text = match self.selected_row() {
            Some(actor) => Text::from(vec![
                Line::from(vec![label("Path"), Span::raw(sanitize(&actor.path))]),
                Line::from(vec![label("Parent"), Span::raw(sanitize(&actor.parent))]),
                Line::from(vec![
                    label("Message"),
                    Span::raw(sanitize(&actor.message_type)),
                ]),
                Line::from(vec![label("Manageable"), yes_no(actor.manageable)]),
                Line::from(vec![label("JSON send"), yes_no(actor.json_send)]),
                Line::from(vec![
                    label("Mailbox"),
                    Span::raw(format!(
                        "{} user, {} system  ",
                        actor.mailbox.user_messages, actor.mailbox.system_messages
                    )),
                    actor.state(theme),
                ]),
                Line::from(vec![
                    label("Flags"),
                    Span::styled(
                        format!(
                            "paused={} suspended={} scheduled={}",
                            actor.mailbox.paused, actor.mailbox.suspended, actor.mailbox.scheduled
                        ),
                        theme.dim(),
                    ),
                ]),
            ]),
            None => Text::styled("No actor selected", theme.dim()),
        };
        frame.render_widget(
            Paragraph::new(text).block(
                Block::bordered()
                    .title(" Details ")
                    .border_style(theme.dim()),
            ),
            area,
        );
    }

    fn render_messages(&self, frame: &mut Frame, area: Rect) {
        let theme = self.theme;
        let lines: Vec<Line> = self
            .messages
            .iter()
            .rev()
            .map(|event| {
                let kind = event["event"].as_str().unwrap_or("?");
                let kind_style = match kind {
                    "received" => theme.fg(Color::Green),
                    "enqueued" => theme.fg(Color::Blue),
                    _ => theme.fg(Color::Red),
                };
                let mut spans = vec![Span::styled(format!("{kind:<15} "), kind_style)];
                if let Some(message) = event.get("message") {
                    spans.push(Span::raw(sanitize(&message.to_string())));
                } else if let Some(error) = event["payload_error"].as_str() {
                    spans.push(Span::styled(sanitize(error), theme.fg(Color::Red)));
                } else {
                    spans.push(Span::styled(
                        sanitize(event["message_type"].as_str().unwrap_or("?")),
                        theme.dim(),
                    ));
                }
                if let Some(sender) = event["sender"].as_str() {
                    spans.push(Span::styled(
                        format!("  ← {}", sanitize(sender)),
                        theme.dim(),
                    ));
                }
                Line::from(spans)
            })
            .collect();
        let title = format!(" Messages, newest first ({}) ", lines.len());
        let paragraph = if lines.is_empty() {
            Paragraph::new(Text::styled("No message events recorded", theme.dim()))
        } else {
            Paragraph::new(lines)
        };
        frame.render_widget(
            paragraph.block(Block::bordered().title(title).border_style(theme.dim())),
            area,
        );
    }

    fn render_status(&self, frame: &mut Frame, area: Rect) {
        let theme = self.theme;
        let line = if let Mode::Confirm(command) = &self.mode {
            let request = serde_json::to_value(command).unwrap_or_default();
            Line::styled(
                format!(
                    " {} {}? Press y to confirm, any other key cancels.",
                    request["op"].as_str().unwrap_or_default(),
                    request["path"].as_str().unwrap_or_default()
                ),
                theme.fg(Color::Yellow).add_modifier(Modifier::BOLD),
            )
        } else if let Some((severity, text)) = &self.status {
            let style = match severity {
                Severity::Info => Style::new(),
                Severity::Success => theme.fg(Color::Green),
                Severity::Error => theme.fg(Color::Red).add_modifier(Modifier::BOLD),
            };
            Line::styled(format!(" {}", sanitize(text)), style)
        } else {
            Line::default()
        };
        frame.render_widget(line, area);
    }

    fn render_input(&self, frame: &mut Frame, area: Rect) {
        let theme = self.theme;
        let (prompt, edit) = match &self.mode {
            Mode::Command(edit) => (":", edit),
            Mode::Filter => ("/", &self.filter),
            _ => {
                frame.render_widget(Line::styled(HINTS, theme.dim()), area);
                return;
            }
        };
        let prefix = Line::from(vec![
            Span::styled(prompt, theme.accent()),
            Span::raw(sanitize(&edit.text[..edit.cursor])),
        ]);
        let cursor = u16::try_from(prefix.width()).unwrap_or(u16::MAX);
        frame.render_widget(
            Line::from(vec![
                Span::styled(prompt, theme.accent()),
                Span::raw(sanitize(&edit.text)),
            ]),
            area,
        );
        frame.set_cursor_position((area.x.saturating_add(cursor), area.y));
    }

    fn render_help(&self, frame: &mut Frame) {
        let text = format!("Keys\n{KEYS}\n\nCommands (press :)\n{HELP}");
        let height = u16::try_from(text.lines().count() + 2).unwrap_or(u16::MAX);
        let [area] = Layout::vertical([Constraint::Length(height)])
            .flex(Flex::Center)
            .areas(frame.area());
        let [area] = Layout::horizontal([Constraint::Length(84)])
            .flex(Flex::Center)
            .areas(area);
        frame.render_widget(Clear, area);
        frame.render_widget(
            Paragraph::new(text).wrap(Wrap { trim: false }).block(
                Block::bordered()
                    .title(" Help (any key closes) ")
                    .border_style(self.theme.accent()),
            ),
            area,
        );
    }
}

/// Escapes control characters so server-provided text cannot drive the terminal.
fn sanitize(text: &str) -> String {
    let mut safe = String::with_capacity(text.len());
    for ch in text.chars() {
        if ch.is_control() {
            safe.extend(ch.escape_default());
        } else {
            safe.push(ch);
        }
    }
    safe
}

#[cfg(test)]
mod tests {
    use std::{
        sync::mpsc::{self, Sender as ChannelSender},
        time::Duration,
    };

    use picard::{actors::*, console::ConsoleOptions};
    use ratatui::{Terminal, backend::TestBackend};

    use super::*;

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

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn screen(app: &mut App) -> String {
        let mut terminal = Terminal::new(TestBackend::new(140, 40)).unwrap();
        terminal.draw(|frame| app.render(frame)).unwrap();
        let buffer = terminal.backend().buffer();
        buffer
            .content()
            .chunks(usize::from(buffer.area.width))
            .map(|row| {
                row.iter()
                    .map(ratatui::buffer::Cell::symbol)
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn line_edit_edits_and_completes() {
        let mut edit = LineEdit::default();
        for ch in "paä".chars() {
            assert!(edit.edit(key(KeyCode::Char(ch))));
        }
        edit.edit(key(KeyCode::Backspace));
        assert_eq!(edit.text, "pa");
        assert!(edit.complete(&[]).is_empty());
        assert_eq!(edit.text, "pause ");
        let mut command = LineEdit::with_text("re".to_owned());
        assert_eq!(command.complete(&[]).len(), 2);
        assert_eq!(command.text, "res");
        command.edit(key(KeyCode::Char('u')));
        command.complete(&[]);
        assert_eq!(command.text, "resume ");

        let paths = ["/user/worker-a".to_owned(), "/user/worker-b".to_owned()];
        edit.edit(key(KeyCode::Char('/')));
        assert_eq!(edit.complete(&paths).len(), 2);
        assert_eq!(edit.text, "pause /user/worker-");
        edit.edit(key(KeyCode::Char('b')));
        assert!(edit.complete(&paths).is_empty());
        assert_eq!(edit.text, "pause /user/worker-b ");

        edit.edit(key(KeyCode::Left));
        edit.edit(KeyEvent::new(KeyCode::Char('u'), KeyModifiers::CONTROL));
        assert_eq!((edit.text.as_str(), edit.cursor), (" ", 0));
    }

    #[test]
    fn sanitize_escapes_control_characters() {
        assert_eq!(sanitize("a\u{1b}[2Jb\n"), "a\\u{1b}[2Jb\\n");
    }

    #[test]
    fn manages_a_live_application() {
        let sys = ActorSystem::new().unwrap();
        let (tx, rx) = mpsc::channel();
        let worker = sys.actor_of_args::<Worker, _>("worker", tx).unwrap();
        worker.tell(1_u32, None);
        assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), 1);
        let server = sys
            .start_console(ConsoleOptions {
                bind: "127.0.0.1:0".parse().unwrap(),
                token: TOKEN.into(),
            })
            .unwrap();
        let url = format!("ws://{}/console", server.local_addr());
        let mut client = Client::connect(&url, TOKEN).unwrap();
        let mut app = App::new(&url, false);
        app.refresh(&mut client).unwrap();
        let text = screen(&mut app);
        assert!(!text.contains("worker") && text.contains(&url), "{text}");
        app.select_path("/user");
        app.on_key(key(KeyCode::Right), &mut client).unwrap();
        assert!(screen(&mut app).contains("worker"));
        app.on_key(key(KeyCode::Left), &mut client).unwrap();
        assert!(!screen(&mut app).contains("worker"));
        app.on_key(key(KeyCode::Right), &mut client).unwrap();

        for code in "/worker".chars().map(KeyCode::Char).chain([KeyCode::Enter]) {
            app.on_key(key(code), &mut client).unwrap();
        }
        assert_eq!(app.selected.as_deref(), Some("/user/worker"));
        assert_eq!(app.visible().len(), 1);

        app.on_key(key(KeyCode::Char('p')), &mut client).unwrap();
        assert!(app.selected_row().unwrap().mailbox.paused);
        worker.tell(2_u32, None);
        assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());
        app.refresh(&mut client).unwrap();
        assert_eq!(app.selected_row().unwrap().mailbox.user_messages, 1);
        let text = screen(&mut app);
        assert!(text.contains("enqueued"), "{text}");
        app.on_key(key(KeyCode::Char('p')), &mut client).unwrap();
        assert!(!app.selected_row().unwrap().mailbox.paused);
        assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), 2);

        app.on_key(key(KeyCode::Char('s')), &mut client).unwrap();
        for code in "3".chars().map(KeyCode::Char).chain([KeyCode::Enter]) {
            app.on_key(key(code), &mut client).unwrap();
        }
        if app.selected_row().unwrap().json_send {
            assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), 3);
        } else {
            assert_eq!(app.status.as_ref().unwrap().0, Severity::Error);
        }

        app.on_key(key(KeyCode::Char('x')), &mut client).unwrap();
        app.on_key(key(KeyCode::Char('n')), &mut client).unwrap();
        assert_eq!(app.status.as_ref().unwrap().1, "Cancelled");
        app.on_key(key(KeyCode::Char('x')), &mut client).unwrap();
        assert!(screen(&mut app).contains("stop /user/worker?"));
        app.on_key(key(KeyCode::Char('y')), &mut client).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while app.actors.iter().any(|actor| actor.path == "/user/worker") {
            assert!(Instant::now() < deadline, "the worker did not stop");
            std::thread::sleep(Duration::from_millis(10));
            app.refresh(&mut client).unwrap();
        }

        app.on_key(key(KeyCode::Char('q')), &mut client).unwrap();
        assert!(app.quit);
        client.close().unwrap();
        server.stop().unwrap();
        futures::executor::block_on(sys.shutdown()).unwrap();
    }
}
