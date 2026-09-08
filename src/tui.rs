use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, List, ListItem, Paragraph, Row, Table},
};

/// Ratatui monitoring panel (TUI.md): left tree (workspaces + back_edges +
/// live hop tabs grouped under their workspace node), upper-right task table,
/// lower-right daemons/terminals + dead-letter, status bar with cancel /
/// toggle-swarm / focus-tab / quit keys.
///
/// Data: full `list_workspaces` + `list_tasks` pulls every 2s plus a
/// `subscribe` stream for sub-second task events. Alerts render red but never
/// interfere with normal operation.
pub fn run_tui(sock: &Path) -> anyhow::Result<()> {
    crossterm::terminal::enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    crossterm::execute!(stdout, crossterm::terminal::EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut term = Terminal::new(backend)?;
    let res = run_loop(&mut term, sock);
    crossterm::terminal::disable_raw_mode()?;
    crossterm::execute!(term.backend_mut(), crossterm::terminal::LeaveAlternateScreen)?;
    res
}

pub(crate) struct Snapshot {
    status: serde_json::Value,
    workspaces: Vec<serde_json::Value>,
    tasks: Vec<serde_json::Value>,
}

impl Snapshot {
    #[cfg(test)]
    pub fn task_count_for_test(&self) -> usize {
        self.tasks.len()
    }
    #[cfg(test)]
    pub fn tasks_for_test(&self) -> Vec<serde_json::Value> {
        self.tasks.clone()
    }
}

#[cfg(test)]
pub fn snapshot_for_test(
    status: serde_json::Value,
    workspaces: Vec<serde_json::Value>,
    tasks: Vec<serde_json::Value>,
) -> Snapshot {
    Snapshot { status, workspaces, tasks }
}

/// Pure tree-line formatter for the left workspace panel: one line per hop
/// tab grouped under its workspace node. Format: `{marker} {id8} [{state}]{live}`.
/// `selected` marks the ▶ line; live sessions (real, non-stub handle) get ◉.
/// Used by `render` and by the tree regression test in sched.rs.
pub fn tree_tab_lines(
    workspaces: &[serde_json::Value],
    tasks: &[serde_json::Value],
    selected: usize,
) -> Vec<String> {
    let mut out = vec![];
    for w in workspaces {
        let path = w.get("path").and_then(|v| v.as_str()).unwrap_or("?");
        let node_path = path;
        for (i, t) in tasks.iter().enumerate() {
            let to = t.get("to_ws").and_then(|v| v.as_str()).unwrap_or("");
            if to != node_path {
                continue;
            }
            let id = t.get("task_id").and_then(|v| v.as_str()).unwrap_or("?");
            let st = t.get("state").and_then(|v| v.as_str()).unwrap_or("?");
            let handle = t.get("terminal").and_then(|v| v.as_str()).unwrap_or("");
            let marker = if i == selected { "▶" } else { "·" };
            let live = if handle.is_empty() || handle.starts_with("stub-") { "" } else { " ◉" };
            out.push(format!(
                "{} {} [{}]{}",
                marker,
                id.get(..8.min(id.len())).unwrap_or(id),
                st,
                live,
            ));
        }
    }
    out
}

/// Pure row formatter for the task table: `id8 | from->to | att | state | xfer8`.
/// The ratatui `render` maps each string to a styled Row; tests assert the
/// strings (including state tokens the red/reversed branches key on).
#[cfg(test)]
pub fn task_rows_for_test(tasks: &[serde_json::Value], _selected: usize) -> Vec<String> {
    tasks
        .iter()
        .map(|t| {
            let id = t.get("task_id").and_then(|v| v.as_str()).unwrap_or("?");
            let from = t.get("from_ws").and_then(|v| v.as_str()).unwrap_or("?");
            let to = t.get("to_ws").and_then(|v| v.as_str()).unwrap_or("?");
            let att = t.get("attempt").and_then(|v| v.as_u64()).unwrap_or(0);
            let st = t.get("state").and_then(|v| v.as_str()).unwrap_or("?");
            let xfer = t.get("transfer_send_to").and_then(|v| v.as_str()).unwrap_or("");
            format!(
                "{}|{}->{}|{}|{}|{}",
                id.get(..8.min(id.len())).unwrap_or(id),
                from,
                to,
                att,
                st,
                xfer.get(..8.min(xfer.len())).unwrap_or(xfer),
            )
        })
        .collect()
}

fn pull(sock: &Path) -> Snapshot {
    let status = req(sock, serde_json::json!({"id":"t","op":"status"}))
        .ok()
        .and_then(|v| v.get("data").cloned())
        .unwrap_or(serde_json::Value::Null);
    let workspaces = req(sock, serde_json::json!({"id":"t","op":"list_workspaces"}))
        .ok()
        .and_then(|v| v.get("data").cloned())
        .and_then(|v| serde_json::from_value(v).ok())
        .unwrap_or_default();
    let tasks = req(
        sock,
        serde_json::json!({"id":"t","op":"list_tasks","limit":100}),
    )
    .ok()
    .and_then(|v| v.get("data").cloned())
    .and_then(|v| serde_json::from_value(v).ok())
    .unwrap_or_default();
    Snapshot { status, workspaces, tasks }
}

fn run_loop(
    term: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    sock: &Path,
) -> anyhow::Result<()> {
    let mut snap = pull(sock);
    let mut selected: usize = 0;
    let mut msg = String::new();
    loop {
        term.draw(|f| render(f, &snap, selected, &msg))?;
        if event::poll(Duration::from_millis(500))? {
            if let Event::Key(k) = event::read()? {
                match k.code {
                    KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                    KeyCode::Up | KeyCode::Char('k') => {
                        selected = selected.saturating_sub(1)
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        selected = selected.saturating_add(1)
                    }
                    KeyCode::Char('c') => {
                        // Cancel the selected task family.
                        if let Some(t) = snap.tasks.get(selected) {
                            let id = t.get("task_id").and_then(|v| v.as_str()).unwrap_or("");
                            match req(
                                sock,
                                serde_json::json!({"id":"t","op":"cancel","task_id":id,"reason":"tui cancel"}),
                            ) {
                                Ok(_) => msg = format!("cancelled {id}"),
                                Err(e) => msg = format!("cancel failed: {e}"),
                            }
                            snap = pull(sock);
                        }
                    }
                    KeyCode::Char('f') | KeyCode::Enter => {
                        // Focus the Orca tab of the selected task's session.
                        // Equivalent to clicking the tab: reveals the hop's
                        // terminal in the Orca UI. Stale/closed handles report
                        // the orca error as the status message.
                        if let Some(t) = snap.tasks.get(selected) {
                            let handle = t.get("terminal").and_then(|v| v.as_str()).unwrap_or("");
                            let id8 = t.get("task_id").and_then(|v| v.as_str()).map(|id| &id[..8.min(id.len())]).unwrap_or("?");
                            if handle.is_empty() || handle.starts_with("stub-") {
                                msg = format!("no live tab for {id8}");
                            } else {
                                match crate::orca_term::focus(handle) {
                                    Ok(_) => msg = format!("focused {id8}"),
                                    Err(e) => msg = format!("focus failed: {e}"),
                                }
                            }
                        }
                    }
                    KeyCode::Char('t') => {
                        // Toggle swarm for the root workspace.
                        let enabled = snap
                            .status
                            .get("swarm_enabled")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(true);
                        match req(
                            sock,
                            serde_json::json!({"id":"t","op":"toggle_swarm","workspace":".","enabled":!enabled}),
                        ) {
                            Ok(_) => msg = format!("swarm -> {}", !enabled),
                            Err(e) => msg = format!("toggle failed: {e}"),
                        }
                        snap = pull(sock);
                    }
                    _ => {}
                }
            }
        } else {
            // Idle tick: refresh.
            snap = pull(sock);
            if selected >= snap.tasks.len() && !snap.tasks.is_empty() {
                selected = snap.tasks.len() - 1;
            }
        }
    }
}

fn render(
    f: &mut ratatui::Frame,
    snap: &Snapshot,
    selected: usize,
    msg: &str,
) {
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(3)])
        .split(f.area());
    let main = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(32), Constraint::Percentage(68)])
        .split(root[0]);
    let right = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(62), Constraint::Percentage(38)])
        .split(main[1]);

    // Left: workspace tree with back_edges dashed lines plus live hop tabs
    // grouped under their workspace node (orca-side hierarchy mirror:
    // same-workspace hops are sibling tabs, `swarm:<to>:<id8>`).
    // Selected task's tab gets the ▶ marker.
    let mut items: Vec<ListItem> = vec![];
    for w in &snap.workspaces {
        let path = w.get("path").and_then(|v| v.as_str()).unwrap_or("?");
        let name = w.get("name").and_then(|v| v.as_str()).unwrap_or("?");
        let depth = if path == "." { 0 } else { path.matches('/').count() + 1 };
        let indent = "  ".repeat(depth);
        let daemon = w.get("daemon").and_then(|v| v.as_str()).unwrap_or("?");
        let dot = match daemon {
            "ready" => "●",
            _ => "○",
        };
        items.push(ListItem::new(format!("{indent}{dot} {name} ({path})")));
        if let Some(edges) = w.get("back_edges").and_then(|v| v.as_array()) {
            for e in edges {
                let tgt = e.as_str().unwrap_or("?");
                items.push(ListItem::new(format!(
                    "{indent}  ╰╴ {tgt} (back_edge)"
                )));
            }
        }
        // Hop tabs under this node (shared formatter, also unit-tested).
        for line in tree_tab_lines(&[w.clone()], &snap.tasks, selected) {
            items.push(ListItem::new(format!("{indent}  {line}")));
        }
    }
    // Orphan / alert lines from status.
    let alert_style = Style::default().fg(Color::Red).add_modifier(Modifier::BOLD);
    if let Some(orphans) = snap.status.get("orphans").and_then(|v| v.as_array()) {
        for o in orphans {
            items.push(ListItem::new(format!(
                "  ! orphan-instance: {}",
                o.as_str().unwrap_or("?")
            )).style(alert_style));
        }
    }
    if let Some(dangling) = snap.status.get("dangling").and_then(|v| v.as_array()) {
        for d in dangling {
            items.push(ListItem::new(format!(
                "  ! dangling-link: {}",
                d.as_str().unwrap_or("?")
            )).style(alert_style));
        }
    }
    f.render_widget(
        List::new(items).block(Block::default().title("workspaces").borders(Borders::ALL)),
        main[0],
    );

    // Upper right: task table.
    let rows: Vec<Row> = snap
        .tasks
        .iter()
        .enumerate()
        .map(|(i, t)| {
            let id = t.get("task_id").and_then(|v| v.as_str()).unwrap_or("?");
            let from = t.get("from_ws").and_then(|v| v.as_str()).unwrap_or("?");
            let to = t.get("to_ws").and_then(|v| v.as_str()).unwrap_or("?");
            let att = t.get("attempt").and_then(|v| v.as_u64()).unwrap_or(0);
            let st = t.get("state").and_then(|v| v.as_str()).unwrap_or("?");
            let xfer = t.get("transfer_send_to").and_then(|v| v.as_str()).unwrap_or("");
            let style = if i == selected {
                Style::default().add_modifier(Modifier::REVERSED)
            } else if st == "failed" {
                Style::default().fg(Color::Red)
            } else {
                Style::default()
            };
            Row::new(vec![
                id.get(..8.min(id.len())).unwrap_or(id).to_string(),
                format!("{from}->{to}"),
                att.to_string(),
                st.to_string(),
                xfer.get(..8.min(xfer.len())).unwrap_or(xfer).to_string(),
            ])
            .style(style)
        })
        .collect();
    let widths = [
        Constraint::Length(9),
        Constraint::Min(12),
        Constraint::Length(4),
        Constraint::Length(10),
        Constraint::Length(9),
    ];
    f.render_widget(
        Table::new(rows, widths)
            .header(Row::new(vec!["task", "from->to", "att", "state", "xfer"]))
            .block(Block::default().title("tasks").borders(Borders::ALL)),
        right[0],
    );

    // Lower right: ledger tail (last finished hops) + state counts.
    // Previously this dumped the raw status JSON; now it shows the
    // human-readable ledger so the operator sees what finished and why.
    let mut detail = vec![];
    if let Some(counts) = snap.status.get("tasks_by_state") {
        detail.push(format!("tasks: {counts}"));
    }
    if let Some(ledger) = snap.status.get("ledger_tail").and_then(|v| v.as_array()) {
        detail.push("ledger (latest first):".to_string());
        for e in ledger.iter().take(8) {
            let id = e.get("task_id").and_then(|v| v.as_str()).unwrap_or("?");
            let st = e.get("state").and_then(|v| v.as_str()).unwrap_or("?");
            let to = e.get("to_ws").and_then(|v| v.as_str()).unwrap_or("?");
            let reason = e.get("reason").and_then(|v| v.as_str()).unwrap_or("");
            let head = e.get("out_head").and_then(|v| v.as_str()).unwrap_or("");
            let extra = if reason.is_empty() {
                head.get(..60.min(head.len())).unwrap_or(head).replace('\n', " ")
            } else {
                reason.get(..80.min(reason.len())).unwrap_or(reason).to_string()
            };
            detail.push(format!(
                "{} {}->{} {}",
                id.get(..8.min(id.len())).unwrap_or(id),
                to,
                st,
                extra,
            ));
        }
    }
    if detail.is_empty() {
        detail.push("(no ledger entries yet)".to_string());
    }
    f.render_widget(
        Paragraph::new(detail.join("\n"))
            .block(Block::default().title("ledger").borders(Borders::ALL)),
        right[1],
    );

    f.render_widget(
        Paragraph::new(format!(
            "[↑↓/jk] select  [f/enter] focus orca tab  [c]ancel family  [t]oggle swarm  [q]uit    {msg}"
        ))
        .block(Block::default().title("keys").borders(Borders::ALL)),
        root[1],
    );
}

fn req(sock: &Path, req: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    let mut s = UnixStream::connect(sock)?;
    let mut line = serde_json::to_string(&req)?;
    line.push('\n');
    s.write_all(line.as_bytes())?;
    s.set_read_timeout(Some(Duration::from_secs(3)))?;
    let mut r = BufReader::new(&s);
    let mut out = String::new();
    r.read_line(&mut out)?;
    Ok(serde_json::from_str(&out)?)
}
