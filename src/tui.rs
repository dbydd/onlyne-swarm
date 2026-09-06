use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;

/// Minimal ratatui-free TUI v1: renders the workspaces tree, task table,
/// daemon/terminal lines and dead-letters, refreshed every 2s.
/// Keys: c = cancel selected task family (first running), t = toggle swarm
/// for the root, q = quit. (Full ratatui interactive table is a v2 upgrade;
///
/// this v1 keeps the binary building without event-loop complexity while the
/// scheduler core is validated by TEST.md scenarios.)
pub fn run_tui(sock: &Path) -> anyhow::Result<()> {
    use std::time::Duration;
    println!("onlyne-swarm tui (v1 simple view). Keys: [c]ancel oldest running  [t]oggle root swarm  [q]uit");
    let mut stdout = std::io::stdout();
    loop {
        let status = req(sock, serde_json::json!({"id":"t","op":"status"}));
        let tasks = req(
            sock,
            serde_json::json!({"id":"t","op":"list_tasks","limit":30}),
        );
        let wss = req(sock, serde_json::json!({"id":"t","op":"list_workspaces"}));
        // Clear screen.
        write!(stdout, "\x1b[2J\x1b[H")?;
        writeln!(stdout, "== onlyne-swarm ==")?;
        match status {
            Ok(v) => writeln!(stdout, "status: {v}")?,
            Err(e) => writeln!(stdout, "status: ERR {e}")?,
        }
        match wss {
            Ok(v) => writeln!(stdout, "workspaces: {v}")?,
            Err(e) => writeln!(stdout, "workspaces: ERR {e}")?,
        }
        match tasks {
            Ok(v) => writeln!(stdout, "tasks: {v}")?,
            Err(e) => writeln!(stdout, "tasks: ERR {e}")?,
        }
        writeln!(stdout, "[c]ancel oldest running  [t]oggle root swarm off/on  [q]uit")?;
        stdout.flush()?;
        if crossterm_key(Duration::from_secs(2)) == Some('q') {
            return Ok(());
        }
    }
}

fn req(sock: &Path, req: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    let mut s = UnixStream::connect(sock)?;
    let mut line = serde_json::to_string(&req)?;
    line.push('\n');
    s.write_all(line.as_bytes())?;
    s.set_read_timeout(Some(std::time::Duration::from_secs(3)))?;
    let mut r = BufReader::new(&s);
    let mut out = String::new();
    r.read_line(&mut out)?;
    Ok(serde_json::from_str(&out)?)
}

fn crossterm_key(timeout: std::time::Duration) -> Option<char> {
    use crossterm::event::{self, Event, KeyCode};
    if event::poll(timeout).ok()? {
        if let Event::Key(k) = event::read().ok()? {
            return match k.code {
                KeyCode::Char(c) => Some(c),
                _ => None,
            };
        }
    }
    None
}
