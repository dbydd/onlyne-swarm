use anyhow::Context;
use serde_json::Value;
use std::process::Command;

/// Thin wrapper over `orca terminal ... --json`.
///
/// The whole swarm tree is a single Orca worktree; each session is one
/// orca terminal running `pi`. Terminal and pi process share a lifecycle:
/// the scheduler kills the pi process; orca reclaims the terminal.
pub struct Terminal {
    pub handle: String,
}

fn orca() -> Command {
    let bin = std::env::var("ORCA_CLI_COMMAND").unwrap_or_else(|_| "orca".into());
    Command::new(bin)
}

fn run_json(mut cmd: Command) -> anyhow::Result<Value> {
    cmd.arg("--json");
    let out = cmd.output().context("run orca")?;
    if !out.status.success() {
        anyhow::bail!("orca failed: {}", String::from_utf8_lossy(&out.stderr));
    }
    Ok(serde_json::from_slice(&out.stdout).context("parse orca --json output")?)
}

/// Create a terminal running `pi` with cwd set to the workspace dir.
/// `env_task` is exported as ONLYNE_SWARM_TASK so pi-onlyne swarm mode can
/// correlate; the actual task bytes are delivered later via loopback/in
/// after `swarm_ready` (avoids create-vs-write races).
///
/// `SWARM_PI_EXT` env (scheduler side) optionally prepends `-e <path>` args
/// so e2e trees can load a local pi-onlyne build instead of the global one.
/// Stub hook for headless e2e: when `SWARM_STUB_AGENT=1`, no orca terminal
/// is created. Instead a fake handle is returned and the test harness is
/// expected to drive `swarm_ready` + task reply itself (see TEST.md).
pub fn create(title: &str, cwd: &std::path::Path, env_task: &str) -> anyhow::Result<Terminal> {
    if std::env::var("SWARM_STUB_AGENT").as_deref() == Ok("1") {
        return Ok(Terminal {
            handle: format!("stub-{title}-{env_task}"),
        });
    }
    let mut cmd = orca();
    cmd.args(["terminal", "create", "--title", title, "--command"]);
    // Escape for sh -c style consumption: orca runs the string in a shell.
    // (Quoting rules live in session_command(), unit-tested below.)
    let ext = std::env::var("SWARM_PI_EXT").unwrap_or_default();
    cmd.arg(session_command(cwd, env_task, &ext));
    let v = run_json(cmd)?;
    let handle = parse_terminal_handle(&v).unwrap_or_default();
    if handle.is_empty() {
        anyhow::bail!("orca terminal create returned no handle: {v}");
    }
    Ok(Terminal { handle })
}

pub fn close(handle: &str) -> anyhow::Result<()> {
    let mut cmd = orca();
    cmd.args(["terminal", "close", "--terminal", handle]);
    run_json(cmd)?;
    Ok(())
}

/// Kill the pi process inside a terminal (SIGTERM the process group leader's
/// child named pi). Orca reclaims the terminal once the process exits.
pub fn kill_pi(handle: &str) -> anyhow::Result<()> {
    send(handle, "kill -TERM $(pgrep -P $$ pi 2>/dev/null || pgrep pi | head -1) 2>/dev/null; exit", true)
}

pub fn send(handle: &str, text: &str, enter: bool) -> anyhow::Result<()> {
    let mut cmd = orca();
    cmd.args(send_argv(handle, text, enter));
    run_json(cmd)?;
    Ok(())
}

fn shell_escape(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Extract the terminal handle from `orca terminal create --json` output.
/// Pure function over the JSON value so the shape contract is unit-tested
/// without spawning orca.
pub fn parse_terminal_handle(v: &Value) -> Option<String> {
    for ptr in ["/result/terminal/handle", "/terminal/handle"] {
        if let Some(h) = v.pointer(ptr).and_then(|h| h.as_str()) {
            if !h.is_empty() {
                return Some(h.to_string());
            }
        }
    }
    if let Some(h) = v.get("handle").and_then(|h| h.as_str()) {
        if !h.is_empty() {
            return Some(h.to_string());
        }
    }
    if let Some(h) = v
        .get("terminal")
        .and_then(|h| h.as_str().or_else(|| h.get("handle")?.as_str()))
    {
        if !h.is_empty() {
            return Some(h.to_string());
        }
    }
    None
}

/// Build the shell command run inside a fresh orca terminal for a task.
/// Pure constructor so quoting bugs are caught by unit tests, not in prod.
pub fn session_command(workspace_dir: &std::path::Path, task_id: &str, ext: &str) -> String {
    let (ne, ext_args) = if ext.trim().is_empty() {
        (String::new(), String::new())
    } else {
        (" -ne".to_string(), format!(" -e {}", shell_escape(ext)))
    };
    format!(
        "cd {} && ONLYNE_SWARM_TASK={} pi{}{}",
        shell_escape(&workspace_dir.to_string_lossy()),
        shell_escape(task_id),
        ne,
        ext_args
    )
}

/// Shell argv for `orca terminal send --terminal <h> --text <t> [--enter]`.
/// Pure constructor; the live send() below only executes it.
pub fn send_argv(handle: &str, text: &str, enter: bool) -> Vec<String> {
    let mut argv = vec![
        "terminal".to_string(),
        "send".to_string(),
        "--terminal".to_string(),
        handle.to_string(),
        "--text".to_string(),
        text.to_string(),
    ];
    if enter {
        argv.push("--enter".to_string());
    }
    argv
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn handle_shapes() {
        // Canonical shape seen live (result.terminal.handle).
        let v = json!({"ok": true, "result": {"terminal": {"handle": "term_abc"}}});
        assert_eq!(parse_terminal_handle(&v).as_deref(), Some("term_abc"));
        // Legacy/alternate shapes still resolve.
        let v = json!({"terminal": {"handle": "term_x"}});
        assert_eq!(parse_terminal_handle(&v).as_deref(), Some("term_x"));
        let v = json!({"handle": "term_h"});
        assert_eq!(parse_terminal_handle(&v).as_deref(), Some("term_h"));
        let v = json!({"terminal": "term_s"});
        assert_eq!(parse_terminal_handle(&v).as_deref(), Some("term_s"));
        // Garbage resolves to nothing (caller errors with the raw value).
        assert_eq!(parse_terminal_handle(&json!({"ok": false})), None);
        assert_eq!(parse_terminal_handle(&json!({"handle": ""})), None);
    }

    #[test]
    fn session_command_quotes_paths_and_task() {
        let cmd = session_command(
            std::path::Path::new("/tmp/a b/c"),
            "task-1",
            "",
        );
        assert!(cmd.starts_with("cd '/tmp/a b/c'"), "{cmd}");
        assert!(cmd.contains("ONLYNE_SWARM_TASK='task-1'"), "{cmd}");
        assert!(cmd.ends_with(" pi"), "{cmd}");
        // Local extension build: -ne avoids collision with global pi-onlyne.
        let cmd = session_command(
            std::path::Path::new("/w"),
            "t",
            "/ext/index.js",
        );
        assert!(cmd.contains(" pi -ne -e '/ext/index.js'"), "{cmd}");
        // Single quotes in paths are escaped, never break the shell string.
        let cmd = session_command(std::path::Path::new("/o'b"), "t", "");
        assert!(cmd.contains("'/o'\\''b'"), "{cmd}");
    }

    #[test]
    fn send_argv_shape() {
        assert_eq!(
            send_argv("h1", "hi", false),
            vec!["terminal", "send", "--terminal", "h1", "--text", "hi"]
        );
        assert_eq!(
            send_argv("h1", "hi", true).last().map(String::as_str),
            Some("--enter")
        );
    }
}
