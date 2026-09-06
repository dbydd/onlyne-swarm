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
pub fn create(title: &str, cwd: &std::path::Path, env_task: &str) -> anyhow::Result<Terminal> {
    let mut cmd = orca();
    cmd.args(["terminal", "create", "--title", title, "--command"]);
    // Escape for sh -c style consumption: orca runs the string in a shell.
    let sh = format!(
        "cd {} && ONLYNE_SWARM_TASK={} pi",
        shell_escape(&cwd.to_string_lossy()),
        shell_escape(env_task)
    );
    cmd.arg(sh);
    let v = run_json(cmd)?;
    let handle = v
        .pointer("/result/terminal/handle")
        .or_else(|| v.pointer("/terminal/handle"))
        .or_else(|| v.get("handle"))
        .or_else(|| v.get("terminal"))
        .and_then(|h| h.as_str().or_else(|| h.get("handle")?.as_str()))
        .unwrap_or("")
        .to_string();
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
    cmd.args(["terminal", "send", "--terminal", handle, "--text", text]);
    if enter {
        cmd.arg("--enter");
    }
    run_json(cmd)?;
    Ok(())
}

fn shell_escape(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}
