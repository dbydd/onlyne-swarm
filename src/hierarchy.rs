use std::process::Command;

/// Orca hierarchy registration (SPEC amendment 2, verified V1–V5).
///
/// Hand-verified facts (do NOT re-derive from `terminal create --help`):
/// - `project setup-existing-folder --kind folder` registers the dir but the
///   entity is NOT addressable via `worktree show --worktree path:` and gets a
///   FRESH repoId, so `worktree set --parent-worktree` fails with
///   LINEAGE_PARENT_CONTEXT_CONFLICT (parent must share repository).
/// - `repo add --path` also yields selector_not_found on `worktree show`.
/// - `worktree create` always makes a NEW checkout elsewhere — never usable
///   for pointing at an existing `.ws/<name>` dir.
/// Conclusion: Orca CLI offers no way to attach an existing directory as a
/// child worktree of the swarm root. Hop terminals stay under the root
/// worktree as `swarm:*` tabs (205288e behavior). This module only probes
/// reachability so sync can log why hierarchy was skipped.

fn orca() -> Command {
    let bin = std::env::var("ORCA_CLI_COMMAND").unwrap_or_else(|_| "orca".into());
    Command::new(bin)
}

fn run_json(mut cmd: Command) -> Option<serde_json::Value> {
    cmd.arg("--json");
    let out = cmd.output().ok()?;
    if !out.status.success() {
        return None;
    }
    serde_json::from_slice(&out.stdout).ok()
}

/// True when the Orca runtime answers. Hierarchy registration is skipped
/// entirely when Orca is unreachable (headless e2e, non-macOS, no Orca).
pub fn reachable() -> bool {
    let mut cmd = orca();
    cmd.args(["status"]);
    run_json(cmd).is_some()
}

/// Probe whether an existing dir is already an addressable Orca worktree.
/// Returns the worktree id when found.
pub fn show_worktree_id(dir: &std::path::Path) -> Option<String> {
    let mut cmd = orca();
    cmd.args(["worktree", "show", "--worktree"]);
    cmd.arg(format!("path:{}", dir.display()));
    let v = run_json(cmd)?;
    if v.get("ok").and_then(|o| o.as_bool()) != Some(true) {
        return None;
    }
    v.pointer("/result/worktree/id")
        .and_then(|id| id.as_str())
        .map(String::from)
}

/// Attempt hierarchy registration for one workspace dir. Currently always
/// reports the verified-negative outcome; kept as a function so a future
/// Orca release with folder-child support plugs in here.
pub enum HierarchyOutcome {
    /// Already (or now) an addressable worktree under the swarm root.
    Attached { worktree_id: String },
    /// No CLI path attaches an existing dir as a child worktree (V1–V5).
    Unsupported,
    /// Orca unreachable; caller logs and continues.
    Skipped,
}

pub fn ensure_child(_root: &std::path::Path, dir: &std::path::Path) -> HierarchyOutcome {
    if !reachable() {
        return HierarchyOutcome::Skipped;
    }
    if let Some(id) = show_worktree_id(dir) {
        return HierarchyOutcome::Attached { worktree_id: id };
    }
    HierarchyOutcome::Unsupported
}
