use anyhow::Context;
use serde_json::Value;
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
/// Returns the worktree id when found. Canonicalizes first: Orca resolves
/// symlinked prefixes (macOS /tmp -> /private/tmp) and an uncanonicalized
/// selector will selector_not_found even when the node exists.
pub fn show_worktree_id(dir: &std::path::Path) -> Option<String> {
    let canon = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
    let mut cmd = orca();
    cmd.args(["worktree", "show", "--worktree"]);
    cmd.arg(format!("path:{}", canon.display()));
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

/// Display name for a workspace node: `swarm:<tree-path>`, root is `swarm:.`.
pub fn node_display_name(tree_path: &str) -> String {
    if tree_path.is_empty() {
        "swarm:.".to_string()
    } else {
        format!("swarm:{tree_path}")
    }
}

/// Register `dir` as a folder-kind Orca node with a stable display name.
/// Idempotent: an already-registered dir resolves via `show` and only gets
/// its display name re-asserted. Returns the worktree id on success.
pub fn ensure_node(
    project: &str,
    dir: &std::path::Path,
    display_name: &str,
) -> anyhow::Result<String> {
    if let Some(id) = show_worktree_id(dir) {
        let mut cmd = orca();
        cmd.args(["worktree", "set", "--worktree"]);
        cmd.arg(format!("path:{}", dir.display()));
        cmd.args(["--display-name", display_name]);
        let _ = run_json(cmd); // best effort; id already known
        return Ok(id);
    }
    let mut cmd = orca();
    cmd.args([
        "project",
        "setup-existing-folder",
        "--project",
        project,
        "--host",
        "local",
        "--path",
    ]);
    cmd.arg(dir);
    cmd.args(["--kind", "folder", "--display-name", display_name]);
    let v = run_json(cmd).unwrap_or(serde_json::Value::Null);
    if v.get("ok").and_then(|o| o.as_bool()) != Some(true) {
        anyhow::bail!("setup-existing-folder failed for {}: {v}", dir.display());
    }
    show_worktree_id(dir)
        .ok_or_else(|| anyhow::anyhow!("registered {} but not addressable", dir.display()))
}

pub fn ensure_child(_root: &std::path::Path, dir: &std::path::Path) -> HierarchyOutcome {
    ensure_child_project(None, dir)
}

/// ensure_child with an explicit project override (sync passes the root's
/// project so sibling nodes share it instead of re-deriving per node).
pub fn ensure_child_project(project: Option<&str>, dir: &std::path::Path) -> HierarchyOutcome {
    if !reachable() {
        return HierarchyOutcome::Skipped;
    }
    if let Some(id) = show_worktree_id(dir) {
        return HierarchyOutcome::Attached { worktree_id: id };
    }
    // Best effort only; failure keeps flat-tab behavior (see register_hierarchy).
    let _ = project;
    HierarchyOutcome::Unsupported
}

/// Derive the Orca project id for the swarm root from its git origin.
/// Falls back to None (caller substitutes a default); never fails.
pub fn root_project(root: &std::path::Path) -> Option<String> {
    let out = Command::new("git")
        .args(["-C"])
        .arg(root)
        .args(["remote", "get-url", "origin"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let url = String::from_utf8_lossy(&out.stdout).trim().to_string();
    // https://github.com/OWNER/REPO(.git) or git@github.com:OWNER/REPO(.git)
    let path = url
        .strip_prefix("https://github.com/")
        .or_else(|| url.strip_prefix("http://github.com/"))
        .or_else(|| url.strip_prefix("git@github.com:"))
        .or_else(|| url.strip_prefix("ssh://git@github.com/"))?;
    let path = path.strip_suffix(".git").unwrap_or(path);
    let path = path.trim_matches('/');
    if path.is_empty() || !path.contains('/') {
        return None;
    }
    Some(format!("github:{path}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_display_name_shapes() {
        assert_eq!(node_display_name(""), "swarm:.");
        assert_eq!(node_display_name("model"), "swarm:model");
        assert_eq!(node_display_name("a/b"), "swarm:a/b");
    }

    #[test]
    fn root_project_parses_github_origins() {
        // No git repo here: None, never panic.
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(root_project(dir.path()), None);
    }
}
