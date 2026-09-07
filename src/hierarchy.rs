use std::process::Command;

/// Orca hierarchy registration: one folder-kind node per workspace dir.
///
/// Hand-verified facts (live CLI probes, see commit history):
/// - `project setup-existing-folder --kind folder` registers the dir as an
///   addressable worktree (`worktree show --worktree path:<canon-dir>` resolves
///   it; Orca canonicalizes symlinked prefixes, so callers must pass the
///   canonical path — an uncanonicalized /tmp/... selector_not_founds while
///   /private/tmp/... resolves).
/// - `terminal create --worktree path:<node-dir>` lands the tab under that
///   node; same-workspace concurrent hops are sibling tabs (verified: 2 tabs
///   under one node). Selector must also be canonicalized.
/// - `worktree set --parent-worktree` across repos is refused
///   (LINEAGE_PARENT_CONTEXT_CONFLICT), so nodes stay same-level siblings —
///   no parent/child chain. That is fine: visibility comes from node + tab
///   placement, not lineage.
/// - `worktree create` always makes a NEW checkout elsewhere — never usable
///   for pointing at an existing `.ws/<name>` dir.

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

/// Outcome of ensuring one workspace node exists.

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

/// Outcome of ensuring one workspace node exists.
pub enum HierarchyOutcome {
    /// Node registered (or already present), addressable by path.
    Attached { worktree_id: String },
    /// Registration failed; hop tabs fall back to the root worktree.
    Unsupported,
    /// Orca unreachable; caller logs and continues.
    Skipped,
}

/// Ensure the folder-kind node for one workspace dir exists, with the given
/// display name. Best effort: any failure maps to Unsupported so sync never
/// breaks; the hop still runs, its tab just lands under the root worktree.
pub fn ensure_child(project: Option<&str>, dir: &std::path::Path, display_name: &str) -> HierarchyOutcome {
    if !reachable() {
        return HierarchyOutcome::Skipped;
    }
    let project = project.unwrap_or("github:dbydd/onlyne");
    match ensure_node(project, dir, display_name) {
        Ok(worktree_id) => HierarchyOutcome::Attached { worktree_id },
        Err(_) => HierarchyOutcome::Unsupported,
    }
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
