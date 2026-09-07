use anyhow::Context;
use serde::Serialize;
use std::collections::BTreeSet;
use std::os::unix::fs::symlink;
use std::path::Path;

use crate::template::Effective;

pub const INSTANCE_CONFIG: &str = r#"#:schema ./onlyne-config.schema.json
[workspace]
name = "swarm-node"

[logging]
level = "info"

[io]
in_format = "markdown"
out_content = "latest_only"
out_cursor = "consume"
history_context_messages = 20

[loopback.io]
in_format = "markdown"
out_content = "latest_only"
out_cursor = "consume"
history_context_messages = 20

[adapters.telegram]
enabled = false

[adapters.feishu]
enabled = false

[adapters.qqbot]
enabled = false

[adapters.wechat]
enabled = false

[swarm]
enabled = true
"#;

#[derive(Debug, Default, Serialize)]
pub struct SyncReport {
    pub created: Vec<String>,
    pub orphans: Vec<String>,
    pub dangling: Vec<String>,
    pub workspaces: usize,
    /// Orca hierarchy notes, one per non-root workspace.
    #[serde(default)]
    pub hierarchy: Vec<String>,
}

/// Generate `.ws` from `.agents/.schedule`.
///
/// - Missing instance dirs are created with loopback-only config + effective snapshot.
/// - Existing instances: config and `swarm.workspace.jsonc` are never overwritten;
///   only `onlyne_in/` symlinks are refreshed.
/// - Instances with no description are kept and reported as orphans (never deleted).
pub fn run_sync(root: &Path) -> anyhow::Result<SyncReport> {
    let tree = crate::template::load_tree(root)?;
    let mut report = SyncReport {
        workspaces: tree.len(),
        ..Default::default()
    };
    let known: BTreeSet<&str> = tree.iter().map(|e| e.path.as_str()).collect();

    for e in &tree {
        let ws = crate::root::resolve_instance(root, &e.path);
        let onlyne = ws.join(".onlyne");
        let is_new = !onlyne.join("config.toml").exists();
        std::fs::create_dir_all(onlyne.join("channels/loopback"))
            .with_context(|| format!("create {}", onlyne.display()))?;
        std::fs::create_dir_all(onlyne.join("run"))?;
        std::fs::create_dir_all(onlyne.join("logs"))?;
        // NOTE: loopback in/out are real FIFOs owned by the workspace daemon
        // (created via mkfifo on daemon start). The scheduler must NOT create
        // placeholder regular files here: a regular file at `in` would block
        // the daemon from creating its FIFO and would break symlink delivery.
        // Dangling onlyne_in links are reported by refresh_links instead.
        if is_new {
            std::fs::write(onlyne.join("config.toml"), INSTANCE_CONFIG)?;
            // Snapshot stores back_edges tree-absolute (already normalized);
            // normalize_edge() is idempotent on them at load time.
            let snap = serde_json::to_string_pretty(&serde_json::json!({
                "name": e.name,
                "role": e.role,
                "model": {"provider": e.model.provider, "model": e.model.model, "effort": e.model.effort},
                "back_edges": e.back_edges,
            }))?;
            std::fs::write(onlyne.join("swarm.workspace.jsonc"), snap + "\n")?;
            report.created.push(display_path(&e.path));
        }
    }

    // Orphans: existing instance dirs with no description.
    let inst = crate::root::instances_dir(root);
    if inst.is_dir() {
        let mut found: Vec<String> = vec![];
        collect_rel(&inst, &inst, &mut found)?;
        for f in found {
            if !known.contains(f.as_str()) {
                report.orphans.push(f);
            }
        }
    }

    refresh_links(root, &tree, &mut report)?;
    register_hierarchy(root, &tree, &mut report);
    Ok(report)
}

/// Best-effort Orca node registration per workspace. Never fails sync;
/// records the outcome. Each `.ws/<name>` becomes a folder-kind node
/// (`swarm:<tree-path>` display name) so hop terminals land as tabs under
/// their own workspace node instead of piling up under the root worktree.
fn register_hierarchy(root: &Path, tree: &[Effective], report: &mut SyncReport) {
    use crate::hierarchy::HierarchyOutcome;
    let project = crate::hierarchy::root_project(root);
    for e in tree {
        if e.path.is_empty() {
            continue; // root is already an Orca worktree; nothing to attach
        }
        let ws = crate::root::resolve_instance(root, &e.path);
        let name = crate::hierarchy::node_display_name(&e.path);
        match crate::hierarchy::ensure_child(project.as_deref(), &ws, &name) {
            HierarchyOutcome::Attached { worktree_id } => report.hierarchy.push(format!(
                "{}: node {}",
                display_path(&e.path),
                worktree_id
            )),
            HierarchyOutcome::Unsupported => report.hierarchy.push(format!(
                "{}: node skipped (Orca registration failed; tabs fall back to root worktree)",
                display_path(&e.path)
            )),
            HierarchyOutcome::Skipped => {}
        }
    }
}

fn display_path(p: &str) -> String {
    if p.is_empty() {
        ".".into()
    } else {
        p.into()
    }
}

fn collect_rel(base: &Path, dir: &Path, out: &mut Vec<String>) -> anyhow::Result<()> {
    for e in std::fs::read_dir(dir)? {
        let e = e?;
        if e.file_type()?.is_dir() {
            let rel = e
                .path()
                .strip_prefix(base)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            // Only count dirs that look like instances (have .onlyne).
            if e.path().join(".onlyne").is_dir() {
                out.push(rel.clone());
            }
            collect_rel(base, &e.path(), out)?;
        }
    }
    Ok(())
}

/// Refresh `onlyne_in/<target>/` symlinks in every real workspace (incl. root).
/// Each link points at the target's `.onlyne/channels/loopback/in` absolute path.
fn refresh_links(root: &Path, tree: &[Effective], report: &mut SyncReport) -> anyhow::Result<()> {
    let targets: Vec<&str> = tree.iter().map(|e| e.path.as_str()).collect();
    for e in tree {
        let ws = crate::root::resolve_instance(root, &e.path);
        let view = ws.join("onlyne_in");
        std::fs::create_dir_all(&view)?;
        for t in &targets {
            if *t == e.path {
                continue;
            }
            let link_rel = if t.is_empty() { "_root".into() } else { (*t).to_string() };
            let link_path = view.join(&link_rel);
            let target_in = crate::root::loopback_in(&crate::root::resolve_instance(root, t));
            if let Some(parent) = link_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let _ = std::fs::remove_file(&link_path);
            symlink(&target_in, &link_path).with_context(|| {
                format!(
                    "symlink {} -> {}",
                    link_path.display(),
                    target_in.display()
                )
            })?;
            // symlink_metadata: a FIFO target exists even with no reader; a
            // missing daemon (no `in` yet) reports dangling without following.
            if std::fs::symlink_metadata(&target_in).is_err() {
                report.dangling.push(format!(
                    "{}: onlyne_in/{}",
                    display_path(&e.path),
                    link_rel
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sync_creates_instances_and_links() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join(".agents/.schedule/a")).unwrap();
        std::fs::write(
            root.join(".agents/.schedule/a/template.workspace.jsonc"),
            r#"{"name": "a", "role": "r"}"#,
        )
        .unwrap();
        let rep = run_sync(root).unwrap();
        assert_eq!(rep.workspaces, 2);
        assert!(rep.created.iter().any(|c| c == "a"));
        let cfg = std::fs::read_to_string(
            root.join(".ws/a/.onlyne/config.toml"),
        )
        .unwrap();
        assert!(cfg.contains("[swarm]"));
        // Root view links to a's loopback in.
        let link = root.join("onlyne_in/a");
        assert!(link.is_symlink());
        // A's view links back to root ("_root").
        assert!(root
            .join(".ws/a/onlyne_in/_root")
            .is_symlink());
        // Hand-written overlay preserved on re-sync.
        std::fs::write(
            root.join(".ws/a/.onlyne/swarm.workspace.jsonc"),
            r#"{"role": "mine"}"#,
        )
        .unwrap();
        run_sync(root).unwrap();
        let snap = std::fs::read_to_string(
            root.join(".ws/a/.onlyne/swarm.workspace.jsonc"),
        )
        .unwrap();
        assert!(snap.contains("mine"));
    }

    #[test]
    fn orphan_instances_are_reported_not_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join(".ws/ghost/.onlyne")).unwrap();
        let rep = run_sync(root).unwrap();
        assert!(rep.orphans.iter().any(|o| o == "ghost"));
        assert!(root.join(".ws/ghost").exists());
    }
}
