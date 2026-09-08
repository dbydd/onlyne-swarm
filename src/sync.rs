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
    /// Orca cleanup notes: stale ghost nodes sync could not remove.
    #[serde(default)]
    pub hierarchy: Vec<String>,
}

/// Files/dirs under an instance `.pi/` that are install artifacts or
/// caches: never inherited from root, pi installs them on first boot.
const PI_SKIP_ENTRIES: &[&str] = &["npm", "node_modules", "cache", "sessions", "themes"];

/// Ensure `.ws/<role>/.pi/onlyne.json` carries `watch.autoStart = true`.
/// pi-onlyne reads `watch.autoStart` (default false) from `<cwd>/.pi/onlyne.json`;
/// without it the watcher never starts and `swarm_ready` is never emitted,
/// so every task to that workspace hangs until the ready-timeout.
/// Merges with existing keys: only the `watch.autoStart` leaf is forced.
fn ensure_onlyne_autostart(pi_dir: &Path) -> anyhow::Result<bool> {
    let path = pi_dir.join("onlyne.json");
    let mut v: serde_json::Value = if path.exists() {
        serde_json::from_str(&std::fs::read_to_string(&path)?).unwrap_or(serde_json::Value::Null)
    } else {
        serde_json::Value::Null
    };
    if !v.is_object() {
        v = serde_json::json!({});
    }
    let changed = v
        .get("watch")
        .and_then(|w| w.get("autoStart"))
        .and_then(|a| a.as_bool())
        != Some(true);
    if changed {
        if v.get("watch").map(|w| w.is_object()).unwrap_or(false) {
            v["watch"]["autoStart"] = serde_json::json!(true);
        } else {
            v["watch"] = serde_json::json!({"autoStart": true});
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, serde_json::to_string_pretty(&v)? + "\n")?;
    }
    Ok(changed)
}

/// Materialize an instance `.pi/` dir from root `.pi/` with per-role override.
///
/// Source priority: `.agents/.schedule/<role>/.pi/**` > `<root>/.pi/**`.
/// Copy-if-absent per file (same rule as instance config): existing values
/// are never touched; `--force` (future flag) may overwrite. Skips install
/// artifacts and caches (npm/, node_modules/, cache/, sessions/, themes/).
/// `settings.json` IS inherited; `hindsight.json` is per-role and NOT
/// inherited by default. Returns true when anything was written.
fn materialize_pi(root: &Path, role_path: &str, ws: &Path) -> anyhow::Result<bool> {
    // Skip the root pseudo-workspace: it IS the source.
    if role_path.is_empty() {
        return Ok(false);
    }
    let mut changed = false;
    let dst = ws.join(".pi");
    let role_src = crate::root::schedule_dir(root).join(role_path).join(".pi");
    let root_src = root.join(".pi");
    // Union of filenames from both layers.
    let mut names: std::collections::BTreeSet<String> = Default::default();
    for layer in [&role_src, &root_src] {
        if let Ok(rd) = std::fs::read_dir(layer) {
            for e in rd.flatten() {
                if let Some(n) = e.file_name().to_str() {
                    if !n.starts_with('.') {
                        names.insert(n.to_string());
                    }
                }
            }
        }
    }
    // hindsight.json is per-role state: never inherit from root.
    names.remove("hindsight.json");
    for name in &names {
        if PI_SKIP_ENTRIES.contains(&name.as_str()) {
            continue;
        }
        let src = if role_src.join(name).exists() {
            role_src.join(name)
        } else {
            root_src.join(name)
        };
        let target = dst.join(name);
        if target.exists() {
            continue;
        }
        if src.is_dir() {
            copy_dir_recursive(&src, &target)?;
            changed = true;
        } else if src.is_file() {
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::copy(&src, &target)?;
            changed = true;
        }
    }
    if ensure_onlyne_autostart(&dst)? {
        changed = true;
    }
    Ok(changed)
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(dst)?;
    for e in std::fs::read_dir(src)? {
        let e = e?;
        let name = e.file_name();
        let name_str = name.to_string_lossy();
        if name_str.starts_with('.') || PI_SKIP_ENTRIES.contains(&name_str.as_ref()) {
            continue;
        }
        let (s, d) = (e.path(), dst.join(&name));
        if e.file_type()?.is_dir() {
            copy_dir_recursive(&s, &d)?;
        } else {
            std::fs::copy(&s, &d)?;
        }
    }
    Ok(())
}

/// Append `[swarm] enabled = true` to an existing instance config that
/// predates the template (R2 drift guard). Idempotent: configs already
/// carrying the section are untouched. Ported from init's ensure_swarm_enabled
/// so `workspace create` and `run` share one rule.
fn ensure_swarm_enabled(cfg_path: &Path) -> anyhow::Result<bool> {
    use std::fmt::Write as _;
    if !cfg_path.exists() {
        return Ok(false);
    }
    let text = std::fs::read_to_string(cfg_path)?;
    // Scan section-aware: only an `enabled` line inside `[swarm]` counts.
    let mut in_swarm = false;
    let mut has_enabled_true = false;
    let mut has_swarm_section = false;
    for line in text.lines() {
        let t = line.trim();
        if t.starts_with('[') {
            in_swarm = t == "[swarm]";
            has_swarm_section |= in_swarm;
            continue;
        }
        if in_swarm && t.starts_with("enabled") {
            if t.split_once('=').map(|(_, v)| v.trim() == "true").unwrap_or(false) {
                has_enabled_true = true;
            }
        }
    }
    if has_enabled_true {
        return Ok(false);
    }
    let mut out = text;
    if !out.is_empty() && !out.ends_with('\n') {
        out.push('\n');
    }
    if has_swarm_section {
        // Section exists but enabled!=true: flip the line in place.
        let mut fixed = String::new();
        let mut in_sw = false;
        for line in out.lines() {
            let t = line.trim();
            if t.starts_with('[') {
                in_sw = t == "[swarm]";
                fixed.push_str(line);
                fixed.push('\n');
                continue;
            }
            if in_sw && t.starts_with("enabled") {
                let _ = writeln!(fixed, "enabled = true");
                in_sw = false; // only first occurrence
                continue;
            }
            fixed.push_str(line);
            fixed.push('\n');
        }
        std::fs::write(cfg_path, fixed)?;
    } else {
        out.push_str("\n[swarm]\nenabled = true\n");
        std::fs::write(cfg_path, out)?;
    }
    Ok(true)
}

/// Swarm-ready three gates for one workspace (R3). Returns the list of
/// missing gates; empty means ready. Pure over the filesystem so the
/// contract is unit-tested without a scheduler:
/// 1. `.onlyne/config.toml` carries `[swarm] enabled = true`
///    (same section-aware scan as the R2 drift guard).
/// 2. `.pi/onlyne.json` has `watch.autoStart = true` (watcher off means
///    `swarm_ready` is never emitted).
/// 3. `.pi/settings.json` packages include pi-onlyne (else the session has
///    no swarm tools at all).
pub fn swarm_ready_gaps(ws: &Path) -> Vec<String> {
    let mut gaps = vec![];
    let cfg = ws.join(".onlyne/config.toml");
    let text = std::fs::read_to_string(&cfg).unwrap_or_default();
    let mut in_swarm = false;
    let mut enabled = false;
    for line in text.lines() {
        let t = line.trim();
        if t.starts_with('[') {
            in_swarm = t == "[swarm]";
            continue;
        }
        if in_swarm
            && t.starts_with("enabled")
            && t.split_once('=').map(|(_, v)| v.trim() == "true").unwrap_or(false)
        {
            enabled = true;
        }
    }
    if !enabled {
        gaps.push("missing [swarm] enabled = true in .onlyne/config.toml".into());
    }
    let onlyne_json = ws.join(".pi/onlyne.json");
    let v: serde_json::Value = std::fs::read_to_string(&onlyne_json)
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or(serde_json::Value::Null);
    if v.pointer("/watch/autoStart").and_then(|a| a.as_bool()) != Some(true) {
        gaps.push("missing watch.autoStart = true in .pi/onlyne.json".into());
    }
    let settings = ws.join(".pi/settings.json");
    let has_plugin = std::fs::read_to_string(&settings)
        .ok()
        .and_then(|t| serde_json::from_str::<serde_json::Value>(&t).ok())
        .and_then(|v| v.get("packages").cloned())
        .and_then(|p| serde_json::from_value::<Vec<String>>(p).ok())
        .map(|pkgs| pkgs.iter().any(|p| p.contains("pi-onlyne")))
        .unwrap_or(false);
    if !has_plugin {
        gaps.push("missing pi-onlyne in .pi/settings.json packages".into());
    }
    gaps
}

/// Generate `.ws` from `.agents/.schedule`.
///
/// - Missing instance dirs are created with loopback-only config + effective snapshot.
/// - Existing instances: config and `swarm.workspace.jsonc` are never overwritten;
///   only `onlyne_in/` symlinks are refreshed.
/// - Every instance `.pi/` is materialized additive (copy-if-absent) from
///   root `.pi/` with per-role `.schedule/<role>/.pi/` override; `onlyne.json`
///   always ends with `watch.autoStart = true`.
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
        // R1/R2 additive migration for existing instances: never overwrite
        // hand values, only fill gaps (config [swarm] section, .pi files).
        ensure_swarm_enabled(&onlyne.join("config.toml"))?;
        materialize_pi(root, &e.path, &ws)?;
    }

    // Orphans: existing instance dirs with no description.
    collect_orphans(root, &known, &mut report)?;

    refresh_links(root, &tree, &mut report)?;
    register_hierarchy(root, &tree, &mut report);
    Ok(report)
}

/// Inspect generated-workspace health without writing configs or refreshing
/// symlinks. `status` uses this so the TUI can render orphan/dangling alerts.
pub fn inspect(root: &Path) -> anyhow::Result<SyncReport> {
    let tree = crate::template::load_tree(root)?;
    let mut report = SyncReport {
        workspaces: tree.len(),
        ..Default::default()
    };
    let known: BTreeSet<&str> = tree.iter().map(|e| e.path.as_str()).collect();
    collect_orphans(root, &known, &mut report)?;
    inspect_links(root, &tree, &mut report);
    Ok(report)
}

fn collect_orphans(
    root: &Path,
    known: &BTreeSet<&str>,
    report: &mut SyncReport,
) -> anyhow::Result<()> {
    let inst = crate::root::instances_dir(root);
    if !inst.is_dir() {
        return Ok(());
    }
    let mut found = vec![];
    collect_rel(&inst, &inst, &mut found)?;
    for f in found {
        if !known.contains(f.as_str()) {
            report.orphans.push(f);
        }
    }
    Ok(())
}

fn inspect_links(root: &Path, tree: &[Effective], report: &mut SyncReport) {
    let targets: Vec<&str> = tree.iter().map(|e| e.path.as_str()).collect();
    for e in tree {
        let ws = crate::root::resolve_instance(root, &e.path);
        let view = ws.join("onlyne_in");
        for target in &targets {
            if *target == e.path {
                continue;
            }
            let target_name = if target.is_empty() { "_root" } else { target };
            let link = view.join(target_name);
            let target_in = crate::root::loopback_in(&crate::root::resolve_instance(root, target));
            if std::fs::symlink_metadata(&link).is_err() || std::fs::metadata(&target_in).is_err() {
                report.dangling.push(format!(
                    "{}: onlyne_in/{}",
                    display_path(&e.path),
                    target_name
                ));
            }
        }
    }
}

/// Best-effort Orca node cleanup per workspace. Folder-kind nodes are Orca-side
/// metadata with no directory backlink: deleting `.ws/<name>` leaves a ghost
/// node that renders as Unknown. sync removes leftovers instead of creating
/// nodes (registration is disabled: ghost nodes are unlistable and hide tabs).
/// Never fails sync; records the outcome.
fn register_hierarchy(root: &Path, tree: &[Effective], report: &mut SyncReport) {
    use crate::hierarchy::HierarchyOutcome;
    for e in tree {
        if e.path.is_empty() {
            continue; // root is already an Orca worktree; nothing to clean
        }
        let ws = crate::root::resolve_instance(root, &e.path);
        let name = crate::hierarchy::node_display_name(&e.path);
        match crate::hierarchy::ensure_child(None, &ws, &name) {
            HierarchyOutcome::Clean => {}
            HierarchyOutcome::Stale { detail } => report.hierarchy.push(format!(
                "{}: stale orca node ({detail})",
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
    fn ready_gates_list_missing_gates() {
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path().join("ws");
        // Empty workspace: all three gates missing.
        assert_eq!(swarm_ready_gaps(&ws).len(), 3);
        // Gate 1 only.
        std::fs::create_dir_all(ws.join(".onlyne")).unwrap();
        std::fs::write(ws.join(".onlyne/config.toml"), "[swarm]\nenabled = true\n").unwrap();
        assert_eq!(swarm_ready_gaps(&ws).len(), 2);
        // Gates 1+2.
        std::fs::create_dir_all(ws.join(".pi")).unwrap();
        std::fs::write(ws.join(".pi/onlyne.json"), r#"{"watch":{"autoStart":true}}"#).unwrap();
        let gaps = swarm_ready_gaps(&ws);
        assert_eq!(gaps.len(), 1);
        assert!(gaps[0].contains("pi-onlyne"));
        // All three.
        std::fs::write(ws.join(".pi/settings.json"), r#"{"packages":["npm:pi-onlyne@^0.8.1"]}"#).unwrap();
        assert!(swarm_ready_gaps(&ws).is_empty());
    }

    #[test]
    fn pi_materializes_copy_if_absent_with_role_override() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        // Root .pi has settings + onlyne.json (autoStart off) + skipped npm/.
        std::fs::create_dir_all(root.join(".pi/npm")).unwrap();
        std::fs::write(root.join(".pi/npm/pkg"), "x").unwrap();
        std::fs::write(
            root.join(".pi/settings.json"),
            r#"{"packages":["npm:pi-onlyne@^0.8.1"]}"#,
        )
        .unwrap();
        std::fs::write(
            root.join(".pi/onlyne.json"),
            r#"{"watch":{"autoStart":false},"keep":"mine"}"#,
        )
        .unwrap();
        // Per-role override wins for settings.json.
        std::fs::create_dir_all(root.join(".agents/.schedule/a/.pi")).unwrap();
        std::fs::write(
            root.join(".agents/.schedule/a/template.workspace.jsonc"),
            r#"{"name": "a", "role": "r"}"#,
        )
        .unwrap();
        std::fs::write(
            root.join(".agents/.schedule/a/.pi/settings.json"),
            r#"{"role":"override"}"#,
        )
        .unwrap();
        run_sync(root).unwrap();
        let pi = root.join(".ws/a/.pi");
        assert_eq!(
            std::fs::read_to_string(pi.join("settings.json")).unwrap(),
            r#"{"role":"override"}"#
        );
        // onlyne.json merged: autoStart forced true, other keys kept.
        let v: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(pi.join("onlyne.json")).unwrap())
                .unwrap();
        assert_eq!(v.pointer("/watch/autoStart"), Some(&serde_json::json!(true)));
        assert_eq!(v.get("keep").and_then(|k| k.as_str()), Some("mine"));
        // Install artifacts never inherited.
        assert!(!pi.join("npm").exists());
        // Second sync never overwrites hand values.
        std::fs::write(pi.join("settings.json"), r#"{"role":"hand"}"#).unwrap();
        run_sync(root).unwrap();
        assert_eq!(
            std::fs::read_to_string(pi.join("settings.json")).unwrap(),
            r#"{"role":"hand"}"#
        );
    }

    #[test]
    fn swarm_section_appended_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join(".agents/.schedule/a")).unwrap();
        std::fs::write(
            root.join(".agents/.schedule/a/template.workspace.jsonc"),
            r#"{"name": "a", "role": "r"}"#,
        )
        .unwrap();
        run_sync(root).unwrap();
        // Simulate a pre-template instance: strip [swarm], keep the rest.
        let cfg = root.join(".ws/a/.onlyne/config.toml");
        let stripped: String = std::fs::read_to_string(&cfg)
            .unwrap()
            .lines()
            .filter(|l| l.trim() != "[swarm]" && l.trim() != "enabled = true")
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        std::fs::write(&cfg, &stripped).unwrap();
        assert!(!std::fs::read_to_string(&cfg).unwrap().contains("[swarm]"));
        run_sync(root).unwrap();
        let back = std::fs::read_to_string(&cfg).unwrap();
        assert!(back.contains("[swarm]") && back.contains("enabled = true"));
        assert!(back.contains("[workspace]")); // other keys untouched
        // Idempotent: third sync changes nothing.
        run_sync(root).unwrap();
        assert_eq!(std::fs::read_to_string(&cfg).unwrap(), back);
    }

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
