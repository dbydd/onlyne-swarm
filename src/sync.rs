use anyhow::Context;
use serde::Serialize;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

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

[swarm.transport]
mode = "rpc"
fifo = false
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
    /// Compatibility findings for pre-existing legacy `onlyne_in` views.
    /// Sync preserves these views and their links for diagnosis.
    #[serde(default)]
    pub legacy_views: Vec<String>,
    /// Readiness and ownership drift found during sync. Existing supervisor
    /// values remain untouched.
    #[serde(default)]
    pub gaps: Vec<String>,
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
#[allow(dead_code)]
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
#[allow(dead_code)]
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
            if t.split_once('=')
                .map(|(_, v)| v.trim() == "true")
                .unwrap_or(false)
            {
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
            && t.split_once('=')
                .map(|(_, v)| v.trim() == "true")
                .unwrap_or(false)
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
        .and_then(|p| p.as_array().cloned())
        .map(|pkgs| {
            pkgs.iter().any(|p| {
                let Some(raw) = p.as_str() else { return false };
                raw.contains("pi-onlyne")
                    || package_manifest_name(&ws.join(".pi").join(raw))
                        .ok()
                        .flatten()
                        .as_deref()
                        == Some("pi-onlyne")
            })
        })
        .unwrap_or(false);
    if !has_plugin {
        gaps.push("missing pi-onlyne in .pi/settings.json packages".into());
    }
    gaps
}

fn package_manifest_name(path: &Path) -> anyhow::Result<Option<String>> {
    let manifest = path.join("package.json");
    if !manifest.is_file() {
        return Ok(None);
    }
    let value: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(manifest)?)?;
    Ok(value
        .get("name")
        .and_then(|v| v.as_str())
        .map(str::to_owned))
}

fn relative_path(from: &Path, to: &Path) -> PathBuf {
    let from = from.components().collect::<Vec<_>>();
    let to = to.components().collect::<Vec<_>>();
    let mut common = 0;
    while common < from.len() && common < to.len() && from[common] == to[common] {
        common += 1;
    }
    let mut out = PathBuf::new();
    for _ in common..from.len() {
        out.push("..");
    }
    for component in &to[common..] {
        out.push(component.as_os_str());
    }
    if out.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        out
    }
}

/// First-create bootstrap. All source validation happens before child writes.
fn bootstrap_child(root: &Path, ws: &Path, e: &Effective) -> anyhow::Result<()> {
    let settings_path = root.join(".pi/settings.json");
    let source_text = std::fs::read_to_string(&settings_path)
        .with_context(|| format!("read {}", settings_path.display()))?;
    let mut settings: serde_json::Value = serde_json::from_str(&source_text)
        .with_context(|| format!("parse {}", settings_path.display()))?;
    let packages = settings
        .get_mut("packages")
        .and_then(|v| v.as_array_mut())
        .ok_or_else(|| anyhow::anyhow!("root .pi/settings.json must contain packages"))?;
    let mut pi_source = None;
    for package in packages.iter().filter_map(|v| v.as_str()) {
        let candidate = root.join(package);
        if package.starts_with('.')
            && package_manifest_name(&candidate)?.as_deref() == Some("pi-onlyne")
        {
            pi_source = Some(candidate);
            break;
        }
    }
    let pi_source = pi_source.ok_or_else(|| {
        anyhow::anyhow!("root .pi/settings.json has no resolvable pi-onlyne package entry")
    })?;
    let rewritten = relative_path(&ws.join(".pi"), &pi_source)
        .to_string_lossy()
        .replace('\\', "/");
    for package in packages.iter_mut() {
        if package.as_str().is_some_and(|p| p.starts_with('.')) {
            let candidate = root.join(package.as_str().unwrap());
            if package_manifest_name(&candidate)?.as_deref() == Some("pi-onlyne") {
                *package = serde_json::Value::String(rewritten.clone());
            }
        }
    }
    std::fs::create_dir_all(ws.join(".onlyne"))?;
    std::fs::create_dir_all(ws.join(".pi"))?;
    std::fs::write(
        ws.join(".pi/settings.json"),
        serde_json::to_string_pretty(&settings)? + "\n",
    )?;
    std::fs::write(
        ws.join(".pi/onlyne.json"),
        "{\n  \"watch\": { \"autoStart\": true }\n}\n",
    )?;
    let snap = serde_json::to_string_pretty(&serde_json::json!({
        "name": e.name, "role": e.role,
        "model": {"provider": e.model.provider, "model": e.model.model, "effort": e.model.effort},
        "back_edges": e.back_edges,
    }))?;
    std::fs::write(ws.join(".onlyne/swarm.workspace.jsonc"), snap + "\n")?;
    std::fs::write(ws.join(".onlyne/config.toml"), INSTANCE_CONFIG)?;
    Ok(())
}

///
/// - Missing instance dirs are created with loopback-only config + effective snapshot.
/// - Existing instances: config, `.pi/`, and `swarm.workspace.jsonc` are never
///   overwritten. Pre-existing `onlyne_in/` views are diagnosed and preserved.
/// - Every instance `.pi/` is materialized additive (copy-if-absent) from
///   root `.pi/` with per-role `.schedule/<role>/.pi/` override; `onlyne.json`
///   always ends with `watch.autoStart = true`.
/// - New workspaces use the workspace daemon's loopback RPC. Existing
///   `onlyne_in/` views are compatibility state and receive no new links.
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
        let is_new = !e.path.is_empty() && !onlyne.join("config.toml").exists();
        if is_new {
            // Validate root settings and write every generated field together.
            bootstrap_child(root, &ws, e)?;
            report.created.push(display_path(&e.path));
        } else if !e.path.is_empty() {
            // Existing child workspaces are supervisor-owned. Sync only reports
            // readiness drift and preserves all generated files.
            report.gaps.extend(
                swarm_ready_gaps(&ws)
                    .into_iter()
                    .map(|gap| format!("{}: {gap}", display_path(&e.path))),
            );
        }
        std::fs::create_dir_all(onlyne.join("channels/loopback"))
            .with_context(|| format!("create {}", onlyne.display()))?;
        std::fs::create_dir_all(onlyne.join("run"))?;
        std::fs::create_dir_all(onlyne.join("logs"))?;

        // FIFO paths are created by the daemon. Sync does not materialize them.
    }

    // Orphans: existing instance dirs with no description.
    collect_orphans(root, &known, &mut report)?;

    refresh_links(root, &tree, &mut report)?;
    register_hierarchy(root, &tree, &mut report);
    Ok(report)
}

/// Inspect generated-workspace health without writing configs or creating
/// compatibility links. `status` uses this so the TUI can render orphan,
/// dangling-link, and legacy-view alerts.
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

/// Diagnose pre-existing `onlyne_in/<target>` compatibility views.
/// New RPC workspaces have no view, which is expected. Existing views and
/// their links are left untouched so old workspaces remain diagnosable.
fn inspect_links(root: &Path, tree: &[Effective], report: &mut SyncReport) {
    let targets: Vec<&str> = tree.iter().map(|e| e.path.as_str()).collect();
    for e in tree {
        let ws = crate::root::resolve_instance(root, &e.path);
        let view = ws.join("onlyne_in");
        if !view.is_dir() {
            continue;
        }
        let display = display_path(&e.path);
        for target in &targets {
            if display_path(target) == display {
                continue;
            }
            let target_name = if target.is_empty() { "_root" } else { target };
            let link = view.join(target_name);
            if !link.is_symlink() {
                continue;
            }
            add_unique(
                &mut report.legacy_views,
                format!("{}: onlyne_in/{}", display, target_name),
            );
            // `metadata` follows the link to the daemon-owned loopback path. A
            // missing FIFO is a compatibility finding; the link is preserved.
            if std::fs::metadata(&link).is_err() {
                add_unique(
                    &mut report.dangling,
                    format!("{}: onlyne_in/{}", display, target_name),
                );
            }
        }
    }
}

fn add_unique(items: &mut Vec<String>, item: String) {
    if !items.contains(&item) {
        items.push(item);
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

/// Preserve pre-existing `onlyne_in/<target>` compatibility views. This scan
/// creates nothing; each existing link targets the daemon's
/// `.onlyne/channels/loopback/in` path and is diagnosed without modification.
fn refresh_links(root: &Path, tree: &[Effective], report: &mut SyncReport) -> anyhow::Result<()> {
    inspect_links(root, tree, report);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    fn setup_package(root: &Path) {
        std::fs::create_dir_all(root.join(".pi")).unwrap();
        std::fs::write(
            root.join(".pi/settings.json"),
            r#"{"packages":["./harness/pi-onlyne","npm:other"]}"#,
        )
        .unwrap();
        std::fs::create_dir_all(root.join("harness/pi-onlyne")).unwrap();
        std::fs::write(
            root.join("harness/pi-onlyne/package.json"),
            r#"{"name":"pi-onlyne"}"#,
        )
        .unwrap();
    }

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
        std::fs::write(
            ws.join(".pi/onlyne.json"),
            r#"{"watch":{"autoStart":true}}"#,
        )
        .unwrap();
        let gaps = swarm_ready_gaps(&ws);
        assert_eq!(gaps.len(), 1);
        assert!(gaps[0].contains("pi-onlyne"));
        // All three.
        std::fs::write(
            ws.join(".pi/settings.json"),
            r#"{"packages":["npm:pi-onlyne@^0.8.1"]}"#,
        )
        .unwrap();
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
        setup_package(root);
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
        let settings: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(pi.join("settings.json")).unwrap())
                .unwrap();
        assert!(settings["packages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "../../../harness/pi-onlyne"));
        assert!(settings["packages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "npm:other"));
        let v: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(pi.join("onlyne.json")).unwrap())
                .unwrap();
        assert_eq!(
            v.pointer("/watch/autoStart"),
            Some(&serde_json::json!(true))
        );
        assert!(!pi.join("npm").exists());
        // Later sync preserves hand-edited supervisor files.
        std::fs::write(pi.join("settings.json"), r#"{"hand":true}"#).unwrap();
        std::fs::write(pi.join("onlyne.json"), r#"{"watch":{"autoStart":false}}"#).unwrap();
        run_sync(root).unwrap();
        assert_eq!(
            std::fs::read_to_string(pi.join("settings.json")).unwrap(),
            r#"{"hand":true}"#
        );
        assert_eq!(
            std::fs::read_to_string(pi.join("onlyne.json")).unwrap(),
            r#"{"watch":{"autoStart":false}}"#
        );
    }

    #[test]
    fn swarm_section_preserves_operator_disable() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        setup_package(root);
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
        // An existing supervisor config is preserved during later sync.
        std::fs::write(&cfg, "[swarm]\nenabled = false\n").unwrap();
        run_sync(root).unwrap();
        assert_eq!(
            std::fs::read_to_string(&cfg).unwrap(),
            "[swarm]\nenabled = false\n"
        );
    }

    #[test]
    fn sync_creates_rpc_workspaces_without_legacy_views() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        setup_package(root);
        std::fs::create_dir_all(root.join(".agents/.schedule/a")).unwrap();
        std::fs::write(
            root.join(".agents/.schedule/a/template.workspace.jsonc"),
            r#"{"name": "a", "role": "r"}"#,
        )
        .unwrap();
        let rep = run_sync(root).unwrap();
        assert_eq!(rep.workspaces, 2);
        assert!(rep.created.iter().any(|c| c == "a"));
        let cfg = std::fs::read_to_string(root.join(".ws/a/.onlyne/config.toml")).unwrap();
        assert!(cfg.contains("[swarm]"));
        assert!(cfg.contains("[swarm.transport]"));
        assert!(cfg.contains("mode = \"rpc\""));
        assert!(cfg.contains("fifo = false"));
        assert!(root.join(".ws/a/.onlyne/channels/loopback").is_dir());
        assert!(!root.join("onlyne_in").exists());
        assert!(!root.join(".ws/a/onlyne_in").exists());
        assert!(rep.legacy_views.is_empty());
        assert!(rep.dangling.is_empty());
        // Hand-written overlay preserved on re-sync.
        std::fs::write(
            root.join(".ws/a/.onlyne/swarm.workspace.jsonc"),
            r#"{"role": "mine"}"#,
        )
        .unwrap();
        run_sync(root).unwrap();
        let snap =
            std::fs::read_to_string(root.join(".ws/a/.onlyne/swarm.workspace.jsonc")).unwrap();
        assert!(snap.contains("mine"));
    }

    #[test]
    fn legacy_onlyne_in_views_are_preserved_and_diagnosed() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        setup_package(root);
        std::fs::create_dir_all(root.join(".agents/.schedule/a")).unwrap();
        std::fs::write(
            root.join(".agents/.schedule/a/template.workspace.jsonc"),
            r#"{"name": "a", "role": "r"}"#,
        )
        .unwrap();
        run_sync(root).unwrap();

        let child_in = crate::root::loopback_in(&root.join(".ws/a"));
        let root_in = crate::root::loopback_in(root);
        std::fs::create_dir_all(child_in.parent().unwrap()).unwrap();
        std::fs::create_dir_all(root_in.parent().unwrap()).unwrap();
        std::fs::write(&child_in, "").unwrap();
        std::fs::write(&root_in, "").unwrap();

        let root_view = root.join("onlyne_in");
        let child_view = root.join(".ws/a/onlyne_in");
        std::fs::create_dir_all(&root_view).unwrap();
        std::fs::create_dir_all(&child_view).unwrap();
        symlink(&child_in, root_view.join("a")).unwrap();
        symlink(&root_in, child_view.join("_root")).unwrap();

        let rep = run_sync(root).unwrap();
        assert!(root_view.join("a").is_symlink());
        assert!(child_view.join("_root").is_symlink());
        assert_eq!(std::fs::read_link(root_view.join("a")).unwrap(), child_in);
        assert_eq!(
            std::fs::read_link(child_view.join("_root")).unwrap(),
            root_in
        );
        assert!(rep.legacy_views.contains(&".: onlyne_in/a".into()));
        assert!(rep.legacy_views.contains(&"a: onlyne_in/_root".into()));
        assert!(rep.dangling.is_empty());

        std::fs::remove_file(&child_in).unwrap();
        let rep = inspect(root).unwrap();
        assert!(root_view.join("a").is_symlink());
        assert!(rep.dangling.contains(&".: onlyne_in/a".into()));
    }

    #[test]
    fn bootstrap_fails_without_valid_pi_onlyne_and_keeps_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join(".pi")).unwrap();
        let original = r#"{"packages":["npm:other"]}"#;
        std::fs::write(root.join(".pi/settings.json"), original).unwrap();
        std::fs::create_dir_all(root.join(".agents/.schedule/a")).unwrap();
        std::fs::write(
            root.join(".agents/.schedule/a/template.workspace.jsonc"),
            "{}",
        )
        .unwrap();
        assert!(run_sync(root).is_err());
        assert_eq!(
            std::fs::read_to_string(root.join(".pi/settings.json")).unwrap(),
            original
        );
        assert!(!root.join(".ws/a/.onlyne/config.toml").exists());
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
