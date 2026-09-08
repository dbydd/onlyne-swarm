use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::sync::Arc;

use crate::sched::{self, Sched};

/// Subscribe to every workspace daemon's `run/s` at top priority and
/// route swarm traffic. Runs on a plain thread (blocking sockets), one
/// connection per workspace daemon, with reconnect on drop.
pub fn pump(sched: Arc<Sched>) {
    let tree = crate::template::load_tree(&sched.root).unwrap_or_default();
    for e in tree {
        let s = sched.clone();
        let path = if e.path.is_empty() { "." } else { &e.path }.to_string();
        std::thread::spawn(move || watch_workspace(s, path));
    }
}

fn watch_workspace(sched: Arc<Sched>, ws_path: String) {
    loop {
        let ws = crate::root::resolve_instance(&sched.root, &ws_path);
        let sock = crate::root::onlyne_sock(&ws);
        if let Err(e) = watch_once(&sched, &ws_path, &sock) {
            tracing::warn!(workspace = %ws_path, error = %e, "daemon watch dropped; reconnecting");
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

fn watch_once(sched: &Arc<Sched>, ws_path: &str, sock: &std::path::Path) -> anyhow::Result<()> {
    let mut stream = UnixStream::connect(sock)?;
    // Top-priority subscription. Requires the onlyne本体 priority extension
    // (see ARCH.md §5); older daemons ignore the field and behave as before.
    stream.write_all(
        b"{\"id\":\"swarm\",\"op\":\"subscribe_events\",\"priority\":4294967295,\"consume_timeout_ms\":400}\n",
    )?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    loop {
        line.clear();
        let n = reader.read_line(&mut line)?;
        if n == 0 {
            return Ok(());
        }
        if line.trim().is_empty() {
            continue;
        }
        let v: serde_json::Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        route_event(sched, ws_path, &v, &mut stream);
    }
}

fn route_event(
    sched: &Arc<Sched>,
    ws_path: &str,
    v: &serde_json::Value,
    stream: &mut UnixStream,
) {
    if v.get("event").and_then(|e| e.as_bool()) != Some(true) {
        return;
    }
    let typ = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
    let data = v.get("data").cloned().unwrap_or(serde_json::Value::Null);
    // Event lines wrap the core Event enum: {type, data:{type, data:{...}}}.
    // Unwrap one level when present so text/message extraction works.
    let inner = data.get("data").cloned().unwrap_or(serde_json::Value::Null);
    let data = if inner.is_object() { inner } else { data };
    match typ {
        "inbound_message" => {
            let text = data
                .get("text")
                .and_then(|t| t.as_str())
                .unwrap_or("");
            if let Some(msg) = crate::proto::parse(text) {
                // Swarm task inbound: schedule a session for it. Do NOT
                // consume: the pi-onlyne session sits at tier 1 and needs
                // the same event to claim its hop. The scheduler already
                // ignores duplicate task_ids, so double delivery is safe.
                on_task_inbound(sched, ws_path, &msg);
            }
        }
        "outbound_message" => {
            let text = data
                .get("text")
                .and_then(|t| t.as_str())
                .unwrap_or("");
            if let Some(msg) = crate::proto::parse(text) {
                consumed_ack(stream, v);
                if let Err(e) = sched::on_out(sched, ws_path, &msg) {
                    tracing::warn!(error = %e, "on_out failed");
                }
            }
        }
        "workspace_state_changed" => {
            // pi-onlyne swarm-mode recycle ack arrives as WorkspaceStateChanged
            // `swarm_recycled {task_id, terminal_handle, reason}`.
            // close_terminal may already have observed the same ack by polling
            // history; on_recycled is idempotent so a late event only closes
            // the tab handle if it somehow remains.
            let msg_all = data
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("");
            if let Some(body) = msg_all.strip_prefix("swarm_recycled ") {
                let parsed: serde_json::Value = serde_json::from_str(body).unwrap_or_default();
                let task_id = parsed.get("task_id").and_then(|v| v.as_str()).unwrap_or("");
                let reason = parsed.get("reason").and_then(|v| v.as_str()).unwrap_or("recycled");
                tracing::info!(workspace = %ws_path, task = %task_id, reason = %reason, "swarm_recycled ack observed");
                consumed_ack(stream, v);
                if !task_id.is_empty() {
                    if let Err(e) = sched::on_recycled(sched, task_id, reason) {
                        tracing::warn!(error = %e, "on_recycled failed");
                    }
                }
                return;
            }
            // pi-onlyne swarm-mode handshake arrives as a WorkspaceStateChanged
            // event whose message is `swarm_ready {workspace, terminal_handle}`.
            let msg = data
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("");
            let body = msg.strip_prefix("swarm_ready ").unwrap_or("");
            if body.is_empty() {
                return;
            }
            let parsed: serde_json::Value = serde_json::from_str(body).unwrap_or_default();
            let handle = parsed
                .get("terminal_handle")
                .and_then(|h| h.as_str())
                .unwrap_or("");
            let w = parsed
                .get("workspace")
                .and_then(|w| w.as_str())
                .unwrap_or(ws_path);
            // Map the daemon-side absolute workspace path back to a tree path.
            let w = tree_path_for(sched, w).unwrap_or_else(|| w.to_string());
            consumed_ack(stream, v);
            if let Err(e) = sched::on_ready(sched, &w, handle) {
                tracing::warn!(error = %e, "on_ready failed");
            }
        }
        _ => {}
    }
}

/// Map a daemon-side absolute workspace path back to a tree-relative path
/// ("." for root). Falls back to the input when outside this tree.
/// Pure prefix logic lives in tree_path_for_root() (unit-tested); this
/// wrapper only canonicalizes both sides first (/tmp vs /private/tmp).
fn tree_path_for(sched: &Arc<Sched>, abs: &str) -> Option<String> {
    let canon = |p: &str| {
        std::fs::canonicalize(p)
            .map(|c| c.to_string_lossy().replace("\\", "/"))
            .unwrap_or_else(|_| p.to_string())
    };
    tree_path_for_root(&canon(&sched.root.to_string_lossy()), &canon(abs))
}

fn tree_path_for_root(root: &str, abs_c: &str) -> Option<String> {
    let inst = format!("{root}/.ws/");
    if abs_c == root || abs_c == format!("{root}/") {
        return Some(".".into());
    }
    if let Some(rest) = abs_c.strip_prefix(&inst) {
        return Some(rest.trim_end_matches('/').to_string());
    }
    // Already tree-relative (tests, supervisor submits).
    if !abs_c.starts_with('/') {
        return Some(abs_c.to_string());
    }
    None
}

/// A swarm task arrived at a workspace in-channel: register it (idempotent,
/// payload persisted) and dispatch a session for it.
fn on_task_inbound(sched: &Arc<Sched>, ws_path: &str, msg: &crate::proto::SwarmMessage) {
    let to = if ws_path == "." { "." } else { ws_path };
    let inserted = sched
        .db
        .insert_task(
            &msg.header.task_id,
            &msg.header.from,
            to,
            &msg.header.transfer_send_to,
            msg.header.attempt,
            &msg.payload,
        )
        .unwrap_or(false);
    if !inserted {
        return; // Duplicate delivery: drop.
    }
    // Fire-and-forget: no parent bookkeeping, no pending counter.
    // Every inbound task spawns its own session downstream.
    sched.emit(
        "task_created",
        serde_json::json!({"task_id": msg.header.task_id, "from": msg.header.from, "to": to}),
    );
    if let Err(e) = sched::dispatch_public(sched, &msg.header.task_id, to) {
        tracing::warn!(task = %msg.header.task_id, error = %e, "dispatch failed");
        let _ = sched::on_early_exit(sched, &msg.header.task_id, "dispatch failed");
    }
}

/// Reply with the `consume` op naming the event's `event_seq`, so the
/// daemon skips all lower-priority tiers for this event.
fn consumed_ack(stream: &mut UnixStream, v: &serde_json::Value) {
    let Some(seq) = v.get("event_seq").and_then(|s| s.as_u64()) else {
        return;
    };
    use std::io::Write;
    let line = serde_json::json!({"id": "swarm-consume", "op": "consume", "event_seq": seq});
    let _ = writeln!(stream, "{line}");
}

#[cfg(test)]
mod tests {
    use super::tree_path_for_root;

    #[test]
    fn daemon_paths_map_to_tree_paths() {
        // Root itself (both /tmp spellings canonicalize before this fn).
        assert_eq!(tree_path_for_root("/r", "/r"), Some(".".into()));
        assert_eq!(tree_path_for_root("/r", "/r/"), Some(".".into()));
        // Nested instances.
        assert_eq!(
            tree_path_for_root("/r", "/r/.ws/a"),
            Some("a".into())
        );
        assert_eq!(
            tree_path_for_root("/r", "/r/.ws/a/b/"),
            Some("a/b".into())
        );
        // Tree-relative input passes through (tests, supervisor submits).
        assert_eq!(tree_path_for_root("/r", "a"), Some("a".into()));
        // Outside the tree: no mapping.
        assert_eq!(tree_path_for_root("/r", "/other/x"), None);
        // A sibling that merely shares the prefix must not match.
        assert_eq!(tree_path_for_root("/r", "/r-evil/a"), None);
    }
}
