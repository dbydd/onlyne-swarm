use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::sync::Arc;

use crate::sched::{self, Sched};

/// Subscribe to every workspace daemon's `onlyne.sock` at top priority and
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
                // Swarm task inbound: consume (cancel lower-priority delivery),
                // then schedule a session for it.
                consumed_ack(stream, v);
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
                if let Err(e) = sched::on_reply(sched, ws_path, &msg) {
                    tracing::warn!(error = %e, "on_reply failed");
                }
            }
        }
        "workspace_state_changed" => {
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
fn tree_path_for(sched: &Arc<Sched>, abs: &str) -> Option<String> {
    // Canonicalize: macOS /tmp symlinks to /private/tmp, and the daemon may
    // report either form. Compare canonicalized prefixes.
    let canon = |p: &str| {
        std::fs::canonicalize(p)
            .map(|c| c.to_string_lossy().replace("\\", "/"))
            .unwrap_or_else(|_| p.to_string())
    };
    let root = canon(&sched.root.to_string_lossy());
    let abs_c = canon(abs);
    let inst = format!("{root}/_onlyne_workspaces/");
    if abs_c == root || abs_c == format!("{root}/") {
        return Some(".".into());
    }
    if let Some(rest) = abs_c.strip_prefix(&inst) {
        return Some(rest.trim_end_matches('/').to_string());
    }
    // Already tree-relative (tests, supervisor submits).
    if !abs.starts_with('/') {
        return Some(abs.to_string());
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
            &msg.header.reply_to,
            msg.header.attempt,
            &msg.payload,
        )
        .unwrap_or(false);
    if !inserted {
        return; // Duplicate delivery: drop.
    }
    // Parent bookkeeping: a new child means +1 pending on the parent.
    if !msg.header.reply_to.is_empty() {
        let _ = sched.db.bump_parent(&msg.header.reply_to, 1);
    }
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
