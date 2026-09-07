use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::Arc;

use crate::sched::{self, Sched};

/// Serve swarm.sock: line-delimited JSON requests, one-line responses.
/// `subscribe` upgrades the connection to an event stream.
pub async fn serve(root: &Path) -> anyhow::Result<()> {
    let sock = crate::root::swarm_sock(root);
    if sock.exists() {
        // Take over a stale socket; refuse if a live scheduler answers.
        match UnixStream::connect(&sock) {
            Ok(_) => anyhow::bail!("swarm scheduler already running at {}", sock.display()),
            Err(_) => {
                let _ = std::fs::remove_file(&sock);
            }
        }
    }
    let sched = Sched::new(
        root.to_path_buf(),
        crate::db::Db::open(root)?,
    );
    // Reap orphan pending tasks from a previous run: terminals are gone,
    // so they count as early exits (failed ledger rows, no replay).
    reap_previous_run(&sched);
    let listener = UnixListener::bind(&sock)?;
    // Spawn the daemon event-subscription pump (priority consumer on each run/s).
    {
        let s = sched.clone();
        std::thread::spawn(move || crate::events::pump(s));
    }
    // Spawn idle-pool + ready-timeout reaper.
    {
        let s = sched.clone();
        std::thread::spawn(move || reap_loop(s));
    }
    loop {
        let (stream, _) = listener.accept()?;
        let s = sched.clone();
        std::thread::spawn(move || {
            let _ = handle_conn(s, stream);
        });
    }
}

fn reap_previous_run(sched: &Arc<Sched>) {
    let Ok(rows) = sched.db.list(None, 1000) else {
        return;
    };
    for r in rows {
        if r.state == crate::db::TaskState::Running || r.state == crate::db::TaskState::Pending {
            let _ = sched::on_early_exit(sched, &r.task_id, "scheduler restarted");
        }
    }
}

fn reap_loop(sched: Arc<Sched>) {
    loop {
        std::thread::sleep(std::time::Duration::from_secs(10));
        // Idle pool TTL 60s.
        {
            let mut idle = sched.idle.lock().unwrap();
            for handles in idle.values_mut() {
                handles.retain(|t| t.since.elapsed() < std::time::Duration::from_secs(60));
            }
        }
        // Ready timeout 120s: terminal created but no swarm_ready.
        // Guard: a task that already has a ready terminal (recorded handle
        // differs from the stub placeholder or the out already landed) is
        // not a stall — reaping it would kill a live working session.
        {
            let timed_out: Vec<String> = sched
                .awaiting_ready
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, t)| t.elapsed() > std::time::Duration::from_secs(120))
                .map(|(k, _)| k.clone())
                .collect();
            for task_id in timed_out {
                let delivered = sched.db.get(&task_id).ok().flatten().map(|r| {
                    !r.terminal.is_empty()
                        && !r.terminal.starts_with("stub-")
                        && (r.state == crate::db::TaskState::Running
                            || r.state == crate::db::TaskState::Done)
                }).unwrap_or(false);
                sched.awaiting_ready.lock().unwrap().remove(&task_id);
                if delivered {
                    continue;
                }
                sched.emit(
                    "task_stalled_ready_timeout",
                    serde_json::json!({"task_id": task_id}),
                );
                let _ = sched::on_early_exit(&sched, &task_id, "swarm_ready timeout");
            }
        }
    }
}

fn handle_conn(sched: Arc<Sched>, stream: UnixStream) -> anyhow::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;
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
        let req: serde_json::Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                write_resp(&mut writer, &None, false, None, Some(format!("bad json: {e}")))?;
                continue;
            }
        };
        let id = req.get("id").cloned();
        let op = req.get("op").and_then(|o| o.as_str()).unwrap_or("");
        match op {
            "subscribe" => {
                write_resp(&mut writer, &id, true, Some(serde_json::json!({"subscribed": true})), None)?;
                let mut rx = sched.bus.sender().subscribe();
                while let Ok(ev) = rx.blocking_recv() {
                    let msg = serde_json::json!({"event": true, "type": ev.typ, "data": ev.data});
                    if writeln!(writer, "{}", msg).is_err() {
                        return Ok(());
                    }
                }
                return Ok(());
            }
            "status" => {
                let counts = sched.db.counts().unwrap_or_default();
                let tree = crate::template::load_tree(&sched.root).unwrap_or_default();
                let data = serde_json::json!({
                    "root": sched.root.to_string_lossy(),
                    "workspaces": tree.len(),
                    "tasks_by_state": counts,
                    "ledger_tail": sched.db.ledger(20).unwrap_or_default(),
                });
                write_resp(&mut writer, &id, true, Some(data), None)?;
            }
            "list_workspaces" => {
                let tree = crate::template::load_tree(&sched.root).unwrap_or_default();
                let items: Vec<_> = tree
                    .iter()
                    .map(|e| {
                        serde_json::json!({
                            "path": if e.path.is_empty() { "." } else { &e.path },
                            "name": e.name,
                            "model": {"provider": e.model.provider, "model": e.model.model, "effort": e.model.effort},
                            "back_edges": e.back_edges,
                        })
                    })
                    .collect();
                write_resp(&mut writer, &id, true, Some(serde_json::json!(items)), None)?;
            }
            "list_tasks" => {
                let state = req.get("state").and_then(|s| s.as_str());
                let limit = req.get("limit").and_then(|l| l.as_u64()).unwrap_or(50) as usize;
                match sched.db.list(state, limit) {
                    Ok(rows) => write_resp(&mut writer, &id, true, Some(serde_json::json!(rows)), None)?,
                    Err(e) => write_resp(&mut writer, &id, false, None, Some(e.to_string()))?,
                }
            }
            "submit" => {
                let to = req.get("to").and_then(|t| t.as_str()).unwrap_or("");
                let payload = req.get("payload_markdown").and_then(|p| p.as_str()).unwrap_or("");
                // Supervisor submits from "." unless told otherwise.
                let from = req.get("from").and_then(|f| f.as_str()).unwrap_or(".");
                match sched::submit(&sched, from, to, payload) {
                    Ok(task_id) => write_resp(
                        &mut writer,
                        &id,
                        true,
                        Some(serde_json::json!({"task_id": task_id})),
                        None,
                    )?,
                    Err(e) => write_resp(&mut writer, &id, false, None, Some(e.to_string()))?,
                }
            }
            "cancel" => {
                let task_id = req.get("task_id").and_then(|t| t.as_str()).unwrap_or("");
                let reason = req.get("reason").and_then(|r| r.as_str()).unwrap_or("cancelled");
                match sched::cancel(&sched, task_id, reason) {
                    Ok(ids) => write_resp(
                        &mut writer,
                        &id,
                        true,
                        Some(serde_json::json!({"cancelled": ids})),
                        None,
                    )?,
                    Err(e) => write_resp(&mut writer, &id, false, None, Some(e.to_string()))?,
                }
            }
            "toggle_swarm" => {
                let ws = req.get("workspace").and_then(|w| w.as_str()).unwrap_or("");
                let enabled = req.get("enabled").and_then(|e| e.as_bool()).unwrap_or(false);
                match toggle_swarm(&sched, ws, enabled) {
                    Ok(()) => write_resp(
                        &mut writer,
                        &id,
                        true,
                        Some(serde_json::json!({"workspace": ws, "enabled": enabled})),
                        None,
                    )?,
                    Err(e) => write_resp(&mut writer, &id, false, None, Some(e.to_string()))?,
                }
            }
            _ => write_resp(&mut writer, &id, false, None, Some(format!("unknown op: {op}")))?,
        }
    }
}

/// Drain semantics: disabling stops new session creation; running tasks finish.
/// Re-enabling resumes. State persists in the workspace config.toml [swarm] table.
fn toggle_swarm(sched: &Arc<Sched>, workspace: &str, enabled: bool) -> anyhow::Result<()> {
    let ws = crate::root::resolve_instance(&sched.root, workspace);
    let cfg_path = ws.join(".onlyne/config.toml");
    let mut text = std::fs::read_to_string(&cfg_path).unwrap_or_default();
    if text.contains("[swarm]") {
        // Flip the enabled line inside [swarm].
        let mut out = String::new();
        let mut in_swarm = false;
        for line in text.lines() {
            let t = line.trim();
            if t.starts_with('[') {
                in_swarm = t == "[swarm]";
                out.push_str(line);
                out.push('\n');
                continue;
            }
            if in_swarm && t.starts_with("enabled") {
                out.push_str(&format!("enabled = {enabled}\n"));
                continue;
            }
            out.push_str(line);
            out.push('\n');
        }
        text = out;
    } else {
        text.push_str(&format!("\n[swarm]\nenabled = {enabled}\n"));
    }
    std::fs::write(&cfg_path, text)?;
    Ok(())
}

fn write_resp(
    w: &mut UnixStream,
    id: &Option<serde_json::Value>,
    ok: bool,
    data: Option<serde_json::Value>,
    error: Option<String>,
) -> anyhow::Result<()> {
    let msg = serde_json::json!({
        "id": id,
        "ok": ok,
        "data": data,
        "error": error.map(|m| serde_json::json!({"message": m})),
    });
    writeln!(w, "{msg}")?;
    Ok(())
}
