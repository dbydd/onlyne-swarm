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

/// One dead-terminal sweep iteration, extracted for unit testing.
/// Returns (failed_task_ids, retried_task_ids).
///
/// Retry policy is root-configurable: `.onlyne/swarm.workspace.jsonc` may set
/// `"retry": {"max_attempts": 0}` for rings the operator wants retried
/// without a cap (max_attempts 0 = unbounded, each failure replays the task
/// unchanged with attempt+1). Absent config keeps MAX_ATTEMPTS = 3.
fn sweep_dead_terminals(sched: &Arc<Sched>) -> (Vec<String>, Vec<String>) {
    const MAX_ATTEMPTS: u32 = 3;
    let max_attempts: Option<u32> = sched.root_retry_max_attempts();
    let mut failed = vec![];
    let mut retried = vec![];
    // Dead-terminal sweep: a Running task whose orca tab is gone (pi
    // exited without writing out) would otherwise sit in running
    // forever. Detect via `terminal show`, record failed, and requeue
    // with attempt+1 so the hop is retried in a fresh terminal.
    // Attempt cap lives here, not in dispatch. A 15s delivery grace avoids
    // racing a clean stub/session exit against the daemon's out event relay.
    const DELIVERY_GRACE: std::time::Duration = std::time::Duration::from_secs(15);
    let since = sched.running_since.lock().unwrap().clone();
    let running: Vec<(String, String, u32)> = sched
        .db
        .list(Some("running"), 200)
        .unwrap_or_default()
        .into_iter()
        .filter(|r| {
            !r.terminal.is_empty()
                && !r.terminal.starts_with("stub-")
                && since
                    .get(&r.task_id)
                    .map(|t| t.elapsed() >= DELIVERY_GRACE)
                    .unwrap_or(false)
        })
        .map(|r| (r.task_id.clone(), r.terminal.clone(), r.attempt))
        .collect();
    for (task_id, handle, attempt) in running {
        if crate::orca_term::is_alive(&handle) {
            continue;
        }
        // Terminal dead, no out: failed + ledger, then retry or drop.
        let _ = sched::on_early_exit(sched, &task_id, "terminal exited before out");
        failed.push(task_id.clone());
        if max_attempts.map(|m| attempt < m).unwrap_or(attempt < MAX_ATTEMPTS) {
            if let Ok(Some(task)) = sched.db.get(&task_id) {
                let retry_id = uuid::Uuid::new_v4().to_string();
                if sched
                    .db
                    .insert_task(
                        &retry_id,
                        &task.from_ws,
                        &task.to_ws,
                        &task.transfer_send_to,
                        attempt + 1,
                        &task.payload,
                    )
                    .unwrap_or(false)
                {
                    sched.emit(
                        "task_retried",
                        serde_json::json!({
                            "task_id": retry_id,
                            "from_failed": task_id,
                            "attempt": attempt + 1,
                        }),
                    );
                    // Best effort: orca may be unreachable in tests.
                    let _ = sched::dispatch_public(&sched, &retry_id, &task.to_ws);
                    retried.push(retry_id);
                }
            }
        } else {
            tracing::warn!(task = %task_id, attempt, max_attempts = ?max_attempts, "terminal dead, attempt cap reached; not retrying");
        }
    }
    (failed, retried)
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
        // Dead-terminal sweep lives in sweep_dead_terminals (unit-tested).
        let (_failed, _retried) = sweep_dead_terminals(&sched);
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
                // Marquee policy: an unbounded root retry budget replays a
                // ready-timeout stall as a fresh terminal instead of parking
                // the ring. Bounded roots keep the old fail-stop behavior.
                if sched.root_retry_max_attempts() == Some(0) {
                    if let Ok(Some(task)) = sched.db.get(&task_id) {
                        let retry_id = uuid::Uuid::new_v4().to_string();
                        let _ = sched::on_early_exit(&sched, &task_id, "swarm_ready timeout");
                        if sched
                            .db
                            .insert_task(
                                &retry_id,
                                &task.from_ws,
                                &task.to_ws,
                                &task.transfer_send_to,
                                task.attempt + 1,
                                &task.payload,
                            )
                            .unwrap_or(false)
                        {
                            sched.emit(
                                "task_retried",
                                serde_json::json!({
                                    "task_id": retry_id,
                                    "from_failed": task_id,
                                    "attempt": task.attempt + 1,
                                }),
                            );
                            let _ = sched::dispatch_public(&sched, &retry_id, &task.to_ws);
                        }
                    }
                    continue;
                }
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
                let health = crate::sync::inspect(&sched.root).unwrap_or_default();
                let data = serde_json::json!({
                    "root": sched.root.to_string_lossy(),
                    "workspaces": tree.len(),
                    "tasks_by_state": counts,
                    "ledger_tail": sched.db.ledger(20).unwrap_or_default(),
                    "orphans": health.orphans,
                    "dangling": health.dangling,
                });
                write_resp(&mut writer, &id, true, Some(data), None)?;
            }
            "graph" => match sched.db.graph() {
                Ok(graph) => write_resp(&mut writer, &id, true, Some(serde_json::json!(graph)), None)?,
                Err(e) => write_resp(&mut writer, &id, false, None, Some(e.to_string()))?,
            },
            "list_workspaces" => {
                let tree = crate::template::load_tree(&sched.root).unwrap_or_default();
                let items: Vec<_> = tree
                    .iter()
                    .map(|e| {
                        let path = if e.path.is_empty() { "." } else { &e.path };
                        let ws = crate::root::resolve_instance(&sched.root, &e.path);
                        let daemon = if crate::daemon::is_alive(&crate::root::onlyne_sock(&ws)) {
                            "ready"
                        } else {
                            "offline"
                        };
                        serde_json::json!({
                            "path": path,
                            "name": e.name,
                            "model": {"provider": e.model.provider, "model": e.model.model, "effort": e.model.effort},
                            "back_edges": e.back_edges,
                            "daemon": daemon,
                        })
                    })
                    .collect();
                write_resp(&mut writer, &id, true, Some(serde_json::json!(items)), None)?;
            }
            "list_tasks" => {
                let filter = task_filter(&req);
                match sched.db.list_page(&filter) {
                    Ok(page) => write_resp(&mut writer, &id, true, Some(serde_json::json!(page)), None)?,
                    Err(e) => write_resp(&mut writer, &id, false, None, Some(e.to_string()))?,
                }
            }
            "task_detail" => {
                let task_id = req.get("task_id").and_then(|v| v.as_str()).unwrap_or("");
                match sched.db.detail(task_id) {
                    Ok(Some(detail)) => write_resp(&mut writer, &id, true, Some(serde_json::json!(detail)), None)?,
                    Ok(None) => write_resp(
                        &mut writer,
                        &id,
                        false,
                        None,
                        Some(format!("unknown task: {task_id}")),
                    )?,
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
                let force = req.get("force").and_then(|f| f.as_bool()).unwrap_or(false);
                match sched::cancel_force(&sched, task_id, reason, force) {
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

fn task_filter(req: &serde_json::Value) -> crate::db::TaskFilter {
    crate::db::TaskFilter {
        state: req.get("state").and_then(|v| v.as_str()).map(str::to_owned),
        to_ws: req.get("to_ws").and_then(|v| v.as_str()).map(str::to_owned),
        from_ws: req.get("from_ws").and_then(|v| v.as_str()).map(str::to_owned),
        text: req.get("text").and_then(|v| v.as_str()).map(str::to_owned),
        since: req.get("since").and_then(|v| v.as_i64()),
        retry_only: req.get("retry_only").and_then(|v| v.as_bool()).unwrap_or(false),
        limit: req.get("limit").and_then(|v| v.as_u64()).unwrap_or(50) as usize,
        offset: req.get("offset").and_then(|v| v.as_u64()).unwrap_or(0) as usize,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{Db, TaskState};

    fn test_sched() -> Arc<Sched> {
        let dir = tempfile::tempdir().unwrap();
        let root: &'static std::path::Path =
            Box::leak(dir.path().join("root").into_boxed_path());
        std::fs::create_dir_all(root).unwrap();
        let _ = Box::leak(Box::new(dir));
        let db = Db::open(root).unwrap();
        Sched::new(root.to_path_buf(), db)
    }

    #[test]
    fn task_filter_maps_all_history_predicates() {
        let filter = task_filter(&serde_json::json!({
            "state": "active",
            "to_ws": "scout",
            "from_ws": "model",
            "text": "needle",
            "since": 123,
            "retry_only": true,
            "limit": 42,
            "offset": 84,
        }));
        assert_eq!(filter.state.as_deref(), Some("active"));
        assert_eq!(filter.to_ws.as_deref(), Some("scout"));
        assert_eq!(filter.from_ws.as_deref(), Some("model"));
        assert_eq!(filter.text.as_deref(), Some("needle"));
        assert_eq!(filter.since, Some(123));
        assert!(filter.retry_only);
        assert_eq!(filter.limit, 42);
        assert_eq!(filter.offset, 84);
    }

    #[test]
    fn sweep_ignores_stub_handles() {
        std::env::set_var("ORCA_CLI_COMMAND", "/bin/false");
        let s = test_sched();
        s.db.insert_task("stub1", ".", "a", "", 1, "p").unwrap();
        s.db.set_state("stub1", TaskState::Running).unwrap();
        s.db.set_terminal("stub1", "stub-term").unwrap();
        let (failed, retried) = sweep_dead_terminals(&s);
        assert!(failed.is_empty());
        assert!(retried.is_empty());
        std::env::remove_var("ORCA_CLI_COMMAND");
    }

    #[test]
    fn fresh_delivery_skips_dead_terminal_in_grace() {
        std::env::set_var("ORCA_CLI_COMMAND", "/bin/false");
        let s = test_sched();
        s.db.insert_task("fresh1", ".", "a", "", 1, "p").unwrap();
        s.db.set_state("fresh1", TaskState::Running).unwrap();
        s.db.set_terminal("fresh1", "term-fresh").unwrap();
        // Inside the grace window: never reaped, even though the orca probe
        // would say dead (the out-event race the sweep must not win).
        s.running_since.lock().unwrap().insert("fresh1".into(), std::time::Instant::now());
        let (failed, retried) = sweep_dead_terminals(&s);
        assert!(failed.is_empty());
        assert!(retried.is_empty());
        // Older than grace still stays untouched when the Orca probe is
        // unavailable: only an explicit `status=exited` may fail a task.
        // The explicit-status parser has dedicated unit coverage in
        // orca_term::tests::terminal_liveness_requires_explicit_exited_status.
        s.running_since.lock().unwrap().insert(
            "fresh1".into(),
            std::time::Instant::now() - std::time::Duration::from_secs(30),
        );
        let (failed, retried) = sweep_dead_terminals(&s);
        assert!(failed.is_empty());
        assert!(retried.is_empty());
        std::env::remove_var("ORCA_CLI_COMMAND");
    }
}
