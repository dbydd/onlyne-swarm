use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

/// Scheduler events fanned out to swarm.sock `subscribe` clients and the TUI.
#[derive(Debug, Clone)]
pub struct SchedEvent {
    pub typ: String,
    pub data: serde_json::Value,
}

#[derive(Default)]
pub struct Bus {
    tx: Mutex<Option<broadcast::Sender<SchedEvent>>>,
}

impl Bus {
    pub fn sender(&self) -> broadcast::Sender<SchedEvent> {
        let mut g = self.tx.lock().unwrap();
        if g.is_none() {
            let (tx, _) = broadcast::channel(1024);
            *g = Some(tx);
        }
        g.as_ref().unwrap().clone()
    }
}

/// Runtime state shared by the scheduler loop, event subscriptions and IPC.
pub struct Sched {
    pub root: PathBuf,
    pub db: crate::db::Db,
    pub bus: Arc<Bus>,
    /// task_id -> terminal handle for running tasks.
    pub terminals: Mutex<HashMap<String, String>>,
    /// workspace path -> idle ready terminal handles (transient pool).
    pub idle: Mutex<HashMap<String, Vec<IdleTerminal>>>,
    /// task_id -> time the terminal was created (ready-timeout tracking).
    pub awaiting_ready: Mutex<HashMap<String, std::time::Instant>>,
}

#[derive(Debug, Clone)]
pub struct IdleTerminal {
    pub handle: String,
    pub since: std::time::Instant,
}

impl Sched {
    pub fn new(root: PathBuf, db: crate::db::Db) -> Arc<Self> {
        Arc::new(Self {
            root,
            db,
            bus: Arc::new(Bus::default()),
            terminals: Mutex::new(HashMap::new()),
            idle: Mutex::new(HashMap::new()),
            awaiting_ready: Mutex::new(HashMap::new()),
        })
    }

    pub fn emit(&self, typ: &str, data: serde_json::Value) {
        let _ = self.bus.sender().send(SchedEvent {
            typ: typ.into(),
            data,
        });
    }
}

/// Submit a payload to a workspace: allocate task_id, persist, and queue for dispatch.
/// Returns the task_id (= session id).
pub fn submit(
    sched: &Arc<Sched>,
    from: &str,
    to: &str,
    payload_markdown: &str,
) -> anyhow::Result<String> {
    let tree = crate::template::load_tree(&sched.root)?;
    let known = tree.iter().any(|e| {
        let p = if e.path.is_empty() { "." } else { &e.path };
        p == to
    });
    if !known {
        anyhow::bail!("unknown workspace: {to}");
    }
    let task_id = uuid::Uuid::new_v4().to_string();
    let inserted = sched.db.insert_task(
        &task_id,
        from,
        to,
        "",
        1,
        payload_markdown,
    )?;
    if !inserted {
        anyhow::bail!("duplicate task_id: {task_id}");
    }
    sched.emit(
        "task_created",
        serde_json::json!({"task_id": task_id, "from": from, "to": to}),
    );
    dispatch(sched, &task_id, to)?;
    Ok(task_id)
}

/// Dispatch a pending task: reuse an idle ready terminal on the same path,
/// else create a new orca terminal running pi and wait for `swarm_ready`.
/// Public so the event pump can dispatch tasks arriving via loopback/in.
pub fn dispatch_public(sched: &Arc<Sched>, task_id: &str, to: &str) -> anyhow::Result<()> {
    dispatch(sched, task_id, to)
}

fn dispatch(sched: &Arc<Sched>, task_id: &str, to: &str) -> anyhow::Result<()> {
    // 1. Try idle pool (same workspace path affinity).
    let idle_handle = sched
        .idle
        .lock()
        .unwrap()
        .get_mut(to)
        .and_then(|v| v.pop())
        .map(|t| t.handle);
    if let Some(handle) = idle_handle {
        deliver_to_terminal(sched, task_id, to, &handle)?;
        return Ok(());
    }
    // 2. Create a new terminal running pi in swarm mode. The session reports
    // `swarm_ready`; on_ready() then delivers the persisted payload through
    // loopback/in into the session's followUp task queue.
    let ws = crate::root::resolve_instance(&sched.root, to);
    let title = format!("swarm:{to}:{task_id_short}", task_id_short = &task_id[..8]);
    let term = crate::orca_term::create(&title, &ws, task_id)?;
    sched
        .terminals
        .lock()
        .unwrap()
        .insert(task_id.into(), term.handle.clone());
    sched
        .awaiting_ready
        .lock()
        .unwrap()
        .insert(task_id.into(), std::time::Instant::now());
    sched.db.set_terminal(task_id, &term.handle)?;
    sched.db
        .set_state(task_id, crate::db::TaskState::Running)?;
    sched.emit(
        "task_running",
        serde_json::json!({"task_id": task_id, "to": to, "terminal_handle": term.handle}),
    );
    Ok(())
}

/// Called when `swarm_ready{workspace, terminal_handle}` arrives from a daemon
/// subscription. Matches a pending task on the same workspace path (fork+exec:
/// any clean ready terminal on that path may take the task).
///
/// Matching order matters (a live session was once killed by timeout while
/// working): prefer the task whose created terminal handle equals the reported
/// handle — dispatch records it at create time and orca exposes the same value
/// as ORCA_TERMINAL_HANDLE inside the terminal. Only fall back to path-based
/// matching for handles we never created (manual sessions, empty env).
pub fn on_ready(
    sched: &Arc<Sched>,
    workspace: &str,
    terminal_handle: &str,
) -> anyhow::Result<()> {
    let rows = sched.db.list(None, 200)?;
    let awaiting = sched.awaiting_ready.lock().unwrap();
    let terminals = sched.terminals.lock().unwrap();
    let candidate = match_candidate(&rows, &awaiting, &terminals, workspace, terminal_handle);
    drop(awaiting);
    drop(terminals);
    match candidate {
        Some(task_id) => {
            sched
                .terminals
                .lock()
                .unwrap()
                .insert(task_id.clone(), terminal_handle.into());
            sched.awaiting_ready.lock().unwrap().remove(&task_id);
            deliver_to_terminal(sched, &task_id, workspace, terminal_handle)?;
            // Re-assert the swarm title: pi overwrites the create-time title
            // on boot, so without this the tab shows a generic pi title.
            // Best effort; a failed rename must not fail the delivery.
            let title =
                format!("swarm:{}:{}", workspace, &task_id[..8.min(task_id.len())]);
            let _ = crate::orca_term::rename(terminal_handle, &title);
            Ok(())
        }
        None => {
            // No pending task: park in the transient idle pool (60s TTL reaped elsewhere).
            sched
                .idle
                .lock()
                .unwrap()
                .entry(workspace.into())
                .or_default()
                .push(IdleTerminal {
                    handle: terminal_handle.into(),
                    since: std::time::Instant::now(),
                });
            Ok(())
        }
    }
}

/// Pure matcher for on_ready: handle-first, path second. Kept free of IO
/// so the race contract is unit-tested without a database.
fn match_candidate(
    rows: &[crate::db::TaskRow],
    awaiting: &HashMap<String, std::time::Instant>,
    terminals: &HashMap<String, String>,
    workspace: &str,
    terminal_handle: &str,
) -> Option<String> {
    let eligible = |r: &crate::db::TaskRow| {
        (r.state == crate::db::TaskState::Pending
            || r.state == crate::db::TaskState::Running)
            && r.to_ws == workspace
            && (awaiting.contains_key(&r.task_id) || terminals.contains_key(&r.task_id))
    };
    if !terminal_handle.is_empty() {
        if let Some(hit) = rows.iter().find(|r| {
            eligible(r) && terminals.get(&r.task_id).map(String::as_str) == Some(terminal_handle)
        }) {
            return Some(hit.task_id.clone());
        }
    }
    rows.iter()
        .filter(|r| eligible(r))
        .map(|r| r.task_id.clone())
        .next()
}

fn deliver_to_terminal(
    sched: &Arc<Sched>,
    task_id: &str,
    to: &str,
    handle: &str,
) -> anyhow::Result<()> {
    let task = sched
        .db
        .get(task_id)?
        .ok_or_else(|| anyhow::anyhow!("unknown task {task_id}"))?;
    let tree = crate::template::load_tree(&sched.root)?;
    let role = tree
        .iter()
        .find(|e| {
            let p = if e.path.is_empty() { "." } else { &e.path };
            p == to
        })
        .map(|e| e.role.clone())
        .unwrap_or_default();
    // Full wire text from the persisted payload: header + role + payload.
    let header = crate::proto::SwarmHeader {
        task_id: task_id.into(),
        from: task.from_ws.clone(),
        transfer_send_to: task.transfer_send_to.clone(),
        attempt: task.attempt,
    };
    let wire = crate::proto::render(&header, &role, &task.payload);
    write_loopback_in(&crate::root::resolve_instance(&sched.root, to), &wire)?;
    sched.db.set_terminal(task_id, handle)?;
    sched
        .terminals
        .lock()
        .unwrap()
        .insert(task_id.into(), handle.into());
    sched.db
        .set_state(task_id, crate::db::TaskState::Running)?;
    Ok(())
}

fn write_loopback_in(ws: &std::path::Path, text: &str) -> anyhow::Result<()> {
    // `in` is a real FIFO owned by the workspace daemon: open-write-close
    // delivers one message (EOF ends the message). Never create it here.
    // Fire-and-forget per hop: no cross-hop write lock (amendment-1 removes
    // the callback path that made coalesced writes fatal).
    use std::io::Write;
    let p = crate::root::loopback_in(ws);
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .open(&p)?;
    f.write_all(text.as_bytes())?;
    f.flush()?;
    Ok(())
}

/// Called when an out message with a swarm header is observed on a workspace.
/// Fire-and-forget: mark done, append the ledger row, recycle the terminal.
/// No callback forwarding, no parent bookkeeping, no supervisor notify.
pub fn on_out(
    sched: &Arc<Sched>,
    from_ws: &str,
    msg: &crate::proto::SwarmMessage,
) -> anyhow::Result<()> {
    let task_id = &msg.header.task_id;
    let Some(task) = sched.db.get(task_id)? else {
        return Ok(()); // Unknown task: ignore.
    };
    // Idempotent redelivery: an already-terminal task never re-records.
    match task.state {
        crate::db::TaskState::Done
        | crate::db::TaskState::Failed
        | crate::db::TaskState::Cancelled
        | crate::db::TaskState::Closed => return Ok(()),
        _ => {}
    }
    sched.db.set_state(task_id, crate::db::TaskState::Done)?;
    let out_head: String = msg.payload.chars().take(200).collect();
    sched.db.set_ledger(task_id, "done", &out_head, "")?;
    crate::db::append_ledger_line(
        &sched.root,
        &crate::db::LedgerEvent {
            task_id: task_id.clone(),
            transfer_send_to: task.transfer_send_to.clone(),
            from_ws: from_ws.into(),
            to_ws: task.to_ws.clone(),
            state: "done".into(),
            out_head,
            reason: String::new(),
        },
    );
    sched.emit(
        "task_done",
        serde_json::json!({"task_id": task_id, "from": from_ws}),
    );
    close_terminal(sched, task_id)?;
    sched.db.set_state(task_id, crate::db::TaskState::Closed)?;
    sched.emit("task_closed", serde_json::json!({"task_id": task_id}));
    Ok(())
}

/// Called when a session exits without writing out (early exit = failure).
/// Records failed + ledger, recycles the terminal. No callback is written
/// anywhere (amendment-1: fire-and-forget). The task is NOT replayed.
pub fn on_early_exit(sched: &Arc<Sched>, task_id: &str, reason: &str) -> anyhow::Result<()> {
    let Some(task) = sched.db.get(task_id)? else {
        return Ok(());
    };
    if task.state == crate::db::TaskState::Done
        || task.state == crate::db::TaskState::Closed
    {
        return Ok(());
    }
    sched.db.set_state(task_id, crate::db::TaskState::Failed)?;
    sched.db.set_ledger(task_id, "failed", "", reason)?;
    crate::db::append_ledger_line(
        &sched.root,
        &crate::db::LedgerEvent {
            task_id: task_id.into(),
            transfer_send_to: task.transfer_send_to.clone(),
            from_ws: task.from_ws.clone(),
            to_ws: task.to_ws.clone(),
            state: "failed".into(),
            out_head: String::new(),
            reason: crate::proto::failed_reason(reason),
        },
    );
    sched.emit(
        "task_failed",
        serde_json::json!({"task_id": task_id, "to": task.to_ws, "reason": reason}),
    );
    close_terminal(sched, task_id)?;
    Ok(())
}

/// Cancel a task family: kill terminals, mark cancelled, append ledger rows.
/// Lineage follows transfer_send_to downstream (spawns), not upstream waits.
pub fn cancel(sched: &Arc<Sched>, task_id: &str, reason: &str) -> anyhow::Result<Vec<String>> {
    let fam = sched.db.family(task_id)?;
    let mut out = vec![];
    for t in &fam {
        if t.state == crate::db::TaskState::Closed
            || t.state == crate::db::TaskState::Cancelled
        {
            continue;
        }
        sched.db.set_state(&t.task_id, crate::db::TaskState::Cancelled)?;
        sched.db.set_ledger(&t.task_id, "cancelled", "", reason)?;
        crate::db::append_ledger_line(
            &sched.root,
            &crate::db::LedgerEvent {
                task_id: t.task_id.clone(),
                transfer_send_to: t.transfer_send_to.clone(),
                from_ws: t.from_ws.clone(),
                to_ws: t.to_ws.clone(),
                state: "cancelled".into(),
                out_head: String::new(),
                reason: crate::proto::cancelled_reason(reason),
            },
        );
        close_terminal(sched, &t.task_id)?;
        out.push(t.task_id.clone());
        sched.emit(
            "task_closed",
            serde_json::json!({"task_id": t.task_id, "reason": "cancelled"}),
        );
    }
    Ok(out)
}

fn close_terminal(sched: &Arc<Sched>, task_id: &str) -> anyhow::Result<()> {
    let handle = sched.terminals.lock().unwrap().remove(task_id);
    if let Some(h) = handle {
        if h.starts_with("stub-") {
            return Ok(());
        }
        // Best effort: kill pi, then close the orca terminal tab.
        // kill first (reclaims the pi process); close drops the tab even if
        // the process already exited, so no orphan tabs accumulate in Orca.
        // Scoped to this hop's task id: never a global pgrep sweep.
        let _ = crate::orca_term::kill_pi_for_task(&h, task_id);
        let _ = crate::orca_term::close(&h);
    }
    Ok(())
}

#[cfg(test)]
mod sched_tests {
    use super::*;
    use crate::db::{Db, TaskState};

    fn test_sched() -> Arc<Sched> {
        let dir = tempfile::tempdir().unwrap();
        // Db::open borrows root; keep dir alive via leak for test simplicity.
        let root: &'static std::path::Path =
            Box::leak(dir.path().join("root").into_boxed_path());
        std::fs::create_dir_all(root).unwrap();
        // Leak dir too so the tempdir is not deleted mid-test.
        let _ = Box::leak(Box::new(dir));
        let db = Db::open(root).unwrap();
        Sched::new(root.to_path_buf(), db)
    }

    fn hdr(task_id: &str, transfer: &str) -> crate::proto::SwarmHeader {
        crate::proto::SwarmHeader {
            task_id: task_id.into(),
            from: ".".into(),
            transfer_send_to: transfer.into(),
            attempt: 1,
        }
    }

    fn reply_msg(task_id: &str, transfer: &str, payload: &str) -> crate::proto::SwarmMessage {
        crate::proto::SwarmMessage {
            header: hdr(task_id, transfer),
            payload: payload.into(),
        }
    }

    /// Insert a task row directly (bypasses orca terminal creation).
    fn seed(sched: &Arc<Sched>, task_id: &str, to: &str, transfer: &str, state: TaskState) {
        sched
            .db
            .insert_task(task_id, ".", to, transfer, 1, "payload")
            .unwrap();
        sched.db.set_state(task_id, state).unwrap();
    }

    #[test]
    fn out_without_parent_marks_done_and_closes() {
        let s = test_sched();
        seed(&s, "t1", "a", "", TaskState::Running);
        on_out(&s, "a", &reply_msg("t1", "", "done")).unwrap();
        let t = s.db.get("t1").unwrap().unwrap();
        assert_eq!(t.state, TaskState::Closed);
        let tail = s.db.ledger(10).unwrap();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].state, "done");
    }

    #[test]
    fn out_is_fire_and_forget_no_parent_write() {
        let s = test_sched();
        seed(&s, "parent", "a", "", TaskState::Running);
        seed(&s, "child", "b", "parent", TaskState::Running);
        on_out(&s, "b", &reply_msg("child", "parent", "child done")).unwrap();
        let child = s.db.get("child").unwrap().unwrap();
        assert_eq!(child.state, TaskState::Closed);
        // Parent untouched: no callback, no counter, no close.
        let parent = s.db.get("parent").unwrap().unwrap();
        assert_eq!(parent.state, TaskState::Running);
    }

    #[test]
    fn duplicate_out_is_idempotent() {
        let s = test_sched();
        seed(&s, "c", "b", "", TaskState::Running);
        let msg = reply_msg("c", "", "x");
        on_out(&s, "b", &msg).unwrap();
        on_out(&s, "b", &msg).unwrap();
        let tail = s.db.ledger(10).unwrap();
        assert_eq!(tail.len(), 1);
    }

    #[test]
    fn unknown_out_is_ignored() {
        let s = test_sched();
        on_out(&s, "a", &reply_msg("ghost", "", "x")).unwrap();
    }

    #[test]
    fn early_exit_records_failed_without_callback() {
        let s = test_sched();
        seed(&s, "c", "b", "p", TaskState::Running);
        seed(&s, "p", "a", "", TaskState::Running);
        on_early_exit(&s, "c", "boom").unwrap();
        assert_eq!(s.db.get("c").unwrap().unwrap().state, TaskState::Failed);
        assert_eq!(s.db.get("p").unwrap().unwrap().state, TaskState::Running);
        let tail = s.db.ledger(10).unwrap();
        assert!(tail.iter().any(|e| e.task_id == "c" && e.state == "failed"));
    }

    #[test]
    fn ready_matches_own_terminal_first() {
        use crate::db::TaskState;
        let s = test_sched();
        seed(&s, "own", "a", "", TaskState::Running);
        seed(&s, "other", "a", "", TaskState::Running);
        s.terminals.lock().unwrap().insert("own".into(), "term-OWN".into());
        s.terminals.lock().unwrap().insert("other".into(), "term-OTHER".into());
        s.awaiting_ready.lock().unwrap().insert("own".into(), std::time::Instant::now());
        s.awaiting_ready.lock().unwrap().insert("other".into(), std::time::Instant::now());
        // The rows list newest-first; path-only matching would pick "other".
        // Handle-first matching must pick the task whose terminal we created.
        let rows = s.db.list(None, 200).unwrap();
        let awaiting = s.awaiting_ready.lock().unwrap();
        let terminals = s.terminals.lock().unwrap();
        let hit = match_candidate(&rows, &awaiting, &terminals, "a", "term-OWN");
        assert_eq!(hit.as_deref(), Some("own"));
        // Unknown handle falls back to path matching without panic.
        let hit = match_candidate(&rows, &awaiting, &terminals, "a", "term-STRANGER");
        assert!(hit == Some("own".into()) || hit == Some("other".into()));
        // Wrong workspace matches nothing.
        assert_eq!(match_candidate(&rows, &awaiting, &terminals, "b", "term-OWN"), None);
    }

    #[test]
    fn tree_groups_hops_under_their_workspace_and_marks_focusable_tab() {
        let workspaces = vec![
            serde_json::json!({"path": "."}),
            serde_json::json!({"path": "model"}),
        ];
        let tasks = vec![
            serde_json::json!({"task_id": "root-task-abcdefgh", "to_ws": ".", "state": "running", "terminal": "term_root"}),
            serde_json::json!({"task_id": "model-task-abcdefgh", "to_ws": "model", "state": "running", "terminal": "term_model"}),
            serde_json::json!({"task_id": "old-model-abcdefgh", "to_ws": "model", "state": "closed", "terminal": ""}),
        ];
        let lines = crate::tui::tree_tab_lines(&workspaces, &tasks, 1);
        assert_eq!(lines.len(), 3);
        assert!(lines[0].contains("root-tas") && lines[0].contains("◉"));
        assert!(lines[1].starts_with("▶ model-ta") && lines[1].contains("◉"));
        assert!(lines[2].starts_with("· old-mode") && !lines[2].contains("◉"));
    }

    #[test]
    fn render_snapshot_shapes() {
        let tasks = vec![
            serde_json::json!({"task_id": "abcdefgh-1234", "from_ws": ".", "to_ws": "a",
                               "attempt": 1, "state": "running", "transfer_send_to": ""}),
            serde_json::json!({"task_id": "x", "from_ws": "a", "to_ws": "b",
                               "attempt": 3, "state": "failed", "transfer_send_to": "p"}),
        ];
        let snap = crate::tui::snapshot_for_test(
            serde_json::Value::Null,
            vec![],
            tasks.clone(),
        );
        assert_eq!(snap.task_count_for_test(), 2);
        let rows = crate::tui::task_rows_for_test(&snap.tasks_for_test(), 0);
        assert_eq!(rows.len(), 2);
        // Failed row keeps its state string for the red style branch.
        assert!(rows[1].contains("failed"));
    }
}
