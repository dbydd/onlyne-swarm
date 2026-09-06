use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::broadcast;

/// Process-wide serializer for loopback/in FIFO writes. Concurrent O_WRONLY
/// opens to the same FIFO can merge at the daemon's read boundary and lose a
/// message; the scheduler must never lose a callback, so all its FIFO writes
/// go through this lock. (Agents themselves may still write concurrently;
/// that risk is explicitly ignored per SPEC.)
static FIFO_WRITE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
fn fifo_lock() -> &'static Mutex<()> {
    FIFO_WRITE_LOCK.get_or_init(|| Mutex::new(()))
}

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
pub fn on_ready(
    sched: &Arc<Sched>,
    workspace: &str,
    terminal_handle: &str,
) -> anyhow::Result<()> {
    // Find oldest pending/awaiting task for this workspace path.
    let candidate: Option<String> = {
        let awaiting = sched.awaiting_ready.lock().unwrap();
        let rows = sched.db.list(None, 200)?;
        rows.into_iter()
            .filter(|r| {
                (r.state == crate::db::TaskState::Pending
                    || r.state == crate::db::TaskState::Running)
                    && r.to_ws == workspace
                    && (awaiting.contains_key(&r.task_id)
                        || sched.terminals.lock().unwrap().contains_key(&r.task_id))
            })
            .map(|r| r.task_id)
            .next()
    };
    match candidate {
        Some(task_id) => {
            sched
                .terminals
                .lock()
                .unwrap()
                .insert(task_id.clone(), terminal_handle.into());
            sched.awaiting_ready.lock().unwrap().remove(&task_id);
            deliver_to_terminal(sched, &task_id, workspace, terminal_handle)?;
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
        reply_to: task.reply_to.clone(),
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
    // Serialized process-wide: see FIFO_WRITE_LOCK.
    let _guard = fifo_lock().lock().unwrap();
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
pub fn on_reply(
    sched: &Arc<Sched>,
    from_ws: &str,
    msg: &crate::proto::SwarmMessage,
) -> anyhow::Result<()> {
    let task_id = &msg.header.task_id;
    let Some(task) = sched.db.get(task_id)? else {
        return Ok(()); // Unknown task: ignore.
    };
    // Idempotent redelivery: an already-terminal task (Replied/Failed/
    // Cancelled/Closed) never re-forwards nor double-decrements the parent.
    match task.state {
        crate::db::TaskState::Replied
        | crate::db::TaskState::Failed
        | crate::db::TaskState::Cancelled
        | crate::db::TaskState::Closed => return Ok(()),
        _ => {}
    }
    sched.db
        .set_state(task_id, crate::db::TaskState::Replied)?;
    sched.emit(
        "task_replied",
        serde_json::json!({"task_id": task_id, "from": from_ws, "reply_to": task.reply_to}),
    );
    // Forward the callback to the parent (if any): write into the parent's
    // workspace in as a new message carrying the same family headers.
    if !task.reply_to.is_empty() {
        if let Some(parent) = sched.db.get(&task.reply_to)? {
            let fwd = crate::proto::SwarmHeader {
                task_id: task.task_id.clone(),
                from: from_ws.into(),
                reply_to: parent.task_id.clone(),
                attempt: task.attempt,
            };
            let parent_ws = crate::root::resolve_instance(&sched.root, &parent.to_ws);
            let wire = crate::proto::render(&fwd, "", &msg.payload);
            write_loopback_in(&parent_ws, &wire)?;
            sched.emit(
                "callback_forwarded",
                serde_json::json!({"task_id": task_id, "to_parent": parent.task_id}),
            );
            if let Some(updated) = sched.db.dec_parent(&parent.task_id)? {
                if updated.pending_replies <= 0 {
                    maybe_close(sched, &parent.task_id)?;
                }
            }
        } else {
            sched.db.dead_letter(task_id, "parent task missing")?;
        }
    } else if !task.from_ws.is_empty() && task.from_ws != "." {
        // Top-level task from a supervisor workspace: notify the `from` workspace.
        let sup_ws = crate::root::resolve_instance(&sched.root, &task.from_ws);
        if sup_ws.is_dir() {
            let fwd = crate::proto::SwarmHeader {
                task_id: task.task_id.clone(),
                from: from_ws.into(),
                reply_to: String::new(),
                attempt: task.attempt,
            };
            write_loopback_in(&sup_ws, &crate::proto::render(&fwd, "", &msg.payload))?;
            sched.emit(
                "callback_forwarded",
                serde_json::json!({"task_id": task_id, "to_parent": task.from_ws}),
            );
        }
    }
    maybe_close(sched, task_id)?;
    Ok(())
}

/// Called when a session exits without writing out (early exit = failure).
/// Writes the failure callback to the initiator and recycles the terminal.
/// The task itself is NOT replayed; retry belongs to external pi plugins.
pub fn on_early_exit(sched: &Arc<Sched>, task_id: &str, reason: &str) -> anyhow::Result<()> {
    let Some(task) = sched.db.get(task_id)? else {
        return Ok(());
    };
    if task.state == crate::db::TaskState::Replied
        || task.state == crate::db::TaskState::Closed
    {
        return Ok(());
    }
    sched.db
        .set_state(task_id, crate::db::TaskState::Failed)?;
    sched.emit(
        "task_failed",
        serde_json::json!({"task_id": task_id, "to": task.to_ws, "reason": reason}),
    );
    let body = crate::proto::failed_payload(reason, "");
    if !task.reply_to.is_empty() {
        if let Some(parent) = sched.db.get(&task.reply_to)? {
            let fwd = crate::proto::SwarmHeader {
                task_id: task_id.into(),
                from: task.to_ws.clone(),
                reply_to: parent.task_id.clone(),
                attempt: task.attempt,
            };
            let parent_ws = crate::root::resolve_instance(&sched.root, &parent.to_ws);
            if parent_ws.is_dir() {
                write_loopback_in(&parent_ws, &crate::proto::render(&fwd, "", &body))?;
                sched.emit(
                    "callback_forwarded",
                    serde_json::json!({"task_id": task_id, "to_parent": parent.task_id}),
                );
                if let Some(updated) = sched.db.dec_parent(&parent.task_id)? {
                    if updated.pending_replies <= 0 {
                        maybe_close(sched, &parent.task_id)?;
                    }
                }
            } else {
                sched.db.dead_letter(task_id, "parent workspace missing")?;
            }
        } else {
            sched.db.dead_letter(task_id, "parent task missing")?;
        }
    }
    close_terminal(sched, task_id)?;
    Ok(())
}

/// Cancel a task family: kill terminals, mark cancelled, notify initiator.
pub fn cancel(sched: &Arc<Sched>, task_id: &str, reason: &str) -> anyhow::Result<Vec<String>> {
    let fam = sched.db.family(task_id)?;
    let mut out = vec![];
    for t in &fam {
        if t.state == crate::db::TaskState::Closed
            || t.state == crate::db::TaskState::Cancelled
        {
            continue;
        }
        sched
            .db
            .set_state(&t.task_id, crate::db::TaskState::Cancelled)?;
        close_terminal(sched, &t.task_id)?;
        out.push(t.task_id.clone());
        sched.emit(
            "task_closed",
            serde_json::json!({"task_id": t.task_id, "reason": "cancelled"}),
        );
    }
    if let Some(root_task) = sched.db.get(task_id)? {
        if !root_task.reply_to.is_empty() {
            if let Some(parent) = sched.db.get(&root_task.reply_to)? {
                let body = crate::proto::cancelled_payload(reason, "");
                let fwd = crate::proto::SwarmHeader {
                    task_id: task_id.into(),
                    from: root_task.to_ws.clone(),
                    reply_to: parent.task_id.clone(),
                    attempt: root_task.attempt,
                };
                let parent_ws = crate::root::resolve_instance(&sched.root, &parent.to_ws);
                if parent_ws.is_dir() {
                    write_loopback_in(&parent_ws, &crate::proto::render(&fwd, "", &body))?;
                }
                sched.db.dec_parent(&parent.task_id)?;
            }
        }
    }
    Ok(out)
}

/// Close when: own out written (replied) AND no pending child replies.
pub fn maybe_close(sched: &Arc<Sched>, task_id: &str) -> anyhow::Result<()> {
    let Some(t) = sched.db.get(task_id)? else {
        return Ok(());
    };
    if t.state == crate::db::TaskState::Replied && t.pending_replies <= 0 {
        close_terminal(sched, task_id)?;
        sched.db
            .set_state(task_id, crate::db::TaskState::Closed)?;
        sched.emit("task_closed", serde_json::json!({"task_id": task_id}));
    }
    Ok(())
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
        let _ = crate::orca_term::kill_pi(&h);
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

    fn hdr(task_id: &str, reply_to: &str) -> crate::proto::SwarmHeader {
        crate::proto::SwarmHeader {
            task_id: task_id.into(),
            from: ".".into(),
            reply_to: reply_to.into(),
            attempt: 1,
        }
    }

    fn reply_msg(task_id: &str, reply_to: &str, payload: &str) -> crate::proto::SwarmMessage {
        crate::proto::SwarmMessage {
            header: hdr(task_id, reply_to),
            payload: payload.into(),
        }
    }

    /// Insert a task row directly (bypasses orca terminal creation).
    fn seed(sched: &Arc<Sched>, task_id: &str, to: &str, reply_to: &str, state: TaskState) {
        sched
            .db
            .insert_task(task_id, ".", to, reply_to, 1, "payload")
            .unwrap();
        sched.db.set_state(task_id, state).unwrap();
    }

    #[test]
    fn reply_without_parent_closes_task() {
        let s = test_sched();
        seed(&s, "t1", "a", "", TaskState::Running);
        on_reply(&s, "a", &reply_msg("t1", "", "done")).unwrap();
        let t = s.db.get("t1").unwrap().unwrap();
        assert_eq!(t.state, TaskState::Closed);
    }

    #[test]
    fn reply_forwards_callback_and_decrements_parent() {
        let s = test_sched();
        // Parent workspace dir must exist for the FIFO write; create a fake
        // instance dir with a real fifo? write_loopback_in opens the fifo, so
        // instead point parent at "." (root) -- still needs a fifo. Use a
        // real FIFO via mkfifo through std::process? Simpler: create the
        // parent in workspace "a" and pre-create root loopback fifo.
        seed(&s, "parent", "a", "", TaskState::Running);
        s.db.bump_parent("parent", 1).unwrap();
        seed(&s, "child", "b", "parent", TaskState::Running);
        // Pre-create the parent workspace loopback fifo so the forward lands.
        let ws_a = crate::root::resolve_instance(&s.root, "a");
        std::fs::create_dir_all(ws_a.join(".onlyne/channels/loopback")).unwrap();
        let fifo = crate::root::loopback_in(&ws_a);
        // SAFETY: test-only; create fifo via libc mknod through std.
        #[cfg(unix)]
        {
            let _ = std::fs::remove_file(&fifo);
            // Use mkfifo(1); guaranteed on macOS/dev machines.
            let st = std::process::Command::new("mkfifo").arg(&fifo).status().unwrap();
            assert!(st.success());
            // Hold a reader so the scheduler's O_WRONLY open never blocks.
            let fifo2 = fifo.clone();
            std::thread::spawn(move || {
                use std::io::Read;
                loop {
                    if let Ok(mut f) = std::fs::File::open(&fifo2) {
                        let mut buf = Vec::new();
                        let _ = f.read_to_end(&mut buf);
                    } else {
                        break;
                    }
                }
            });
        }
        on_reply(&s, "b", &reply_msg("child", "parent", "child done")).unwrap();
        let child = s.db.get("child").unwrap().unwrap();
        assert_eq!(child.state, TaskState::Closed);
        let parent = s.db.get("parent").unwrap().unwrap();
        assert_eq!(parent.pending_replies, 0);
    }

    #[test]
    fn duplicate_reply_is_idempotent() {
        let s = test_sched();
        seed(&s, "p", "a", "", TaskState::Running);
        s.db.bump_parent("p", 1).unwrap();
        seed(&s, "c", "b", "p", TaskState::Running);
        let ws_a = crate::root::resolve_instance(&s.root, "a");
        std::fs::create_dir_all(ws_a.join(".onlyne/channels/loopback")).unwrap();
        // No fifo: forward would fail. Instead use parent == child ws trick?
        // Simpler: point parent workspace at a dir WITH a drained fifo.
        let fifo = crate::root::loopback_in(&ws_a);
        let _ = std::fs::remove_file(&fifo);
        let st = std::process::Command::new("mkfifo").arg(&fifo).status().unwrap();
        assert!(st.success());
        let fifo2 = fifo.clone();
        std::thread::spawn(move || {
            use std::io::Read;
            loop {
                if let Ok(mut f) = std::fs::File::open(&fifo2) {
                    let mut buf = Vec::new();
                    let _ = f.read_to_end(&mut buf);
                } else {
                    break;
                }
            }
        });
        let msg = reply_msg("c", "p", "x");
        on_reply(&s, "b", &msg).unwrap();
        // Second identical delivery must be a no-op (no double decrement).
        on_reply(&s, "b", &msg).unwrap();
        let parent = s.db.get("p").unwrap().unwrap();
        assert_eq!(parent.pending_replies, 0);
    }

    #[test]
    fn unknown_reply_is_ignored() {
        let s = test_sched();
        on_reply(&s, "a", &reply_msg("ghost", "", "x")).unwrap();
    }

    #[test]
    fn render_snapshot_shapes() {
        let tasks = vec![
            serde_json::json!({"task_id": "abcdefgh-1234", "from_ws": ".", "to_ws": "a",
                               "attempt": 1, "state": "running", "pending_replies": 2}),
            serde_json::json!({"task_id": "x", "from_ws": "a", "to_ws": "b",
                               "attempt": 3, "state": "failed", "pending_replies": 0}),
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
