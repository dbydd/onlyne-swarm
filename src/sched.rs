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
    use std::io::Write;
    let p = crate::root::loopback_in(ws);
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .open(&p)?;
    f.write_all(text.as_bytes())?;
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
            write_loopback_in(&parent_ws, &crate::proto::render(&fwd, "", &msg.payload))?;
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
        // Best effort: kill pi; orca reclaims the terminal.
        let _ = crate::orca_term::kill_pi(&h);
    }
    Ok(())
}
