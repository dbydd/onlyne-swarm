use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicBool;
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
    /// Set on shutdown so pump/reaper threads stop reconnecting and exit.
    pub shutdown: Arc<AtomicBool>,
    /// task_id -> terminal handle for running tasks.
    pub terminals: Mutex<HashMap<String, String>>,
    /// workspace path -> idle ready terminal handles (transient pool).
    pub idle: Mutex<HashMap<String, Vec<IdleTerminal>>>,
    /// task_id -> successful FIFO delivery time. Dead-terminal detection waits
    /// a short grace window so an out event racing with process exit wins.
    pub running_since: Mutex<HashMap<String, std::time::Instant>>,
    /// R4: task_id -> (hop_state, entered_at). Memory-only dwell clock for
    /// TUI display; cleared on restart (pre-restart dwell is unknowable).
    pub hop_since: Mutex<HashMap<String, (String, std::time::Instant)>>,
    /// R4: task_id -> last hop_overlong emit. Drives the CR6 repeat policy.
    pub overlong_last: Mutex<HashMap<String, std::time::Instant>>,
    /// R4 (CR3): tasks idle while the hop still awaits `swarm_complete`.
    /// The plugin owns the exit reminder and the auto-quit for these, so
    /// the scheduler records + displays them and never fails them by TTL.
    /// Idles without this flag are stale reports and do hit the TTL.
    pub idle_pending_exit: Mutex<std::collections::HashSet<String>>,
    /// R4: recent lifecycle alerts exposed through status. These are kept
    /// small (5 rows) and use stable messages readable by the TUI alerts
    /// pane.
    pub alerts: Mutex<std::collections::VecDeque<String>>,
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
            shutdown: Arc::new(AtomicBool::new(false)),
            terminals: Mutex::new(HashMap::new()),
            idle: Mutex::new(HashMap::new()),
            running_since: Mutex::new(HashMap::new()),
            hop_since: Mutex::new(HashMap::new()),
            overlong_last: Mutex::new(HashMap::new()),
            idle_pending_exit: Mutex::new(std::collections::HashSet::new()),
            alerts: Mutex::new(std::collections::VecDeque::new()),
        })
    }

    pub fn note_alert(&self, text: String) {
        let mut alerts = self.alerts.lock().unwrap();
        alerts.retain(|line| line != &text);
        if alerts.len() >= 5 {
            alerts.pop_front();
        }
        alerts.push_back(text);
    }

    /// R4: lifecycle alerts (adopted / dropped-out / overlong) surfaced
    /// through `status` so the TUI and巡检 read one source.
    pub fn recent_alerts(&self) -> Vec<String> {
        self.alerts.lock().unwrap().iter().cloned().collect()
    }

    pub fn emit(&self, typ: &str, data: serde_json::Value) {
        let _ = self.bus.sender().send(SchedEvent {
            typ: typ.into(),
            data,
        });
    }

    /// Root retry budget for the dead-terminal sweep, read from the root
    /// `.onlyne/swarm.workspace.jsonc` (`"retry": {"max_attempts": N}`).
    /// `Some(0)` means unbounded full-task replay; `None` keeps the default
    /// compiled cap. Malformed config falls back to `None` by design.
    pub fn root_retry_max_attempts(&self) -> Option<u32> {
        let raw = std::fs::read_to_string(crate::root::swarm_ws_config(&self.root)).ok()?;
        let v: serde_json::Value = crate::template::parse_lenient(&raw).ok()?;
        v.get("retry")
            .and_then(|r| r.get("max_attempts"))
            .and_then(|m| m.as_u64())
            .map(|m| m.min(u32::MAX as u64) as u32)
    }

    /// R4 timeouts from root `.onlyne/swarm.workspace.jsonc`:
    /// `"timeouts": {"dispatched_secs": N, "busy_secs": {"<role>": N},
    /// "idle_secs": N}`. Defaults: dispatched 120, busy unlimited per
    /// role, idle 60. `ready` (delivery write) is a fixed 30s, not
    /// configurable (a stuck FIFO write means the daemon is dead).
    pub fn timeout_secs(&self, key: &str) -> u64 {
        let raw = std::fs::read_to_string(crate::root::swarm_ws_config(&self.root))
            .unwrap_or_default();
        let v: serde_json::Value = crate::template::parse_lenient(&raw).unwrap_or_default();
        v.get("timeouts")
            .and_then(|t| t.get(key))
            .and_then(|n| n.as_u64())
            .unwrap_or(match key {
                "dispatched_secs" => 120,
                "idle_secs" => 60,
                _ => 120,
            })
    }

    /// R4 per-role busy (long-run) cap in seconds. None = unlimited.
    pub fn busy_limit_secs(&self, role: &str) -> Option<u64> {
        let raw = std::fs::read_to_string(crate::root::swarm_ws_config(&self.root))
            .unwrap_or_default();
        let v: serde_json::Value = crate::template::parse_lenient(&raw).unwrap_or_default();
        v.get("timeouts")
            .and_then(|t| t.get("busy_secs"))
            .and_then(|b| b.get(role))
            .and_then(|n| n.as_u64())
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

/// R4 hop transition helper: persist hop_state + in-memory since + emit.
/// `hop_since` tracks (state, entered_at) for TUI dwell display.
fn set_hop(
    sched: &Arc<Sched>,
    task_id: &str,
    from: &str,
    to: &str,
) -> anyhow::Result<()> {
    sched.db.set_hop(task_id, to)?;
    sched
        .hop_since
        .lock()
        .unwrap()
        .insert(task_id.into(), (to.into(), std::time::Instant::now()));
    sched.emit(
        "hop_state",
        serde_json::json!({"task_id": task_id, "from_state": from, "to_state": to}),
    );
    Ok(())
}

fn current_hop(sched: &Arc<Sched>, task_id: &str) -> String {
    sched
        .db
        .get(task_id)
        .ok()
        .flatten()
        .map(|r| r.hop_state)
        .unwrap_or_default()
}

fn dispatch(sched: &Arc<Sched>, task_id: &str, to: &str) -> anyhow::Result<()> {
    // 1. Try idle pool (same workspace path affinity). A reused pane is
    // already ready with payload written inline: straight to busy (R4 §3).
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
    sched.db.set_terminal(task_id, &term.handle)?;
    sched.db
        .set_state(task_id, crate::db::TaskState::Running)?;
    let _ = set_hop(sched, task_id, "", "dispatched");
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
    let terminals = sched.terminals.lock().unwrap();
    let candidate = match_candidate(&rows, &terminals, workspace, terminal_handle);
    drop(terminals);
    match candidate {
        Some(task_id) => {
            sched
                .terminals
                .lock()
                .unwrap()
                .insert(task_id.clone(), terminal_handle.into());
            // R4: handshake matched => ready; deliver_to_terminal moves to
            // busy on successful write. A failed write leaves ready so the
            // 30s delivery guard (not the 120s handshake guard) owns it.
            let from = current_hop(sched, &task_id);
            let _ = set_hop(sched, &task_id, &from, "ready");
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

/// R4: hop busy/idle reports from the session (pi-onlyne agent hooks).
/// Body: `{workspace, terminal_handle, task_id[, pending_exit]}`.
/// Match key is task_id (handles may change across adoption).
pub fn on_hop_activity(
    sched: &Arc<Sched>,
    task_id: &str,
    busy: bool,
    pending_exit: bool,
) -> anyhow::Result<()> {
    let Some(task) = sched.db.get(task_id)? else {
        return Ok(()); // Unknown task: race residue, silent drop.
    };
    match task.state {
        crate::db::TaskState::Done
        | crate::db::TaskState::Failed
        | crate::db::TaskState::Cancelled
        | crate::db::TaskState::Closed => {
            sched.note_alert(format!(
                "dropped-out: {} {}", &task_id[..8.min(task_id.len())], task.state.as_str()));
            sched.emit(
                "out_for_terminal_task_dropped",
                serde_json::json!({"task_id": task_id, "state": task.state.as_str()}),
            );
            return Ok(());
        }
        _ => {}
    }
    let cur = task.hop_state.clone();
    if busy {
        // CR2: ''/dispatched/ready/idle -> busy are all normal (the plugin
        // is faster than the scheduler on reuse + adoption paths). Only
        // busy->busy is a no-op.
        if cur == "busy" {
            return Ok(());
        }
        set_hop(sched, task_id, &cur, "busy")?;
        sched.idle_pending_exit.lock().unwrap().remove(task_id);
        // A busy report proves liveness: refresh the delivery clock so the
        // dead-terminal sweep cannot reap a working session.
        sched.running_since.lock().unwrap().insert(task_id.into(), std::time::Instant::now());
    } else {
        if cur == "idle" {
            return Ok(()); // duplicate idle: display heartbeat only, clock untouched
        }
        let _ = pending_exit; // reminder actor stays plugin-local (CR3); scheduler only records
        if pending_exit {
            sched.idle_pending_exit.lock().unwrap().insert(task_id.into());
        } else {
            sched.idle_pending_exit.lock().unwrap().remove(task_id);
        }
        set_hop(sched, task_id, &cur, "idle")?;
    }
    Ok(())
}

/// Pure matcher for on_ready: handle-first, path second. Kept free of IO
/// so the race contract is unit-tested without a database.
fn match_candidate(
    rows: &[crate::db::TaskRow],
    terminals: &HashMap<String, String>,
    workspace: &str,
    terminal_handle: &str,
) -> Option<String> {
    // R4: only handshake-waiting rows can accept `swarm_ready`. A live
    // busy row retains its terminal handle for adoption, yet must never
    // steal a later ready event for another task.
    let waiting = |r: &crate::db::TaskRow| {
        matches!(r.hop_state.as_str(), "" | "dispatched" | "ready")
    };
    let eligible = |r: &crate::db::TaskRow| {
        (r.state == crate::db::TaskState::Pending
            || r.state == crate::db::TaskState::Running)
            && r.to_ws == workspace
            && waiting(r)
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
    sched.running_since.lock().unwrap().insert(task_id.into(), std::time::Instant::now());
    sched.db.set_terminal(task_id, handle)?;
    sched
        .terminals
        .lock()
        .unwrap()
        .insert(task_id.into(), handle.into());
    sched.db
        .set_state(task_id, crate::db::TaskState::Running)?;
    // R4: payload written => busy (covers fresh dispatch via on_ready and
    // idle-pool reuse straight from dispatch).
    let from = current_hop(sched, task_id);
    let _ = set_hop(sched, task_id, &from, "busy");
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

/// A handoff that starts with the canonical failure marker carries useful
/// evidence for downstream roles, yet is an explicitly non-success terminal
/// outcome. Role contracts use this exact first non-empty line when a task
/// premise is stale or a required proof/evaluation failed.
fn failed_handoff(payload: &str) -> bool {
    payload
        .lines()
        .find(|line| !line.trim().is_empty())
        .map(|line| line.trim_start().starts_with("> hop-failed:"))
        .unwrap_or(false)
}

/// Called when an out message with a swarm header is observed on a workspace.
/// Fire-and-forget: preserve the handoff evidence, record done or failed, then
/// recycle the terminal. No callback forwarding, no parent bookkeeping, no
/// supervisor notify.
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
    // R5.3: emit instead of silently dropping — a dropped out means a live
    // session's handoff evaporated (e.g. restart reconcile raced the out).
    // The emit makes the loss visible in TUI/subscribe streams; without it
    // the接力 stalls at the hop boundary with no trace.
    match task.state {
        crate::db::TaskState::Done
        | crate::db::TaskState::Failed
        | crate::db::TaskState::Cancelled
        | crate::db::TaskState::Closed => {
            sched.note_alert(format!(
                "dropped-out: {} {}", &task_id[..8.min(task_id.len())], task.state.as_str()));
            sched.emit(
                "out_for_terminal_task_dropped",
                serde_json::json!({"task_id": task_id, "from": from_ws, "state": task.state.as_str()}),
            );
            tracing::warn!(task = %task_id, from = %from_ws, state = %task.state.as_str(), "out_for_terminal_task_dropped");
            return Ok(());
        }
        _ => {}
    }
    let out_head: String = msg.payload.chars().take(200).collect();
    let failed = failed_handoff(&msg.payload);
    let terminal_state = if failed {
        crate::db::TaskState::Failed
    } else {
        crate::db::TaskState::Done
    };
    let ledger_state = if failed { "failed" } else { "done" };
    let reason = if failed {
        "swarm-failed: handoff declares hop-failed"
    } else {
        ""
    };
    sched.db.set_state(task_id, terminal_state)?;
    sched.db.set_ledger(task_id, ledger_state, &out_head, reason)?;
    crate::db::append_ledger_line(
        &sched.root,
        &crate::db::LedgerEvent {
            task_id: task_id.clone(),
            transfer_send_to: task.transfer_send_to.clone(),
            from_ws: from_ws.into(),
            to_ws: task.to_ws.clone(),
            state: ledger_state.into(),
            out_head,
            reason: reason.into(),
        },
    );
    if failed {
        sched.emit(
            "task_failed",
            serde_json::json!({"task_id": task_id, "from": from_ws, "reason": reason}),
        );
    } else {
        sched.emit(
            "task_done",
            serde_json::json!({"task_id": task_id, "from": from_ws}),
        );
    }
    close_terminal(sched, task_id)?;
    if !failed {
        sched.db.set_state(task_id, crate::db::TaskState::Closed)?;
        sched.emit("task_closed", serde_json::json!({"task_id": task_id}));
    }
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

/// Cancel a task family: mark cancelled, append ledger rows, recycle tabs.
/// Lineage follows transfer_send_to downstream (spawns), not upstream waits.
/// Sessions die by their own hand (reclaim protocol); see close_terminal.
/// `force` adds the operator-only scoped pkill before tab close.
#[allow(dead_code)]
pub fn cancel(sched: &Arc<Sched>, task_id: &str, reason: &str) -> anyhow::Result<Vec<String>> {
    cancel_force(sched, task_id, reason, false)
}

/// Cancel with the manual escape hatch: `force` runs the task-scoped pkill
/// inside each tab before closing it. Off the default path.
pub fn cancel_force(sched: &Arc<Sched>, task_id: &str, reason: &str, force: bool) -> anyhow::Result<Vec<String>> {
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
        if force {
            force_kill(sched, &t.task_id);
        }
        close_terminal(sched, &t.task_id)?;
        out.push(t.task_id.clone());
        sched.emit(
            "task_closed",
            serde_json::json!({"task_id": t.task_id, "reason": "cancelled"}),
        );
    }
    Ok(out)
}

/// Called when the session uplinks `swarm_recycled`. A worker quit closes its
/// ledger life immediately. Roots with `retry.max_attempts = 0` replay that
/// whole task as attempt+1; every other root preserves explicit-quit stop.
/// A normal done task was already closed by on_out, so it is a no-op.
pub fn on_recycled(sched: &Arc<Sched>, task_id: &str, reason: &str) -> anyhow::Result<()> {
    let Some(task) = sched.db.get(task_id)? else {
        return Ok(());
    };
    if task.state == crate::db::TaskState::Done
        || task.state == crate::db::TaskState::Closed
        || task.state == crate::db::TaskState::Cancelled
        || task.state == crate::db::TaskState::Failed
    {
        close_tab_only(sched, task_id)?;
        return Ok(());
    }
    sched.db.set_state(task_id, crate::db::TaskState::Failed)?;
    let reason = format!("swarm-recycled: {reason}");
    sched.db.set_ledger(task_id, "failed", "", &reason)?;
    crate::db::append_ledger_line(
        &sched.root,
        &crate::db::LedgerEvent {
            task_id: task_id.into(),
            transfer_send_to: task.transfer_send_to.clone(),
            from_ws: task.from_ws.clone(),
            to_ws: task.to_ws.clone(),
            state: "failed".into(),
            out_head: String::new(),
            reason: crate::proto::failed_reason(&reason),
        },
    );
    sched.emit(
        "task_failed",
        serde_json::json!({"task_id": task_id, "to": task.to_ws, "reason": reason}),
    );
    close_tab_only(sched, task_id)?;
    if sched.root_retry_max_attempts() == Some(0) {
        let retry_id = uuid::Uuid::new_v4().to_string();
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
            let _ = dispatch_public(sched, &retry_id, &task.to_ws);
        }
    }
    Ok(())
}

/// Remove the tracked tab and close it. The session has already accepted a
/// recycle/quit, so this must not send another downlink control message.
fn close_tab_only(sched: &Arc<Sched>, task_id: &str) -> anyhow::Result<()> {
    sched.running_since.lock().unwrap().remove(task_id);
    sched.hop_since.lock().unwrap().remove(task_id);
    let _ = sched.db.set_hop(task_id, "");
    let handle = sched.terminals.lock().unwrap().remove(task_id);
    if let Some(h) = handle {
        if !h.starts_with("stub-") {
            let _ = crate::orca_term::close(&h);
        }
    }
    Ok(())
}

/// Signal the session to recycle itself, then close the tab.
/// Ownership (amendment 3): the session dies by its own hand — the scheduler
/// sends the downlink signal, waits briefly for the `swarm_recycled` ack,
/// then closes the Orca tab regardless. No shell injection on this path.
fn signal_recycle(sched: &Arc<Sched>, task_id: &str, reason: &str) {
    let task = sched.db.get(task_id).ok().flatten();
    let to = task.map(|t| t.to_ws).unwrap_or_default();
    let to = if to.is_empty() { ".".into() } else { to };
    let ws = crate::root::resolve_instance(&sched.root, &to);
    let _ = write_loopback_in(&ws, &crate::proto::render_ctl(task_id, reason));
}

/// Wait up to `timeout` for the session's `swarm_recycled` ack, polling the
/// daemon history for the ack line. Returns true on ack.
fn wait_recycled_ack(sched: &Arc<Sched>, task_id: &str, timeout: std::time::Duration) -> bool {
    let task = sched.db.get(task_id).ok().flatten();
    let to = task.map(|t| t.to_ws).unwrap_or_default();
    let to = if to.is_empty() { ".".into() } else { to };
    let ws = crate::root::resolve_instance(&sched.root, &to);
    let sock = crate::root::onlyne_sock(&ws);
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if let Ok(hist) = daemon_history(&sock, 10) {
            for line in hist {
                if line.contains("swarm_recycled") && line.contains(task_id) {
                    return true;
                }
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
    false
}

/// One-shot fetch of recent loopback history lines from a workspace daemon.
fn daemon_history(sock: &std::path::Path, limit: usize) -> anyhow::Result<Vec<String>> {
    use std::io::{BufRead, BufReader, Write};
    let mut s = std::os::unix::net::UnixStream::connect(sock)?;
    let req = serde_json::json!({
        "id": "reclaim-probe",
        "op": "fetch_channel_history",
        "channel_id": "loopback",
        "limit": limit,
    });
    writeln!(s, "{req}")?;
    s.set_read_timeout(Some(std::time::Duration::from_secs(3)))?;
    let mut r = BufReader::new(&s);
    let mut out = String::new();
    r.read_line(&mut out)?;
    let v: serde_json::Value = serde_json::from_str(&out)?;
    let items = v.pointer("/data").and_then(|d| d.as_array()).cloned().unwrap_or_default();
    Ok(items
        .iter()
        .filter_map(|m| m.get("text").and_then(|t| t.as_str()).map(String::from))
        .collect())
}

/// Operator-only escape hatch: force close the Orca tab immediately.
/// There is deliberately no scheduler-side shell injection: once pi owns the
/// terminal input, sending `pkill` text is not a reliable shell command and
/// can target the wrong process. Orca tab close is the supervisor primitive.
fn force_kill(sched: &Arc<Sched>, task_id: &str) {
    let handle = sched.terminals.lock().unwrap().get(task_id).cloned();
    if let Some(h) = handle {
        if !h.starts_with("stub-") {
            let _ = crate::orca_term::close(&h);
        }
    }
}

fn close_terminal(sched: &Arc<Sched>, task_id: &str) -> anyhow::Result<()> {
    sched.running_since.lock().unwrap().remove(task_id);
    sched.hop_since.lock().unwrap().remove(task_id);
    // R4: terminal rows carry no hop substate.
    let _ = sched.db.set_hop(task_id, "");
    let handle = sched.terminals.lock().unwrap().remove(task_id);
    if let Some(h) = handle {
        if h.starts_with("stub-") {
            return Ok(());
        }
        // Reclaim protocol: signal, wait briefly for the ack, close the tab
        // regardless. The session exits its own process; the scheduler only
        // ever touches the Orca tab object. No shell injection here —
        // kill_pi_for_task is operator-only (cancel --force).
        signal_recycle(sched, task_id, "close");
        if !wait_recycled_ack(sched, task_id, std::time::Duration::from_secs(5)) {
            tracing::warn!(task = %task_id, handle = %h, "recycle_no_ack");
        }
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
    fn failed_handoff_marker_records_failed_and_keeps_evidence() {
        // ARIS ops-110 regression: an agent may hand over an explanation
        // whose contract says the hop failed. Preserve the out head for the
        // next role, yet never publish it as a successful done/closed hop.
        let s = test_sched();
        seed(&s, "fh1", "writer", "", TaskState::Running);
        let payload = "> hop-failed: task premise is stale\n\nNo files were written.";
        on_out(&s, "writer", &reply_msg("fh1", "", payload)).unwrap();
        let task = s.db.get("fh1").unwrap().unwrap();
        assert_eq!(task.state, TaskState::Failed);
        assert_eq!(task.ledger_state, "failed");
        assert!(task.out_head.starts_with("> hop-failed:"));
        assert!(task.reason.contains("handoff declares hop-failed"));
        let tail = s.db.ledger(10).unwrap();
        assert!(tail.iter().any(|row| row.task_id == "fh1" && row.state == "failed"));
    }

    #[test]
    fn marker_later_in_normal_handoff_does_not_change_success() {
        // The marker is a first non-empty-line protocol token. Quoting it
        // later in an otherwise successful report must remain a done/closed
        // handoff, so prose cannot accidentally change control semantics.
        let s = test_sched();
        seed(&s, "fh2", "writer", "", TaskState::Running);
        let payload = "Report completed.\n\nPrior attempt said: > hop-failed: stale";
        on_out(&s, "writer", &reply_msg("fh2", "", payload)).unwrap();
        let task = s.db.get("fh2").unwrap().unwrap();
        assert_eq!(task.state, TaskState::Closed);
        assert_eq!(task.ledger_state, "done");
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
    fn recycled_quit_marks_failed_without_retry() {
        let s = test_sched();
        seed(&s, "quit1", "a", "", TaskState::Running);
        on_recycled(&s, "quit1", "quit:no input").unwrap();
        let t = s.db.get("quit1").unwrap().unwrap();
        assert_eq!(t.state, TaskState::Failed);
        let tail = s.db.ledger(10).unwrap();
        assert!(tail.iter().any(|e| e.task_id == "quit1" && e.reason.contains("swarm-recycled")));
    }

    #[test]
    fn ready_matches_own_terminal_first() {
        use crate::db::TaskState;
        let s = test_sched();
        seed(&s, "own", "a", "", TaskState::Running);
        seed(&s, "other", "a", "", TaskState::Running);
        s.terminals.lock().unwrap().insert("own".into(), "term-OWN".into());
        s.terminals.lock().unwrap().insert("other".into(), "term-OTHER".into());
        s.hop_since.lock().unwrap().insert("own".into(), ("dispatched".into(), std::time::Instant::now()));
        s.hop_since.lock().unwrap().insert("other".into(), ("dispatched".into(), std::time::Instant::now()));
        // The rows list newest-first; path-only matching would pick "other".
        // Handle-first matching must pick the task whose terminal we created.
        let rows = s.db.list(None, 200).unwrap();
        let terminals = s.terminals.lock().unwrap();
        let hit = match_candidate(&rows, &terminals, "a", "term-OWN");
        assert_eq!(hit.as_deref(), Some("own"));
        // Unknown handle falls back to path matching without panic.
        let hit = match_candidate(&rows, &terminals, "a", "term-STRANGER");
        assert!(hit == Some("own".into()) || hit == Some("other".into()));
        // Wrong workspace matches nothing.
        assert_eq!(match_candidate(&rows, &terminals, "b", "term-OWN"), None);
    }

    #[test]
    fn role_graph_groups_hops_and_marks_focusable_tab() {
        let workspaces = vec![
            serde_json::json!({"path": ".", "name": "root", "daemon": "ready", "back_edges": []}),
            serde_json::json!({"path": "model", "name": "model", "daemon": "ready", "back_edges": []}),
        ];
        let tasks = vec![
            serde_json::json!({"task_id": "root-task-abcdefgh", "to_ws": ".", "state": "running", "terminal": "term_root"}),
            serde_json::json!({"task_id": "model-task-abcdefgh", "to_ws": "model", "state": "running", "terminal": "term_model"}),
        ];
        let graph = serde_json::json!({"by_ws": [], "edges": []});
        let layout = crate::tui::build_graph_layout(&workspaces, &tasks, &graph, 52, 20, Some("model-task-abcdefgh"), &[]);
        let text = layout.lines.iter().map(|line| line.text.as_str()).collect::<Vec<_>>().join("\n");
        assert!(text.contains("root ●") && text.contains("model ●"));
        assert!(text.contains("root-ta") && text.contains("model-ta"));
        assert!(text.contains("◉"));
        assert!(layout.lines.iter().any(|line| line.kind == crate::tui::GraphLineKind::Selected));
    }

    #[test]
    fn detail_snapshot_shapes() {
        let detail = serde_json::json!({
            "task": {"task_id":"abcdefgh-1234", "from_ws":".", "to_ws":"a", "attempt":1, "state":"running", "terminal":"", "created_at":0, "payload":"body", "out_head":"", "reason":""},
            "parent": null,
            "children": []
        });
        let (title, body) = crate::tui::detail_text(&detail);
        assert!(title.contains("abcdefgh"));
        assert!(body.contains("body"));
    }

    #[test]
    fn hop_transitions_follow_allowed_edges() {
        // R4 §6: dispatched→ready→busy→idle→busy→terminal; adoption lands busy.
        let s = test_sched();
        seed(&s, "h1", "a", "", TaskState::Running);
        // '' -> dispatched (fresh dispatch path)
        assert_eq!(current_hop(&s, "h1"), "");
        set_hop(&s, "h1", "", "dispatched").unwrap();
        assert_eq!(s.db.get("h1").unwrap().unwrap().hop_state, "dispatched");
        // dispatched --busy--> busy (CR2: plugin faster than scheduler)
        on_hop_activity(&s, "h1", true, false).unwrap();
        assert_eq!(s.db.get("h1").unwrap().unwrap().hop_state, "busy");
        // busy -> busy no-op
        on_hop_activity(&s, "h1", true, false).unwrap();
        assert_eq!(s.db.get("h1").unwrap().unwrap().hop_state, "busy");
        // busy -> idle -> busy round trip
        on_hop_activity(&s, "h1", false, true).unwrap();
        assert_eq!(s.db.get("h1").unwrap().unwrap().hop_state, "idle");
        on_hop_activity(&s, "h1", true, false).unwrap();
        assert_eq!(s.db.get("h1").unwrap().unwrap().hop_state, "busy");
        // idle --idle--> idle no-op (display heartbeat only)
        on_hop_activity(&s, "h1", false, false).unwrap();
        on_hop_activity(&s, "h1", false, false).unwrap();
        assert_eq!(s.db.get("h1").unwrap().unwrap().hop_state, "idle");
    }

    #[test]
    fn hop_activity_on_terminal_row_emits_drop() {
        // R4 §6: busy/idle arriving at a terminal row reuses the drop event.
        let s = test_sched();
        seed(&s, "t1", "a", "", TaskState::Closed);
        let mut rx = s.bus.sender().subscribe();
        on_hop_activity(&s, "t1", true, false).unwrap();
        let ev = rx.try_recv().expect("drop event emitted");
        assert_eq!(ev.typ, "out_for_terminal_task_dropped");
        // Unknown task: silent drop, no event.
        let mut rx2 = s.bus.sender().subscribe();
        on_hop_activity(&s, "ghost", true, false).unwrap();
        assert!(rx2.try_recv().is_err());
    }

    #[test]
    fn timeout_fail_closes_terminal() {
        // CR1: timeout death and tab recycle are one transaction.
        // on_early_exit ends in close_terminal; a stub handle exercises
        // the bookkeeping without spawning orca.
        let s = test_sched();
        seed(&s, "c1", "a", "", TaskState::Running);
        s.db.set_terminal("c1", "stub-x").unwrap();
        s.terminals.lock().unwrap().insert("c1".into(), "stub-x".into());
        s.hop_since.lock().unwrap()
            .insert("c1".into(), ("dispatched".into(), std::time::Instant::now()));
        on_early_exit(&s, "c1", "swarm_ready timeout").unwrap();
        let t = s.db.get("c1").unwrap().unwrap();
        assert_eq!(t.state, TaskState::Failed);
        assert!(!s.terminals.lock().unwrap().contains_key("c1"));
        assert!(!s.hop_since.lock().unwrap().contains_key("c1"));
        assert_eq!(t.hop_state, "");
    }
}
