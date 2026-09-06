use anyhow::Context;
use rusqlite::{Connection, params};
use serde::Serialize;
use std::path::Path;
use std::sync::Mutex;

#[derive(Debug, Clone, Serialize)]
pub struct LedgerEvent {
    pub task_id: String,
    pub transfer_send_to: String,
    pub from_ws: String,
    pub to_ws: String,
    pub state: String,
    pub out_head: String,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Pending,
    Running,
    Done,
    Failed,
    Cancelled,
    Closed,
}

impl TaskState {
    fn as_str(self) -> &'static str {
        match self {
            TaskState::Pending => "pending",
            TaskState::Running => "running",
            TaskState::Done => "done",
            TaskState::Failed => "failed",
            TaskState::Cancelled => "cancelled",
            TaskState::Closed => "closed",
        }
    }
    fn from_str(s: &str) -> Self {
        match s {
            "running" => TaskState::Running,
            "done" => TaskState::Done,
            "failed" => TaskState::Failed,
            "cancelled" => TaskState::Cancelled,
            "closed" => TaskState::Closed,
            _ => TaskState::Pending,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TaskRow {
    pub task_id: String,
    pub from_ws: String,
    pub to_ws: String,
    pub transfer_send_to: String,
    pub attempt: u32,
    pub state: TaskState,
    pub terminal: String,
    #[serde(skip_serializing)]
    pub payload: String,
}

pub struct Db {
    inner: Mutex<Connection>,
}

impl Db {
    pub fn open(root: &Path) -> anyhow::Result<Self> {
        let path = crate::root::swarm_db(root);
        if let Some(p) = path.parent() {
            std::fs::create_dir_all(p)?;
        }
        let conn = Connection::open(&path).with_context(|| format!("open {}", path.display()))?;
        // Amendment-1 schema: no pending_replies, no dead_letter table.
        // Old databases carry the old layout; they are dropped, not migrated.
        let old_layout: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('tasks') WHERE name='pending_replies'",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);
        if old_layout > 0 {
            conn.execute_batch("DROP TABLE IF EXISTS tasks;")?;
        }
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS tasks(
               task_id TEXT PRIMARY KEY,
               from_ws TEXT NOT NULL,
               to_ws TEXT NOT NULL,
               transfer_send_to TEXT NOT NULL DEFAULT '',
               attempt INTEGER NOT NULL DEFAULT 1,
               state TEXT NOT NULL DEFAULT 'pending',
               terminal TEXT NOT NULL DEFAULT '',
               payload TEXT NOT NULL DEFAULT '',
               out_head TEXT NOT NULL DEFAULT '',
               reason TEXT NOT NULL DEFAULT '',
               ledger_state TEXT NOT NULL DEFAULT '',
               created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
             );",
        )?;
        Ok(Self {
            inner: Mutex::new(conn),
        })
    }

    pub fn insert_task(
        &self,
        task_id: &str,
        from_ws: &str,
        to_ws: &str,
        transfer_send_to: &str,
        attempt: u32,
        payload: &str,
    ) -> anyhow::Result<bool> {
        let c = self.inner.lock().unwrap();
        let n = c.execute(
            "INSERT OR IGNORE INTO tasks(task_id, from_ws, to_ws, transfer_send_to, attempt, payload) VALUES(?,?,?,?,?,?)",
            params![task_id, from_ws, to_ws, transfer_send_to, attempt, payload],
        )?;
        Ok(n == 1)
    }

    pub fn set_state(&self, task_id: &str, state: TaskState) -> anyhow::Result<()> {
        self.inner.lock().unwrap().execute(
            "UPDATE tasks SET state=? WHERE task_id=?",
            params![state.as_str(), task_id],
        )?;
        Ok(())
    }

    pub fn set_terminal(&self, task_id: &str, terminal: &str) -> anyhow::Result<()> {
        self.inner.lock().unwrap().execute(
            "UPDATE tasks SET terminal=? WHERE task_id=?",
            params![terminal, task_id],
        )?;
        Ok(())
    }

    /// Record a terminal ledger row: terminal state + out head + reason.
    pub fn set_ledger(
        &self,
        task_id: &str,
        ledger_state: &str,
        out_head: &str,
        reason: &str,
    ) -> anyhow::Result<()> {
        self.inner.lock().unwrap().execute(
            "UPDATE tasks SET ledger_state=?, out_head=?, reason=? WHERE task_id=?",
            params![ledger_state, out_head, reason, task_id],
        )?;
        Ok(())
    }

    pub fn get(&self, task_id: &str) -> anyhow::Result<Option<TaskRow>> {
        let c = self.inner.lock().unwrap();
        let mut st = c.prepare(
            "SELECT task_id, from_ws, to_ws, transfer_send_to, attempt, state, terminal, payload FROM tasks WHERE task_id=?",
        )?;
        let mut rows = st.query(params![task_id])?;
        let Some(r) = rows.next()? else { return Ok(None) };
        Ok(Some(row(r)?))
    }

    /// Lineage tree rooted at `task_id` (downstream spawns via transfer_send_to).
    pub fn family(&self, task_id: &str) -> anyhow::Result<Vec<TaskRow>> {
        let c = self.inner.lock().unwrap();
        let mut st = c.prepare(
            "WITH RECURSIVE fam(id) AS (
               SELECT ? UNION SELECT task_id FROM tasks, fam WHERE transfer_send_to = fam.id
             ) SELECT task_id, from_ws, to_ws, transfer_send_to, attempt, state, terminal, payload
               FROM tasks WHERE task_id IN fam",
        )?;
        let rows = st.query_map(params![task_id], row)?;
        let mut out = vec![];
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    pub fn list(&self, state: Option<&str>, limit: usize) -> anyhow::Result<Vec<TaskRow>> {
        let c = self.inner.lock().unwrap();
        let sql = if state.is_some() {
            "SELECT task_id, from_ws, to_ws, transfer_send_to, attempt, state, terminal, payload FROM tasks WHERE state=? ORDER BY created_at DESC LIMIT ?"
        } else {
            "SELECT task_id, from_ws, to_ws, transfer_send_to, attempt, state, terminal, payload FROM tasks ORDER BY created_at DESC LIMIT ?"
        };
        let mut st = c.prepare(sql)?;
        let iter = if let Some(s) = state {
            st.query_map(params![s, limit as i64], row)?
        } else {
            st.query_map(params![limit as i64], row)?
        };
        let mut out = vec![];
        for r in iter {
            out.push(r?);
        }
        Ok(out)
    }

    pub fn counts(&self) -> anyhow::Result<std::collections::BTreeMap<String, i64>> {
        let c = self.inner.lock().unwrap();
        let mut st = c.prepare("SELECT state, COUNT(*) FROM tasks GROUP BY state")?;
        let mut map = std::collections::BTreeMap::new();
        for r in st.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))? {
            let (k, v) = r?;
            map.insert(k, v);
        }
        Ok(map)
    }

    /// Ledger tail: terminal rows (done/failed/cancelled) newest first.
    pub fn ledger(&self, limit: usize) -> anyhow::Result<Vec<LedgerEvent>> {
        let c = self.inner.lock().unwrap();
        let mut st = c.prepare(
            "SELECT task_id, transfer_send_to, from_ws, to_ws, ledger_state, out_head, reason FROM tasks
             WHERE ledger_state IN ('done','failed','cancelled','spawn_failed') ORDER BY created_at DESC LIMIT ?",
        )?;
        let mut out = vec![];
        for r in st.query_map(params![limit as i64], |r| {
            Ok(LedgerEvent {
                task_id: r.get(0)?,
                transfer_send_to: r.get(1)?,
                from_ws: r.get(2)?,
                to_ws: r.get(3)?,
                state: r.get(4)?,
                out_head: r.get(5)?,
                reason: r.get(6)?,
            })
        })? {
            out.push(r?);
        }
        Ok(out)
    }

    /// Ledger file tail at `<root>/.onlyne/ledger.jsonl` (append-only mirror).
    pub fn ledger_path(root: &Path) -> std::path::PathBuf {
        root.join(".onlyne/ledger.jsonl")
    }
}

fn row(r: &rusqlite::Row) -> rusqlite::Result<TaskRow> {
    Ok(TaskRow {
        task_id: r.get(0)?,
        from_ws: r.get(1)?,
        to_ws: r.get(2)?,
        transfer_send_to: r.get(3)?,
        attempt: r.get(4)?,
        state: TaskState::from_str(&r.get::<_, String>(5)?),
        terminal: r.get(6)?,
        payload: r.get(7).unwrap_or_default(),
    })
}

/// Append one ledger line to `<root>/.onlyne/ledger.jsonl` (best effort).
pub fn append_ledger_line(root: &Path, ev: &LedgerEvent) {
    use std::io::Write;
    let path = Db::ledger_path(root);
    if let Some(p) = path.parent() {
        let _ = std::fs::create_dir_all(p);
    }
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
    {
        let line = serde_json::json!({
            "ts": chrono::Utc::now().timestamp(),
            "task_id": ev.task_id,
            "transfer_send_to": ev.transfer_send_to,
            "from": ev.from_ws,
            "to": ev.to_ws,
            "state": ev.state,
            "out_head": ev.out_head,
            "reason": ev.reason,
        });
        let _ = writeln!(f, "{line}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn insert_is_idempotent_and_family_follows_lineage() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path()).unwrap();
        assert!(db.insert_task("a", ".", "x", "", 1, "pa").unwrap());
        assert!(!db.insert_task("a", ".", "x", "", 1, "pa").unwrap());
        db.insert_task("b", "x", "y", "a", 1, "pb").unwrap();
        assert_eq!(db.family("a").unwrap().len(), 2);
        assert_eq!(db.get("a").unwrap().unwrap().payload, "pa");
    }

    #[test]
    fn old_waiting_schema_is_dropped_not_migrated() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".onlyne/swarm.db");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE tasks(task_id TEXT PRIMARY KEY, reply_to TEXT, pending_replies INTEGER);",
        )
        .unwrap();
        drop(conn);
        let db = Db::open(dir.path()).unwrap();
        assert!(db.insert_task("n", ".", "x", "", 1, "p").unwrap());
        assert_eq!(db.get("n").unwrap().unwrap().transfer_send_to, "");
    }

    #[test]
    fn ledger_tail_lists_terminal_rows() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path()).unwrap();
        db.insert_task("a", ".", "x", "", 1, "p").unwrap();
        db.set_state("a", TaskState::Running).unwrap();
        assert!(db.ledger(10).unwrap().is_empty());
        db.set_state("a", TaskState::Done).unwrap();
        db.set_ledger("a", "done", "out head", "").unwrap();
        let tail = db.ledger(10).unwrap();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].task_id, "a");
        assert_eq!(tail[0].out_head, "out head");
    }
}
