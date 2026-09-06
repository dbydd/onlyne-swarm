use anyhow::Context;
use rusqlite::{Connection, params};
use serde::Serialize;
use std::path::Path;
use std::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Pending,
    Running,
    Replied,
    Failed,
    Cancelled,
    Closed,
}

impl TaskState {
    fn as_str(self) -> &'static str {
        match self {
            TaskState::Pending => "pending",
            TaskState::Running => "running",
            TaskState::Replied => "replied",
            TaskState::Failed => "failed",
            TaskState::Cancelled => "cancelled",
            TaskState::Closed => "closed",
        }
    }
    fn from_str(s: &str) -> Self {
        match s {
            "running" => TaskState::Running,
            "replied" => TaskState::Replied,
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
    pub reply_to: String,
    pub attempt: u32,
    pub state: TaskState,
    pub pending_replies: i64,
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
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS tasks(
               task_id TEXT PRIMARY KEY,
               from_ws TEXT NOT NULL,
               to_ws TEXT NOT NULL,
               reply_to TEXT NOT NULL DEFAULT '',
               attempt INTEGER NOT NULL DEFAULT 1,
               state TEXT NOT NULL DEFAULT 'pending',
               pending_replies INTEGER NOT NULL DEFAULT 0,
               terminal TEXT NOT NULL DEFAULT '',
               payload TEXT NOT NULL DEFAULT '',
               created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
             );
             CREATE TABLE IF NOT EXISTS schema_flags(name TEXT PRIMARY KEY);
             CREATE TABLE IF NOT EXISTS dead_letter(
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               task_id TEXT NOT NULL,
               reason TEXT NOT NULL,
               created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
             );",
        )?;
        // Idempotent migration for pre-payload databases.
        let migrated: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM schema_flags WHERE name='payload_v1'",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);
        if migrated == 0 {
            let has_payload: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM pragma_table_info('tasks') WHERE name='payload'",
                    [],
                    |r| r.get(0),
                )
                .unwrap_or(0);
            if has_payload == 0 {
                conn.execute("ALTER TABLE tasks ADD COLUMN payload TEXT NOT NULL DEFAULT ''", [])?;
            }
            conn.execute(
                "INSERT OR IGNORE INTO schema_flags(name) VALUES('payload_v1')",
                [],
            )?;
        }
        Ok(Self {
            inner: Mutex::new(conn),
        })
    }

    pub fn insert_task(
        &self,
        task_id: &str,
        from_ws: &str,
        to_ws: &str,
        reply_to: &str,
        attempt: u32,
        payload: &str,
    ) -> anyhow::Result<bool> {
        let c = self.inner.lock().unwrap();
        let n = c.execute(
            "INSERT OR IGNORE INTO tasks(task_id, from_ws, to_ws, reply_to, attempt, payload) VALUES(?,?,?,?,?,?)",
            params![task_id, from_ws, to_ws, reply_to, attempt, payload],
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

    pub fn bump_parent(&self, parent: &str, delta: i64) -> anyhow::Result<()> {
        if parent.is_empty() {
            return Ok(());
        }
        self.inner.lock().unwrap().execute(
            "UPDATE tasks SET pending_replies = pending_replies + ? WHERE task_id=?",
            params![delta, parent],
        )?;
        Ok(())
    }

    pub fn get(&self, task_id: &str) -> anyhow::Result<Option<TaskRow>> {
        let c = self.inner.lock().unwrap();
        let mut st = c.prepare(
            "SELECT task_id, from_ws, to_ws, reply_to, attempt, state, pending_replies, terminal, payload FROM tasks WHERE task_id=?",
        )?;
        let mut rows = st.query(params![task_id])?;
        let Some(r) = rows.next()? else { return Ok(None) };
        Ok(Some(row(r)?))
    }

    pub fn dec_parent(&self, parent: &str) -> anyhow::Result<Option<TaskRow>> {
        self.bump_parent(parent, -1)?;
        self.get(parent)
    }

    pub fn family(&self, task_id: &str) -> anyhow::Result<Vec<TaskRow>> {
        let c = self.inner.lock().unwrap();
        let mut st = c.prepare(
            "WITH RECURSIVE fam(id) AS (
               SELECT ? UNION SELECT task_id FROM tasks, fam WHERE reply_to = fam.id
             ) SELECT task_id, from_ws, to_ws, reply_to, attempt, state, pending_replies, terminal, payload
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
            "SELECT task_id, from_ws, to_ws, reply_to, attempt, state, pending_replies, terminal, payload FROM tasks WHERE state=? ORDER BY created_at DESC LIMIT ?"
        } else {
            "SELECT task_id, from_ws, to_ws, reply_to, attempt, state, pending_replies, terminal, payload FROM tasks ORDER BY created_at DESC LIMIT ?"
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

    pub fn dead_letter(&self, task_id: &str, reason: &str) -> anyhow::Result<()> {
        self.inner.lock().unwrap().execute(
            "INSERT INTO dead_letter(task_id, reason) VALUES(?,?)",
            params![task_id, reason],
        )?;
        Ok(())
    }

    pub fn dead_letters(&self, limit: usize) -> anyhow::Result<Vec<(String, String)>> {
        let c = self.inner.lock().unwrap();
        let mut st = c.prepare("SELECT task_id, reason FROM dead_letter ORDER BY id DESC LIMIT ?")?;
        let mut out = vec![];
        for r in st.query_map(params![limit as i64], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })? {
            out.push(r?);
        }
        Ok(out)
    }
}

fn row(r: &rusqlite::Row) -> rusqlite::Result<TaskRow> {
    Ok(TaskRow {
        task_id: r.get(0)?,
        from_ws: r.get(1)?,
        to_ws: r.get(2)?,
        reply_to: r.get(3)?,
        attempt: r.get(4)?,
        state: TaskState::from_str(&r.get::<_, String>(5)?),
        pending_replies: r.get(6)?,
        terminal: r.get(7)?,
        payload: r.get(8).unwrap_or_default(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn insert_is_idempotent_and_parent_counts() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path()).unwrap();
        assert!(db.insert_task("a", ".", "x", "", 1, "pa").unwrap());
        assert!(!db.insert_task("a", ".", "x", "", 1, "pa").unwrap());
        db.insert_task("b", "x", "y", "a", 1, "pb").unwrap();
        db.bump_parent("a", 1).unwrap();
        let p = db.get("a").unwrap().unwrap();
        assert_eq!(p.pending_replies, 1);
        let p = db.dec_parent("a").unwrap().unwrap();
        assert_eq!(p.pending_replies, 0);
        assert_eq!(db.family("a").unwrap().len(), 2);
        assert_eq!(db.get("a").unwrap().unwrap().payload, "pa");
    }
}
