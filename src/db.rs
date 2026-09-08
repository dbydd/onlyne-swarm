use anyhow::Context;
use rusqlite::{Connection, params, params_from_iter, types::Value as SqlValue};
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
    pub fn as_str(self) -> &'static str {
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
    pub out_head: String,
    pub reason: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Default)]
pub struct TaskFilter {
    pub state: Option<String>,
    pub to_ws: Option<String>,
    pub from_ws: Option<String>,
    pub text: Option<String>,
    pub since: Option<i64>,
    pub retry_only: bool,
    pub limit: usize,
    pub offset: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct TaskPage {
    pub rows: Vec<TaskRow>,
    pub total: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TaskDetailTask {
    pub task_id: String,
    pub from_ws: String,
    pub to_ws: String,
    pub transfer_send_to: String,
    pub attempt: u32,
    pub state: TaskState,
    pub terminal: String,
    pub payload: String,
    pub out_head: String,
    pub reason: String,
    pub created_at: i64,
}

impl From<TaskRow> for TaskDetailTask {
    fn from(row: TaskRow) -> Self {
        Self {
            task_id: row.task_id,
            from_ws: row.from_ws,
            to_ws: row.to_ws,
            transfer_send_to: row.transfer_send_to,
            attempt: row.attempt,
            state: row.state,
            terminal: row.terminal,
            payload: row.payload,
            out_head: row.out_head,
            reason: row.reason,
            created_at: row.created_at,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TaskDetail {
    pub task: TaskDetailTask,
    pub parent: Option<TaskRow>,
    pub children: Vec<TaskRow>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct GraphCount {
    pub ws: String,
    pub state: String,
    pub n: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct GraphEdge {
    pub from: String,
    pub to: String,
    pub n: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct GraphData {
    pub by_ws: Vec<GraphCount>,
    pub edges: Vec<GraphEdge>,
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
        get_with_conn(&c, task_id)
    }

    /// Lineage tree rooted at `task_id` (downstream spawns via transfer_send_to).
    pub fn family(&self, task_id: &str) -> anyhow::Result<Vec<TaskRow>> {
        let c = self.inner.lock().unwrap();
        let mut st = c.prepare(
            "WITH RECURSIVE fam(id) AS (
               SELECT ? UNION SELECT task_id FROM tasks, fam WHERE transfer_send_to = fam.id
             ) SELECT task_id, from_ws, to_ws, transfer_send_to, attempt, state, terminal, payload, out_head, reason, created_at
               FROM tasks WHERE task_id IN fam ORDER BY created_at ASC, rowid ASC",
        )?;
        let mapped = st.query_map(params![task_id], row)?;
        let rows = collect_rows(mapped)?;
        Ok(rows)
    }

    /// Paginated history query. The filter is shared by the TUI and IPC, so
    /// `total` and `rows` always use exactly the same predicates.
    pub fn list_page(&self, filter: &TaskFilter) -> anyhow::Result<TaskPage> {
        let c = self.inner.lock().unwrap();
        let (where_sql, args) = task_where(filter);
        let count_sql = format!("SELECT COUNT(*) FROM tasks{where_sql}");
        let total: i64 = c.query_row(&count_sql, params_from_iter(args.clone()), |r| r.get(0))?;

        let limit = filter.limit.clamp(1, 500) as i64;
        let offset = filter.offset.min(i64::MAX as usize) as i64;
        let sql = format!(
            "SELECT task_id, from_ws, to_ws, transfer_send_to, attempt, state, terminal, payload, out_head, reason, created_at \
             FROM tasks{where_sql} ORDER BY created_at DESC, rowid DESC LIMIT ? OFFSET ?"
        );
        let mut row_args = args;
        row_args.push(SqlValue::Integer(limit));
        row_args.push(SqlValue::Integer(offset));
        let mut st = c.prepare(&sql)?;
        let rows = collect_rows(st.query_map(params_from_iter(row_args), row)?)?;
        Ok(TaskPage { rows, total })
    }

    /// Legacy scheduler query. It keeps its old shape for lifecycle callers;
    /// IPC and the TUI use `list_page` for filtered pagination.
    pub fn list(&self, state: Option<&str>, limit: usize) -> anyhow::Result<Vec<TaskRow>> {
        self.list_page(&TaskFilter {
            state: state.map(str::to_owned),
            limit,
            ..Default::default()
        })
        .map(|page| page.rows)
    }

    pub fn detail(&self, task_id: &str) -> anyhow::Result<Option<TaskDetail>> {
        let c = self.inner.lock().unwrap();
        let Some(task) = get_with_conn(&c, task_id)? else {
            return Ok(None);
        };
        let parent = if task.transfer_send_to.is_empty() {
            None
        } else {
            get_with_conn(&c, &task.transfer_send_to)?
        };
        let mut st = c.prepare(
            "WITH RECURSIVE fam(id) AS (
               SELECT ? UNION SELECT task_id FROM tasks, fam WHERE transfer_send_to = fam.id
             ) SELECT task_id, from_ws, to_ws, transfer_send_to, attempt, state, terminal, payload, out_head, reason, created_at
               FROM tasks WHERE task_id IN fam ORDER BY created_at ASC, rowid ASC",
        )?;
        let children = collect_rows(st.query_map(params![task_id], row)?)?
            .into_iter()
            .filter(|row| row.task_id != task_id)
            .collect();
        Ok(Some(TaskDetail {
            task: task.into(),
            parent,
            children,
        }))
    }

    pub fn graph(&self) -> anyhow::Result<GraphData> {
        let c = self.inner.lock().unwrap();
        let mut by_ws_stmt = c.prepare(
            "SELECT to_ws, state, COUNT(*) FROM tasks GROUP BY to_ws, state ORDER BY to_ws ASC, state ASC",
        )?;
        let by_ws = by_ws_stmt
            .query_map([], |r| {
                Ok(GraphCount {
                    ws: r.get(0)?,
                    state: r.get(1)?,
                    n: r.get(2)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut edge_stmt = c.prepare(
            "SELECT from_ws, to_ws, COUNT(*) FROM tasks WHERE from_ws <> to_ws \
             GROUP BY from_ws, to_ws ORDER BY from_ws ASC, to_ws ASC",
        )?;
        let edges = edge_stmt
            .query_map([], |r| {
                Ok(GraphEdge {
                    from: r.get(0)?,
                    to: r.get(1)?,
                    n: r.get(2)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(GraphData { by_ws, edges })
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
             WHERE ledger_state IN ('done','failed','cancelled','spawn_failed','adopted') ORDER BY created_at DESC LIMIT ?",
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

fn get_with_conn(c: &Connection, task_id: &str) -> anyhow::Result<Option<TaskRow>> {
    let mut st = c.prepare(
        "SELECT task_id, from_ws, to_ws, transfer_send_to, attempt, state, terminal, payload, out_head, reason, created_at \
         FROM tasks WHERE task_id=?",
    )?;
    let mut rows = st.query(params![task_id])?;
    let Some(r) = rows.next()? else {
        return Ok(None);
    };
    Ok(Some(row(r)?))
}

fn collect_rows<I>(rows: I) -> anyhow::Result<Vec<TaskRow>>
where
    I: IntoIterator<Item = rusqlite::Result<TaskRow>>,
{
    rows.into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn task_where(filter: &TaskFilter) -> (String, Vec<SqlValue>) {
    let mut clauses: Vec<String> = Vec::new();
    let mut args = Vec::new();
    match filter.state.as_deref().unwrap_or("all") {
        "" | "all" => {}
        "active" => clauses.push("state IN ('pending','running')".into()),
        state => {
            clauses.push("state=?".into());
            args.push(SqlValue::Text(state.into()));
        }
    }
    if let Some(to_ws) = filter.to_ws.as_deref().filter(|s| !s.is_empty()) {
        clauses.push("to_ws=?".into());
        args.push(SqlValue::Text(to_ws.into()));
    }
    if let Some(from_ws) = filter.from_ws.as_deref().filter(|s| !s.is_empty()) {
        clauses.push("from_ws=?".into());
        args.push(SqlValue::Text(from_ws.into()));
    }
    if let Some(text) = filter.text.as_deref().filter(|s| !s.is_empty()) {
        clauses.push("(payload LIKE ? OR out_head LIKE ? OR reason LIKE ?)".into());
        let pattern = SqlValue::Text(format!("%{text}%"));
        args.extend([pattern.clone(), pattern.clone(), pattern]);
    }
    if let Some(since) = filter.since {
        clauses.push("created_at >= ?".into());
        args.push(SqlValue::Integer(since));
    }
    if filter.retry_only {
        clauses.push("attempt > 1".into());
    }
    let where_sql = if clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", clauses.join(" AND "))
    };
    (where_sql, args)
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
        out_head: r.get(8).unwrap_or_default(),
        reason: r.get(9).unwrap_or_default(),
        created_at: r.get(10).unwrap_or_default(),
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
    fn insert_id(
        db: &Db,
        id: &str,
        from: &str,
        to: &str,
        parent: &str,
        attempt: u32,
        payload: &str,
        created_at: i64,
    ) {
        db.insert_task(id, from, to, parent, attempt, payload)
            .unwrap();
        db.inner
            .lock()
            .unwrap()
            .execute(
                "UPDATE tasks SET created_at=? WHERE task_id=?",
                params![created_at, id],
            )
            .unwrap();
    }

    #[test]
    fn filtered_page_counts_orders_and_pages() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path()).unwrap();
        insert_id(&db, "a", ".", "scout", "", 1, "alpha", 100);
        db.set_state("a", TaskState::Running).unwrap();
        insert_id(&db, "b", "scout", "model", "a", 2, "needle payload", 200);
        db.set_state("b", TaskState::Done).unwrap();
        db.set_ledger("b", "done", "needle output", "").unwrap();
        insert_id(&db, "c", "model", "scout", "a", 3, "plain", 300);
        db.set_state("c", TaskState::Failed).unwrap();
        db.set_ledger("c", "failed", "", "needle reason").unwrap();

        let active = db
            .list_page(&TaskFilter {
                state: Some("active".into()),
                limit: 50,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(active.total, 1);
        assert_eq!(active.rows[0].task_id, "a");

        let first = db
            .list_page(&TaskFilter {
                text: Some("needle".into()),
                retry_only: true,
                limit: 1,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(first.total, 2);
        assert_eq!(first.rows[0].task_id, "c");
        let second = db
            .list_page(&TaskFilter {
                text: Some("needle".into()),
                retry_only: true,
                limit: 1,
                offset: 1,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(second.rows[0].task_id, "b");

        let scoped = db
            .list_page(&TaskFilter {
                state: Some("all".into()),
                to_ws: Some("scout".into()),
                from_ws: Some("model".into()),
                since: Some(250),
                limit: 50,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(scoped.total, 1);
        assert_eq!(scoped.rows[0].task_id, "c");
    }

    #[test]
    fn graph_and_detail_expose_aggregates_and_lineage() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path()).unwrap();
        insert_id(&db, "root", ".", "scout", "", 1, "root payload", 100);
        db.set_state("root", TaskState::Running).unwrap();
        insert_id(
            &db,
            "child",
            "scout",
            "model",
            "root",
            2,
            "child payload",
            200,
        );
        db.set_state("child", TaskState::Done).unwrap();
        db.set_ledger("child", "done", "child output", "").unwrap();
        insert_id(
            &db,
            "self",
            "model",
            "model",
            "root",
            1,
            "self payload",
            300,
        );
        db.set_state("self", TaskState::Failed).unwrap();
        db.set_ledger("self", "failed", "", "self reason").unwrap();

        let graph = db.graph().unwrap();
        assert!(
            graph
                .by_ws
                .iter()
                .any(|row| row.ws == "scout" && row.state == "running" && row.n == 1)
        );
        assert!(
            graph
                .edges
                .iter()
                .any(|edge| edge.from == "." && edge.to == "scout" && edge.n == 1)
        );
        assert!(
            graph
                .edges
                .iter()
                .any(|edge| edge.from == "scout" && edge.to == "model" && edge.n == 1)
        );
        assert!(!graph.edges.iter().any(|edge| edge.from == edge.to));

        let detail = db.detail("child").unwrap().unwrap();
        assert_eq!(detail.task.payload, "child payload");
        assert_eq!(detail.task.out_head, "child output");
        let json = serde_json::to_value(&detail).unwrap();
        assert_eq!(
            json.pointer("/task/payload").and_then(|v| v.as_str()),
            Some("child payload")
        );
        assert!(json.pointer("/parent/payload").is_none());
        assert_eq!(detail.parent.unwrap().task_id, "root");
        let root = db.detail("root").unwrap().unwrap();
        assert_eq!(root.children.len(), 2);
        assert!(root.children.iter().any(|row| row.task_id == "child"));
        assert!(root.children.iter().any(|row| row.reason == "self reason"));
    }

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
