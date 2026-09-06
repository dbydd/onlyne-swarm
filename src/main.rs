mod daemon;
mod db;
mod events;
mod ipc;
mod orca;
mod orca_term;
mod proto;
mod root;
mod sched;
mod sync;
mod template;
mod tui;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "onlyne-swarm", version, about = "Reactive multi-agent DAG workflow scheduler derived from Onlyne")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Start the scheduler in the foreground (cwd is the swarm root; syncs .schedule first)
    Run {
        /// Bypass the ancestor-marker check (allows nested starts)
        #[arg(long)]
        force: bool,
    },
    /// Show status from an already-running scheduler
    Attach,
    /// Submit a Markdown payload file as a task to a workspace
    Submit {
        /// Target workspace tree-relative path (e.g. planner, a/b; root is ".")
        #[arg(long)]
        to: String,
        /// Markdown payload file
        #[arg(long)]
        payload: PathBuf,
    },
    /// Cancel a task family
    Cancel {
        task_id: String,
        #[arg(long)]
        reason: Option<String>,
    },
    /// List tasks or workspaces
    List {
        /// tasks | workspaces
        #[arg(long, default_value = "tasks")]
        what: String,
        #[arg(long)]
        state: Option<String>,
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    /// Show status from an already-running scheduler (same as attach)
    Status,
    /// Open the monitoring TUI (connects to the running scheduler)
    Tui,
    /// Manage generated workspace instances
    Workspace {
        #[command(subcommand)]
        cmd: WorkspaceCmd,
    },
}

#[derive(Subcommand)]
enum WorkspaceCmd {
    /// Generate _onlyne_workspaces from .agents/.schedule
    Create,
    /// Same as create (reconcile; never deletes instances or hand-written files)
    Sync,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Run { force } => {
            let cwd = std::env::current_dir()?;
            let root_p = root::ensure_root(&cwd, force)?;
            let report = sync::run_sync(&root_p)?;
            println!("sync: +{} created, {} orphans, {} dangling links",
                report.created.len(), report.orphans.len(), report.dangling.len());
            for o in &report.orphans {
                println!("orphan-instance: {o}");
            }
            for d in &report.dangling {
                println!("dangling-link: {d}");
            }
            let _db = db::Db::open(&root_p)?;
            let mut children = daemon::ensure_all(&root_p)?;
            println!("onlyne-swarm scheduler running at {}", root_p.display());
            let serve = ipc::serve(&root_p);
            tokio::select! {
                r = serve => r?,
                _ = tokio::signal::ctrl_c() => {
                    println!("shutting down; stopping managed daemons");
                    daemon::stop_all(&mut children).await;
                }
            }
            Ok(())
        }
        Cmd::Attach | Cmd::Status => {
            let cwd = std::env::current_dir()?;
            let root_p = root::cwd_root(&cwd);
            let v = orca::client_request(&root::swarm_sock(&root_p), serde_json::json!({"id": "cli", "op": "status"}))?;
            println!("{}", serde_json::to_string_pretty(&v)?);
            Ok(())
        }
        Cmd::Submit { to, payload } => {
            let cwd = std::env::current_dir()?;
            let root_p = root::cwd_root(&cwd);
            let text = std::fs::read_to_string(&payload)?;
            let v = orca::client_request(
                &root::swarm_sock(&root_p),
                serde_json::json!({"id": "cli", "op": "submit", "to": to, "payload_markdown": text}),
            )?;
            println!("{}", serde_json::to_string_pretty(&v)?);
            Ok(())
        }
        Cmd::Cancel { task_id, reason } => {
            let cwd = std::env::current_dir()?;
            let root_p = root::cwd_root(&cwd);
            let v = orca::client_request(
                &root::swarm_sock(&root_p),
                serde_json::json!({"id": "cli", "op": "cancel", "task_id": task_id, "reason": reason}),
            )?;
            println!("{}", serde_json::to_string_pretty(&v)?);
            Ok(())
        }
        Cmd::List { what, state, limit } => {
            let cwd = std::env::current_dir()?;
            let root_p = root::cwd_root(&cwd);
            let op = if what == "workspaces" { "list_workspaces" } else { "list_tasks" };
            let v = orca::client_request(
                &root::swarm_sock(&root_p),
                serde_json::json!({"id": "cli", "op": op, "state": state, "limit": limit}),
            )?;
            println!("{}", serde_json::to_string_pretty(&v)?);
            Ok(())
        }
        Cmd::Tui => {
            let cwd = std::env::current_dir()?;
            let root_p = root::cwd_root(&cwd);
            tui::run_tui(&root::swarm_sock(&root_p))
        }
        Cmd::Workspace { cmd } => {
            let cwd = std::env::current_dir()?;
            let root_p = root::ensure_root(&cwd, false)?;
            match cmd {
                WorkspaceCmd::Create | WorkspaceCmd::Sync => {
                    let report = sync::run_sync(&root_p)?;
                    println!("{}", serde_json::to_string_pretty(&report)?);
                    Ok(())
                }
            }
        }
    }
}
