mod daemon;
mod db;
mod events;
mod hierarchy;
mod init;
mod ipc;
mod orca;
mod orca_term;
mod proto;
mod root;
mod sched;
mod skill;
mod sync;
mod template;
mod tui;

use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::{
    generate,
    shells::{Fish, Zsh},
};
use std::io;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "onlyne-swarm", version, about = "Reactive multi-agent DAG workflow scheduler derived from Onlyne")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Initialize this directory as a swarm root (Onlyne workspace + full template starter)
    Init,
    /// Write or refresh the supervisor skill at .agents/skills/onlyne-swarm/SKILL.md
    ExportSkill,
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
    /// Cancel a task family (signal + ack + tab close; no shell injection)
    Cancel {
        task_id: String,
        #[arg(long)]
        reason: Option<String>,
        /// Manual escape hatch: scoped pkill inside the tab before closing.
        /// Off the default path; the session normally dies by its own hand.
        #[arg(long)]
        force: bool,
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
    /// Print shell completions (zsh or fish) to stdout
    ShellCompletions {
        shell: CompletionShell,
    },
    /// Manage generated workspace instances
    Workspace {
        #[command(subcommand)]
        cmd: WorkspaceCmd,
    },
}

#[derive(Subcommand)]
enum WorkspaceCmd {
    /// Generate .ws from .agents/.schedule
    Create,
    /// Same as create (reconcile; never deletes instances or hand-written files)
    Sync,
}

#[derive(Copy, Clone, clap::ValueEnum)]
enum CompletionShell {
    Zsh,
    Fish,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Init => {
            let cwd = std::env::current_dir()?;
            let root_p = crate::init::run_init(&cwd)?;
            println!("initialized swarm root at {}", root_p.display());
            Ok(())
        }
        Cmd::ExportSkill => {
            let cwd = std::env::current_dir()?;
            let root_p = root::cwd_root(&cwd);
            let path = crate::skill::export_skill(&root_p)?;
            println!("exported skill {}", path.display());
            Ok(())
        }
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
            for h in &report.hierarchy {
                println!("orca-node: {h}");
            }
            let _db = db::Db::open(&root_p)?;
            let mut children = daemon::ensure_all(&root_p)?;
            println!("onlyne-swarm scheduler running at {}", root_p.display());
            // serve() owns Ctrl-C/SIGTERM: it sets the Sched shutdown flag
            // (pump + reaper threads observe it and exit), then returns.
            // Only afterwards do we reap the managed daemons here.
            ipc::serve(&root_p).await?;
            println!("shutting down; stopping managed daemons");
            daemon::stop_all(&mut children).await;
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
        Cmd::Cancel { task_id, reason, force } => {
            let cwd = std::env::current_dir()?;
            let root_p = root::cwd_root(&cwd);
            let v = orca::client_request(
                &root::swarm_sock(&root_p),
                serde_json::json!({"id": "cli", "op": "cancel", "task_id": task_id, "reason": reason, "force": force}),
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
        Cmd::ShellCompletions { shell } => {
            shell_completions(shell);
            Ok(())
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

fn shell_completions(shell: CompletionShell) {
    let mut cmd = Cli::command();
    let name = cmd.get_name().to_string();
    match shell {
        CompletionShell::Zsh => generate(Zsh, &mut cmd, name, &mut io::stdout()),
        CompletionShell::Fish => generate(Fish, &mut cmd, name, &mut io::stdout()),
    }
}

#[cfg(test)]
mod cli_tests {
    use super::*;
    use clap_complete::shells::{Fish, Zsh};

    fn completion_text(shell: impl clap_complete::Generator) -> String {
        let mut cmd = Cli::command();
        let name = cmd.get_name().to_string();
        let mut out = Vec::new();
        generate(shell, &mut cmd, name, &mut out);
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn completion_command_is_exposed() {
        let cmd = Cli::command();
        assert!(
            cmd.get_subcommands()
                .any(|sc| sc.get_name() == "shell-completions")
        );
    }

    #[test]
    fn completions_include_swarm_commands() {
        let text = completion_text(Zsh);
        assert!(text.contains("export-skill"));
        assert!(text.contains("shell-completions"));
        assert!(!text.contains("--shell-completions"));
    }

    #[test]
    fn zsh_completion_mentions_swarm() {
        let text = completion_text(Zsh);
        assert!(text.contains("#compdef onlyne-swarm"));
    }

    #[test]
    fn fish_completion_mentions_swarm() {
        let text = completion_text(Fish);
        assert!(text.contains("complete -c onlyne-swarm"));
    }
}
