use anyhow::{Context, bail};
use std::path::{Path, PathBuf};

/// The swarm root is the scheduler's startup cwd. No upward search.
pub fn cwd_root(cwd: &Path) -> PathBuf {
    cwd.to_path_buf()
}

/// Refuse to start a scheduler in a nested directory: any ancestor containing
/// `_onlyne_workspaces` or `.onlyne/swarm.db` means we are inside another tree.
pub fn ensure_root(cwd: &Path, force: bool) -> anyhow::Result<PathBuf> {
    if !force {
        let mut cur = cwd.to_path_buf();
        loop {
            if cur.join("_onlyne_workspaces").is_dir() || cur.join(".onlyne/swarm.db").exists() {
                if cur != cwd {
                    bail!(
                        "nested swarm start refused: {} is inside swarm tree at {}. Run `onlyne-swarm attach` there, or start at the root, or pass --force",
                        cwd.display(),
                        cur.display()
                    );
                }
                break;
            }
            match cur.parent() {
                Some(p) => cur = p.to_path_buf(),
                None => break,
            }
        }
    }
    let onlyne = cwd.join(".onlyne");
    std::fs::create_dir_all(onlyne.join("run")).with_context(|| "create .onlyne/run")?;
    Ok(cwd.to_path_buf())
}

pub fn schedule_dir(root: &Path) -> PathBuf {
    root.join(".agents/.schedule")
}

pub fn instances_dir(root: &Path) -> PathBuf {
    root.join("_onlyne_workspaces")
}

pub fn swarm_db(root: &Path) -> PathBuf {
    root.join(".onlyne/swarm.db")
}

pub fn swarm_sock(root: &Path) -> PathBuf {
    root.join(".onlyne/run/swarm.sock")
}

/// Resolve an instance workspace dir from a tree-relative target ("a/b", "." = root itself).
pub fn resolve_instance(root: &Path, target: &str) -> PathBuf {
    if target == "." || target.is_empty() {
        root.to_path_buf()
    } else {
        instances_dir(root).join(target)
    }
}

pub fn loopback_in(ws_root: &Path) -> PathBuf {
    ws_root
        .join(".onlyne/channels/loopback/in")
}

pub fn onlyne_sock(ws_root: &Path) -> PathBuf {
    ws_root.join(".onlyne/run/onlyne.sock")
}

pub fn swarm_ws_config(ws_root: &Path) -> PathBuf {
    ws_root.join(".onlyne/swarm.workspace.jsonc")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn root_is_cwd_no_upward_search() {
        let p = PathBuf::from("/tmp/x/y");
        assert_eq!(cwd_root(&p), p);
    }

    #[test]
    fn nested_start_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        std::fs::create_dir_all(root.join("_onlyne_workspaces")).unwrap();
        let child = root.join("_onlyne_workspaces/a");
        std::fs::create_dir_all(&child).unwrap();
        let err = ensure_root(&child, false).unwrap_err();
        assert!(err.to_string().contains("nested swarm start refused"));
        assert!(ensure_root(&child, true).is_ok());
    }

}
