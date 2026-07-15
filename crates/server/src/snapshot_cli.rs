use clap::{Args, Subcommand};
use rustqueue_storage::{DataDirectoryLock, SnapshotStore};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Args, Debug)]
pub struct SnapshotCommand {
    #[command(subcommand)]
    pub action: SnapshotAction,
}

#[derive(Subcommand, Debug)]
pub enum SnapshotAction {
    Export {
        #[arg(long)]
        data_path: PathBuf,
        #[arg(long)]
        snapshot_dir: PathBuf,
        #[arg(long)]
        name: String,
    },
    Verify {
        #[arg(long)]
        snapshot_dir: PathBuf,
        #[arg(long)]
        name: String,
    },
    Restore {
        #[arg(long)]
        snapshot_dir: PathBuf,
        #[arg(long)]
        name: String,
        #[arg(long)]
        target: PathBuf,
    },
}

pub fn run(command: SnapshotCommand) -> anyhow::Result<()> {
    match command.action {
        SnapshotAction::Export {
            data_path,
            snapshot_dir,
            name,
        } => {
            if !data_path.is_dir() {
                anyhow::bail!("data path {} is not a directory", data_path.display());
            }
            if snapshot_dir.starts_with(&data_path) {
                anyhow::bail!("snapshot directory must be outside the live data directory");
            }
            let _data_lock = DataDirectoryLock::acquire(&data_path)?;
            let files = collect_files(&data_path)?;
            let last_applied = read_last_applied(&data_path)?;
            let store = SnapshotStore::new(&snapshot_dir)?;
            let destination = store.export_tree(&name, last_applied, &data_path, &files)?;
            println!(
                "snapshot {} exported with {} files at {}",
                name,
                files.len(),
                destination.display()
            );
        }
        SnapshotAction::Verify { snapshot_dir, name } => {
            let manifest = SnapshotStore::new(snapshot_dir)?.verify(&name)?;
            println!(
                "snapshot {} is valid: {} files, last applied index {}",
                name,
                manifest.files.len(),
                manifest.last_applied_index
            );
        }
        SnapshotAction::Restore {
            snapshot_dir,
            name,
            target,
        } => {
            if snapshot_dir.starts_with(&target) {
                anyhow::bail!("snapshot directory must be outside the restore target");
            }
            let manifest = SnapshotStore::new(snapshot_dir)?.restore(&name, &target)?;
            println!(
                "snapshot {} restored to {} with {} files",
                name,
                target.display(),
                manifest.files.len()
            );
        }
    }
    Ok(())
}

fn collect_files(root: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut directories = vec![root.to_path_buf()];
    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with(".deleted-") || name.ends_with(".tmp") || name == ".rustqueue.lock"
            {
                continue;
            }
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                directories.push(path);
            } else if file_type.is_file() {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}

fn read_last_applied(data_path: &Path) -> std::io::Result<u64> {
    let consensus = data_path.join("consensus");
    if !consensus.is_dir() {
        return Ok(0);
    }

    let mut maximum = 0;
    let mut directories = vec![consensus];
    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                directories.push(path);
                continue;
            }
            if !file_type.is_file()
                || path.parent().and_then(Path::file_name) != Some("raft-state".as_ref())
            {
                continue;
            }
            if entry.file_name() == "applied.boundary" {
                if let Some(index) = rustqueue_consensus::read_applied_boundary_index(&path)? {
                    maximum = maximum.max(index);
                }
                continue;
            }
            if entry.file_name() == "state.json" {
                let state: serde_json::Value =
                    serde_json::from_slice(&fs::read(&path)?).map_err(std::io::Error::other)?;
                if let Some(index) = state
                    .get("last_applied")
                    .and_then(|value| value.get("index"))
                    .and_then(serde_json::Value::as_u64)
                {
                    maximum = maximum.max(index);
                }
            }
        }
    }
    Ok(maximum)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_maximum_applied_index_across_raft_groups() {
        let data = tempfile::tempdir().unwrap();
        for (group, index) in [(0, 12), (4, 29), (8, 21)] {
            let directory = data
                .path()
                .join(format!("consensus/groups/{group}/raft-state"));
            fs::create_dir_all(&directory).unwrap();
            fs::write(
                directory.join("state.json"),
                format!(r#"{{"last_applied":{{"index":{index}}}}}"#),
            )
            .unwrap();
        }
        fs::write(data.path().join("consensus/state.json"), b"not raft state").unwrap();

        assert_eq!(read_last_applied(data.path()).unwrap(), 29);
    }

    #[test]
    fn rejects_corrupt_raft_state() {
        let data = tempfile::tempdir().unwrap();
        let directory = data.path().join("consensus/groups/0/raft-state");
        fs::create_dir_all(&directory).unwrap();
        fs::write(directory.join("state.json"), b"not json").unwrap();

        assert!(read_last_applied(data.path()).is_err());
    }
}
