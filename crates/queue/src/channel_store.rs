use crate::channel::{ChannelCheckpoint, ChannelCommand, ChannelState};
use crate::metadata::store_bytes_atomic_with_failpoint;
use crate::BrokerError;
use rustqueue_protocol::validate_name;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const WAL_MAGIC: &[u8; 4] = b"RCW7";
const CHECKPOINT_MAGIC: &[u8; 4] = b"RCC7";
const HEADER_LEN: usize = 12;
const MAX_COMMAND_BYTES: usize = 1024 * 1024;
const MAX_CHECKPOINT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_RECOVERY_COMMANDS: usize = 2_048;
const CHECKPOINT_INTERVAL: usize = 1024;

pub(crate) struct ChannelStore {
    directory: PathBuf,
    checkpoint_path: PathBuf,
    wal_path: PathBuf,
    wal: File,
    commands_since_checkpoint: usize,
    isolated: bool,
}

impl ChannelStore {
    pub fn create(directory: &Path, state: &ChannelState) -> Result<Self, BrokerError> {
        fs::create_dir_all(directory)?;
        let stem = hex::encode(state.name.as_bytes());
        let checkpoint_path = directory.join(format!("{stem}.checkpoint"));
        let wal_path = directory.join(format!("{stem}.wal"));
        // A channel name may be reused after a crash interrupted deletion.
        // Make the empty WAL durable before publishing the new checkpoint so
        // stale pause/empty commands can never be replayed into the new life.
        let wal = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&wal_path)?;
        wal.sync_all()?;
        drop(wal);
        File::open(directory)?.sync_all()?;
        write_checkpoint(&checkpoint_path, &state.checkpoint())?;
        let wal = OpenOptions::new().read(true).append(true).open(&wal_path)?;
        Ok(Self {
            directory: directory.into(),
            checkpoint_path,
            wal_path,
            wal,
            commands_since_checkpoint: 0,
            isolated: false,
        })
    }

    pub fn open(
        checkpoint_path: &Path,
        max_ack_gap: usize,
    ) -> Result<(ChannelState, Self), BrokerError> {
        let checkpoint = read_checkpoint(checkpoint_path)?;
        let directory = checkpoint_path
            .parent()
            .expect("checkpoint has parent")
            .to_path_buf();
        let stem = checkpoint_path.file_stem().expect("checkpoint has stem");
        let expected_stem = hex::encode(checkpoint.name.as_bytes());
        if validate_name(&checkpoint.name).is_err() || stem.to_str() != Some(expected_stem.as_str())
        {
            return Err(BrokerError::InvalidRecord(
                "channel checkpoint name does not match its file".into(),
            ));
        }
        let wal_path = directory.join(format!("{}.wal", stem.to_string_lossy()));
        let commands = recover_wal(&wal_path)?;
        let mut state = ChannelState::from_checkpoint(checkpoint, max_ack_gap)?;
        for command in &commands {
            state.apply(command);
        }
        let wal = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&wal_path)?;
        Ok((
            state,
            Self {
                directory,
                checkpoint_path: checkpoint_path.into(),
                wal_path,
                wal,
                commands_since_checkpoint: commands.len(),
                isolated: false,
            },
        ))
    }

    pub fn append(&mut self, command: &ChannelCommand) -> Result<(), BrokerError> {
        self.append_buffered(command)?;
        rustqueue_storage::crash_failpoint("channel_after_wal_append_before_fsync");
        self.sync()?;
        rustqueue_storage::crash_failpoint("channel_after_wal_fsync_before_return");
        Ok(())
    }

    pub fn append_buffered(&mut self, command: &ChannelCommand) -> Result<(), BrokerError> {
        self.ensure_available()?;
        let body = encode_command(command);
        if body.len() > MAX_COMMAND_BYTES {
            return Err(BrokerError::InvalidRecord(
                "channel command is too large".into(),
            ));
        }
        let mut header = [0u8; HEADER_LEN];
        header[0..4].copy_from_slice(WAL_MAGIC);
        header[4..8].copy_from_slice(&(body.len() as u32).to_be_bytes());
        header[8..12].copy_from_slice(&crc32c::crc32c(&body).to_be_bytes());
        if let Err(error) = self.append_bytes(&header, &body) {
            self.isolated = true;
            return Err(error);
        }
        self.commands_since_checkpoint += 1;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<(), BrokerError> {
        self.ensure_available()?;
        if let Err(error) = self.wal.sync_data() {
            self.isolated = true;
            return Err(error.into());
        }
        Ok(())
    }

    pub fn checkpoint_if_needed(&mut self, state: &ChannelState) -> Result<(), BrokerError> {
        if self.commands_since_checkpoint < CHECKPOINT_INTERVAL {
            return Ok(());
        }
        self.checkpoint(state)
    }

    pub fn checkpoint(&mut self, state: &ChannelState) -> Result<(), BrokerError> {
        self.ensure_available()?;
        if let Err(error) = self.write_checkpoint_and_reset(state) {
            self.isolated = true;
            return Err(error);
        }
        self.commands_since_checkpoint = 0;
        Ok(())
    }

    fn append_bytes(&mut self, header: &[u8], body: &[u8]) -> Result<(), BrokerError> {
        self.wal.write_all(header)?;
        self.wal.write_all(body)?;
        Ok(())
    }

    fn write_checkpoint_and_reset(&mut self, state: &ChannelState) -> Result<(), BrokerError> {
        write_checkpoint(&self.checkpoint_path, &state.checkpoint())?;
        self.wal = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.wal_path)?;
        self.wal.sync_all()?;
        self.wal = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.wal_path)?;
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }

    fn ensure_available(&self) -> Result<(), BrokerError> {
        if self.isolated {
            Err(BrokerError::StorageUnavailable)
        } else {
            Ok(())
        }
    }

    pub fn remove(self) -> Result<(), BrokerError> {
        self.wal.sync_all()?;
        drop(self.wal);
        // Removing the WAL first means an interrupted delete can at worst
        // resurrect the checkpoint with an empty WAL, never leave stale
        // commands behind a successfully removed checkpoint.
        for path in [&self.wal_path, &self.checkpoint_path] {
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }
}

pub(crate) fn checkpoint_paths(directory: &Path) -> Result<Vec<PathBuf>, BrokerError> {
    if !directory.exists() {
        return Ok(Vec::new());
    }
    let mut paths = Vec::new();
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path
            .extension()
            .is_some_and(|extension| extension == "checkpoint")
        {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

fn write_checkpoint(path: &Path, checkpoint: &ChannelCheckpoint) -> Result<(), BrokerError> {
    let body = serde_json::to_vec(checkpoint)
        .map_err(|error| BrokerError::InvalidRecord(error.to_string()))?;
    if body.len() as u64 > MAX_CHECKPOINT_BYTES {
        return Err(BrokerError::InvalidRecord(
            "channel checkpoint exceeds the maximum size".into(),
        ));
    }
    let mut bytes = Vec::with_capacity(HEADER_LEN + body.len());
    bytes.extend_from_slice(CHECKPOINT_MAGIC);
    bytes.extend_from_slice(&(body.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&crc32c::crc32c(&body).to_be_bytes());
    bytes.extend_from_slice(&body);
    store_bytes_atomic_with_failpoint(
        path,
        &bytes,
        Some("checkpoint_after_file_fsync_before_rename"),
    )
    .map_err(Into::into)
}

fn read_checkpoint(path: &Path) -> Result<ChannelCheckpoint, BrokerError> {
    if fs::metadata(path)?.len() > MAX_CHECKPOINT_BYTES + HEADER_LEN as u64 {
        return Err(BrokerError::InvalidRecord(
            "channel checkpoint exceeds the maximum size".into(),
        ));
    }
    let bytes = fs::read(path)?;
    parse_checkpoint(&bytes)
}

fn parse_checkpoint(bytes: &[u8]) -> Result<ChannelCheckpoint, BrokerError> {
    if bytes.len() < HEADER_LEN || &bytes[0..4] != CHECKPOINT_MAGIC {
        return Err(BrokerError::InvalidRecord(
            "channel checkpoint header is invalid".into(),
        ));
    }
    let len = u32::from_be_bytes(bytes[4..8].try_into().unwrap()) as usize;
    if len as u64 > MAX_CHECKPOINT_BYTES
        || len != bytes.len() - HEADER_LEN
        || crc32c::crc32c(&bytes[HEADER_LEN..])
            != u32::from_be_bytes(bytes[8..12].try_into().unwrap())
    {
        return Err(BrokerError::InvalidRecord(
            "channel checkpoint checksum is invalid".into(),
        ));
    }
    serde_json::from_slice(&bytes[HEADER_LEN..])
        .map_err(|error| BrokerError::InvalidRecord(error.to_string()))
}

pub(crate) fn fuzz_channel_state(checkpoint: &[u8], wal: &[u8]) {
    let Ok(checkpoint) = parse_checkpoint(checkpoint) else {
        return;
    };
    let Ok(mut state) = ChannelState::from_checkpoint(checkpoint, 65_536) else {
        return;
    };
    let mut offset = 0usize;
    while wal.len().saturating_sub(offset) >= HEADER_LEN {
        let header = &wal[offset..offset + HEADER_LEN];
        if &header[0..4] != WAL_MAGIC {
            return;
        }
        let len = u32::from_be_bytes(header[4..8].try_into().unwrap()) as usize;
        if len > MAX_COMMAND_BYTES {
            return;
        }
        let Some(end) = offset
            .checked_add(HEADER_LEN)
            .and_then(|value| value.checked_add(len))
        else {
            return;
        };
        if end > wal.len() {
            return;
        }
        let body = &wal[offset + HEADER_LEN..end];
        if crc32c::crc32c(body) != u32::from_be_bytes(header[8..12].try_into().unwrap()) {
            return;
        }
        let Ok(command) = decode_command(body) else {
            return;
        };
        state.apply(&command);
        offset = end;
    }
    let _ = state.checkpoint();
}

fn recover_wal(path: &Path) -> Result<Vec<ChannelCommand>, BrokerError> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    let length = file.metadata()?.len();
    let mut offset = 0u64;
    let mut commands = Vec::new();
    while offset < length {
        if length - offset < HEADER_LEN as u64 {
            file.set_len(offset)?;
            file.sync_all()?;
            break;
        }
        file.seek(SeekFrom::Start(offset))?;
        let mut header = [0u8; HEADER_LEN];
        file.read_exact(&mut header)?;
        if &header[0..4] != WAL_MAGIC {
            return Err(BrokerError::InvalidRecord(format!(
                "channel WAL corruption at byte {offset}"
            )));
        }
        let body_len = u32::from_be_bytes(header[4..8].try_into().unwrap()) as usize;
        if body_len > MAX_COMMAND_BYTES {
            return Err(BrokerError::InvalidRecord(
                "channel WAL command is too large".into(),
            ));
        }
        let end = offset + HEADER_LEN as u64 + body_len as u64;
        if end > length {
            file.set_len(offset)?;
            file.sync_all()?;
            break;
        }
        let mut body = vec![0; body_len];
        file.read_exact(&mut body)?;
        let expected = u32::from_be_bytes(header[8..12].try_into().unwrap());
        if crc32c::crc32c(&body) != expected {
            return Err(BrokerError::InvalidRecord(format!(
                "channel WAL checksum failure at byte {offset}"
            )));
        }
        commands.push(decode_command(&body)?);
        if commands.len() > MAX_RECOVERY_COMMANDS {
            return Err(BrokerError::InvalidRecord(
                "channel WAL contains too many commands without a checkpoint".into(),
            ));
        }
        offset = end;
    }
    Ok(commands)
}

fn encode_command(command: &ChannelCommand) -> Vec<u8> {
    let mut body = Vec::with_capacity(27);
    match *command {
        ChannelCommand::Finish {
            position,
            message_id,
        } => {
            body.push(1);
            body.extend_from_slice(&position.to_be_bytes());
            body.extend_from_slice(&message_id.to_be_bytes());
        }
        ChannelCommand::Requeue {
            position,
            message_id,
            available_at_ms,
            attempts,
        } => {
            body.push(2);
            body.extend_from_slice(&position.to_be_bytes());
            body.extend_from_slice(&message_id.to_be_bytes());
            body.extend_from_slice(&available_at_ms.to_be_bytes());
            body.extend_from_slice(&attempts.to_be_bytes());
        }
        ChannelCommand::Pause { paused } => {
            body.extend_from_slice(&[3, paused as u8]);
        }
        ChannelCommand::Empty { through_position } => {
            body.push(4);
            body.extend_from_slice(&through_position.to_be_bytes());
        }
        ChannelCommand::Evict { through_position } => {
            body.push(5);
            body.extend_from_slice(&through_position.to_be_bytes());
        }
    }
    body
}

fn decode_command(body: &[u8]) -> Result<ChannelCommand, BrokerError> {
    let invalid = || BrokerError::InvalidRecord("channel WAL command is invalid".into());
    match body.first().copied() {
        Some(1) if body.len() == 17 => Ok(ChannelCommand::Finish {
            position: u64::from_be_bytes(body[1..9].try_into().unwrap()),
            message_id: u64::from_be_bytes(body[9..17].try_into().unwrap()),
        }),
        Some(2) if body.len() == 27 => Ok(ChannelCommand::Requeue {
            position: u64::from_be_bytes(body[1..9].try_into().unwrap()),
            message_id: u64::from_be_bytes(body[9..17].try_into().unwrap()),
            available_at_ms: i64::from_be_bytes(body[17..25].try_into().unwrap()),
            attempts: u16::from_be_bytes(body[25..27].try_into().unwrap()),
        }),
        Some(3) if body.len() == 2 && body[1] <= 1 => Ok(ChannelCommand::Pause {
            paused: body[1] == 1,
        }),
        Some(4) if body.len() == 9 => Ok(ChannelCommand::Empty {
            through_position: u64::from_be_bytes(body[1..9].try_into().unwrap()),
        }),
        Some(5) if body.len() == 9 => Ok(ChannelCommand::Evict {
            through_position: u64::from_be_bytes(body[1..9].try_into().unwrap()),
        }),
        _ => Err(invalid()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn recovers_durable_finish_and_truncates_a_partial_tail() {
        let root = tempdir().unwrap();
        let state = ChannelState::new("workers".into(), 0, false, 65_536);
        let mut store = ChannelStore::create(root.path(), &state).unwrap();
        store
            .append(&ChannelCommand::Finish {
                position: 1,
                message_id: 7,
            })
            .unwrap();
        drop(store);
        let wal = root.path().join(format!("{}.wal", hex::encode("workers")));
        OpenOptions::new()
            .append(true)
            .open(&wal)
            .unwrap()
            .write_all(b"torn")
            .unwrap();
        let checkpoint = root
            .path()
            .join(format!("{}.checkpoint", hex::encode("workers")));
        let (state, _) = ChannelStore::open(&checkpoint, 65_536).unwrap();
        assert_eq!(state.ack_floor_position, 1);
        assert_eq!(fs::metadata(wal).unwrap().len(), (HEADER_LEN + 17) as u64);
    }

    #[test]
    fn refuses_a_checksum_corrupt_complete_wal_tail() {
        let root = tempdir().unwrap();
        let state = ChannelState::new("workers".into(), 0, false, 65_536);
        let mut store = ChannelStore::create(root.path(), &state).unwrap();
        store
            .append(&ChannelCommand::Finish {
                position: 1,
                message_id: 7,
            })
            .unwrap();
        drop(store);
        let wal = root.path().join(format!("{}.wal", hex::encode("workers")));
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&wal)
            .unwrap();
        file.seek(SeekFrom::Start(HEADER_LEN as u64)).unwrap();
        file.write_all(b"X").unwrap();
        file.sync_all().unwrap();
        drop(file);
        let checkpoint = root
            .path()
            .join(format!("{}.checkpoint", hex::encode("workers")));
        assert!(matches!(
            ChannelStore::open(&checkpoint, 65_536),
            Err(BrokerError::InvalidRecord(message)) if message.contains("checksum")
        ));
    }

    #[test]
    fn command_decoder_rejects_trailing_bytes() {
        let command = ChannelCommand::Finish {
            position: 1,
            message_id: 7,
        };
        let mut encoded = encode_command(&command);
        encoded.push(0);
        assert!(decode_command(&encoded).is_err());
    }

    #[test]
    fn recreating_a_channel_discards_an_orphaned_old_wal() {
        let root = tempdir().unwrap();
        let original = ChannelState::new("workers".into(), 0, false, 65_536);
        let mut store = ChannelStore::create(root.path(), &original).unwrap();
        store
            .append(&ChannelCommand::Pause { paused: true })
            .unwrap();
        drop(store);

        let checkpoint = root
            .path()
            .join(format!("{}.checkpoint", hex::encode("workers")));
        fs::remove_file(&checkpoint).unwrap();
        File::open(root.path()).unwrap().sync_all().unwrap();

        let replacement = ChannelState::new("workers".into(), 10, false, 65_536);
        drop(ChannelStore::create(root.path(), &replacement).unwrap());
        let (recovered, _) = ChannelStore::open(&checkpoint, 65_536).unwrap();
        assert!(!recovered.paused);
        assert_eq!(recovered.ack_floor_position, 10);
    }

    #[test]
    fn rejects_a_checkpoint_renamed_to_another_channel_identity() {
        let root = tempdir().unwrap();
        let state = ChannelState::new("workers".into(), 0, false, 65_536);
        drop(ChannelStore::create(root.path(), &state).unwrap());
        let original = root
            .path()
            .join(format!("{}.checkpoint", hex::encode("workers")));
        let renamed = root
            .path()
            .join(format!("{}.checkpoint", hex::encode("other")));
        fs::rename(original, &renamed).unwrap();

        assert!(matches!(
            ChannelStore::open(&renamed, 65_536),
            Err(BrokerError::InvalidRecord(message)) if message.contains("does not match")
        ));
    }
}
