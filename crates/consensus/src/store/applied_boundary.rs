use super::*;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};

const MAGIC: &[u8; 8] = b"RQAPB001";
const HEADER_BYTES: usize = 16;
const SLOT_BYTES: usize = 40;
const FILE_BYTES: usize = HEADER_BYTES + SLOT_BYTES * 2;

pub(super) fn read_applied_state(path: &Path) -> io::Result<Option<LogId<NodeId>>> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut bytes = vec![0; FILE_BYTES];
    file.read_exact(&mut bytes)?;
    validate_header(&bytes)?;
    Ok((0..2)
        .filter_map(|slot| decode_slot(&bytes[slot_offset(slot)..slot_offset(slot) + SLOT_BYTES]))
        .max_by_key(|(generation, _)| *generation)
        .map(|(_, log_id)| log_id))
}

pub(super) fn write_applied_state(path: &Path, state: &StateMachineData) -> io::Result<()> {
    let Some(log_id) = state.last_applied else {
        return Ok(());
    };
    let parent = path.parent().expect("applied boundary has parent");
    fs::create_dir_all(parent)?;
    let existed = path.exists();
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    if !existed || file.metadata()?.len() != FILE_BYTES as u64 {
        let mut initial = vec![0; FILE_BYTES];
        initial[..MAGIC.len()].copy_from_slice(MAGIC);
        initial[8..12].copy_from_slice(&1u32.to_le_bytes());
        file.set_len(0)?;
        file.write_all(&initial)?;
        file.sync_all()?;
        File::open(parent)?.sync_all()?;
    }

    file.seek(SeekFrom::Start(0))?;
    let mut bytes = vec![0; FILE_BYTES];
    file.read_exact(&mut bytes)?;
    validate_header(&bytes)?;
    let generation = (0..2)
        .filter_map(|slot| decode_slot(&bytes[slot_offset(slot)..slot_offset(slot) + SLOT_BYTES]))
        .map(|(generation, _)| generation)
        .max()
        .unwrap_or(0)
        .wrapping_add(1)
        .max(1);
    let slot = (generation as usize) & 1;
    let encoded = encode_slot(generation, log_id);
    file.seek(SeekFrom::Start(slot_offset(slot) as u64))?;
    file.write_all(&encoded)?;
    file.sync_data()
}

fn slot_offset(slot: usize) -> usize {
    HEADER_BYTES + slot * SLOT_BYTES
}

fn validate_header(bytes: &[u8]) -> io::Result<()> {
    if bytes.len() != FILE_BYTES
        || &bytes[..MAGIC.len()] != MAGIC
        || u32::from_le_bytes(bytes[8..12].try_into().unwrap()) != 1
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid applied-boundary file",
        ));
    }
    Ok(())
}

fn encode_slot(generation: u64, log_id: LogId<NodeId>) -> [u8; SLOT_BYTES] {
    let mut slot = [0; SLOT_BYTES];
    slot[..8].copy_from_slice(&generation.to_le_bytes());
    slot[8..16].copy_from_slice(&log_id.leader_id.term.to_le_bytes());
    slot[16..24].copy_from_slice(&log_id.leader_id.node_id.to_le_bytes());
    slot[24..32].copy_from_slice(&log_id.index.to_le_bytes());
    let checksum = crc32c::crc32c(&slot[..32]);
    slot[32..36].copy_from_slice(&checksum.to_le_bytes());
    slot
}

fn decode_slot(slot: &[u8]) -> Option<(u64, LogId<NodeId>)> {
    let generation = u64::from_le_bytes(slot.get(..8)?.try_into().ok()?);
    if generation == 0 {
        return None;
    }
    let expected = u32::from_le_bytes(slot.get(32..36)?.try_into().ok()?);
    if crc32c::crc32c(slot.get(..32)?) != expected {
        return None;
    }
    let term = u64::from_le_bytes(slot.get(8..16)?.try_into().ok()?);
    let node_id = u64::from_le_bytes(slot.get(16..24)?.try_into().ok()?);
    let index = u64::from_le_bytes(slot.get(24..32)?.try_into().ok()?);
    Some((
        generation,
        LogId::new(openraft::CommittedLeaderId::new(term, node_id), index),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn falls_back_to_the_previous_slot_after_a_torn_write() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("applied.boundary");
        let mut state = StateMachineData::default();
        let first = LogId::new(openraft::CommittedLeaderId::new(2, 1), 7);
        state.last_applied = Some(first);
        write_applied_state(&path, &state).unwrap();
        let second = LogId::new(openraft::CommittedLeaderId::new(2, 1), 8);
        state.last_applied = Some(second);
        write_applied_state(&path, &state).unwrap();
        assert_eq!(read_applied_state(&path).unwrap(), Some(second));

        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(slot_offset(0) as u64 + 8))
            .unwrap();
        file.write_all(b"torn").unwrap();
        file.sync_all().unwrap();
        assert_eq!(read_applied_state(&path).unwrap(), Some(first));
    }
}
