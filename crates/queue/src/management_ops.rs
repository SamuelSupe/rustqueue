use crate::management::ManagementResult;
use crate::metadata::{load_optional, store_atomic};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::path::{Path, PathBuf};

const FORMAT: u8 = 1;
const MAX_COMPLETED: usize = 8_192;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum OperationState {
    Pending,
    Completed,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct OperationRecord {
    fingerprint: String,
    topic: String,
    state: OperationState,
    result: Option<ManagementResult>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct OperationCatalog {
    format: u8,
    #[serde(default)]
    records: BTreeMap<String, OperationRecord>,
    #[serde(default)]
    completed_order: VecDeque<String>,
}

pub(crate) enum OperationLookup {
    New,
    Pending,
    Completed(ManagementResult),
}

impl Default for OperationCatalog {
    fn default() -> Self {
        Self {
            format: FORMAT,
            records: BTreeMap::new(),
            completed_order: VecDeque::new(),
        }
    }
}

impl OperationCatalog {
    pub(crate) fn load(data_path: &Path) -> io::Result<(PathBuf, Self)> {
        let path = data_path.join("management-operations.json");
        let catalog = load_optional::<Self>(&path)?.unwrap_or_default();
        if catalog.format != FORMAT
            || catalog
                .records
                .iter()
                .any(|(id, record)| !valid_id(id) || record.topic.is_empty())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid management operation catalog",
            ));
        }
        Ok((path, catalog))
    }

    pub(crate) fn lookup(&self, id: &str, fingerprint: &str) -> io::Result<OperationLookup> {
        validate_id(id)?;
        let Some(record) = self.records.get(id) else {
            return Ok(OperationLookup::New);
        };
        if record.fingerprint != fingerprint {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "management operation ID was reused for a different action",
            ));
        }
        Ok(match (&record.state, &record.result) {
            (OperationState::Pending, _) => OperationLookup::Pending,
            (OperationState::Completed, Some(result)) => OperationLookup::Completed(result.clone()),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "completed management operation has no result",
                ))
            }
        })
    }

    pub(crate) fn prepare(
        &mut self,
        path: &Path,
        id: &str,
        fingerprint: String,
        topic: String,
    ) -> io::Result<()> {
        validate_id(id)?;
        let mut next = self.clone();
        next.records.insert(
            id.to_owned(),
            OperationRecord {
                fingerprint,
                topic,
                state: OperationState::Pending,
                result: None,
            },
        );
        store_atomic(path, &next)?;
        *self = next;
        Ok(())
    }

    pub(crate) fn complete(
        &mut self,
        path: &Path,
        id: &str,
        result: ManagementResult,
    ) -> io::Result<()> {
        let mut next = self.clone();
        let record = next.records.get_mut(id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "management operation disappeared")
        })?;
        record.state = OperationState::Completed;
        record.result = Some(result);
        next.completed_order.push_back(id.to_owned());
        while next.completed_order.len() > MAX_COMPLETED {
            if let Some(expired) = next.completed_order.pop_front() {
                if next
                    .records
                    .get(&expired)
                    .is_some_and(|record| record.state == OperationState::Completed)
                {
                    next.records.remove(&expired);
                }
            }
        }
        store_atomic(path, &next)?;
        *self = next;
        Ok(())
    }

    pub(crate) fn blocks_topic(&self, topic: &str) -> bool {
        self.records
            .values()
            .any(|record| record.topic == topic && record.state == OperationState::Pending)
    }
}

fn validate_id(id: &str) -> io::Result<()> {
    if valid_id(id) {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "management operation ID must be 16..=128 safe ASCII characters",
        ))
    }
}

fn valid_id(id: &str) -> bool {
    (16..=128).contains(&id.len())
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn pending_operations_survive_restart_and_completed_entries_do_not_block() {
        let root = tempdir().unwrap();
        let (path, mut catalog) = OperationCatalog::load(root.path()).unwrap();
        catalog
            .prepare(
                &path,
                "operation-00000001",
                "topic:create:orders".into(),
                "orders".into(),
            )
            .unwrap();
        let (_, mut catalog) = OperationCatalog::load(root.path()).unwrap();
        assert!(catalog.blocks_topic("orders"));
        catalog
            .complete(
                &path,
                "operation-00000001",
                ManagementResult {
                    revision: 2,
                    changed: true,
                },
            )
            .unwrap();
        assert!(!catalog.blocks_topic("orders"));
    }
}
