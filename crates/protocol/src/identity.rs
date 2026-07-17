use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

#[derive(
    Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[serde(transparent)]
pub struct CellId(pub u64);

impl CellId {
    pub const BOOTSTRAP: Self = Self(1);

    pub fn new(value: u64) -> Result<Self, &'static str> {
        (value != 0)
            .then_some(Self(value))
            .ok_or("cell ID must be non-zero")
    }
}

impl fmt::Display for CellId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(
    Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
pub struct GlobalGroupId {
    pub cell: CellId,
    pub local: u64,
}

/// A collision-free address for every Raft group in a federation.
///
/// Partition IDs keep the Cell in which they were created even after their
/// mutable Home Cell changes. Control-plane groups use explicit variants so
/// they never rely on sentinel local IDs such as `0` or `u64::MAX`.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GroupKey {
    Root,
    Catalog { shard: u64 },
    CellMetadata { cell: CellId },
    Partition(GlobalGroupId),
}

impl GroupKey {
    pub fn catalog(shard: u64) -> Result<Self, &'static str> {
        (shard != 0)
            .then_some(Self::Catalog { shard })
            .ok_or("catalog shard ID must be non-zero")
    }

    pub fn cell_metadata(cell: CellId) -> Self {
        Self::CellMetadata { cell }
    }

    pub fn partition(cell: CellId, local: u64) -> Result<Self, &'static str> {
        GlobalGroupId::new(cell, local).map(Self::Partition)
    }

    pub fn partition_id(self) -> Option<GlobalGroupId> {
        match self {
            Self::Partition(id) => Some(id),
            _ => None,
        }
    }

    pub fn storage_component(self) -> String {
        self.to_string()
    }
}

impl fmt::Display for GroupKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Root => formatter.write_str("root"),
            Self::Catalog { shard } => write!(formatter, "catalog-{shard}"),
            Self::CellMetadata { cell } => write!(formatter, "cell-{cell}-meta"),
            Self::Partition(id) => write!(formatter, "partition-{}-{}", id.cell, id.local),
        }
    }
}

impl FromStr for GroupKey {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value == "root" {
            return Ok(Self::Root);
        }
        if let Some(shard) = value.strip_prefix("catalog-") {
            return Self::catalog(parse_nonzero(shard, "invalid catalog group key")?);
        }
        if let Some(cell) = value
            .strip_prefix("cell-")
            .and_then(|value| value.strip_suffix("-meta"))
        {
            return Ok(Self::cell_metadata(CellId::new(parse_nonzero(
                cell,
                "invalid Cell metadata group key",
            )?)?));
        }
        if let Some(value) = value.strip_prefix("partition-") {
            let (cell, local) = value.split_once('-').ok_or("invalid partition group key")?;
            return Self::partition(
                CellId::new(parse_nonzero(cell, "invalid partition Cell ID")?)?,
                parse_nonzero(local, "invalid partition local group ID")?,
            );
        }
        Err("unknown group key")
    }
}

fn parse_nonzero(value: &str, error: &'static str) -> Result<u64, &'static str> {
    value
        .parse::<u64>()
        .ok()
        .filter(|value| *value != 0)
        .ok_or(error)
}

impl GlobalGroupId {
    pub fn new(cell: CellId, local: u64) -> Result<Self, &'static str> {
        if local == 0 {
            return Err("local group ID must be non-zero");
        }
        Ok(Self { cell, local })
    }
}

impl fmt::Display for GlobalGroupId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.cell, self.local)
    }
}

#[derive(
    Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
pub struct InternalMessageId {
    pub group: GlobalGroupId,
    pub log_index: u64,
    pub ordinal: u32,
    pub incarnation: u32,
}

impl InternalMessageId {
    pub fn new(
        group: GlobalGroupId,
        log_index: u64,
        ordinal: u32,
        incarnation: u32,
    ) -> Result<Self, &'static str> {
        if log_index == 0 || incarnation == 0 {
            return Err("message log index and incarnation must be non-zero");
        }
        Ok(Self {
            group,
            log_index,
            ordinal,
            incarnation,
        })
    }

    pub fn to_be_bytes(self) -> [u8; 32] {
        let mut bytes = [0_u8; 32];
        bytes[0..8].copy_from_slice(&self.group.cell.0.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.group.local.to_be_bytes());
        bytes[16..24].copy_from_slice(&self.log_index.to_be_bytes());
        bytes[24..28].copy_from_slice(&self.ordinal.to_be_bytes());
        bytes[28..32].copy_from_slice(&self.incarnation.to_be_bytes());
        bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_ids_are_cell_and_incarnation_scoped() {
        let group = GlobalGroupId::new(CellId::new(7).unwrap(), 42).unwrap();
        let first = InternalMessageId::new(group, 9, 1, 1).unwrap();
        let reused_wire_slot = InternalMessageId::new(group, 9, 1, 2).unwrap();
        assert_ne!(first, reused_wire_slot);
        assert_ne!(first.to_be_bytes(), reused_wire_slot.to_be_bytes());
    }

    #[test]
    fn group_keys_are_stable_path_components_and_round_trip() {
        let keys = [
            GroupKey::Root,
            GroupKey::catalog(9).unwrap(),
            GroupKey::cell_metadata(CellId(7)),
            GroupKey::partition(CellId(7), 42).unwrap(),
        ];
        for key in keys {
            let encoded = key.storage_component();
            assert!(!encoded.contains('/'));
            assert_eq!(encoded.parse::<GroupKey>().unwrap(), key);
        }
        assert!("partition-1-0".parse::<GroupKey>().is_err());
        assert!("cell-0-meta".parse::<GroupKey>().is_err());
    }
}
