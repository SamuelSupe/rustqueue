use super::{GlobalGroupId, PartitionHome};
use serde::{de::Error, Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;

pub fn serialize<S>(
    partitions: &BTreeMap<GlobalGroupId, PartitionHome>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    partitions
        .values()
        .collect::<Vec<_>>()
        .serialize(serializer)
}

pub fn deserialize<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<GlobalGroupId, PartitionHome>, D::Error>
where
    D: Deserializer<'de>,
{
    let mut partitions = BTreeMap::new();
    for partition in Vec::<PartitionHome>::deserialize(deserializer)? {
        if partitions.insert(partition.id, partition).is_some() {
            return Err(D::Error::custom("duplicate global partition ID"));
        }
    }
    Ok(partitions)
}
