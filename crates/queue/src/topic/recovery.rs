use crate::model::MessageMeta;
use crate::BrokerError;
use rustqueue_storage::{PayloadRef, RecoveryMetadataRef};
use std::path::PathBuf;
use std::sync::Arc;

const MAGIC: &[u8; 4] = b"RQTM";
const VERSION: u32 = 2;
pub(super) const HEADER_LEN: usize = 12;
pub(super) const MESSAGE_LEN: usize = 60;

#[derive(Clone, Debug)]
pub(super) struct Summary {
    pub count: u64,
    pub first: MessageMeta,
    pub last: MessageMeta,
    pub min_available_at_ms: i64,
    pub max_available_at_ms: i64,
}

pub(super) fn encode<'a>(messages: impl Iterator<Item = &'a MessageMeta> + Clone) -> Vec<u8> {
    let count = messages.clone().count();
    let mut bytes = Vec::with_capacity(HEADER_LEN + count * MESSAGE_LEN);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&VERSION.to_be_bytes());
    bytes.extend_from_slice(&(count as u32).to_be_bytes());
    for message in messages {
        let start = bytes.len();
        bytes.extend_from_slice(&message.position.to_be_bytes());
        bytes.extend_from_slice(&message.id.to_be_bytes());
        bytes.extend_from_slice(&message.timestamp_ns.to_be_bytes());
        bytes.extend_from_slice(&message.available_at_ms.to_be_bytes());
        bytes.extend_from_slice(&message.log_index.to_be_bytes());
        bytes.extend_from_slice(&message.payload.offset.to_be_bytes());
        bytes.extend_from_slice(&message.payload.len.to_be_bytes());
        bytes.extend_from_slice(&message.payload.crc32c.to_be_bytes());
        bytes.extend_from_slice(&crc32c::crc32c(&bytes[start..]).to_be_bytes());
    }
    bytes
}

pub(super) fn inspect(reference: &RecoveryMetadataRef) -> Result<Summary, BrokerError> {
    let header = reference.read_range(0, HEADER_LEN)?;
    let count = decode_header(&header)?;
    if count == 0 {
        return Err(BrokerError::InvalidRecord(
            "topic recovery index is empty".into(),
        ));
    }
    let expected = HEADER_LEN as u64
        + count
            .checked_mul(MESSAGE_LEN as u64)
            .ok_or_else(|| BrokerError::InvalidRecord("topic recovery index overflow".into()))?;
    if reference.len() != expected {
        return Err(BrokerError::InvalidRecord(
            "topic recovery index length mismatch".into(),
        ));
    }
    let mut min_available_at_ms = i64::MAX;
    let mut max_available_at_ms = i64::MIN;
    let mut first = None;
    let mut last: Option<MessageMeta> = None;
    let mut ordinal = 0;
    while ordinal < count {
        let page = read_page(reference, ordinal, (count - ordinal).min(1024) as usize)?;
        for message in page {
            if last.as_ref().is_some_and(|previous| {
                message.position != previous.position.saturating_add(1) || message.id < previous.id
            }) {
                return Err(BrokerError::InvalidRecord(
                    "topic recovery index range is invalid".into(),
                ));
            }
            min_available_at_ms = min_available_at_ms.min(message.available_at_ms);
            max_available_at_ms = max_available_at_ms.max(message.available_at_ms);
            first.get_or_insert_with(|| message.clone());
            last = Some(message);
        }
        ordinal += (count - ordinal).min(1024);
    }
    Ok(Summary {
        count,
        first: first.expect("non-empty recovery index"),
        last: last.expect("non-empty recovery index"),
        min_available_at_ms,
        max_available_at_ms,
    })
}

pub(super) fn read_page(
    reference: &RecoveryMetadataRef,
    first_ordinal: u64,
    count: usize,
) -> Result<Vec<MessageMeta>, BrokerError> {
    if count == 0 {
        return Ok(Vec::new());
    }
    let offset = HEADER_LEN as u64
        + first_ordinal
            .checked_mul(MESSAGE_LEN as u64)
            .ok_or_else(|| BrokerError::InvalidRecord("topic index offset overflow".into()))?;
    let length = count
        .checked_mul(MESSAGE_LEN)
        .ok_or_else(|| BrokerError::InvalidRecord("topic index page overflow".into()))?;
    let bytes = reference.read_range(offset, length)?;
    let mut messages = Vec::with_capacity(count);
    let path = Arc::new(reference.segment_path().to_path_buf());
    for entry in bytes.chunks_exact(MESSAGE_LEN) {
        messages.push(decode_entry(
            Arc::clone(&path),
            reference.segment_len(),
            entry,
        )?);
    }
    Ok(messages)
}

fn decode_header(bytes: &[u8]) -> Result<u64, BrokerError> {
    if bytes.len() < HEADER_LEN
        || &bytes[0..4] != MAGIC
        || u32::from_be_bytes(bytes[4..8].try_into().unwrap()) != VERSION
    {
        return Err(BrokerError::InvalidRecord(
            "topic recovery index header is invalid".into(),
        ));
    }
    Ok(u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as u64)
}

fn decode_entry(
    path: Arc<PathBuf>,
    segment_len: u64,
    bytes: &[u8],
) -> Result<MessageMeta, BrokerError> {
    if bytes.len() != MESSAGE_LEN
        || crc32c::crc32c(&bytes[..MESSAGE_LEN - 4])
            != u32::from_be_bytes(bytes[MESSAGE_LEN - 4..].try_into().unwrap())
    {
        return Err(BrokerError::InvalidRecord(
            "topic recovery entry checksum mismatch".into(),
        ));
    }
    let position = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
    let id = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
    let timestamp_ns = i64::from_be_bytes(bytes[16..24].try_into().unwrap());
    let available_at_ms = i64::from_be_bytes(bytes[24..32].try_into().unwrap());
    let log_index = u64::from_be_bytes(bytes[32..40].try_into().unwrap());
    let offset = u64::from_be_bytes(bytes[40..48].try_into().unwrap());
    let len = u32::from_be_bytes(bytes[48..52].try_into().unwrap());
    let crc32c = u32::from_be_bytes(bytes[52..56].try_into().unwrap());
    if position == 0
        || id == 0
        || len == 0
        || offset
            .checked_add(len as u64)
            .is_none_or(|end| end > segment_len)
    {
        return Err(BrokerError::InvalidRecord(
            "topic recovery payload boundary is invalid".into(),
        ));
    }
    Ok(MessageMeta {
        position,
        id,
        timestamp_ns,
        available_at_ms,
        log_index,
        payload: PayloadRef {
            path,
            offset,
            len,
            crc32c,
        },
    })
}

#[cfg(test)]
mod checkpoint_tests {
    use crate::topic::{MessageIndexCache, Topic};
    use crate::{Broker, BrokerConfig};
    use std::sync::{atomic::AtomicBool, Arc};
    use std::time::Duration;
    use tempfile::tempdir;

    #[tokio::test]
    async fn legacy_dpub_checkpoints_recover_without_duplicating_shared_deadlines() {
        let root = tempdir().unwrap();
        let config = BrokerConfig {
            data_path: root.path().into(),
            ..BrokerConfig::default()
        };
        let broker = Broker::open(config.clone()).unwrap();
        broker.create_channel("events", "workers").await.unwrap();
        broker
            .publish("events", vec![b"first".to_vec()], Duration::from_secs(60))
            .await
            .unwrap();
        let ready = broker
            .publish("events", vec![b"ready".to_vec()], Duration::ZERO)
            .await
            .unwrap()[0];
        broker
            .publish("events", vec![b"last".to_vec()], Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(
            broker
                .next_message("events", "workers", None)
                .await
                .unwrap()
                .unwrap()
                .id,
            ready
        );
        broker
            .requeue("events", "workers", ready, Duration::from_secs(120))
            .await
            .unwrap();
        drop(broker);
        let directory = root.path().join("topics").join(hex::encode("events"));
        let cache = MessageIndexCache::new(
            config.message_index_cache_bytes,
            1,
            16,
            Arc::new(AtomicBool::new(true)),
        );
        let mut topic = Topic::open(
            &directory,
            config.max_segment_bytes,
            config.max_ack_gap,
            config.storage_feature_level,
            Arc::clone(&cache),
        )
        .unwrap();
        topic.recover_channels().unwrap();
        for position in [1, 3] {
            let crate::topic::index::Lookup::Found(message) = topic.messages.lookup(position)
            else {
                panic!("active metadata must be resident");
            };
            topic
                .channels
                .get_mut("workers")
                .unwrap()
                .state
                .requeued_until
                .insert(position, message.available_at_ms);
        }
        topic.spill_message_metadata().unwrap();
        topic.checkpoint_channels().unwrap();
        drop(topic);
        let mut topic = Topic::open(
            &directory,
            config.max_segment_bytes,
            config.max_ack_gap,
            config.storage_feature_level,
            cache,
        )
        .unwrap();
        topic.recover_channels().unwrap();
        let checkpoint = topic.channels["workers"].state.checkpoint();
        assert_eq!(checkpoint.requeued_until.len(), 1);
        assert!(checkpoint.requeued_until.contains_key(&2));
        assert_eq!(checkpoint.attempts.get(&2), Some(&1));
        drop(topic);
        let reopened = Broker::open(config).unwrap();
        let stats = reopened.stats().unwrap();
        let channel = &stats.topics[0].channels[0];
        assert_eq!(
            (channel.depth, channel.deferred_count, channel.requeue_count),
            (3, 3, 1)
        );
    }

    #[tokio::test]
    async fn legacy_dpub_checkpoint_gaps_still_fence_lost_positions() {
        let root = tempdir().unwrap();
        let config = BrokerConfig {
            data_path: root.path().into(),
            ..BrokerConfig::default()
        };
        let broker = Broker::open(config.clone()).unwrap();
        broker.create_channel("events", "workers").await.unwrap();
        broker
            .publish("events", vec![b"kept".to_vec()], Duration::ZERO)
            .await
            .unwrap();
        drop(broker);
        let directory = root.path().join("topics").join(hex::encode("events"));
        let cache = MessageIndexCache::new(
            config.message_index_cache_bytes,
            1,
            16,
            Arc::new(AtomicBool::new(true)),
        );
        let mut topic = Topic::open(
            &directory,
            config.max_segment_bytes,
            config.max_ack_gap,
            config.storage_feature_level,
            Arc::clone(&cache),
        )
        .unwrap();
        topic.recover_channels().unwrap();
        topic
            .channels
            .get_mut("workers")
            .unwrap()
            .state
            .requeued_until
            .insert(4, 100);
        topic.checkpoint_channels().unwrap();
        drop(topic);
        let mut topic = Topic::open(
            &directory,
            config.max_segment_bytes,
            config.max_ack_gap,
            config.storage_feature_level,
            cache,
        )
        .unwrap();
        topic.recover_channels().unwrap();
        assert_eq!(topic.next_position(), 5);
        drop(topic);
        let reopened = Broker::open(config).unwrap();
        reopened
            .publish("events", vec![b"next".to_vec()], Duration::ZERO)
            .await
            .unwrap();
        let deliveries = reopened
            .fetch_batch(
                "events",
                "workers",
                8,
                usize::MAX,
                Duration::from_secs(30),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            deliveries
                .iter()
                .map(|delivery| delivery.body.as_ref())
                .collect::<Vec<_>>(),
            vec![b"kept".as_slice(), b"next".as_slice()]
        );
    }
}
