use super::index_cache::PageKey;
pub(crate) use super::index_cache::{MessageIndexCache, MetadataReservation, PageRequest};
use super::recovery;
use crate::model::MessageMeta;
use crate::BrokerError;
use rustqueue_storage::RecoveryMetadataRef;
use std::collections::{BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const PAGE_MESSAGES: u64 = 1024;

pub(crate) enum Lookup {
    Found(MessageMeta),
    Load(PageRequest),
    Absent,
}

#[derive(Clone, Debug)]
struct SealedMessages {
    metadata: RecoveryMetadataRef,
    count: u64,
    first_position: u64,
    last_position: u64,
    first_id: u64,
    last_id: u64,
    first_timestamp_ns: i64,
    last_timestamp_ns: i64,
    last_log_index: u64,
    min_available_at_ms: i64,
    max_available_at_ms: i64,
}

pub(crate) struct MessageIndex {
    sealed: VecDeque<SealedMessages>,
    active: VecDeque<MessageMeta>,
    total_count: u64,
    cache: Arc<MessageIndexCache>,
}

impl MessageIndex {
    pub(crate) fn new(cache: Arc<MessageIndexCache>) -> Self {
        Self {
            sealed: VecDeque::new(),
            active: VecDeque::new(),
            total_count: 0,
            cache,
        }
    }

    pub(crate) fn recover_sealed(
        &mut self,
        metadata: RecoveryMetadataRef,
    ) -> Result<(), BrokerError> {
        let summary = recovery::inspect(&metadata)?;
        self.push_sealed(SealedMessages::from_summary(metadata, summary))
    }

    pub(crate) fn recover_active(&mut self, messages: Vec<MessageMeta>) -> Result<(), BrokerError> {
        let count = messages.len();
        self.extend_active(messages)?;
        self.cache.add_recovered_active(count);
        Ok(())
    }

    pub(crate) fn append(
        &mut self,
        messages: Vec<MessageMeta>,
        reservation: &mut MetadataReservation,
    ) -> Result<(), BrokerError> {
        let count = messages.len();
        self.extend_active(messages)?;
        reservation.consume(count);
        Ok(())
    }

    pub(crate) fn seal_path(
        &mut self,
        path: &Path,
        metadata: RecoveryMetadataRef,
    ) -> Result<(), BrokerError> {
        let count = self
            .active
            .iter()
            .filter(|message| message.payload.path.as_ref() == path)
            .count();
        let first = self
            .active
            .iter()
            .find(|message| message.payload.path.as_ref() == path)
            .cloned()
            .ok_or_else(|| {
                BrokerError::InvalidRecord("sealed segment has no message metadata".into())
            })?;
        let last = self
            .active
            .iter()
            .rev()
            .find(|message| message.payload.path.as_ref() == path)
            .cloned()
            .expect("non-empty metadata");
        let (min_available_at_ms, max_available_at_ms) =
            self.active_for_path(path)
                .fold((i64::MAX, i64::MIN), |(min, max), message| {
                    (
                        min.min(message.available_at_ms),
                        max.max(message.available_at_ms),
                    )
                });
        self.active
            .retain(|message| message.payload.path.as_ref() != path);
        // Cold topics must not retain uncharged buffers after their metadata spills.
        self.active.shrink_to_fit();
        self.cache.release_active(count);
        self.total_count = self.total_count.saturating_sub(count as u64);
        self.push_sealed(SealedMessages {
            metadata,
            count: count as u64,
            first_position: first.position,
            last_position: last.position,
            first_id: first.id,
            last_id: last.id,
            first_timestamp_ns: first.timestamp_ns,
            last_timestamp_ns: last.timestamp_ns,
            last_log_index: last.log_index,
            min_available_at_ms,
            max_available_at_ms,
        })
    }

    pub(crate) fn lookup(&self, position: u64) -> Lookup {
        if let Some(first) = self.active.front() {
            if position >= first.position {
                let offset = position.saturating_sub(first.position) as usize;
                return self
                    .active
                    .get(offset)
                    .filter(|message| message.position == position)
                    .cloned()
                    .map_or(Lookup::Absent, Lookup::Found);
            }
        }
        let index = self
            .sealed
            .partition_point(|segment| segment.last_position < position);
        let Some(segment) = self
            .sealed
            .get(index)
            .filter(|segment| position >= segment.first_position)
        else {
            return Lookup::Absent;
        };
        let ordinal = position - segment.first_position;
        let page = ordinal / PAGE_MESSAGES;
        let first_ordinal = page * PAGE_MESSAGES;
        let count = (segment.count - first_ordinal).min(PAGE_MESSAGES) as usize;
        let key = PageKey {
            segment: segment.metadata.segment_path().to_path_buf(),
            page,
        };
        let offset = (ordinal - first_ordinal) as usize;
        if let Some(message) = self.cache.get(&key, offset) {
            return if message.position == position {
                Lookup::Found(message)
            } else {
                Lookup::Absent
            };
        }
        Lookup::Load(PageRequest {
            key,
            metadata: segment.metadata.clone(),
            first_ordinal,
            count,
        })
    }

    pub(crate) fn position_by_id(&self, id: u64) -> Result<Option<u64>, BrokerError> {
        if let Some(message) = self.active.iter().find(|message| message.id == id) {
            return Ok(Some(message.position));
        }
        let index = self.sealed.partition_point(|segment| segment.last_id < id);
        let Some(segment) = self
            .sealed
            .get(index)
            .filter(|segment| id >= segment.first_id)
        else {
            return Ok(None);
        };
        let mut low = 0u64;
        let mut high = segment.count;
        while low < high {
            let ordinal = low + (high - low) / 2;
            let message = self.load_ordinal(segment, ordinal)?;
            if message.id < id {
                low = ordinal + 1;
            } else {
                high = ordinal;
            }
        }
        if low >= segment.count {
            return Ok(None);
        }
        let message = self.load_ordinal(segment, low)?;
        Ok((message.id == id).then_some(message.position))
    }

    pub(crate) fn total_count(&self) -> u64 {
        self.total_count
    }

    pub(crate) fn for_each_deferred_range(
        &self,
        now_ms: i64,
        last_position: u64,
        mut visit: impl FnMut(u64, u64),
    ) -> Result<(), BrokerError> {
        for segment in &self.sealed {
            if segment.first_position > last_position {
                break;
            }
            if segment.max_available_at_ms <= now_ms {
                continue;
            }
            let last = segment.last_position.min(last_position);
            if segment.min_available_at_ms > now_ms {
                visit(segment.first_position, last);
                continue;
            }
            // Mixed deadlines require one bounded page, never a backlog-sized set.
            let count = last - segment.first_position + 1;
            let mut ordinal = 0;
            while ordinal < count {
                let length = (count - ordinal).min(PAGE_MESSAGES) as usize;
                let page = recovery::read_page(&segment.metadata, ordinal, length)?;
                visit_deferred_ranges(page.iter(), now_ms, &mut visit);
                ordinal += length as u64;
            }
        }
        visit_deferred_ranges(
            self.active
                .iter()
                .take_while(|message| message.position <= last_position),
            now_ms,
            &mut visit,
        );
        Ok(())
    }

    pub(crate) fn active_count(&self) -> usize {
        self.active.len()
    }

    #[cfg(test)]
    pub(crate) fn active_metadata_allocated_bytes(&self) -> usize {
        self.active.capacity() * std::mem::size_of::<MessageMeta>()
    }

    pub(crate) fn active_last_position(&self) -> Option<u64> {
        self.active.back().map(|message| message.position)
    }

    #[cfg(test)]
    pub(crate) fn sealed_count(&self) -> usize {
        self.sealed.len()
    }

    pub(crate) fn last_position(&self) -> Option<u64> {
        self.active
            .back()
            .map(|message| message.position)
            .or_else(|| self.sealed.back().map(|segment| segment.last_position))
    }

    pub(crate) fn first_position(&self) -> Option<u64> {
        self.sealed
            .front()
            .map(|segment| segment.first_position)
            .or_else(|| self.active.front().map(|message| message.position))
    }

    pub(crate) fn position_gaps(&self, last_position: u64) -> Vec<(u64, u64)> {
        let mut gaps = Vec::new();
        let mut next = 1u64;
        let active = self
            .active
            .front()
            .zip(self.active.back())
            .map(|(first, last)| (first.position, last.position));
        for (first, last) in self
            .sealed
            .iter()
            .map(|segment| (segment.first_position, segment.last_position))
            .chain(active)
        {
            if first > last_position {
                break;
            }
            if next < first {
                gaps.push((next, first.saturating_sub(1).min(last_position)));
            }
            let Some(after) = last.checked_add(1) else {
                return gaps;
            };
            next = after;
        }
        if next <= last_position {
            gaps.push((next, last_position));
        }
        gaps
    }

    pub(crate) fn last_timestamp_ns(&self) -> Option<i64> {
        self.active
            .back()
            .map(|message| message.timestamp_ns)
            .or_else(|| self.sealed.back().map(|segment| segment.last_timestamp_ns))
    }

    pub(crate) fn oldest_timestamp_ns(&self) -> Option<i64> {
        self.sealed
            .front()
            .map(|segment| segment.first_timestamp_ns)
            .or_else(|| self.active.front().map(|message| message.timestamp_ns))
    }

    pub(crate) fn first_message_after(
        &self,
        position: u64,
    ) -> Result<Option<(u64, i64)>, BrokerError> {
        let sealed_index = self
            .sealed
            .partition_point(|segment| segment.last_position <= position);
        if let Some(segment) = self.sealed.get(sealed_index) {
            let ordinal = position
                .saturating_add(1)
                .saturating_sub(segment.first_position)
                .min(segment.count.saturating_sub(1));
            let message = self.load_ordinal(segment, ordinal)?;
            return Ok(Some((message.position, message.timestamp_ns)));
        }
        Ok(self
            .active
            .iter()
            .find(|message| message.position > position)
            .map(|message| (message.position, message.timestamp_ns)))
    }

    pub(crate) fn through_timestamp_after(
        &self,
        position: u64,
        cutoff_ns: i64,
        max_messages: u64,
    ) -> Result<Option<(u64, u64)>, BrokerError> {
        let mut through = None;
        let mut messages = 0u64;
        let sealed_index = self
            .sealed
            .partition_point(|segment| segment.last_position <= position);
        for segment in self.sealed.iter().skip(sealed_index) {
            let first_ordinal = position
                .saturating_add(1)
                .saturating_sub(segment.first_position)
                .min(segment.count);
            if first_ordinal >= segment.count || segment.first_timestamp_ns > cutoff_ns {
                continue;
            }
            if segment.last_timestamp_ns <= cutoff_ns {
                let expired = segment.count - first_ordinal;
                let take = expired.min(max_messages.saturating_sub(messages));
                if take == 0 {
                    break;
                }
                through = if take == expired {
                    Some(segment.last_position)
                } else {
                    Some(
                        self.load_ordinal(segment, first_ordinal + take - 1)?
                            .position,
                    )
                };
                messages = messages.saturating_add(take);
                if take < expired || messages >= max_messages {
                    return Ok(through.map(|through| (through, messages)));
                }
                continue;
            }
            let mut low = first_ordinal;
            let mut high = segment.count;
            while low < high {
                let ordinal = low + (high - low) / 2;
                if self.load_ordinal(segment, ordinal)?.timestamp_ns <= cutoff_ns {
                    low = ordinal + 1;
                } else {
                    high = ordinal;
                }
            }
            if low > first_ordinal {
                let take = (low - first_ordinal).min(max_messages.saturating_sub(messages));
                let last = self.load_ordinal(segment, first_ordinal + take - 1)?;
                through = Some(last.position);
                messages = messages.saturating_add(take);
            }
            return Ok(through.map(|through| (through, messages)));
        }
        let first_active = self
            .active
            .partition_point(|message| message.position <= position);
        let end = self
            .active
            .partition_point(|message| message.timestamp_ns <= cutoff_ns);
        let take = ((end - first_active) as u64).min(max_messages.saturating_sub(messages));
        if take > 0 {
            through = self
                .active
                .get(first_active + take as usize - 1)
                .map(|message| message.position);
            messages = messages.saturating_add(take);
        }
        Ok(through.map(|through| (through, messages)))
    }

    pub(crate) fn count_after_position(&self, position: u64) -> u64 {
        let sealed = self
            .sealed
            .iter()
            .map(|segment| {
                if position < segment.first_position {
                    segment.count
                } else if position < segment.last_position {
                    segment.last_position.saturating_sub(position)
                } else {
                    0
                }
            })
            .sum::<u64>();
        sealed.saturating_add(
            self.active
                .iter()
                .filter(|message| message.position > position)
                .count() as u64,
        )
    }

    /// Returns a conservative position at or before the first message newer
    /// than the cutoff. At most one sealed segment is retained extra.
    pub(crate) fn retain_from_timestamp(&self, cutoff: i64, next_position: u64) -> u64 {
        let index = self
            .sealed
            .partition_point(|segment| segment.last_timestamp_ns < cutoff);
        if let Some(segment) = self.sealed.get(index) {
            return segment.first_position;
        }
        let index = self
            .active
            .partition_point(|message| message.timestamp_ns < cutoff);
        self.active
            .get(index)
            .map_or(next_position, |message| message.position)
    }

    pub(crate) fn retain_from_ids(&self, ids: &BTreeSet<u64>, next_position: u64) -> u64 {
        ids.iter()
            .filter_map(|id| {
                let index = self.sealed.partition_point(|segment| segment.last_id < *id);
                self.sealed
                    .get(index)
                    .map(|segment| segment.first_position)
                    .or_else(|| {
                        self.active
                            .iter()
                            .find(|message| message.id == *id)
                            .map(|message| message.position)
                    })
            })
            .min()
            .unwrap_or(next_position)
    }

    pub(crate) fn purge_through_log_index(&self, retain_from: u64) -> Option<u64> {
        self.sealed
            .iter()
            .take_while(|segment| segment.last_position < retain_from)
            .last()
            .map(|segment| segment.last_log_index)
    }

    pub(crate) fn first_purge_path(&self, retain_from: u64) -> Option<&Path> {
        self.sealed
            .front()
            .filter(|segment| segment.last_position < retain_from)
            .map(|segment| segment.metadata.segment_path())
    }

    pub(crate) fn eviction_range(&self, through_index: u64) -> Option<(u64, u64, u64)> {
        let mut segments = self
            .sealed
            .iter()
            .take_while(|segment| segment.last_log_index <= through_index);
        let first = segments.next()?;
        let mut last = first;
        for segment in segments {
            last = segment;
        }
        Some((
            first.first_position,
            last.last_position,
            last.last_position
                .saturating_sub(first.first_position)
                .saturating_add(1),
        ))
    }

    pub(crate) fn remove_missing_paths(&mut self, existing: &BTreeSet<PathBuf>) {
        let removed: Vec<_> = self
            .sealed
            .iter()
            .filter(|segment| !existing.contains(segment.metadata.segment_path()))
            .map(|segment| {
                (
                    segment.metadata.segment_path().to_path_buf(),
                    segment.count,
                    segment.first_position,
                    segment.last_position,
                )
            })
            .collect();
        for (path, count, _, _) in &removed {
            self.total_count = self.total_count.saturating_sub(*count);
            self.cache.invalidate(path);
        }
        self.sealed
            .retain(|segment| existing.contains(segment.metadata.segment_path()));
    }

    pub(crate) fn active_for_path<'a>(
        &'a self,
        path: &'a Path,
    ) -> impl Iterator<Item = &'a MessageMeta> + Clone + 'a {
        self.active
            .iter()
            .filter(move |message| message.payload.path.as_ref() == path)
    }

    fn extend_active(&mut self, messages: Vec<MessageMeta>) -> Result<(), BrokerError> {
        let mut expected = self
            .active
            .back()
            .map(|message| message.position.saturating_add(1));
        if expected.is_none()
            && self.sealed.back().is_some_and(|segment| {
                messages
                    .first()
                    .is_some_and(|message| message.position <= segment.last_position)
            })
        {
            return Err(BrokerError::InvalidRecord(
                "topic message position ranges overlap".into(),
            ));
        }
        for message in &messages {
            if expected.is_some_and(|expected| message.position != expected) {
                return Err(BrokerError::InvalidRecord(
                    "topic message positions are not contiguous".into(),
                ));
            }
            expected = Some(message.position.saturating_add(1));
        }
        self.total_count = self.total_count.saturating_add(messages.len() as u64);
        self.active.extend(messages);
        Ok(())
    }

    fn load_ordinal(
        &self,
        segment: &SealedMessages,
        ordinal: u64,
    ) -> Result<MessageMeta, BrokerError> {
        let page = ordinal / PAGE_MESSAGES;
        let first_ordinal = page * PAGE_MESSAGES;
        let count = (segment.count - first_ordinal).min(PAGE_MESSAGES) as usize;
        let request = PageRequest {
            key: PageKey {
                segment: segment.metadata.segment_path().to_path_buf(),
                page,
            },
            metadata: segment.metadata.clone(),
            first_ordinal,
            count,
        };
        self.cache.load_blocking(request.clone())?;
        self.cache
            .get(&request.key, (ordinal - first_ordinal) as usize)
            .ok_or_else(|| BrokerError::InvalidRecord("topic index page is incomplete".into()))
    }

    fn push_sealed(&mut self, segment: SealedMessages) -> Result<(), BrokerError> {
        if segment.count == 0
            || segment.last_position != segment.first_position.saturating_add(segment.count - 1)
            || self
                .sealed
                .back()
                .is_some_and(|previous| previous.last_position >= segment.first_position)
            || self
                .active
                .front()
                .is_some_and(|message| segment.last_position >= message.position)
        {
            return Err(BrokerError::InvalidRecord(
                "sealed topic message range is invalid".into(),
            ));
        }
        self.total_count = self.total_count.saturating_add(segment.count);
        self.sealed.push_back(segment);
        Ok(())
    }
}

fn visit_deferred_ranges<'a>(
    messages: impl Iterator<Item = &'a MessageMeta>,
    now_ms: i64,
    visit: &mut impl FnMut(u64, u64),
) {
    let mut range: Option<(u64, u64)> = None;
    for message in messages {
        if message.available_at_ms > now_ms {
            if let Some((_, last)) = range.as_mut() {
                *last = message.position;
            } else {
                range = Some((message.position, message.position));
            }
        } else if let Some((first, last)) = range.take() {
            visit(first, last);
        }
    }
    if let Some((first, last)) = range {
        visit(first, last);
    }
}

impl Drop for MessageIndex {
    fn drop(&mut self) {
        for segment in &self.sealed {
            self.cache.invalidate(segment.metadata.segment_path());
        }
        self.cache.release_active(self.active.len());
    }
}

impl SealedMessages {
    fn from_summary(metadata: RecoveryMetadataRef, summary: recovery::Summary) -> Self {
        Self {
            metadata,
            count: summary.count,
            first_position: summary.first.position,
            last_position: summary.last.position,
            first_id: summary.first.id,
            last_id: summary.last.id,
            first_timestamp_ns: summary.first.timestamp_ns,
            last_timestamp_ns: summary.last.timestamp_ns,
            last_log_index: summary.last.log_index,
            min_available_at_ms: summary.min_available_at_ms,
            max_available_at_ms: summary.max_available_at_ms,
        }
    }
}
