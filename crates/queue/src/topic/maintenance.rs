use super::*;
use crate::eviction::{self, ProtectiveEviction};
use crate::model::{QueueAggregateStats, TopicStats};
use rustqueue_storage::ScrubTarget;
use std::collections::BTreeSet;

impl Topic {
    pub fn empty_channel(&mut self, channel: &str) -> Result<(), BrokerError> {
        self.persist_channel(
            channel,
            ChannelCommand::Empty {
                through_position: self.deliverable_position,
            },
        )
    }

    pub fn empty_topic(&mut self) -> Result<(), BrokerError> {
        let through = self.deliverable_position;
        let has_durable_channels = self.has_durable_channels();
        let channels: Vec<_> = self.channels.keys().cloned().collect();
        for channel in channels {
            self.persist_channel(
                &channel,
                ChannelCommand::Empty {
                    through_position: through,
                },
            )?;
        }
        if !has_durable_channels {
            self.set_unrouted_from_position(Some(self.manifest.next_position))?;
        }
        Ok(())
    }

    pub fn set_channel_paused(&mut self, channel: &str, paused: bool) -> Result<(), BrokerError> {
        self.persist_channel(channel, ChannelCommand::Pause { paused })
    }

    pub fn stats(&mut self) -> TopicStats {
        let last = self.deliverable_position;
        let now_ms = now_ms();
        let scheduled = self.messages.deferred_positions(now_ms);
        let (segment_count, segment_bytes) = self.log.storage_usage();
        let mut channels: Vec<_> = self
            .channels
            .values_mut()
            .map(|channel| channel.state.stats(last, &scheduled, now_ms))
            .collect();
        channels.sort_by(|left, right| left.name.cmp(&right.name));
        TopicStats {
            name: self.name.clone(),
            paused: self.manifest.paused,
            published_count: self.published_count,
            message_count: self.messages.total_count(),
            segment_count,
            segment_bytes,
            channels,
        }
    }

    pub fn add_aggregate_stats(&mut self, aggregate: &mut QueueAggregateStats) {
        let last = self.deliverable_position;
        let now_ms = now_ms();
        let scheduled = self.messages.deferred_positions(now_ms);
        let (segment_count, segment_bytes) = self.log.storage_usage();
        aggregate.topic_count = aggregate.topic_count.saturating_add(1);
        aggregate.message_count = aggregate
            .message_count
            .saturating_add(self.messages.total_count());
        aggregate.segment_count = aggregate.segment_count.saturating_add(segment_count);
        aggregate.segment_bytes = aggregate.segment_bytes.saturating_add(segment_bytes);
        for channel in self.channels.values_mut() {
            let (depth, in_flight, deferred, ack_gap) =
                channel.state.metric_counts(last, &scheduled, now_ms);
            aggregate.channel_count = aggregate.channel_count.saturating_add(1);
            aggregate.channel_depth = aggregate.channel_depth.saturating_add(depth);
            aggregate.channel_in_flight = aggregate.channel_in_flight.saturating_add(in_flight);
            aggregate.channel_deferred = aggregate.channel_deferred.saturating_add(deferred);
            aggregate.channel_ack_gap = aggregate.channel_ack_gap.saturating_add(ack_gap);
        }
    }

    pub fn oldest_message_timestamp(&self) -> Option<i64> {
        self.messages.oldest_timestamp_ns()
    }

    pub fn protective_evict_oldest(
        &mut self,
        audit_directory: &Path,
        retained_paths: &BTreeSet<PathBuf>,
    ) -> Result<Option<ProtectiveEviction>, BrokerError> {
        if self.messages.total_count() == 0 {
            return Ok(None);
        }
        self.seal_log()?;
        let Some((segment, through_index)) = self
            .log
            .oldest_inactive_boundary_retaining(retained_paths)?
        else {
            return Ok(None);
        };
        let Some((first_position, through_position, messages)) =
            self.messages.eviction_range(through_index)
        else {
            return Ok(None);
        };
        let report = ProtectiveEviction {
            topic: self.name.clone(),
            first_position,
            through_position,
            messages,
            segment: segment.clone(),
            created_at_ms: now_ms().max(0) as u64,
        };
        store_atomic(&self.manifest_path, &self.manifest)?;
        eviction::write_intent(audit_directory, &report)?;
        if !self.has_durable_channels() {
            let retained_from = self
                .unrouted_start_position()?
                .max(through_position.saturating_add(1))
                .min(self.manifest.next_position);
            self.set_unrouted_from_position(Some(retained_from))?;
        }
        let channels: Vec<_> = self.channels.keys().cloned().collect();
        for channel in channels {
            self.persist_channel(&channel, ChannelCommand::Evict { through_position })?;
        }
        if self
            .log
            .purge_prefix_retaining(through_index, retained_paths)?
            == 0
        {
            return Err(BrokerError::InvalidRecord(
                "protective eviction found no removable segment".into(),
            ));
        }
        self.remove_purged_messages()?;
        Ok(Some(report))
    }

    pub fn compact(
        &mut self,
        bootstrap_retention: Duration,
        retained_paths: &BTreeSet<PathBuf>,
        retained_message_ids: &BTreeSet<u64>,
    ) -> Result<usize, BrokerError> {
        let cutoff =
            now_ns().saturating_sub(bootstrap_retention.as_nanos().min(i64::MAX as u128) as i64);
        let bootstrap_from = self
            .messages
            .retain_from_timestamp(cutoff, self.manifest.next_position);
        let channel_from = self
            .channels
            .values()
            .filter(|channel| !channel.state.ephemeral)
            .map(|channel| channel.state.ack_floor_position.saturating_add(1))
            .min();
        let channel_from = match channel_from {
            Some(position) => position,
            None => self.unrouted_start_position()?,
        };
        let in_flight_from = self
            .channels
            .values()
            .filter_map(|channel| channel.state.first_in_flight_position())
            .min()
            .unwrap_or(self.manifest.next_position);
        let outbox_from = self
            .messages
            .retain_from_ids(retained_message_ids, self.manifest.next_position);
        let retain_from = bootstrap_from
            .min(channel_from)
            .min(in_flight_from)
            .min(outbox_from);
        if self
            .messages
            .active_last_position()
            .is_some_and(|position| position < retain_from)
        {
            self.seal_log()?;
        }
        let through_index = self.messages.purge_through_log_index(retain_from);
        let Some(through_index) = through_index else {
            return Ok(0);
        };
        if self
            .messages
            .first_purge_path(retain_from)
            .is_some_and(|path| retained_paths.contains(path))
        {
            return Ok(0);
        }
        // The manifest position must reach disk before its source segments are
        // removed, otherwise an empty topic could reuse positions after restart.
        store_atomic(&self.manifest_path, &self.manifest)?;
        let removed = self
            .log
            .purge_prefix_retaining(through_index, retained_paths)?;
        if removed > 0 {
            self.remove_purged_messages()?;
        }
        Ok(removed)
    }

    pub fn scrub_targets(&self) -> Result<Vec<ScrubTarget>, BrokerError> {
        Ok(self.log.scrub_targets(false)?)
    }

    pub fn sync(&mut self) -> Result<(), BrokerError> {
        self.log.sync()?;
        self.checkpoint_channels()?;
        store_atomic(&self.manifest_path, &self.manifest)?;
        Ok(())
    }

    pub fn expire_in_flight(&mut self) -> Result<usize, BrokerError> {
        self.channels
            .values_mut()
            .try_fold(0usize, |total, channel| {
                Ok(total.saturating_add(channel.expire_in_flight()?))
            })
    }

    pub fn expire_channel_in_flight(&mut self, channel: &str) -> Result<usize, BrokerError> {
        self.channels
            .get_mut(channel)
            .ok_or(BrokerError::ChannelNotFound)?
            .expire_in_flight()
    }

    pub fn has_expired_in_flight(&self) -> bool {
        self.channels
            .values()
            .any(|channel| channel.has_expired_in_flight())
    }

    pub fn channel_has_expired_in_flight(&self, channel: &str) -> Result<bool, BrokerError> {
        Ok(self
            .channels
            .get(channel)
            .ok_or(BrokerError::ChannelNotFound)?
            .has_expired_in_flight())
    }

    pub fn checkpoint_channels(&mut self) -> Result<(), BrokerError> {
        for runtime in self.channels.values_mut() {
            if let Some(store) = runtime.store.as_mut() {
                store.checkpoint(&runtime.state)?;
            }
        }
        Ok(())
    }

    pub fn mark_deleted(&mut self) -> Result<(), BrokerError> {
        self.manifest.deleted = true;
        store_atomic(&self.manifest_path, &self.manifest)?;
        Ok(())
    }

    fn remove_purged_messages(&mut self) -> Result<(), BrokerError> {
        let existing: BTreeSet<_> = self.log.segment_paths()?.into_iter().collect();
        self.messages.remove_missing_paths(&existing);
        Ok(())
    }
}
