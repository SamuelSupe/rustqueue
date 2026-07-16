use super::*;
use crate::eviction::{self, ProtectiveEviction};
use crate::model::TopicStats;
use std::collections::BTreeSet;

impl Topic {
    pub fn empty_channel(&mut self, channel: &str) -> Result<(), BrokerError> {
        self.persist_channel(
            channel,
            ChannelCommand::Empty {
                through_position: self.last_position(),
            },
        )
    }

    pub fn empty_topic(&mut self) -> Result<(), BrokerError> {
        let through = self.last_position();
        let channels: Vec<_> = self.channels.keys().cloned().collect();
        for channel in channels {
            self.persist_channel(
                &channel,
                ChannelCommand::Empty {
                    through_position: through,
                },
            )?;
        }
        Ok(())
    }

    pub fn set_channel_paused(&mut self, channel: &str, paused: bool) -> Result<(), BrokerError> {
        self.persist_channel(channel, ChannelCommand::Pause { paused })
    }

    pub fn stats(&self) -> TopicStats {
        let last = self.last_position();
        let mut channels: Vec<_> = self
            .channels
            .values()
            .map(|channel| channel.state.stats(last))
            .collect();
        channels.sort_by(|left, right| left.name.cmp(&right.name));
        TopicStats {
            name: self.name.clone(),
            paused: self.manifest.paused,
            message_count: self.messages.len() as u64,
            channels,
        }
    }

    pub fn oldest_message_timestamp(&self) -> Option<i64> {
        self.messages.front().map(|message| message.timestamp_ns)
    }

    pub fn protective_evict_oldest(
        &mut self,
        audit_directory: &Path,
        retained_paths: &BTreeSet<PathBuf>,
    ) -> Result<Option<ProtectiveEviction>, BrokerError> {
        if self.messages.is_empty() {
            return Ok(None);
        }
        self.log.seal()?;
        let Some((segment, through_index)) = self
            .log
            .oldest_inactive_boundary_retaining(retained_paths)?
        else {
            return Ok(None);
        };
        let mut affected = self
            .messages
            .iter()
            .filter(|message| message.log_index <= through_index);
        let Some(first) = affected.next() else {
            return Ok(None);
        };
        let first_position = first.position;
        let mut through_position = first.position;
        let mut messages = 1u64;
        for message in affected {
            through_position = message.position;
            messages = messages.saturating_add(1);
        }
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
            .iter()
            .find(|message| message.timestamp_ns >= cutoff)
            .map_or(self.manifest.next_position, |message| message.position);
        let channel_from = self
            .channels
            .values()
            .filter(|channel| !channel.state.ephemeral)
            .map(|channel| channel.state.ack_floor_position.saturating_add(1))
            .min()
            .unwrap_or(self.manifest.next_position);
        let in_flight_from = self
            .channels
            .values()
            .filter_map(|channel| channel.state.first_in_flight_position())
            .min()
            .unwrap_or(self.manifest.next_position);
        let outbox_from = self
            .messages
            .iter()
            .filter(|message| retained_message_ids.contains(&message.id))
            .map(|message| message.position)
            .min()
            .unwrap_or(self.manifest.next_position);
        let retain_from = bootstrap_from
            .min(channel_from)
            .min(in_flight_from)
            .min(outbox_from);
        let through_index = self
            .messages
            .iter()
            .take_while(|message| message.position < retain_from)
            .last()
            .map(|message| message.log_index);
        let Some(through_index) = through_index else {
            return Ok(0);
        };
        store_atomic(&self.manifest_path, &self.manifest)?;
        self.log.seal()?;
        let removed = self
            .log
            .purge_prefix_retaining(through_index, retained_paths)?;
        if removed > 0 {
            self.remove_purged_messages()?;
        }
        Ok(removed)
    }

    pub fn scrub(&self) -> Result<usize, BrokerError> {
        Ok(self.log.scrub()?)
    }

    pub fn sync(&self) -> Result<(), BrokerError> {
        self.log.sync()?;
        store_atomic(&self.manifest_path, &self.manifest)?;
        Ok(())
    }

    pub fn mark_deleted(&mut self) -> Result<(), BrokerError> {
        self.manifest.deleted = true;
        store_atomic(&self.manifest_path, &self.manifest)?;
        Ok(())
    }

    fn remove_purged_messages(&mut self) -> Result<(), BrokerError> {
        let existing: BTreeSet<_> = self.log.segment_paths()?.into_iter().collect();
        while self
            .messages
            .front()
            .is_some_and(|message| !existing.contains(message.payload.path.as_ref()))
        {
            self.messages.pop_front();
        }
        Ok(())
    }
}
