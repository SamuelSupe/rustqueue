use super::CatalogState;
use crate::{ChannelDescriptor, ChannelLifecycle};
use std::collections::BTreeMap;

impl CatalogState {
    pub fn prepare_channel(&mut self, topic: &str, channel: &str) -> Result<u64, String> {
        let topic = self
            .topics
            .get_mut(topic)
            .ok_or_else(|| "Catalog topic not found".to_owned())?;
        if topic.deleting {
            return Err("Catalog topic deletion is in progress".into());
        }
        if let Some(existing) = topic.channels.get(channel) {
            if existing.state == ChannelLifecycle::Deleting {
                return Err("channel deletion is still in progress".into());
            }
            return Ok(existing.generation);
        }
        let generation = topic.catalog_revision.saturating_add(1).max(1);
        topic.catalog_revision = generation;
        topic.channel_tombstones.remove(channel);
        topic.channels.insert(
            channel.to_owned(),
            ChannelDescriptor {
                name: channel.to_owned(),
                generation,
                state: ChannelLifecycle::Preparing,
                ephemeral: channel.ends_with("#ephemeral"),
                leases: BTreeMap::new(),
                lease_started: false,
                paused: false,
            },
        );
        self.epoch = self.epoch.saturating_add(1);
        Ok(generation)
    }

    pub fn update_channel(
        &mut self,
        topic: &str,
        channel: &str,
        generation: u64,
        state: ChannelLifecycle,
        paused: bool,
    ) -> Result<(), String> {
        let topic = self
            .topics
            .get_mut(topic)
            .ok_or_else(|| "Catalog topic not found".to_owned())?;
        if topic.deleting {
            return Err("Catalog topic deletion is in progress".into());
        }
        let descriptor = topic
            .channels
            .get_mut(channel)
            .ok_or_else(|| "Catalog channel not found".to_owned())?;
        if descriptor.generation != generation {
            return Err("Catalog channel generation mismatch".into());
        }
        if descriptor.state == ChannelLifecycle::Deleting && state != ChannelLifecycle::Deleting {
            return Err("Catalog channel deletion cannot be reversed".into());
        }
        descriptor.state = state;
        descriptor.paused = paused;
        topic.catalog_revision = topic.catalog_revision.saturating_add(1);
        self.epoch = self.epoch.saturating_add(1);
        Ok(())
    }

    pub fn remove_channel(
        &mut self,
        topic: &str,
        channel: &str,
        generation: u64,
    ) -> Result<(), String> {
        let topic = self
            .topics
            .get_mut(topic)
            .ok_or_else(|| "Catalog topic not found".to_owned())?;
        let Some(existing) = topic.channels.get(channel) else {
            return Ok(());
        };
        if existing.generation != generation {
            return Err("Catalog channel generation mismatch".into());
        }
        topic.channels.remove(channel);
        topic
            .channel_tombstones
            .insert(channel.to_owned(), generation);
        topic.catalog_revision = topic.catalog_revision.saturating_add(1);
        self.epoch = self.epoch.saturating_add(1);
        Ok(())
    }

    pub fn renew_ephemeral_lease(
        &mut self,
        topic: &str,
        channel: &str,
        lease_id: u64,
        expires_at_ms: i64,
    ) -> Result<(), String> {
        let topic = self
            .topics
            .get_mut(topic)
            .ok_or_else(|| "Catalog topic not found".to_owned())?;
        if topic.deleting {
            return Err("Catalog topic deletion is in progress".into());
        }
        if !topic.channels.contains_key(channel) {
            if !channel.ends_with("#ephemeral") {
                return Err("only an ephemeral lease may create a Catalog channel".into());
            }
            let generation = topic.catalog_revision.saturating_add(1).max(1);
            topic.catalog_revision = generation;
            topic.channel_tombstones.remove(channel);
            topic.channels.insert(
                channel.to_owned(),
                ChannelDescriptor {
                    name: channel.to_owned(),
                    generation,
                    state: ChannelLifecycle::Preparing,
                    ephemeral: true,
                    leases: BTreeMap::new(),
                    lease_started: false,
                    paused: false,
                },
            );
        }
        let descriptor = topic
            .channels
            .get_mut(channel)
            .expect("ephemeral channel exists after lease preparation");
        if !descriptor.ephemeral {
            return Err("channel is not ephemeral".into());
        }
        if descriptor.state == ChannelLifecycle::Deleting {
            return Err("ephemeral channel deletion is in progress".into());
        }
        descriptor.lease_started = true;
        descriptor.leases.insert(lease_id, expires_at_ms);
        topic.catalog_revision = topic.catalog_revision.saturating_add(1);
        self.epoch = self.epoch.saturating_add(1);
        Ok(())
    }

    pub fn release_ephemeral_lease(
        &mut self,
        topic: &str,
        channel: &str,
        lease_id: u64,
        now_ms: i64,
    ) -> Result<bool, String> {
        let topic = self
            .topics
            .get_mut(topic)
            .ok_or_else(|| "Catalog topic not found".to_owned())?;
        let Some(descriptor) = topic.channels.get_mut(channel) else {
            return Ok(false);
        };
        if !descriptor.ephemeral {
            return Err("channel is not ephemeral".into());
        }
        descriptor.leases.remove(&lease_id);
        let deleting = mark_ephemeral_deleting(descriptor, now_ms);
        topic.catalog_revision = topic.catalog_revision.saturating_add(1);
        self.epoch = self.epoch.saturating_add(1);
        Ok(deleting)
    }

    pub fn expire_ephemeral_leases(&mut self, now_ms: i64) -> usize {
        let mut expired = 0;
        for topic in self.topics.values_mut() {
            for descriptor in topic.channels.values_mut() {
                if mark_ephemeral_deleting(descriptor, now_ms) {
                    expired += 1;
                    topic.catalog_revision = topic.catalog_revision.saturating_add(1);
                }
            }
        }
        if expired > 0 {
            self.epoch = self.epoch.saturating_add(1);
        }
        expired
    }
}

fn mark_ephemeral_deleting(descriptor: &mut ChannelDescriptor, now_ms: i64) -> bool {
    if !descriptor.ephemeral
        || !descriptor.lease_started
        || descriptor.state != ChannelLifecycle::Active
        || descriptor
            .leases
            .values()
            .any(|expires_at| *expires_at > now_ms)
    {
        return false;
    }
    descriptor.state = ChannelLifecycle::Deleting;
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CatalogTopic, RoutingMode};
    use std::collections::{BTreeMap, BTreeSet};

    fn catalog() -> CatalogState {
        CatalogState {
            topics: BTreeMap::from([(
                "events".into(),
                CatalogTopic {
                    name: "events".into(),
                    deleting: false,
                    routing_mode: RoutingMode::Elastic,
                    topology_generation: 1,
                    routing_epoch: 1,
                    catalog_revision: 0,
                    feature_level: 4,
                    paused: false,
                    channels: BTreeMap::new(),
                    channel_tombstones: BTreeMap::new(),
                    partitions: BTreeMap::new(),
                    partition_numbers: BTreeMap::new(),
                    bucket_ranges: Vec::new(),
                    home_cells: BTreeSet::new(),
                },
            )]),
            ..CatalogState::default()
        }
    }

    #[test]
    fn channel_generation_prevents_delete_create_aba() {
        let mut catalog = catalog();
        let first = catalog.prepare_channel("events", "workers").unwrap();
        catalog
            .update_channel(
                "events",
                "workers",
                first,
                ChannelLifecycle::Deleting,
                false,
            )
            .unwrap();
        assert!(catalog.prepare_channel("events", "workers").is_err());
        catalog.remove_channel("events", "workers", first).unwrap();
        let second = catalog.prepare_channel("events", "workers").unwrap();
        assert!(second > first);
    }

    #[test]
    fn first_ephemeral_lease_atomically_prepares_channel() {
        let mut catalog = catalog();
        catalog
            .renew_ephemeral_lease("events", "tail#ephemeral", 7, 1_000)
            .unwrap();

        let channel = &catalog.topics["events"].channels["tail#ephemeral"];
        assert_eq!(channel.state, ChannelLifecycle::Preparing);
        assert!(channel.ephemeral);
        assert!(channel.lease_started);
        assert_eq!(channel.leases.get(&7), Some(&1_000));
    }

    #[test]
    fn last_ephemeral_release_atomically_starts_deletion() {
        let mut catalog = catalog();
        catalog
            .renew_ephemeral_lease("events", "tail#ephemeral", 7, 1_000)
            .unwrap();
        let generation = catalog.topics["events"].channels["tail#ephemeral"].generation;
        catalog
            .update_channel(
                "events",
                "tail#ephemeral",
                generation,
                ChannelLifecycle::Active,
                false,
            )
            .unwrap();

        assert!(catalog
            .release_ephemeral_lease("events", "tail#ephemeral", 7, 500)
            .unwrap());
        assert_eq!(
            catalog.topics["events"].channels["tail#ephemeral"].state,
            ChannelLifecycle::Deleting
        );
    }

    #[test]
    fn live_ephemeral_lease_prevents_deletion() {
        let mut catalog = catalog();
        for (lease_id, expires_at_ms) in [(7, 1_000), (8, 2_000)] {
            catalog
                .renew_ephemeral_lease("events", "tail#ephemeral", lease_id, expires_at_ms)
                .unwrap();
        }
        let generation = catalog.topics["events"].channels["tail#ephemeral"].generation;
        catalog
            .update_channel(
                "events",
                "tail#ephemeral",
                generation,
                ChannelLifecycle::Active,
                false,
            )
            .unwrap();

        assert!(!catalog
            .release_ephemeral_lease("events", "tail#ephemeral", 7, 1_500)
            .unwrap());
        assert_eq!(
            catalog.topics["events"].channels["tail#ephemeral"].state,
            ChannelLifecycle::Active
        );
    }

    #[test]
    fn expiry_only_deletes_active_channel_after_all_leases_expire() {
        let mut catalog = catalog();
        catalog
            .renew_ephemeral_lease("events", "tail#ephemeral", 7, 1_000)
            .unwrap();
        let generation = catalog.topics["events"].channels["tail#ephemeral"].generation;
        catalog
            .update_channel(
                "events",
                "tail#ephemeral",
                generation,
                ChannelLifecycle::Active,
                false,
            )
            .unwrap();

        assert_eq!(catalog.expire_ephemeral_leases(999), 0);
        assert_eq!(catalog.expire_ephemeral_leases(1_000), 1);
        assert_eq!(catalog.expire_ephemeral_leases(2_000), 0);
    }
}
