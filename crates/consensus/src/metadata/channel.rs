use super::*;

impl MetadataCatalog {
    pub fn install_channel_metadata(
        &self,
        topic: &str,
        descriptor: ChannelDescriptor,
    ) -> Result<(), String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        let topic = state
            .topics
            .get_mut(topic)
            .ok_or_else(|| "topic not found".to_owned())?;
        if topic.state != TopicState::Active {
            return Err("topic is not active".into());
        }
        if topic
            .channels
            .get(&descriptor.name)
            .is_some_and(|existing| existing.generation > descriptor.generation)
        {
            return Ok(());
        }
        topic.next_channel_generation = topic
            .next_channel_generation
            .max(descriptor.generation.saturating_add(1));
        topic.channels.insert(descriptor.name.clone(), descriptor);
        topic.channel_catalog_revision = topic.channel_catalog_revision.saturating_add(1);
        state.epoch = state.epoch.saturating_add(1);
        Ok(())
    }

    pub fn prepare_channel(&self, topic: &str, channel: &str) -> Result<Option<u64>, String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        let topic = state
            .topics
            .get_mut(topic)
            .ok_or_else(|| "topic not found".to_owned())?;
        if topic.state != TopicState::Active {
            return Err("topic is not active".into());
        }
        if let Some(existing) = topic.channels.get(channel) {
            if existing.state == ChannelLifecycle::Deleting {
                return Ok(None);
            }
            return Ok(Some(existing.generation));
        }
        let generation = topic.next_channel_generation;
        topic.next_channel_generation = topic.next_channel_generation.saturating_add(1);
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
        topic.channel_catalog_revision = topic.channel_catalog_revision.saturating_add(1);
        state.epoch = state.epoch.saturating_add(1);
        Ok(Some(generation))
    }

    pub fn activate_channel(
        &self,
        topic: &str,
        channel: &str,
        generation: u64,
    ) -> Result<(), String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        let Some(topic) = state.topics.get_mut(topic) else {
            return Ok(());
        };
        let Some(descriptor) = topic.channels.get_mut(channel) else {
            return Ok(());
        };
        if descriptor.generation > generation {
            return Ok(());
        }
        if descriptor.generation < generation {
            return Err("channel generation mismatch".into());
        }
        if descriptor.state == ChannelLifecycle::Deleting {
            return Ok(());
        }
        descriptor.state = ChannelLifecycle::Active;
        topic.channel_catalog_revision = topic.channel_catalog_revision.saturating_add(1);
        state.epoch = state.epoch.saturating_add(1);
        Ok(())
    }

    pub fn prepare_delete_channel(
        &self,
        topic: &str,
        channel: &str,
    ) -> Result<Option<u64>, String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        let Some(topic) = state.topics.get_mut(topic) else {
            return Ok(None);
        };
        let Some(descriptor) = topic.channels.get_mut(channel) else {
            return Ok(None);
        };
        descriptor.state = ChannelLifecycle::Deleting;
        let generation = descriptor.generation;
        topic.channel_catalog_revision = topic.channel_catalog_revision.saturating_add(1);
        state.epoch = state.epoch.saturating_add(1);
        Ok(Some(generation))
    }

    pub fn complete_delete_channel(
        &self,
        topic: &str,
        channel: &str,
        generation: u64,
    ) -> Result<(), String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        let Some(topic) = state.topics.get_mut(topic) else {
            return Ok(());
        };
        if let Some(existing) = topic.channels.get(channel) {
            if existing.generation > generation {
                return Ok(());
            }
            if existing.generation < generation {
                return Err("channel generation mismatch".into());
            }
        }
        topic.channels.remove(channel);
        topic.channel_catalog_revision = topic.channel_catalog_revision.saturating_add(1);
        state.epoch = state.epoch.saturating_add(1);
        Ok(())
    }

    pub fn channel_is_active(&self, topic: &str, channel: &str) -> bool {
        self.state
            .read()
            .expect("metadata lock poisoned")
            .topics
            .get(topic)
            .and_then(|topic| topic.channels.get(channel))
            .is_some_and(|channel| channel.state == ChannelLifecycle::Active)
    }

    pub fn channel(&self, topic: &str, channel: &str) -> Option<ChannelDescriptor> {
        self.state
            .read()
            .expect("metadata lock poisoned")
            .topics
            .get(topic)
            .and_then(|topic| topic.channels.get(channel))
            .cloned()
    }

    pub fn active_channels(&self, topic: &str) -> Vec<String> {
        self.topic(topic)
            .map(|topic| {
                topic
                    .channels
                    .into_values()
                    .filter(|channel| channel.state == ChannelLifecycle::Active)
                    .map(|channel| channel.name)
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn renew_ephemeral_lease(
        &self,
        topic: &str,
        channel: &str,
        lease_id: u64,
        expires_at_ms: i64,
    ) -> Result<(), String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        let Some(descriptor) = state
            .topics
            .get_mut(topic)
            .and_then(|topic| topic.channels.get_mut(channel))
        else {
            return Ok(());
        };
        if descriptor.state == ChannelLifecycle::Deleting {
            return Ok(());
        }
        if !descriptor.ephemeral {
            return Err("channel is not ephemeral".into());
        }
        descriptor.lease_started = true;
        descriptor.leases.insert(lease_id, expires_at_ms);
        state.epoch = state.epoch.saturating_add(1);
        Ok(())
    }

    pub fn release_ephemeral_lease(
        &self,
        topic: &str,
        channel: &str,
        lease_id: u64,
    ) -> Result<(), String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        let Some(descriptor) = state
            .topics
            .get_mut(topic)
            .and_then(|topic| topic.channels.get_mut(channel))
        else {
            return Ok(());
        };
        if !descriptor.ephemeral {
            return Err("channel is not ephemeral".into());
        }
        descriptor.leases.remove(&lease_id);
        state.epoch = state.epoch.saturating_add(1);
        Ok(())
    }

    pub fn expired_ephemeral_channels(&self, now_ms: i64) -> Vec<(String, String)> {
        self.state
            .read()
            .expect("metadata lock poisoned")
            .topics
            .values()
            .flat_map(|topic| {
                topic
                    .channels
                    .values()
                    .filter(move |channel| {
                        channel.ephemeral
                            && channel.lease_started
                            && channel.state == ChannelLifecycle::Active
                            && channel
                                .leases
                                .values()
                                .all(|expires_at| *expires_at <= now_ms)
                    })
                    .map(|channel| (topic.name.clone(), channel.name.clone()))
            })
            .collect()
    }

    pub fn set_topic_paused(&self, topic: &str, paused: bool) -> Result<(), String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        let topic = state
            .topics
            .get_mut(topic)
            .ok_or_else(|| "topic not found".to_owned())?;
        topic.paused = paused;
        state.epoch = state.epoch.saturating_add(1);
        Ok(())
    }

    pub fn set_channel_metadata_paused(
        &self,
        topic: &str,
        channel: &str,
        paused: bool,
    ) -> Result<(), String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        let Some(topic) = state.topics.get_mut(topic) else {
            return Ok(());
        };
        let Some(channel) = topic.channels.get_mut(channel) else {
            return Ok(());
        };
        if channel.paused != paused {
            channel.paused = paused;
            topic.channel_catalog_revision = topic.channel_catalog_revision.saturating_add(1);
            state.epoch = state.epoch.saturating_add(1);
        }
        Ok(())
    }
}
