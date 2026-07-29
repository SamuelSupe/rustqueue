use super::*;
use crate::metadata::topic_directory;

impl Broker {
    pub(super) fn get_or_create_topic(&self, name: &str) -> Result<Arc<TopicHandle>, BrokerError> {
        validate_name(name).map_err(|_| BrokerError::InvalidTopic)?;
        self.ensure_management_access(name, None)?;
        if let Some(topic) = self.inner.topics.read().get(name).cloned() {
            return Ok(topic);
        }
        let _lifecycle = self.inner.topic_lifecycle.lock();
        self.get_or_create_topic_locked(name)
    }

    pub(super) fn get_or_create_topic_locked(
        &self,
        name: &str,
    ) -> Result<Arc<TopicHandle>, BrokerError> {
        self.cleanup_retired_topic(name)?;
        let mut topics = self.inner.topics.write();
        if let Some(topic) = topics.get(name).cloned() {
            return Ok(topic);
        }
        if topics.len() >= self.inner.config.max_topics {
            return Err(BrokerError::TopicLimit);
        }
        let directory = topic_directory(&self.inner.config.data_path, name);
        let topic = TopicHandle::create(
            &directory,
            name,
            self.inner.config.max_segment_bytes,
            self.inner.config.max_ack_gap,
            self.inner.compatibility.active_writer_feature_level,
            Arc::clone(&self.inner.message_index_cache),
        )?;
        topics.insert(name.into(), Arc::clone(&topic));
        drop(topics);
        self.bump_registry()?;
        Ok(topic)
    }

    fn cleanup_retired_topic(&self, name: &str) -> Result<(), BrokerError> {
        let mut retired = self.inner.retired_topics.lock();
        let Some(handle) = retired.get(name) else {
            return Ok(());
        };
        if Arc::strong_count(handle) > 1 {
            return Err(BrokerError::TopicRetiring);
        }
        let directory = topic_directory(&self.inner.config.data_path, name);
        if self.inner.payload_reader.has_active_under(&directory) {
            return Err(BrokerError::TopicRetiring);
        }
        let handle = retired.remove(name).expect("retired topic still exists");
        drop(handle);
        if directory.exists() {
            self.inner.payload_reader.invalidate_under(&directory);
            std::fs::remove_dir_all(directory)?;
        }
        std::fs::File::open(&self.inner.topics_root)?.sync_all()?;
        Ok(())
    }

    pub(super) fn cleanup_drained_retired_topics(&self) -> Result<usize, BrokerError> {
        let ready: Vec<_> = self
            .inner
            .retired_topics
            .lock()
            .iter()
            .filter_map(|(name, handle)| {
                let directory = topic_directory(&self.inner.config.data_path, name);
                (Arc::strong_count(handle) == 1
                    && !self.inner.payload_reader.has_active_under(&directory))
                .then(|| name.clone())
            })
            .collect();
        for name in &ready {
            self.cleanup_retired_topic(name)?;
        }
        Ok(ready.len())
    }

    pub(super) fn delete_topic_locked(&self, name: &str) -> Result<bool, BrokerError> {
        let Some(handle) = self.inner.topics.read().get(name).cloned() else {
            return Ok(false);
        };
        let commit_gate = handle.commit_gate.lock();
        handle.state.lock().mark_deleted()?;
        drop(commit_gate);
        self.inner.topics.write().remove(name);
        let directory = topic_directory(&self.inner.config.data_path, name);
        if Arc::strong_count(&handle) == 1
            && !self.inner.payload_reader.has_active_under(&directory)
        {
            self.inner.payload_reader.invalidate_under(&directory);
            drop(handle);
            std::fs::remove_dir_all(directory)?;
            std::fs::File::open(&self.inner.topics_root)?.sync_all()?;
        } else {
            self.inner
                .retired_topics
                .lock()
                .insert(name.to_owned(), handle);
        }
        Ok(true)
    }

    pub(super) fn topic(&self, name: &str) -> Result<Arc<TopicHandle>, BrokerError> {
        validate_name(name).map_err(|_| BrokerError::InvalidTopic)?;
        self.inner
            .topics
            .read()
            .get(name)
            .cloned()
            .ok_or(BrokerError::TopicNotFound)
    }
}
