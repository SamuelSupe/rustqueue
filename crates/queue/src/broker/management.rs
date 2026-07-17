use super::*;
use crate::management::{
    ChannelManagementAction, ManagementFenceSnapshot, ManagementResult, TopicManagementAction,
};
use crate::management_ops::OperationLookup;

impl Broker {
    pub fn management_fences_ready(&self) -> bool {
        self.inner.management_fences_ready.load(Ordering::Acquire)
    }

    pub async fn sync_management_fences(
        &self,
        snapshot: ManagementFenceSnapshot,
    ) -> Result<(), BrokerError> {
        let broker = self.clone();
        self.storage_task(move || {
            let mut fences = broker.inner.fences.lock();
            fences.replace(snapshot);
            fences.store(&broker.inner.fences_path)?;
            broker
                .inner
                .management_fences_ready
                .store(true, Ordering::Release);
            Ok(())
        })
        .await
    }

    pub async fn manage_topic(
        &self,
        operation_id: &str,
        topic: &str,
        action: TopicManagementAction,
        expected_revision: u64,
        tombstone_until_ms: Option<i64>,
    ) -> Result<ManagementResult, BrokerError> {
        validate_name(topic).map_err(|_| BrokerError::InvalidTopic)?;
        let broker = self.clone();
        let operation_id = operation_id.to_owned();
        let topic = topic.to_owned();
        self.storage_task(move || {
            let _lifecycle = broker.inner.topic_lifecycle.lock();
            let fingerprint = serde_json::to_string(&("topic", &topic, action))
                .map_err(|error| BrokerError::InvalidRecord(error.to_string()))?;
            let replayed = match broker.prepare_management_operation(
                &operation_id,
                &fingerprint,
                &topic,
                expected_revision,
            )? {
                PreparedOperation::Completed(result) => return Ok(result),
                PreparedOperation::New => false,
                PreparedOperation::Pending => true,
            };
            let mut changed = false;
            match action {
                TopicManagementAction::Create => {
                    let existed = broker.inner.topics.read().contains_key(&topic);
                    broker.get_or_create_topic_locked(&topic)?;
                    changed = !existed;
                    let mut fences = broker.inner.fences.lock();
                    if fences.clear_topic(&topic) {
                        fences.store(&broker.inner.fences_path)?;
                        changed = true;
                    }
                }
                TopicManagementAction::Pause | TopicManagementAction::Unpause => {
                    broker
                        .topic(&topic)?
                        .state
                        .lock()
                        .set_paused(action == TopicManagementAction::Pause)?;
                    changed = true;
                }
                TopicManagementAction::Empty => {
                    broker.topic(&topic)?.state.lock().empty_topic()?;
                    changed = true;
                }
                TopicManagementAction::Delete | TopicManagementAction::Tombstone => {
                    let until = valid_tombstone(tombstone_until_ms)?;
                    let mut fences = broker.inner.fences.lock();
                    if fences.set_topic(&topic, until) {
                        fences.store(&broker.inner.fences_path)?;
                        changed = true;
                    }
                    drop(fences);
                    if action == TopicManagementAction::Delete {
                        changed |= broker.delete_topic_locked(&topic)?;
                    }
                }
            }
            if changed || replayed {
                broker.bump_registry()?;
            }
            let result = ManagementResult {
                revision: broker.registry_revision(),
                changed,
            };
            rustqueue_storage::crash_failpoint("management_after_action_before_complete");
            broker.complete_management_operation(&operation_id, result.clone())?;
            Ok(result)
        })
        .await
    }

    pub async fn manage_channel(
        &self,
        operation_id: &str,
        topic: &str,
        channel: &str,
        action: ChannelManagementAction,
        expected_revision: u64,
        tombstone_until_ms: Option<i64>,
    ) -> Result<ManagementResult, BrokerError> {
        validate_name(topic).map_err(|_| BrokerError::InvalidTopic)?;
        validate_channel(channel)?;
        let broker = self.clone();
        let operation_id = operation_id.to_owned();
        let topic = topic.to_owned();
        let channel = channel.to_owned();
        self.storage_task(move || {
            let _lifecycle = broker.inner.topic_lifecycle.lock();
            let fingerprint = serde_json::to_string(&("channel", &topic, &channel, action))
                .map_err(|error| BrokerError::InvalidRecord(error.to_string()))?;
            let replayed = match broker.prepare_management_operation(
                &operation_id,
                &fingerprint,
                &topic,
                expected_revision,
            )? {
                PreparedOperation::Completed(result) => return Ok(result),
                PreparedOperation::New => false,
                PreparedOperation::Pending => true,
            };
            let mut changed = false;
            match action {
                ChannelManagementAction::Create => {
                    let handle = broker.get_or_create_topic_locked(&topic)?;
                    changed = handle
                        .state
                        .lock()
                        .create_channel(&channel, broker.inner.config.bootstrap_retention)?;
                    let mut fences = broker.inner.fences.lock();
                    if fences.clear_channel(&topic, &channel) {
                        fences.store(&broker.inner.fences_path)?;
                        changed = true;
                    }
                    handle.signal();
                }
                ChannelManagementAction::Pause | ChannelManagementAction::Unpause => {
                    broker
                        .topic(&topic)?
                        .state
                        .lock()
                        .set_channel_paused(&channel, action == ChannelManagementAction::Pause)?;
                    changed = true;
                }
                ChannelManagementAction::Empty => {
                    broker.topic(&topic)?.state.lock().empty_channel(&channel)?;
                    changed = true;
                }
                ChannelManagementAction::Delete | ChannelManagementAction::Tombstone => {
                    let until = valid_tombstone(tombstone_until_ms)?;
                    let mut fences = broker.inner.fences.lock();
                    if fences.set_channel(&topic, &channel, until) {
                        fences.store(&broker.inner.fences_path)?;
                        changed = true;
                    }
                    drop(fences);
                    if action == ChannelManagementAction::Delete {
                        if let Ok(handle) = broker.topic(&topic) {
                            if handle
                                .state
                                .lock()
                                .channel_names()
                                .iter()
                                .any(|name| name == &channel)
                            {
                                handle.state.lock().delete_channel(&channel)?;
                                changed = true;
                            }
                        }
                    }
                }
            }
            if changed || replayed {
                broker.bump_registry()?;
            }
            let result = ManagementResult {
                revision: broker.registry_revision(),
                changed,
            };
            rustqueue_storage::crash_failpoint("management_after_action_before_complete");
            broker.complete_management_operation(&operation_id, result.clone())?;
            Ok(result)
        })
        .await
    }

    pub(crate) fn ensure_management_access(
        &self,
        topic: &str,
        channel: Option<&str>,
    ) -> Result<(), BrokerError> {
        if !self.management_fences_ready() {
            return Err(BrokerError::ManagementUnavailable);
        }
        if self.inner.management_ops.lock().blocks_topic(topic) {
            return Err(BrokerError::ManagementUnavailable);
        }
        let fences = self.inner.fences.lock();
        if fences.topic_blocked(topic, now_ms()) {
            return Err(BrokerError::TopicTombstoned);
        }
        if channel.is_some_and(|channel| fences.channel_blocked(topic, channel, now_ms())) {
            return Err(BrokerError::ChannelTombstoned);
        }
        Ok(())
    }

    fn check_revision(&self, expected: u64) -> Result<(), BrokerError> {
        let actual = self.registry_revision();
        if expected == actual {
            Ok(())
        } else {
            Err(BrokerError::RevisionConflict { expected, actual })
        }
    }

    fn prepare_management_operation(
        &self,
        operation_id: &str,
        fingerprint: &str,
        topic: &str,
        expected_revision: u64,
    ) -> Result<PreparedOperation, BrokerError> {
        let mut operations = self.inner.management_ops.lock();
        match operations
            .lookup(operation_id, fingerprint)
            .map_err(operation_catalog_error)?
        {
            OperationLookup::Completed(result) => Ok(PreparedOperation::Completed(result)),
            OperationLookup::Pending => Ok(PreparedOperation::Pending),
            OperationLookup::New => {
                self.check_revision(expected_revision)?;
                operations
                    .prepare(
                        &self.inner.management_ops_path,
                        operation_id,
                        fingerprint.to_owned(),
                        topic.to_owned(),
                    )
                    .map_err(operation_catalog_error)?;
                Ok(PreparedOperation::New)
            }
        }
    }

    fn complete_management_operation(
        &self,
        operation_id: &str,
        result: ManagementResult,
    ) -> Result<(), BrokerError> {
        self.inner.management_ops.lock().complete(
            &self.inner.management_ops_path,
            operation_id,
            result,
        )?;
        Ok(())
    }
}

enum PreparedOperation {
    New,
    Pending,
    Completed(ManagementResult),
}

fn valid_tombstone(value: Option<i64>) -> Result<i64, BrokerError> {
    value
        .filter(|until| *until > now_ms())
        .ok_or_else(|| BrokerError::InvalidRecord("active tombstone deadline is required".into()))
}

fn operation_catalog_error(error: std::io::Error) -> BrokerError {
    if error.kind() == std::io::ErrorKind::InvalidInput {
        BrokerError::OperationConflict
    } else {
        BrokerError::Io(error)
    }
}
