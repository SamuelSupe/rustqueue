use super::{backend, event, resource_state, ManagementError};
use crate::app::AppState;
use crate::management::preview::{current_snapshot, ensure_owners_healthy};
use crate::resources;
use rustqueue_operator::{
    ManagedResourceAction, ManagedResourcePhase, RustQueueChannel, RustQueueTopic,
};

pub struct Reconciler {
    state: AppState,
}

impl Reconciler {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }

    pub async fn run(self) {
        let mut interval = tokio::time::interval(self.state.config.poll_interval);
        loop {
            interval.tick().await;
            if let Err(error) = self.reconcile().await {
                tracing::warn!(
                    error = error.detail(),
                    "console management reconciliation deferred"
                );
            }
        }
    }

    async fn reconcile(&self) -> Result<(), ManagementError> {
        let _guard = self.state.mutation_lock.lock().await;
        let snapshot = current_snapshot(&self.state)?;
        let catalog = resources::list(
            &self.state.client,
            &self.state.config.namespace,
            &self.state.config.queue_name,
        )
        .await
        .map_err(|error| ManagementError::unavailable(error.to_string()))?;
        for topic in catalog.topics.into_iter().filter(topic_pending) {
            self.reconcile_topic(&snapshot, topic).await;
        }
        let catalog = resources::list(
            &self.state.client,
            &self.state.config.namespace,
            &self.state.config.queue_name,
        )
        .await
        .map_err(|error| ManagementError::unavailable(error.to_string()))?;
        let active_topics: std::collections::BTreeSet<_> = catalog
            .topics
            .iter()
            .filter(|topic| topic.spec.phase == ManagedResourcePhase::Active)
            .map(|topic| topic.spec.topic.clone())
            .collect();
        for channel in catalog
            .channels
            .into_iter()
            .filter(channel_pending)
            .filter(|channel| active_topics.contains(channel.spec.topic.as_str()))
        {
            self.reconcile_channel(&snapshot, channel).await;
        }
        Ok(())
    }

    async fn reconcile_topic(&self, snapshot: &crate::model::Snapshot, resource: RustQueueTopic) {
        let operation = resource.spec.operation.clone().expect("pending operation");
        let action = resource_state::action_name(operation.action);
        let target = resource.spec.topic.clone();
        let result = async {
            ensure_owners_healthy(snapshot, &resource.spec.owners)?;
            if matches!(
                operation.action,
                ManagedResourceAction::Create | ManagedResourceAction::Delete
            ) {
                backend::sync_fences(&self.state, snapshot).await?;
            }
            if let Some(owner) = next_owner(&resource.spec.owners, &operation.completed_owners) {
                backend::apply_owner(
                    &self.state,
                    snapshot,
                    backend::OwnerOperation {
                        owner,
                        operation_id: &operation.id,
                        topic: &resource.spec.topic,
                        channel: None,
                        action,
                        tombstone_until_ms: resource.spec.tombstone_until_ms,
                    },
                )
                .await?;
                resource_state::record_topic_owner(&self.state, resource.clone(), owner).await?;
                return Ok(false);
            }
            if operation.action == ManagedResourceAction::Delete {
                resource_state::tombstone_topic_channels(
                    &self.state,
                    &resource.spec.topic,
                    resource.spec.tombstone_until_ms,
                )
                .await?;
            }
            resource_state::finish_topic(&self.state, resource.clone()).await?;
            Ok(true)
        }
        .await;
        self.finish_topic_attempt(resource, action, &target, result)
            .await;
    }

    async fn reconcile_channel(
        &self,
        snapshot: &crate::model::Snapshot,
        resource: RustQueueChannel,
    ) {
        let operation = resource.spec.operation.clone().expect("pending operation");
        let action = resource_state::action_name(operation.action);
        let target = format!("{}/{}", resource.spec.topic, resource.spec.channel);
        let result = async {
            ensure_owners_healthy(snapshot, &resource.spec.owners)?;
            if matches!(
                operation.action,
                ManagedResourceAction::Create | ManagedResourceAction::Delete
            ) {
                backend::sync_fences(&self.state, snapshot).await?;
            }
            if let Some(owner) = next_owner(&resource.spec.owners, &operation.completed_owners) {
                backend::apply_owner(
                    &self.state,
                    snapshot,
                    backend::OwnerOperation {
                        owner,
                        operation_id: &operation.id,
                        topic: &resource.spec.topic,
                        channel: Some(&resource.spec.channel),
                        action,
                        tombstone_until_ms: resource.spec.tombstone_until_ms,
                    },
                )
                .await?;
                resource_state::record_channel_owner(&self.state, resource.clone(), owner).await?;
                return Ok(false);
            }
            resource_state::finish_channel(&self.state, resource.clone()).await?;
            Ok(true)
        }
        .await;
        self.finish_channel_attempt(resource, action, &target, result)
            .await;
    }

    async fn finish_topic_attempt(
        &self,
        resource: RustQueueTopic,
        action: &str,
        target: &str,
        result: Result<bool, ManagementError>,
    ) {
        match result {
            Ok(true) => {
                event::emit(
                    &self.state,
                    &format!("topic.{action}"),
                    target,
                    "success",
                    "operation completed",
                )
                .await;
            }
            Ok(false) => {}
            Err(error) if error.retryable() => {
                resource_state::note_topic_error(&self.state, resource, error.detail()).await;
            }
            Err(error) => {
                resource_state::fail_topic(&self.state, resource, error.detail()).await;
                event::emit(
                    &self.state,
                    &format!("topic.{action}"),
                    target,
                    "failed",
                    error.detail(),
                )
                .await;
            }
        }
    }

    async fn finish_channel_attempt(
        &self,
        resource: RustQueueChannel,
        action: &str,
        target: &str,
        result: Result<bool, ManagementError>,
    ) {
        match result {
            Ok(true) => {
                event::emit(
                    &self.state,
                    &format!("channel.{action}"),
                    target,
                    "success",
                    "operation completed",
                )
                .await;
            }
            Ok(false) => {}
            Err(error) if error.retryable() => {
                resource_state::note_channel_error(&self.state, resource, error.detail()).await;
            }
            Err(error) => {
                resource_state::fail_channel(&self.state, resource, error.detail()).await;
                event::emit(
                    &self.state,
                    &format!("channel.{action}"),
                    target,
                    "failed",
                    error.detail(),
                )
                .await;
            }
        }
    }
}

fn topic_pending(resource: &RustQueueTopic) -> bool {
    matches!(
        resource.spec.phase,
        ManagedResourcePhase::Preparing | ManagedResourcePhase::Applying
    ) && resource.spec.operation.is_some()
}

fn channel_pending(resource: &RustQueueChannel) -> bool {
    matches!(
        resource.spec.phase,
        ManagedResourcePhase::Preparing | ManagedResourcePhase::Applying
    ) && resource.spec.operation.is_some()
}

fn next_owner<'a>(owners: &'a [String], completed: &[String]) -> Option<&'a str> {
    owners
        .iter()
        .find(|owner| !completed.contains(owner))
        .map(String::as_str)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resumes_at_the_first_unfinished_owner() {
        let owners = vec!["one".into(), "two".into(), "three".into()];
        let completed = vec!["one".into(), "three".into()];
        assert_eq!(next_owner(&owners, &completed), Some("two"));
    }
}
