use crate::crd::{OperationStatus, RustQueueCondition, RustQueueStatus};
use crate::RustQueue;
use chrono::{DateTime, Utc};

const HISTORY_LIMIT: usize = 20;

pub(super) struct StatusBuilder<'a> {
    cluster: &'a RustQueue,
    status: RustQueueStatus,
}

pub(super) struct OperationUpdate<'a> {
    pub id: &'a str,
    pub kind: &'a str,
    pub phase: &'a str,
    pub target: &'a str,
    pub revision: &'a str,
    pub message: &'a str,
    pub previous_image: Option<String>,
    pub current_broker: Option<String>,
}

impl<'a> StatusBuilder<'a> {
    pub fn new(cluster: &'a RustQueue, desired: i32, ready: i32, feature_level: u32) -> Self {
        let previous = cluster.status.clone().unwrap_or_default();
        Self {
            cluster,
            status: RustQueueStatus {
                observed_generation: cluster.metadata.generation,
                desired_brokers: desired,
                ready_brokers: ready,
                phase: previous.phase,
                message: previous.message,
                active_storage_feature_level: feature_level,
                conditions: previous.conditions,
                current_operation: previous.current_operation,
                operation_history: previous.operation_history,
                orphaned_pvcs: previous.orphaned_pvcs,
                desired_storage_size: cluster.spec.storage_size.clone(),
            },
        }
    }

    pub fn summary(mut self, phase: impl Into<String>, message: impl Into<String>) -> Self {
        self.status.phase = phase.into();
        self.status.message = message.into();
        self
    }

    pub fn condition(
        mut self,
        type_: &str,
        value: bool,
        reason: &str,
        message: impl Into<String>,
    ) -> Self {
        let status = if value { "True" } else { "False" };
        let message = message.into();
        let previous = self
            .status
            .conditions
            .iter()
            .find(|condition| condition.type_ == type_);
        let unchanged = previous
            .is_some_and(|condition| condition.status == status && condition.reason == reason);
        let transition = previous
            .filter(|_| unchanged)
            .map(|condition| condition.last_transition_time.clone())
            .unwrap_or_else(now);
        self.status
            .conditions
            .retain(|condition| condition.type_ != type_);
        self.status.conditions.push(RustQueueCondition {
            type_: type_.into(),
            status: status.into(),
            reason: reason.into(),
            message,
            observed_generation: self.cluster.metadata.generation,
            last_transition_time: transition,
        });
        self.status
            .conditions
            .sort_by(|left, right| left.type_.cmp(&right.type_));
        self
    }

    pub fn operation(mut self, update: OperationUpdate<'_>) -> Self {
        let timestamp = now();
        let current = self.status.current_operation.take();
        let previous_completed_at = current
            .as_ref()
            .filter(|operation| operation.id == update.id && operation.phase == update.phase)
            .and_then(|operation| operation.completed_at.clone());
        let started_at = current
            .as_ref()
            .filter(|operation| operation.id == update.id)
            .map(|operation| operation.started_at.clone())
            .unwrap_or_else(|| timestamp.clone());
        if let Some(previous) = current.filter(|operation| operation.id != update.id) {
            if self
                .status
                .operation_history
                .last()
                .is_none_or(|operation| operation.id != previous.id)
            {
                self.status.operation_history.push(previous);
                let excess = self
                    .status
                    .operation_history
                    .len()
                    .saturating_sub(HISTORY_LIMIT);
                self.status.operation_history.drain(..excess);
            }
        }
        let completed_at = matches!(update.phase, "Completed" | "Failed" | "Blocked")
            .then(|| previous_completed_at.unwrap_or_else(|| timestamp.clone()));
        self.status.current_operation = Some(OperationStatus {
            id: update.id.into(),
            kind: update.kind.into(),
            phase: update.phase.into(),
            target: update.target.into(),
            revision: update.revision.into(),
            message: update.message.into(),
            started_at,
            updated_at: timestamp,
            completed_at,
            previous_image: update.previous_image,
            current_broker: update.current_broker,
        });
        self
    }

    pub fn orphaned_pvcs(mut self, pvcs: Vec<String>) -> Self {
        self.status.orphaned_pvcs = pvcs;
        self
    }

    pub fn build(self) -> RustQueueStatus {
        self.status
    }
}

pub(super) fn operation_id(kind: &str, target: &str, revision: &str) -> String {
    format!(
        "{kind}-{:08x}",
        crc32c::crc32c(format!("{target}\0{revision}").as_bytes())
    )
}

pub(super) fn elapsed_seconds(started_at: &str) -> Option<u64> {
    let started = DateTime::parse_from_rfc3339(started_at).ok()?;
    Utc::now()
        .signed_duration_since(started)
        .to_std()
        .ok()
        .map(|duration| duration.as_secs())
}

fn now() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{BrokerScheduling, RolloutPolicy, RustQueueSpec, WorkloadResources};
    use kube::api::ObjectMeta;
    use std::collections::BTreeMap;

    fn cluster() -> RustQueue {
        RustQueue {
            metadata: ObjectMeta {
                name: Some("queue".into()),
                namespace: Some("test".into()),
                generation: Some(4),
                ..Default::default()
            },
            spec: RustQueueSpec {
                image: "queue:v2".into(),
                image_pull_policy: "IfNotPresent".into(),
                min_brokers: 1,
                max_brokers: 3,
                eligible_node_selector: "queue=true".into(),
                storage_class_name: "ssd".into(),
                storage_size: "10Gi".into(),
                storage_feature_level: 1,
                message_index_cache_bytes: 1024,
                maintenance_startup_delay_seconds: 30,
                node_delivery_inflight_bytes: 4096,
                connection_delivery_inflight_bytes: 1024,
                min_free_bytes: 0,
                disk_high_watermark_percent: 85,
                disk_low_watermark_percent: 75,
                protective_eviction_enabled: false,
                disk_pressure_grace_seconds: 60,
                bootstrap_retention_seconds: 90,
                max_message_bytes: 1024,
                max_topics: 100,
                max_publish_workers: 32,
                publish_worker_idle_seconds: 60,
                detailed_queue_metrics: false,
                max_detailed_metric_series: 1_000,
                registry_secret_name: None,
                console_management_enabled: false,
                client_tls_secret_name: None,
                proxy_node_selector: BTreeMap::new(),
                proxy_tcp_max_connection_age_seconds: 300,
                discovery_replicas: 2,
                maintenance: None,
                rollout: RolloutPolicy::default(),
                broker_scheduling: BrokerScheduling::default(),
                broker_resources: WorkloadResources::default(),
            },
            status: None,
        }
    }

    #[test]
    fn preserves_transition_time_for_an_unchanged_condition() {
        let mut resource = cluster();
        let first = StatusBuilder::new(&resource, 3, 2, 1)
            .condition("Ready", false, "Reconciling", "waiting")
            .build();
        let transition = first.conditions[0].last_transition_time.clone();
        resource.status = Some(first);
        let second = StatusBuilder::new(&resource, 3, 3, 1)
            .condition("Ready", false, "Reconciling", "new detail")
            .build();
        assert_eq!(second.conditions[0].last_transition_time, transition);
    }

    #[test]
    fn moves_replaced_operations_into_bounded_history() {
        let resource = cluster();
        let status = StatusBuilder::new(&resource, 3, 3, 1)
            .operation(OperationUpdate {
                id: "rollout-1",
                kind: "Rollout",
                phase: "Running",
                target: "queue:v2",
                revision: "r2",
                message: "running",
                previous_image: Some("queue:v1".into()),
                current_broker: Some("queue-2".into()),
            })
            .build();
        assert_eq!(status.current_operation.unwrap().id, "rollout-1");
    }

    #[test]
    fn terminal_operation_keeps_its_first_completion_time() {
        let mut resource = cluster();
        let update = || OperationUpdate {
            id: "rollout-1",
            kind: "Rollout",
            phase: "Completed",
            target: "queue:v2",
            revision: "r2",
            message: "done",
            previous_image: Some("queue:v1".into()),
            current_broker: None,
        };
        resource.status = Some(
            StatusBuilder::new(&resource, 3, 3, 1)
                .operation(update())
                .build(),
        );
        resource
            .status
            .as_mut()
            .unwrap()
            .current_operation
            .as_mut()
            .unwrap()
            .completed_at = Some("2000-01-01T00:00:00Z".into());
        let first = resource
            .status
            .as_ref()
            .unwrap()
            .current_operation
            .as_ref()
            .unwrap()
            .completed_at
            .clone();
        let second = StatusBuilder::new(&resource, 3, 3, 1)
            .operation(update())
            .build();
        assert_eq!(second.current_operation.unwrap().completed_at, first);
    }
}
