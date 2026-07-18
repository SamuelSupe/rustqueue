use crate::config::Config;
use crate::kubernetes::{broker_from_pod, event_views, pvc_map};
use crate::managed_view;
use crate::model::*;
use crate::resources::{self, ManagedResources};
use crate::state::LiveState;
mod observe;
use anyhow::{Context, Result};
use k8s_openapi::api::core::v1::{Event, PersistentVolumeClaim, Pod};
use kube::api::{Api, ListParams};
use kube::ResourceExt;
use rustqueue_operator::RustQueue;
use rustqueue_telemetry::HistogramSnapshot;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct Collector {
    config: Config,
    client: kube::Client,
    http: reqwest::Client,
    state: Arc<LiveState>,
    mutation_lock: Arc<Mutex<()>>,
    observation_cache: Arc<Mutex<observe::ObservationCache>>,
}

impl Collector {
    pub fn new(
        config: Config,
        client: kube::Client,
        http: reqwest::Client,
        state: Arc<LiveState>,
        mutation_lock: Arc<Mutex<()>>,
    ) -> Self {
        Self {
            config,
            client,
            http,
            state,
            mutation_lock,
            observation_cache: Arc::new(Mutex::new(Default::default())),
        }
    }

    pub async fn run(self) {
        let mut interval = tokio::time::interval(self.config.poll_interval);
        loop {
            interval.tick().await;
            match self.collect().await {
                Ok((snapshot, counters)) => self.state.publish(snapshot, counters),
                Err(error) => {
                    tracing::warn!(%error, "console collection failed");
                    self.state.record_error(error.to_string());
                }
            }
        }
    }

    async fn collect(&self) -> Result<(Snapshot, RawCounters)> {
        let cluster = Api::<RustQueue>::namespaced(self.client.clone(), &self.config.namespace)
            .get(&self.config.queue_name)
            .await
            .with_context(|| format!("read RustQueue {}", self.config.queue_name))?;
        let selector = format!(
            "app.kubernetes.io/instance={},app.kubernetes.io/component=broker",
            self.config.queue_name
        );
        let pods = Api::<Pod>::namespaced(self.client.clone(), &self.config.namespace)
            .list(&ListParams::default().labels(&selector))
            .await?
            .items;
        let pvcs =
            Api::<PersistentVolumeClaim>::namespaced(self.client.clone(), &self.config.namespace)
                .list(&ListParams::default().labels(&selector))
                .await?
                .items;
        let events = Api::<Event>::namespaced(self.client.clone(), &self.config.namespace)
            .list(&ListParams::default().limit(500))
            .await?
            .items;

        let pvc_map = pvc_map(pvcs);
        let brokers = pods
            .into_iter()
            .map(|pod| broker_from_pod(pod, &pvc_map))
            .collect::<Vec<_>>();
        let managed = if self.config.management_enabled {
            resources::list(
                &self.client,
                &self.config.namespace,
                &self.config.queue_name,
            )
            .await?
        } else {
            ManagedResources::default()
        };
        let brokers = self.observe_brokers(brokers, &managed).await?;
        let (mut snapshot, counters) = build_snapshot(&cluster, brokers, events);
        if self.config.management_enabled {
            let _catalog_guard = self.mutation_lock.lock().await;
            let managed = resources::reconcile(&self.client, &cluster, &snapshot.topics).await?;
            managed_view::merge(&mut snapshot.topics, &managed);
            snapshot.management.enabled = true;
            snapshot.management.crd_fresh = true;
            drop(_catalog_guard);
            snapshot.management.registry_available = self.registry_available().await;
            if !snapshot.management.registry_available {
                snapshot.complete = false;
                snapshot.errors.push("Registry health check failed".into());
            }
        }
        Ok((snapshot, counters))
    }

    async fn registry_available(&self) -> bool {
        self.http
            .get(format!(
                "http://{}-discovery:4161/v1/health",
                self.config.queue_name
            ))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
    }
}

fn build_snapshot(
    cluster: &RustQueue,
    brokers: Vec<BrokerView>,
    events: Vec<Event>,
) -> (Snapshot, RawCounters) {
    let status = cluster.status.clone().unwrap_or_default();
    let mut summary = SummaryView::default();
    let mut storage = StorageView::default();
    let mut counters = RawCounters {
        at_ms: now_ms(),
        ..Default::default()
    };
    let mut errors = Vec::new();
    let mut topics = BTreeMap::<String, TopicAggregate>::new();

    for broker in &brokers {
        let Some(observation) = &broker.observation else {
            errors.push(format!(
                "{}: {}",
                broker.name,
                broker.error.as_deref().unwrap_or("observation unavailable")
            ));
            continue;
        };
        counters.membership = counters.membership.rotate_left(7) ^ observation.node.id;
        accumulate_runtime(&mut summary, &mut counters, &observation.runtime);
        accumulate_storage(&mut storage, &broker.name, observation);
        for topic in &observation.queue.topics {
            topics
                .entry(topic.name.clone())
                .or_default()
                .add(&broker.name, topic);
        }
    }
    let topics: Vec<_> = topics.into_values().map(TopicAggregate::finish).collect();
    summary.stored_messages = topics.iter().map(|topic| topic.stored_messages).sum();
    summary.depth = topics
        .iter()
        .flat_map(|topic| &topic.channels)
        .map(|channel| channel.depth)
        .sum();
    summary.in_flight = topics
        .iter()
        .flat_map(|topic| &topic.channels)
        .map(|channel| channel.in_flight)
        .sum();
    summary.deferred = topics
        .iter()
        .flat_map(|topic| &topic.channels)
        .map(|channel| channel.deferred)
        .sum();
    storage.used_percent = if storage.total_bytes == 0 {
        0.0
    } else {
        storage.total_bytes.saturating_sub(storage.available_bytes) as f64 * 100.0
            / storage.total_bytes as f64
    };
    let complete =
        !brokers.is_empty() && errors.is_empty() && brokers.len() as i32 >= status.desired_brokers;
    let anomalies = anomalies(cluster, &brokers, &errors);
    let snapshot = Snapshot {
        schema_version: 1,
        collected_at_ms: counters.at_ms,
        complete,
        errors,
        cluster: ClusterView {
            name: cluster.name_any(),
            namespace: cluster.namespace().unwrap_or_default(),
            phase: status.phase.clone(),
            message: status.message.clone(),
            desired_brokers: status.desired_brokers,
            ready_brokers: status.ready_brokers,
            active_storage_feature_level: status.active_storage_feature_level,
            observed_generation: status.observed_generation,
            generation: cluster.metadata.generation,
            spec: serde_json::to_value(&cluster.spec).unwrap_or_default(),
        },
        summary,
        brokers,
        topics,
        storage,
        conditions: status.conditions,
        current_operation: status.current_operation,
        operation_history: status.operation_history,
        events: event_views(cluster, events),
        anomalies,
        history: Vec::new(),
        management: ManagementView::default(),
    };
    (snapshot, counters)
}

fn accumulate_runtime(
    summary: &mut SummaryView,
    counters: &mut RawCounters,
    value: &RuntimeCounters,
) {
    summary.connections = summary.connections.saturating_add(value.tcp_connections);
    summary.retry_total = summary.retry_total.saturating_add(value.requeued_messages);
    summary.dead_letter_total = summary
        .dead_letter_total
        .saturating_add(value.dead_letter_messages);
    summary.throttled_total = summary
        .throttled_total
        .saturating_add(value.publish_throttled_requests);
    counters.publish_messages = counters
        .publish_messages
        .saturating_add(value.publish_messages);
    counters.delivered_messages = counters
        .delivered_messages
        .saturating_add(value.delivered_messages);
    counters.finished_messages = counters
        .finished_messages
        .saturating_add(value.finished_messages);
    counters.publish_bytes = counters.publish_bytes.saturating_add(value.publish_bytes);
}

fn accumulate_storage(storage: &mut StorageView, name: &str, value: &BrokerObservation) {
    storage.total_bytes = storage.total_bytes.saturating_add(value.disk.total_bytes);
    storage.available_bytes = storage
        .available_bytes
        .saturating_add(value.disk.available_bytes);
    storage.segment_count = storage
        .segment_count
        .saturating_add(value.storage.segment_count);
    storage.segment_bytes = storage
        .segment_bytes
        .saturating_add(value.storage.segment_bytes);
    if value.disk.pressure {
        storage.pressure_brokers.push(name.into());
    }
    merge_histogram(&mut storage.fsync, &value.queue.latency.fsync);
    merge_histogram(
        &mut storage.group_commit_wait,
        &value.queue.latency.group_commit_wait,
    );
    merge_histogram(&mut storage.payload_read, &value.queue.latency.payload_read);
    merge_histogram(&mut storage.scrub, &value.queue.latency.scrub);
    merge_histogram(&mut storage.gc, &value.queue.latency.gc);
}

fn merge_histogram(target: &mut HistogramSnapshot, value: &HistogramSnapshot) {
    if target.buckets.len() < value.buckets.len() {
        target.buckets.resize(value.buckets.len(), 0);
    }
    for (target, value) in target.buckets.iter_mut().zip(&value.buckets) {
        *target = target.saturating_add(*value);
    }
    target.count = target.count.saturating_add(value.count);
    target.sum_us = target.sum_us.saturating_add(value.sum_us);
}

#[derive(Default)]
struct TopicAggregate {
    name: String,
    owners: BTreeSet<String>,
    paused: bool,
    stored_messages: u64,
    segment_count: u64,
    segment_bytes: u64,
    channels: BTreeMap<String, ChannelAggregate>,
}

impl TopicAggregate {
    fn add(&mut self, owner: &str, topic: &rustqueue_queue::TopicStats) {
        self.name = topic.name.clone();
        self.owners.insert(owner.into());
        self.paused |= topic.paused;
        self.stored_messages = self.stored_messages.saturating_add(topic.message_count);
        self.segment_count = self.segment_count.saturating_add(topic.segment_count);
        self.segment_bytes = self.segment_bytes.saturating_add(topic.segment_bytes);
        for channel in &topic.channels {
            self.channels
                .entry(channel.name.clone())
                .or_default()
                .add(owner, channel);
        }
    }

    fn finish(self) -> TopicView {
        TopicView {
            name: self.name,
            owners: self.owners.into_iter().collect(),
            paused: self.paused,
            stored_messages: self.stored_messages,
            segment_count: self.segment_count,
            segment_bytes: self.segment_bytes,
            channels: self
                .channels
                .into_values()
                .map(ChannelAggregate::finish)
                .collect(),
            managed_phase: String::new(),
            management_revision: 0,
            tombstone_until_ms: None,
            management_error: None,
            resource_uid: String::new(),
            resource_version: String::new(),
        }
    }
}

#[derive(Default)]
struct ChannelAggregate {
    name: String,
    owners: BTreeSet<String>,
    depth: u64,
    in_flight: u64,
    deferred: u64,
    ack_gap: u64,
    paused: bool,
    ephemeral: bool,
}

impl ChannelAggregate {
    fn add(&mut self, owner: &str, channel: &rustqueue_queue::ChannelStats) {
        self.name = channel.name.clone();
        self.owners.insert(owner.into());
        self.depth = self.depth.saturating_add(channel.depth);
        self.in_flight = self.in_flight.saturating_add(channel.in_flight_count);
        self.deferred = self.deferred.saturating_add(channel.deferred_count);
        self.ack_gap = self.ack_gap.saturating_add(channel.ack_gap);
        self.paused |= channel.paused;
        self.ephemeral |= channel.ephemeral;
    }

    fn finish(self) -> ChannelView {
        ChannelView {
            name: self.name,
            owners: self.owners.into_iter().collect(),
            depth: self.depth,
            in_flight: self.in_flight,
            deferred: self.deferred,
            ack_gap: self.ack_gap,
            paused: self.paused,
            ephemeral: self.ephemeral,
            managed_phase: String::new(),
            management_revision: 0,
            tombstone_until_ms: None,
            management_error: None,
            resource_uid: String::new(),
            resource_version: String::new(),
        }
    }
}

fn anomalies(cluster: &RustQueue, brokers: &[BrokerView], errors: &[String]) -> Vec<AnomalyView> {
    let mut output = Vec::new();
    if cluster
        .status
        .as_ref()
        .is_none_or(|status| status.phase != "Ready")
    {
        output.push(AnomalyView {
            severity: "warning".into(),
            code: "cluster_not_ready".into(),
            subject: cluster.name_any(),
            detail: cluster
                .status
                .as_ref()
                .map(|status| status.message.clone())
                .unwrap_or_else(|| "Operator status is not available".into()),
        });
    }
    for broker in brokers {
        if broker.error.is_some() {
            output.push(AnomalyView {
                severity: "critical".into(),
                code: "broker_unreachable".into(),
                subject: broker.name.clone(),
                detail: broker.error.clone().unwrap_or_default(),
            });
        } else if broker
            .observation
            .as_ref()
            .is_some_and(|observation| observation.disk.pressure)
        {
            output.push(AnomalyView {
                severity: "critical".into(),
                code: "disk_pressure".into(),
                subject: broker.name.clone(),
                detail: "Publishing is protected by the disk watermark".into(),
            });
        } else if broker
            .observation
            .as_ref()
            .is_some_and(|observation| !observation.readiness.storage_healthy)
        {
            output.push(AnomalyView {
                severity: "critical".into(),
                code: "storage_unhealthy".into(),
                subject: broker.name.clone(),
                detail: "Local storage has been isolated".into(),
            });
        }
    }
    if !errors.is_empty() && output.is_empty() {
        output.push(AnomalyView {
            severity: "warning".into(),
            code: "partial_snapshot".into(),
            subject: cluster.name_any(),
            detail: errors.join("; "),
        });
    }
    output
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
