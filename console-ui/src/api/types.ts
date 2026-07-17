export interface Histogram {
  buckets: number[];
  count: number;
  sum_us: number;
}

export interface RuntimeCounters {
  tcp_connections: number;
  publish_messages: number;
  publish_bytes: number;
  publish_inflight_bytes: number;
  publish_throttled_requests: number;
  delivered_messages: number;
  finished_messages: number;
  requeued_messages: number;
  dead_letter_messages: number;
  protocol_errors: number;
  storage_errors: number;
  protective_evictions: number;
  protective_evicted_messages: number;
}

export interface ChannelStats {
  name: string;
  depth: number;
  in_flight_count: number;
  deferred_count: number;
  paused: boolean;
  ephemeral: boolean;
  ack_cursor: number;
  ack_gap: number;
}

export interface TopicStats {
  name: string;
  paused: boolean;
  message_count: number;
  segment_count: number;
  segment_bytes: number;
  channels: ChannelStats[];
}

export interface BrokerObservation {
  registry_revision: number;
  node: {
    id: number;
    address: string;
    version: string;
    data_format: number;
    compatibility: Record<string, unknown>;
  };
  readiness: {
    process_ready: boolean;
    storage_healthy: boolean;
    disk_ready: boolean;
    publish_ready: boolean;
    consume_ready: boolean;
    draining: boolean;
    management_fences_ready: boolean;
  };
  disk: {
    total_bytes: number;
    available_bytes: number;
    used_percent: number;
    pressure: boolean;
    high_watermark_percent: number;
    low_watermark_percent: number;
    min_free_bytes: number;
    protective_eviction_enabled: boolean;
  };
  storage: { segment_count: number; segment_bytes: number };
  runtime: RuntimeCounters;
  queue: {
    publish_group_commit: {
      commits: number;
      requests: number;
      max_batch_requests: number;
      active_workers?: number;
      retired_workers?: number;
      rejected_workers?: number;
    };
    latency: Record<string, Histogram>;
    topics: TopicStats[];
  };
  limits: { max_message_bytes: number; max_backlog_messages: number; max_connections: number };
}

export interface Pvc {
  name: string;
  phase: string;
  requested: string;
  capacity: string;
  storage_class: string;
}

export interface Broker {
  name: string;
  node_name: string;
  pod_ip: string;
  phase: string;
  ready: boolean;
  restarts: number;
  image: string;
  image_id: string;
  started_at?: string;
  pvc?: Pvc;
  observation?: BrokerObservation;
  error?: string;
}

export interface Channel {
  name: string;
  owners: string[];
  depth: number;
  in_flight: number;
  deferred: number;
  ack_gap: number;
  paused: boolean;
  ephemeral: boolean;
  managed_phase: string;
  management_revision: number;
  tombstone_until_ms?: number;
  management_error?: string;
  resource_uid: string;
  resource_version: string;
}

export interface Topic {
  name: string;
  owners: string[];
  paused: boolean;
  stored_messages: number;
  segment_count: number;
  segment_bytes: number;
  channels: Channel[];
  managed_phase: string;
  management_revision: number;
  tombstone_until_ms?: number;
  management_error?: string;
  resource_uid: string;
  resource_version: string;
}

export interface Condition {
  type: string;
  status: string;
  reason: string;
  message: string;
  observedGeneration?: number;
  lastTransitionTime: string;
}

export interface Operation {
  id: string;
  kind: string;
  phase: string;
  target: string;
  revision: string;
  message: string;
  startedAt: string;
  updatedAt: string;
  completedAt?: string;
  previousImage?: string;
  currentBroker?: string;
}

export interface EventItem {
  at: string;
  type_: string;
  reason: string;
  message: string;
  object: string;
  count: number;
}

export interface TrendSample {
  at_ms: number;
  publish_per_second: number;
  deliver_per_second: number;
  finish_per_second: number;
  publish_bytes_per_second: number;
  depth: number;
  in_flight: number;
  disk_used_percent: number;
}

export interface Snapshot {
  schema_version: number;
  collected_at_ms: number;
  complete: boolean;
  errors: string[];
  cluster: {
    name: string;
    namespace: string;
    phase: string;
    message: string;
    desired_brokers: number;
    ready_brokers: number;
    active_storage_feature_level: number;
    observed_generation?: number;
    generation?: number;
    spec: Record<string, unknown>;
  };
  summary: {
    stored_messages: number;
    depth: number;
    in_flight: number;
    deferred: number;
    connections: number;
    publish_per_second: number;
    deliver_per_second: number;
    finish_per_second: number;
    publish_bytes_per_second: number;
    retry_total: number;
    dead_letter_total: number;
    throttled_total: number;
  };
  brokers: Broker[];
  topics: Topic[];
  storage: {
    total_bytes: number;
    available_bytes: number;
    used_percent: number;
    segment_count: number;
    segment_bytes: number;
    pressure_brokers: string[];
    fsync: Histogram;
    group_commit_wait: Histogram;
    payload_read: Histogram;
    scrub: Histogram;
    gc: Histogram;
  };
  conditions: Condition[];
  current_operation?: Operation;
  operation_history: Operation[];
  events: EventItem[];
  anomalies: Array<{ severity: string; code: string; subject: string; detail: string }>;
  history: TrendSample[];
  management: {
    enabled: boolean;
    registry_available: boolean;
    crd_fresh: boolean;
  };
}

export interface ManagementStatus {
  enabled: boolean;
  unlocked: boolean;
  expires_at_ms?: number;
  csrf_token?: string;
  confirmation: string;
}

export interface ManagementAction {
  kind: 'topic' | 'channel';
  action: 'create' | 'pause' | 'unpause' | 'empty' | 'delete' | 'retry';
  topic: string;
  channel?: string;
}

export interface ActionPreview {
  action_token: string;
  expires_at_ms: number;
  confirmation_required?: string;
  impact: {
    owners: string[];
    stored_messages: number;
    depth: number;
    in_flight: number;
    deferred: number;
    connections: number;
    warnings: string[];
  };
}
