use super::ClusterRuntime;
use std::collections::BTreeMap;

impl ClusterRuntime {
    pub async fn render_prometheus_metrics(&self) -> String {
        let metadata = self.metadata.snapshot();
        let labels: BTreeMap<_, _> = metadata
            .topics
            .values()
            .flat_map(|topic| {
                topic.partitions.iter().map(|partition| {
                    (
                        partition.group_key(),
                        (topic.name.as_str(), partition.number),
                    )
                })
            })
            .collect();
        let groups: Vec<_> = self.groups.read().await.values().cloned().collect();
        let mut output = String::from(
            "# TYPE rustqueue_group_term gauge\n\
             # TYPE rustqueue_group_has_leader gauge\n\
             # TYPE rustqueue_group_last_log_index gauge\n\
             # TYPE rustqueue_group_last_applied_index gauge\n\
             # TYPE rustqueue_group_apply_lag gauge\n\
             # TYPE rustqueue_group_voters gauge\n\
             # TYPE rustqueue_scrub_records_total counter\n\
             # TYPE rustqueue_replica_repairs_total counter\n\
             # TYPE rustqueue_retention_moved_total counter\n\
             # TYPE rustqueue_retention_failures_total counter\n\
             # TYPE rustqueue_protective_evicted_messages_total counter\n\
             # TYPE rustqueue_protective_evicted_bytes_total counter\n\
             # TYPE rustqueue_disk_pressure_seconds gauge\n",
        );
        for (group_index, group) in groups.into_iter().enumerate() {
            let metrics = group.raft().metrics().borrow().clone();
            let (topic, partition) = labels
                .get(&group.group_key())
                .copied()
                .unwrap_or(("__metadata__", 0));
            let label = format!(
                "group_id=\"{}\",topic=\"{}\",partition=\"{}\"",
                group.group_key(),
                topic,
                partition
            );
            let last_log = metrics.last_log_index.unwrap_or_default();
            let last_applied = metrics.last_applied.map_or(0, |log_id| log_id.index);
            output.push_str(&format!(
                "rustqueue_group_term{{{label}}} {}\n\
                 rustqueue_group_has_leader{{{label}}} {}\n\
                 rustqueue_group_last_log_index{{{label}}} {last_log}\n\
                 rustqueue_group_last_applied_index{{{label}}} {last_applied}\n\
                 rustqueue_group_apply_lag{{{label}}} {}\n\
                 rustqueue_group_voters{{{label}}} {}\n",
                metrics.current_term,
                u8::from(metrics.current_leader.is_some()),
                last_log.saturating_sub(last_applied),
                metrics.membership_config.voter_ids().count(),
            ));
            output.push_str(&group.latency_metrics().render(&label, group_index == 0));
        }
        let clock = self.clock.status();
        output.push_str(
            "# TYPE rustqueue_clock_healthy gauge\n# TYPE rustqueue_clock_offset_ms gauge\n\
             # TYPE rustqueue_node_disk_used_percent gauge\n\
             # TYPE rustqueue_node_disk_free_bytes gauge\n\
             # TYPE rustqueue_node_storage_eligible gauge\n",
        );
        output.push_str(&format!(
            "rustqueue_clock_healthy {}\nrustqueue_clock_offset_ms {}\n\
             rustqueue_scrub_records_total {}\n\
             rustqueue_replica_repairs_total {}\n\
             rustqueue_retention_moved_total {}\n\
             rustqueue_retention_failures_total {}\n\
             rustqueue_protective_evicted_messages_total {}\n\
             rustqueue_protective_evicted_bytes_total {}\n\
             rustqueue_disk_pressure_seconds {}\n",
            u8::from(clock.healthy),
            clock.offset_ms,
            self.scrub_record_count(),
            self.replica_repair_count(),
            self.retention_moved
                .load(std::sync::atomic::Ordering::Relaxed),
            self.retention_failures
                .load(std::sync::atomic::Ordering::Relaxed),
            self.protective_evicted_messages
                .load(std::sync::atomic::Ordering::Relaxed),
            self.protective_evicted_bytes
                .load(std::sync::atomic::Ordering::Relaxed),
            self.disk_pressure_since_ms()
                .map_or(0, |since| crate::wall_time_ms().saturating_sub(since)
                    / 1_000),
        ));
        for (node_id, health) in &metadata.node_health {
            output.push_str(&format!(
                "rustqueue_node_disk_used_percent{{node_id=\"{node_id}\"}} {}\n\
                 rustqueue_node_disk_free_bytes{{node_id=\"{node_id}\"}} {}\n\
                 rustqueue_node_storage_eligible{{node_id=\"{node_id}\"}} {}\n",
                health.disk_used_percent,
                health.disk_free_bytes,
                u8::from(health.storage_eligible),
            ));
        }
        let node_label = format!("node_id=\"{}\"", self.node_id);
        output.push_str(&self.forward_latency.render(
            "rustqueue_gateway_forward_duration_seconds",
            "Gateway forwarding latency in seconds.",
            &node_label,
        ));
        output.push_str(&self.repair_latency.render(
            "rustqueue_repair_duration_seconds",
            "Replica repair latency in seconds.",
            &node_label,
        ));
        output.push_str(&self.federation_metrics.render());
        if let Some(control) = &self.control {
            let is_catalog_leader = control
                .catalog
                .as_ref()
                .is_some_and(|group| group.leader_state().0 == Some(self.node_id));
            if !is_catalog_leader {
                output.push_str(&crate::network_metrics::render_network_metrics());
                return output;
            }
            output.push_str(
                "# TYPE rustqueue_federation_migration_info gauge\n\
                 # TYPE rustqueue_federation_migration_lag_entries gauge\n\
                 # TYPE rustqueue_federation_migration_lag_known gauge\n",
            );
            for operation in control.metadata.catalog_snapshot().migrations.values() {
                let phase = super::federation_metrics::migration_phase(operation.phase);
                let labels = format!(
                    "operation_id=\"{}\",topic=\"{}\",partition=\"{}\",source_cell=\"{}\",target_cell=\"{}\",phase=\"{}\"",
                    operation.operation_id,
                    operation.topic,
                    operation.partition,
                    operation.source,
                    operation.target,
                    phase,
                );
                let (lag, known) = if operation.observed_lag_entries == u64::MAX {
                    (0, 0)
                } else {
                    (operation.observed_lag_entries, 1)
                };
                output.push_str(&format!(
                    "rustqueue_federation_migration_info{{{labels}}} 1\n\
                     rustqueue_federation_migration_lag_entries{{{labels}}} {lag}\n\
                     rustqueue_federation_migration_lag_known{{{labels}}} {known}\n",
                ));
            }
        }
        output.push_str(&crate::network_metrics::render_network_metrics());
        output
    }
}
