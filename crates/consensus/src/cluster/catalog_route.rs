use super::control_plane::CachedTopic;
use super::*;

impl ClusterRuntime {
    pub async fn catalog_snapshot_routed_local(&self) -> RoutedResponse<crate::CatalogState> {
        let Some(control) = &self.control else {
            return RoutedResponse::failed("independent Catalog is disabled", None, 0);
        };
        let Some(group) = &control.catalog else {
            return RoutedResponse::failed("Catalog group is not hosted here", None, 0);
        };
        let (leader, term) = group.leader_state();
        if leader != Some(self.node_id) {
            return RoutedResponse::not_leader(leader, term);
        }
        if let Err(error) = group.ensure_quorum_local().await {
            return RoutedResponse::failed(error.to_string(), leader, term);
        }
        RoutedResponse::success(control.metadata.catalog_snapshot(), self.node_id, term)
    }

    pub async fn catalog_snapshot_fresh(&self) -> anyhow::Result<crate::CatalogState> {
        let control = self
            .control
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("independent Catalog is disabled"))?;
        if let Some(group) = &control.catalog {
            if group.leader_state().0 == Some(self.node_id) {
                group.ensure_quorum_local().await?;
                return Ok(control.metadata.catalog_snapshot());
            }
        }
        let mut candidate = control
            .catalog
            .as_ref()
            .and_then(|group| group.leader_state().0)
            .or_else(|| control.voters.iter().next().copied());
        for _ in 0..3 {
            let node_id = candidate.ok_or_else(|| anyhow::anyhow!("Catalog has no voter"))?;
            let node = control
                .nodes
                .get(&node_id)
                .ok_or_else(|| anyhow::anyhow!("Catalog voter {node_id} is unknown"))?;
            let response: RoutedResponse<crate::CatalogState> = crate::post_binary_limited(
                &self.client,
                format!(
                    "{}/federation/catalog/snapshot",
                    node.addr.trim_end_matches('/')
                ),
                &(),
                INTERNAL_SMALL_FRAME_BYTES,
                INTERNAL_CATALOG_FRAME_BYTES,
            )
            .await?;
            match response {
                RoutedResponse::Success { value, .. } => return Ok(value),
                RoutedResponse::NotLeader(redirect) => candidate = redirect.leader_id,
                RoutedResponse::Failed { message, .. } => return Err(anyhow::anyhow!(message)),
            }
        }
        anyhow::bail!("Catalog leader changed repeatedly")
    }

    pub async fn catalog_topic_routed_local(
        &self,
        topic: &str,
    ) -> RoutedResponse<Option<crate::CatalogTopic>> {
        let Some(control) = &self.control else {
            return RoutedResponse::failed("independent Catalog is disabled", None, 0);
        };
        let Some(group) = &control.catalog else {
            return RoutedResponse::failed("Catalog group is not hosted here", None, 0);
        };
        let (leader, term) = group.leader_state();
        if leader != Some(self.node_id) {
            return RoutedResponse::not_leader(leader, term);
        }
        if let Err(error) = group.ensure_quorum_local().await {
            return RoutedResponse::failed(error.to_string(), leader, term);
        }
        let topic = control
            .metadata
            .catalog_snapshot()
            .topics
            .get(topic)
            .cloned();
        RoutedResponse::success(topic, self.node_id, term)
    }

    pub async fn catalog_route(
        &self,
        topic: &str,
        operation_id: u64,
        partition: Option<u16>,
        routing_key: Option<&[u8]>,
    ) -> Result<crate::RouteDecision, crate::RouteError> {
        let control = self
            .control
            .as_ref()
            .ok_or(crate::RouteError::CatalogUnavailable {
                retry_after_ms: 1_000,
            })?;
        let topic_state = self
            .cached_catalog_topic(topic)
            .await
            .map_err(|_| crate::RouteError::CatalogUnavailable {
                retry_after_ms: control.retry_after_ms,
            })?
            .ok_or(crate::RouteError::TopicNotFound)?;
        route_cached_topic(
            &topic_state,
            operation_id,
            partition,
            routing_key,
            self.metadata.snapshot().cell_id,
            control.retry_after_ms,
        )
    }

    pub(super) async fn catalog_fetch_routes(
        &self,
        topic: &str,
        cursor: usize,
        limit: usize,
    ) -> Result<(Vec<crate::RouteDecision>, usize), crate::RouteError> {
        let control = self
            .control
            .as_ref()
            .ok_or(crate::RouteError::CatalogUnavailable {
                retry_after_ms: 1_000,
            })?;
        let topic = self
            .cached_catalog_topic(topic)
            .await
            .map_err(|_| crate::RouteError::CatalogUnavailable {
                retry_after_ms: control.retry_after_ms,
            })?
            .ok_or(crate::RouteError::TopicNotFound)?;
        if topic.deleting {
            return Err(crate::RouteError::TopicDeleting);
        }
        let mut active = topic
            .partition_numbers
            .values()
            .filter_map(|id| topic.partitions.get(id))
            .filter(|partition| partition.lifecycle == crate::PartitionHomeLifecycle::Active)
            .collect::<Vec<_>>();
        active.sort_by_key(|partition| (partition.number, partition.id));
        if active.is_empty() {
            return Err(crate::RouteError::NoActivePartition);
        }
        let take = limit.min(active.len());
        let total = active.len();
        let routes = (0..take)
            .map(|offset| {
                let partition = active[(cursor + offset) % active.len()].clone();
                crate::RouteDecision {
                    direct: partition.home_cell == self.metadata.snapshot().cell_id,
                    partition,
                    topology_generation: topic.topology_generation,
                    routing_epoch: topic.routing_epoch,
                }
            })
            .collect();
        Ok((routes, total))
    }

    pub(super) async fn catalog_message_route(
        &self,
        topic: &str,
        message_id: u64,
    ) -> Result<crate::RouteDecision, crate::RouteError> {
        let control = self
            .control
            .as_ref()
            .ok_or(crate::RouteError::CatalogUnavailable {
                retry_after_ms: 1_000,
            })?;
        let topic = self
            .cached_catalog_topic(topic)
            .await
            .map_err(|_| crate::RouteError::CatalogUnavailable {
                retry_after_ms: control.retry_after_ms,
            })?
            .ok_or(crate::RouteError::TopicNotFound)?;
        if topic.deleting {
            return Err(crate::RouteError::TopicDeleting);
        }
        let wire_slot = (message_id >> 48) as u16;
        let partition = topic
            .partitions
            .values()
            .find(|partition| partition.wire_slot == wire_slot)
            .ok_or(crate::RouteError::PartitionNotActive)?;
        route_partition(
            &topic,
            partition,
            self.metadata.snapshot().cell_id,
            control.retry_after_ms,
        )
    }

    pub async fn catalog_topic_descriptor(
        &self,
        topic: &str,
    ) -> Result<Option<crate::CatalogTopic>, crate::RouteError> {
        let control = self
            .control
            .as_ref()
            .ok_or(crate::RouteError::CatalogUnavailable {
                retry_after_ms: 1_000,
            })?;
        self.cached_catalog_topic(topic)
            .await
            .map(|topic| topic.map(|topic| (*topic).clone()))
            .map_err(|_| crate::RouteError::CatalogUnavailable {
                retry_after_ms: control.retry_after_ms,
            })
    }

    pub(super) async fn invalidate_catalog_topic(&self, topic: &str) {
        if let Some(control) = &self.control {
            control.topic_cache.write().await.remove(topic);
        }
    }

    async fn cached_catalog_topic(
        &self,
        topic: &str,
    ) -> anyhow::Result<Option<Arc<crate::CatalogTopic>>> {
        let control = self
            .control
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("independent Catalog is disabled"))?;
        let (fresh, stale) = {
            let cache = control.topic_cache.read().await;
            let cached = cache.get(topic);
            (
                cached
                    .filter(|cached| cached.expires_at > std::time::Instant::now())
                    .map(|cached| Arc::clone(&cached.topic)),
                cached.map(|cached| Arc::clone(&cached.topic)),
            )
        };
        if let Some(topic) = fresh {
            return Ok(Some(topic));
        }
        let topic_state = match self.fetch_catalog_topic(topic).await {
            Ok(Some(topic_state)) => topic_state,
            Ok(None) => {
                control.topic_cache.write().await.remove(topic);
                return Ok(None);
            }
            Err(error) => {
                if let Some(stale) = stale {
                    self.federation_metrics.stale_cache_used();
                    tracing::warn!(topic, %error, "using stale Catalog route while quorum is unavailable");
                    return Ok(Some(stale));
                }
                return Err(error);
            }
        };
        let topic_state = Arc::new(topic_state);
        control.topic_cache.write().await.insert(
            topic.to_owned(),
            CachedTopic {
                expires_at: std::time::Instant::now() + control.route_cache,
                topic: Arc::clone(&topic_state),
            },
        );
        Ok(Some(topic_state))
    }

    async fn fetch_catalog_topic(
        &self,
        topic: &str,
    ) -> anyhow::Result<Option<crate::CatalogTopic>> {
        let control = self
            .control
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("independent Catalog is disabled"))?;
        if let Some(group) = &control.catalog {
            if group.leader_state().0 == Some(self.node_id) {
                group.ensure_quorum_local().await?;
                return Ok(control
                    .metadata
                    .catalog_snapshot()
                    .topics
                    .get(topic)
                    .cloned());
            }
        }
        let mut candidate = control
            .catalog
            .as_ref()
            .and_then(|group| group.leader_state().0)
            .or_else(|| control.voters.iter().next().copied());
        for _ in 0..3 {
            let node_id = candidate.ok_or_else(|| anyhow::anyhow!("Catalog has no voter"))?;
            let node = control
                .nodes
                .get(&node_id)
                .ok_or_else(|| anyhow::anyhow!("Catalog voter {node_id} is unknown"))?;
            let response: RoutedResponse<Option<crate::CatalogTopic>> = crate::post_binary_limited(
                &self.client,
                format!(
                    "{}/federation/catalog/topics/{topic}",
                    node.addr.trim_end_matches('/')
                ),
                &(),
                INTERNAL_SMALL_FRAME_BYTES,
                crate::INTERNAL_CATALOG_FRAME_BYTES,
            )
            .await?;
            match response {
                RoutedResponse::Success { value, .. } => return Ok(value),
                RoutedResponse::NotLeader(redirect) => candidate = redirect.leader_id,
                RoutedResponse::Failed { message, .. } => return Err(anyhow::anyhow!(message)),
            }
        }
        anyhow::bail!("Catalog leader changed repeatedly")
    }
}

fn route_cached_topic(
    topic: &crate::CatalogTopic,
    operation_id: u64,
    requested: Option<u16>,
    routing_key: Option<&[u8]>,
    preferred_cell: crate::CellId,
    retry_after_ms: u64,
) -> Result<crate::RouteDecision, crate::RouteError> {
    if topic.deleting {
        return Err(crate::RouteError::TopicDeleting);
    }
    let partition = if let Some(number) = requested {
        topic
            .partition_numbers
            .get(&u32::from(number))
            .and_then(|id| topic.partitions.get(id))
            .ok_or(crate::RouteError::PartitionNotActive)?
    } else if let Some(key) = routing_key {
        let bucket = (crc32c::crc32c(key) % crate::VIRTUAL_BUCKET_COUNT) as u16;
        let range = topic
            .bucket_ranges
            .binary_search_by(|range| {
                if bucket < range.start {
                    std::cmp::Ordering::Greater
                } else if bucket > range.end {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .ok()
            .and_then(|index| topic.bucket_ranges.get(index))
            .ok_or(crate::RouteError::NoActivePartition)?;
        topic
            .partitions
            .get(&range.partition)
            .ok_or(crate::RouteError::PartitionNotActive)?
    } else {
        let active = topic
            .partition_numbers
            .values()
            .filter_map(|id| topic.partitions.get(id))
            .filter(|partition| partition.lifecycle == crate::PartitionHomeLifecycle::Active)
            .collect::<Vec<_>>();
        if active.is_empty() {
            return Err(crate::RouteError::NoActivePartition);
        }
        let preferred = active
            .iter()
            .copied()
            .filter(|partition| partition.home_cell == preferred_cell)
            .collect::<Vec<_>>();
        let candidates = if preferred.is_empty() {
            &active
        } else {
            &preferred
        };
        candidates[operation_id as usize % candidates.len()]
    };
    route_partition(topic, partition, preferred_cell, retry_after_ms)
}

fn route_partition(
    topic: &crate::CatalogTopic,
    partition: &crate::PartitionHome,
    preferred_cell: crate::CellId,
    retry_after_ms: u64,
) -> Result<crate::RouteDecision, crate::RouteError> {
    match partition.lifecycle {
        crate::PartitionHomeLifecycle::Migrating => {
            Err(crate::RouteError::MigrationFenced { retry_after_ms })
        }
        crate::PartitionHomeLifecycle::Active => Ok(crate::RouteDecision {
            partition: partition.clone(),
            topology_generation: topic.topology_generation,
            routing_epoch: topic.routing_epoch,
            direct: partition.home_cell == preferred_cell,
        }),
        _ => Err(crate::RouteError::PartitionNotActive),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cached_route_rejects_migrating_partition() {
        let id = crate::GlobalGroupId::new(crate::CellId::BOOTSTRAP, 7).unwrap();
        let partition = crate::PartitionHome {
            id,
            number: 0,
            wire_slot: 1,
            wire_incarnation: 1,
            home_cell: crate::CellId::BOOTSTRAP,
            lifecycle: crate::PartitionHomeLifecycle::Migrating,
            routing_epoch: 2,
        };
        let topic = crate::CatalogTopic {
            name: "events".into(),
            deleting: false,
            routing_mode: crate::RoutingMode::Elastic,
            topology_generation: 1,
            routing_epoch: 2,
            catalog_revision: 1,
            feature_level: crate::FEATURE_LEVEL_FEDERATED_SCHEMA,
            paused: false,
            channels: BTreeMap::new(),
            channel_tombstones: BTreeMap::new(),
            partitions: BTreeMap::from([(id, partition)]),
            partition_numbers: BTreeMap::from([(0, id)]),
            bucket_ranges: Vec::new(),
            home_cells: BTreeSet::from([crate::CellId::BOOTSTRAP]),
        };
        assert!(matches!(
            route_cached_topic(&topic, 0, Some(0), None, crate::CellId::BOOTSTRAP, 1000),
            Err(crate::RouteError::MigrationFenced { .. })
        ));
    }
}
