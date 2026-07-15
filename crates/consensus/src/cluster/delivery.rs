use super::*;
use crate::network_metrics::RpcKind;
use futures::{future::try_join_all, stream::FuturesUnordered, StreamExt};

impl ClusterRuntime {
    pub async fn fetch(&self, request: FetchRequest) -> anyhow::Result<FetchResponse> {
        self.ensure_clock_safe().map_err(anyhow::Error::msg)?;
        if self.control_plane_enabled() {
            return self.fetch_federated(request).await;
        }
        self.fetch_local_cell(request).await
    }

    async fn fetch_federated(&self, request: FetchRequest) -> anyhow::Result<FetchResponse> {
        for attempt in 0..2 {
            let (routes, active_count) = self
                .catalog_fetch_routes(
                    &request.topic,
                    request.partition_cursor,
                    fetch_scheduler::MAX_READY_PROBES,
                )
                .await
                .map_err(anyhow::Error::msg)?;
            let next_cursor = request.partition_cursor.wrapping_add(routes.len()) % active_count;
            let mut probes = FuturesUnordered::new();
            for route in routes {
                let mut probe = request.clone();
                probe.partition = u16::try_from(route.partition.number).ok();
                probes.push(async move {
                    let ready = self.ready_home(route.clone(), probe).await;
                    (route, ready)
                });
            }
            let mut completed_probe = false;
            let mut stale = false;
            let mut last_error = None;
            while let Some((route, result)) = probes.next().await {
                let ready = match result {
                    Ok(ready) => {
                        completed_probe = true;
                        ready
                    }
                    Err(FederationForwardError::StaleRoute(_)) => {
                        stale = true;
                        continue;
                    }
                    Err(error) => {
                        last_error = Some(error);
                        continue;
                    }
                };
                if !ready {
                    continue;
                }
                let mut fetch = request.clone();
                fetch.partition = u16::try_from(route.partition.number).ok();
                fetch.wait_ms = 0;
                match self.fetch_home(route, fetch).await {
                    Ok(mut response) if !response.deliveries.is_empty() => {
                        response.partition_cursor = next_cursor;
                        record_fetch_metrics(&response);
                        return Ok(response);
                    }
                    Ok(_) => {}
                    Err(FederationForwardError::StaleRoute(_)) => stale = true,
                    Err(error) => last_error = Some(error),
                }
            }
            if stale && attempt == 0 {
                self.federation_metrics.retry();
                self.invalidate_catalog_topic(&request.topic).await;
                continue;
            }
            if !completed_probe {
                if let Some(error) = last_error {
                    return Err(error.into());
                }
            }
            crate::network_metrics::network_metrics().record_fetch(0, 0);
            return Ok(FetchResponse {
                deliveries: Vec::new(),
                partition_cursor: next_cursor,
                error: None,
            });
        }
        anyhow::bail!("Catalog fetch route remained stale after refresh")
    }

    async fn fetch_local_cell(&self, request: FetchRequest) -> anyhow::Result<FetchResponse> {
        let topic = self
            .metadata
            .topic_route(&request.topic)
            .ok_or_else(|| anyhow::anyhow!("topic not found"))?;
        if topic.active_count() == 0 {
            anyhow::bail!("topic has no active partitions");
        }
        let state = self.fetch_scheduler.state(&request.topic, &request.channel);
        let mut claims = state.claim(&topic);
        if claims.is_empty() {
            state
                .wait_for_claim(std::time::Duration::from_millis(request.wait_ms as u64))
                .await;
            claims = state.claim(&topic);
        }
        let next_cursor = request
            .partition_cursor
            .wrapping_add(fetch_scheduler::MAX_READY_PROBES)
            % topic.active_count();
        let mut probes = FuturesUnordered::new();
        for claim in claims {
            let mut probe = request.clone();
            probe.partition = Some(claim.partition.number);
            probes.push(async move {
                let result = self
                    .wait_partition_ready(claim.partition.as_ref(), probe)
                    .await;
                (claim, result)
            });
        }
        let mut completed_probe = false;
        let mut last_error = None;
        while let Some((claim, ready)) = probes.next().await {
            let ready = match ready {
                Ok(ready) => {
                    completed_probe = true;
                    ready
                }
                Err(error) => {
                    last_error = Some(error);
                    continue;
                }
            };
            if !ready {
                continue;
            }
            let mut fetch = request.clone();
            fetch.partition = Some(claim.partition.number);
            fetch.wait_ms = 0;
            match self.fetch_partition(claim.partition.as_ref(), fetch).await {
                Ok(mut response) if !response.deliveries.is_empty() => {
                    state.mark_ready(claim.partition.global_id());
                    response.partition_cursor = next_cursor;
                    record_fetch_metrics(&response);
                    return Ok(response);
                }
                Ok(_) => {}
                Err(error) => last_error = Some(error),
            }
        }
        if !completed_probe {
            if let Some(error) = last_error {
                return Err(error);
            }
        }
        crate::network_metrics::network_metrics().record_fetch(0, 0);
        Ok(FetchResponse {
            deliveries: Vec::new(),
            partition_cursor: next_cursor,
            error: None,
        })
    }

    pub async fn touch(&self, request: TouchRequest) -> anyhow::Result<()> {
        if self.control_plane_enabled() {
            for attempt in 0..2 {
                let route = self
                    .catalog_message_route(&request.topic, request.message_id)
                    .await
                    .map_err(anyhow::Error::msg)?;
                match self.touch_home(route, request.clone()).await {
                    Ok(()) => return Ok(()),
                    Err(FederationForwardError::StaleRoute(_)) if attempt == 0 => {
                        self.federation_metrics.retry();
                        self.invalidate_catalog_topic(&request.topic).await;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            anyhow::bail!("Catalog TOUCH route remained stale after refresh");
        }
        let partition = self.partition_for_message(&request.topic, request.message_id)?;
        if self.leader_routes.prefers_local(&partition, self.node_id) {
            if let Some(group) = self.group(partition.group_key()).await {
                if self
                    .accept_routed(&partition, group.touch_routed_local(request.clone()))?
                    .is_some()
                {
                    return Ok(());
                }
            }
        }
        self.post_to_leader(
            &partition,
            "touch",
            &request,
            RpcKind::Ack,
            INTERNAL_SMALL_FRAME_BYTES,
            INTERNAL_SMALL_FRAME_BYTES,
        )
        .await
    }

    pub async fn release(&self, request: ReleaseRequest) -> anyhow::Result<()> {
        if self.control_plane_enabled() {
            return self.release_federated(request).await;
        }
        self.release_local_cell(request).await
    }

    async fn release_federated(&self, request: ReleaseRequest) -> anyhow::Result<()> {
        for attempt in 0..2 {
            let mut grouped =
                BTreeMap::<crate::GlobalGroupId, (crate::RouteDecision, Vec<u64>)>::new();
            for id in &request.message_ids {
                let route = self
                    .catalog_message_route(&request.topic, *id)
                    .await
                    .map_err(anyhow::Error::msg)?;
                grouped
                    .entry(route.partition.id)
                    .or_insert_with(|| (route, Vec::new()))
                    .1
                    .push(*id);
            }
            let releases = grouped.into_values().map(|(route, message_ids)| {
                self.release_home(
                    route,
                    ReleaseRequest {
                        topic: request.topic.clone(),
                        channel: request.channel.clone(),
                        message_ids,
                    },
                )
            });
            match try_join_all(releases).await {
                Ok(_) => return Ok(()),
                Err(FederationForwardError::StaleRoute(_)) if attempt == 0 => {
                    self.federation_metrics.retry();
                    self.invalidate_catalog_topic(&request.topic).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
        anyhow::bail!("Catalog release route remained stale after refresh")
    }

    async fn release_local_cell(&self, request: ReleaseRequest) -> anyhow::Result<()> {
        let mut grouped: HashMap<crate::GlobalGroupId, (PartitionDescriptor, Vec<u64>)> =
            HashMap::new();
        for id in request.message_ids {
            if let Ok(partition) = self.partition_for_message(&request.topic, id) {
                grouped
                    .entry(partition.global_id())
                    .or_insert_with(|| (partition, Vec::new()))
                    .1
                    .push(id);
            }
        }
        try_join_all(grouped.into_values().map(|(partition, message_ids)| {
            let release = ReleaseRequest {
                topic: request.topic.clone(),
                channel: request.channel.clone(),
                message_ids,
            };
            async move {
                if self.leader_routes.prefers_local(&partition, self.node_id) {
                    if let Some(group) = self.group(partition.group_key()).await {
                        if self
                            .accept_routed(&partition, group.release_routed_local(release.clone()))?
                            .is_some()
                        {
                            return Ok(());
                        }
                    }
                }
                self.post_to_leader(
                    &partition,
                    "release",
                    &release,
                    RpcKind::Ack,
                    INTERNAL_SMALL_FRAME_BYTES,
                    INTERNAL_SMALL_FRAME_BYTES,
                )
                .await
            }
        }))
        .await?;
        Ok(())
    }
}

fn record_fetch_metrics(response: &FetchResponse) {
    let body_bytes = response
        .deliveries
        .iter()
        .map(|delivery| delivery.body.len())
        .sum();
    crate::network_metrics::network_metrics().record_fetch(response.deliveries.len(), body_bytes);
}
