use super::*;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FederationWriteForward {
    pub route: crate::RouteDecision,
    pub envelope: crate::CommandEnvelope,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FederationFetchForward {
    pub route: crate::RouteDecision,
    pub request: FetchRequest,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FederationReadyForward {
    pub route: crate::RouteDecision,
    pub request: FetchRequest,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FederationTouchForward {
    pub route: crate::RouteDecision,
    pub request: TouchRequest,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FederationReleaseForward {
    pub route: crate::RouteDecision,
    pub request: ReleaseRequest,
}

#[derive(Clone, Debug, Deserialize, Serialize, thiserror::Error)]
pub enum FederationForwardError {
    #[error("stale Catalog route: {0}")]
    StaleRoute(String),
    #[error("invalid federation forward: {0}")]
    Invalid(String),
    #[error("Home Cell is unavailable: {0}")]
    Unavailable(String),
}

impl ClusterRuntime {
    pub(super) async fn write_catalog_partition(
        &self,
        topic: &str,
        operation_id: u64,
        partition: Option<u16>,
        routing_key: Option<&[u8]>,
        command: QueueCommand,
    ) -> anyhow::Result<(QueueResponse, crate::RouteDecision)> {
        for attempt in 0..2 {
            let route = self
                .catalog_route(topic, operation_id, partition, routing_key)
                .await
                .map_err(anyhow::Error::msg)?;
            match self.write_home(route.clone(), command.clone()).await {
                Ok(response) => return Ok((response, route)),
                Err(FederationForwardError::StaleRoute(_)) if attempt == 0 => {
                    self.federation_metrics.retry();
                    self.invalidate_catalog_topic(topic).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
        anyhow::bail!("Catalog route remained stale after refresh")
    }

    pub(super) async fn write_catalog_message(
        &self,
        topic: &str,
        message_id: u64,
        command: QueueCommand,
    ) -> anyhow::Result<QueueResponse> {
        for attempt in 0..2 {
            let route = self
                .catalog_message_route(topic, message_id)
                .await
                .map_err(anyhow::Error::msg)?;
            match self.write_home(route, command.clone()).await {
                Ok(response) => return Ok(response),
                Err(FederationForwardError::StaleRoute(_)) if attempt == 0 => {
                    self.federation_metrics.retry();
                    self.invalidate_catalog_topic(topic).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
        anyhow::bail!("Catalog message route remained stale after refresh")
    }

    pub async fn forwarded_write_local(
        &self,
        forward: FederationWriteForward,
    ) -> Result<QueueResponse, FederationForwardError> {
        forward
            .envelope
            .validate()
            .map_err(FederationForwardError::Invalid)?;
        if !forward
            .envelope
            .command
            .is_scoped_to(crate::COMMAND_SCOPE_PARTITION)
        {
            return Err(FederationForwardError::Invalid(
                "forwarded write is not a partition command".into(),
            ));
        }
        let topic = partition_command_topic(&forward.envelope.command)
            .ok_or_else(|| FederationForwardError::Invalid("command has no topic".into()))?;
        let partition = self.local_partition_for_route(topic, &forward.route)?;
        self.write_partition(&partition, forward.envelope.command)
            .await
            .map_err(|error| FederationForwardError::Unavailable(error.to_string()))
    }

    pub async fn forwarded_fetch_local(
        &self,
        mut forward: FederationFetchForward,
    ) -> Result<FetchResponse, FederationForwardError> {
        let partition = self.local_partition_for_route(&forward.request.topic, &forward.route)?;
        forward.request.partition = Some(partition.number);
        forward.request.wait_ms = forward.request.wait_ms.min(1_000);
        self.fetch_partition(&partition, forward.request)
            .await
            .map_err(|error| FederationForwardError::Unavailable(error.to_string()))
    }

    pub async fn forwarded_ready_local(
        &self,
        mut forward: FederationReadyForward,
    ) -> Result<bool, FederationForwardError> {
        let partition = self.local_partition_for_route(&forward.request.topic, &forward.route)?;
        forward.request.partition = Some(partition.number);
        forward.request.wait_ms = forward.request.wait_ms.min(1_000);
        self.wait_partition_ready(&partition, forward.request)
            .await
            .map_err(|error| FederationForwardError::Unavailable(error.to_string()))
    }

    pub async fn forwarded_touch_local(
        &self,
        forward: FederationTouchForward,
    ) -> Result<(), FederationForwardError> {
        let partition = self.local_partition_for_route(&forward.request.topic, &forward.route)?;
        self.post_to_leader(
            &partition,
            "touch",
            &forward.request,
            crate::network_metrics::RpcKind::Ack,
            INTERNAL_SMALL_FRAME_BYTES,
            INTERNAL_SMALL_FRAME_BYTES,
        )
        .await
        .map_err(|error| FederationForwardError::Unavailable(error.to_string()))
    }

    pub async fn forwarded_release_local(
        &self,
        forward: FederationReleaseForward,
    ) -> Result<(), FederationForwardError> {
        let partition = self.local_partition_for_route(&forward.request.topic, &forward.route)?;
        self.post_to_leader(
            &partition,
            "release",
            &forward.request,
            crate::network_metrics::RpcKind::Ack,
            INTERNAL_SMALL_FRAME_BYTES,
            INTERNAL_SMALL_FRAME_BYTES,
        )
        .await
        .map_err(|error| FederationForwardError::Unavailable(error.to_string()))
    }

    pub(super) async fn write_home(
        &self,
        route: crate::RouteDecision,
        command: QueueCommand,
    ) -> Result<QueueResponse, FederationForwardError> {
        let request = FederationWriteForward {
            route: route.clone(),
            envelope: crate::CommandEnvelope::new(command),
        };
        if route.partition.home_cell == self.metadata.snapshot().cell_id {
            let result = self.forwarded_write_local(request).await;
            if matches!(result, Err(FederationForwardError::StaleRoute(_))) {
                self.federation_metrics.stale_route();
            }
            return result;
        }
        self.post_home(
            route.partition.home_cell,
            "write",
            &request,
            INTERNAL_WRITE_FRAME_BYTES,
            INTERNAL_WRITE_RESPONSE_BYTES,
        )
        .await
    }

    pub(super) async fn fetch_home(
        &self,
        route: crate::RouteDecision,
        request: FetchRequest,
    ) -> Result<FetchResponse, FederationForwardError> {
        let forward = FederationFetchForward {
            route: route.clone(),
            request,
        };
        if route.partition.home_cell == self.metadata.snapshot().cell_id {
            let result = self.forwarded_fetch_local(forward).await;
            if matches!(result, Err(FederationForwardError::StaleRoute(_))) {
                self.federation_metrics.stale_route();
            }
            return result;
        }
        self.post_home(
            route.partition.home_cell,
            "fetch",
            &forward,
            INTERNAL_SMALL_FRAME_BYTES,
            INTERNAL_FETCH_RESPONSE_BYTES,
        )
        .await
    }

    pub(super) async fn ready_home(
        &self,
        route: crate::RouteDecision,
        request: FetchRequest,
    ) -> Result<bool, FederationForwardError> {
        let forward = FederationReadyForward {
            route: route.clone(),
            request,
        };
        if route.partition.home_cell == self.metadata.snapshot().cell_id {
            let result = self.forwarded_ready_local(forward).await;
            if matches!(result, Err(FederationForwardError::StaleRoute(_))) {
                self.federation_metrics.stale_route();
            }
            return result;
        }
        self.post_home(
            route.partition.home_cell,
            "ready",
            &forward,
            INTERNAL_SMALL_FRAME_BYTES,
            INTERNAL_SMALL_FRAME_BYTES,
        )
        .await
    }

    pub(super) async fn touch_home(
        &self,
        route: crate::RouteDecision,
        request: TouchRequest,
    ) -> Result<(), FederationForwardError> {
        let forward = FederationTouchForward {
            route: route.clone(),
            request,
        };
        if route.partition.home_cell == self.metadata.snapshot().cell_id {
            let result = self.forwarded_touch_local(forward).await;
            if matches!(result, Err(FederationForwardError::StaleRoute(_))) {
                self.federation_metrics.stale_route();
            }
            return result;
        }
        self.post_home(
            route.partition.home_cell,
            "touch",
            &forward,
            INTERNAL_SMALL_FRAME_BYTES,
            INTERNAL_SMALL_FRAME_BYTES,
        )
        .await
    }

    pub(super) async fn release_home(
        &self,
        route: crate::RouteDecision,
        request: ReleaseRequest,
    ) -> Result<(), FederationForwardError> {
        let forward = FederationReleaseForward {
            route: route.clone(),
            request,
        };
        if route.partition.home_cell == self.metadata.snapshot().cell_id {
            let result = self.forwarded_release_local(forward).await;
            if matches!(result, Err(FederationForwardError::StaleRoute(_))) {
                self.federation_metrics.stale_route();
            }
            return result;
        }
        self.post_home(
            route.partition.home_cell,
            "release",
            &forward,
            INTERNAL_SMALL_FRAME_BYTES,
            INTERNAL_SMALL_FRAME_BYTES,
        )
        .await
    }

    pub(super) async fn post_home<Req, Resp>(
        &self,
        cell: crate::CellId,
        operation: &str,
        request: &Req,
        request_limit: usize,
        response_limit: usize,
    ) -> Result<Resp, FederationForwardError>
    where
        Req: Serialize + ?Sized,
        Resp: serde::de::DeserializeOwned,
    {
        self.federation_metrics.forwarded();
        let control = self.control.as_ref().ok_or_else(|| {
            FederationForwardError::Unavailable("control plane is disabled".into())
        })?;
        let root = self
            .root_snapshot_cached()
            .await
            .map_err(|error| FederationForwardError::Unavailable(error.to_string()))?;
        let descriptor = root.cells.get(&cell).ok_or_else(|| {
            FederationForwardError::Unavailable(format!("Home Cell {cell} is unknown"))
        })?;
        let mut last_error = None;
        let mut attempted = BTreeSet::new();
        for node_id in descriptor.routers.iter().chain(descriptor.nodes.iter()) {
            if !attempted.insert(*node_id) {
                continue;
            }
            let Some(node) = control.nodes.get(node_id) else {
                continue;
            };
            let response = crate::post_binary_limited::<_, Result<Resp, FederationForwardError>>(
                &self.client,
                format!(
                    "{}/federation/data/{operation}",
                    node.addr.trim_end_matches('/')
                ),
                request,
                request_limit,
                response_limit,
            )
            .await;
            match response {
                Ok(Ok(value)) => return Ok(value),
                Ok(Err(error @ FederationForwardError::StaleRoute(_))) => {
                    self.federation_metrics.stale_route();
                    return Err(error);
                }
                Ok(Err(error @ FederationForwardError::Invalid(_))) => return Err(error),
                Ok(Err(error)) => last_error = Some(error),
                Err(error) => {
                    last_error = Some(FederationForwardError::Unavailable(error.to_string()))
                }
            }
        }
        self.federation_metrics.unavailable();
        Err(last_error.unwrap_or_else(|| {
            FederationForwardError::Unavailable("Home Cell has no reachable router".into())
        }))
    }

    fn local_partition_for_route(
        &self,
        topic: &str,
        route: &crate::RouteDecision,
    ) -> Result<PartitionDescriptor, FederationForwardError> {
        let local_cell = self.metadata.snapshot().cell_id;
        if route.partition.home_cell != local_cell {
            return Err(FederationForwardError::StaleRoute(format!(
                "route targets Cell {}, but this gateway belongs to Cell {}",
                route.partition.home_cell, local_cell
            )));
        }
        self.metadata
            .topic(topic)
            .filter(|topic| topic.state == crate::TopicState::Active)
            .and_then(|topic| {
                topic
                    .partitions
                    .into_iter()
                    .find(|partition| partition.global_id() == route.partition.id)
            })
            .filter(|partition| {
                partition.home_cell == local_cell
                    && partition.lifecycle == crate::PartitionLifecycle::Active
                    && u32::from(partition.number) == route.partition.number
            })
            .ok_or_else(|| {
                FederationForwardError::StaleRoute(
                    "partition is no longer active in the routed Home Cell".into(),
                )
            })
    }
}

fn partition_command_topic(command: &QueueCommand) -> Option<&str> {
    match command {
        QueueCommand::Batch { commands } => {
            let topic = commands.first().and_then(partition_command_topic)?;
            commands
                .iter()
                .all(|command| partition_command_topic(command) == Some(topic))
                .then_some(topic)
        }
        QueueCommand::Publish { topic, .. }
        | QueueCommand::CreateChannel { topic, .. }
        | QueueCommand::DeleteChannel { topic, .. }
        | QueueCommand::EmptyTopic { topic }
        | QueueCommand::EmptyChannel { topic, .. }
        | QueueCommand::PauseChannel { topic, .. }
        | QueueCommand::Finish { topic, .. }
        | QueueCommand::Requeue { topic, .. }
        | QueueCommand::ProtectiveEvict { topic, .. } => Some(topic),
        _ => None,
    }
}
