use super::*;

#[derive(Clone, Debug, Default)]
pub struct ControlPlaneOptions {
    pub enabled: bool,
    pub nodes: BTreeMap<NodeId, crate::NodeDescriptor>,
    pub voters: BTreeSet<NodeId>,
    pub max_home_cells_per_topic: usize,
    pub route_cache_ms: u64,
    pub retry_after_ms: u64,
}

pub(super) struct CachedTopic {
    pub expires_at: std::time::Instant,
    pub topic: Arc<crate::CatalogTopic>,
}

pub(super) struct ControlPlane {
    pub metadata: Arc<MetadataCatalog>,
    pub nodes: BTreeMap<NodeId, BasicNode>,
    pub voters: BTreeSet<NodeId>,
    pub root: Option<Arc<ConsensusNode>>,
    pub catalog: Option<Arc<ConsensusNode>>,
    pub topic_cache: tokio::sync::RwLock<HashMap<String, CachedTopic>>,
    pub root_cache: tokio::sync::RwLock<Arc<crate::FederationRoot>>,
    pub route_cache: std::time::Duration,
    pub retry_after_ms: u64,
}

impl ControlPlane {
    #[allow(clippy::too_many_arguments)]
    pub async fn open(
        node_id: NodeId,
        cluster_name: &str,
        directory: &Path,
        broker: Arc<Broker>,
        client: reqwest::Client,
        snapshot_client: reqwest::Client,
        options: ControlPlaneOptions,
    ) -> anyhow::Result<Self> {
        let nodes = options
            .nodes
            .iter()
            .map(|(id, node)| (*id, BasicNode::new(node.raft_address.clone())))
            .collect::<BTreeMap<_, _>>();
        let metadata = Arc::new(
            MetadataCatalog::new_control_plane(
                options.nodes,
                options.voters.clone(),
                options.max_home_cells_per_topic,
            )
            .map_err(anyhow::Error::msg)?,
        );
        let mut root = None;
        let mut catalog = None;
        if options.voters.contains(&node_id) {
            let root_key = GroupKey::Root;
            root = Some(
                ConsensusNode::open_group(
                    root_key,
                    node_id,
                    &format!("{cluster_name}-root"),
                    nodes.clone(),
                    directory.join("groups").join(root_key.storage_component()),
                    Arc::clone(&broker),
                    Arc::clone(&metadata),
                    Network::for_group_with_snapshot(
                        client.clone(),
                        snapshot_client.clone(),
                        root_key,
                    ),
                    StateMachineRole::Root,
                )
                .await?,
            );
            let catalog_key = GroupKey::catalog(1).map_err(anyhow::Error::msg)?;
            catalog = Some(
                ConsensusNode::open_group(
                    catalog_key,
                    node_id,
                    &format!("{cluster_name}-catalog-1"),
                    nodes.clone(),
                    directory
                        .join("groups")
                        .join(catalog_key.storage_component()),
                    broker,
                    Arc::clone(&metadata),
                    Network::for_group_with_snapshot(client, snapshot_client, catalog_key),
                    StateMachineRole::Catalog { shard: 1 },
                )
                .await?,
            );
        }
        let root_cache = tokio::sync::RwLock::new(Arc::new(metadata.root_snapshot()));
        Ok(Self {
            metadata,
            nodes,
            voters: options.voters,
            root,
            catalog,
            topic_cache: tokio::sync::RwLock::new(HashMap::new()),
            root_cache,
            route_cache: std::time::Duration::from_millis(options.route_cache_ms),
            retry_after_ms: options.retry_after_ms,
        })
    }

    pub fn hosted_groups(&self) -> impl Iterator<Item = (GroupKey, Arc<ConsensusNode>)> + '_ {
        self.root
            .iter()
            .map(|group| (GroupKey::Root, Arc::clone(group)))
            .chain(self.catalog.iter().map(|group| {
                (
                    GroupKey::catalog(1).expect("Catalog shard is non-zero"),
                    Arc::clone(group),
                )
            }))
    }
}

impl ClusterRuntime {
    pub fn control_plane_enabled(&self) -> bool {
        self.control.is_some()
    }

    pub fn control_metadata(&self) -> Option<&Arc<MetadataCatalog>> {
        self.control.as_ref().map(|control| &control.metadata)
    }

    pub(super) async fn write_control(
        &self,
        command: QueueCommand,
    ) -> anyhow::Result<QueueResponse> {
        let required = command.required_feature_level();
        let observed = self.observed_feature_floor();
        if required > observed {
            anyhow::bail!(
                "federation command requires feature level {required}, observed floor is {observed}"
            );
        }
        let scope = command.scope();
        let group_key = match scope {
            crate::COMMAND_SCOPE_ROOT => GroupKey::Root,
            crate::COMMAND_SCOPE_CATALOG => {
                GroupKey::catalog(1).expect("Catalog shard ID is non-zero")
            }
            _ => anyhow::bail!("command does not belong to an independent control group"),
        };
        let Some(control) = &self.control else {
            return self.metadata_group().write(command).await;
        };
        if let Some(group) = self.group(group_key).await {
            return group.write(command).await;
        }

        let envelope = crate::CommandEnvelope::new(command);
        let mut candidate = control.voters.iter().next().copied();
        for _ in 0..3 {
            let node_id = candidate.ok_or_else(|| anyhow::anyhow!("control group has no voter"))?;
            let node = control
                .nodes
                .get(&node_id)
                .ok_or_else(|| anyhow::anyhow!("control voter {node_id} is not configured"))?;
            let response: RoutedResponse<QueueResponse> = crate::post_binary_limited(
                &self.client,
                format!(
                    "{}/raft/groups/{group_key}/write",
                    node.addr.trim_end_matches('/')
                ),
                &envelope,
                INTERNAL_WRITE_FRAME_BYTES,
                INTERNAL_WRITE_RESPONSE_BYTES,
            )
            .await?;
            match response {
                RoutedResponse::Success { value, .. } => return Ok(value),
                RoutedResponse::NotLeader(redirect) => candidate = redirect.leader_id,
                RoutedResponse::Failed { message, .. } => return Err(anyhow::anyhow!(message)),
            }
        }
        anyhow::bail!("control group leader changed repeatedly")
    }

    pub(super) async fn sync_catalog_topic(&self, topic: &str) -> anyhow::Result<()> {
        if self.control.is_none() {
            return Ok(());
        }
        let descriptor = self
            .metadata
            .topic(topic)
            .ok_or_else(|| anyhow::anyhow!("topic metadata is unavailable"))?;
        let response = self
            .write_control(QueueCommand::SyncCatalogTopic { descriptor })
            .await?;
        ensure_response(&response)?;
        self.invalidate_catalog_topic(topic).await;
        Ok(())
    }

    pub(super) async fn remove_control_topic(&self, topic: &str) -> anyhow::Result<()> {
        if self.control.is_none() {
            return Ok(());
        }
        let response = self
            .write_control(QueueCommand::RemoveCatalogTopic {
                topic: topic.to_owned(),
            })
            .await?;
        ensure_response(&response)?;
        self.invalidate_catalog_topic(topic).await;
        Ok(())
    }

    pub async fn initialize_control_groups(&self) -> anyhow::Result<()> {
        let Some(control) = &self.control else {
            return Ok(());
        };
        for key in [
            GroupKey::Root,
            GroupKey::catalog(1).expect("Catalog shard ID is non-zero"),
        ] {
            let host = *control
                .voters
                .iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("control group has no voter"))?;
            if host == self.node_id {
                self.initialize_control_group_local(key, control.voters.clone())
                    .await?;
                continue;
            }
            let node = control
                .nodes
                .get(&host)
                .ok_or_else(|| anyhow::anyhow!("control voter {host} is not configured"))?;
            self.client
                .post(format!(
                    "{}/raft/groups/{key}/initialize",
                    node.addr.trim_end_matches('/')
                ))
                .json(&InitializeGroupRequest {
                    voters: control.voters.clone(),
                })
                .send()
                .await?
                .error_for_status()?;
        }
        Ok(())
    }

    pub async fn initialize_control_group_local(
        &self,
        key: GroupKey,
        voters: BTreeSet<NodeId>,
    ) -> anyhow::Result<()> {
        if !matches!(key, GroupKey::Root | GroupKey::Catalog { .. }) {
            anyhow::bail!("group is not a Root or Catalog group");
        }
        let control = self
            .control
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("independent control plane is disabled"))?;
        let group = self
            .group(key)
            .await
            .ok_or_else(|| anyhow::anyhow!("control group {key} is not hosted locally"))?;
        if group
            .raft()
            .metrics()
            .borrow()
            .membership_config
            .voter_ids()
            .next()
            .is_some()
        {
            return Ok(());
        }
        let members = voters
            .iter()
            .map(|id| {
                control
                    .nodes
                    .get(id)
                    .cloned()
                    .map(|node| (*id, node))
                    .ok_or_else(|| anyhow::anyhow!("control voter {id} is unknown"))
            })
            .collect::<anyhow::Result<_>>()?;
        match group.initialize(members).await {
            Ok(()) => Ok(()),
            Err(error) if error.to_string().contains("already initialized") => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}
