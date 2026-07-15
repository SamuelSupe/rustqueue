use super::*;

impl ClusterRuntime {
    pub(super) fn node(&self, node_id: NodeId) -> Option<BasicNode> {
        self.nodes.get(&node_id).cloned().or_else(|| {
            self.metadata
                .node(node_id)
                .map(|descriptor| BasicNode::new(descriptor.raft_address))
        })
    }

    pub(super) fn nodes_snapshot(&self) -> BTreeMap<NodeId, BasicNode> {
        let mut nodes = self.nodes.clone();
        for descriptor in self.metadata.snapshot().nodes.into_values() {
            nodes.insert(descriptor.id, BasicNode::new(descriptor.raft_address));
        }
        nodes
    }

    pub fn validate_node_descriptor(
        &self,
        descriptor: &crate::NodeDescriptor,
    ) -> anyhow::Result<()> {
        let registered = self.metadata.node(descriptor.id).ok_or_else(|| {
            anyhow::anyhow!(
                "node {} must be registered before it can join",
                descriptor.id
            )
        })?;
        if registered.id != descriptor.id
            || registered.raft_address != descriptor.raft_address
            || registered.broadcast_address != descriptor.broadcast_address
            || registered.tcp_port != descriptor.tcp_port
            || registered.http_port != descriptor.http_port
            || registered.tls_server_name != descriptor.tls_server_name
            || registered.failure_domain != descriptor.failure_domain
            || descriptor
                .peer_id
                .as_ref()
                .is_some_and(|peer| registered.peer_id.as_ref() != Some(peer))
        {
            anyhow::bail!(
                "node descriptor does not match its registered address, TLS name, or failure domain"
            );
        }
        Ok(())
    }

    pub async fn admit_discovered_node(
        &self,
        descriptor: crate::NodeDescriptor,
    ) -> anyhow::Result<bool> {
        let peer_id = descriptor
            .peer_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow::anyhow!("discovered node has no peer ID"))?;
        if peer_id.len() > 128 {
            anyhow::bail!("discovered peer ID is too long");
        }

        let needs_registration = self.metadata.node(descriptor.id).as_ref() != Some(&descriptor);
        let is_member = self
            .metadata_group
            .raft()
            .metrics()
            .borrow()
            .membership_config
            .nodes()
            .any(|(node_id, _)| *node_id == descriptor.id);
        let needs_learner = descriptor.id != self.node_id && !is_member;
        if !needs_registration && !needs_learner {
            return Ok(false);
        }

        self.probe_discovered_node(&descriptor).await?;
        if needs_registration {
            let response = self
                .metadata_group()
                .write(QueueCommand::RegisterNode {
                    descriptor: descriptor.clone(),
                })
                .await?;
            ensure_response(&response)?;
        }
        if needs_learner {
            self.add_metadata_learner(descriptor.id).await?;
        }
        Ok(true)
    }

    async fn probe_discovered_node(
        &self,
        descriptor: &crate::NodeDescriptor,
    ) -> anyhow::Result<()> {
        let response = self
            .client
            .get(format!(
                "{}/raft/time",
                descriptor.raft_address.trim_end_matches('/')
            ))
            .send()
            .await?
            .error_for_status()?;
        let payload: serde_json::Value = response.json().await?;
        if payload["node_id"].as_u64() != Some(descriptor.id) {
            anyhow::bail!("discovered Raft endpoint reported a different node ID");
        }
        let advertised = crate::feature::advertised_feature_level(&payload);
        let required = self.active_feature_level();
        if advertised < required {
            anyhow::bail!(
                "discovered node feature level {advertised} is below active cluster level {required}"
            );
        }
        Ok(())
    }
}
