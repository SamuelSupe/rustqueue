use super::*;

impl ClusterRuntime {
    pub async fn root_snapshot_routed_local(&self) -> RoutedResponse<crate::FederationRoot> {
        let Some(control) = &self.control else {
            return RoutedResponse::failed("independent Root is disabled", None, 0);
        };
        let Some(group) = &control.root else {
            return RoutedResponse::failed("Root group is not hosted here", None, 0);
        };
        let (leader, term) = group.leader_state();
        if leader != Some(self.node_id) {
            return RoutedResponse::not_leader(leader, term);
        }
        if let Err(error) = group.ensure_quorum_local().await {
            return RoutedResponse::failed(error.to_string(), leader, term);
        }
        RoutedResponse::success(control.metadata.root_snapshot(), self.node_id, term)
    }

    pub async fn root_snapshot_fresh(&self) -> anyhow::Result<crate::FederationRoot> {
        let control = self
            .control
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("independent Root is disabled"))?;
        if let Some(group) = &control.root {
            if group.leader_state().0 == Some(self.node_id) {
                group.ensure_quorum_local().await?;
                let root = control.metadata.root_snapshot();
                *control.root_cache.write().await = Arc::new(root.clone());
                return Ok(root);
            }
        }
        let mut candidate = control
            .root
            .as_ref()
            .and_then(|group| group.leader_state().0)
            .or_else(|| control.voters.iter().next().copied());
        for _ in 0..3 {
            let node_id = candidate.ok_or_else(|| anyhow::anyhow!("Root has no voter"))?;
            let node = control
                .nodes
                .get(&node_id)
                .ok_or_else(|| anyhow::anyhow!("Root voter {node_id} is unknown"))?;
            let response: RoutedResponse<crate::FederationRoot> = crate::post_binary_limited(
                &self.client,
                format!(
                    "{}/federation/root/snapshot",
                    node.addr.trim_end_matches('/')
                ),
                &(),
                INTERNAL_SMALL_FRAME_BYTES,
                INTERNAL_CATALOG_FRAME_BYTES,
            )
            .await?;
            match response {
                RoutedResponse::Success { value, .. } => {
                    *control.root_cache.write().await = Arc::new(value.clone());
                    return Ok(value);
                }
                RoutedResponse::NotLeader(redirect) => candidate = redirect.leader_id,
                RoutedResponse::Failed { message, .. } => return Err(anyhow::anyhow!(message)),
            }
        }
        anyhow::bail!("Root leader changed repeatedly")
    }

    pub(super) async fn root_snapshot_cached(&self) -> anyhow::Result<Arc<crate::FederationRoot>> {
        let control = self
            .control
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("independent Root is disabled"))?;
        if control.root.is_some() {
            return Ok(Arc::new(control.metadata.root_snapshot()));
        }
        Ok(Arc::clone(&*control.root_cache.read().await))
    }
}
