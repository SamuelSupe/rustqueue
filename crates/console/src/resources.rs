use crate::model::TopicView;
use crate::session::now_ms;
use anyhow::{bail, Context};
use kube::api::{Api, ListParams, ObjectMeta, PostParams};
use kube::{Resource, ResourceExt};
use rustqueue_operator::{
    ManagedResourcePhase, RustQueue, RustQueueChannel, RustQueueChannelSpec, RustQueueTopic,
    RustQueueTopicSpec,
};
use rustqueue_queue::{ChannelFence, ManagementFenceSnapshot};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Default)]
pub struct ManagedResources {
    pub topics: Vec<RustQueueTopic>,
    pub channels: Vec<RustQueueChannel>,
}

impl ManagedResources {
    pub fn fences(&self) -> ManagementFenceSnapshot {
        let now = now_ms() as i64;
        let topics = self
            .topics
            .iter()
            .filter_map(|topic| {
                let until = topic.spec.tombstone_until_ms?;
                (until > now).then(|| (topic.spec.topic.clone(), until))
            })
            .collect();
        let channels = self
            .channels
            .iter()
            .filter_map(|channel| {
                let until = channel.spec.tombstone_until_ms?;
                (until > now).then(|| ChannelFence {
                    topic: channel.spec.topic.clone(),
                    channel: channel.spec.channel.clone(),
                    until_ms: until,
                })
            })
            .collect();
        let mut revisions: Vec<_> = self
            .topics
            .iter()
            .map(|resource| resource.resource_version().unwrap_or_default())
            .collect();
        revisions.extend(
            self.channels
                .iter()
                .map(|resource| resource.resource_version().unwrap_or_default()),
        );
        revisions.sort();
        ManagementFenceSnapshot {
            revision: format!("{:08x}", crc32c::crc32c(revisions.join("\0").as_bytes())),
            topics,
            channels,
        }
    }
}

pub async fn list(
    client: &kube::Client,
    namespace: &str,
    queue: &str,
) -> anyhow::Result<ManagedResources> {
    let params = ListParams::default().labels(&format!("rustqueue.io/queue={queue}"));
    let topics = Api::<RustQueueTopic>::namespaced(client.clone(), namespace)
        .list(&params)
        .await?
        .items;
    let channels = Api::<RustQueueChannel>::namespaced(client.clone(), namespace)
        .list(&params)
        .await?
        .items;
    Ok(ManagedResources { topics, channels })
}

pub async fn reconcile(
    client: &kube::Client,
    cluster: &RustQueue,
    observed: &[TopicView],
) -> anyhow::Result<ManagedResources> {
    let namespace = cluster.namespace().context("RustQueue namespace")?;
    let queue = cluster.name_any();
    let current = list(client, &namespace, &queue).await?;
    let topic_api = Api::<RustQueueTopic>::namespaced(client.clone(), &namespace);
    let channel_api = Api::<RustQueueChannel>::namespaced(client.clone(), &namespace);
    let owner = cluster
        .controller_owner_ref(&())
        .context("RustQueue owner reference")?;
    let mut known_topics: BTreeMap<_, _> = current
        .topics
        .iter()
        .map(|resource| (resource.spec.topic.clone(), resource.clone()))
        .collect();
    let mut known_channels: BTreeMap<_, _> = current
        .channels
        .iter()
        .map(|resource| {
            (
                (resource.spec.topic.clone(), resource.spec.channel.clone()),
                resource.clone(),
            )
        })
        .collect();
    let mut observed_ephemeral = BTreeSet::new();

    for topic in observed {
        match known_topics.remove(&topic.name) {
            Some(mut resource)
                if may_reconcile(&resource.spec.phase, resource.spec.tombstone_until_ms) =>
            {
                if resource.spec.owners != topic.owners || resource.spec.paused != topic.paused {
                    resource.spec.owners = topic.owners.clone();
                    resource.spec.paused = topic.paused;
                    resource.spec.phase = ManagedResourcePhase::Active;
                    resource.spec.tombstone_until_ms = None;
                    resource.spec.last_error = None;
                    resource.spec.revision = resource.spec.revision.saturating_add(1);
                    topic_api
                        .replace(&resource.name_any(), &PostParams::default(), &resource)
                        .await?;
                }
            }
            Some(_) => {}
            None => {
                let mut resource = RustQueueTopic::new(
                    &topic_resource_name(&queue, &topic.name),
                    RustQueueTopicSpec {
                        queue: queue.clone(),
                        topic: topic.name.clone(),
                        owners: topic.owners.clone(),
                        phase: ManagedResourcePhase::Active,
                        revision: 1,
                        paused: topic.paused,
                        tombstone_until_ms: None,
                        last_error: None,
                        operation: None,
                    },
                );
                resource.metadata = metadata(resource.metadata, &queue, owner.clone());
                topic_api.create(&PostParams::default(), &resource).await?;
            }
        }
        for channel in &topic.channels {
            let key = (topic.name.clone(), channel.name.clone());
            if channel.ephemeral {
                observed_ephemeral.insert(key.clone());
            }
            match known_channels.remove(&key) {
                Some(mut resource)
                    if may_reconcile(&resource.spec.phase, resource.spec.tombstone_until_ms) =>
                {
                    if resource.spec.owners != channel.owners
                        || resource.spec.paused != channel.paused
                        || resource.spec.ephemeral != channel.ephemeral
                    {
                        resource.spec.owners = channel.owners.clone();
                        resource.spec.paused = channel.paused;
                        resource.spec.ephemeral = channel.ephemeral;
                        resource.spec.phase = ManagedResourcePhase::Active;
                        resource.spec.tombstone_until_ms = None;
                        resource.spec.last_error = None;
                        resource.spec.revision = resource.spec.revision.saturating_add(1);
                        channel_api
                            .replace(&resource.name_any(), &PostParams::default(), &resource)
                            .await?;
                    }
                }
                Some(_) => {}
                None => {
                    let mut resource = RustQueueChannel::new(
                        &channel_resource_name(&queue, &topic.name, &channel.name),
                        RustQueueChannelSpec {
                            queue: queue.clone(),
                            topic: topic.name.clone(),
                            channel: channel.name.clone(),
                            owners: channel.owners.clone(),
                            phase: ManagedResourcePhase::Active,
                            revision: 1,
                            paused: channel.paused,
                            ephemeral: channel.ephemeral,
                            tombstone_until_ms: None,
                            last_error: None,
                            operation: None,
                        },
                    );
                    resource.metadata = metadata(resource.metadata, &queue, owner.clone());
                    channel_api
                        .create(&PostParams::default(), &resource)
                        .await?;
                }
            }
        }
    }

    for resource in known_channels
        .values()
        .filter(|resource| resource.spec.ephemeral)
    {
        let key = (resource.spec.topic.clone(), resource.spec.channel.clone());
        if !observed_ephemeral.contains(&key) {
            channel_api
                .delete(&resource.name_any(), &Default::default())
                .await?;
        }
    }
    list(client, &namespace, &queue).await
}

pub async fn get_topic(
    client: &kube::Client,
    namespace: &str,
    queue: &str,
    topic: &str,
) -> anyhow::Result<Option<RustQueueTopic>> {
    let value = Api::<RustQueueTopic>::namespaced(client.clone(), namespace)
        .get_opt(&topic_resource_name(queue, topic))
        .await?;
    if value
        .as_ref()
        .is_some_and(|resource| resource.spec.queue != queue || resource.spec.topic != topic)
    {
        bail!("topic resource hash collision")
    }
    Ok(value)
}

pub async fn get_channel(
    client: &kube::Client,
    namespace: &str,
    queue: &str,
    topic: &str,
    channel: &str,
) -> anyhow::Result<Option<RustQueueChannel>> {
    let value = Api::<RustQueueChannel>::namespaced(client.clone(), namespace)
        .get_opt(&channel_resource_name(queue, topic, channel))
        .await?;
    if value.as_ref().is_some_and(|resource| {
        resource.spec.queue != queue
            || resource.spec.topic != topic
            || resource.spec.channel != channel
    }) {
        bail!("channel resource hash collision")
    }
    Ok(value)
}

pub fn topic_resource_name(queue: &str, topic: &str) -> String {
    hashed_name("topic", &[queue, topic])
}

pub fn channel_resource_name(queue: &str, topic: &str, channel: &str) -> String {
    hashed_name("channel", &[queue, topic, channel])
}

fn hashed_name(prefix: &str, fields: &[&str]) -> String {
    let mut digest = Sha256::new();
    for field in fields {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field.as_bytes());
    }
    format!("{prefix}-{}", &hex::encode(digest.finalize())[..32])
}

fn metadata(
    mut metadata: ObjectMeta,
    queue: &str,
    owner: k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference,
) -> ObjectMeta {
    metadata.labels = Some(BTreeMap::from([(
        "rustqueue.io/queue".into(),
        queue.into(),
    )]));
    metadata.owner_references = Some(vec![owner]);
    metadata
}

fn may_reconcile(phase: &ManagedResourcePhase, tombstone_until_ms: Option<i64>) -> bool {
    *phase == ManagedResourcePhase::Active
        || (*phase == ManagedResourcePhase::Tombstoned
            && tombstone_until_ms.is_none_or(|until| until <= now_ms() as i64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_names_are_stable_and_distinguish_boundaries() {
        assert_eq!(
            topic_resource_name("q", "orders"),
            topic_resource_name("q", "orders")
        );
        assert_ne!(
            channel_resource_name("q", "a:b", "c"),
            channel_resource_name("q", "a", "b:c")
        );
    }
}
