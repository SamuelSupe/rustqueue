use crate::model::{ChannelView, TopicView};
use crate::resources::ManagedResources;
use kube::ResourceExt;
use rustqueue_operator::{ManagedResourcePhase, RustQueueTopic};
use std::collections::BTreeMap;

pub fn merge(topics: &mut Vec<TopicView>, managed: &ManagedResources) {
    let mut topic_index: BTreeMap<String, usize> = topics
        .iter()
        .enumerate()
        .map(|(index, topic)| (topic.name.clone(), index))
        .collect();
    for resource in &managed.topics {
        let index = if let Some(index) = topic_index.get(&resource.spec.topic).copied() {
            index
        } else {
            topics.push(TopicView {
                name: resource.spec.topic.clone(),
                owners: resource.spec.owners.clone(),
                paused: resource.spec.paused,
                ..Default::default()
            });
            let index = topics.len() - 1;
            topic_index.insert(resource.spec.topic.clone(), index);
            index
        };
        apply_topic(&mut topics[index], resource);
    }
    for resource in &managed.channels {
        let Some(index) = topic_index.get(&resource.spec.topic).copied() else {
            continue;
        };
        let topic = &mut topics[index];
        let channel = if let Some(channel) = topic
            .channels
            .iter_mut()
            .find(|channel| channel.name == resource.spec.channel)
        {
            channel
        } else {
            topic.channels.push(ChannelView {
                name: resource.spec.channel.clone(),
                owners: resource.spec.owners.clone(),
                paused: resource.spec.paused,
                ephemeral: resource.spec.ephemeral,
                ..Default::default()
            });
            topic.channels.last_mut().expect("channel was inserted")
        };
        channel.managed_phase = phase_name(&resource.spec.phase).into();
        channel.management_revision = resource.spec.revision;
        channel.tombstone_until_ms = resource.spec.tombstone_until_ms;
        channel.management_error = resource.spec.last_error.clone();
        channel.resource_uid = resource.uid().unwrap_or_default();
        channel.resource_version = resource.resource_version().unwrap_or_default();
    }
    topics.sort_by(|left, right| left.name.cmp(&right.name));
    for topic in topics {
        topic
            .channels
            .sort_by(|left, right| left.name.cmp(&right.name));
    }
}

fn apply_topic(topic: &mut TopicView, resource: &RustQueueTopic) {
    topic.managed_phase = phase_name(&resource.spec.phase).into();
    topic.management_revision = resource.spec.revision;
    topic.tombstone_until_ms = resource.spec.tombstone_until_ms;
    topic.management_error = resource.spec.last_error.clone();
    topic.resource_uid = resource.uid().unwrap_or_default();
    topic.resource_version = resource.resource_version().unwrap_or_default();
}

fn phase_name(phase: &ManagedResourcePhase) -> &'static str {
    match phase {
        ManagedResourcePhase::Preparing => "PREPARING",
        ManagedResourcePhase::Active => "ACTIVE",
        ManagedResourcePhase::Applying => "APPLYING",
        ManagedResourcePhase::Failed => "FAILED",
        ManagedResourcePhase::Tombstoned => "TOMBSTONED",
    }
}
