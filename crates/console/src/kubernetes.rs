use crate::model::{BrokerView, EventView, PvcView};
use k8s_openapi::api::core::v1::{Event, PersistentVolumeClaim, Pod};
use kube::ResourceExt;
use rustqueue_operator::RustQueue;
use std::collections::BTreeMap;

pub fn broker_from_pod(pod: Pod, pvcs: &BTreeMap<String, PvcView>) -> BrokerView {
    let name = pod.name_any();
    let status = pod.status.as_ref();
    let container = status
        .and_then(|status| status.container_statuses.as_ref())
        .and_then(|statuses| statuses.iter().find(|status| status.name == "broker"));
    BrokerView {
        pvc: pvcs.get(&format!("data-{name}")).cloned(),
        node_name: pod
            .spec
            .as_ref()
            .and_then(|spec| spec.node_name.clone())
            .unwrap_or_default(),
        pod_ip: status
            .and_then(|status| status.pod_ip.clone())
            .unwrap_or_default(),
        phase: status
            .and_then(|status| status.phase.clone())
            .unwrap_or_default(),
        ready: status
            .and_then(|status| status.conditions.as_ref())
            .is_some_and(|conditions| {
                conditions
                    .iter()
                    .any(|condition| condition.type_ == "Ready" && condition.status == "True")
            }),
        restarts: container
            .map(|status| status.restart_count)
            .unwrap_or_default(),
        image: container
            .map(|status| status.image.clone())
            .unwrap_or_else(|| {
                pod.spec
                    .as_ref()
                    .and_then(|spec| {
                        spec.containers
                            .iter()
                            .find(|container| container.name == "broker")
                    })
                    .map(|container| container.image.clone().unwrap_or_default())
                    .unwrap_or_default()
            }),
        image_id: container
            .map(|status| status.image_id.clone())
            .unwrap_or_default(),
        started_at: status
            .and_then(|status| status.start_time.as_ref())
            .map(|time| time.0.to_string()),
        name,
        observation: None,
        error: None,
    }
}

pub fn pvc_map(pvcs: Vec<PersistentVolumeClaim>) -> BTreeMap<String, PvcView> {
    pvcs.into_iter()
        .map(|pvc| {
            let name = pvc.name_any();
            let view = PvcView {
                name: name.clone(),
                phase: pvc
                    .status
                    .as_ref()
                    .and_then(|status| status.phase.clone())
                    .unwrap_or_default(),
                requested: pvc
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.resources.as_ref())
                    .and_then(|resources| resources.requests.as_ref())
                    .and_then(|requests| requests.get("storage"))
                    .map(|quantity| quantity.0.clone())
                    .unwrap_or_default(),
                capacity: pvc
                    .status
                    .as_ref()
                    .and_then(|status| status.capacity.as_ref())
                    .and_then(|capacity| capacity.get("storage"))
                    .map(|quantity| quantity.0.clone())
                    .unwrap_or_default(),
                storage_class: pvc
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.storage_class_name.clone())
                    .unwrap_or_default(),
            };
            (name, view)
        })
        .collect()
}

pub fn event_views(cluster: &RustQueue, events: Vec<Event>) -> Vec<EventView> {
    let uid = cluster.uid().unwrap_or_default();
    let name = cluster.name_any();
    let mut events: Vec<_> =
        events
            .into_iter()
            .filter(|event| {
                event.involved_object.uid.as_deref() == Some(uid.as_str())
                    || event.involved_object.name.as_deref().is_some_and(|object| {
                        object == name || object.starts_with(&format!("{name}-"))
                    })
            })
            .map(|event| EventView {
                at: event
                    .event_time
                    .as_ref()
                    .map(|time| time.0.to_string())
                    .or_else(|| event.last_timestamp.as_ref().map(|time| time.0.to_string()))
                    .or_else(|| {
                        event
                            .metadata
                            .creation_timestamp
                            .as_ref()
                            .map(|time| time.0.to_string())
                    })
                    .unwrap_or_default(),
                type_: event.type_.unwrap_or_default(),
                reason: event.reason.unwrap_or_default(),
                message: event.message.unwrap_or_default(),
                object: event.involved_object.name.unwrap_or_default(),
                count: event.count.unwrap_or(1),
            })
            .collect();
    events.sort_by(|left, right| right.at.cmp(&left.at));
    events.truncate(100);
    events
}
