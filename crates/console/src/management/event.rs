use crate::app::AppState;
use k8s_openapi::api::core::v1::{Event, EventSource, ObjectReference};
use kube::api::{Api, ObjectMeta, PostParams};

pub async fn emit(state: &AppState, action: &str, target: &str, result: &str, detail: &str) {
    let normal = matches!(result, "accepted" | "success");
    let api = Api::<Event>::namespaced(state.client.clone(), &state.config.namespace);
    let event = Event {
        metadata: ObjectMeta {
            generate_name: Some(format!("{}-console-", state.config.queue_name)),
            namespace: Some(state.config.namespace.clone()),
            ..Default::default()
        },
        involved_object: ObjectReference {
            api_version: Some("rustqueue.io/v1alpha1".into()),
            kind: Some("RustQueue".into()),
            name: Some(state.config.queue_name.clone()),
            namespace: Some(state.config.namespace.clone()),
            ..Default::default()
        },
        action: Some(action.into()),
        message: Some(format!("{action} {target}: {result}; {detail}")),
        reason: Some(match result {
            "accepted" => "ConsoleManagementAccepted".into(),
            "success" => "ConsoleManagementSucceeded".into(),
            _ => "ConsoleManagementFailed".into(),
        }),
        reporting_component: Some("rustqueue-console".into()),
        source: Some(EventSource {
            component: Some("rustqueue-console".into()),
            ..Default::default()
        }),
        type_: Some(if normal {
            "Normal".into()
        } else {
            "Warning".into()
        }),
        ..Default::default()
    };
    if let Err(error) = api.create(&PostParams::default(), &event).await {
        tracing::warn!(%error, action, result, "write console management Kubernetes Event failed");
    }
}
