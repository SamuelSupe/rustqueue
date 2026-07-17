use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ManagedResourcePhase {
    Preparing,
    Active,
    Applying,
    Failed,
    Tombstoned,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ManagedResourceAction {
    Create,
    Pause,
    Unpause,
    Empty,
    Delete,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ManagedResourceOperation {
    pub id: String,
    pub action: ManagedResourceAction,
    #[serde(default)]
    pub completed_owners: Vec<String>,
    #[serde(default)]
    pub attempt: u32,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "rustqueue.io",
    version = "v1alpha1",
    kind = "RustQueueTopic",
    plural = "rustqueuetopics",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct RustQueueTopicSpec {
    pub queue: String,
    pub topic: String,
    #[serde(default)]
    pub owners: Vec<String>,
    pub phase: ManagedResourcePhase,
    #[serde(default)]
    pub revision: u64,
    #[serde(default)]
    pub paused: bool,
    #[serde(default)]
    pub tombstone_until_ms: Option<i64>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub operation: Option<ManagedResourceOperation>,
}

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "rustqueue.io",
    version = "v1alpha1",
    kind = "RustQueueChannel",
    plural = "rustqueuechannels",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct RustQueueChannelSpec {
    pub queue: String,
    pub topic: String,
    pub channel: String,
    #[serde(default)]
    pub owners: Vec<String>,
    pub phase: ManagedResourcePhase,
    #[serde(default)]
    pub revision: u64,
    #[serde(default)]
    pub paused: bool,
    #[serde(default)]
    pub ephemeral: bool,
    #[serde(default)]
    pub tombstone_until_ms: Option<i64>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub operation: Option<ManagedResourceOperation>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube::CustomResourceExt;

    #[test]
    fn management_crds_expose_camel_case_fences_and_closed_phases() {
        let topic = serde_json::to_value(RustQueueTopic::crd()).unwrap();
        let spec = &topic["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"];
        assert!(spec.get("tombstoneUntilMs").is_some());
        assert!(spec.get("tombstone_until_ms").is_none());
        assert!(spec.get("operation").is_some());
        assert_eq!(
            spec["phase"]["enum"],
            serde_json::json!(["PREPARING", "ACTIVE", "APPLYING", "FAILED", "TOMBSTONED"])
        );

        let channel = serde_json::to_value(RustQueueChannel::crd()).unwrap();
        let channel_spec = &channel["spec"]["versions"][0]["schema"]["openAPIV3Schema"]
            ["properties"]["spec"]["properties"];
        assert!(channel_spec.get("ephemeral").is_some());
        assert!(channel_spec.get("lastError").is_some());
        let schema = channel.to_string();
        for action in ["CREATE", "PAUSE", "UNPAUSE", "EMPTY", "DELETE"] {
            assert!(schema.contains(action));
        }
    }
}
