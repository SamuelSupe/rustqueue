use super::apply::ApplyRequest;
use super::ManagementError;
use crate::session::ActionChallenge;
use rustqueue_operator::{
    ManagedResourceAction, ManagedResourceOperation, ManagedResourcePhase, RustQueueChannel,
    RustQueueTopic,
};
use sha2::{Digest, Sha256};

pub struct StartedOperation {
    pub id: String,
    pub revision: u64,
}

pub(super) fn new_operation(
    request: &ApplyRequest,
    challenge: &ActionChallenge,
    now: i64,
) -> Result<ManagedResourceOperation, ManagementError> {
    let action = parse_action(&request.action)?;
    let mut digest = Sha256::new();
    digest.update(challenge.token.as_bytes());
    digest.update(request.kind.as_bytes());
    digest.update(request.topic.as_bytes());
    digest.update(request.channel.as_deref().unwrap_or_default().as_bytes());
    Ok(ManagedResourceOperation {
        id: format!("op-{}", &hex::encode(digest.finalize())[..32]),
        action,
        completed_owners: Vec::new(),
        attempt: 1,
        created_at_ms: now,
        updated_at_ms: now,
    })
}

pub(super) fn retry_operation(
    operation: &mut Option<ManagedResourceOperation>,
    phase: &ManagedResourcePhase,
    now: i64,
) -> Result<(), ManagementError> {
    if *phase != ManagedResourcePhase::Failed {
        return Err(ManagementError::conflict(
            "E_RETRY_NOT_FAILED",
            "only a failed operation can be retried",
        ));
    }
    let operation = operation.as_mut().ok_or_else(|| {
        ManagementError::conflict(
            "E_RETRY_MISSING_OPERATION",
            "failed resource has no operation",
        )
    })?;
    operation.attempt = operation.attempt.saturating_add(1);
    operation.updated_at_ms = now;
    Ok(())
}

pub(super) fn started_topic(resource: &RustQueueTopic) -> StartedOperation {
    StartedOperation {
        id: resource.spec.operation.as_ref().unwrap().id.clone(),
        revision: resource.spec.revision,
    }
}

pub(super) fn started_channel(resource: &RustQueueChannel) -> StartedOperation {
    StartedOperation {
        id: resource.spec.operation.as_ref().unwrap().id.clone(),
        revision: resource.spec.revision,
    }
}

fn parse_action(action: &str) -> Result<ManagedResourceAction, ManagementError> {
    match action {
        "create" => Ok(ManagedResourceAction::Create),
        "pause" => Ok(ManagedResourceAction::Pause),
        "unpause" => Ok(ManagedResourceAction::Unpause),
        "empty" => Ok(ManagedResourceAction::Empty),
        "delete" => Ok(ManagedResourceAction::Delete),
        _ => Err(ManagementError::bad_request(
            "E_BAD_ACTION",
            "unsupported management action",
        )),
    }
}

pub fn action_name(action: ManagedResourceAction) -> &'static str {
    match action {
        ManagedResourceAction::Create => "create",
        ManagedResourceAction::Pause => "pause",
        ManagedResourceAction::Unpause => "unpause",
        ManagedResourceAction::Empty => "empty",
        ManagedResourceAction::Delete => "delete",
    }
}

pub(super) fn phase_for(action: ManagedResourceAction) -> ManagedResourcePhase {
    if action == ManagedResourceAction::Create {
        ManagedResourcePhase::Preparing
    } else {
        ManagedResourcePhase::Applying
    }
}

pub(super) fn final_phase(action: ManagedResourceAction) -> ManagedResourcePhase {
    if action == ManagedResourceAction::Delete {
        ManagedResourcePhase::Tombstoned
    } else {
        ManagedResourcePhase::Active
    }
}

pub(super) fn update_paused(paused: &mut bool, action: ManagedResourceAction) {
    if action == ManagedResourceAction::Pause {
        *paused = true;
    } else if action == ManagedResourceAction::Unpause {
        *paused = false;
    }
}

pub(super) fn update_tombstone(
    current: &mut Option<i64>,
    action: ManagedResourceAction,
    tombstone_until_ms: Option<i64>,
) {
    if action == ManagedResourceAction::Create {
        *current = None;
    } else if action == ManagedResourceAction::Delete {
        *current = tombstone_until_ms;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_retains_identity_and_completed_owners() {
        let mut operation = Some(ManagedResourceOperation {
            id: "operation-00000001".into(),
            action: ManagedResourceAction::Pause,
            completed_owners: vec!["broker-1".into()],
            attempt: 1,
            created_at_ms: 10,
            updated_at_ms: 10,
        });
        retry_operation(&mut operation, &ManagedResourcePhase::Failed, 20).unwrap();
        let operation = operation.unwrap();
        assert_eq!(operation.id, "operation-00000001");
        assert_eq!(operation.completed_owners, vec!["broker-1"]);
        assert_eq!(operation.attempt, 2);
        assert_eq!(operation.updated_at_ms, 20);
    }
}
