use anyhow::Error;

#[derive(Debug, thiserror::Error)]
pub(super) enum OperationAttemptError {
    #[error("{0}")]
    Retryable(#[source] Error),
    #[error("{0}")]
    NeedsOperator(#[source] Error),
}

impl OperationAttemptError {
    pub(super) fn retryable(error: impl Into<Error>) -> Self {
        Self::Retryable(error.into())
    }

    pub(super) fn needs_operator(error: impl Into<Error>) -> Self {
        Self::NeedsOperator(error.into())
    }

    pub(super) fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable(_))
    }
}

pub(super) type OperationAttempt<T> = Result<T, OperationAttemptError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_policy_is_carried_by_the_error_type() {
        let transient = OperationAttemptError::retryable(anyhow::anyhow!("transport failed"));
        let blocked = OperationAttemptError::needs_operator(anyhow::anyhow!("no replacement node"));
        assert!(transient.is_retryable());
        assert!(!blocked.is_retryable());
    }
}
