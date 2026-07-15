use super::*;

impl StateMachineStore {
    pub(super) fn apply_batch(
        &self,
        commands: &[QueueCommand],
        finish: bool,
    ) -> Result<QueueResponse, BrokerError> {
        self.broker.begin_replicated_batch();
        let mut results = Vec::with_capacity(commands.len());
        for command in commands {
            match self.apply_command(command) {
                Ok(response) => results.push(response),
                Err(error) if recovery::is_fatal_queue_error(&error) => {
                    let _ = self.broker.finish_replicated_batch();
                    return Err(error);
                }
                Err(error) => results.push(QueueResponse {
                    message_ids: Vec::new(),
                    error: Some(error.to_string()),
                    results: Vec::new(),
                }),
            }
        }
        if finish {
            self.broker.finish_replicated_batch()?;
        }
        Ok(QueueResponse {
            message_ids: Vec::new(),
            error: None,
            results,
        })
    }
}
