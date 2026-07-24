#[derive(Debug, Eq, PartialEq)]
pub(super) struct ProducerRestartFence {
    pub baseline_nonce: Option<String>,
    pub confirmed: bool,
}

pub(super) fn producer_restart_fence(
    enabled: bool,
    gateways_advertised: bool,
    current_nonce: &str,
    previous_baseline: Option<&str>,
) -> ProducerRestartFence {
    if !enabled || !gateways_advertised {
        return ProducerRestartFence {
            baseline_nonce: None,
            confirmed: false,
        };
    }
    let Some(baseline) = previous_baseline else {
        return ProducerRestartFence {
            baseline_nonce: Some(current_nonce.into()),
            confirmed: false,
        };
    };
    ProducerRestartFence {
        baseline_nonce: Some(baseline.into()),
        confirmed: current_nonce != baseline,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn confirmation_requires_a_nonce_change_after_advertisement() {
        let first = producer_restart_fence(true, true, "before", None);
        assert_eq!(
            first,
            ProducerRestartFence {
                baseline_nonce: Some("before".into()),
                confirmed: false,
            }
        );
        assert!(!producer_restart_fence(true, true, "before", Some("before")).confirmed);
        assert!(producer_restart_fence(true, true, "after", Some("before")).confirmed);
    }

    #[test]
    fn losing_advertisement_resets_the_confirmation_fence() {
        assert_eq!(
            producer_restart_fence(true, false, "after", Some("before")),
            ProducerRestartFence {
                baseline_nonce: None,
                confirmed: false,
            }
        );
        assert_eq!(
            producer_restart_fence(false, true, "after", Some("before")),
            ProducerRestartFence {
                baseline_nonce: None,
                confirmed: false,
            }
        );
    }
}
