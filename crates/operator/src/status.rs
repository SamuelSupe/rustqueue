use crate::crd::ClusterCondition;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

pub fn condition(
    generation: Option<i64>,
    type_: &str,
    status: bool,
    reason: &str,
    message: impl Into<String>,
) -> ClusterCondition {
    ClusterCondition {
        type_: type_.into(),
        status: if status { "True" } else { "False" }.into(),
        reason: reason.into(),
        message: message.into(),
        last_transition_time: now(),
        observed_generation: generation,
    }
}

pub fn now() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| OffsetDateTime::now_utc().unix_timestamp().to_string())
}

pub fn unix_now() -> i64 {
    OffsetDateTime::now_utc().unix_timestamp()
}
