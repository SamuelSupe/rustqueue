use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum NameError {
    #[error("name must contain between 1 and 64 characters")]
    Length,
    #[error("name contains an unsupported character")]
    Character,
}

pub fn validate_name(name: &str) -> Result<(), NameError> {
    let base = name.strip_suffix("#ephemeral").unwrap_or(name);
    if base.is_empty() || name.len() > 64 {
        return Err(NameError::Length);
    }
    if !base
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(NameError::Character);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_nsq_names() {
        for name in ["topic", "orders.v1", "a_b-c", "jobs#ephemeral"] {
            assert!(validate_name(name).is_ok(), "{name}");
        }
    }

    #[test]
    fn rejects_invalid_names() {
        for name in ["", "bad/name", "bad name", "#ephemeral"] {
            assert!(validate_name(name).is_err(), "{name}");
        }
    }
}
