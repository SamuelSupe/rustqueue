use crate::validate_name;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    Identify,
    Auth,
    Subscribe { topic: String, channel: String },
    Publish { topic: String },
    MultiPublish { topic: String },
    DeferredPublish { topic: String, delay_ms: u64 },
    Ready(u64),
    Finish(u64),
    Requeue { id: u64, delay_ms: i64 },
    Touch(u64),
    Close,
    Noop,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CommandError {
    #[error("E_INVALID {0}")]
    Invalid(&'static str),
    #[error("E_BAD_TOPIC invalid topic")]
    BadTopic,
    #[error("E_BAD_CHANNEL invalid channel")]
    BadChannel,
}

impl Command {
    pub fn parse(line: &[u8]) -> Result<Self, CommandError> {
        let line = std::str::from_utf8(line)
            .map_err(|_| CommandError::Invalid("command is not UTF-8"))?
            .trim_end_matches(['\r', '\n']);
        let parts: Vec<&str> = line.split_ascii_whitespace().collect();
        let command = parts
            .first()
            .ok_or(CommandError::Invalid("empty command"))?;

        match (*command, parts.as_slice()) {
            ("IDENTIFY", [..]) if parts.len() == 1 => Ok(Self::Identify),
            ("AUTH", [..]) if parts.len() == 1 => Ok(Self::Auth),
            ("SUB", [_, topic, channel]) => {
                validate_name(topic).map_err(|_| CommandError::BadTopic)?;
                validate_name(channel).map_err(|_| CommandError::BadChannel)?;
                Ok(Self::Subscribe {
                    topic: (*topic).to_owned(),
                    channel: (*channel).to_owned(),
                })
            }
            ("PUB", [_, topic]) => {
                validate_name(topic).map_err(|_| CommandError::BadTopic)?;
                Ok(Self::Publish {
                    topic: (*topic).to_owned(),
                })
            }
            ("MPUB", [_, topic]) => {
                validate_name(topic).map_err(|_| CommandError::BadTopic)?;
                Ok(Self::MultiPublish {
                    topic: (*topic).to_owned(),
                })
            }
            ("DPUB", [_, topic, delay]) => {
                validate_name(topic).map_err(|_| CommandError::BadTopic)?;
                Ok(Self::DeferredPublish {
                    topic: (*topic).to_owned(),
                    delay_ms: parse_u64(delay)?,
                })
            }
            ("RDY", [_]) => Ok(Self::Ready(1)),
            ("RDY", [_, count]) => Ok(Self::Ready(parse_u64(count)?)),
            ("FIN", [_, id]) => Ok(Self::Finish(parse_id(id)?)),
            ("REQ", [_, id, delay]) => Ok(Self::Requeue {
                id: parse_id(id)?,
                delay_ms: parse_i64(delay)?,
            }),
            ("TOUCH", [_, id]) => Ok(Self::Touch(parse_id(id)?)),
            ("CLS", [..]) if parts.len() == 1 => Ok(Self::Close),
            ("NOP", [..]) if parts.len() == 1 => Ok(Self::Noop),
            _ => Err(CommandError::Invalid("unsupported command or arguments")),
        }
    }
}

fn parse_u64(value: &str) -> Result<u64, CommandError> {
    value
        .parse()
        .map_err(|_| CommandError::Invalid("expected unsigned integer"))
}

fn parse_i64(value: &str) -> Result<i64, CommandError> {
    value
        .parse()
        .map_err(|_| CommandError::Invalid("expected integer"))
}

fn parse_id(value: &str) -> Result<u64, CommandError> {
    if value.len() != 16 {
        return Err(CommandError::Invalid(
            "message ID must contain 16 hex bytes",
        ));
    }
    u64::from_str_radix(value, 16)
        .map_err(|_| CommandError::Invalid("message ID must contain 16 hex bytes"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_core_commands() {
        assert_eq!(
            Command::parse(b"SUB orders workers\n").unwrap(),
            Command::Subscribe {
                topic: "orders".into(),
                channel: "workers".into()
            }
        );
        assert_eq!(Command::parse(b"RDY 100\n").unwrap(), Command::Ready(100));
        assert_eq!(Command::parse(b"RDY\n").unwrap(), Command::Ready(1));
        assert_eq!(
            Command::parse(b"REQ 00000000000000ff 42\n").unwrap(),
            Command::Requeue {
                id: 255,
                delay_ms: 42
            }
        );
        assert_eq!(
            Command::parse(b"REQ 00000000000000ff -1\n").unwrap(),
            Command::Requeue {
                id: 255,
                delay_ms: -1
            }
        );
    }
}
