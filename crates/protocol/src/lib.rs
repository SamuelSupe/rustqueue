mod command;
mod frame;
mod identify;
mod mpub;
mod name;

pub use command::{Command, CommandError};
pub use frame::{encode_frame, encode_message, encode_message_header, FrameType};
pub use identify::{IdentifyRequest, IdentifyResponse};
pub use mpub::{parse_mpub_body, parse_mpub_bytes, MpubError, MAX_MPUB_MESSAGES};
pub use name::{validate_name, NameError};

pub const MAGIC_V2: &[u8; 4] = b"  V2";
pub const HEARTBEAT: &[u8] = b"_heartbeat_";
pub const OK: &[u8] = b"OK";
pub const CLOSE_WAIT: &[u8] = b"CLOSE_WAIT";
pub const MESSAGE_ID_LEN: usize = 16;
pub const MAX_MESSAGE_BYTES: usize = 100 * 1024 * 1024;
pub const MAX_BATCH_BYTES: usize = 128 * 1024 * 1024;
