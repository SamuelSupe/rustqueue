use std::io;

pub const HEADER_LEN: usize = 48;
// Public publish payload is capped at 64 MiB. The durable envelope also needs
// room for per-message IDs, timestamps, lengths and checksums.
pub const MAX_RECORD_BYTES: usize = 72 * 1024 * 1024;
const MAGIC: &[u8; 4] = b"RQV7";
const VERSION: u8 = 7;

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordKind {
    PublishBatch = 1,
    EvictionGap = 2,
    Noop = 3,
}

impl TryFrom<u8> for RecordKind {
    type Error = io::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::PublishBatch),
            2 => Ok(Self::EvictionGap),
            3 => Ok(Self::Noop),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown record kind {value}"),
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub kind: RecordKind,
    pub flags: u16,
    pub index: u64,
    pub timestamp_ns: i64,
    pub message_id: u64,
    pub available_at_ms: i64,
    pub payload: Vec<u8>,
}

impl Record {
    pub fn encoded_len(&self) -> usize {
        HEADER_LEN + self.payload.len()
    }

    pub fn encode(&self) -> io::Result<Vec<u8>> {
        if self.payload.len() > MAX_RECORD_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "record payload exceeds storage limit",
            ));
        }
        let mut bytes = vec![0u8; self.encoded_len()];
        bytes[0..4].copy_from_slice(MAGIC);
        bytes[4] = VERSION;
        bytes[5] = self.kind as u8;
        bytes[6..8].copy_from_slice(&self.flags.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.index.to_be_bytes());
        bytes[16..24].copy_from_slice(&self.timestamp_ns.to_be_bytes());
        bytes[24..32].copy_from_slice(&self.message_id.to_be_bytes());
        bytes[32..40].copy_from_slice(&self.available_at_ms.to_be_bytes());
        bytes[40..44].copy_from_slice(&(self.payload.len() as u32).to_be_bytes());
        bytes[HEADER_LEN..].copy_from_slice(&self.payload);

        let checksum = crc32c::crc32c_append(crc32c::crc32c(&bytes[..44]), &bytes[HEADER_LEN..]);
        bytes[44..48].copy_from_slice(&checksum.to_be_bytes());
        Ok(bytes)
    }

    pub fn decode(header: &[u8; HEADER_LEN], payload: Vec<u8>) -> io::Result<Self> {
        if &header[0..4] != MAGIC || header[4] != VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid record magic or version",
            ));
        }
        let expected_len = u32::from_be_bytes(header[40..44].try_into().unwrap()) as usize;
        if expected_len != payload.len() || expected_len > MAX_RECORD_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid record payload length",
            ));
        }
        let checksum = crc32c::crc32c_append(crc32c::crc32c(&header[..44]), &payload);
        let expected_crc = u32::from_be_bytes(header[44..48].try_into().unwrap());
        if checksum != expected_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "record checksum mismatch",
            ));
        }
        Ok(Self {
            kind: RecordKind::try_from(header[5])?,
            flags: u16::from_be_bytes(header[6..8].try_into().unwrap()),
            index: u64::from_be_bytes(header[8..16].try_into().unwrap()),
            timestamp_ns: i64::from_be_bytes(header[16..24].try_into().unwrap()),
            message_id: u64::from_be_bytes(header[24..32].try_into().unwrap()),
            available_at_ms: i64::from_be_bytes(header[32..40].try_into().unwrap()),
            payload,
        })
    }

    pub fn payload_len(header: &[u8; HEADER_LEN]) -> io::Result<usize> {
        if &header[0..4] != MAGIC || header[4] != VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid record magic or version",
            ));
        }
        let len = u32::from_be_bytes(header[40..44].try_into().unwrap()) as usize;
        if len > MAX_RECORD_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "record payload exceeds storage limit",
            ));
        }
        Ok(len)
    }
}
