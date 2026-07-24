use std::io;

pub const HEADER_LEN: usize = 48;
pub const LEGACY_MAX_RECORD_BYTES: usize = 72 * 1024 * 1024;
// A 128 MiB command body can grow by 20 bytes per message in the durable
// envelope. Keep enough bounded headroom for the maximum supported batch.
pub const MAX_RECORD_BYTES: usize = 160 * 1024 * 1024;
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

impl RecordKind {
    pub const fn required_writer_feature_level(self, payload_len: usize) -> u32 {
        match self {
            Self::PublishBatch if payload_len > LEGACY_MAX_RECORD_BYTES => 2,
            Self::PublishBatch | Self::EvictionGap | Self::Noop => 1,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordHeader {
    pub kind: RecordKind,
    pub flags: u16,
    pub index: u64,
    pub timestamp_ns: i64,
    pub message_id: u64,
    pub available_at_ms: i64,
}

impl Record {
    pub fn encoded_len(&self) -> usize {
        HEADER_LEN + self.payload.len()
    }

    pub fn encode(&self) -> io::Result<Vec<u8>> {
        let header = self.header().encode(&[self.payload.as_slice()])?;
        let mut bytes = Vec::with_capacity(self.encoded_len());
        bytes.extend_from_slice(&header);
        bytes.extend_from_slice(&self.payload);
        Ok(bytes)
    }

    pub fn header(&self) -> RecordHeader {
        RecordHeader {
            kind: self.kind,
            flags: self.flags,
            index: self.index,
            timestamp_ns: self.timestamp_ns,
            message_id: self.message_id,
            available_at_ms: self.available_at_ms,
        }
    }

    pub fn decode(header: &[u8; HEADER_LEN], payload: Vec<u8>) -> io::Result<Self> {
        let decoded = RecordHeader::decode(header)?;
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
            kind: decoded.kind,
            flags: decoded.flags,
            index: decoded.index,
            timestamp_ns: decoded.timestamp_ns,
            message_id: decoded.message_id,
            available_at_ms: decoded.available_at_ms,
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

impl RecordHeader {
    pub fn decode(header: &[u8; HEADER_LEN]) -> io::Result<Self> {
        if &header[0..4] != MAGIC || header[4] != VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid record magic or version",
            ));
        }
        Ok(Self {
            kind: RecordKind::try_from(header[5])?,
            flags: u16::from_be_bytes(header[6..8].try_into().unwrap()),
            index: u64::from_be_bytes(header[8..16].try_into().unwrap()),
            timestamp_ns: i64::from_be_bytes(header[16..24].try_into().unwrap()),
            message_id: u64::from_be_bytes(header[24..32].try_into().unwrap()),
            available_at_ms: i64::from_be_bytes(header[32..40].try_into().unwrap()),
        })
    }

    pub fn encode(&self, payload: &[&[u8]]) -> io::Result<[u8; HEADER_LEN]> {
        let payload_len = payload
            .iter()
            .try_fold(0usize, |total, part| total.checked_add(part.len()));
        let Some(payload_len) = payload_len.filter(|len| *len <= MAX_RECORD_BYTES) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "record payload exceeds storage limit",
            ));
        };
        let payload_len = u32::try_from(payload_len).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "record payload is too large")
        })?;
        let mut header = [0u8; HEADER_LEN];
        header[0..4].copy_from_slice(MAGIC);
        header[4] = VERSION;
        header[5] = self.kind as u8;
        header[6..8].copy_from_slice(&self.flags.to_be_bytes());
        header[8..16].copy_from_slice(&self.index.to_be_bytes());
        header[16..24].copy_from_slice(&self.timestamp_ns.to_be_bytes());
        header[24..32].copy_from_slice(&self.message_id.to_be_bytes());
        header[32..40].copy_from_slice(&self.available_at_ms.to_be_bytes());
        header[40..44].copy_from_slice(&payload_len.to_be_bytes());
        let checksum = payload
            .iter()
            .fold(crc32c::crc32c(&header[..44]), |crc, part| {
                crc32c::crc32c_append(crc, part)
            });
        header[44..48].copy_from_slice(&checksum.to_be_bytes());
        Ok(header)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_v7_publish_records_require_feature_level_two() {
        assert_eq!(
            RecordKind::PublishBatch.required_writer_feature_level(LEGACY_MAX_RECORD_BYTES),
            1
        );
        assert_eq!(
            RecordKind::PublishBatch
                .required_writer_feature_level(LEGACY_MAX_RECORD_BYTES.saturating_add(1)),
            2
        );
        assert_eq!(
            RecordKind::EvictionGap.required_writer_feature_level(MAX_RECORD_BYTES),
            1
        );
    }
}
