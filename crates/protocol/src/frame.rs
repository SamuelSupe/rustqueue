use bytes::{BufMut, BytesMut};

#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameType {
    Response = 0,
    Error = 1,
    Message = 2,
}

pub fn encode_frame(frame_type: FrameType, body: &[u8]) -> BytesMut {
    let size = 4usize.saturating_add(body.len());
    let mut frame = BytesMut::with_capacity(4 + size);
    frame.put_u32(size as u32);
    frame.put_i32(frame_type as i32);
    frame.extend_from_slice(body);
    frame
}

pub fn encode_message(timestamp_ns: i64, attempts: u16, id: u64, body: &[u8]) -> BytesMut {
    let header = encode_message_header(timestamp_ns, attempts, id, body.len());
    let mut frame = BytesMut::with_capacity(header.len().saturating_add(body.len()));
    frame.extend_from_slice(&header);
    frame.extend_from_slice(body);
    frame
}

pub fn encode_message_header(
    timestamp_ns: i64,
    attempts: u16,
    id: u64,
    body_len: usize,
) -> [u8; 34] {
    const MESSAGE_HEADER_BYTES: usize = 26;
    let mut header = [0u8; 34];
    let frame_body_bytes = MESSAGE_HEADER_BYTES.saturating_add(body_len);
    header[..4].copy_from_slice(&((4 + frame_body_bytes) as u32).to_be_bytes());
    header[4..8].copy_from_slice(&(FrameType::Message as i32).to_be_bytes());
    header[8..16].copy_from_slice(&timestamp_ns.to_be_bytes());
    header[16..18].copy_from_slice(&attempts.to_be_bytes());
    encode_hex_id(id, &mut header[18..34]);
    header
}

fn encode_hex_id(mut id: u64, output: &mut [u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in output.iter_mut().rev() {
        *byte = HEX[(id & 0xf) as usize];
        id >>= 4;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_uses_network_byte_order() {
        let frame = encode_frame(FrameType::Response, b"OK");
        assert_eq!(&frame[..], &[0, 0, 0, 6, 0, 0, 0, 0, b'O', b'K']);
    }

    #[test]
    fn message_is_encoded_directly_into_the_final_frame() {
        let frame = encode_message(7, 2, 0x0123_abcd, b"body");
        assert_eq!(u32::from_be_bytes(frame[..4].try_into().unwrap()), 34);
        assert_eq!(i32::from_be_bytes(frame[4..8].try_into().unwrap()), 2);
        assert_eq!(&frame[18..34], b"000000000123abcd");
        assert_eq!(&frame[34..], b"body");
    }

    #[test]
    fn message_header_can_be_written_separately_from_the_body() {
        let header = encode_message_header(7, 2, 0x0123_abcd, 4);
        assert_eq!(u32::from_be_bytes(header[..4].try_into().unwrap()), 34);
        assert_eq!(&header[18..34], b"000000000123abcd");
    }
}
