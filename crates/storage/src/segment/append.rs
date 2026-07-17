use crate::HEADER_LEN;
use std::io::{self, IoSlice, Write};

const MAX_VECTORS: usize = 128;

pub(super) fn write_parts(
    writer: &mut impl Write,
    header: &[u8; HEADER_LEN],
    payload: &[&[u8]],
) -> io::Result<()> {
    let parts: Vec<_> = std::iter::once(header.as_slice())
        .chain(payload.iter().copied())
        .filter(|part| !part.is_empty())
        .collect();
    let mut part_index = 0usize;
    let mut part_offset = 0usize;
    while part_index < parts.len() {
        let mut vectors = Vec::with_capacity(MAX_VECTORS);
        vectors.push(IoSlice::new(&parts[part_index][part_offset..]));
        vectors.extend(
            parts[part_index + 1..]
                .iter()
                .take(MAX_VECTORS - 1)
                .map(|part| IoSlice::new(part)),
        );
        let written = writer.write_vectored(&vectors)?;
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write record parts",
            ));
        }
        let mut remaining = written;
        while remaining > 0 {
            let available = parts[part_index].len() - part_offset;
            if remaining < available {
                part_offset += remaining;
                remaining = 0;
            } else {
                remaining -= available;
                part_index += 1;
                part_offset = 0;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_all_parts_in_order() {
        let header = [7u8; HEADER_LEN];
        let mut output = Vec::new();
        write_parts(&mut output, &header, &[b"one", b"", b"two"]).unwrap();
        assert_eq!(&output[..HEADER_LEN], &header);
        assert_eq!(&output[HEADER_LEN..], b"onetwo");
    }
}
