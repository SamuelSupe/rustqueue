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
    let mut vectors = Vec::with_capacity(MAX_VECTORS);
    while part_index < parts.len() {
        vectors.clear();
        vectors.push(IoSlice::new(&parts[part_index][part_offset..]));
        vectors.extend(
            parts[part_index + 1..]
                .iter()
                .take(MAX_VECTORS - 1)
                .map(|part| IoSlice::new(part)),
        );
        let written = match writer.write_vectored(&vectors) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => result?,
        };
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

    struct InterruptingShortWriter {
        output: Vec<u8>,
        calls: usize,
        interrupted_calls: usize,
        short_writes: usize,
    }

    impl InterruptingShortWriter {
        fn new() -> Self {
            Self {
                output: Vec::new(),
                calls: 0,
                interrupted_calls: 0,
                short_writes: 0,
            }
        }
    }

    impl Write for InterruptingShortWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.output.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn write_vectored(&mut self, buffers: &[IoSlice<'_>]) -> io::Result<usize> {
            self.calls += 1;
            if self.calls % 2 == 1 {
                self.interrupted_calls += 1;
                return Err(io::Error::new(io::ErrorKind::Interrupted, "retry"));
            }

            let available = buffers.iter().map(|buffer| buffer.len()).sum::<usize>();
            let limit = available.min(2);
            let mut remaining = limit;
            for buffer in buffers {
                let written = buffer.len().min(remaining);
                self.output.extend_from_slice(&buffer[..written]);
                remaining -= written;
                if remaining == 0 {
                    break;
                }
            }
            if limit < available {
                self.short_writes += 1;
            }
            Ok(limit)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn writes_all_parts_in_order() {
        let header = [7u8; HEADER_LEN];
        let mut output = Vec::new();
        write_parts(&mut output, &header, &[b"one", b"", b"two"]).unwrap();
        assert_eq!(&output[..HEADER_LEN], &header);
        assert_eq!(&output[HEADER_LEN..], b"onetwo");
    }

    #[test]
    fn retries_interrupted_vectored_writes_and_preserves_part_order() {
        let header = [7u8; HEADER_LEN];
        let mut writer = InterruptingShortWriter::new();

        write_parts(&mut writer, &header, &[b"one", b"", b"two", b"three"]).unwrap();

        let mut expected = header.to_vec();
        expected.extend_from_slice(b"onetwothree");
        assert_eq!(writer.output, expected);
        assert!(writer.interrupted_calls > 0);
        assert!(writer.short_writes > 0);
    }
}
