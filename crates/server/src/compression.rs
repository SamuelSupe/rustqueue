use async_compression::tokio::write::DeflateEncoder;
use async_compression::Level;
use flate2::{Decompress, FlushDecompress, Status};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf, ReadHalf, WriteHalf};

pub trait ClientIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T> ClientIo for T where T: AsyncRead + AsyncWrite + Unpin + Send {}
pub type BoxIo = Box<dyn ClientIo>;

pub fn snappy(io: BoxIo) -> BoxIo {
    Box::new(tokio_snappy::SnappyIO::new(io))
}

pub fn deflate(io: BoxIo, level: i32) -> BoxIo {
    let (read, write) = tokio::io::split(io);
    let read = DeflateReader::new(read);
    let write = DeflateEncoder::with_quality(write, Level::Precise(level));
    Box::new(SplitIo { read, write })
}

struct SplitIo {
    read: DeflateReader<ReadHalf<BoxIo>>,
    write: DeflateEncoder<WriteHalf<BoxIo>>,
}

/// Reads the raw, indefinitely sync-flushed DEFLATE stream used by NSQ.
/// `pynsq` legitimately produces `BufError` when zlib needs another input
/// chunk, so that status is treated as backpressure instead of corruption.
struct DeflateReader<R> {
    inner: R,
    decoder: Decompress,
    compressed: Box<[u8; 16 * 1024]>,
    start: usize,
    end: usize,
}

impl<R> DeflateReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            decoder: Decompress::new(false),
            compressed: Box::new([0; 16 * 1024]),
            start: 0,
            end: 0,
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for DeflateReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.as_mut().get_mut();
        if output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        loop {
            if this.start < this.end {
                let before_in = this.decoder.total_in();
                let before_out = this.decoder.total_out();
                let status = this.decoder.decompress(
                    &this.compressed[this.start..this.end],
                    output.initialize_unfilled(),
                    FlushDecompress::None,
                )?;
                let consumed = (this.decoder.total_in() - before_in) as usize;
                let produced = (this.decoder.total_out() - before_out) as usize;
                this.start += consumed;
                output.advance(produced);

                if produced > 0 || status == Status::StreamEnd {
                    return Poll::Ready(Ok(()));
                }
                if consumed > 0 {
                    continue;
                }
            }

            if this.start > 0 {
                this.compressed.copy_within(this.start..this.end, 0);
                this.end -= this.start;
                this.start = 0;
            }
            if this.end == this.compressed.len() {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "DEFLATE decoder made no progress",
                )));
            }

            let mut incoming = ReadBuf::new(&mut this.compressed[this.end..]);
            match Pin::new(&mut this.inner).poll_read(context, &mut incoming) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => {
                    let read = incoming.filled().len();
                    if read == 0 {
                        return Poll::Ready(Ok(()));
                    }
                    this.end += read;
                }
            }
        }
    }
}

impl AsyncRead for SplitIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.read).poll_read(context, buffer)
    }
}

impl AsyncWrite for SplitIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.write).poll_write(context, buffer)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.write).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.write).poll_shutdown(context)
    }
}

#[cfg(test)]
mod tests {
    use super::DeflateReader;
    use flate2::write::DeflateEncoder as SyncDeflateEncoder;
    use flate2::Compression;
    use std::io::Write;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn reads_python_style_sync_flushed_stream() {
        let mut encoder = SyncDeflateEncoder::new(Vec::new(), Compression::new(6));
        encoder.write_all(b"SUB events workers\n").unwrap();
        encoder.flush().unwrap();
        encoder.write_all(b"RDY 1\n").unwrap();
        encoder.flush().unwrap();
        let compressed = encoder.get_ref().clone();

        let (mut sender, receiver) = tokio::io::duplex(64);
        let send = tokio::spawn(async move {
            for chunk in compressed.chunks(3) {
                sender.write_all(chunk).await.unwrap();
            }
        });
        let mut reader = DeflateReader::new(receiver);
        let mut decoded = Vec::new();
        reader.read_to_end(&mut decoded).await.unwrap();
        send.await.unwrap();
        assert_eq!(decoded, b"SUB events workers\nRDY 1\n");
    }
}
