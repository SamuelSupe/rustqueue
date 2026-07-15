use rustqueue_storage::snapshot_archive_plan;
use std::fs::File as StdFile;
use std::future::Future;
use std::io::{self, SeekFrom};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};

const MAX_BLOCKING_READ: usize = 256 * 1024;

pub struct SnapshotData {
    inner: SnapshotDataInner,
}

pub(crate) enum SnapshotInput {
    Archive(PathBuf),
    Generation(PathBuf),
}

enum SnapshotDataInner {
    Reader(VirtualArchive),
    Receiver {
        path: PathBuf,
        file: tokio::fs::File,
    },
}

struct VirtualArchive {
    directory: PathBuf,
    parts: Vec<ArchivePart>,
    position: u64,
    total_bytes: u64,
}

struct ArchivePart {
    start: u64,
    len: u64,
    source: ArchiveSource,
    file_position: u64,
    pending_seek: Option<u64>,
}

enum ArchiveSource {
    Memory(Arc<[u8]>),
    File {
        path: PathBuf,
        file: Option<tokio::fs::File>,
        opening: Option<tokio::task::JoinHandle<io::Result<StdFile>>>,
    },
}

impl SnapshotData {
    pub(crate) fn reader(directory: impl AsRef<Path>) -> io::Result<Self> {
        let plan = snapshot_archive_plan(directory.as_ref())?;
        let mut parts = Vec::with_capacity(plan.files.len() + 1);
        let header: Arc<[u8]> = plan.header.into();
        let mut start = 0u64;
        parts.push(ArchivePart {
            start,
            len: header.len() as u64,
            source: ArchiveSource::Memory(header),
            file_position: 0,
            pending_seek: None,
        });
        start = parts[0].len;
        for (path, len) in plan.files {
            parts.push(ArchivePart {
                start,
                len,
                source: ArchiveSource::File {
                    path,
                    file: None,
                    opening: None,
                },
                file_position: 0,
                pending_seek: None,
            });
            start = start
                .checked_add(len)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "snapshot overflow"))?;
        }
        if start != plan.total_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot archive length mismatch",
            ));
        }
        Ok(Self {
            inner: SnapshotDataInner::Reader(VirtualArchive {
                directory: directory.as_ref().to_path_buf(),
                parts,
                position: 0,
                total_bytes: start,
            }),
        })
    }

    pub(crate) async fn receiver(path: PathBuf) -> io::Result<Self> {
        let file = tokio::fs::File::create(&path).await?;
        Ok(Self {
            inner: SnapshotDataInner::Receiver { path, file },
        })
    }

    pub(crate) async fn finish_received(self) -> io::Result<SnapshotInput> {
        match self.inner {
            SnapshotDataInner::Receiver { path, file } => {
                file.sync_all().await?;
                Ok(SnapshotInput::Archive(path))
            }
            SnapshotDataInner::Reader(reader) => Ok(SnapshotInput::Generation(reader.directory)),
        }
    }
}

impl AsyncRead for SnapshotData {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut self.inner {
            SnapshotDataInner::Receiver { file, .. } => Pin::new(file).poll_read(context, buffer),
            SnapshotDataInner::Reader(reader) => reader.poll_read(context, buffer),
        }
    }
}

impl VirtualArchive {
    fn poll_read(
        &mut self,
        context: &mut Context<'_>,
        target: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.position >= self.total_bytes || target.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let part = self
            .parts
            .iter_mut()
            .find(|part| self.position >= part.start && self.position < part.start + part.len)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "snapshot part missing"));
        let part = match part {
            Ok(part) => part,
            Err(error) => return Poll::Ready(Err(error)),
        };
        let local = self.position - part.start;
        let wanted = (part.len - local)
            .min(target.remaining() as u64)
            .min(MAX_BLOCKING_READ as u64) as usize;
        match &mut part.source {
            ArchiveSource::Memory(bytes) => {
                target.put_slice(&bytes[local as usize..local as usize + wanted]);
            }
            ArchiveSource::File {
                path,
                file,
                opening,
            } => {
                if file.is_none() {
                    if opening.is_none() {
                        let path = path.clone();
                        *opening = Some(tokio::task::spawn_blocking(move || StdFile::open(path)));
                    }
                    let opened = match Pin::new(opening.as_mut().unwrap()).poll(context) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Ok(Ok(file))) => file,
                        Poll::Ready(Ok(Err(error))) => return Poll::Ready(Err(error)),
                        Poll::Ready(Err(error)) => {
                            return Poll::Ready(Err(io::Error::other(error.to_string())))
                        }
                    };
                    *opening = None;
                    *file = Some(tokio::fs::File::from_std(opened));
                }
                let file = file.as_mut().unwrap();
                if part.file_position != local {
                    if part.pending_seek != Some(local) {
                        if let Err(error) = Pin::new(&mut *file).start_seek(SeekFrom::Start(local))
                        {
                            return Poll::Ready(Err(error));
                        }
                        part.pending_seek = Some(local);
                    }
                    match Pin::new(&mut *file).poll_complete(context) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                        Poll::Ready(Ok(position)) => {
                            part.pending_seek = None;
                            part.file_position = position;
                        }
                    }
                }
                let initialized = target.initialize_unfilled_to(wanted);
                let mut limited = ReadBuf::new(initialized);
                match Pin::new(file).poll_read(context, &mut limited) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Ready(Ok(())) => {}
                }
                let read = limited.filled().len();
                if read == 0 && wanted > 0 {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "snapshot source file was truncated",
                    )));
                }
                target.advance(read);
                self.position += read as u64;
                part.file_position += read as u64;
                return Poll::Ready(Ok(()));
            }
        }
        self.position += wanted as u64;
        Poll::Ready(Ok(()))
    }

    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        let next = match position {
            SeekFrom::Start(position) => position as i128,
            SeekFrom::Current(delta) => self.position as i128 + delta as i128,
            SeekFrom::End(delta) => self.total_bytes as i128 + delta as i128,
        };
        if next < 0 || next > self.total_bytes as i128 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "snapshot seek is outside the archive",
            ));
        }
        self.position = next as u64;
        Ok(self.position)
    }
}

impl AsyncSeek for SnapshotData {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        match &mut self.inner {
            SnapshotDataInner::Reader(reader) => reader.seek(position).map(|_| ()),
            SnapshotDataInner::Receiver { file, .. } => Pin::new(file).start_seek(position),
        }
    }

    fn poll_complete(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<u64>> {
        match &mut self.inner {
            SnapshotDataInner::Reader(reader) => Poll::Ready(Ok(reader.position)),
            SnapshotDataInner::Receiver { file, .. } => Pin::new(file).poll_complete(context),
        }
    }
}

impl AsyncWrite for SnapshotData {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        match &mut self.inner {
            SnapshotDataInner::Receiver { file, .. } => Pin::new(file).poll_write(context, buffer),
            SnapshotDataInner::Reader(_) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "snapshot reader is immutable",
            ))),
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        match &mut self.inner {
            SnapshotDataInner::Receiver { file, .. } => Pin::new(file).poll_flush(context),
            SnapshotDataInner::Reader(_) => Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        match &mut self.inner {
            SnapshotDataInner::Receiver { file, .. } => Pin::new(file).poll_shutdown(context),
            SnapshotDataInner::Reader(_) => Poll::Ready(Ok(())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustqueue_storage::{GenerationStore, LinkedGenerationFile};
    use tempfile::tempdir;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    #[tokio::test]
    async fn virtual_archive_is_seekable_without_materializing_payloads() {
        let root = tempdir().unwrap();
        let sources = tempdir().unwrap();
        let state = sources.path().join("state");
        let payload = sources.path().join("payload");
        std::fs::write(&state, b"state").unwrap();
        std::fs::write(&payload, b"payload").unwrap();
        let store = GenerationStore::open(root.path()).unwrap();
        let files: Vec<LinkedGenerationFile> = vec![
            GenerationStore::describe_source(&state, "snapshot-state.bin").unwrap(),
            GenerationStore::describe_source(&payload, "payloads/000000.rqseg").unwrap(),
        ];
        let generation = store.install_linked("one", 1, &files).unwrap();
        let mut snapshot = SnapshotData::reader(generation).unwrap();
        let SnapshotDataInner::Reader(reader) = &snapshot.inner else {
            panic!("snapshot generation must be readable");
        };
        assert!(reader.parts.iter().all(|part| match &part.source {
            ArchiveSource::Memory(_) => true,
            ArchiveSource::File { file, opening, .. } => file.is_none() && opening.is_none(),
        }));
        let mut all = Vec::new();
        snapshot.read_to_end(&mut all).await.unwrap();
        snapshot.seek(SeekFrom::Start(0)).await.unwrap();
        let mut prefix = [0u8; 8];
        snapshot.read_exact(&mut prefix).await.unwrap();
        assert_eq!(&prefix, b"RQSARCH1");
        assert!(all
            .windows(b"payload".len())
            .any(|window| window == b"payload"));
    }
}
