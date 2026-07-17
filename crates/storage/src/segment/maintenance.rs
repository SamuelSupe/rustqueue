use super::*;

impl SegmentLog {
    pub fn truncate_suffix(&mut self, from_index: u64) -> Result<(), StorageError> {
        self.ensure_available()?;
        let result = self.truncate_suffix_inner(from_index);
        if result.is_err() {
            self.isolate();
        }
        result
    }

    fn truncate_suffix_inner(&mut self, from_index: u64) -> Result<(), StorageError> {
        let Some(first_removed) = self
            .records
            .iter()
            .position(|record| record.index >= from_index)
        else {
            return Ok(());
        };
        let location = self.records[first_removed].clone();
        let removed_paths: Vec<PathBuf> = self.records[first_removed..]
            .iter()
            .map(|item| item.segment.as_ref().clone())
            .filter(|path| path != location.segment.as_ref())
            .collect();
        OpenOptions::new()
            .write(true)
            .open(location.segment.as_ref())?
            .set_len(location.offset)?;
        recovery_index::remove(location.segment.as_ref())?;
        self.recovery_metadata.remove(location.segment.as_ref());
        for path in removed_paths {
            if path.exists() {
                fs::remove_file(&path)?;
            }
            recovery_index::remove(&path)?;
            self.checksums.remove(&path);
            self.recovery_metadata.remove(&path);
        }
        let (_, _, bytes, crc32c) = scan_segment(location.segment.as_ref(), true)?;
        self.checksums
            .insert(location.segment.as_ref().clone(), (bytes, crc32c));
        self.records.truncate(first_removed);
        self.current_path = location.segment.as_ref().clone();
        self.current_segment = location.segment;
        self.current = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.current_path)?;
        self.current_len = self.current.metadata()?.len();
        self.current.sync_all()?;
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }

    /// Removes only a contiguous prefix of complete immutable segments.
    pub fn purge_prefix(&mut self, through_index: u64) -> Result<usize, StorageError> {
        self.purge_prefix_retaining(through_index, &BTreeSet::new())
    }

    pub fn purge_prefix_retaining(
        &mut self,
        through_index: u64,
        retained: &BTreeSet<PathBuf>,
    ) -> Result<usize, StorageError> {
        self.ensure_available()?;
        let result = self.purge_prefix_retaining_inner(through_index, retained);
        if result.is_err() {
            self.isolate();
        }
        result
    }

    fn purge_prefix_retaining_inner(
        &mut self,
        through_index: u64,
        retained: &BTreeSet<PathBuf>,
    ) -> Result<usize, StorageError> {
        let mut removable = Vec::new();
        for path in segment_paths(&self.directory)? {
            if path == self.current_path || retained.contains(&path) {
                break;
            }
            let Some((_, last_index)) = self.record_index_range(&path) else {
                break;
            };
            if last_index > through_index {
                break;
            }
            removable.push(path);
        }
        if removable.is_empty() {
            return Ok(0);
        }
        self.current.sync_all()?;
        crash_failpoint("gc_before_segment_delete");
        for path in &removable {
            fs::remove_file(path)?;
            recovery_index::remove(path)?;
            self.checksums.remove(path);
            self.recovery_metadata.remove(path);
        }
        let removed_through = removable
            .last()
            .and_then(|path| self.record_index_range(path))
            .map_or(through_index, |(_, last)| last);
        let keep_from = self
            .records
            .partition_point(|record| record.index <= removed_through);
        self.records.drain(..keep_from);
        self.start_index = self
            .records
            .first()
            .map_or_else(|| through_index.saturating_add(1), |record| record.index);
        crash_failpoint("gc_after_segment_delete_before_dir_fsync");
        File::open(&self.directory)?.sync_all()?;
        Ok(removable.len())
    }

    pub fn scrub(&self) -> Result<usize, StorageError> {
        self.ensure_available()?;
        let mut count = 0;
        for target in self.scrub_targets(true)? {
            count += match scrub::verify(&target, 0) {
                Ok(records) => records,
                Err(error) => {
                    self.isolate();
                    return Err(error);
                }
            };
        }
        Ok(count)
    }

    pub fn scrub_targets(&self, include_active: bool) -> Result<Vec<ScrubTarget>, StorageError> {
        self.ensure_available()?;
        Ok(self
            .checksums
            .iter()
            .filter(|(path, _)| include_active || *path != &self.current_path)
            .map(|(path, (expected_len, expected_crc32c))| ScrubTarget {
                path: path.clone(),
                expected_len: *expected_len,
                expected_crc32c: *expected_crc32c,
            })
            .collect())
    }

    pub fn scrub_target(
        target: &ScrubTarget,
        bytes_per_second: u64,
    ) -> Result<usize, StorageError> {
        scrub::verify(target, bytes_per_second)
    }

    pub fn segment_paths(&self) -> Result<Vec<PathBuf>, StorageError> {
        Ok(segment_paths(&self.directory)?)
    }

    pub fn seal(&mut self) -> Result<(), StorageError> {
        self.ensure_available()?;
        let result = self.seal_inner();
        if result.is_err() {
            self.isolate();
        }
        result
    }

    pub fn immutable_file(&self, path: &Path) -> Option<(u64, u32)> {
        (path != self.current_path)
            .then(|| self.checksums.get(path).copied())
            .flatten()
    }

    pub fn oldest_inactive_boundary(&self) -> Result<Option<(PathBuf, u64)>, StorageError> {
        self.oldest_inactive_boundary_retaining(&BTreeSet::new())
    }

    pub fn oldest_inactive_boundary_retaining(
        &self,
        retained: &BTreeSet<PathBuf>,
    ) -> Result<Option<(PathBuf, u64)>, StorageError> {
        for path in segment_paths(&self.directory)? {
            if path == self.current_path || retained.contains(&path) {
                continue;
            }
            if let Some((_, last_index)) = self.record_index_range(&path) {
                return Ok(Some((path, last_index)));
            }
        }
        Ok(None)
    }

    fn seal_inner(&mut self) -> Result<(), StorageError> {
        self.current.sync_all()?;
        if self.current_len > 0 {
            self.rotate(self.next_index())?;
        }
        Ok(())
    }
}
