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
        let Some(location) = self.location(from_index)? else {
            return Ok(());
        };
        self.current.sync_all()?;
        let directory = File::open(&self.directory)?;
        let paths = segment_paths(&self.directory)?;
        let target = paths
            .iter()
            .position(|path| path == location.segment.as_ref())
            .ok_or_else(|| {
                StorageError::Io(io::Error::new(
                    io::ErrorKind::NotFound,
                    "truncate target segment disappeared",
                ))
            })?;
        for path in paths.iter().skip(target + 1).rev() {
            if path.exists() {
                fs::remove_file(path)?;
            }
            recovery_index::remove(path)?;
            self.checksums.remove(path);
            self.sealed_indexes.remove(path);
            directory.sync_all()?;
        }
        let target_file = OpenOptions::new()
            .write(true)
            .open(location.segment.as_ref())?;
        target_file.set_len(location.offset)?;
        target_file.sync_all()?;
        recovery_index::remove(location.segment.as_ref())?;
        directory.sync_all()?;
        self.sealed_indexes.remove(location.segment.as_ref());
        self.resident_records
            .retain(|record| record.index < from_index);
        let (locations, _, bytes, crc32c) = scan_segment(location.segment.as_ref(), true)?;
        self.resident_records
            .retain(|record| record.segment.as_ref() != location.segment.as_ref());
        self.resident_records.extend(locations);
        self.checksums
            .insert(location.segment.as_ref().clone(), (bytes, crc32c));
        self.current_path = location.segment.as_ref().clone();
        self.current_segment = location.segment;
        self.current = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.current_path)?;
        self.current_len = self.current.metadata()?.len();
        self.refresh_aggregates();
        self.start_index = self.first_index().unwrap_or(from_index);
        self.current.sync_all()?;
        directory.sync_all()?;
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
        let directory = File::open(&self.directory)?;
        crash_failpoint("gc_before_segment_delete");
        let removed_through = removable
            .last()
            .and_then(|path| self.record_index_range(path))
            .map_or(through_index, |(_, last)| last);
        for path in &removable {
            fs::remove_file(path)?;
            recovery_index::remove(path)?;
            crash_failpoint("gc_after_segment_delete_before_dir_fsync");
            directory.sync_all()?;
            self.checksums.remove(path);
            self.sealed_indexes.remove(path);
        }
        self.resident_records
            .retain(|record| record.index > removed_through);
        self.refresh_aggregates();
        self.start_index = self
            .first_index()
            .unwrap_or_else(|| through_index.saturating_add(1));
        directory.sync_all()?;
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
        let mut targets: Vec<_> = self
            .checksums
            .iter()
            .filter(|(path, _)| include_active || *path != &self.current_path)
            .map(|(path, (expected_len, expected_crc32c))| ScrubTarget {
                path: path.clone(),
                expected_len: *expected_len,
                expected_crc32c: *expected_crc32c,
                kind: scrub::ScrubKind::Segment,
            })
            .collect();
        targets.extend(self.sealed_indexes.iter().map(|(path, index)| ScrubTarget {
            path: recovery_index::index_path(path),
            expected_len: index.metadata_offset.saturating_add(index.metadata_len),
            expected_crc32c: index.body_crc32c,
            kind: scrub::ScrubKind::RecoveryIndex {
                body_offset: recovery_index::HEADER_LEN,
            },
        }));
        Ok(targets)
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
                // Eviction and purge may only remove a contiguous prefix. If
                // the oldest segment is still leased, choosing a later one
                // would advance channel gaps without being able to delete the
                // corresponding prefix.
                break;
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
