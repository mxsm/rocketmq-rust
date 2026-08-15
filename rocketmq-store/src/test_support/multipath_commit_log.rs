// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use crate::consume_queue::mapped_file_queue::MappedFileQueue;
use crate::log_file::commit_log_path_set::CommitLogPathSet;
use crate::log_file::commit_log_path_set::StoreFaultInjector as StoreFaultInjectorContract;
use crate::log_file::commit_log_path_set::StoreFaultPoint;
use crate::log_file::mapped_file::MappedFile;

use super::StoreFaultInjector;

/// Focused CommitLog harness used by the F-08 integration suite.
pub struct MultipathCommitLogHarness {
    queue: MappedFileQueue,
    paths: Arc<CommitLogPathSet>,
    mapped_file_size: u64,
}

impl MultipathCommitLogHarness {
    pub fn try_new(
        writable: Vec<PathBuf>,
        readonly: Vec<PathBuf>,
        mapped_file_size: u64,
        injector: Option<Arc<StoreFaultInjector>>,
    ) -> io::Result<Self> {
        let injector: Arc<dyn StoreFaultInjectorContract> = injector.unwrap_or_default();
        let paths = Arc::new(CommitLogPathSet::try_new_with_fault_injector(
            writable,
            readonly,
            mapped_file_size,
            injector,
        )?);
        let queue = MappedFileQueue::new_commit_log(Arc::clone(&paths), mapped_file_size, None);
        Ok(Self {
            queue,
            paths,
            mapped_file_size,
        })
    }

    pub fn load(&mut self) -> bool {
        self.queue.load()
    }

    pub fn create_segment(&self, offset: u64, payload: &[u8]) -> io::Result<PathBuf> {
        if payload.len() > self.mapped_file_size as usize {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "payload exceeds segment"));
        }
        let mapped_file = self
            .queue
            .try_create_mapped_file(offset)
            .ok_or_else(|| io::Error::other("segment creation failed"))?;
        if self
            .paths
            .should_fail(StoreFaultPoint::Append, Path::new(mapped_file.get_file_name().as_str()))
        {
            self.queue.fence_writes();
            return Err(io::Error::other("injected CommitLog append failure"));
        }
        if !mapped_file.append_message_bytes(payload) {
            self.queue.fence_writes();
            return Err(io::Error::other("CommitLog append failed"));
        }
        Ok(PathBuf::from(mapped_file.get_file_name().as_str()))
    }

    pub fn segment_owners(&self) -> Vec<(u64, PathBuf)> {
        self.queue
            .get_mapped_files()
            .iter()
            .map(|mapped_file| {
                let path = PathBuf::from(mapped_file.get_file_name().as_str());
                (
                    mapped_file.get_file_from_offset(),
                    path.parent().expect("segment has parent").to_path_buf(),
                )
            })
            .collect()
    }

    pub fn read_range(&self, offset: i64, size: i32) -> io::Result<Vec<u8>> {
        let owners = self.queue.get_mapped_files();
        for mapped_file in owners.iter() {
            let start = mapped_file.get_file_from_offset();
            if start <= offset as u64
                && offset as u64 <= start.saturating_add(self.mapped_file_size)
                && self
                    .paths
                    .should_fail(StoreFaultPoint::Read, Path::new(mapped_file.get_file_name().as_str()))
            {
                return Err(io::Error::other("injected CommitLog read failure"));
            }
        }
        let results = self
            .queue
            .read_handle()
            .get_bulk_data(offset, size)
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "CommitLog range unavailable"))?;
        let mut bytes = Vec::with_capacity(size.max(0) as usize);
        for result in results {
            bytes.extend_from_slice(result.get_buffer());
        }
        Ok(bytes)
    }

    pub fn retire(&self, root: impl AsRef<Path>) -> io::Result<()> {
        self.paths.retire(root.as_ref())
    }

    pub fn candidate_roots(&self, point: StoreFaultPoint) -> io::Result<Vec<PathBuf>> {
        self.paths.creation_candidates(point)
    }

    pub fn truncate(&self, offset: i64) -> bool {
        if self.queue.get_mapped_files().iter().any(|mapped_file| {
            self.paths.should_fail(
                StoreFaultPoint::Truncate,
                Path::new(mapped_file.get_file_name().as_str()),
            )
        }) {
            self.queue.fence_writes();
            return false;
        }
        self.queue.try_truncate_dirty_files(offset)
    }

    pub fn is_write_fenced(&self) -> bool {
        self.queue.is_write_fenced()
    }

    pub fn flush(&self) -> bool {
        if self.queue.get_mapped_files().iter().any(|mapped_file| {
            self.paths
                .should_fail(StoreFaultPoint::Flush, Path::new(mapped_file.get_file_name().as_str()))
        }) {
            self.queue.fence_writes();
            return false;
        }
        self.queue.try_flush(0).is_ok()
    }

    pub fn destroy(&mut self) -> bool {
        if self.queue.get_mapped_files().iter().any(|mapped_file| {
            self.paths
                .should_fail(StoreFaultPoint::Delete, Path::new(mapped_file.get_file_name().as_str()))
        }) {
            self.queue.fence_writes();
            return false;
        }
        self.queue.destroy_with_outcome()
    }
}
