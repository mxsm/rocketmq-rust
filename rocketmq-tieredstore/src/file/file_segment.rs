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

use std::ops::Range;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use parking_lot::Mutex;
use rocketmq_error::RocketMQError;

use crate::error::TieredStoreErrorKind;
use crate::error::{self};
use crate::metadata::FileSegmentMetadata;
use crate::provider::TieredStoreProvider;

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum FileSegmentType {
    CommitLog,
    ConsumeQueue,
    Index,
}

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileSegmentStatus {
    New,
    Sealed,
    Deleted,
}

#[trait_variant::make(FileSegment: Send)]
pub trait FileSegmentInner: Sync {
    fn path(&self) -> &str;

    fn segment_type(&self) -> FileSegmentType;

    fn base_offset(&self) -> u64;

    fn append_position(&self) -> u64;

    fn commit_position(&self) -> u64;

    fn metadata(&self) -> FileSegmentMetadata;

    async fn append(&self, data: Bytes, timestamp_millis: i64) -> Result<usize, RocketMQError>;

    async fn read(&self, range: Range<u64>) -> Result<Bytes, RocketMQError>;

    async fn commit(&self) -> Result<(), RocketMQError>;

    async fn seal(&self) -> Result<(), RocketMQError>;
}

pub struct TieredFileSegment<P> {
    path: String,
    segment_type: FileSegmentType,
    base_offset: u64,
    max_size: u64,
    append_position: AtomicU64,
    commit_position: AtomicU64,
    metadata: Mutex<FileSegmentMetadata>,
    pending: Mutex<Vec<Bytes>>,
    provider: P,
}

impl<P> TieredFileSegment<P> {
    pub fn new(
        path: String,
        segment_type: FileSegmentType,
        base_offset: u64,
        max_size: u64,
        metadata: FileSegmentMetadata,
        provider: P,
    ) -> Self {
        let size = metadata.size;
        Self {
            path,
            segment_type,
            base_offset,
            max_size,
            append_position: AtomicU64::new(size),
            commit_position: AtomicU64::new(size),
            metadata: Mutex::new(metadata),
            pending: Mutex::new(Vec::new()),
            provider,
        }
    }

    fn current_status(&self) -> FileSegmentStatus {
        self.metadata.lock().status
    }

    pub fn max_size(&self) -> u64 {
        self.max_size
    }

    pub fn next_absolute_offset(&self) -> u64 {
        self.base_offset
            .saturating_add(self.append_position.load(Ordering::Acquire))
    }

    pub fn committed_absolute_end(&self) -> u64 {
        self.base_offset
            .saturating_add(self.commit_position.load(Ordering::Acquire))
    }

    pub fn min_timestamp(&self) -> i64 {
        self.metadata.lock().begin_timestamp
    }

    pub fn max_timestamp(&self) -> i64 {
        self.metadata.lock().end_timestamp
    }

    pub fn can_hold(&self, absolute_offset: u64, len: usize) -> bool {
        if self.current_status() != FileSegmentStatus::New {
            return false;
        }
        let relative_offset = absolute_offset.saturating_sub(self.base_offset);
        absolute_offset >= self.base_offset && relative_offset.saturating_add(len as u64) <= self.max_size
    }

    pub fn contains_committed_range(&self, absolute_offset: u64, len: usize) -> bool {
        if self.current_status() == FileSegmentStatus::Deleted {
            return false;
        }
        absolute_offset >= self.base_offset
            && absolute_offset.saturating_add(len as u64) <= self.committed_absolute_end()
    }

    pub fn is_expired_sealed(&self, expire_before_millis: i64) -> bool {
        let metadata = self.metadata.lock();
        metadata.status == FileSegmentStatus::Sealed
            && metadata.end_timestamp != i64::MIN
            && metadata.end_timestamp < expire_before_millis
    }

    pub fn mark_deleted(&self) {
        self.metadata.lock().status = FileSegmentStatus::Deleted;
    }
}

impl<P> FileSegment for TieredFileSegment<P>
where
    P: TieredStoreProvider,
{
    fn path(&self) -> &str {
        &self.path
    }

    fn segment_type(&self) -> FileSegmentType {
        self.segment_type
    }

    fn base_offset(&self) -> u64 {
        self.base_offset
    }

    fn append_position(&self) -> u64 {
        self.append_position.load(Ordering::Acquire)
    }

    fn commit_position(&self) -> u64 {
        self.commit_position.load(Ordering::Acquire)
    }

    fn metadata(&self) -> FileSegmentMetadata {
        self.metadata.lock().clone()
    }

    async fn append(&self, data: Bytes, timestamp_millis: i64) -> Result<usize, RocketMQError> {
        if self.current_status() != FileSegmentStatus::New {
            return Err(error::from_kind(
                TieredStoreErrorKind::SegmentClosed,
                self.path.clone(),
                "tiered file segment is not appendable",
            ));
        }

        let len = data.len();
        let mut pending = self.pending.lock();
        let position = self.append_position.load(Ordering::Acquire);
        let next = position.saturating_add(len as u64);
        if next > self.max_size {
            return Err(error::from_kind(
                TieredStoreErrorKind::SegmentFull,
                self.path.clone(),
                "tiered file segment is full",
            ));
        }

        {
            let mut metadata = self.metadata.lock();
            metadata.begin_timestamp = metadata.begin_timestamp.min(timestamp_millis);
            metadata.end_timestamp = metadata.end_timestamp.max(timestamp_millis);
        }
        pending.push(data);
        self.append_position.store(next, Ordering::Release);
        Ok(len)
    }

    async fn read(&self, range: Range<u64>) -> Result<Bytes, RocketMQError> {
        let commit_position = self.commit_position.load(Ordering::Acquire);
        if range.start >= commit_position || range.end <= range.start {
            return Err(error::from_kind(
                TieredStoreErrorKind::IllegalOffset,
                self.path.clone(),
                "invalid tiered segment read range",
            ));
        }
        let end = range.end.min(commit_position);
        let started = std::time::Instant::now();
        let result = self
            .provider
            .read(self.path.clone(), range.start, (end - range.start) as usize)
            .await;
        rocketmq_observability::metrics::tiered_store::record_provider_read(
            &self.path,
            result.as_ref().map(|bytes| bytes.len() as u64).unwrap_or(0),
            result.is_ok(),
            started.elapsed().as_millis() as u64,
        );
        result
    }

    async fn commit(&self) -> Result<(), RocketMQError> {
        let buffers = {
            let mut pending = self.pending.lock();
            if pending.is_empty() {
                return Ok(());
            }
            std::mem::take(&mut *pending)
        };

        let mut position = self.commit_position.load(Ordering::Acquire);
        for (index, buffer) in buffers.iter().cloned().enumerate() {
            let started = std::time::Instant::now();
            let result = self.provider.write(self.path.clone(), position, buffer).await;
            rocketmq_observability::metrics::tiered_store::record_provider_write(
                &self.path,
                result.as_ref().map(|written| *written as u64).unwrap_or(0),
                result.is_ok(),
                started.elapsed().as_millis() as u64,
            );
            match result {
                Ok(written) => {
                    position = position.saturating_add(written as u64);
                    self.commit_position.store(position, Ordering::Release);
                }
                Err(error) => {
                    let mut pending = self.pending.lock();
                    pending.extend(buffers[index..].iter().cloned());
                    return Err(error);
                }
            }
        }

        {
            let mut metadata = self.metadata.lock();
            metadata.size = position;
        }
        Ok(())
    }

    async fn seal(&self) -> Result<(), RocketMQError> {
        let mut metadata = self.metadata.lock();
        metadata.status = FileSegmentStatus::Sealed;
        metadata.seal_timestamp = current_time_millis();
        Ok(())
    }
}

fn current_time_millis() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use rocketmq_error::RocketMQError;

    use crate::file::FileSegment;
    use crate::file::FileSegmentType;
    use crate::provider::MemoryProvider;
    use crate::provider::TieredStoreProvider;

    #[tokio::test]
    async fn append_commit_and_read_segment() -> Result<(), RocketMQError> {
        let provider = MemoryProvider::default();
        let segment = provider
            .create_segment("topic/0/commitlog/000".to_owned(), FileSegmentType::CommitLog, 0, 64)
            .await?;

        segment.append(Bytes::from_static(b"hello"), 100).await?;
        assert_eq!(segment.append_position(), 5);
        assert_eq!(segment.commit_position(), 0);

        segment.commit().await?;
        assert_eq!(segment.commit_position(), 5);
        assert_eq!(segment.read(0..5).await?, Bytes::from_static(b"hello"));
        Ok(())
    }
}
