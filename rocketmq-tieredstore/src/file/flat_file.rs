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

use std::sync::Arc;

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use parking_lot::Mutex;
use rocketmq_error::RocketMQError;

use crate::config::TieredStoreConfig;
use crate::error;
use crate::file::FileSegment;
use crate::file::FileSegmentStatus;
use crate::file::FileSegmentType;
use crate::file::TieredFileSegment;
use crate::metadata::JsonMetadataStore;
use crate::metadata::TieredMetadataStore;
use crate::metadata::TopicQueueMetadata;
use crate::provider::TieredStoreProvider;

pub const CONSUME_QUEUE_UNIT_SIZE: usize = 20;
pub const MESSAGE_STORE_TIMESTAMP_POSITION: usize = 56;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumeQueueUnit {
    pub commit_log_offset: i64,
    pub size: i32,
    pub tags_code: i64,
}

impl ConsumeQueueUnit {
    pub fn encode(self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(CONSUME_QUEUE_UNIT_SIZE);
        bytes.put_i64(self.commit_log_offset);
        bytes.put_i32(self.size);
        bytes.put_i64(self.tags_code);
        bytes.freeze()
    }

    pub fn decode(mut bytes: Bytes) -> Result<Self, RocketMQError> {
        if bytes.len() != CONSUME_QUEUE_UNIT_SIZE {
            return Err(error::illegal_argument(format!(
                "tiered consume queue unit must be {CONSUME_QUEUE_UNIT_SIZE} bytes, got {}",
                bytes.len()
            )));
        }
        Ok(Self {
            commit_log_offset: bytes.get_i64(),
            size: bytes.get_i32(),
            tags_code: bytes.get_i64(),
        })
    }
}

pub struct TieredFlatFile<P>
where
    P: TieredStoreProvider,
{
    topic: String,
    queue_id: i32,
    config: Arc<TieredStoreConfig>,
    metadata_store: Arc<JsonMetadataStore>,
    provider: P,
    commit_log_segments: Mutex<Vec<Arc<TieredFileSegment<P>>>>,
    consume_queue_segments: Mutex<Vec<Arc<TieredFileSegment<P>>>>,
}

impl<P> TieredFlatFile<P>
where
    P: TieredStoreProvider,
{
    pub fn new(
        topic: String,
        queue_id: i32,
        config: Arc<TieredStoreConfig>,
        metadata_store: Arc<JsonMetadataStore>,
        provider: P,
    ) -> Self {
        Self {
            topic,
            queue_id,
            config,
            metadata_store,
            provider,
            commit_log_segments: Mutex::new(Vec::new()),
            consume_queue_segments: Mutex::new(Vec::new()),
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn queue_id(&self) -> i32 {
        self.queue_id
    }

    pub async fn append_commit_log(&self, message: Bytes, store_timestamp: i64) -> Result<u64, RocketMQError> {
        let append_len = message.len();
        let absolute_offset = self.commit_log_append_offset();
        let segment = self
            .ensure_writable_segment(
                FileSegmentType::CommitLog,
                self.config.commit_log_segment_size,
                absolute_offset,
                append_len,
            )
            .await?;
        let offset = segment.next_absolute_offset();
        segment.append(message, store_timestamp).await?;
        Ok(offset)
    }

    pub async fn append_consume_queue(
        &self,
        queue_offset: i64,
        unit: ConsumeQueueUnit,
        store_timestamp: i64,
    ) -> Result<(), RocketMQError> {
        if queue_offset < 0 {
            return Err(error::illegal_argument(
                "tiered consume queue offset must not be negative",
            ));
        }
        let absolute_offset = (queue_offset as u64).saturating_mul(CONSUME_QUEUE_UNIT_SIZE as u64);
        let expected_offset = self.consume_queue_append_byte_offset();
        if !self.consume_queue_segments.lock().is_empty() && absolute_offset != expected_offset {
            return Err(error::illegal_argument(format!(
                "tiered consume queue offset gap, expected byte offset {expected_offset}, got {absolute_offset}"
            )));
        }
        let segment = self
            .ensure_writable_segment(
                FileSegmentType::ConsumeQueue,
                self.config.consume_queue_segment_size,
                absolute_offset,
                CONSUME_QUEUE_UNIT_SIZE,
            )
            .await?;
        segment.append(unit.encode(), store_timestamp).await?;
        Ok(())
    }

    pub async fn commit(&self) -> Result<(), RocketMQError> {
        let commit_log_segments = { self.commit_log_segments.lock().clone() };
        for segment in commit_log_segments {
            segment.commit().await?;
            self.metadata_store.upsert_file_segment(segment.metadata()).await?;
        }

        let consume_queue_segments = { self.consume_queue_segments.lock().clone() };
        for segment in consume_queue_segments {
            segment.commit().await?;
            self.metadata_store.upsert_file_segment(segment.metadata()).await?;
        }
        if !self.consume_queue_segments.lock().is_empty() {
            self.metadata_store
                .upsert_queue(TopicQueueMetadata {
                    topic: self.topic.clone(),
                    queue_id: self.queue_id,
                    min_offset: self.consume_queue_min_offset(),
                    max_offset: self.consume_queue_commit_offset(),
                    update_timestamp: current_time_millis(),
                })
                .await?;
        }
        Ok(())
    }

    pub async fn recover(&self) -> Result<(), RocketMQError> {
        let mut commit_log_segments = Vec::new();
        let mut consume_queue_segments = Vec::new();

        for mut metadata in self
            .metadata_store
            .list_file_segments(&self.topic, self.queue_id)
            .await?
        {
            if metadata.status == FileSegmentStatus::Deleted {
                continue;
            }
            let max_size = self.max_segment_size(metadata.segment_type)?;
            let real_size = self.provider.segment_size(metadata.path.clone()).await?;
            if metadata.size != real_size {
                metadata.size = real_size.min(max_size);
                self.metadata_store.upsert_file_segment(metadata.clone()).await?;
            }
            let segment = Arc::new(TieredFileSegment::new(
                metadata.path.clone(),
                metadata.segment_type,
                metadata.base_offset,
                max_size,
                metadata,
                self.provider.clone(),
            ));
            match segment.segment_type() {
                FileSegmentType::CommitLog => commit_log_segments.push(segment),
                FileSegmentType::ConsumeQueue => consume_queue_segments.push(segment),
                FileSegmentType::Index => {}
            }
        }

        commit_log_segments.sort_by_key(|segment| segment.base_offset());
        consume_queue_segments.sort_by_key(|segment| segment.base_offset());
        *self.commit_log_segments.lock() = commit_log_segments;
        *self.consume_queue_segments.lock() = consume_queue_segments;
        Ok(())
    }

    pub fn commit_log_append_offset(&self) -> u64 {
        self.commit_log_segments
            .lock()
            .last()
            .map(|segment| segment.next_absolute_offset())
            .unwrap_or(0)
    }

    pub fn consume_queue_min_offset(&self) -> i64 {
        self.consume_queue_segments
            .lock()
            .first()
            .map(|segment| (segment.base_offset() / CONSUME_QUEUE_UNIT_SIZE as u64) as i64)
            .unwrap_or(0)
    }

    pub fn consume_queue_append_offset(&self) -> i64 {
        (self.consume_queue_append_byte_offset() / CONSUME_QUEUE_UNIT_SIZE as u64) as i64
    }

    pub fn consume_queue_commit_offset(&self) -> i64 {
        self.consume_queue_segments
            .lock()
            .last()
            .map(|segment| {
                ((segment.base_offset().saturating_add(segment.commit_position())) / CONSUME_QUEUE_UNIT_SIZE as u64)
                    as i64
            })
            .unwrap_or(0)
    }

    pub async fn cleanup_expired(&self, now_millis: i64) -> Result<(), RocketMQError> {
        let reserved_millis = self.config.file_reserved_time.as_millis() as i64;
        let expire_before_millis = now_millis.saturating_sub(reserved_millis);
        let previous_consume_queue_commit_offset = self.consume_queue_commit_offset();

        for segment in self.expired_segments(FileSegmentType::ConsumeQueue, expire_before_millis) {
            self.delete_segment(segment).await?;
        }
        self.refresh_queue_metadata(previous_consume_queue_commit_offset)
            .await?;

        let first_retained_commit_log_offset = self.first_retained_commit_log_offset().await?;
        let expired_commit_log_segments = self
            .expired_segments(FileSegmentType::CommitLog, expire_before_millis)
            .into_iter()
            .filter(|segment| commit_log_segment_is_unreferenced(segment, first_retained_commit_log_offset));

        for segment in expired_commit_log_segments {
            self.delete_segment(segment).await?;
        }
        Ok(())
    }

    pub async fn read_consume_queue_unit(&self, queue_offset: i64) -> Result<Option<ConsumeQueueUnit>, RocketMQError> {
        if queue_offset < 0 {
            return Err(error::illegal_argument(
                "tiered consume queue offset must not be negative",
            ));
        }
        let byte_offset = (queue_offset as u64).saturating_mul(CONSUME_QUEUE_UNIT_SIZE as u64);
        let Some(bytes) = self
            .read_from_segments(FileSegmentType::ConsumeQueue, byte_offset, CONSUME_QUEUE_UNIT_SIZE)
            .await?
        else {
            return Ok(None);
        };
        ConsumeQueueUnit::decode(bytes).map(Some)
    }

    pub async fn read_message_by_queue_offset(&self, queue_offset: i64) -> Result<Option<Bytes>, RocketMQError> {
        let Some(unit) = self.read_consume_queue_unit(queue_offset).await? else {
            return Ok(None);
        };
        if unit.commit_log_offset < 0 || unit.size <= 0 {
            return Ok(None);
        }
        self.read_commit_log(unit.commit_log_offset as u64, unit.size as usize)
            .await
    }

    pub async fn read_message_store_timestamp(&self, queue_offset: i64) -> Result<Option<i64>, RocketMQError> {
        let Some(message) = self.read_message_by_queue_offset(queue_offset).await? else {
            return Ok(None);
        };
        Ok(decode_message_store_timestamp(&message))
    }

    pub async fn read_commit_log(&self, offset: u64, length: usize) -> Result<Option<Bytes>, RocketMQError> {
        self.read_from_segments(FileSegmentType::CommitLog, offset, length)
            .await
    }

    pub async fn queue_offset_by_time(&self, timestamp_millis: i64) -> Result<i64, RocketMQError> {
        let cq_min = self.consume_queue_min_offset();
        let cq_max = self.consume_queue_commit_offset().saturating_sub(1);
        if cq_max == -1 || cq_max < cq_min {
            return Ok(cq_min);
        }

        let Some(max_store_time) = self.read_message_store_timestamp(cq_max).await? else {
            return Ok(cq_min);
        };
        if max_store_time < timestamp_millis {
            return Ok(cq_max.saturating_add(1));
        }

        let Some(min_store_time) = self.read_message_store_timestamp(cq_min).await? else {
            return Ok(cq_min);
        };
        if min_store_time > timestamp_millis {
            return Ok(cq_min);
        }

        let (mut low, mut high) = self.timestamp_search_range(timestamp_millis, cq_min, cq_max);
        while low < high {
            let middle = low.saturating_add((high - low) / 2);
            let Some(store_time) = self.read_message_store_timestamp(middle).await? else {
                return Ok(low);
            };
            if store_time < timestamp_millis {
                low = middle.saturating_add(1);
            } else {
                high = middle;
            }
        }
        Ok(low)
    }

    pub fn min_store_timestamp(&self) -> i64 {
        let mut min_store_time = -1;
        if let Some(timestamp) = segment_min_timestamp(&self.commit_log_segments.lock()) {
            min_store_time = min_store_time.max(timestamp);
        }
        if let Some(timestamp) = segment_min_timestamp(&self.consume_queue_segments.lock()) {
            min_store_time = min_store_time.max(timestamp);
        }
        min_store_time
    }

    pub fn max_store_timestamp(&self) -> i64 {
        segment_max_timestamp(&self.commit_log_segments.lock()).unwrap_or(-1)
    }

    async fn ensure_writable_segment(
        &self,
        segment_type: FileSegmentType,
        max_size: u64,
        absolute_offset: u64,
        append_len: usize,
    ) -> Result<Arc<TieredFileSegment<P>>, RocketMQError> {
        let existing = match segment_type {
            FileSegmentType::CommitLog => self.commit_log_segments.lock().last().cloned(),
            FileSegmentType::ConsumeQueue => self.consume_queue_segments.lock().last().cloned(),
            FileSegmentType::Index => return Err(error::internal("index segment is managed by tiered index service")),
        };
        if let Some(segment) = existing {
            if segment.can_hold(absolute_offset, append_len) {
                return Ok(segment);
            }
            segment.commit().await?;
            segment.seal().await?;
            self.metadata_store.upsert_file_segment(segment.metadata()).await?;
        }

        let path = segment_path(&self.topic, self.queue_id, segment_type, absolute_offset);
        let segment = Arc::new(
            self.provider
                .create_segment(path, segment_type, absolute_offset, max_size)
                .await?,
        );
        self.metadata_store.upsert_file_segment(segment.metadata()).await?;
        match segment_type {
            FileSegmentType::CommitLog => {
                let mut segments = self.commit_log_segments.lock();
                if let Some(existing) = segments
                    .last()
                    .filter(|segment| segment.can_hold(absolute_offset, append_len))
                    .cloned()
                {
                    return Ok(existing);
                }
                segments.push(segment.clone());
            }
            FileSegmentType::ConsumeQueue => {
                let mut segments = self.consume_queue_segments.lock();
                if let Some(existing) = segments
                    .last()
                    .filter(|segment| segment.can_hold(absolute_offset, append_len))
                    .cloned()
                {
                    return Ok(existing);
                }
                segments.push(segment.clone());
            }
            FileSegmentType::Index => {}
        }
        Ok(segment)
    }

    fn consume_queue_append_byte_offset(&self) -> u64 {
        self.consume_queue_segments
            .lock()
            .last()
            .map(|segment| segment.next_absolute_offset())
            .unwrap_or(0)
    }

    fn max_segment_size(&self, segment_type: FileSegmentType) -> Result<u64, RocketMQError> {
        match segment_type {
            FileSegmentType::CommitLog => Ok(self.config.commit_log_segment_size),
            FileSegmentType::ConsumeQueue => Ok(self.config.consume_queue_segment_size),
            FileSegmentType::Index => Err(error::internal("index segment is managed by tiered index service")),
        }
    }

    fn expired_segments(
        &self,
        segment_type: FileSegmentType,
        expire_before_millis: i64,
    ) -> Vec<Arc<TieredFileSegment<P>>> {
        let segments = match segment_type {
            FileSegmentType::CommitLog => self.commit_log_segments.lock().clone(),
            FileSegmentType::ConsumeQueue => self.consume_queue_segments.lock().clone(),
            FileSegmentType::Index => Vec::new(),
        };
        segments
            .into_iter()
            .filter(|segment| segment.is_expired_sealed(expire_before_millis))
            .collect()
    }

    fn remove_segment(&self, segment_type: FileSegmentType, base_offset: u64, path: &str) {
        let mut segments = match segment_type {
            FileSegmentType::CommitLog => self.commit_log_segments.lock(),
            FileSegmentType::ConsumeQueue => self.consume_queue_segments.lock(),
            FileSegmentType::Index => return,
        };
        segments.retain(|segment| segment.base_offset() != base_offset || segment.path() != path);
    }

    async fn delete_segment(&self, segment: Arc<TieredFileSegment<P>>) -> Result<(), RocketMQError> {
        let metadata = segment.metadata();
        self.metadata_store
            .mark_file_segment_deleted(&metadata.path, metadata.base_offset)
            .await?;
        segment.mark_deleted();
        self.provider.delete(metadata.path.clone()).await?;
        self.remove_segment(metadata.segment_type, metadata.base_offset, &metadata.path);
        Ok(())
    }

    async fn refresh_queue_metadata(&self, previous_commit_offset: i64) -> Result<(), RocketMQError> {
        let has_consume_queue_segments = !self.consume_queue_segments.lock().is_empty();
        let (min_offset, max_offset) = if has_consume_queue_segments {
            (self.consume_queue_min_offset(), self.consume_queue_commit_offset())
        } else {
            (previous_commit_offset, previous_commit_offset)
        };
        self.metadata_store
            .upsert_queue(TopicQueueMetadata {
                topic: self.topic.clone(),
                queue_id: self.queue_id,
                min_offset,
                max_offset,
                update_timestamp: current_time_millis(),
            })
            .await
    }

    async fn first_retained_commit_log_offset(&self) -> Result<Option<u64>, RocketMQError> {
        let consume_queue_segments = self.consume_queue_segments.lock().clone();
        let mut first_offset: Option<u64> = None;
        for segment in consume_queue_segments {
            if segment.commit_position() < CONSUME_QUEUE_UNIT_SIZE as u64 {
                continue;
            }
            let unit = ConsumeQueueUnit::decode(segment.read(0..CONSUME_QUEUE_UNIT_SIZE as u64).await?)?;
            if unit.commit_log_offset < 0 {
                continue;
            }
            first_offset = Some(match first_offset {
                Some(offset) => offset.min(unit.commit_log_offset as u64),
                None => unit.commit_log_offset as u64,
            });
        }
        Ok(first_offset)
    }

    fn timestamp_search_range(&self, timestamp_millis: i64, cq_min: i64, cq_max: i64) -> (i64, i64) {
        let segments = self.consume_queue_segments.lock().clone();
        for segment in segments {
            let min_timestamp = segment.min_timestamp();
            let max_timestamp = segment.max_timestamp();
            if min_timestamp <= timestamp_millis && timestamp_millis <= max_timestamp {
                let min_offset = (segment.base_offset() / CONSUME_QUEUE_UNIT_SIZE as u64) as i64;
                let max_offset =
                    (segment.committed_absolute_end() / CONSUME_QUEUE_UNIT_SIZE as u64).saturating_sub(1) as i64;
                return (min_offset.max(cq_min), max_offset.min(cq_max));
            }
        }
        (cq_min, cq_max)
    }

    async fn read_from_segments(
        &self,
        segment_type: FileSegmentType,
        absolute_offset: u64,
        length: usize,
    ) -> Result<Option<Bytes>, RocketMQError> {
        if length == 0 {
            return Ok(Some(Bytes::new()));
        }
        let segments = match segment_type {
            FileSegmentType::CommitLog => self.commit_log_segments.lock().clone(),
            FileSegmentType::ConsumeQueue => self.consume_queue_segments.lock().clone(),
            FileSegmentType::Index => return Ok(None),
        };
        let Some(segment) = segments
            .into_iter()
            .find(|segment| segment.contains_committed_range(absolute_offset, length))
        else {
            return Ok(None);
        };
        let relative_offset = absolute_offset.saturating_sub(segment.base_offset());
        let bytes = segment
            .read(relative_offset..relative_offset.saturating_add(length as u64))
            .await?;
        Ok(Some(bytes))
    }
}

fn commit_log_segment_is_unreferenced<P>(
    segment: &TieredFileSegment<P>,
    first_retained_commit_log_offset: Option<u64>,
) -> bool {
    first_retained_commit_log_offset
        .map(|offset| segment.committed_absolute_end() <= offset)
        .unwrap_or(true)
}

fn segment_path(topic: &str, queue_id: i32, segment_type: FileSegmentType, base_offset: u64) -> String {
    let segment_name = match segment_type {
        FileSegmentType::CommitLog => "commitlog",
        FileSegmentType::ConsumeQueue => "consumequeue",
        FileSegmentType::Index => "index",
    };
    format!("{topic}/{queue_id}/{segment_name}/{base_offset:020}")
}

fn current_time_millis() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(_) => 0,
    }
}

fn decode_message_store_timestamp(message: &Bytes) -> Option<i64> {
    let end = MESSAGE_STORE_TIMESTAMP_POSITION.saturating_add(std::mem::size_of::<i64>());
    if message.len() < end {
        return None;
    }
    let mut bytes = [0; std::mem::size_of::<i64>()];
    bytes.copy_from_slice(&message[MESSAGE_STORE_TIMESTAMP_POSITION..end]);
    Some(i64::from_be_bytes(bytes))
}

fn segment_min_timestamp<P>(segments: &[Arc<TieredFileSegment<P>>]) -> Option<i64> {
    segments
        .iter()
        .map(|segment| segment.min_timestamp())
        .filter(|timestamp| *timestamp != i64::MAX)
        .min()
}

fn segment_max_timestamp<P>(segments: &[Arc<TieredFileSegment<P>>]) -> Option<i64> {
    segments
        .iter()
        .map(|segment| segment.max_timestamp())
        .filter(|timestamp| *timestamp != i64::MIN)
        .max()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use rocketmq_error::RocketMQError;

    use super::*;
    use crate::metadata::JsonMetadataStore;
    use crate::metadata::TieredMetadataStore;
    use crate::provider::MemoryProvider;
    use crate::provider::TieredStoreProvider;

    fn test_config(root: std::path::PathBuf) -> Arc<TieredStoreConfig> {
        Arc::new(TieredStoreConfig {
            store_path_root_dir: root,
            backend_provider: "memory".to_owned(),
            commit_log_segment_size: 8,
            consume_queue_segment_size: CONSUME_QUEUE_UNIT_SIZE as u64 * 2,
            file_reserved_time: Duration::from_millis(1),
            ..TieredStoreConfig::default()
        })
    }

    #[tokio::test]
    async fn consume_queue_offsets_are_contiguous() -> Result<(), RocketMQError> {
        let temp_dir = tempfile::tempdir().map_err(|err| RocketMQError::Internal(err.to_string()))?;
        let config = test_config(temp_dir.path().to_path_buf());
        let metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        let flat_file = TieredFlatFile::new(
            "TopicA".to_owned(),
            0,
            config,
            metadata_store,
            MemoryProvider::default(),
        );

        flat_file
            .append_consume_queue(
                10,
                ConsumeQueueUnit {
                    commit_log_offset: 0,
                    size: 4,
                    tags_code: 1,
                },
                100,
            )
            .await?;
        assert_eq!(flat_file.consume_queue_min_offset(), 10);
        assert_eq!(flat_file.consume_queue_append_offset(), 11);

        let result = flat_file
            .append_consume_queue(
                12,
                ConsumeQueueUnit {
                    commit_log_offset: 4,
                    size: 4,
                    tags_code: 1,
                },
                101,
            )
            .await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn recovers_segments_from_metadata_and_provider_size() -> Result<(), RocketMQError> {
        let temp_dir = tempfile::tempdir().map_err(|err| RocketMQError::Internal(err.to_string()))?;
        let config = test_config(temp_dir.path().to_path_buf());
        let metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        let provider = MemoryProvider::default();
        let flat_file = TieredFlatFile::new(
            "TopicA".to_owned(),
            0,
            config.clone(),
            metadata_store.clone(),
            provider.clone(),
        );

        flat_file.append_commit_log(Bytes::from_static(b"abcd"), 100).await?;
        flat_file
            .append_consume_queue(
                0,
                ConsumeQueueUnit {
                    commit_log_offset: 0,
                    size: 4,
                    tags_code: 1,
                },
                100,
            )
            .await?;
        flat_file.commit().await?;

        let recovered = TieredFlatFile::new("TopicA".to_owned(), 0, config, metadata_store, provider);
        recovered.recover().await?;

        assert_eq!(recovered.commit_log_append_offset(), 4);
        assert_eq!(recovered.consume_queue_min_offset(), 0);
        assert_eq!(recovered.consume_queue_commit_offset(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn cleanup_deletes_expired_sealed_segments() -> Result<(), RocketMQError> {
        let temp_dir = tempfile::tempdir().map_err(|err| RocketMQError::Internal(err.to_string()))?;
        let config = test_config(temp_dir.path().to_path_buf());
        let metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        let provider = MemoryProvider::default();
        let flat_file = TieredFlatFile::new("TopicA".to_owned(), 0, config, metadata_store, provider.clone());

        flat_file.append_commit_log(Bytes::from_static(b"abcd"), 10).await?;
        flat_file.append_commit_log(Bytes::from_static(b"efgh"), 11).await?;
        flat_file.append_commit_log(Bytes::from_static(b"ijkl"), 1_000).await?;
        flat_file.commit().await?;

        let first_path = segment_path("TopicA", 0, FileSegmentType::CommitLog, 0);
        assert_eq!(provider.segment_size(first_path.clone()).await?, 8);

        flat_file.cleanup_expired(2_000).await?;

        assert_eq!(provider.segment_size(first_path).await?, 0);
        assert_eq!(flat_file.commit_log_append_offset(), 12);
        Ok(())
    }

    #[tokio::test]
    async fn cleanup_keeps_expired_commit_log_referenced_by_retained_consume_queue() -> Result<(), RocketMQError> {
        let temp_dir = tempfile::tempdir().map_err(|err| RocketMQError::Internal(err.to_string()))?;
        let config = Arc::new(TieredStoreConfig {
            store_path_root_dir: temp_dir.path().to_path_buf(),
            backend_provider: "memory".to_owned(),
            commit_log_segment_size: 8,
            consume_queue_segment_size: CONSUME_QUEUE_UNIT_SIZE as u64 * 3,
            file_reserved_time: Duration::from_millis(1),
            ..TieredStoreConfig::default()
        });
        let metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        let provider = MemoryProvider::default();
        let flat_file = TieredFlatFile::new("TopicA".to_owned(), 0, config, metadata_store, provider.clone());

        for (queue_offset, body, timestamp) in [
            (0, Bytes::from_static(b"abcd"), 10),
            (1, Bytes::from_static(b"efgh"), 11),
            (2, Bytes::from_static(b"ijkl"), 1_000),
        ] {
            let commit_log_offset = flat_file.append_commit_log(body.clone(), timestamp).await?;
            flat_file
                .append_consume_queue(
                    queue_offset,
                    ConsumeQueueUnit {
                        commit_log_offset: commit_log_offset as i64,
                        size: body.len() as i32,
                        tags_code: 1,
                    },
                    timestamp,
                )
                .await?;
        }
        flat_file.commit().await?;

        let first_path = segment_path("TopicA", 0, FileSegmentType::CommitLog, 0);
        flat_file.cleanup_expired(2_000).await?;

        assert_eq!(provider.segment_size(first_path).await?, 8);
        assert_eq!(
            flat_file.read_message_by_queue_offset(0).await?,
            Some(Bytes::from_static(b"abcd"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn cleanup_deletes_commit_log_after_referencing_consume_queue_expires() -> Result<(), RocketMQError> {
        let temp_dir = tempfile::tempdir().map_err(|err| RocketMQError::Internal(err.to_string()))?;
        let config = test_config(temp_dir.path().to_path_buf());
        let metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        let provider = MemoryProvider::default();
        let flat_file = TieredFlatFile::new("TopicA".to_owned(), 0, config, metadata_store.clone(), provider.clone());

        for (queue_offset, body, timestamp) in [
            (0, Bytes::from_static(b"abcd"), 10),
            (1, Bytes::from_static(b"efgh"), 11),
            (2, Bytes::from_static(b"ijkl"), 1_000),
        ] {
            let commit_log_offset = flat_file.append_commit_log(body.clone(), timestamp).await?;
            flat_file
                .append_consume_queue(
                    queue_offset,
                    ConsumeQueueUnit {
                        commit_log_offset: commit_log_offset as i64,
                        size: body.len() as i32,
                        tags_code: 1,
                    },
                    timestamp,
                )
                .await?;
        }
        flat_file.commit().await?;

        let first_path = segment_path("TopicA", 0, FileSegmentType::CommitLog, 0);
        flat_file.cleanup_expired(2_000).await?;

        assert_eq!(provider.segment_size(first_path).await?, 0);
        assert_eq!(flat_file.consume_queue_min_offset(), 2);
        let queue_metadata = metadata_store
            .get_queue("TopicA", 0)
            .await?
            .ok_or_else(|| RocketMQError::Internal("missing topic queue metadata".to_owned()))?;
        assert_eq!(queue_metadata.min_offset, 2);
        assert_eq!(queue_metadata.max_offset, 3);
        Ok(())
    }
}
