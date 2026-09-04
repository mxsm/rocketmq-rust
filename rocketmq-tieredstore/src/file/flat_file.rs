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

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use parking_lot::Mutex;
use rocketmq_model::boundary_type::BoundaryType;
use rocketmq_observability::metrics::tiered_store::TieredStoreMetrics;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;

use crate::config::TieredStoreConfig;
use crate::error;
use crate::fetcher::read_ahead_cache::block_size;
use crate::fetcher::read_ahead_cache::ReadAheadCache;
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

    pub fn decode(operation: StoreOperation, bytes: Bytes) -> Result<Self, StoreError> {
        Self::decode_slice(operation, bytes.as_ref())
    }

    /// Decodes one borrowed ConsumeQueue unit without allocating a temporary buffer.
    pub fn decode_slice(operation: StoreOperation, bytes: &[u8]) -> Result<Self, StoreError> {
        if bytes.len() != CONSUME_QUEUE_UNIT_SIZE {
            return Err(error::request_invalid(operation));
        }
        let mut commit_log_offset = [0_u8; 8];
        commit_log_offset.copy_from_slice(&bytes[..8]);
        let mut size = [0_u8; 4];
        size.copy_from_slice(&bytes[8..12]);
        let mut tags_code = [0_u8; 8];
        tags_code.copy_from_slice(&bytes[12..20]);
        Ok(Self {
            commit_log_offset: i64::from_be_bytes(commit_log_offset),
            size: i32::from_be_bytes(size),
            tags_code: i64::from_be_bytes(tags_code),
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
    metrics: Arc<TieredStoreMetrics>,
    read_ahead_cache: Arc<ReadAheadCache>,
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
        let read_ahead_cache = Arc::new(ReadAheadCache::new(
            config.read_ahead_cache_enable,
            config.read_ahead_cache_max_bytes,
            config.read_ahead_cache_expire,
        ));
        Self::new_with_read_ahead_cache(
            topic,
            queue_id,
            config,
            metadata_store,
            provider,
            read_ahead_cache,
            Arc::new(TieredStoreMetrics::default()),
        )
    }

    pub(crate) fn new_with_read_ahead_cache(
        topic: String,
        queue_id: i32,
        config: Arc<TieredStoreConfig>,
        metadata_store: Arc<JsonMetadataStore>,
        provider: P,
        read_ahead_cache: Arc<ReadAheadCache>,
        metrics: Arc<TieredStoreMetrics>,
    ) -> Self {
        Self {
            topic,
            queue_id,
            config,
            metadata_store,
            provider,
            metrics,
            read_ahead_cache,
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

    pub async fn append_commit_log(&self, message: Bytes, store_timestamp: i64) -> Result<u64, StoreError> {
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
    ) -> Result<(), StoreError> {
        if queue_offset < 0 {
            return Err(error::request_invalid(StoreOperation::AppendDerived));
        }
        let absolute_offset = (queue_offset as u64).saturating_mul(CONSUME_QUEUE_UNIT_SIZE as u64);
        let expected_offset = self.consume_queue_append_byte_offset();
        if !self.consume_queue_segments.lock().is_empty() && absolute_offset != expected_offset {
            return Err(error::request_invalid(StoreOperation::AppendDerived));
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

    pub async fn commit(&self) -> Result<(), StoreError> {
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

    pub async fn recover(&self) -> Result<(), StoreError> {
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
            let max_size = self.max_segment_size(StoreOperation::Load, metadata.segment_type)?;
            let real_size = self
                .provider
                .segment_size(StoreOperation::Load, metadata.path.clone())
                .await?;
            if metadata.size != real_size {
                metadata.size = real_size.min(max_size);
                self.metadata_store.upsert_file_segment(metadata.clone()).await?;
            }
            let segment = Arc::new(TieredFileSegment::new_with_metrics(
                metadata.path.clone(),
                metadata.segment_type,
                metadata.base_offset,
                max_size,
                metadata,
                self.provider.clone(),
                self.metrics.clone(),
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

    pub async fn cleanup_expired(&self, now_millis: i64) -> Result<(), StoreError> {
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

    pub async fn read_consume_queue_unit(&self, queue_offset: i64) -> Result<Option<ConsumeQueueUnit>, StoreError> {
        self.read_consume_queue_unit_with_operation(StoreOperation::Read, queue_offset)
            .await
    }

    pub(crate) async fn read_consume_queue_unit_with_operation(
        &self,
        operation: StoreOperation,
        queue_offset: i64,
    ) -> Result<Option<ConsumeQueueUnit>, StoreError> {
        if queue_offset < 0 {
            return Err(error::request_invalid(operation));
        }
        let byte_offset = (queue_offset as u64).saturating_mul(CONSUME_QUEUE_UNIT_SIZE as u64);
        let Some(bytes) = self
            .read_from_segments(
                operation,
                FileSegmentType::ConsumeQueue,
                byte_offset,
                CONSUME_QUEUE_UNIT_SIZE,
            )
            .await?
        else {
            return Ok(None);
        };
        ConsumeQueueUnit::decode_slice(operation, bytes.as_ref()).map(Some)
    }

    pub async fn read_message_by_queue_offset(&self, queue_offset: i64) -> Result<Option<Bytes>, StoreError> {
        self.read_message_by_queue_offset_with_operation(StoreOperation::Read, queue_offset)
            .await
    }

    async fn read_message_by_queue_offset_with_operation(
        &self,
        operation: StoreOperation,
        queue_offset: i64,
    ) -> Result<Option<Bytes>, StoreError> {
        let Some(unit) = self
            .read_consume_queue_unit_with_operation(operation, queue_offset)
            .await?
        else {
            return Ok(None);
        };
        if unit.commit_log_offset < 0 || unit.size <= 0 {
            return Ok(None);
        }
        self.read_from_segments(
            operation,
            FileSegmentType::CommitLog,
            unit.commit_log_offset as u64,
            unit.size as usize,
        )
        .await
    }

    pub async fn read_message_store_timestamp(&self, queue_offset: i64) -> Result<Option<i64>, StoreError> {
        self.read_message_store_timestamp_with_operation(StoreOperation::QueryOffset, queue_offset)
            .await
    }

    async fn read_message_store_timestamp_with_operation(
        &self,
        operation: StoreOperation,
        queue_offset: i64,
    ) -> Result<Option<i64>, StoreError> {
        let Some(message) = self
            .read_message_by_queue_offset_with_operation(operation, queue_offset)
            .await?
        else {
            return Ok(None);
        };
        Ok(decode_message_store_timestamp(&message))
    }

    pub async fn read_commit_log(&self, offset: u64, length: usize) -> Result<Option<Bytes>, StoreError> {
        self.read_from_segments(StoreOperation::Read, FileSegmentType::CommitLog, offset, length)
            .await
    }

    pub async fn queue_offset_by_time(&self, timestamp_millis: i64) -> Result<i64, StoreError> {
        self.queue_offset_by_time_with_boundary(timestamp_millis, BoundaryType::Lower)
            .await
    }

    pub async fn queue_offset_by_time_with_boundary(
        &self,
        timestamp_millis: i64,
        boundary_type: BoundaryType,
    ) -> Result<i64, StoreError> {
        let cq_min = self.consume_queue_min_offset();
        let cq_commit = self.consume_queue_commit_offset();
        let cq_max = cq_commit.saturating_sub(1);
        if cq_max == -1 || cq_max < cq_min {
            return Ok(cq_min);
        }

        let Some(max_store_time) = self
            .read_message_store_timestamp_with_operation(StoreOperation::QueryOffset, cq_max)
            .await?
        else {
            return Ok(cq_min);
        };
        if max_store_time < timestamp_millis {
            return Ok(match boundary_type {
                BoundaryType::Lower => cq_commit,
                BoundaryType::Upper => cq_max,
            });
        }

        let Some(min_store_time) = self
            .read_message_store_timestamp_with_operation(StoreOperation::QueryOffset, cq_min)
            .await?
        else {
            return Ok(cq_min);
        };
        if min_store_time > timestamp_millis {
            return Ok(cq_min);
        }

        let (mut low, mut high) = self.timestamp_search_range(timestamp_millis, cq_min, cq_max);
        match boundary_type {
            BoundaryType::Lower => {
                while low < high {
                    let middle = low.saturating_add((high - low) / 2);
                    let Some(store_time) = self
                        .read_message_store_timestamp_with_operation(StoreOperation::QueryOffset, middle)
                        .await?
                    else {
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
            BoundaryType::Upper => {
                let mut result = cq_min;
                while low <= high {
                    let middle = low.saturating_add((high - low) / 2);
                    let Some(store_time) = self
                        .read_message_store_timestamp_with_operation(StoreOperation::QueryOffset, middle)
                        .await?
                    else {
                        return Ok(result);
                    };
                    if store_time <= timestamp_millis {
                        result = middle;
                        low = middle.saturating_add(1);
                    } else if middle == 0 {
                        break;
                    } else {
                        high = middle.saturating_sub(1);
                    }
                }
                Ok(result)
            }
        }
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
    ) -> Result<Arc<TieredFileSegment<P>>, StoreError> {
        let existing = match segment_type {
            FileSegmentType::CommitLog => self.commit_log_segments.lock().last().cloned(),
            FileSegmentType::ConsumeQueue => self.consume_queue_segments.lock().last().cloned(),
            FileSegmentType::Index => {
                return Err(error::unsupported(StoreOperation::AppendDerived));
            }
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
                .create_segment(
                    StoreOperation::AppendDerived,
                    path,
                    segment_type,
                    absolute_offset,
                    max_size,
                )
                .await?
                .with_metrics(self.metrics.clone()),
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

    fn max_segment_size(&self, operation: StoreOperation, segment_type: FileSegmentType) -> Result<u64, StoreError> {
        match segment_type {
            FileSegmentType::CommitLog => Ok(self.config.commit_log_segment_size),
            FileSegmentType::ConsumeQueue => Ok(self.config.consume_queue_segment_size),
            FileSegmentType::Index => Err(error::unsupported(operation)),
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

    async fn delete_segment(&self, segment: Arc<TieredFileSegment<P>>) -> Result<(), StoreError> {
        let metadata = segment.metadata();
        // The provider object remains readable until its deletion succeeds. Only
        // then publish the durable metadata tombstone and remove the segment
        // from the live view.
        self.provider
            .delete(StoreOperation::AppendDerived, metadata.path.clone())
            .await?;
        self.metadata_store
            .mark_file_segment_deleted(&metadata.path, metadata.base_offset)
            .await?;
        segment.mark_deleted();
        self.read_ahead_cache.invalidate_path(&metadata.path);
        self.remove_segment(metadata.segment_type, metadata.base_offset, &metadata.path);
        Ok(())
    }

    async fn refresh_queue_metadata(&self, previous_commit_offset: i64) -> Result<(), StoreError> {
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

    async fn first_retained_commit_log_offset(&self) -> Result<Option<u64>, StoreError> {
        let consume_queue_segments = self.consume_queue_segments.lock().clone();
        let mut first_offset: Option<u64> = None;
        for segment in consume_queue_segments {
            if segment.commit_position() < CONSUME_QUEUE_UNIT_SIZE as u64 {
                continue;
            }
            let bytes = self
                .read_ahead_cache
                .read(
                    StoreOperation::AppendDerived,
                    &segment,
                    0..CONSUME_QUEUE_UNIT_SIZE as u64,
                    block_size(segment.segment_type()),
                )
                .await?;
            let unit = ConsumeQueueUnit::decode_slice(StoreOperation::AppendDerived, bytes.as_ref())?;
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
        operation: StoreOperation,
        segment_type: FileSegmentType,
        absolute_offset: u64,
        length: usize,
    ) -> Result<Option<Bytes>, StoreError> {
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
        let bytes = self
            .read_ahead_cache
            .read(
                operation,
                &segment,
                relative_offset..relative_offset.saturating_add(length as u64),
                block_size(segment_type),
            )
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
    use bytes::BytesMut;
    use rocketmq_store_api::StoreError;

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

    fn message_with_store_timestamp(store_timestamp: i64, body: &[u8]) -> Bytes {
        let mut bytes = BytesMut::zeroed(MESSAGE_STORE_TIMESTAMP_POSITION + std::mem::size_of::<i64>());
        bytes[MESSAGE_STORE_TIMESTAMP_POSITION..MESSAGE_STORE_TIMESTAMP_POSITION + std::mem::size_of::<i64>()]
            .copy_from_slice(&store_timestamp.to_be_bytes());
        bytes.extend_from_slice(body);
        bytes.freeze()
    }

    #[test]
    fn consume_queue_unit_supports_owned_and_borrowed_decode() -> Result<(), StoreError> {
        let unit = ConsumeQueueUnit {
            commit_log_offset: 42,
            size: 128,
            tags_code: 7,
        };
        let encoded = unit.encode();

        assert_eq!(ConsumeQueueUnit::decode(StoreOperation::Read, encoded.clone())?, unit);
        assert_eq!(
            ConsumeQueueUnit::decode_slice(StoreOperation::Read, encoded.as_ref())?,
            unit
        );
        Ok(())
    }

    #[tokio::test]
    async fn consume_queue_reads_retain_query_and_derived_owners() -> Result<(), StoreError> {
        let temp_dir = tempfile::tempdir().expect("create consume queue owner fixture");
        let config = test_config(temp_dir.path().to_path_buf());
        let metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        let flat_file = TieredFlatFile::new(
            "TopicA".to_owned(),
            0,
            config,
            metadata_store,
            MemoryProvider::default(),
        );

        let query_error = flat_file
            .read_message_store_timestamp(-1)
            .await
            .expect_err("a negative timestamp lookup must retain its query owner");
        assert_eq!(query_error.descriptor(), &rocketmq_error::STORAGE_REQUEST_INVALID);
        assert_eq!(query_error.operation(), StoreOperation::QueryOffset);
        assert_eq!(query_error.component(), rocketmq_store_api::StoreComponent::TieredStore);

        let derived_error = flat_file
            .read_consume_queue_unit_with_operation(StoreOperation::AppendDerived, -1)
            .await
            .expect_err("a derived validation read must retain its write owner");
        assert_eq!(derived_error.descriptor(), &rocketmq_error::STORAGE_REQUEST_INVALID);
        assert_eq!(derived_error.operation(), StoreOperation::AppendDerived);
        assert_eq!(
            derived_error.component(),
            rocketmq_store_api::StoreComponent::TieredStore
        );
        Ok(())
    }

    #[tokio::test]
    async fn consume_queue_offsets_are_contiguous() -> Result<(), StoreError> {
        let temp_dir = tempfile::tempdir().map_err(|source| {
            crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source,
            )
        })?;
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
    async fn append_rolls_commit_log_and_consume_queue_segments() -> Result<(), StoreError> {
        let temp_dir = tempfile::tempdir().map_err(|source| {
            crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source,
            )
        })?;
        let config = test_config(temp_dir.path().to_path_buf());
        let metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        let provider = MemoryProvider::default();
        let flat_file = TieredFlatFile::new("TopicA".to_owned(), 0, config, metadata_store, provider.clone());

        for (queue_offset, body, timestamp) in [
            (0, Bytes::from_static(b"aaaa"), 100),
            (1, Bytes::from_static(b"bbbb"), 101),
            (2, Bytes::from_static(b"cccc"), 102),
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

        assert_eq!(
            provider
                .segment_size(
                    StoreOperation::Read,
                    segment_path("TopicA", 0, FileSegmentType::CommitLog, 0)
                )
                .await?,
            8
        );
        assert_eq!(
            provider
                .segment_size(
                    StoreOperation::Read,
                    segment_path("TopicA", 0, FileSegmentType::CommitLog, 8)
                )
                .await?,
            4
        );
        assert_eq!(
            provider
                .segment_size(
                    StoreOperation::Read,
                    segment_path("TopicA", 0, FileSegmentType::ConsumeQueue, 0)
                )
                .await?,
            (CONSUME_QUEUE_UNIT_SIZE * 2) as u64
        );
        assert_eq!(
            provider
                .segment_size(
                    StoreOperation::Read,
                    segment_path("TopicA", 0, FileSegmentType::ConsumeQueue, 40)
                )
                .await?,
            CONSUME_QUEUE_UNIT_SIZE as u64
        );
        assert_eq!(
            flat_file.read_message_by_queue_offset(0).await?,
            Some(Bytes::from_static(b"aaaa"))
        );
        assert_eq!(
            flat_file.read_message_by_queue_offset(2).await?,
            Some(Bytes::from_static(b"cccc"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn queue_offset_by_time_respects_lower_and_upper_boundaries() -> Result<(), StoreError> {
        let temp_dir = tempfile::tempdir().map_err(|source| {
            crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source,
            )
        })?;
        let config = Arc::new(TieredStoreConfig {
            store_path_root_dir: temp_dir.path().to_path_buf(),
            backend_provider: "memory".to_owned(),
            commit_log_segment_size: 512,
            consume_queue_segment_size: CONSUME_QUEUE_UNIT_SIZE as u64 * 4,
            ..TieredStoreConfig::default()
        });
        let metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        let flat_file = TieredFlatFile::new(
            "TopicA".to_owned(),
            0,
            config,
            metadata_store,
            MemoryProvider::default(),
        );

        for (queue_offset, timestamp) in [(0, 100), (1, 200), (2, 300)] {
            let body = message_with_store_timestamp(timestamp, format!("body-{queue_offset}").as_bytes());
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

        assert_eq!(flat_file.queue_offset_by_time(50).await?, 0);
        assert_eq!(flat_file.queue_offset_by_time(150).await?, 1);
        assert_eq!(flat_file.queue_offset_by_time(301).await?, 3);
        assert_eq!(
            flat_file
                .queue_offset_by_time_with_boundary(150, BoundaryType::Upper)
                .await?,
            0
        );
        assert_eq!(
            flat_file
                .queue_offset_by_time_with_boundary(300, BoundaryType::Upper)
                .await?,
            2
        );
        assert_eq!(
            flat_file
                .queue_offset_by_time_with_boundary(301, BoundaryType::Upper)
                .await?,
            2
        );
        Ok(())
    }

    #[tokio::test]
    async fn recovers_segments_from_metadata_and_provider_size() -> Result<(), StoreError> {
        let temp_dir = tempfile::tempdir().map_err(|source| {
            crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source,
            )
        })?;
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
    async fn cleanup_deletes_expired_sealed_segments() -> Result<(), StoreError> {
        let temp_dir = tempfile::tempdir().map_err(|source| {
            crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source,
            )
        })?;
        let config = test_config(temp_dir.path().to_path_buf());
        let metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        let provider = MemoryProvider::default();
        let flat_file = TieredFlatFile::new("TopicA".to_owned(), 0, config, metadata_store, provider.clone());

        flat_file.append_commit_log(Bytes::from_static(b"abcd"), 10).await?;
        flat_file.append_commit_log(Bytes::from_static(b"efgh"), 11).await?;
        flat_file.append_commit_log(Bytes::from_static(b"ijkl"), 1_000).await?;
        flat_file.commit().await?;

        let first_path = segment_path("TopicA", 0, FileSegmentType::CommitLog, 0);
        assert_eq!(
            provider.segment_size(StoreOperation::Read, first_path.clone()).await?,
            8
        );

        flat_file.cleanup_expired(2_000).await?;

        assert_eq!(provider.segment_size(StoreOperation::Read, first_path).await?, 0);
        assert_eq!(flat_file.commit_log_append_offset(), 12);
        Ok(())
    }

    #[tokio::test]
    async fn cleanup_keeps_expired_commit_log_referenced_by_retained_consume_queue() -> Result<(), StoreError> {
        let temp_dir = tempfile::tempdir().map_err(|source| {
            crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source,
            )
        })?;
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

        assert_eq!(provider.segment_size(StoreOperation::Read, first_path).await?, 8);
        assert_eq!(
            flat_file.read_message_by_queue_offset(0).await?,
            Some(Bytes::from_static(b"abcd"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn cleanup_deletes_commit_log_after_referencing_consume_queue_expires() -> Result<(), StoreError> {
        let temp_dir = tempfile::tempdir().map_err(|source| {
            crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source,
            )
        })?;
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

        assert_eq!(provider.segment_size(StoreOperation::Read, first_path).await?, 0);
        assert_eq!(flat_file.consume_queue_min_offset(), 2);
        let queue_metadata = metadata_store
            .get_queue("TopicA", 0)
            .await?
            .ok_or_else(|| crate::error::internal_failure(rocketmq_store_api::StoreOperation::Load))?;
        assert_eq!(queue_metadata.min_offset, 2);
        assert_eq!(queue_metadata.max_offset, 3);
        Ok(())
    }
}
