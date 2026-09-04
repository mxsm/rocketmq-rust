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
use bytes::Bytes;
use rocketmq_observability::metrics::tiered_store::TieredStoreMetrics;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;

use crate::error;
use crate::file::index_generation::encode_generation_metadata;
use crate::file::index_generation::IndexGenerationLease;
use crate::file::index_generation::IndexGenerationManager;
use crate::file::index_generation::IndexGenerationMetadata;
use crate::provider::TieredStoreProvider;

mod codec;

use self::codec::decode_header;
use self::codec::decode_record_header;
use self::codec::decode_record_payload;
use self::codec::decode_segment_entries;
use self::codec::encode_header;
use self::codec::encode_record;
use self::codec::encode_u64;
use self::codec::generation_metadata;
use self::codec::item_base_position;
use self::codec::java_positive_hash;
use self::codec::normalize_entries;
use self::codec::slot_position;
use self::codec::IndexRecord;
use self::codec::IndexSegmentHeader;
use self::codec::HEADER_SIZE;
use self::codec::ITEM_HEADER_SIZE;
use self::codec::SLOT_SIZE;

#[cfg(test)]
use self::codec::IndexRecordHeader;

pub const DEFAULT_INDEX_FILE_PATH: &str = "index/tiered_index_file";

const DEFAULT_HASH_SLOT_COUNT: usize = 1024;
const DEFAULT_INDEX_ITEM_COUNT: usize = 4096;

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TieredIndexEntry {
    pub topic: String,
    pub key: String,
    pub queue_id: i32,
    pub queue_offset: i64,
    pub commit_log_offset: u64,
    pub message_size: usize,
    pub store_timestamp: i64,
}

impl TieredIndexEntry {
    #[inline]
    pub fn in_time_range(&self, begin: i64, end: i64) -> bool {
        self.store_timestamp >= begin && self.store_timestamp <= end
    }
}

#[derive(Clone)]
pub struct IndexFileSegment<P>
where
    P: TieredStoreProvider,
{
    directory: String,
    provider: P,
    hash_slot_count: usize,
    max_index_items: usize,
    generation_manager: Arc<IndexGenerationManager<P>>,
    generation_lock: Arc<tokio::sync::RwLock<()>>,
    validated_generation: Arc<parking_lot::RwLock<Option<u64>>>,
    metrics: Arc<TieredStoreMetrics>,
    raw: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppendOutcome {
    Appended,
    Full,
}

impl<P> IndexFileSegment<P>
where
    P: TieredStoreProvider,
{
    pub fn new(directory: String, provider: P) -> Self {
        Self::with_limits(directory, provider, DEFAULT_HASH_SLOT_COUNT, DEFAULT_INDEX_ITEM_COUNT)
    }

    pub fn with_limits(directory: String, provider: P, hash_slot_count: usize, max_index_items: usize) -> Self {
        Self::with_limits_and_metrics(
            directory,
            provider,
            hash_slot_count,
            max_index_items,
            Arc::new(TieredStoreMetrics::default()),
        )
    }

    pub fn with_limits_and_metrics(
        directory: String,
        provider: P,
        hash_slot_count: usize,
        max_index_items: usize,
        metrics: Arc<TieredStoreMetrics>,
    ) -> Self {
        let generation_manager = Arc::new(IndexGenerationManager::new(directory.clone(), provider.clone()));
        Self {
            directory,
            provider,
            hash_slot_count: hash_slot_count.max(1),
            max_index_items: max_index_items.max(1),
            generation_manager,
            generation_lock: Arc::new(tokio::sync::RwLock::new(())),
            validated_generation: Arc::new(parking_lot::RwLock::new(None)),
            metrics,
            raw: false,
        }
    }

    pub fn default_path() -> &'static str {
        DEFAULT_INDEX_FILE_PATH
    }

    pub fn path(&self) -> &str {
        &self.directory
    }

    async fn provider_read(
        &self,
        operation: StoreOperation,
        path: String,
        position: u64,
        length: usize,
    ) -> Result<Bytes, StoreError> {
        let started = std::time::Instant::now();
        let result = self.provider.read(operation, path.clone(), position, length).await;
        self.metrics.record_provider_read(
            &path,
            result.as_ref().map(|bytes| bytes.len() as u64).unwrap_or(0),
            result.is_ok(),
            started.elapsed().as_millis() as u64,
        );
        result
    }

    async fn provider_write(
        &self,
        operation: StoreOperation,
        path: String,
        position: u64,
        data: Bytes,
    ) -> Result<usize, StoreError> {
        let started = std::time::Instant::now();
        let result = self.provider.write(operation, path.clone(), position, data).await;
        self.metrics.record_provider_write(
            &path,
            result.as_ref().map(|written| *written as u64).unwrap_or(0),
            result.is_ok(),
            started.elapsed().as_millis() as u64,
        );
        result
    }

    pub async fn append_entry(&self, entry: &TieredIndexEntry) -> Result<(), StoreError> {
        if self.raw {
            return self.append_entry_raw(entry).await;
        }
        let _guard = self.generation_lock.write().await;
        let current = self.validated_current(StoreOperation::AppendDerived).await?;
        let target = if current.id() == 0 {
            self.raw_segment(current.path().to_owned())
        } else {
            self.raw_segment(self.delta_path())
        };
        target.append_entry_raw(entry).await
    }

    async fn append_entry_raw(&self, entry: &TieredIndexEntry) -> Result<(), StoreError> {
        let mut segment_timestamps = self.load_manifest(StoreOperation::AppendDerived).await?;
        if segment_timestamps.is_empty() {
            let timestamp = entry.store_timestamp.max(0);
            segment_timestamps.push(timestamp);
            self.persist_manifest(&segment_timestamps).await?;
        }

        for _ in 0..2 {
            let Some(timestamp) = segment_timestamps.last().copied() else {
                return Err(error::state_corrupted(StoreOperation::AppendDerived));
            };
            let path = self.segment_path(timestamp);
            match self.try_append_to_segment(&path, timestamp, entry).await? {
                AppendOutcome::Appended => return Ok(()),
                AppendOutcome::Full => {
                    let next_timestamp = entry.store_timestamp.max(timestamp.saturating_add(1)).max(0);
                    segment_timestamps.push(next_timestamp);
                    self.persist_manifest(&segment_timestamps).await?;
                }
            }
        }

        Err(error::contract_error(
            &rocketmq_error::STORAGE_WRITE_FAILED,
            StoreOperation::AppendDerived,
        ))
    }

    pub async fn load_entries(&self) -> Result<Vec<TieredIndexEntry>, StoreError> {
        if self.raw {
            return self.load_entries_raw(StoreOperation::Read).await;
        }
        let _guard = self.generation_lock.read().await;
        let current = self.validated_current(StoreOperation::Read).await?;
        let mut entries = self
            .raw_segment(current.path().to_owned())
            .load_entries_raw(StoreOperation::Read)
            .await?;
        if current.id() > 0 {
            entries.extend(
                self.raw_segment(self.delta_path())
                    .load_entries_raw(StoreOperation::Read)
                    .await?,
            );
        }
        normalize_entries(&mut entries);
        drop(current);
        self.generation_manager.cleanup_retired().await?;
        Ok(entries)
    }

    async fn load_entries_raw(&self, operation: StoreOperation) -> Result<Vec<TieredIndexEntry>, StoreError> {
        let mut entries = Vec::new();
        for timestamp in self.load_manifest(operation).await? {
            let path = self.segment_path(timestamp);
            let size = self.provider.segment_size(operation, path.clone()).await?;
            if size == 0 {
                continue;
            }
            let bytes = self.provider_read(operation, path, 0, size as usize).await?;
            entries.extend(
                decode_segment_entries(operation, &bytes)?
                    .into_iter()
                    .map(|record| record.entry),
            );
        }
        entries.sort_by_key(|entry| (entry.store_timestamp, entry.queue_id, entry.queue_offset));
        entries.dedup();
        Ok(entries)
    }

    pub async fn query_entries(
        &self,
        topic: &str,
        key: &str,
        max_count: usize,
        begin_time: i64,
        end_time: i64,
    ) -> Result<Vec<TieredIndexEntry>, StoreError> {
        if self.raw {
            return self
                .query_entries_raw(topic, key, max_count, begin_time, end_time)
                .await;
        }
        if max_count == 0 || topic.is_empty() || key.is_empty() || begin_time > end_time {
            return Ok(Vec::new());
        }

        let _guard = self.generation_lock.read().await;
        let current = self.validated_current(StoreOperation::Read).await?;
        let mut result = self
            .raw_segment(current.path().to_owned())
            .query_entries_raw(topic, key, max_count, begin_time, end_time)
            .await?;
        if current.id() > 0 && result.len() < max_count {
            result.extend(
                self.raw_segment(self.delta_path())
                    .query_entries_raw(topic, key, max_count - result.len(), begin_time, end_time)
                    .await?,
            );
        }
        normalize_entries(&mut result);
        result.truncate(max_count);
        drop(current);
        self.generation_manager.cleanup_retired().await?;
        Ok(result)
    }

    async fn query_entries_raw(
        &self,
        topic: &str,
        key: &str,
        max_count: usize,
        begin_time: i64,
        end_time: i64,
    ) -> Result<Vec<TieredIndexEntry>, StoreError> {
        if max_count == 0 || topic.is_empty() || key.is_empty() || begin_time > end_time {
            return Ok(Vec::new());
        }

        let hash_code = java_positive_hash(&format!("{topic}#{key}"));
        let mut result = Vec::new();
        let mut segments = self.load_manifest(StoreOperation::Read).await?;
        segments.sort_unstable_by(|left, right| right.cmp(left));
        for timestamp in segments {
            if result.len() >= max_count {
                break;
            }
            let path = self.segment_path(timestamp);
            let size = self.provider.segment_size(StoreOperation::Read, path.clone()).await?;
            if size < HEADER_SIZE as u64 {
                continue;
            }
            let header = self.read_header(StoreOperation::Read, &path).await?;
            if header.begin_timestamp > end_time || header.end_timestamp < begin_time {
                continue;
            }
            let mut segment_entries = self
                .query_segment_entries(
                    &path,
                    &header,
                    topic,
                    key,
                    hash_code,
                    max_count - result.len(),
                    begin_time,
                    end_time,
                )
                .await?;
            result.append(&mut segment_entries);
        }
        result.sort_by_key(|entry| (entry.store_timestamp, entry.queue_id, entry.queue_offset));
        result.truncate(max_count);
        Ok(result)
    }

    pub async fn compact_entries(&self, entries: &[TieredIndexEntry]) -> Result<(), StoreError> {
        if self.raw {
            return self.replace_entries_raw(entries).await;
        }
        let _guard = self.generation_lock.write().await;
        let _current = self.validated_current(StoreOperation::AppendDerived).await?;
        let generation = self
            .generation_manager
            .next_generation(StoreOperation::AppendDerived)
            .await?;
        let temporary_path = self.generation_manager.temporary_path(generation);
        let generation_path = self.generation_manager.generation_path(generation);
        self.provider
            .delete_prefix(StoreOperation::AppendDerived, temporary_path.clone())
            .await?;

        let builder = self.raw_segment(temporary_path.clone());
        let mut normalized = entries.to_vec();
        normalize_entries(&mut normalized);
        for entry in &normalized {
            builder.append_entry_raw(entry).await?;
        }
        // The legacy index format stores timestamps as second deltas from the segment boundary.
        // Derive generation bounds and CRC from the persisted representation so validation does
        // not compare sub-second source timestamps with their compatible on-disk projection.
        let persisted_entries = builder.load_entries_raw(StoreOperation::AppendDerived).await?;
        let metadata = generation_metadata(generation, &persisted_entries);
        let metadata_path = self.generation_manager.metadata_path(&temporary_path);
        let encoded_metadata = encode_generation_metadata(metadata);
        let written = self
            .provider_write(
                StoreOperation::AppendDerived,
                metadata_path.clone(),
                0,
                encoded_metadata.clone(),
            )
            .await?;
        if written != encoded_metadata.len() {
            return Err(error::contract_error(
                &rocketmq_error::STORAGE_WRITE_FAILED,
                StoreOperation::AppendDerived,
            ));
        }
        self.sync_generation(&temporary_path).await?;
        self.validate_generation_path(StoreOperation::AppendDerived, &temporary_path, metadata)
            .await?;

        self.provider
            .rename(StoreOperation::AppendDerived, temporary_path, generation_path)
            .await?;
        self.generation_manager.publish(metadata).await?;
        *self.validated_generation.write() = Some(generation);
        self.provider
            .delete_prefix(StoreOperation::AppendDerived, self.delta_path())
            .await?;
        self.generation_manager.cleanup_retired().await
    }

    pub async fn segment_count(&self) -> Result<usize, StoreError> {
        if self.raw {
            return self.segment_count_raw().await;
        }
        let _guard = self.generation_lock.read().await;
        let current = self.validated_current(StoreOperation::QueryOffset).await?;
        let mut count = self.raw_segment(current.path().to_owned()).segment_count_raw().await?;
        if current.id() > 0 {
            count = count.saturating_add(self.raw_segment(self.delta_path()).segment_count_raw().await?);
        }
        Ok(count)
    }

    async fn segment_count_raw(&self) -> Result<usize, StoreError> {
        Ok(self.load_manifest(StoreOperation::QueryOffset).await?.len())
    }

    async fn replace_entries_raw(&self, entries: &[TieredIndexEntry]) -> Result<(), StoreError> {
        let old_segments = self.load_manifest(StoreOperation::AppendDerived).await?;
        for timestamp in old_segments {
            self.provider
                .delete(StoreOperation::AppendDerived, self.segment_path(timestamp))
                .await?;
        }
        self.provider
            .delete(StoreOperation::AppendDerived, self.manifest_path())
            .await?;
        let mut sorted = entries.to_vec();
        normalize_entries(&mut sorted);
        for entry in sorted {
            self.append_entry_raw(&entry).await?;
        }
        Ok(())
    }

    async fn try_append_to_segment(
        &self,
        path: &str,
        begin_timestamp: i64,
        entry: &TieredIndexEntry,
    ) -> Result<AppendOutcome, StoreError> {
        self.ensure_segment_initialized(path, begin_timestamp).await?;
        let header = self.read_header(StoreOperation::AppendDerived, path).await?;
        if header.item_count as usize >= header.max_index_items as usize {
            return Ok(AppendOutcome::Full);
        }

        let hash_code = java_positive_hash(&format!("{}#{}", entry.topic, entry.key));
        let slot_index = (hash_code as usize) % header.hash_slot_count as usize;
        let slot_position = slot_position(slot_index);
        let previous_offset = self
            .read_slot(StoreOperation::AppendDerived, path, slot_position)
            .await?;
        let append_position = self
            .provider
            .segment_size(StoreOperation::AppendDerived, path.to_owned())
            .await?
            .max(item_base_position(header.hash_slot_count as usize) as u64);

        let record = IndexRecord {
            entry: entry.clone(),
            hash_code,
            previous_offset,
        };
        let bytes = encode_record(StoreOperation::AppendDerived, &record, header.begin_timestamp)?;
        self.provider_write(StoreOperation::AppendDerived, path.to_owned(), append_position, bytes)
            .await?;
        self.provider_write(
            StoreOperation::AppendDerived,
            path.to_owned(),
            slot_position as u64,
            encode_u64(append_position),
        )
        .await?;

        let occupied_slot_count = if previous_offset == 0 {
            header.occupied_slot_count.saturating_add(1)
        } else {
            header.occupied_slot_count
        };
        let updated_header = IndexSegmentHeader {
            begin_timestamp: header.begin_timestamp,
            end_timestamp: header.end_timestamp.max(entry.store_timestamp),
            occupied_slot_count,
            item_count: header.item_count.saturating_add(1),
            hash_slot_count: header.hash_slot_count,
            max_index_items: header.max_index_items,
        };
        self.write_header(path, &updated_header).await?;
        Ok(AppendOutcome::Appended)
    }

    async fn ensure_segment_initialized(&self, path: &str, begin_timestamp: i64) -> Result<(), StoreError> {
        if self
            .provider
            .segment_size(StoreOperation::AppendDerived, path.to_owned())
            .await?
            >= HEADER_SIZE as u64
        {
            return Ok(());
        }
        let header = IndexSegmentHeader {
            begin_timestamp,
            end_timestamp: begin_timestamp,
            occupied_slot_count: 0,
            item_count: 0,
            hash_slot_count: self.hash_slot_count as u32,
            max_index_items: self.max_index_items as u32,
        };
        self.write_header(path, &header).await
    }

    async fn read_header(&self, operation: StoreOperation, path: &str) -> Result<IndexSegmentHeader, StoreError> {
        let bytes = self.provider_read(operation, path.to_owned(), 0, HEADER_SIZE).await?;
        decode_header(operation, &bytes)
    }

    async fn write_header(&self, path: &str, header: &IndexSegmentHeader) -> Result<(), StoreError> {
        self.provider_write(StoreOperation::AppendDerived, path.to_owned(), 0, encode_header(header))
            .await?;
        Ok(())
    }

    async fn read_slot(&self, operation: StoreOperation, path: &str, slot_position: usize) -> Result<u64, StoreError> {
        let bytes = self
            .provider_read(operation, path.to_owned(), slot_position as u64, SLOT_SIZE)
            .await?;
        if bytes.len() < SLOT_SIZE {
            return Ok(0);
        }
        let mut bytes = bytes;
        Ok(bytes.get_u64())
    }

    async fn query_segment_entries(
        &self,
        path: &str,
        header: &IndexSegmentHeader,
        topic: &str,
        key: &str,
        hash_code: u32,
        max_count: usize,
        begin_time: i64,
        end_time: i64,
    ) -> Result<Vec<TieredIndexEntry>, StoreError> {
        let slot_index = (hash_code as usize) % header.hash_slot_count as usize;
        let mut next_offset = self
            .read_slot(StoreOperation::Read, path, slot_position(slot_index))
            .await?;
        let mut result = Vec::new();
        let mut remaining = 512;
        while next_offset > 0 && remaining > 0 && result.len() < max_count {
            let Some(record) = self.read_record(path, next_offset, header.begin_timestamp).await? else {
                break;
            };
            next_offset = record.previous_offset;
            if record.hash_code == hash_code
                && record.entry.topic == topic
                && record.entry.key == key
                && record.entry.in_time_range(begin_time, end_time)
            {
                result.push(record.entry);
            }
            remaining -= 1;
        }
        Ok(result)
    }

    async fn read_record(
        &self,
        path: &str,
        position: u64,
        segment_begin_timestamp: i64,
    ) -> Result<Option<IndexRecord>, StoreError> {
        let header = self
            .provider_read(StoreOperation::Read, path.to_owned(), position, ITEM_HEADER_SIZE)
            .await?;
        if header.len() < ITEM_HEADER_SIZE {
            return Ok(None);
        }
        let decoded_header = decode_record_header(StoreOperation::Read, &header)?;
        let payload_len = decoded_header.topic_len.saturating_add(decoded_header.key_len);
        let payload = self
            .provider
            .read(
                StoreOperation::Read,
                path.to_owned(),
                position.saturating_add(ITEM_HEADER_SIZE as u64),
                payload_len,
            )
            .await?;
        if payload.len() < payload_len {
            return Ok(None);
        }
        decode_record_payload(StoreOperation::Read, decoded_header, &payload, segment_begin_timestamp).map(Some)
    }

    async fn load_manifest(&self, operation: StoreOperation) -> Result<Vec<i64>, StoreError> {
        let path = self.manifest_path();
        let size = self.provider.segment_size(operation, path.clone()).await?;
        if size == 0 {
            return Ok(Vec::new());
        }
        let bytes = self.provider_read(operation, path, 0, size as usize).await?;
        let text = std::str::from_utf8(&bytes)
            .map_err(|source| error::state_corrupted_source(StoreOperation::Load, source))?;
        let mut timestamps = Vec::new();
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let timestamp = line
                .trim()
                .parse::<i64>()
                .map_err(|source| error::state_corrupted_source(StoreOperation::Load, source))?;
            timestamps.push(timestamp);
        }
        timestamps.sort_unstable();
        timestamps.dedup();
        Ok(timestamps)
    }

    async fn persist_manifest(&self, timestamps: &[i64]) -> Result<(), StoreError> {
        let mut timestamps = timestamps.to_vec();
        timestamps.sort_unstable();
        timestamps.dedup();
        let mut data = String::new();
        for timestamp in timestamps {
            data.push_str(&timestamp.to_string());
            data.push('\n');
        }
        self.provider
            .delete(StoreOperation::AppendDerived, self.manifest_path())
            .await?;
        self.provider_write(
            StoreOperation::AppendDerived,
            self.manifest_path(),
            0,
            Bytes::from(data),
        )
        .await?;
        Ok(())
    }

    /// Atomically selects the previously validated generation.
    ///
    /// The current delta remains readable so records appended after the previous generation are not
    /// lost.
    pub async fn rollback_to_previous_generation(&self) -> Result<bool, StoreError> {
        if self.raw {
            return Ok(false);
        }
        let _guard = self.generation_lock.write().await;
        let Some(previous) = self.generation_manager.previous(StoreOperation::Load).await? else {
            return Ok(false);
        };
        self.validate_generation_lease(StoreOperation::Load, &previous).await?;
        if !self
            .generation_manager
            .rollback_to_previous(StoreOperation::Load)
            .await?
        {
            return Ok(false);
        }
        *self.validated_generation.write() = Some(previous.id());
        Ok(true)
    }

    pub async fn current_generation_id(&self) -> Result<u64, StoreError> {
        Ok(self.generation_manager.current(StoreOperation::QueryOffset).await?.id())
    }

    async fn validated_current(&self, operation: StoreOperation) -> Result<IndexGenerationLease, StoreError> {
        let current = self.generation_manager.current(operation).await?;
        if *self.validated_generation.read() == Some(current.id()) {
            return Ok(current);
        }
        match self.validate_generation_lease(operation, &current).await {
            Ok(()) => {
                *self.validated_generation.write() = Some(current.id());
                Ok(current)
            }
            Err(current_error) => {
                if current_error.descriptor() != &rocketmq_error::STORAGE_STATE_CORRUPTED {
                    return Err(current_error);
                }
                let Some(previous) = self.generation_manager.previous(operation).await? else {
                    return Err(current_error);
                };
                self.validate_generation_lease(operation, &previous).await?;
                if !self
                    .generation_manager
                    .rollback_to_validated_previous(operation)
                    .await?
                {
                    return Err(current_error);
                }
                *self.validated_generation.write() = Some(previous.id());
                Ok(previous)
            }
        }
    }

    async fn validate_generation_lease(
        &self,
        operation: StoreOperation,
        generation: &IndexGenerationLease,
    ) -> Result<(), StoreError> {
        let Some(metadata) = generation.metadata() else {
            self.raw_segment(generation.path().to_owned())
                .load_entries_raw(operation)
                .await?;
            return Ok(());
        };
        self.validate_generation_path(operation, generation.path(), metadata)
            .await
    }

    async fn validate_generation_path(
        &self,
        operation: StoreOperation,
        generation_path: &str,
        expected: IndexGenerationMetadata,
    ) -> Result<(), StoreError> {
        let entries = self
            .raw_segment(generation_path.to_owned())
            .load_entries_raw(operation)
            .await?;
        let actual = generation_metadata(expected.generation, &entries);
        if actual != expected {
            return Err(error::state_corrupted(operation));
        }
        Ok(())
    }

    async fn sync_generation(&self, generation_path: &str) -> Result<(), StoreError> {
        for path in self
            .provider
            .list(StoreOperation::Flush, generation_path.to_owned())
            .await?
        {
            self.provider.sync(StoreOperation::Flush, path).await?;
        }
        self.provider
            .sync(StoreOperation::Flush, generation_path.to_owned())
            .await
    }

    fn raw_segment(&self, directory: String) -> Self {
        Self {
            generation_manager: Arc::new(IndexGenerationManager::new(directory.clone(), self.provider.clone())),
            directory,
            provider: self.provider.clone(),
            hash_slot_count: self.hash_slot_count,
            max_index_items: self.max_index_items,
            generation_lock: Arc::new(tokio::sync::RwLock::new(())),
            validated_generation: Arc::new(parking_lot::RwLock::new(None)),
            metrics: self.metrics.clone(),
            raw: true,
        }
    }

    fn delta_path(&self) -> String {
        format!("{}/delta", self.directory)
    }

    fn manifest_path(&self) -> String {
        format!("{}/manifest", self.directory)
    }

    fn segment_path(&self, timestamp: i64) -> String {
        format!("{}/{timestamp}", self.directory)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU8;
    use std::sync::atomic::Ordering;

    use bytes::Bytes;
    use rocketmq_store_api::StoreError;

    use super::*;
    use crate::file::FileSegmentType;
    use crate::file::TieredFileSegment;
    use crate::metadata::FileSegmentMetadata;
    use crate::provider::MemoryProvider;

    #[derive(Clone, Copy, Debug)]
    #[repr(u8)]
    enum FaultStage {
        Build = 1,
        Sync = 2,
        Rename = 3,
        Current = 4,
        Cleanup = 5,
        Read = 6,
    }

    #[derive(Clone)]
    struct FaultProvider {
        inner: MemoryProvider,
        fault: Arc<AtomicU8>,
    }

    impl Default for FaultProvider {
        fn default() -> Self {
            Self {
                inner: MemoryProvider::default(),
                fault: Arc::new(AtomicU8::new(0)),
            }
        }
    }

    impl FaultProvider {
        fn set_fault(&self, stage: FaultStage) {
            self.fault.store(stage as u8, Ordering::Release);
        }

        fn take_fault(&self, stage: FaultStage) -> bool {
            self.fault
                .compare_exchange(stage as u8, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        }

        fn injected(operation: StoreOperation, _stage: FaultStage) -> StoreError {
            crate::error::write_failed(operation, std::io::Error::other("injected index failure"))
        }
    }

    impl TieredStoreProvider for FaultProvider {
        async fn create_segment(
            &self,
            _operation: StoreOperation,
            path: String,
            segment_type: FileSegmentType,
            base_offset: u64,
            max_size: u64,
        ) -> Result<TieredFileSegment<Self>, StoreError> {
            let metadata = FileSegmentMetadata::new(path.clone(), segment_type, base_offset);
            Ok(TieredFileSegment::new(
                path,
                segment_type,
                base_offset,
                max_size,
                metadata,
                self.clone(),
            ))
        }

        async fn segment_size(&self, operation: StoreOperation, path: String) -> Result<u64, StoreError> {
            self.inner.segment_size(operation, path).await
        }

        async fn read(
            &self,
            operation: StoreOperation,
            path: String,
            position: u64,
            length: usize,
        ) -> Result<Bytes, StoreError> {
            let generation_data = path.contains("/generations/gen-") && !path.ends_with("/GENERATION");
            if generation_data && self.take_fault(FaultStage::Read) {
                return Err(crate::error::source_error(
                    &rocketmq_error::STORAGE_OPERATION_TIMED_OUT,
                    operation,
                    std::io::Error::new(std::io::ErrorKind::TimedOut, "injected provider timeout"),
                ));
            }
            self.inner.read(operation, path, position, length).await
        }

        async fn write(
            &self,
            operation: StoreOperation,
            path: String,
            position: u64,
            data: Bytes,
        ) -> Result<usize, StoreError> {
            if path.contains(".tmp/") && self.take_fault(FaultStage::Build) {
                return Err(Self::injected(operation, FaultStage::Build));
            }
            self.inner.write(operation, path, position, data).await
        }

        async fn delete(&self, operation: StoreOperation, path: String) -> Result<(), StoreError> {
            self.inner.delete(operation, path).await
        }

        async fn sync(&self, operation: StoreOperation, path: String) -> Result<(), StoreError> {
            if path.contains(".tmp") && self.take_fault(FaultStage::Sync) {
                return Err(Self::injected(operation, FaultStage::Sync));
            }
            self.inner.sync(operation, path).await
        }

        async fn rename(
            &self,
            operation: StoreOperation,
            source: String,
            destination: String,
        ) -> Result<(), StoreError> {
            if source.contains(".tmp") && self.take_fault(FaultStage::Rename) {
                return Err(Self::injected(operation, FaultStage::Rename));
            }
            self.inner.rename(operation, source, destination).await
        }

        async fn list(&self, operation: StoreOperation, prefix: String) -> Result<Vec<String>, StoreError> {
            self.inner.list(operation, prefix).await
        }

        async fn delete_prefix(&self, operation: StoreOperation, prefix: String) -> Result<(), StoreError> {
            let retired_generation = prefix.contains("/generations/gen-") && !prefix.contains(".tmp");
            if retired_generation && self.take_fault(FaultStage::Cleanup) {
                return Err(Self::injected(operation, FaultStage::Cleanup));
            }
            self.inner.delete_prefix(operation, prefix).await
        }

        async fn atomic_write(&self, operation: StoreOperation, path: String, data: Bytes) -> Result<(), StoreError> {
            if path.ends_with("/CURRENT") && self.take_fault(FaultStage::Current) {
                return Err(Self::injected(operation, FaultStage::Current));
            }
            self.inner.atomic_write(operation, path, data).await
        }
    }

    fn entry(key: &str, timestamp: i64) -> TieredIndexEntry {
        TieredIndexEntry {
            topic: "TopicA".to_owned(),
            key: key.to_owned(),
            queue_id: 0,
            queue_offset: timestamp,
            commit_log_offset: timestamp as u64,
            message_size: 4,
            store_timestamp: timestamp,
        }
    }

    #[tokio::test]
    async fn index_file_appends_loads_and_compacts_entries() -> Result<(), StoreError> {
        let provider = MemoryProvider::default();
        let index_file = IndexFileSegment::with_limits(DEFAULT_INDEX_FILE_PATH.to_owned(), provider, 8, 4);

        index_file.append_entry(&entry("keyA", 100)).await?;
        index_file.append_entry(&entry("keyA", 200)).await?;
        assert_eq!(index_file.load_entries().await?.len(), 2);

        index_file.compact_entries(&[entry("keyA", 200)]).await?;
        let entries = index_file.load_entries().await?;
        assert_eq!(entries, vec![entry("keyA", 200)]);
        Ok(())
    }

    #[tokio::test]
    async fn index_file_rolls_to_multiple_segments_when_full() -> Result<(), StoreError> {
        let provider = MemoryProvider::default();
        let index_file = IndexFileSegment::with_limits(DEFAULT_INDEX_FILE_PATH.to_owned(), provider, 8, 2);

        index_file.append_entry(&entry("keyA", 1000)).await?;
        index_file.append_entry(&entry("keyB", 2000)).await?;
        index_file.append_entry(&entry("keyC", 3000)).await?;

        assert_eq!(index_file.segment_count().await?, 2);
        assert_eq!(index_file.load_entries().await?.len(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn index_file_queries_through_hash_slot_chain() -> Result<(), StoreError> {
        let provider = MemoryProvider::default();
        let index_file = IndexFileSegment::with_limits(DEFAULT_INDEX_FILE_PATH.to_owned(), provider, 2, 8);

        index_file.append_entry(&entry("keyA", 1000)).await?;
        index_file.append_entry(&entry("keyB", 2000)).await?;
        index_file.append_entry(&entry("keyA", 3000)).await?;

        let entries = index_file.query_entries("TopicA", "keyA", 1, 0, 5000).await?;

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, "keyA");
        assert_eq!(entries[0].store_timestamp, 3000);
        Ok(())
    }

    #[test]
    fn java_hash_matches_known_string_hash() {
        assert_eq!(java_positive_hash("TopicA#keyA"), 641_858_195);
    }

    #[tokio::test]
    async fn manifest_decode_preserves_utf8_and_parse_sources() -> Result<(), StoreError> {
        let provider = MemoryProvider::default();
        let index_file = IndexFileSegment::with_limits(DEFAULT_INDEX_FILE_PATH.to_owned(), provider.clone(), 8, 4);
        let path = format!("{DEFAULT_INDEX_FILE_PATH}/manifest");

        provider
            .write(
                StoreOperation::AppendDerived,
                path.clone(),
                0,
                Bytes::from_static(&[0xff]),
            )
            .await?;
        let utf8_error = index_file.segment_count().await.expect_err("invalid manifest UTF-8");
        assert_eq!(utf8_error.descriptor(), &rocketmq_error::STORAGE_STATE_CORRUPTED);
        assert_eq!(utf8_error.operation(), StoreOperation::Load);
        assert!(std::error::Error::source(&utf8_error)
            .and_then(|source| source.downcast_ref::<std::str::Utf8Error>())
            .is_some());

        provider.delete(StoreOperation::AppendDerived, path.clone()).await?;
        provider
            .write(
                StoreOperation::AppendDerived,
                path,
                0,
                Bytes::from_static(b"sensitive-manifest-number-canary\n"),
            )
            .await?;
        let parse_error = index_file.segment_count().await.expect_err("invalid manifest integer");
        assert_eq!(parse_error.descriptor(), &rocketmq_error::STORAGE_STATE_CORRUPTED);
        assert_eq!(parse_error.operation(), StoreOperation::Load);
        assert!(std::error::Error::source(&parse_error)
            .and_then(|source| source.downcast_ref::<std::num::ParseIntError>())
            .is_some());
        let rendered = format!("{parse_error} {parse_error:?}");
        assert!(!rendered.contains("sensitive-manifest-number-canary"));
        Ok(())
    }

    #[test]
    fn record_decode_preserves_topic_and_key_utf8_sources() {
        let header = IndexRecordHeader {
            hash_code: 0,
            queue_id: 0,
            queue_offset: 0,
            commit_log_offset: 0,
            message_size: 1,
            time_diff: 0,
            previous_offset: 0,
            topic_len: 1,
            key_len: 0,
        };
        let topic_error = decode_record_payload(StoreOperation::Read, header.clone(), &Bytes::from_static(&[0xff]), 0)
            .expect_err("invalid topic UTF-8");
        assert!(std::error::Error::source(&topic_error)
            .and_then(|source| source.downcast_ref::<std::str::Utf8Error>())
            .is_some());

        let header = IndexRecordHeader {
            topic_len: 1,
            key_len: 1,
            ..header
        };
        let key_error = decode_record_payload(StoreOperation::Read, header, &Bytes::from_static(&[b't', 0xff]), 0)
            .expect_err("invalid key UTF-8");
        assert!(std::error::Error::source(&key_error)
            .and_then(|source| source.downcast_ref::<std::str::Utf8Error>())
            .is_some());
        for error in [&topic_error, &key_error] {
            assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_STATE_CORRUPTED);
            assert_eq!(error.operation(), StoreOperation::Read);
        }
    }

    #[tokio::test]
    async fn index_generation_kill_points_recover_without_partial_publication() -> Result<(), StoreError> {
        for stage in [
            FaultStage::Build,
            FaultStage::Sync,
            FaultStage::Rename,
            FaultStage::Current,
        ] {
            let provider = FaultProvider::default();
            let index = IndexFileSegment::with_limits(DEFAULT_INDEX_FILE_PATH.to_owned(), provider.clone(), 8, 8);
            index.append_entry(&entry("old", 100)).await?;
            compact_current(&index).await?;
            assert_eq!(index.current_generation_id().await?, 1);

            index.append_entry(&entry("new", 200)).await?;
            let entries = index.load_entries().await?;
            provider.set_fault(stage);
            assert!(index.compact_entries(&entries).await.is_err());
            drop(index);

            let restarted = IndexFileSegment::with_limits(DEFAULT_INDEX_FILE_PATH.to_owned(), provider.clone(), 8, 8);
            assert_eq!(restarted.load_entries().await?.len(), 2, "stage {stage:?}");
            assert_eq!(restarted.current_generation_id().await?, 1, "stage {stage:?}");
            let orphan = format!("{DEFAULT_INDEX_FILE_PATH}/generations/gen-2");
            assert!(
                provider
                    .list(StoreOperation::Load, orphan.clone())
                    .await?
                    .into_iter()
                    .all(|path| !path.starts_with(&orphan)),
                "stage {stage:?} left orphan generation"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn index_cleanup_failure_keeps_published_generation_and_restart_cleans_orphan() -> Result<(), StoreError> {
        let provider = FaultProvider::default();
        let index = IndexFileSegment::with_limits(DEFAULT_INDEX_FILE_PATH.to_owned(), provider.clone(), 8, 8);
        index.append_entry(&entry("one", 100)).await?;
        compact_current(&index).await?;
        index.append_entry(&entry("two", 200)).await?;
        compact_current(&index).await?;
        index.append_entry(&entry("three", 300)).await?;
        let entries = index.load_entries().await?;
        provider.set_fault(FaultStage::Cleanup);
        assert!(index.compact_entries(&entries).await.is_err());
        assert_eq!(index.current_generation_id().await?, 3);
        let generation_one = format!("{DEFAULT_INDEX_FILE_PATH}/generations/gen-1");
        assert!(!provider
            .list(StoreOperation::Load, generation_one.clone())
            .await?
            .is_empty());
        drop(index);

        let restarted = IndexFileSegment::with_limits(DEFAULT_INDEX_FILE_PATH.to_owned(), provider.clone(), 8, 8);
        assert_eq!(restarted.load_entries().await?.len(), 3);
        assert!(provider
            .list(StoreOperation::Load, generation_one.clone())
            .await?
            .is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn index_reader_lease_delays_retired_generation_deletion() -> Result<(), StoreError> {
        let provider = FaultProvider::default();
        let index = IndexFileSegment::with_limits(DEFAULT_INDEX_FILE_PATH.to_owned(), provider.clone(), 8, 8);
        index.append_entry(&entry("one", 100)).await?;
        compact_current(&index).await?;
        let generation_one_lease = index.generation_manager.current(StoreOperation::Read).await?;
        index.append_entry(&entry("two", 200)).await?;
        compact_current(&index).await?;
        index.append_entry(&entry("three", 300)).await?;
        compact_current(&index).await?;

        let generation_one = format!("{DEFAULT_INDEX_FILE_PATH}/generations/gen-1");
        assert!(!provider
            .list(StoreOperation::Load, generation_one.clone())
            .await?
            .is_empty());
        drop(generation_one_lease);
        index.generation_manager.cleanup_retired().await?;
        assert!(provider.list(StoreOperation::Load, generation_one).await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn index_corruption_rolls_back_but_transient_read_failure_does_not() -> Result<(), StoreError> {
        let provider = FaultProvider::default();
        let index = IndexFileSegment::with_limits(DEFAULT_INDEX_FILE_PATH.to_owned(), provider.clone(), 8, 8);
        index.append_entry(&entry("old", 100)).await?;
        compact_current(&index).await?;
        index.append_entry(&entry("new", 200)).await?;
        compact_current(&index).await?;
        drop(index);

        let transient = IndexFileSegment::with_limits(DEFAULT_INDEX_FILE_PATH.to_owned(), provider.clone(), 8, 8);
        assert_eq!(transient.current_generation_id().await?, 2);
        provider.set_fault(FaultStage::Read);
        let error = transient.load_entries().await.unwrap_err();
        assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_OPERATION_TIMED_OUT);
        assert_eq!(transient.current_generation_id().await?, 2);
        drop(transient);

        let generation_two = format!("{DEFAULT_INDEX_FILE_PATH}/generations/gen-2");
        let segment = provider
            .list(StoreOperation::Load, generation_two)
            .await?
            .into_iter()
            .find(|path| path.rsplit('/').next().is_some_and(|name| name.parse::<i64>().is_ok()))
            .ok_or_else(|| crate::error::internal_failure(rocketmq_store_api::StoreOperation::Load))?;
        provider
            .write(
                StoreOperation::AppendDerived,
                segment,
                0,
                Bytes::from_static(&[0, 0, 0, 0]),
            )
            .await?;

        let corrupt = IndexFileSegment::with_limits(DEFAULT_INDEX_FILE_PATH.to_owned(), provider, 8, 8);
        assert_eq!(corrupt.load_entries().await?, vec![entry("old", 100)]);
        assert_eq!(corrupt.current_generation_id().await?, 1);
        Ok(())
    }

    async fn compact_current(index: &IndexFileSegment<FaultProvider>) -> Result<(), StoreError> {
        let entries = index.load_entries().await?;
        index.compact_entries(&entries).await
    }
}
