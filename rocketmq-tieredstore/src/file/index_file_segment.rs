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

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use rocketmq_error::RocketMQError;

use crate::error;
use crate::provider::TieredStoreProvider;

pub const DEFAULT_INDEX_FILE_PATH: &str = "index/tiered_index_file";

const BEGIN_MAGIC_CODE: u32 = 0xCCDD_EEFF ^ (1_880_681_586_u32 + 4);
const HEADER_SIZE: usize = 40;
const SLOT_SIZE: usize = 8;
const ITEM_HEADER_SIZE: usize = 48;
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

#[derive(Debug, Clone)]
pub struct IndexFileSegment<P>
where
    P: TieredStoreProvider,
{
    directory: String,
    provider: P,
    hash_slot_count: usize,
    max_index_items: usize,
}

#[derive(Debug, Clone)]
struct IndexSegmentHeader {
    begin_timestamp: i64,
    end_timestamp: i64,
    occupied_slot_count: u32,
    item_count: u32,
    hash_slot_count: u32,
    max_index_items: u32,
}

#[derive(Debug, Clone)]
struct IndexRecord {
    entry: TieredIndexEntry,
    hash_code: u32,
    previous_offset: u64,
}

#[derive(Debug, Clone)]
struct IndexRecordHeader {
    hash_code: u32,
    queue_id: i32,
    queue_offset: i64,
    commit_log_offset: u64,
    message_size: usize,
    time_diff: i32,
    previous_offset: u64,
    topic_len: usize,
    key_len: usize,
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
        Self {
            directory,
            provider,
            hash_slot_count: hash_slot_count.max(1),
            max_index_items: max_index_items.max(1),
        }
    }

    pub fn default_path() -> &'static str {
        DEFAULT_INDEX_FILE_PATH
    }

    pub fn path(&self) -> &str {
        &self.directory
    }

    pub async fn append_entry(&self, entry: &TieredIndexEntry) -> Result<(), RocketMQError> {
        let mut segment_timestamps = self.load_manifest().await?;
        if segment_timestamps.is_empty() {
            let timestamp = entry.store_timestamp.max(0);
            segment_timestamps.push(timestamp);
            self.persist_manifest(&segment_timestamps).await?;
        }

        for _ in 0..2 {
            let Some(timestamp) = segment_timestamps.last().copied() else {
                return Err(error::internal("tiered index manifest is unexpectedly empty"));
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

        Err(error::internal("failed to append tiered index entry after rolling"))
    }

    pub async fn load_entries(&self) -> Result<Vec<TieredIndexEntry>, RocketMQError> {
        let mut entries = Vec::new();
        for timestamp in self.load_manifest().await? {
            let path = self.segment_path(timestamp);
            let size = self.provider.segment_size(path.clone()).await?;
            if size == 0 {
                continue;
            }
            let bytes = self.provider.read(path, 0, size as usize).await?;
            entries.extend(decode_segment_entries(&bytes)?.into_iter().map(|record| record.entry));
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
    ) -> Result<Vec<TieredIndexEntry>, RocketMQError> {
        if max_count == 0 || topic.is_empty() || key.is_empty() || begin_time > end_time {
            return Ok(Vec::new());
        }

        let hash_code = java_positive_hash(&format!("{topic}#{key}"));
        let mut result = Vec::new();
        let mut segments = self.load_manifest().await?;
        segments.sort_unstable_by(|left, right| right.cmp(left));
        for timestamp in segments {
            if result.len() >= max_count {
                break;
            }
            let path = self.segment_path(timestamp);
            let size = self.provider.segment_size(path.clone()).await?;
            if size < HEADER_SIZE as u64 {
                continue;
            }
            let header = self.read_header(&path).await?;
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

    pub async fn compact_entries(&self, entries: &[TieredIndexEntry]) -> Result<(), RocketMQError> {
        let old_segments = self.load_manifest().await?;
        for timestamp in old_segments {
            self.provider.delete(self.segment_path(timestamp)).await?;
        }
        self.provider.delete(self.manifest_path()).await?;

        let mut sorted = entries.to_vec();
        sorted.sort_by_key(|entry| (entry.store_timestamp, entry.queue_id, entry.queue_offset));
        sorted.dedup();
        for entry in sorted {
            self.append_entry(&entry).await?;
        }
        Ok(())
    }

    pub async fn segment_count(&self) -> Result<usize, RocketMQError> {
        Ok(self.load_manifest().await?.len())
    }

    async fn try_append_to_segment(
        &self,
        path: &str,
        begin_timestamp: i64,
        entry: &TieredIndexEntry,
    ) -> Result<AppendOutcome, RocketMQError> {
        self.ensure_segment_initialized(path, begin_timestamp).await?;
        let header = self.read_header(path).await?;
        if header.item_count as usize >= header.max_index_items as usize {
            return Ok(AppendOutcome::Full);
        }

        let hash_code = java_positive_hash(&format!("{}#{}", entry.topic, entry.key));
        let slot_index = (hash_code as usize) % header.hash_slot_count as usize;
        let slot_position = slot_position(slot_index);
        let previous_offset = self.read_slot(path, slot_position).await?;
        let append_position = self
            .provider
            .segment_size(path.to_owned())
            .await?
            .max(item_base_position(header.hash_slot_count as usize) as u64);

        let record = IndexRecord {
            entry: entry.clone(),
            hash_code,
            previous_offset,
        };
        let bytes = encode_record(&record, header.begin_timestamp)?;
        self.provider.write(path.to_owned(), append_position, bytes).await?;
        self.provider
            .write(path.to_owned(), slot_position as u64, encode_u64(append_position))
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

    async fn ensure_segment_initialized(&self, path: &str, begin_timestamp: i64) -> Result<(), RocketMQError> {
        if self.provider.segment_size(path.to_owned()).await? >= HEADER_SIZE as u64 {
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

    async fn read_header(&self, path: &str) -> Result<IndexSegmentHeader, RocketMQError> {
        let bytes = self.provider.read(path.to_owned(), 0, HEADER_SIZE).await?;
        decode_header(&bytes)
    }

    async fn write_header(&self, path: &str, header: &IndexSegmentHeader) -> Result<(), RocketMQError> {
        self.provider.write(path.to_owned(), 0, encode_header(header)).await?;
        Ok(())
    }

    async fn read_slot(&self, path: &str, slot_position: usize) -> Result<u64, RocketMQError> {
        let bytes = self
            .provider
            .read(path.to_owned(), slot_position as u64, SLOT_SIZE)
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
    ) -> Result<Vec<TieredIndexEntry>, RocketMQError> {
        let slot_index = (hash_code as usize) % header.hash_slot_count as usize;
        let mut next_offset = self.read_slot(path, slot_position(slot_index)).await?;
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
    ) -> Result<Option<IndexRecord>, RocketMQError> {
        let header = self.provider.read(path.to_owned(), position, ITEM_HEADER_SIZE).await?;
        if header.len() < ITEM_HEADER_SIZE {
            return Ok(None);
        }
        let decoded_header = decode_record_header(&header)?;
        let payload_len = decoded_header.topic_len.saturating_add(decoded_header.key_len);
        let payload = self
            .provider
            .read(
                path.to_owned(),
                position.saturating_add(ITEM_HEADER_SIZE as u64),
                payload_len,
            )
            .await?;
        if payload.len() < payload_len {
            return Ok(None);
        }
        decode_record_payload(decoded_header, &payload, segment_begin_timestamp).map(Some)
    }

    async fn load_manifest(&self) -> Result<Vec<i64>, RocketMQError> {
        let path = self.manifest_path();
        let size = self.provider.segment_size(path.clone()).await?;
        if size == 0 {
            return Ok(Vec::new());
        }
        let bytes = self.provider.read(path.clone(), 0, size as usize).await?;
        let text =
            std::str::from_utf8(&bytes).map_err(|err| error::storage_read_failed(path.clone(), err.to_string()))?;
        let mut timestamps = Vec::new();
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let timestamp = line
                .trim()
                .parse::<i64>()
                .map_err(|err| error::storage_read_failed(path.clone(), err.to_string()))?;
            timestamps.push(timestamp);
        }
        timestamps.sort_unstable();
        timestamps.dedup();
        Ok(timestamps)
    }

    async fn persist_manifest(&self, timestamps: &[i64]) -> Result<(), RocketMQError> {
        let mut timestamps = timestamps.to_vec();
        timestamps.sort_unstable();
        timestamps.dedup();
        let mut data = String::new();
        for timestamp in timestamps {
            data.push_str(&timestamp.to_string());
            data.push('\n');
        }
        self.provider.delete(self.manifest_path()).await?;
        self.provider.write(self.manifest_path(), 0, Bytes::from(data)).await?;
        Ok(())
    }

    fn manifest_path(&self) -> String {
        format!("{}/manifest", self.directory)
    }

    fn segment_path(&self, timestamp: i64) -> String {
        format!("{}/{timestamp}", self.directory)
    }
}

fn encode_header(header: &IndexSegmentHeader) -> Bytes {
    let mut bytes = BytesMut::with_capacity(HEADER_SIZE);
    bytes.put_u32(BEGIN_MAGIC_CODE);
    bytes.put_i64(header.begin_timestamp);
    bytes.put_i64(header.end_timestamp);
    bytes.put_u32(header.occupied_slot_count);
    bytes.put_u32(header.item_count);
    bytes.put_u32(header.hash_slot_count);
    bytes.put_u32(header.max_index_items);
    bytes.put_u32(0);
    bytes.freeze()
}

fn decode_header(bytes: &Bytes) -> Result<IndexSegmentHeader, RocketMQError> {
    if bytes.len() < HEADER_SIZE {
        return Err(error::storage_read_failed(
            DEFAULT_INDEX_FILE_PATH,
            "tiered index segment header is incomplete",
        ));
    }
    let mut bytes = bytes.clone();
    let magic = bytes.get_u32();
    if magic != BEGIN_MAGIC_CODE {
        return Err(error::storage_read_failed(
            DEFAULT_INDEX_FILE_PATH,
            "tiered index segment magic code mismatch",
        ));
    }
    let begin_timestamp = bytes.get_i64();
    let end_timestamp = bytes.get_i64();
    let occupied_slot_count = bytes.get_u32();
    let item_count = bytes.get_u32();
    let hash_slot_count = bytes.get_u32().max(1);
    let max_index_items = bytes.get_u32().max(1);
    let _reserved = bytes.get_u32();
    Ok(IndexSegmentHeader {
        begin_timestamp,
        end_timestamp,
        occupied_slot_count,
        item_count,
        hash_slot_count,
        max_index_items,
    })
}

fn encode_record(record: &IndexRecord, segment_begin_timestamp: i64) -> Result<Bytes, RocketMQError> {
    let topic = record.entry.topic.as_bytes();
    let key = record.entry.key.as_bytes();
    if topic.len() > u16::MAX as usize || key.len() > u16::MAX as usize {
        return Err(error::illegal_argument("tiered index topic/key is too long"));
    }
    let time_diff = record
        .entry
        .store_timestamp
        .saturating_sub(segment_begin_timestamp)
        .saturating_div(1000)
        .clamp(i32::MIN as i64, i32::MAX as i64) as i32;

    let mut bytes = BytesMut::with_capacity(ITEM_HEADER_SIZE + topic.len() + key.len());
    bytes.put_u32(record.hash_code);
    bytes.put_i32(record.entry.queue_id);
    bytes.put_i64(record.entry.queue_offset);
    bytes.put_u64(record.entry.commit_log_offset);
    bytes.put_u32(record.entry.message_size as u32);
    bytes.put_i32(time_diff);
    bytes.put_u64(record.previous_offset);
    bytes.put_u16(topic.len() as u16);
    bytes.put_u16(key.len() as u16);
    bytes.put_u32(0);
    bytes.put_slice(topic);
    bytes.put_slice(key);
    Ok(bytes.freeze())
}

fn decode_segment_entries(bytes: &Bytes) -> Result<Vec<IndexRecord>, RocketMQError> {
    let header = decode_header(bytes)?;
    let mut position = item_base_position(header.hash_slot_count as usize);
    let mut records = Vec::with_capacity(header.item_count as usize);
    while records.len() < header.item_count as usize && position < bytes.len() {
        if bytes.len().saturating_sub(position) < ITEM_HEADER_SIZE {
            return Err(error::storage_read_failed(
                DEFAULT_INDEX_FILE_PATH,
                "tiered index item header is incomplete",
            ));
        }
        let decoded_header = decode_record_header(&bytes.slice(position..position + ITEM_HEADER_SIZE))?;
        position += ITEM_HEADER_SIZE;

        let string_len = decoded_header.topic_len.saturating_add(decoded_header.key_len);
        if bytes.len().saturating_sub(position) < string_len {
            return Err(error::storage_read_failed(
                DEFAULT_INDEX_FILE_PATH,
                "tiered index item key payload is incomplete",
            ));
        }
        let payload = bytes.slice(position..position + string_len);
        position += string_len;
        records.push(decode_record_payload(decoded_header, &payload, header.begin_timestamp)?);
    }
    Ok(records)
}

fn decode_record_header(bytes: &Bytes) -> Result<IndexRecordHeader, RocketMQError> {
    if bytes.len() < ITEM_HEADER_SIZE {
        return Err(error::storage_read_failed(
            DEFAULT_INDEX_FILE_PATH,
            "tiered index item header is incomplete",
        ));
    }
    let mut bytes = bytes.clone();
    let hash_code = bytes.get_u32();
    let queue_id = bytes.get_i32();
    let queue_offset = bytes.get_i64();
    let commit_log_offset = bytes.get_u64();
    let message_size = bytes.get_u32() as usize;
    let time_diff = bytes.get_i32();
    let previous_offset = bytes.get_u64();
    let topic_len = bytes.get_u16() as usize;
    let key_len = bytes.get_u16() as usize;
    let _reserved = bytes.get_u32();
    Ok(IndexRecordHeader {
        hash_code,
        queue_id,
        queue_offset,
        commit_log_offset,
        message_size,
        time_diff,
        previous_offset,
        topic_len,
        key_len,
    })
}

fn decode_record_payload(
    header: IndexRecordHeader,
    payload: &Bytes,
    segment_begin_timestamp: i64,
) -> Result<IndexRecord, RocketMQError> {
    let string_len = header.topic_len.saturating_add(header.key_len);
    if payload.len() < string_len {
        return Err(error::storage_read_failed(
            DEFAULT_INDEX_FILE_PATH,
            "tiered index item key payload is incomplete",
        ));
    }
    let topic = std::str::from_utf8(&payload[..header.topic_len])
        .map_err(|err| error::storage_read_failed(DEFAULT_INDEX_FILE_PATH, err.to_string()))?
        .to_owned();
    let key = std::str::from_utf8(&payload[header.topic_len..string_len])
        .map_err(|err| error::storage_read_failed(DEFAULT_INDEX_FILE_PATH, err.to_string()))?
        .to_owned();

    Ok(IndexRecord {
        entry: TieredIndexEntry {
            topic,
            key,
            queue_id: header.queue_id,
            queue_offset: header.queue_offset,
            commit_log_offset: header.commit_log_offset,
            message_size: header.message_size,
            store_timestamp: segment_begin_timestamp.saturating_add((header.time_diff as i64).saturating_mul(1000)),
        },
        hash_code: header.hash_code,
        previous_offset: header.previous_offset,
    })
}

fn encode_u64(value: u64) -> Bytes {
    let mut bytes = BytesMut::with_capacity(SLOT_SIZE);
    bytes.put_u64(value);
    bytes.freeze()
}

fn slot_position(slot_index: usize) -> usize {
    HEADER_SIZE + slot_index.saturating_mul(SLOT_SIZE)
}

fn item_base_position(hash_slot_count: usize) -> usize {
    HEADER_SIZE + hash_slot_count.saturating_mul(SLOT_SIZE)
}

fn java_positive_hash(key: &str) -> u32 {
    let mut hash = 0_i32;
    for value in key.encode_utf16() {
        hash = hash.wrapping_mul(31).wrapping_add(value as i32);
    }
    if hash < 0 {
        hash.wrapping_neg() as u32
    } else {
        hash as u32
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_error::RocketMQError;

    use super::*;
    use crate::provider::MemoryProvider;

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
    async fn index_file_appends_loads_and_compacts_entries() -> Result<(), RocketMQError> {
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
    async fn index_file_rolls_to_multiple_segments_when_full() -> Result<(), RocketMQError> {
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
    async fn index_file_queries_through_hash_slot_chain() -> Result<(), RocketMQError> {
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
}
