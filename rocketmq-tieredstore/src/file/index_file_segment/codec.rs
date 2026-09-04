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
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;

use super::TieredIndexEntry;
use crate::error;
use crate::file::index_generation::crc32;
use crate::file::index_generation::IndexGenerationMetadata;

const BEGIN_MAGIC_CODE: u32 = 0xCCDD_EEFF ^ (1_880_681_586_u32 + 4);
pub(super) const HEADER_SIZE: usize = 40;
pub(super) const SLOT_SIZE: usize = 8;
pub(super) const ITEM_HEADER_SIZE: usize = 48;

#[derive(Debug, Clone)]
pub(super) struct IndexSegmentHeader {
    pub(super) begin_timestamp: i64,
    pub(super) end_timestamp: i64,
    pub(super) occupied_slot_count: u32,
    pub(super) item_count: u32,
    pub(super) hash_slot_count: u32,
    pub(super) max_index_items: u32,
}

#[derive(Debug, Clone)]
pub(super) struct IndexRecord {
    pub(super) entry: TieredIndexEntry,
    pub(super) hash_code: u32,
    pub(super) previous_offset: u64,
}

#[derive(Debug, Clone)]
pub(super) struct IndexRecordHeader {
    pub(super) hash_code: u32,
    pub(super) queue_id: i32,
    pub(super) queue_offset: i64,
    pub(super) commit_log_offset: u64,
    pub(super) message_size: usize,
    pub(super) time_diff: i32,
    pub(super) previous_offset: u64,
    pub(super) topic_len: usize,
    pub(super) key_len: usize,
}

pub(super) fn normalize_entries(entries: &mut Vec<TieredIndexEntry>) {
    entries.sort_by(|left, right| {
        left.topic
            .cmp(&right.topic)
            .then(left.key.cmp(&right.key))
            .then(left.store_timestamp.cmp(&right.store_timestamp))
            .then(left.queue_id.cmp(&right.queue_id))
            .then(left.queue_offset.cmp(&right.queue_offset))
            .then(left.commit_log_offset.cmp(&right.commit_log_offset))
            .then(left.message_size.cmp(&right.message_size))
    });
    entries.dedup();
}

pub(super) fn generation_metadata(generation: u64, entries: &[TieredIndexEntry]) -> IndexGenerationMetadata {
    if entries.is_empty() {
        return IndexGenerationMetadata::empty(generation);
    }
    let mut canonical = BytesMut::new();
    let mut min_timestamp = i64::MAX;
    let mut max_timestamp = i64::MIN;
    let mut min_commit_log_offset = u64::MAX;
    let mut max_commit_log_offset = 0_u64;
    for entry in entries {
        canonical.put_u16(entry.topic.len() as u16);
        canonical.put_slice(entry.topic.as_bytes());
        canonical.put_u16(entry.key.len() as u16);
        canonical.put_slice(entry.key.as_bytes());
        canonical.put_i32(entry.queue_id);
        canonical.put_i64(entry.queue_offset);
        canonical.put_u64(entry.commit_log_offset);
        canonical.put_u64(entry.message_size as u64);
        canonical.put_i64(entry.store_timestamp);
        min_timestamp = min_timestamp.min(entry.store_timestamp);
        max_timestamp = max_timestamp.max(entry.store_timestamp);
        min_commit_log_offset = min_commit_log_offset.min(entry.commit_log_offset);
        max_commit_log_offset = max_commit_log_offset.max(entry.commit_log_offset);
    }
    IndexGenerationMetadata {
        generation,
        entry_count: entries.len() as u64,
        min_timestamp,
        max_timestamp,
        min_commit_log_offset,
        max_commit_log_offset,
        content_crc: crc32(&canonical),
    }
}

pub(super) fn encode_header(header: &IndexSegmentHeader) -> Bytes {
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

pub(super) fn decode_header(operation: StoreOperation, bytes: &Bytes) -> Result<IndexSegmentHeader, StoreError> {
    if bytes.len() < HEADER_SIZE {
        return Err(error::state_corrupted(operation));
    }
    let mut bytes = bytes.clone();
    let magic = bytes.get_u32();
    if magic != BEGIN_MAGIC_CODE {
        return Err(error::state_corrupted(operation));
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

pub(super) fn encode_record(
    operation: StoreOperation,
    record: &IndexRecord,
    segment_begin_timestamp: i64,
) -> Result<Bytes, StoreError> {
    let topic = record.entry.topic.as_bytes();
    let key = record.entry.key.as_bytes();
    if topic.len() > u16::MAX as usize || key.len() > u16::MAX as usize {
        return Err(error::request_invalid(operation));
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

pub(super) fn decode_segment_entries(operation: StoreOperation, bytes: &Bytes) -> Result<Vec<IndexRecord>, StoreError> {
    let header = decode_header(operation, bytes)?;
    let mut position = item_base_position(header.hash_slot_count as usize);
    let mut records = Vec::with_capacity(header.item_count as usize);
    while records.len() < header.item_count as usize && position < bytes.len() {
        if bytes.len().saturating_sub(position) < ITEM_HEADER_SIZE {
            return Err(error::state_corrupted(operation));
        }
        let decoded_header = decode_record_header(operation, &bytes.slice(position..position + ITEM_HEADER_SIZE))?;
        position += ITEM_HEADER_SIZE;

        let string_len = decoded_header.topic_len.saturating_add(decoded_header.key_len);
        if bytes.len().saturating_sub(position) < string_len {
            return Err(error::state_corrupted(operation));
        }
        let payload = bytes.slice(position..position + string_len);
        position += string_len;
        records.push(decode_record_payload(
            operation,
            decoded_header,
            &payload,
            header.begin_timestamp,
        )?);
    }
    Ok(records)
}

pub(super) fn decode_record_header(operation: StoreOperation, bytes: &Bytes) -> Result<IndexRecordHeader, StoreError> {
    if bytes.len() < ITEM_HEADER_SIZE {
        return Err(error::state_corrupted(operation));
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

pub(super) fn decode_record_payload(
    operation: StoreOperation,
    header: IndexRecordHeader,
    payload: &Bytes,
    segment_begin_timestamp: i64,
) -> Result<IndexRecord, StoreError> {
    let string_len = header.topic_len.saturating_add(header.key_len);
    if payload.len() < string_len {
        return Err(error::state_corrupted(operation));
    }
    let topic = std::str::from_utf8(&payload[..header.topic_len])
        .map_err(|source| error::state_corrupted_source(operation, source))?
        .to_owned();
    let key = std::str::from_utf8(&payload[header.topic_len..string_len])
        .map_err(|source| error::state_corrupted_source(operation, source))?
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

pub(super) fn encode_u64(value: u64) -> Bytes {
    let mut bytes = BytesMut::with_capacity(SLOT_SIZE);
    bytes.put_u64(value);
    bytes.freeze()
}

pub(super) fn slot_position(slot_index: usize) -> usize {
    HEADER_SIZE + slot_index.saturating_mul(SLOT_SIZE)
}

pub(super) fn item_base_position(hash_slot_count: usize) -> usize {
    HEADER_SIZE + hash_slot_count.saturating_mul(SLOT_SIZE)
}

pub(super) fn java_positive_hash(key: &str) -> u32 {
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
