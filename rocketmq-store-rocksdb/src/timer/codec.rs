// Copyright 2026 The RocketMQ Rust Authors
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

use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;
use rocketmq_store_api::TimerEngineId;
use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::TimerPayloadStoreLocator;
use rocketmq_store_api::TimerSourceCqOffset;
use rocketmq_store_api::EXTENDED_TIMELINE_FORMAT_VERSION;

use crate::error::codec_contract;
use crate::error::codec_corrupted;
use crate::error::state_corrupted_source;

const TIMELINE_KEY_SIZE: usize = 35;
const TIMELINE_VALUE_SIZE: usize = 66;
const READY_KEY_SIZE: usize = 35;
const LOOKUP_KEY_HEADER_SIZE: usize = 4;
const LOOKUP_VALUE_SIZE: usize = 47;
const KEY_VERSION: u8 = 1;
const CRC_SIZE: usize = 4;

/// Ordered key for one long-horizon timer generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TimelineKeyV1 {
    /// Original, unrounded millisecond deadline.
    pub due_time_ms: i64,
    /// Stable delivery lane.
    pub lane: u16,
    /// Logical timer identity.
    pub timer_id: TimerId,
    /// Generation fencing stale work.
    pub generation: TimerGeneration,
}

impl TimelineKeyV1 {
    /// Encodes a lexicographically time-ordered key.
    pub fn encode(self) -> [u8; TIMELINE_KEY_SIZE] {
        let mut output = [0u8; TIMELINE_KEY_SIZE];
        output[0] = KEY_VERSION;
        output[1..9].copy_from_slice(&ordered_i64(self.due_time_ms).to_be_bytes());
        output[9..11].copy_from_slice(&self.lane.to_be_bytes());
        output[11..27].copy_from_slice(&self.timer_id.get().to_be_bytes());
        output[27..35].copy_from_slice(&self.generation.get().to_be_bytes());
        output
    }

    /// Decodes and validates a timeline key.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown versions or malformed lengths.
    pub fn decode(operation: StoreOperation, bytes: &[u8]) -> Result<Self, StoreError> {
        if bytes.len() != TIMELINE_KEY_SIZE || bytes[0] != KEY_VERSION {
            return Err(codec_corrupted(operation));
        }
        Ok(Self {
            due_time_ms: unordered_i64(read_u64(operation, bytes, 1)?),
            lane: read_u16(operation, bytes, 9)?,
            timer_id: TimerId::new(read_u128(operation, bytes, 11)?),
            generation: TimerGeneration::new(read_u64(operation, bytes, 27)?),
        })
    }

    /// Returns the fixed encoded key size.
    pub const fn encoded_size() -> usize {
        TIMELINE_KEY_SIZE
    }
}

/// Small Timeline value. Complete message bytes live in the payload store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimelineRecordV1 {
    /// Long-horizon payload-store locator.
    pub payload: TimerPayloadStoreLocator,
    /// Durable source Timer ConsumeQueue offset.
    pub source_cq_offset: TimerSourceCqOffset,
    /// Original physical source identity, used only for replay reconciliation.
    pub source_physical_offset: i64,
    /// Original physical source length.
    pub source_size: u32,
    /// Optimistic state version.
    pub state_version: u64,
    /// Persisted owner engine.
    pub owner_engine: TimerEngineId,
    /// True only for Java-compatible shadow records.
    pub shadow_only: bool,
}

impl TimelineRecordV1 {
    /// Encodes the record with a trailing CRC32C.
    pub fn encode(self) -> [u8; TIMELINE_VALUE_SIZE] {
        let mut output = [0u8; TIMELINE_VALUE_SIZE];
        output[0..2].copy_from_slice(&EXTENDED_TIMELINE_FORMAT_VERSION.to_be_bytes());
        output[2..6].copy_from_slice(&self.payload.due_day_utc().to_be_bytes());
        output[6..8].copy_from_slice(&self.payload.lane().to_be_bytes());
        output[8..16].copy_from_slice(&self.payload.segment_id().to_be_bytes());
        output[16..24].copy_from_slice(&self.payload.offset().to_be_bytes());
        output[24..28].copy_from_slice(&self.payload.length().to_be_bytes());
        output[28..32].copy_from_slice(&self.payload.checksum().to_be_bytes());
        output[32..40].copy_from_slice(&self.source_cq_offset.get().to_be_bytes());
        output[40..48].copy_from_slice(&self.source_physical_offset.to_be_bytes());
        output[48..52].copy_from_slice(&self.source_size.to_be_bytes());
        output[52..60].copy_from_slice(&self.state_version.to_be_bytes());
        output[60] = encode_engine(self.owner_engine);
        output[61] = u8::from(self.shadow_only);
        let checksum = crc32c(&output[..TIMELINE_VALUE_SIZE - CRC_SIZE]);
        output[TIMELINE_VALUE_SIZE - CRC_SIZE..].copy_from_slice(&checksum.to_be_bytes());
        output
    }

    /// Decodes and verifies a Timeline value.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown versions, malformed lengths, invalid locators, or CRC damage.
    pub fn decode(operation: StoreOperation, bytes: &[u8]) -> Result<Self, StoreError> {
        if bytes.len() != TIMELINE_VALUE_SIZE
            || read_u16(operation, bytes, 0)? != EXTENDED_TIMELINE_FORMAT_VERSION
            || crc32c(&bytes[..TIMELINE_VALUE_SIZE - CRC_SIZE])
                != read_u32(operation, bytes, TIMELINE_VALUE_SIZE - CRC_SIZE)?
        {
            return Err(codec_corrupted(operation));
        }
        let payload = TimerPayloadStoreLocator::try_new(
            read_i32(operation, bytes, 2)?,
            read_u16(operation, bytes, 6)?,
            read_u64(operation, bytes, 8)?,
            read_u64(operation, bytes, 16)?,
            read_u32(operation, bytes, 24)?,
            read_u32(operation, bytes, 28)?,
        )
        .map_err(|source| state_corrupted_source(operation, source))?;
        let source_cq_offset = read_i64(operation, bytes, 32)?;
        let source_physical_offset = read_i64(operation, bytes, 40)?;
        let source_size = read_u32(operation, bytes, 48)?;
        if source_cq_offset < 0 || source_physical_offset < 0 || source_size == 0 || bytes[61] > 1 {
            return Err(codec_corrupted(operation));
        }
        Ok(Self {
            payload,
            source_cq_offset: TimerSourceCqOffset::new(source_cq_offset),
            source_physical_offset,
            source_size,
            state_version: read_u64(operation, bytes, 52)?,
            owner_engine: decode_engine(operation, bytes[60])?,
            shadow_only: bytes[61] == 1,
        })
    }

    /// Returns the fixed encoded value size.
    pub const fn encoded_size() -> usize {
        TIMELINE_VALUE_SIZE
    }
}

/// Structured recall lookup key, avoiding delimiter collisions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecallLookupKeyV1 {
    /// Engine route encoded into the lookup namespace.
    pub engine: TimerEngineId,
    /// Original real topic.
    pub topic: String,
    /// Producer unique key.
    pub unique_key: String,
}

impl RecallLookupKeyV1 {
    /// Encodes length-prefixed topic and unique-key components.
    ///
    /// # Errors
    ///
    /// Returns an error when a component is empty or cannot fit a `u16` length.
    pub fn encode(&self, operation: StoreOperation) -> Result<Vec<u8>, StoreError> {
        let topic = self.topic.as_bytes();
        let unique_key = self.unique_key.as_bytes();
        let topic_len = u16::try_from(topic.len()).map_err(|_| codec_contract(operation))?;
        let unique_len = u16::try_from(unique_key.len()).map_err(|_| codec_contract(operation))?;
        if topic.is_empty() || unique_key.is_empty() {
            return Err(codec_contract(operation));
        }
        let mut output = Vec::with_capacity(LOOKUP_KEY_HEADER_SIZE + topic.len() + unique_key.len() + 2);
        output.push(KEY_VERSION);
        output.push(encode_engine(self.engine));
        output.extend_from_slice(&topic_len.to_be_bytes());
        output.extend_from_slice(topic);
        output.extend_from_slice(&unique_len.to_be_bytes());
        output.extend_from_slice(unique_key);
        Ok(output)
    }

    /// Decodes a structured recall lookup key.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed lengths, UTF-8, or unknown engines.
    pub fn decode(operation: StoreOperation, bytes: &[u8]) -> Result<Self, StoreError> {
        if bytes.len() < LOOKUP_KEY_HEADER_SIZE + 2 || bytes[0] != KEY_VERSION {
            return Err(codec_corrupted(operation));
        }
        let topic_len = usize::from(read_u16(operation, bytes, 2)?);
        let topic_start = LOOKUP_KEY_HEADER_SIZE;
        let topic_end = topic_start.saturating_add(topic_len);
        if topic_len == 0 || topic_end.saturating_add(2) > bytes.len() {
            return Err(codec_corrupted(operation));
        }
        let unique_len = usize::from(read_u16(operation, bytes, topic_end)?);
        let unique_start = topic_end + 2;
        let unique_end = unique_start.saturating_add(unique_len);
        if unique_len == 0 || unique_end != bytes.len() {
            return Err(codec_corrupted(operation));
        }
        Ok(Self {
            engine: decode_engine(operation, bytes[1])?,
            topic: std::str::from_utf8(&bytes[topic_start..topic_end])
                .map_err(|source| state_corrupted_source(operation, source))?
                .to_owned(),
            unique_key: std::str::from_utf8(&bytes[unique_start..unique_end])
                .map_err(|source| state_corrupted_source(operation, source))?
                .to_owned(),
        })
    }
}

/// Value referenced by a structured Recall lookup key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecallLookupValueV1 {
    /// Logical timer identity.
    pub timer_id: TimerId,
    /// Active generation.
    pub generation: TimerGeneration,
    /// Original deadline.
    pub due_time_ms: i64,
    /// Stable lane.
    pub lane: u16,
    /// State version observed when the lookup was written.
    pub state_version: u64,
}

impl RecallLookupValueV1 {
    /// Encodes the lookup value with CRC32C.
    pub fn encode(self) -> [u8; LOOKUP_VALUE_SIZE] {
        let mut output = [0u8; LOOKUP_VALUE_SIZE];
        output[0] = KEY_VERSION;
        output[1..17].copy_from_slice(&self.timer_id.get().to_be_bytes());
        output[17..25].copy_from_slice(&self.generation.get().to_be_bytes());
        output[25..33].copy_from_slice(&self.due_time_ms.to_be_bytes());
        output[33..35].copy_from_slice(&self.lane.to_be_bytes());
        output[35..43].copy_from_slice(&self.state_version.to_be_bytes());
        let checksum = crc32c(&output[..LOOKUP_VALUE_SIZE - CRC_SIZE]);
        output[LOOKUP_VALUE_SIZE - CRC_SIZE..].copy_from_slice(&checksum.to_be_bytes());
        output
    }

    /// Decodes and verifies a lookup value.
    pub fn decode(operation: StoreOperation, bytes: &[u8]) -> Result<Self, StoreError> {
        if bytes.len() != LOOKUP_VALUE_SIZE
            || bytes[0] != KEY_VERSION
            || crc32c(&bytes[..LOOKUP_VALUE_SIZE - CRC_SIZE])
                != read_u32(operation, bytes, LOOKUP_VALUE_SIZE - CRC_SIZE)?
        {
            return Err(codec_corrupted(operation));
        }
        Ok(Self {
            timer_id: TimerId::new(read_u128(operation, bytes, 1)?),
            generation: TimerGeneration::new(read_u64(operation, bytes, 17)?),
            due_time_ms: read_i64(operation, bytes, 25)?,
            lane: read_u16(operation, bytes, 33)?,
            state_version: read_u64(operation, bytes, 35)?,
        })
    }
}

/// Encodes a durable ready key ordered by lane and deadline.
pub fn encode_ready_key(key: TimelineKeyV1) -> [u8; READY_KEY_SIZE] {
    let mut output = [0u8; READY_KEY_SIZE];
    output[0] = KEY_VERSION;
    output[1..3].copy_from_slice(&key.lane.to_be_bytes());
    output[3..11].copy_from_slice(&ordered_i64(key.due_time_ms).to_be_bytes());
    output[11..27].copy_from_slice(&key.timer_id.get().to_be_bytes());
    output[27..35].copy_from_slice(&key.generation.get().to_be_bytes());
    output
}

/// Decodes a durable ready key.
pub fn decode_ready_key(operation: StoreOperation, bytes: &[u8]) -> Result<TimelineKeyV1, StoreError> {
    if bytes.len() != READY_KEY_SIZE || bytes[0] != KEY_VERSION {
        return Err(codec_corrupted(operation));
    }
    Ok(TimelineKeyV1 {
        due_time_ms: unordered_i64(read_u64(operation, bytes, 3)?),
        lane: read_u16(operation, bytes, 1)?,
        timer_id: TimerId::new(read_u128(operation, bytes, 11)?),
        generation: TimerGeneration::new(read_u64(operation, bytes, 27)?),
    })
}

pub(crate) const fn encode_engine(engine: TimerEngineId) -> u8 {
    match engine {
        TimerEngineId::JavaCompat => 0,
        TimerEngineId::ExtendedTimeline => 1,
    }
}

pub(crate) fn decode_engine(operation: StoreOperation, value: u8) -> Result<TimerEngineId, StoreError> {
    match value {
        0 => Ok(TimerEngineId::JavaCompat),
        1 => Ok(TimerEngineId::ExtendedTimeline),
        _ => Err(codec_corrupted(operation)),
    }
}

pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = 0u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0x82F6_3B78 & mask);
        }
    }
    !crc
}

const fn ordered_i64(value: i64) -> u64 {
    (value as u64) ^ (1u64 << 63)
}

const fn unordered_i64(value: u64) -> i64 {
    (value ^ (1u64 << 63)) as i64
}

fn read_array<const N: usize>(operation: StoreOperation, bytes: &[u8], offset: usize) -> Result<[u8; N], StoreError> {
    let value = bytes
        .get(offset..offset.saturating_add(N))
        .ok_or_else(|| codec_corrupted(operation))?;
    value
        .try_into()
        .map_err(|source| state_corrupted_source(operation, source))
}

fn read_u16(operation: StoreOperation, bytes: &[u8], offset: usize) -> Result<u16, StoreError> {
    Ok(u16::from_be_bytes(read_array(operation, bytes, offset)?))
}

fn read_u32(operation: StoreOperation, bytes: &[u8], offset: usize) -> Result<u32, StoreError> {
    Ok(u32::from_be_bytes(read_array(operation, bytes, offset)?))
}

fn read_i32(operation: StoreOperation, bytes: &[u8], offset: usize) -> Result<i32, StoreError> {
    Ok(i32::from_be_bytes(read_array(operation, bytes, offset)?))
}

fn read_u64(operation: StoreOperation, bytes: &[u8], offset: usize) -> Result<u64, StoreError> {
    Ok(u64::from_be_bytes(read_array(operation, bytes, offset)?))
}

fn read_i64(operation: StoreOperation, bytes: &[u8], offset: usize) -> Result<i64, StoreError> {
    Ok(i64::from_be_bytes(read_array(operation, bytes, offset)?))
}

fn read_u128(operation: StoreOperation, bytes: &[u8], offset: usize) -> Result<u128, StoreError> {
    Ok(u128::from_be_bytes(read_array(operation, bytes, offset)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32c_matches_the_standard_check_value() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn signed_deadlines_remain_lexicographically_ordered() {
        let key = |due_time_ms| {
            TimelineKeyV1 {
                due_time_ms,
                lane: 0,
                timer_id: TimerId::new(1),
                generation: TimerGeneration::new(0),
            }
            .encode()
        };
        assert!(key(-1) < key(0));
        assert!(key(7_999) < key(8_000));
        assert!(key(8_000) < key(8_001));
    }
}
