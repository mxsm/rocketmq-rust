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

use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::TimerSourceCqOffset;
use thiserror::Error;

use crate::timer::storage_format::crc32c;

const PAYLOAD_MAGIC: u32 = 0x5450_4C31;
const PAYLOAD_VERSION: u16 = 1;
const PAYLOAD_HEADER_SIZE: usize = 72;
const PAYLOAD_TRAILER_SIZE: usize = 4;

/// Complete, independently recoverable long-horizon Timer payload record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimerPayloadRecordV1 {
    /// Original, unrounded millisecond deadline.
    pub due_time_ms: i64,
    /// Stable delivery lane.
    pub lane: u16,
    /// Logical timer identity.
    pub timer_id: TimerId,
    /// Generation fencing stale work.
    pub generation: TimerGeneration,
    /// Durable source Timer ConsumeQueue offset.
    pub source_cq_offset: TimerSourceCqOffset,
    /// Original source CommitLog physical offset, used for replay identity only.
    pub source_physical_offset: i64,
    /// Original real-topic queue id.
    pub real_queue_id: i32,
    /// Original real topic.
    pub real_topic: String,
    /// Complete original CommitLog frame or an equivalent complete message encoding.
    pub frame: Vec<u8>,
}

impl TimerPayloadRecordV1 {
    /// Returns the UTC day partition derived from the original deadline.
    ///
    /// # Errors
    ///
    /// Returns an error when the deadline predates the Unix epoch or the day exceeds V1 range.
    pub(crate) fn due_day_utc_checked(&self) -> Result<i32, TimerPayloadRecordViolation> {
        if self.due_time_ms < 0 {
            return Err(TimerPayloadRecordViolation::InvalidDeadline(self.due_time_ms));
        }
        i32::try_from(self.due_time_ms.div_euclid(86_400_000))
            .map_err(|_| TimerPayloadRecordViolation::InvalidDeadline(self.due_time_ms))
    }

    /// Returns the encoded record length.
    ///
    /// # Errors
    ///
    /// Returns an error when variable fields cannot fit the V1 length fields.
    pub(crate) fn encoded_len_checked(&self) -> Result<usize, TimerPayloadRecordViolation> {
        validate(self)?;
        Ok(PAYLOAD_HEADER_SIZE
            .saturating_add(self.real_topic.len())
            .saturating_add(self.frame.len())
            .saturating_add(PAYLOAD_TRAILER_SIZE))
    }

    /// Encodes the record with a trailing CRC32C.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid metadata or oversized variable fields.
    pub(crate) fn encode_checked(&self) -> Result<Vec<u8>, TimerPayloadRecordViolation> {
        let total_len = self.encoded_len_checked()?;
        let total_len_u32 =
            u32::try_from(total_len).map_err(|_| TimerPayloadRecordViolation::RecordTooLarge(total_len))?;
        let topic_len = u16::try_from(self.real_topic.len())
            .map_err(|_| TimerPayloadRecordViolation::TopicTooLong(self.real_topic.len()))?;
        let frame_len = u32::try_from(self.frame.len())
            .map_err(|_| TimerPayloadRecordViolation::RecordTooLarge(self.frame.len()))?;
        let mut output = Vec::with_capacity(total_len);
        output.extend_from_slice(&PAYLOAD_MAGIC.to_be_bytes());
        output.extend_from_slice(&PAYLOAD_VERSION.to_be_bytes());
        output.extend_from_slice(&(PAYLOAD_HEADER_SIZE as u16).to_be_bytes());
        output.extend_from_slice(&total_len_u32.to_be_bytes());
        output.extend_from_slice(&self.due_time_ms.to_be_bytes());
        output.extend_from_slice(&self.lane.to_be_bytes());
        output.extend_from_slice(&self.timer_id.get().to_be_bytes());
        output.extend_from_slice(&self.generation.get().to_be_bytes());
        output.extend_from_slice(&self.source_cq_offset.get().to_be_bytes());
        output.extend_from_slice(&self.source_physical_offset.to_be_bytes());
        output.extend_from_slice(&self.real_queue_id.to_be_bytes());
        output.extend_from_slice(&topic_len.to_be_bytes());
        output.extend_from_slice(&frame_len.to_be_bytes());
        debug_assert_eq!(output.len(), PAYLOAD_HEADER_SIZE);
        output.extend_from_slice(self.real_topic.as_bytes());
        output.extend_from_slice(&self.frame);
        let checksum = crc32c(&output);
        output.extend_from_slice(&checksum.to_be_bytes());
        Ok(output)
    }

    /// Decodes and verifies a complete payload record.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown versions, length damage, UTF-8 damage, or CRC mismatch.
    pub(crate) fn decode_checked(bytes: &[u8]) -> Result<Self, TimerPayloadRecordViolation> {
        if bytes.len() < PAYLOAD_HEADER_SIZE + PAYLOAD_TRAILER_SIZE {
            return Err(TimerPayloadRecordViolation::Truncated);
        }
        if read_u32(bytes, 0)? != PAYLOAD_MAGIC {
            return Err(TimerPayloadRecordViolation::BadMagic);
        }
        if read_u16(bytes, 4)? != PAYLOAD_VERSION {
            return Err(TimerPayloadRecordViolation::UnsupportedVersion(read_u16(bytes, 4)?));
        }
        if usize::from(read_u16(bytes, 6)?) != PAYLOAD_HEADER_SIZE {
            return Err(TimerPayloadRecordViolation::InvalidHeaderLength);
        }
        let total_len = usize::try_from(read_u32(bytes, 8)?).map_err(|_| TimerPayloadRecordViolation::Truncated)?;
        if total_len != bytes.len() {
            return Err(TimerPayloadRecordViolation::InvalidRecordLength {
                expected: total_len,
                actual: bytes.len(),
            });
        }
        if crc32c(&bytes[..bytes.len() - PAYLOAD_TRAILER_SIZE]) != read_u32(bytes, bytes.len() - PAYLOAD_TRAILER_SIZE)?
        {
            return Err(TimerPayloadRecordViolation::ChecksumMismatch);
        }
        let topic_len = usize::from(read_u16(bytes, 66)?);
        let frame_len = usize::try_from(read_u32(bytes, 68)?).map_err(|_| TimerPayloadRecordViolation::Truncated)?;
        let topic_end = PAYLOAD_HEADER_SIZE.saturating_add(topic_len);
        let frame_end = topic_end.saturating_add(frame_len);
        if topic_len == 0 || frame_len == 0 || frame_end.saturating_add(PAYLOAD_TRAILER_SIZE) != bytes.len() {
            return Err(TimerPayloadRecordViolation::Truncated);
        }
        let due_time_ms = read_i64(bytes, 12)?;
        let source_cq_offset = read_i64(bytes, 46)?;
        let source_physical_offset = read_i64(bytes, 54)?;
        if due_time_ms < 0 || source_cq_offset < 0 || source_physical_offset < 0 {
            return Err(TimerPayloadRecordViolation::InvalidIdentity);
        }
        Ok(Self {
            due_time_ms,
            lane: read_u16(bytes, 20)?,
            timer_id: TimerId::new(read_u128(bytes, 22)?),
            generation: TimerGeneration::new(read_u64(bytes, 38)?),
            source_cq_offset: TimerSourceCqOffset::new(source_cq_offset),
            source_physical_offset,
            real_queue_id: read_i32(bytes, 62)?,
            real_topic: std::str::from_utf8(&bytes[PAYLOAD_HEADER_SIZE..topic_end])
                .map_err(|_| TimerPayloadRecordViolation::InvalidTopicUtf8)?
                .to_owned(),
            frame: bytes[topic_end..frame_end].to_vec(),
        })
    }

    /// Reads the declared total length from a V1 header.
    pub(crate) fn declared_len(header: &[u8]) -> Result<usize, TimerPayloadRecordViolation> {
        if header.len() < PAYLOAD_HEADER_SIZE || read_u32(header, 0)? != PAYLOAD_MAGIC {
            return Err(TimerPayloadRecordViolation::BadMagic);
        }
        usize::try_from(read_u32(header, 8)?).map_err(|_| TimerPayloadRecordViolation::Truncated)
    }

    pub(crate) const fn header_size() -> usize {
        PAYLOAD_HEADER_SIZE
    }

    pub(crate) fn checksum(bytes: &[u8]) -> Result<u32, TimerPayloadRecordViolation> {
        if bytes.len() < PAYLOAD_TRAILER_SIZE {
            return Err(TimerPayloadRecordViolation::Truncated);
        }
        read_u32(bytes, bytes.len() - PAYLOAD_TRAILER_SIZE)
    }

    /// Returns the UTC day partition derived from the original deadline.
    pub fn due_day_utc(&self) -> Option<i32> {
        self.due_day_utc_checked().ok()
    }

    /// Returns the encoded record length.
    pub fn encoded_len(&self) -> Option<usize> {
        self.encoded_len_checked().ok()
    }

    /// Encodes the record with a trailing CRC32C.
    pub fn encode(&self) -> Option<Vec<u8>> {
        self.encode_checked().ok()
    }

    /// Decodes and verifies a complete payload record.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        Self::decode_checked(bytes).ok()
    }
}

fn validate(record: &TimerPayloadRecordV1) -> Result<(), TimerPayloadRecordViolation> {
    record.due_day_utc_checked()?;
    if record.source_cq_offset.get() < 0 || record.source_physical_offset < 0 {
        return Err(TimerPayloadRecordViolation::InvalidIdentity);
    }
    if record.real_topic.is_empty() {
        return Err(TimerPayloadRecordViolation::EmptyTopic);
    }
    if record.frame.is_empty() {
        return Err(TimerPayloadRecordViolation::EmptyFrame);
    }
    Ok(())
}

fn read_array<const N: usize>(bytes: &[u8], offset: usize) -> Result<[u8; N], TimerPayloadRecordViolation> {
    bytes
        .get(offset..offset.saturating_add(N))
        .and_then(|value| value.try_into().ok())
        .ok_or(TimerPayloadRecordViolation::Truncated)
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, TimerPayloadRecordViolation> {
    Ok(u16::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, TimerPayloadRecordViolation> {
    Ok(u32::from_be_bytes(read_array(bytes, offset)?))
}

fn read_i32(bytes: &[u8], offset: usize) -> Result<i32, TimerPayloadRecordViolation> {
    Ok(i32::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, TimerPayloadRecordViolation> {
    Ok(u64::from_be_bytes(read_array(bytes, offset)?))
}

fn read_i64(bytes: &[u8], offset: usize) -> Result<i64, TimerPayloadRecordViolation> {
    Ok(i64::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u128(bytes: &[u8], offset: usize) -> Result<u128, TimerPayloadRecordViolation> {
    Ok(u128::from_be_bytes(read_array(bytes, offset)?))
}

/// Payload record codec error.
#[derive(Debug, Error)]
pub(crate) enum TimerPayloadRecordViolation {
    /// Original deadline cannot map to a V1 UTC-day partition.
    #[error("invalid timer payload deadline: {0}")]
    InvalidDeadline(i64),
    /// Topic length exceeds V1.
    #[error("timer payload topic is too long: {0}")]
    TopicTooLong(usize),
    /// Complete record length exceeds V1.
    #[error("timer payload record is too large: {0}")]
    RecordTooLarge(usize),
    /// Record header or body is truncated.
    #[error("timer payload record is truncated")]
    Truncated,
    /// Record magic is invalid.
    #[error("timer payload record magic is invalid")]
    BadMagic,
    /// Record version is unknown.
    #[error("unsupported timer payload record version: {0}")]
    UnsupportedVersion(u16),
    /// Header size does not match V1.
    #[error("timer payload header length is invalid")]
    InvalidHeaderLength,
    /// Declared record length differs from bytes read.
    #[error("timer payload record length mismatch: expected={expected}, actual={actual}")]
    InvalidRecordLength {
        /// Declared total length.
        expected: usize,
        /// Actual bytes.
        actual: usize,
    },
    /// CRC32C verification failed.
    #[error("timer payload record checksum mismatch")]
    ChecksumMismatch,
    /// Source identity is invalid.
    #[error("timer payload source identity is invalid")]
    InvalidIdentity,
    /// Real topic is empty.
    #[error("timer payload real topic must not be empty")]
    EmptyTopic,
    /// Complete message frame is empty.
    #[error("timer payload frame must not be empty")]
    EmptyFrame,
    /// Real topic bytes are not valid UTF-8.
    #[error("timer payload topic is not valid UTF-8")]
    InvalidTopicUtf8,
}
