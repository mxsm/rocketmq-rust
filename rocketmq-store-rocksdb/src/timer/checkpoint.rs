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
use rocketmq_store_api::TimerSourceCqOffset;
use rocketmq_store_api::TimerTimelineCursor;
use rocketmq_store_api::EXTENDED_TIMELINE_FORMAT_VERSION;

use crate::error::codec_corrupted;
use crate::error::state_corrupted_source;
use crate::timer::codec::crc32c;

const CHECKPOINT_KEY_VERSION: u8 = 1;
const CHECKPOINT_KEY_SIZE: usize = 4;
const CHECKPOINT_VALUE_SIZE: usize = 62;

/// Logical checkpoint namespace.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum TimelineCheckpointKind {
    /// Contiguous Timer ConsumeQueue materialization prefix.
    MaterializedSource = 0,
    /// Per-lane due scan cursor.
    Due = 1,
    /// Per-lane final completion cursor.
    Completion = 2,
}

impl TimelineCheckpointKind {
    fn decode(operation: StoreOperation, value: u8) -> Result<Self, StoreError> {
        match value {
            0 => Ok(Self::MaterializedSource),
            1 => Ok(Self::Due),
            2 => Ok(Self::Completion),
            _ => Err(codec_corrupted(operation)),
        }
    }
}

/// Durable source, due, and completion watermarks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimelineCheckpointV1 {
    /// Last contiguous materialized Timer CQ offset, or `-1` before the first record.
    pub materialized_source_offset: TimerSourceCqOffset,
    /// Last durable due scan position.
    pub due_cursor: TimerTimelineCursor,
    /// Last durable final-completion position.
    pub completion_cursor: TimerTimelineCursor,
    /// Immutable schema/configuration fingerprint.
    pub format_fingerprint: u64,
    /// Monotonic checkpoint generation.
    pub generation: u64,
}

impl TimelineCheckpointV1 {
    /// Encodes a checkpoint with CRC32C.
    pub fn encode(self) -> [u8; CHECKPOINT_VALUE_SIZE] {
        let mut output = [0u8; CHECKPOINT_VALUE_SIZE];
        output[0..2].copy_from_slice(&EXTENDED_TIMELINE_FORMAT_VERSION.to_be_bytes());
        output[2..10].copy_from_slice(&self.materialized_source_offset.get().to_be_bytes());
        output[10..18].copy_from_slice(&self.due_cursor.due_time_ms().to_be_bytes());
        output[18..26].copy_from_slice(&self.due_cursor.sequence().to_be_bytes());
        output[26..34].copy_from_slice(&self.completion_cursor.due_time_ms().to_be_bytes());
        output[34..42].copy_from_slice(&self.completion_cursor.sequence().to_be_bytes());
        output[42..50].copy_from_slice(&self.format_fingerprint.to_be_bytes());
        output[50..58].copy_from_slice(&self.generation.to_be_bytes());
        let checksum = crc32c(&output[..CHECKPOINT_VALUE_SIZE - 4]);
        output[CHECKPOINT_VALUE_SIZE - 4..].copy_from_slice(&checksum.to_be_bytes());
        output
    }

    /// Decodes and verifies a checkpoint.
    pub fn decode(operation: StoreOperation, bytes: &[u8]) -> Result<Self, StoreError> {
        if bytes.len() != CHECKPOINT_VALUE_SIZE
            || read_u16(operation, bytes, 0)? != EXTENDED_TIMELINE_FORMAT_VERSION
            || crc32c(&bytes[..CHECKPOINT_VALUE_SIZE - 4]) != read_u32(operation, bytes, CHECKPOINT_VALUE_SIZE - 4)?
        {
            return Err(codec_corrupted(operation));
        }
        let materialized_source_offset = read_i64(operation, bytes, 2)?;
        if materialized_source_offset < -1 {
            return Err(codec_corrupted(operation));
        }
        Ok(Self {
            materialized_source_offset: TimerSourceCqOffset::new(materialized_source_offset),
            due_cursor: TimerTimelineCursor::new(read_i64(operation, bytes, 10)?, read_u64(operation, bytes, 18)?),
            completion_cursor: TimerTimelineCursor::new(
                read_i64(operation, bytes, 26)?,
                read_u64(operation, bytes, 34)?,
            ),
            format_fingerprint: read_u64(operation, bytes, 42)?,
            generation: read_u64(operation, bytes, 50)?,
        })
    }
}

/// Encodes a checkpoint key. Lane zero is reserved for the global source checkpoint.
pub fn encode_checkpoint_key(kind: TimelineCheckpointKind, lane: u16) -> [u8; CHECKPOINT_KEY_SIZE] {
    let mut output = [0u8; CHECKPOINT_KEY_SIZE];
    output[0] = CHECKPOINT_KEY_VERSION;
    output[1] = kind as u8;
    output[2..4].copy_from_slice(&lane.to_be_bytes());
    output
}

/// Decodes a checkpoint key.
pub fn decode_checkpoint_key(
    operation: StoreOperation,
    bytes: &[u8],
) -> Result<(TimelineCheckpointKind, u16), StoreError> {
    if bytes.len() != CHECKPOINT_KEY_SIZE || bytes[0] != CHECKPOINT_KEY_VERSION {
        return Err(codec_corrupted(operation));
    }
    Ok((
        TimelineCheckpointKind::decode(operation, bytes[1])?,
        read_u16(operation, bytes, 2)?,
    ))
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

fn read_u64(operation: StoreOperation, bytes: &[u8], offset: usize) -> Result<u64, StoreError> {
    Ok(u64::from_be_bytes(read_array(operation, bytes, offset)?))
}

fn read_i64(operation: StoreOperation, bytes: &[u8], offset: usize) -> Result<i64, StoreError> {
    Ok(i64::from_be_bytes(read_array(operation, bytes, offset)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_codec_round_trips() {
        let checkpoint = TimelineCheckpointV1 {
            materialized_source_offset: TimerSourceCqOffset::new(9),
            due_cursor: TimerTimelineCursor::new(11, 13),
            completion_cursor: TimerTimelineCursor::new(17, 19),
            format_fingerprint: 23,
            generation: 29,
        };
        assert_eq!(
            TimelineCheckpointV1::decode(StoreOperation::Read, &checkpoint.encode()).expect("decode"),
            checkpoint
        );
    }
}
