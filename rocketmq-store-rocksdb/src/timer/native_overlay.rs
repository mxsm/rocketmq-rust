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

use std::sync::Arc;

use rocketmq_error::RocketMQError;
use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::TimerSourceCqOffset;

use crate::batch::RocksDbWriteBatch;
use crate::error::codec_error;
use crate::store::KeyValueStore;
use crate::store::RocksDbStore;
use crate::timer::codec::crc32c;
use crate::timer::codec::TimelineKeyV1;
use crate::timer::codec::TimelineRecordV1;
use crate::timer::timeline_index::TimelineIndexEntry;
use crate::timer::NATIVE_LOCATOR_CF;
use crate::timer::NATIVE_MATERIALIZED_CF;
use crate::timer::NATIVE_META_CF;

const NATIVE_KEY_VERSION: u8 = 1;
const NATIVE_LOCATOR_KEY_SIZE: usize = 25;
const NATIVE_LOCATOR_VALUE_SIZE: usize = 2 + 35 + 66 + 8 + 8 + 8 + 4 + 4;
const NATIVE_MARKER_KEY_SIZE: usize = 9;
const NATIVE_MARKER_VALUE_SIZE: usize = 2 + 16 + 8 + 8 + 8 + 8 + 4 + 4;
const NATIVE_CHECKPOINT_VALUE_SIZE: usize = 2 + 8 + 8 + 4 + 8 + 8 + 4;
const NATIVE_CHECKPOINT_KEY: &[u8] = b"native-checkpoint-v1";

/// Native run proof stored beside a direct payload locator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeDurabilityV1 {
    /// Published native manifest generation.
    pub manifest_generation: u64,
    /// Monotonic cumulative native durable bytes.
    pub durable_end: u64,
    /// Stable logical source hash.
    pub record_hash: u64,
    /// CRC32C of the paired native manifest.
    pub manifest_checksum: u32,
}

impl NativeDurabilityV1 {
    /// Rejects future, incomplete, or mismatched native references.
    pub fn validate_against(self, checkpoint: NativeOverlayCheckpointV1) -> Result<(), RocketMQError> {
        if self.manifest_generation == 0
            || self.manifest_generation > checkpoint.manifest_generation
            || self.durable_end == 0
            || self.durable_end > checkpoint.durable_end
            || self.record_hash == 0
            || self.manifest_checksum == 0
        {
            return Err(codec_error(
                "native Timeline locator is ahead of its durable overlay checkpoint",
            ));
        }
        Ok(())
    }
}

/// Direct native record lookup used by delivery and reconciliation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeTimelineLocatorV1 {
    /// Native Timeline entry.
    pub entry: TimelineIndexEntry,
    /// Cross-media durability proof.
    pub durability: NativeDurabilityV1,
}

/// Per-source marker proving that RocksDB references a synced native generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeMaterializedMarkerV1 {
    /// Stable logical timer id.
    pub timer_id: TimerId,
    /// Materialized generation.
    pub generation: TimerGeneration,
    /// Cross-media durability proof.
    pub durability: NativeDurabilityV1,
}

/// Global native/overlay checkpoint. It is committed in the same WriteBatch as the contiguous
/// source checkpoint, after the native run and manifest are synced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeOverlayCheckpointV1 {
    /// Native manifest generation visible to the overlay.
    pub manifest_generation: u64,
    /// Native durable end visible to the overlay.
    pub durable_end: u64,
    /// Native manifest checksum.
    pub manifest_checksum: u32,
    /// Last contiguous source CQ offset represented by both media.
    pub materialized_source_offset: TimerSourceCqOffset,
    /// Monotonic cross-media checkpoint generation.
    pub generation: u64,
}

/// Typed access to the small RocksDB overlay for the native Timeline owner.
#[derive(Clone)]
pub struct RocksDbNativeTimelineOverlay {
    store: Arc<RocksDbStore>,
}

impl RocksDbNativeTimelineOverlay {
    /// Creates a view over the dedicated Extended Timeline database.
    pub fn new(store: Arc<RocksDbStore>) -> Self {
        Self { store }
    }

    /// Appends a direct locator to an existing sync-WAL WriteBatch.
    pub fn append_locator(
        batch: &mut RocksDbWriteBatch,
        locator: NativeTimelineLocatorV1,
    ) -> Result<(), RocketMQError> {
        batch.put_cf(
            NATIVE_LOCATOR_CF,
            encode_locator_key(locator.entry.key.timer_id, locator.entry.key.generation),
            encode_locator(locator),
        );
        Ok(())
    }

    /// Reads and verifies one direct locator.
    pub fn get(
        &self,
        timer_id: TimerId,
        generation: TimerGeneration,
    ) -> Result<Option<NativeTimelineLocatorV1>, RocketMQError> {
        self.store
            .get_cf(NATIVE_LOCATOR_CF, &encode_locator_key(timer_id, generation))?
            .map(|value| {
                let locator = decode_locator(&value)?;
                if locator.entry.key.timer_id != timer_id || locator.entry.key.generation != generation {
                    return Err(codec_error("native Timeline locator key mismatch"));
                }
                Ok(locator)
            })
            .transpose()
    }

    /// Reads a bounded locator page with one RocksDB multi-get.
    pub fn get_many(
        &self,
        keys: &[(TimerId, TimerGeneration)],
    ) -> Result<Vec<Option<NativeTimelineLocatorV1>>, RocketMQError> {
        let encoded = keys
            .iter()
            .map(|(timer_id, generation)| encode_locator_key(*timer_id, *generation).to_vec())
            .collect::<Vec<_>>();
        self.store
            .multi_get_cf(NATIVE_LOCATOR_CF, &encoded)?
            .into_iter()
            .zip(keys)
            .map(|(value, (timer_id, generation))| {
                value
                    .map(|value| {
                        let locator = decode_locator(&value)?;
                        if locator.entry.key.timer_id != *timer_id || locator.entry.key.generation != *generation {
                            return Err(codec_error("native Timeline locator key mismatch"));
                        }
                        Ok(locator)
                    })
                    .transpose()
            })
            .collect()
    }

    /// Appends a source marker to the atomic overlay batch.
    pub fn append_materialized_marker(
        batch: &mut RocksDbWriteBatch,
        source_offset: TimerSourceCqOffset,
        marker: NativeMaterializedMarkerV1,
    ) -> Result<(), RocketMQError> {
        if source_offset.get() < 0 {
            return Err(codec_error("native Timeline source marker offset is negative"));
        }
        batch.put_cf(
            NATIVE_MATERIALIZED_CF,
            encode_marker_key(source_offset),
            encode_marker(marker),
        );
        Ok(())
    }

    /// Reads one source marker for orphan reconciliation.
    pub fn materialized_marker(
        &self,
        source_offset: TimerSourceCqOffset,
    ) -> Result<Option<NativeMaterializedMarkerV1>, RocketMQError> {
        self.store
            .get_cf(NATIVE_MATERIALIZED_CF, &encode_marker_key(source_offset))?
            .map(|value| decode_marker(&value))
            .transpose()
    }

    /// Appends the native checkpoint to the same batch as state/lookup and source checkpoint.
    pub fn append_checkpoint(batch: &mut RocksDbWriteBatch, checkpoint: NativeOverlayCheckpointV1) {
        batch.put_cf(
            NATIVE_META_CF,
            NATIVE_CHECKPOINT_KEY.to_vec(),
            encode_checkpoint(checkpoint),
        );
    }

    /// Reads the durable cross-media checkpoint.
    pub fn checkpoint(&self) -> Result<Option<NativeOverlayCheckpointV1>, RocketMQError> {
        self.store
            .get_cf(NATIVE_META_CF, NATIVE_CHECKPOINT_KEY)?
            .map(|value| decode_checkpoint(&value))
            .transpose()
    }

    /// Appends locator/marker cleanup to a terminal GC batch.
    pub fn append_delete(
        batch: &mut RocksDbWriteBatch,
        timer_id: TimerId,
        generation: TimerGeneration,
        source_offset: TimerSourceCqOffset,
    ) {
        batch.delete_cf(NATIVE_LOCATOR_CF, encode_locator_key(timer_id, generation));
        batch.delete_cf(NATIVE_MATERIALIZED_CF, encode_marker_key(source_offset));
    }
}

fn encode_locator_key(timer_id: TimerId, generation: TimerGeneration) -> [u8; NATIVE_LOCATOR_KEY_SIZE] {
    let mut output = [0u8; NATIVE_LOCATOR_KEY_SIZE];
    output[0] = NATIVE_KEY_VERSION;
    output[1..17].copy_from_slice(&timer_id.get().to_be_bytes());
    output[17..25].copy_from_slice(&generation.get().to_be_bytes());
    output
}

fn encode_locator(locator: NativeTimelineLocatorV1) -> [u8; NATIVE_LOCATOR_VALUE_SIZE] {
    let mut output = [0u8; NATIVE_LOCATOR_VALUE_SIZE];
    output[0..2].copy_from_slice(&1u16.to_be_bytes());
    output[2..37].copy_from_slice(&locator.entry.key.encode());
    output[37..103].copy_from_slice(&locator.entry.record.encode());
    output[103..111].copy_from_slice(&locator.durability.manifest_generation.to_be_bytes());
    output[111..119].copy_from_slice(&locator.durability.durable_end.to_be_bytes());
    output[119..127].copy_from_slice(&locator.durability.record_hash.to_be_bytes());
    output[127..131].copy_from_slice(&locator.durability.manifest_checksum.to_be_bytes());
    let checksum = crc32c(&output[..NATIVE_LOCATOR_VALUE_SIZE - 4]);
    output[NATIVE_LOCATOR_VALUE_SIZE - 4..].copy_from_slice(&checksum.to_be_bytes());
    output
}

fn decode_locator(bytes: &[u8]) -> Result<NativeTimelineLocatorV1, RocketMQError> {
    if bytes.len() != NATIVE_LOCATOR_VALUE_SIZE
        || read_u16(bytes, 0)? != 1
        || crc32c(&bytes[..NATIVE_LOCATOR_VALUE_SIZE - 4]) != read_u32(bytes, NATIVE_LOCATOR_VALUE_SIZE - 4)?
    {
        return Err(codec_error("invalid native Timeline locator"));
    }
    Ok(NativeTimelineLocatorV1 {
        entry: TimelineIndexEntry {
            key: TimelineKeyV1::decode(&bytes[2..37])?,
            record: TimelineRecordV1::decode(&bytes[37..103])?,
        },
        durability: NativeDurabilityV1 {
            manifest_generation: read_u64(bytes, 103)?,
            durable_end: read_u64(bytes, 111)?,
            record_hash: read_u64(bytes, 119)?,
            manifest_checksum: read_u32(bytes, 127)?,
        },
    })
}

fn encode_marker_key(source_offset: TimerSourceCqOffset) -> [u8; NATIVE_MARKER_KEY_SIZE] {
    let mut output = [0u8; NATIVE_MARKER_KEY_SIZE];
    output[0] = NATIVE_KEY_VERSION;
    output[1..9].copy_from_slice(&source_offset.get().to_be_bytes());
    output
}

fn encode_marker(marker: NativeMaterializedMarkerV1) -> [u8; NATIVE_MARKER_VALUE_SIZE] {
    let mut output = [0u8; NATIVE_MARKER_VALUE_SIZE];
    output[0..2].copy_from_slice(&1u16.to_be_bytes());
    output[2..18].copy_from_slice(&marker.timer_id.get().to_be_bytes());
    output[18..26].copy_from_slice(&marker.generation.get().to_be_bytes());
    output[26..34].copy_from_slice(&marker.durability.manifest_generation.to_be_bytes());
    output[34..42].copy_from_slice(&marker.durability.durable_end.to_be_bytes());
    output[42..50].copy_from_slice(&marker.durability.record_hash.to_be_bytes());
    output[50..54].copy_from_slice(&marker.durability.manifest_checksum.to_be_bytes());
    let checksum = crc32c(&output[..NATIVE_MARKER_VALUE_SIZE - 4]);
    output[NATIVE_MARKER_VALUE_SIZE - 4..].copy_from_slice(&checksum.to_be_bytes());
    output
}

fn decode_marker(bytes: &[u8]) -> Result<NativeMaterializedMarkerV1, RocketMQError> {
    if bytes.len() != NATIVE_MARKER_VALUE_SIZE
        || read_u16(bytes, 0)? != 1
        || crc32c(&bytes[..NATIVE_MARKER_VALUE_SIZE - 4]) != read_u32(bytes, NATIVE_MARKER_VALUE_SIZE - 4)?
    {
        return Err(codec_error("invalid native Timeline materialized marker"));
    }
    Ok(NativeMaterializedMarkerV1 {
        timer_id: TimerId::new(read_u128(bytes, 2)?),
        generation: TimerGeneration::new(read_u64(bytes, 18)?),
        durability: NativeDurabilityV1 {
            manifest_generation: read_u64(bytes, 26)?,
            durable_end: read_u64(bytes, 34)?,
            record_hash: read_u64(bytes, 42)?,
            manifest_checksum: read_u32(bytes, 50)?,
        },
    })
}

fn encode_checkpoint(checkpoint: NativeOverlayCheckpointV1) -> [u8; NATIVE_CHECKPOINT_VALUE_SIZE] {
    let mut output = [0u8; NATIVE_CHECKPOINT_VALUE_SIZE];
    output[0..2].copy_from_slice(&1u16.to_be_bytes());
    output[2..10].copy_from_slice(&checkpoint.manifest_generation.to_be_bytes());
    output[10..18].copy_from_slice(&checkpoint.durable_end.to_be_bytes());
    output[18..22].copy_from_slice(&checkpoint.manifest_checksum.to_be_bytes());
    output[22..30].copy_from_slice(&checkpoint.materialized_source_offset.get().to_be_bytes());
    output[30..38].copy_from_slice(&checkpoint.generation.to_be_bytes());
    let checksum = crc32c(&output[..NATIVE_CHECKPOINT_VALUE_SIZE - 4]);
    output[NATIVE_CHECKPOINT_VALUE_SIZE - 4..].copy_from_slice(&checksum.to_be_bytes());
    output
}

fn decode_checkpoint(bytes: &[u8]) -> Result<NativeOverlayCheckpointV1, RocketMQError> {
    if bytes.len() != NATIVE_CHECKPOINT_VALUE_SIZE
        || read_u16(bytes, 0)? != 1
        || crc32c(&bytes[..NATIVE_CHECKPOINT_VALUE_SIZE - 4]) != read_u32(bytes, NATIVE_CHECKPOINT_VALUE_SIZE - 4)?
    {
        return Err(codec_error("invalid native Timeline overlay checkpoint"));
    }
    let checkpoint = NativeOverlayCheckpointV1 {
        manifest_generation: read_u64(bytes, 2)?,
        durable_end: read_u64(bytes, 10)?,
        manifest_checksum: read_u32(bytes, 18)?,
        materialized_source_offset: TimerSourceCqOffset::new(read_i64(bytes, 22)?),
        generation: read_u64(bytes, 30)?,
    };
    if checkpoint.manifest_generation == 0
        || checkpoint.durable_end == 0
        || checkpoint.manifest_checksum == 0
        || checkpoint.materialized_source_offset.get() < -1
        || checkpoint.generation == 0
    {
        return Err(codec_error("invalid native Timeline overlay checkpoint fields"));
    }
    Ok(checkpoint)
}

fn read_array<const N: usize>(bytes: &[u8], offset: usize) -> Result<[u8; N], RocketMQError> {
    bytes
        .get(offset..offset.saturating_add(N))
        .and_then(|value| value.try_into().ok())
        .ok_or_else(|| codec_error("truncated native Timeline overlay record"))
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, RocketMQError> {
    Ok(u16::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, RocketMQError> {
    Ok(u32::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, RocketMQError> {
    Ok(u64::from_be_bytes(read_array(bytes, offset)?))
}

fn read_i64(bytes: &[u8], offset: usize) -> Result<i64, RocketMQError> {
    Ok(i64::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u128(bytes: &[u8], offset: usize) -> Result<u128, RocketMQError> {
    Ok(u128::from_be_bytes(read_array(bytes, offset)?))
}
