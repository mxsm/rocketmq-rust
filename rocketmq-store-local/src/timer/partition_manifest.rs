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

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use thiserror::Error;

use crate::timer::storage_format::crc32c;

const MANIFEST_MAGIC: u32 = 0x5450_4D31;
const LEGACY_MANIFEST_VERSION: u16 = 1;
const LEGACY_MANIFEST_SIZE: usize = 60;
const MANIFEST_VERSION: u16 = 2;
const MANIFEST_SIZE: usize = 68;
const MANIFEST_A: &str = "manifest.a";
const MANIFEST_B: &str = "manifest.b";

/// Physical payload partition key: original UTC due day and stable lane.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TimerPayloadPartitionKey {
    /// UTC day number since Unix epoch.
    pub due_day_utc: i32,
    /// Stable delivery lane.
    pub lane: u16,
}

/// Lifecycle state of one due-day/lane payload partition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum TimerPayloadPartitionState {
    /// New records may be appended.
    Open = 0,
    /// No new record may be appended.
    Sealed = 1,
    /// Timeline/state/snapshot/replication fences permit GC.
    GcEligible = 2,
    /// Partition files were deleted.
    Deleted = 3,
}

impl TimerPayloadPartitionState {
    fn decode(value: u8) -> Result<Self, PartitionManifestFailure> {
        match value {
            0 => Ok(Self::Open),
            1 => Ok(Self::Sealed),
            2 => Ok(Self::GcEligible),
            3 => Ok(Self::Deleted),
            _ => Err(PartitionManifestFailure::InvalidState(value)),
        }
    }
}

/// Checksummed A/B manifest for one payload partition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimerPayloadPartitionManifest {
    /// Partition identity.
    pub key: TimerPayloadPartitionKey,
    /// Lifecycle state.
    pub state: TimerPayloadPartitionState,
    /// Monotonic manifest/snapshot generation.
    pub generation: u64,
    /// Current append segment.
    pub active_segment_id: u64,
    /// Durable length in the current segment.
    pub active_segment_len: u64,
    /// Total durable record count.
    pub record_count: u64,
    /// Total durable encoded bytes.
    pub live_bytes: u64,
    /// Oldest active snapshot generation pin, or zero when this partition is unpinned.
    pub snapshot_pin_generation: u64,
}

impl TimerPayloadPartitionManifest {
    /// Creates an empty open partition manifest.
    pub const fn empty(key: TimerPayloadPartitionKey) -> Self {
        Self {
            key,
            state: TimerPayloadPartitionState::Open,
            generation: 0,
            active_segment_id: 0,
            active_segment_len: 0,
            record_count: 0,
            live_bytes: 0,
            snapshot_pin_generation: 0,
        }
    }

    /// Loads the newest valid A/B copy, or returns an empty manifest when neither exists.
    pub(crate) fn load(directory: &Path, key: TimerPayloadPartitionKey) -> Result<Self, PartitionManifestFailure> {
        let mut candidates = Vec::with_capacity(2);
        for name in [MANIFEST_A, MANIFEST_B] {
            match std::fs::read(directory.join(name)) {
                Ok(bytes) => match Self::decode(&bytes) {
                    Ok(manifest) if manifest.key == key => candidates.push(manifest),
                    Ok(_) => return Err(PartitionManifestFailure::PartitionMismatch),
                    Err(_) => continue,
                },
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(candidates
            .into_iter()
            .max_by_key(|manifest| manifest.generation)
            .unwrap_or_else(|| Self::empty(key)))
    }

    /// Persists the next generation to the alternate manifest copy and synchronizes it.
    pub(crate) fn persist(&mut self, directory: &Path) -> Result<(), PartitionManifestFailure> {
        std::fs::create_dir_all(directory)?;
        self.generation = self.generation.saturating_add(1);
        let name = if self.generation.is_multiple_of(2) {
            MANIFEST_A
        } else {
            MANIFEST_B
        };
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(directory.join(name))?;
        file.write_all(&self.encode())?;
        file.sync_data()?;
        Ok(())
    }

    fn encode(self) -> [u8; MANIFEST_SIZE] {
        let mut output = [0u8; MANIFEST_SIZE];
        output[0..4].copy_from_slice(&MANIFEST_MAGIC.to_be_bytes());
        output[4..6].copy_from_slice(&MANIFEST_VERSION.to_be_bytes());
        output[6..8].copy_from_slice(&(MANIFEST_SIZE as u16).to_be_bytes());
        output[8..12].copy_from_slice(&self.key.due_day_utc.to_be_bytes());
        output[12..14].copy_from_slice(&self.key.lane.to_be_bytes());
        output[14] = self.state as u8;
        output[16..24].copy_from_slice(&self.generation.to_be_bytes());
        output[24..32].copy_from_slice(&self.active_segment_id.to_be_bytes());
        output[32..40].copy_from_slice(&self.active_segment_len.to_be_bytes());
        output[40..48].copy_from_slice(&self.record_count.to_be_bytes());
        output[48..56].copy_from_slice(&self.live_bytes.to_be_bytes());
        output[56..64].copy_from_slice(&self.snapshot_pin_generation.to_be_bytes());
        let checksum = crc32c(&output[..64]);
        output[64..68].copy_from_slice(&checksum.to_be_bytes());
        output
    }

    fn decode(bytes: &[u8]) -> Result<Self, PartitionManifestFailure> {
        let version = read_u16(bytes, 4)?;
        let legacy = version == LEGACY_MANIFEST_VERSION && bytes.len() == LEGACY_MANIFEST_SIZE;
        let current = version == MANIFEST_VERSION && bytes.len() == MANIFEST_SIZE;
        let checksum_offset = if current { 64 } else { 56 };
        if (!legacy && !current)
            || read_u32(bytes, 0)? != MANIFEST_MAGIC
            || usize::from(read_u16(bytes, 6)?) != bytes.len()
            || crc32c(&bytes[..checksum_offset]) != read_u32(bytes, checksum_offset)?
        {
            return Err(PartitionManifestFailure::InvalidRecord);
        }
        Ok(Self {
            key: TimerPayloadPartitionKey {
                due_day_utc: read_i32(bytes, 8)?,
                lane: read_u16(bytes, 12)?,
            },
            state: TimerPayloadPartitionState::decode(bytes[14])?,
            generation: read_u64(bytes, 16)?,
            active_segment_id: read_u64(bytes, 24)?,
            active_segment_len: read_u64(bytes, 32)?,
            record_count: read_u64(bytes, 40)?,
            live_bytes: read_u64(bytes, 48)?,
            snapshot_pin_generation: if current { read_u64(bytes, 56)? } else { 0 },
        })
    }
}

fn read_array<const N: usize>(bytes: &[u8], offset: usize) -> Result<[u8; N], PartitionManifestFailure> {
    bytes
        .get(offset..offset.saturating_add(N))
        .and_then(|value| value.try_into().ok())
        .ok_or(PartitionManifestFailure::InvalidRecord)
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, PartitionManifestFailure> {
    Ok(u16::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, PartitionManifestFailure> {
    Ok(u32::from_be_bytes(read_array(bytes, offset)?))
}

fn read_i32(bytes: &[u8], offset: usize) -> Result<i32, PartitionManifestFailure> {
    Ok(i32::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, PartitionManifestFailure> {
    Ok(u64::from_be_bytes(read_array(bytes, offset)?))
}

/// Partition manifest error.
#[derive(Debug, Error)]
pub(crate) enum PartitionManifestFailure {
    /// Underlying filesystem operation failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Both shape and checksum must be valid.
    #[error("invalid timer payload partition manifest")]
    InvalidRecord,
    /// Manifest belongs to another partition directory.
    #[error("timer payload partition manifest key mismatch")]
    PartitionMismatch,
    /// Unknown lifecycle state.
    #[error("invalid timer payload partition state: {0}")]
    InvalidState(u8),
}
