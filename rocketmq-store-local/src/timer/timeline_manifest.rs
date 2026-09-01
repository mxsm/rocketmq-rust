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

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use thiserror::Error;

use crate::timer::storage_format::crc32c;
use crate::timer::timeline_segment::TimelinePartitionKey;
use crate::timer::timeline_segment::TimelineRunDescriptor;
use crate::timer::timeline_segment::TimelineRunKind;

const MANIFEST_MAGIC: u32 = 0x544D_4631;
const MANIFEST_VERSION: u16 = 1;
const MANIFEST_HEADER_SIZE: usize = 16;
const MANIFEST_TRAILER_SIZE: usize = 4;
const CURRENT_A: &str = "CURRENT.A";
const CURRENT_B: &str = "CURRENT.B";
const ARCHIVE_DIRECTORY: &str = "manifests";

/// Persisted native Timeline view. Readers use exactly one published generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimelineManifestV1 {
    /// Monotonic publication generation.
    pub generation: u64,
    /// Next deterministic run id. It advances only after manifest publication.
    pub next_run_id: u64,
    /// Cumulative bytes made durable before the corresponding overlay commit.
    pub durable_end: u64,
    /// Runs reachable from this generation.
    pub active_runs: Vec<TimelineRunDescriptor>,
    /// Files made unreachable by a published merge and awaiting pin-safe deletion.
    pub garbage_runs: Vec<String>,
    /// Snapshot generation to pinned manifest generation.
    pub snapshot_pins: BTreeMap<u64, u64>,
}

impl Default for TimelineManifestV1 {
    fn default() -> Self {
        Self {
            generation: 0,
            next_run_id: 1,
            durable_end: 0,
            active_runs: Vec::new(),
            garbage_runs: Vec::new(),
            snapshot_pins: BTreeMap::new(),
        }
    }
}

impl TimelineManifestV1 {
    /// Loads the newest valid A/B copy. A damaged newest copy falls back to the other copy.
    pub(crate) fn load(root: &Path) -> Result<Self, TimelineManifestFailure> {
        let mut manifests = Vec::with_capacity(2);
        let mut copies_found = 0usize;
        for name in [CURRENT_A, CURRENT_B] {
            match std::fs::read(root.join(name)) {
                Ok(bytes) => {
                    copies_found = copies_found.saturating_add(1);
                    if let Ok(manifest) = Self::decode(&bytes) {
                        manifests.push(manifest);
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        if let Some(manifest) = manifests.into_iter().max_by_key(|manifest| manifest.generation) {
            return Ok(manifest);
        }
        if copies_found == 0 {
            return Ok(Self::default());
        }
        Err(TimelineManifestFailure::NoValidCopy)
    }

    /// Publishes this candidate as the next A/B generation and returns the durable value.
    pub(crate) fn publish_next(&self, root: &Path) -> Result<Self, TimelineManifestFailure> {
        let mut next = self.clone();
        next.generation = self
            .generation
            .checked_add(1)
            .ok_or(TimelineManifestFailure::GenerationExhausted)?;
        next.validate()?;
        std::fs::create_dir_all(root)?;
        let name = if next.generation.is_multiple_of(2) {
            CURRENT_A
        } else {
            CURRENT_B
        };
        let bytes = next.encode()?;
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(root.join(name))?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        Ok(next)
    }

    /// Persists an immutable generation copy used by cross-media snapshots.
    pub(crate) fn archive(&self, root: &Path) -> Result<(), TimelineManifestFailure> {
        if self.generation == 0 {
            return Err(TimelineManifestFailure::InvalidRecord);
        }
        let directory = root.join(ARCHIVE_DIRECTORY);
        std::fs::create_dir_all(&directory)?;
        let path = directory.join(format!("{:020}.manifest", self.generation));
        let bytes = self.encode()?;
        match OpenOptions::new().create_new(true).write(true).open(&path) {
            Ok(mut file) => {
                file.write_all(&bytes)?;
                file.sync_all()?;
                Ok(())
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if Self::decode(&std::fs::read(path)?)? == *self {
                    Ok(())
                } else {
                    Err(TimelineManifestFailure::ArchiveConflict)
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Loads one immutable generation copy.
    pub(crate) fn load_archive(root: &Path, generation: u64) -> Result<Self, TimelineManifestFailure> {
        Self::decode(&std::fs::read(
            root.join(ARCHIVE_DIRECTORY).join(format!("{generation:020}.manifest")),
        )?)
    }

    /// Returns active runs overlapping an exclusive due range.
    pub fn runs_in_range(&self, from_inclusive_ms: i64, to_exclusive_ms: i64) -> Vec<TimelineRunDescriptor> {
        self.active_runs
            .iter()
            .filter(|run| run.max_due_time_ms >= from_inclusive_ms && run.min_due_time_ms < to_exclusive_ms)
            .cloned()
            .collect()
    }

    /// Returns active runs in one physical partition.
    pub fn partition_runs(&self, partition: TimelinePartitionKey) -> Vec<TimelineRunDescriptor> {
        self.active_runs
            .iter()
            .filter(|run| run.partition == partition)
            .cloned()
            .collect()
    }

    /// Stable checksum used to pair this native generation with a RocksDB overlay sequence.
    pub(crate) fn checksum(&self) -> Result<u32, TimelineManifestFailure> {
        let encoded = self.encode()?;
        read_u32(&encoded, encoded.len().saturating_sub(MANIFEST_TRAILER_SIZE))
    }

    fn validate(&self) -> Result<(), TimelineManifestFailure> {
        if self.next_run_id == 0 {
            return Err(TimelineManifestFailure::InvalidRecord);
        }
        let mut run_ids = BTreeMap::new();
        for run in &self.active_runs {
            if run.relative_path.is_empty()
                || run.relative_path.starts_with('/')
                || run.relative_path.contains("..")
                || run.record_count == 0
                || run.logical_bytes == 0
                || run.min_due_time_ms > run.max_due_time_ms
                || run.min_source_cq_offset > run.max_source_cq_offset
                || run_ids.insert(run.run_id, ()).is_some()
            {
                return Err(TimelineManifestFailure::InvalidRecord);
            }
        }
        for path in &self.garbage_runs {
            if path.is_empty() || path.starts_with('/') || path.contains("..") {
                return Err(TimelineManifestFailure::InvalidRecord);
            }
        }
        if self.snapshot_pins.keys().any(|generation| *generation == 0) {
            return Err(TimelineManifestFailure::InvalidRecord);
        }
        Ok(())
    }

    fn encode(&self) -> Result<Vec<u8>, TimelineManifestFailure> {
        self.validate()?;
        let mut body = Vec::new();
        body.extend_from_slice(&self.generation.to_be_bytes());
        body.extend_from_slice(&self.next_run_id.to_be_bytes());
        body.extend_from_slice(&self.durable_end.to_be_bytes());
        body.extend_from_slice(&u32_len(self.active_runs.len())?.to_be_bytes());
        body.extend_from_slice(&u32_len(self.garbage_runs.len())?.to_be_bytes());
        body.extend_from_slice(&u32_len(self.snapshot_pins.len())?.to_be_bytes());
        for run in &self.active_runs {
            encode_run(&mut body, run)?;
        }
        for path in &self.garbage_runs {
            encode_text(&mut body, path)?;
        }
        for (snapshot_generation, manifest_generation) in &self.snapshot_pins {
            body.extend_from_slice(&snapshot_generation.to_be_bytes());
            body.extend_from_slice(&manifest_generation.to_be_bytes());
        }
        let total_len = MANIFEST_HEADER_SIZE
            .checked_add(body.len())
            .and_then(|value| value.checked_add(MANIFEST_TRAILER_SIZE))
            .ok_or(TimelineManifestFailure::LengthOverflow)?;
        let mut output = Vec::with_capacity(total_len);
        output.extend_from_slice(&MANIFEST_MAGIC.to_be_bytes());
        output.extend_from_slice(&MANIFEST_VERSION.to_be_bytes());
        output.extend_from_slice(&(MANIFEST_HEADER_SIZE as u16).to_be_bytes());
        output.extend_from_slice(
            &u64::try_from(total_len)
                .map_err(|_| TimelineManifestFailure::LengthOverflow)?
                .to_be_bytes(),
        );
        output.extend_from_slice(&body);
        let checksum = crc32c(&output);
        output.extend_from_slice(&checksum.to_be_bytes());
        Ok(output)
    }

    fn decode(bytes: &[u8]) -> Result<Self, TimelineManifestFailure> {
        if bytes.len() < MANIFEST_HEADER_SIZE + MANIFEST_TRAILER_SIZE
            || read_u32(bytes, 0)? != MANIFEST_MAGIC
            || read_u16(bytes, 4)? != MANIFEST_VERSION
            || usize::from(read_u16(bytes, 6)?) != MANIFEST_HEADER_SIZE
            || usize::try_from(read_u64(bytes, 8)?).ok() != Some(bytes.len())
            || crc32c(&bytes[..bytes.len() - MANIFEST_TRAILER_SIZE])
                != read_u32(bytes, bytes.len() - MANIFEST_TRAILER_SIZE)?
        {
            return Err(TimelineManifestFailure::InvalidRecord);
        }
        let body_end = bytes.len() - MANIFEST_TRAILER_SIZE;
        let mut cursor = MANIFEST_HEADER_SIZE;
        let generation = take_u64(bytes, &mut cursor, body_end)?;
        let next_run_id = take_u64(bytes, &mut cursor, body_end)?;
        let durable_end = take_u64(bytes, &mut cursor, body_end)?;
        let run_count = take_u32(bytes, &mut cursor, body_end)?;
        let garbage_count = take_u32(bytes, &mut cursor, body_end)?;
        let pin_count = take_u32(bytes, &mut cursor, body_end)?;
        let mut active_runs = Vec::with_capacity(usize::try_from(run_count).unwrap_or(0));
        for _ in 0..run_count {
            active_runs.push(decode_run(bytes, &mut cursor, body_end)?);
        }
        let mut garbage_runs = Vec::with_capacity(usize::try_from(garbage_count).unwrap_or(0));
        for _ in 0..garbage_count {
            garbage_runs.push(take_text(bytes, &mut cursor, body_end)?);
        }
        let mut snapshot_pins = BTreeMap::new();
        for _ in 0..pin_count {
            let snapshot_generation = take_u64(bytes, &mut cursor, body_end)?;
            let manifest_generation = take_u64(bytes, &mut cursor, body_end)?;
            if snapshot_pins.insert(snapshot_generation, manifest_generation).is_some() {
                return Err(TimelineManifestFailure::InvalidRecord);
            }
        }
        if cursor != body_end {
            return Err(TimelineManifestFailure::InvalidRecord);
        }
        let manifest = Self {
            generation,
            next_run_id,
            durable_end,
            active_runs,
            garbage_runs,
            snapshot_pins,
        };
        manifest.validate()?;
        Ok(manifest)
    }
}

fn encode_run(output: &mut Vec<u8>, run: &TimelineRunDescriptor) -> Result<(), TimelineManifestFailure> {
    output.extend_from_slice(&run.partition.due_day_utc.to_be_bytes());
    output.push(run.partition.due_hour_utc);
    output.push(run.kind as u8);
    output.extend_from_slice(&run.partition.lane.to_be_bytes());
    output.extend_from_slice(&run.run_id.to_be_bytes());
    output.extend_from_slice(&run.created_generation.to_be_bytes());
    output.extend_from_slice(&run.record_count.to_be_bytes());
    output.extend_from_slice(&run.min_due_time_ms.to_be_bytes());
    output.extend_from_slice(&run.max_due_time_ms.to_be_bytes());
    output.extend_from_slice(&run.min_source_cq_offset.to_be_bytes());
    output.extend_from_slice(&run.max_source_cq_offset.to_be_bytes());
    output.extend_from_slice(&run.logical_bytes.to_be_bytes());
    output.extend_from_slice(&run.body_checksum.to_be_bytes());
    encode_text(output, &run.relative_path)
}

fn decode_run(bytes: &[u8], cursor: &mut usize, end: usize) -> Result<TimelineRunDescriptor, TimelineManifestFailure> {
    let due_day_utc = take_i32(bytes, cursor, end)?;
    let due_hour_utc = take_u8(bytes, cursor, end)?;
    let kind = match take_u8(bytes, cursor, end)? {
        0 => TimelineRunKind::Base,
        1 => TimelineRunKind::Delta,
        _ => return Err(TimelineManifestFailure::InvalidRecord),
    };
    let lane = take_u16(bytes, cursor, end)?;
    Ok(TimelineRunDescriptor {
        partition: TimelinePartitionKey {
            due_day_utc,
            due_hour_utc,
            lane,
        },
        kind,
        run_id: take_u64(bytes, cursor, end)?,
        created_generation: take_u64(bytes, cursor, end)?,
        record_count: take_u64(bytes, cursor, end)?,
        min_due_time_ms: take_i64(bytes, cursor, end)?,
        max_due_time_ms: take_i64(bytes, cursor, end)?,
        min_source_cq_offset: take_i64(bytes, cursor, end)?,
        max_source_cq_offset: take_i64(bytes, cursor, end)?,
        logical_bytes: take_u64(bytes, cursor, end)?,
        body_checksum: take_u32(bytes, cursor, end)?,
        relative_path: take_text(bytes, cursor, end)?,
    })
}

fn encode_text(output: &mut Vec<u8>, value: &str) -> Result<(), TimelineManifestFailure> {
    let length = u16::try_from(value.len()).map_err(|_| TimelineManifestFailure::LengthOverflow)?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn take_text(bytes: &[u8], cursor: &mut usize, end: usize) -> Result<String, TimelineManifestFailure> {
    let length = usize::from(take_u16(bytes, cursor, end)?);
    let value = take(bytes, cursor, end, length)?;
    std::str::from_utf8(value)
        .map(str::to_owned)
        .map_err(|_| TimelineManifestFailure::InvalidRecord)
}

fn take<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    end: usize,
    length: usize,
) -> Result<&'a [u8], TimelineManifestFailure> {
    let next = cursor
        .checked_add(length)
        .ok_or(TimelineManifestFailure::LengthOverflow)?;
    if next > end {
        return Err(TimelineManifestFailure::InvalidRecord);
    }
    let value = &bytes[*cursor..next];
    *cursor = next;
    Ok(value)
}

fn take_u8(bytes: &[u8], cursor: &mut usize, end: usize) -> Result<u8, TimelineManifestFailure> {
    Ok(take(bytes, cursor, end, 1)?[0])
}

fn take_u16(bytes: &[u8], cursor: &mut usize, end: usize) -> Result<u16, TimelineManifestFailure> {
    Ok(u16::from_be_bytes(
        take(bytes, cursor, end, 2)?
            .try_into()
            .map_err(|_| TimelineManifestFailure::InvalidRecord)?,
    ))
}

fn take_u32(bytes: &[u8], cursor: &mut usize, end: usize) -> Result<u32, TimelineManifestFailure> {
    Ok(u32::from_be_bytes(
        take(bytes, cursor, end, 4)?
            .try_into()
            .map_err(|_| TimelineManifestFailure::InvalidRecord)?,
    ))
}

fn take_i32(bytes: &[u8], cursor: &mut usize, end: usize) -> Result<i32, TimelineManifestFailure> {
    Ok(i32::from_be_bytes(
        take(bytes, cursor, end, 4)?
            .try_into()
            .map_err(|_| TimelineManifestFailure::InvalidRecord)?,
    ))
}

fn take_u64(bytes: &[u8], cursor: &mut usize, end: usize) -> Result<u64, TimelineManifestFailure> {
    Ok(u64::from_be_bytes(
        take(bytes, cursor, end, 8)?
            .try_into()
            .map_err(|_| TimelineManifestFailure::InvalidRecord)?,
    ))
}

fn take_i64(bytes: &[u8], cursor: &mut usize, end: usize) -> Result<i64, TimelineManifestFailure> {
    Ok(i64::from_be_bytes(
        take(bytes, cursor, end, 8)?
            .try_into()
            .map_err(|_| TimelineManifestFailure::InvalidRecord)?,
    ))
}

fn u32_len(value: usize) -> Result<u32, TimelineManifestFailure> {
    u32::try_from(value).map_err(|_| TimelineManifestFailure::LengthOverflow)
}

fn read_array<const N: usize>(bytes: &[u8], offset: usize) -> Result<[u8; N], TimelineManifestFailure> {
    bytes
        .get(offset..offset.saturating_add(N))
        .and_then(|value| value.try_into().ok())
        .ok_or(TimelineManifestFailure::InvalidRecord)
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, TimelineManifestFailure> {
    Ok(u16::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, TimelineManifestFailure> {
    Ok(u32::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, TimelineManifestFailure> {
    Ok(u64::from_be_bytes(read_array(bytes, offset)?))
}

/// A/B native Timeline manifest failure.
#[derive(Debug, Error)]
pub(crate) enum TimelineManifestFailure {
    /// Underlying filesystem operation failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// No A/B copy has a valid version, length, and checksum.
    #[error("no valid native Timeline manifest copy")]
    NoValidCopy,
    /// Manifest fields or encoded bytes are invalid.
    #[error("invalid native Timeline manifest")]
    InvalidRecord,
    /// Manifest generation cannot advance.
    #[error("native Timeline manifest generation exhausted")]
    GenerationExhausted,
    /// Variable metadata cannot fit the V1 format.
    #[error("native Timeline manifest length overflow")]
    LengthOverflow,
    /// Existing immutable archive differs from the requested generation.
    #[error("native Timeline manifest archive conflict")]
    ArchiveConflict,
}
