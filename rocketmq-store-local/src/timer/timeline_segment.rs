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
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;

use rocketmq_store_api::TimerEngineId;
use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::TimerPayloadStoreLocator;
use rocketmq_store_api::TimerSourceCqOffset;
use thiserror::Error;

use crate::timer::storage_format::crc32c;

const RUN_MAGIC: u32 = 0x5452_4E31;
const RUN_FOOTER_MAGIC: u32 = 0x5452_4631;
const RUN_SEALED_MARKER: u64 = 0x5345_414C_4544_5631;
const RUN_VERSION: u16 = 1;
const RUN_HEADER_SIZE: usize = 144;
const RUN_RECORD_SIZE: usize = 100;
const RUN_FOOTER_SIZE: usize = 40;

/// Physical partition for one hour and delivery lane.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TimelinePartitionKey {
    /// UTC day number since the Unix epoch.
    pub due_day_utc: i32,
    /// UTC hour within `due_day_utc`.
    pub due_hour_utc: u8,
    /// Stable delivery lane.
    pub lane: u16,
}

impl TimelinePartitionKey {
    /// Derives the physical partition from a deadline and lane.
    /// Reports failures through the canonical storage facade.
    pub fn from_deadline(due_time_ms: i64, lane: u16) -> Result<Self, StoreError> {
        Self::from_deadline_typed(due_time_ms, lane).map_err(|error| error.into_store_error(StoreOperation::Read))
    }

    fn from_deadline_typed(due_time_ms: i64, lane: u16) -> Result<Self, TimelineSegmentError> {
        if due_time_ms < 0 {
            return Err(TimelineSegmentError::InvalidDeadline(due_time_ms));
        }
        let due_day_utc = i32::try_from(due_time_ms.div_euclid(86_400_000))
            .map_err(|_| TimelineSegmentError::InvalidDeadline(due_time_ms))?;
        let due_hour_utc = u8::try_from(due_time_ms.rem_euclid(86_400_000).div_euclid(3_600_000))
            .map_err(|_| TimelineSegmentError::InvalidDeadline(due_time_ms))?;
        Ok(Self {
            due_day_utc,
            due_hour_utc,
            lane,
        })
    }

    /// Returns whether the deadline belongs to this partition.
    pub fn contains(self, due_time_ms: i64) -> bool {
        Self::from_deadline(due_time_ms, self.lane).is_ok_and(|candidate| candidate == self)
    }
}

/// Full ordering key used by every native Timeline run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TimelineSegmentKey {
    /// Exact original delivery deadline.
    pub due_time_ms: i64,
    /// Stable lane.
    pub lane: u16,
    /// Logical timer id.
    pub timer_id: TimerId,
    /// Generation fencing stale work.
    pub generation: TimerGeneration,
}

impl TimelineSegmentKey {
    /// Returns this key's physical partition.
    pub(crate) fn partition(self) -> Result<TimelinePartitionKey, TimelineSegmentError> {
        TimelinePartitionKey::from_deadline_typed(self.due_time_ms, self.lane)
    }
}

/// Fixed-size native Timeline record. The complete payload remains in `TimerPayloadStore`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimelineSegmentRecord {
    /// Fully ordered Timeline key.
    pub key: TimelineSegmentKey,
    /// Independent long-horizon payload location.
    pub payload: TimerPayloadStoreLocator,
    /// Source Timer ConsumeQueue offset.
    pub source_cq_offset: TimerSourceCqOffset,
    /// Original CommitLog position, retained as replay identity.
    pub source_physical_offset: i64,
    /// Original CommitLog frame size.
    pub source_size: u32,
    /// State version observed during materialization.
    pub state_version: u64,
    /// Persisted owner engine.
    pub owner_engine: TimerEngineId,
    /// Whether this is a non-delivering Java-compatible observation.
    pub shadow_only: bool,
}

impl TimelineSegmentRecord {
    /// Returns the fixed V1 encoded size.
    pub const fn encoded_size() -> usize {
        RUN_RECORD_SIZE
    }

    fn encode(self) -> Result<[u8; RUN_RECORD_SIZE], TimelineSegmentError> {
        validate_record(self)?;
        let mut output = [0u8; RUN_RECORD_SIZE];
        output[0..8].copy_from_slice(&self.key.due_time_ms.to_be_bytes());
        output[8..10].copy_from_slice(&self.key.lane.to_be_bytes());
        output[10..26].copy_from_slice(&self.key.timer_id.get().to_be_bytes());
        output[26..34].copy_from_slice(&self.key.generation.get().to_be_bytes());
        output[34..38].copy_from_slice(&self.payload.due_day_utc().to_be_bytes());
        output[38..40].copy_from_slice(&self.payload.lane().to_be_bytes());
        output[40..48].copy_from_slice(&self.payload.segment_id().to_be_bytes());
        output[48..56].copy_from_slice(&self.payload.offset().to_be_bytes());
        output[56..60].copy_from_slice(&self.payload.length().to_be_bytes());
        output[60..64].copy_from_slice(&self.payload.checksum().to_be_bytes());
        output[64..72].copy_from_slice(&self.source_cq_offset.get().to_be_bytes());
        output[72..80].copy_from_slice(&self.source_physical_offset.to_be_bytes());
        output[80..84].copy_from_slice(&self.source_size.to_be_bytes());
        output[84..92].copy_from_slice(&self.state_version.to_be_bytes());
        output[92] = match self.owner_engine {
            TimerEngineId::JavaCompat => 0,
            TimerEngineId::ExtendedTimeline => 1,
        };
        output[93] = u8::from(self.shadow_only);
        let checksum = crc32c(&output[..RUN_RECORD_SIZE - 4]);
        output[RUN_RECORD_SIZE - 4..].copy_from_slice(&checksum.to_be_bytes());
        Ok(output)
    }

    fn decode(bytes: &[u8]) -> Result<Self, TimelineSegmentError> {
        if bytes.len() != RUN_RECORD_SIZE
            || crc32c(&bytes[..RUN_RECORD_SIZE - 4]) != read_u32(bytes, RUN_RECORD_SIZE - 4)?
        {
            return Err(TimelineSegmentError::RecordChecksumMismatch);
        }
        let owner_engine = match bytes[92] {
            0 => TimerEngineId::JavaCompat,
            1 => TimerEngineId::ExtendedTimeline,
            value => return Err(TimelineSegmentError::InvalidOwner(value)),
        };
        if bytes[93] > 1 {
            return Err(TimelineSegmentError::InvalidFlags);
        }
        let payload = TimerPayloadStoreLocator::try_new(
            read_i32(bytes, 34)?,
            read_u16(bytes, 38)?,
            read_u64(bytes, 40)?,
            read_u64(bytes, 48)?,
            read_u32(bytes, 56)?,
            read_u32(bytes, 60)?,
        )
        .map_err(|_| TimelineSegmentError::InvalidPayloadLocator)?;
        let record = Self {
            key: TimelineSegmentKey {
                due_time_ms: read_i64(bytes, 0)?,
                lane: read_u16(bytes, 8)?,
                timer_id: TimerId::new(read_u128(bytes, 10)?),
                generation: TimerGeneration::new(read_u64(bytes, 26)?),
            },
            payload,
            source_cq_offset: TimerSourceCqOffset::new(read_i64(bytes, 64)?),
            source_physical_offset: read_i64(bytes, 72)?,
            source_size: read_u32(bytes, 80)?,
            state_version: read_u64(bytes, 84)?,
            owner_engine,
            shadow_only: bytes[93] == 1,
        };
        validate_record(record)?;
        Ok(record)
    }
}

/// Immutable run role.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum TimelineRunKind {
    /// Compacted partition base.
    Base = 0,
    /// Small immutable ingest batch.
    Delta = 1,
}

impl TimelineRunKind {
    fn decode(value: u8) -> Result<Self, TimelineSegmentError> {
        match value {
            0 => Ok(Self::Base),
            1 => Ok(Self::Delta),
            value => Err(TimelineSegmentError::InvalidRunKind(value)),
        }
    }
}

/// Metadata sufficient to skip empty runs without opening their bodies.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimelineRunDescriptor {
    /// Physical partition.
    pub partition: TimelinePartitionKey,
    /// Base or delta run.
    pub kind: TimelineRunKind,
    /// Stable content-derived run identity.
    pub run_id: u64,
    /// Manifest generation in which the writer created the run.
    pub created_generation: u64,
    /// Sorted record count.
    pub record_count: u64,
    /// Inclusive deadline range.
    pub min_due_time_ms: i64,
    /// Inclusive deadline range.
    pub max_due_time_ms: i64,
    /// Inclusive source CQ offset range.
    pub min_source_cq_offset: i64,
    /// Inclusive source CQ offset range.
    pub max_source_cq_offset: i64,
    /// Fixed-size logical body bytes.
    pub logical_bytes: u64,
    /// CRC32C of the full fixed-record body.
    pub body_checksum: u32,
    /// File name relative to the segmented Timeline root.
    pub relative_path: String,
}

/// Writes one sorted, sealed immutable run and synchronizes it before returning.
pub(crate) fn write_timeline_run(
    root: &Path,
    relative_path: &str,
    kind: TimelineRunKind,
    partition: TimelinePartitionKey,
    run_id: u64,
    created_generation: u64,
    records: &[TimelineSegmentRecord],
) -> Result<TimelineRunDescriptor, TimelineSegmentError> {
    if records.is_empty() || relative_path.is_empty() {
        return Err(TimelineSegmentError::EmptyRun);
    }
    validate_sorted_partition(records, partition)?;
    let body = records
        .iter()
        .copied()
        .map(TimelineSegmentRecord::encode)
        .collect::<Result<Vec<_>, _>>()?
        .concat();
    let body_checksum = crc32c(&body);
    let descriptor = descriptor_for(
        relative_path,
        kind,
        partition,
        run_id,
        created_generation,
        records,
        body_checksum,
    )?;
    let path = root.join(relative_path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create_new(true).write(true).open(&path)?;
    file.write_all(&encode_header(&descriptor, records)?)?;
    file.write_all(&body)?;
    file.write_all(&encode_footer(&descriptor))?;
    file.sync_all()?;
    Ok(descriptor)
}

/// Reads only the fixed header/footer and validates the sealed file shape.
pub(crate) fn inspect_timeline_run(
    root: &Path,
    relative_path: &str,
) -> Result<TimelineRunDescriptor, TimelineSegmentError> {
    let path = root.join(relative_path);
    let mut file = OpenOptions::new().read(true).open(path)?;
    let length = file.metadata()?.len();
    let minimum = u64::try_from(RUN_HEADER_SIZE + RUN_RECORD_SIZE + RUN_FOOTER_SIZE)
        .map_err(|_| TimelineSegmentError::LengthOverflow)?;
    if length < minimum {
        return Err(TimelineSegmentError::UnsealedRun);
    }
    let mut header = [0u8; RUN_HEADER_SIZE];
    file.read_exact(&mut header)?;
    let mut footer = [0u8; RUN_FOOTER_SIZE];
    file.seek(SeekFrom::End(-(RUN_FOOTER_SIZE as i64)))?;
    file.read_exact(&mut footer)?;
    let mut descriptor = decode_header(&header, relative_path)?;
    decode_and_validate_footer(&footer, &descriptor)?;
    let expected = u64::try_from(RUN_HEADER_SIZE + RUN_FOOTER_SIZE)
        .map_err(|_| TimelineSegmentError::LengthOverflow)?
        .saturating_add(descriptor.logical_bytes);
    if expected != length {
        return Err(TimelineSegmentError::RunLengthMismatch {
            expected,
            actual: length,
        });
    }
    descriptor.relative_path = relative_path.to_owned();
    Ok(descriptor)
}

/// Streaming reader that keeps at most one fixed record per run in memory.
pub struct TimelineRunReader {
    file: File,
    descriptor: TimelineRunDescriptor,
    next_record: u64,
    body_crc: RunningCrc32c,
    verifies_full_body: bool,
}

impl TimelineRunReader {
    /// Opens a sealed run without scanning its body.
    pub(crate) fn open(root: &Path, descriptor: TimelineRunDescriptor) -> Result<Self, TimelineSegmentError> {
        let inspected = inspect_timeline_run(root, &descriptor.relative_path)?;
        if inspected != descriptor {
            return Err(TimelineSegmentError::ManifestDescriptorMismatch);
        }
        let mut file = OpenOptions::new()
            .read(true)
            .open(root.join(&descriptor.relative_path))?;
        file.seek(SeekFrom::Start(RUN_HEADER_SIZE as u64))?;
        Ok(Self {
            file,
            descriptor,
            next_record: 0,
            body_crc: RunningCrc32c::new(),
            verifies_full_body: true,
        })
    }

    /// Returns the next verified record, or `None` after validating the full body checksum.
    pub(crate) fn read_next(&mut self) -> Result<Option<TimelineSegmentRecord>, TimelineSegmentError> {
        if self.next_record == self.descriptor.record_count {
            if self.verifies_full_body && self.body_crc.finish() != self.descriptor.body_checksum {
                return Err(TimelineSegmentError::BodyChecksumMismatch);
            }
            return Ok(None);
        }
        let mut bytes = [0u8; RUN_RECORD_SIZE];
        self.file.read_exact(&mut bytes)?;
        self.body_crc.update(&bytes);
        let record = TimelineSegmentRecord::decode(&bytes)?;
        if record.key.partition()? != self.descriptor.partition {
            return Err(TimelineSegmentError::PartitionMismatch);
        }
        self.next_record = self.next_record.saturating_add(1);
        Ok(Some(record))
    }

    /// Skips a bounded number of records while retaining checksum verification.
    #[allow(dead_code, reason = "exercised by the in-crate timeline scenarios")]
    pub(crate) fn skip(&mut self, count: u64) -> Result<(), TimelineSegmentError> {
        for _ in 0..count {
            if self.read_next()?.is_none() {
                return Err(TimelineSegmentError::CursorPastEnd);
            }
        }
        Ok(())
    }

    /// Seeks to an exact fixed-record cursor. Subsequent records retain their individual CRC
    /// checks; the aggregate body CRC is checked only for scans that start at record zero.
    pub(crate) fn seek_to(&mut self, position: u64) -> Result<(), TimelineSegmentError> {
        if position > self.descriptor.record_count {
            return Err(TimelineSegmentError::CursorPastEnd);
        }
        let byte_offset = position
            .checked_mul(RUN_RECORD_SIZE as u64)
            .and_then(|offset| offset.checked_add(RUN_HEADER_SIZE as u64))
            .ok_or(TimelineSegmentError::LengthOverflow)?;
        self.file.seek(SeekFrom::Start(byte_offset))?;
        self.next_record = position;
        self.body_crc = RunningCrc32c::new();
        self.verifies_full_body = position == 0;
        Ok(())
    }

    /// Returns the number of body records already consumed.
    pub const fn position(&self) -> u64 {
        self.next_record
    }
}

fn descriptor_for(
    relative_path: &str,
    kind: TimelineRunKind,
    partition: TimelinePartitionKey,
    run_id: u64,
    created_generation: u64,
    records: &[TimelineSegmentRecord],
    body_checksum: u32,
) -> Result<TimelineRunDescriptor, TimelineSegmentError> {
    let first = records.first().ok_or(TimelineSegmentError::EmptyRun)?;
    let last = records.last().ok_or(TimelineSegmentError::EmptyRun)?;
    let (min_source_cq_offset, max_source_cq_offset) =
        records.iter().fold((i64::MAX, i64::MIN), |(minimum, maximum), record| {
            (
                minimum.min(record.source_cq_offset.get()),
                maximum.max(record.source_cq_offset.get()),
            )
        });
    Ok(TimelineRunDescriptor {
        partition,
        kind,
        run_id,
        created_generation,
        record_count: u64::try_from(records.len()).map_err(|_| TimelineSegmentError::LengthOverflow)?,
        min_due_time_ms: first.key.due_time_ms,
        max_due_time_ms: last.key.due_time_ms,
        min_source_cq_offset,
        max_source_cq_offset,
        logical_bytes: u64::try_from(records.len().saturating_mul(RUN_RECORD_SIZE))
            .map_err(|_| TimelineSegmentError::LengthOverflow)?,
        body_checksum,
        relative_path: relative_path.to_owned(),
    })
}

fn encode_header(
    descriptor: &TimelineRunDescriptor,
    records: &[TimelineSegmentRecord],
) -> Result<[u8; RUN_HEADER_SIZE], TimelineSegmentError> {
    let first = records.first().ok_or(TimelineSegmentError::EmptyRun)?;
    let last = records.last().ok_or(TimelineSegmentError::EmptyRun)?;
    let mut output = [0u8; RUN_HEADER_SIZE];
    output[0..4].copy_from_slice(&RUN_MAGIC.to_be_bytes());
    output[4..6].copy_from_slice(&RUN_VERSION.to_be_bytes());
    output[6..8].copy_from_slice(&(RUN_HEADER_SIZE as u16).to_be_bytes());
    output[8] = descriptor.kind as u8;
    output[10..14].copy_from_slice(&descriptor.partition.due_day_utc.to_be_bytes());
    output[14] = descriptor.partition.due_hour_utc;
    output[16..18].copy_from_slice(&descriptor.partition.lane.to_be_bytes());
    output[18..26].copy_from_slice(&descriptor.created_generation.to_be_bytes());
    output[26..34].copy_from_slice(&descriptor.run_id.to_be_bytes());
    output[34..42].copy_from_slice(&descriptor.record_count.to_be_bytes());
    output[42..50].copy_from_slice(&descriptor.min_due_time_ms.to_be_bytes());
    output[50..58].copy_from_slice(&descriptor.max_due_time_ms.to_be_bytes());
    output[58..74].copy_from_slice(&first.key.timer_id.get().to_be_bytes());
    output[74..90].copy_from_slice(&last.key.timer_id.get().to_be_bytes());
    output[90..98].copy_from_slice(&first.key.generation.get().to_be_bytes());
    output[98..106].copy_from_slice(&last.key.generation.get().to_be_bytes());
    output[106..114].copy_from_slice(&descriptor.min_source_cq_offset.to_be_bytes());
    output[114..122].copy_from_slice(&descriptor.max_source_cq_offset.to_be_bytes());
    output[122..130].copy_from_slice(&descriptor.logical_bytes.to_be_bytes());
    output[130..134].copy_from_slice(&descriptor.body_checksum.to_be_bytes());
    let checksum = crc32c(&output[..RUN_HEADER_SIZE - 4]);
    output[RUN_HEADER_SIZE - 4..].copy_from_slice(&checksum.to_be_bytes());
    Ok(output)
}

fn decode_header(bytes: &[u8], relative_path: &str) -> Result<TimelineRunDescriptor, TimelineSegmentError> {
    if bytes.len() != RUN_HEADER_SIZE
        || read_u32(bytes, 0)? != RUN_MAGIC
        || read_u16(bytes, 4)? != RUN_VERSION
        || usize::from(read_u16(bytes, 6)?) != RUN_HEADER_SIZE
        || crc32c(&bytes[..RUN_HEADER_SIZE - 4]) != read_u32(bytes, RUN_HEADER_SIZE - 4)?
    {
        return Err(TimelineSegmentError::InvalidHeader);
    }
    let due_hour_utc = bytes[14];
    if due_hour_utc > 23 || read_u64(bytes, 34)? == 0 || read_u64(bytes, 122)? == 0 {
        return Err(TimelineSegmentError::InvalidHeader);
    }
    Ok(TimelineRunDescriptor {
        partition: TimelinePartitionKey {
            due_day_utc: read_i32(bytes, 10)?,
            due_hour_utc,
            lane: read_u16(bytes, 16)?,
        },
        kind: TimelineRunKind::decode(bytes[8])?,
        run_id: read_u64(bytes, 26)?,
        created_generation: read_u64(bytes, 18)?,
        record_count: read_u64(bytes, 34)?,
        min_due_time_ms: read_i64(bytes, 42)?,
        max_due_time_ms: read_i64(bytes, 50)?,
        min_source_cq_offset: read_i64(bytes, 106)?,
        max_source_cq_offset: read_i64(bytes, 114)?,
        logical_bytes: read_u64(bytes, 122)?,
        body_checksum: read_u32(bytes, 130)?,
        relative_path: relative_path.to_owned(),
    })
}

fn encode_footer(descriptor: &TimelineRunDescriptor) -> [u8; RUN_FOOTER_SIZE] {
    let mut output = [0u8; RUN_FOOTER_SIZE];
    output[0..4].copy_from_slice(&RUN_FOOTER_MAGIC.to_be_bytes());
    output[4..6].copy_from_slice(&RUN_VERSION.to_be_bytes());
    output[6..8].copy_from_slice(&(RUN_FOOTER_SIZE as u16).to_be_bytes());
    output[8..16].copy_from_slice(&descriptor.record_count.to_be_bytes());
    output[16..20].copy_from_slice(&descriptor.body_checksum.to_be_bytes());
    output[20..24].copy_from_slice(&(RUN_RECORD_SIZE as u32).to_be_bytes());
    output[24..32].copy_from_slice(&RUN_SEALED_MARKER.to_be_bytes());
    let checksum = crc32c(&output[..RUN_FOOTER_SIZE - 4]);
    output[RUN_FOOTER_SIZE - 4..].copy_from_slice(&checksum.to_be_bytes());
    output
}

fn decode_and_validate_footer(bytes: &[u8], descriptor: &TimelineRunDescriptor) -> Result<(), TimelineSegmentError> {
    if bytes.len() != RUN_FOOTER_SIZE
        || read_u32(bytes, 0)? != RUN_FOOTER_MAGIC
        || read_u16(bytes, 4)? != RUN_VERSION
        || usize::from(read_u16(bytes, 6)?) != RUN_FOOTER_SIZE
        || read_u64(bytes, 8)? != descriptor.record_count
        || read_u32(bytes, 16)? != descriptor.body_checksum
        || usize::try_from(read_u32(bytes, 20)?).ok() != Some(RUN_RECORD_SIZE)
        || read_u64(bytes, 24)? != RUN_SEALED_MARKER
        || crc32c(&bytes[..RUN_FOOTER_SIZE - 4]) != read_u32(bytes, RUN_FOOTER_SIZE - 4)?
    {
        return Err(TimelineSegmentError::UnsealedRun);
    }
    Ok(())
}

fn validate_sorted_partition(
    records: &[TimelineSegmentRecord],
    partition: TimelinePartitionKey,
) -> Result<(), TimelineSegmentError> {
    let mut previous = None;
    for record in records {
        validate_record(*record)?;
        if record.key.partition()? != partition {
            return Err(TimelineSegmentError::PartitionMismatch);
        }
        if previous.is_some_and(|key| key >= record.key) {
            return Err(TimelineSegmentError::RecordsNotStrictlySorted);
        }
        previous = Some(record.key);
    }
    Ok(())
}

fn validate_record(record: TimelineSegmentRecord) -> Result<(), TimelineSegmentError> {
    if record.key.due_time_ms < 0
        || record.source_cq_offset.get() < 0
        || record.source_physical_offset < 0
        || record.source_size == 0
        || record.payload.lane() != record.key.lane
        || record.payload.due_day_utc() != record.key.partition()?.due_day_utc
    {
        return Err(TimelineSegmentError::InvalidRecord);
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct RunningCrc32c {
    state: u32,
}

impl RunningCrc32c {
    const fn new() -> Self {
        Self { state: !0 }
    }

    fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.state ^= u32::from(*byte);
            for _ in 0..8 {
                let mask = 0u32.wrapping_sub(self.state & 1);
                self.state = (self.state >> 1) ^ (0x82F6_3B78 & mask);
            }
        }
    }

    const fn finish(self) -> u32 {
        !self.state
    }
}

fn read_array<const N: usize>(bytes: &[u8], offset: usize) -> Result<[u8; N], TimelineSegmentError> {
    bytes
        .get(offset..offset.saturating_add(N))
        .and_then(|value| value.try_into().ok())
        .ok_or(TimelineSegmentError::Truncated)
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, TimelineSegmentError> {
    Ok(u16::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, TimelineSegmentError> {
    Ok(u32::from_be_bytes(read_array(bytes, offset)?))
}

fn read_i32(bytes: &[u8], offset: usize) -> Result<i32, TimelineSegmentError> {
    Ok(i32::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, TimelineSegmentError> {
    Ok(u64::from_be_bytes(read_array(bytes, offset)?))
}

fn read_i64(bytes: &[u8], offset: usize) -> Result<i64, TimelineSegmentError> {
    Ok(i64::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u128(bytes: &[u8], offset: usize) -> Result<u128, TimelineSegmentError> {
    Ok(u128::from_be_bytes(read_array(bytes, offset)?))
}

/// Native Timeline run codec failure.
#[derive(Debug, Error)]
pub(crate) enum TimelineSegmentError {
    /// Underlying file operation failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Deadline cannot map to a V1 partition.
    #[error("invalid Timeline deadline: {0}")]
    InvalidDeadline(i64),
    /// A run cannot be empty.
    #[error("native Timeline run must contain at least one record")]
    EmptyRun,
    /// Records must be strictly ordered by the full key.
    #[error("native Timeline records are not strictly sorted")]
    RecordsNotStrictlySorted,
    /// Record metadata is internally inconsistent.
    #[error("invalid native Timeline record")]
    InvalidRecord,
    /// Record belongs to a different physical partition.
    #[error("native Timeline partition mismatch")]
    PartitionMismatch,
    /// Persisted payload location is invalid.
    #[error("invalid native Timeline payload locator")]
    InvalidPayloadLocator,
    /// Owner engine byte is unknown.
    #[error("invalid native Timeline owner engine: {0}")]
    InvalidOwner(u8),
    /// Flag byte is not canonical.
    #[error("invalid native Timeline flags")]
    InvalidFlags,
    /// Run kind is unknown.
    #[error("invalid native Timeline run kind: {0}")]
    InvalidRunKind(u8),
    /// Header version, shape, or checksum is invalid.
    #[error("invalid native Timeline run header")]
    InvalidHeader,
    /// Footer is missing or does not contain a valid sealed marker.
    #[error("native Timeline run is not sealed")]
    UnsealedRun,
    /// Fixed record checksum is invalid.
    #[error("native Timeline record checksum mismatch")]
    RecordChecksumMismatch,
    /// Full body checksum differs from the sealed footer.
    #[error("native Timeline run body checksum mismatch")]
    BodyChecksumMismatch,
    /// File length differs from header metadata.
    #[error("native Timeline run length mismatch: expected={expected}, actual={actual}")]
    RunLengthMismatch {
        /// Expected bytes.
        expected: u64,
        /// Actual bytes.
        actual: u64,
    },
    /// Manifest metadata differs from the sealed run.
    #[error("native Timeline manifest descriptor mismatch")]
    ManifestDescriptorMismatch,
    /// Persisted cursor points beyond the run.
    #[error("native Timeline cursor points past the run end")]
    CursorPastEnd,
    /// Persisted fixed field is truncated.
    #[error("native Timeline run is truncated")]
    Truncated,
    /// An encoded length cannot fit V1.
    #[error("native Timeline length overflow")]
    LengthOverflow,
}

impl TimelineSegmentError {
    /// Promotes this leaf into the canonical storage facade exactly once.
    ///
    /// Filesystem faults keep their typed I/O source, and the remaining
    /// record/run evidence follows the owning operation as read or write
    /// failure evidence. The complete leaf is preserved as the typed source.
    pub(crate) fn into_store_error(self, operation: StoreOperation) -> StoreError {
        let descriptor = match (&self, operation) {
            (Self::Io(_), _) => &rocketmq_error::STORAGE_IO_FAILED,
            (_, StoreOperation::Load | StoreOperation::Read | StoreOperation::QueryOffset) => {
                &rocketmq_error::STORAGE_READ_FAILED
            }
            (_, StoreOperation::Append | StoreOperation::Flush | StoreOperation::AppendDerived) => {
                &rocketmq_error::STORAGE_WRITE_FAILED
            }
            _ => &rocketmq_error::STORAGE_INTERNAL_FAILURE,
        };
        StoreError::new(descriptor, operation).with_source(self)
    }
}
