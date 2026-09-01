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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;
use thiserror::Error;

use crate::timer::metrics::TimerStorageMetrics;
use crate::timer::service::TimerLogRecord;
use crate::timer::storage_format::crc32c;
use crate::timer::storage_format::TimerLogOffset;
use crate::timer::storage_format::TimerSegmentId;

pub const TIMER_LOG_V2_PHYSICAL_RECORD_SIZE: usize = 80;
pub const TIMER_LOG_V2_LOGICAL_RECORD_SIZE: u64 = TimerLogRecord::SIZE as u64;
const DATA_MAGIC: u32 = 0x544C_4732;
const BLANK_MAGIC: u32 = 0x544C_424C;
const RECORD_VERSION: u16 = 2;
const MANIFEST_MAGIC: u32 = 0x544D_4E32;
const MANIFEST_SIZE: usize = 40;
const MANIFEST_FILE: &str = "manifest";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimerLogV2Record {
    pub previous_offset: i64,
    pub source_physical_offset: i64,
    pub source_size: i32,
    pub timer_magic: i32,
    pub deliver_time_ms: i64,
    pub slot_time_ms: i64,
    pub generation: u64,
    pub source_queue_offset: i64,
}

impl TimerLogV2Record {
    pub fn from_legacy(record: TimerLogRecord, generation: u64) -> Self {
        Self {
            previous_offset: record.prev_pos,
            source_physical_offset: record.commit_log_offset,
            source_size: record.size,
            timer_magic: record.magic,
            deliver_time_ms: record.deliver_time_ms,
            slot_time_ms: record.deliver_time_ms,
            generation,
            source_queue_offset: record.queue_offset,
        }
    }

    pub fn to_legacy(self) -> TimerLogRecord {
        TimerLogRecord {
            deliver_time_ms: self.slot_time_ms,
            commit_log_offset: self.source_physical_offset,
            size: self.source_size,
            queue_offset: self.source_queue_offset,
            prev_pos: self.previous_offset,
            magic: self.timer_magic,
        }
    }

    pub fn encode(self) -> [u8; TIMER_LOG_V2_PHYSICAL_RECORD_SIZE] {
        let mut bytes = [0u8; TIMER_LOG_V2_PHYSICAL_RECORD_SIZE];
        bytes[0..4].copy_from_slice(&DATA_MAGIC.to_be_bytes());
        bytes[4..6].copy_from_slice(&RECORD_VERSION.to_be_bytes());
        bytes[6..8].copy_from_slice(&(TIMER_LOG_V2_PHYSICAL_RECORD_SIZE as u16).to_be_bytes());
        bytes[8..16].copy_from_slice(&self.previous_offset.to_be_bytes());
        bytes[16..24].copy_from_slice(&self.source_physical_offset.to_be_bytes());
        bytes[24..28].copy_from_slice(&self.source_size.to_be_bytes());
        bytes[28..32].copy_from_slice(&self.timer_magic.to_be_bytes());
        bytes[32..40].copy_from_slice(&self.deliver_time_ms.to_be_bytes());
        bytes[40..48].copy_from_slice(&self.slot_time_ms.to_be_bytes());
        bytes[48..56].copy_from_slice(&self.generation.to_be_bytes());
        bytes[56..64].copy_from_slice(&self.source_queue_offset.to_be_bytes());
        let checksum = crc32c(&bytes[..76]);
        bytes[76..80].copy_from_slice(&checksum.to_be_bytes());
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, SegmentedTimerLogError> {
        if bytes.len() != TIMER_LOG_V2_PHYSICAL_RECORD_SIZE {
            return Err(SegmentedTimerLogError::InvalidRecordLength(bytes.len()));
        }
        let magic = read_u32(bytes, 0);
        if magic != DATA_MAGIC {
            return Err(SegmentedTimerLogError::InvalidRecordMagic(magic));
        }
        let version = read_u16(bytes, 4);
        if version != RECORD_VERSION {
            return Err(SegmentedTimerLogError::UnsupportedRecordVersion(version));
        }
        if read_u16(bytes, 6) as usize != TIMER_LOG_V2_PHYSICAL_RECORD_SIZE {
            return Err(SegmentedTimerLogError::InvalidRecordLength(read_u16(bytes, 6) as usize));
        }
        if crc32c(&bytes[..76]) != read_u32(bytes, 76) {
            return Err(SegmentedTimerLogError::RecordChecksumMismatch);
        }
        Ok(Self {
            previous_offset: read_i64(bytes, 8),
            source_physical_offset: read_i64(bytes, 16),
            source_size: read_i32(bytes, 24),
            timer_magic: read_i32(bytes, 28),
            deliver_time_ms: read_i64(bytes, 32),
            slot_time_ms: read_i64(bytes, 40),
            generation: read_u64(bytes, 48),
            source_queue_offset: read_i64(bytes, 56),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimerLogBatchEntry {
    pub offset: TimerLogOffset,
    pub record: TimerLogV2Record,
}

#[derive(Debug, PartialEq, Eq)]
pub struct TimerLogReadBatch {
    pub entries: Vec<TimerLogBatchEntry>,
    pub next_cursor: TimerLogOffset,
    pub end_of_log: bool,
}

pub struct SegmentedTimerLog {
    directory: PathBuf,
    segment_size: u64,
    records_per_segment: u64,
    logical_segment_span: u64,
    handle_limit: usize,
    metrics: Arc<TimerStorageMetrics>,
    state: Mutex<SegmentedTimerLogState>,
}

#[derive(Default)]
struct SegmentedTimerLogState {
    segments: BTreeMap<u64, SegmentState>,
    active_start: u64,
    active_records: u64,
    next_offset: u64,
    durable_length: u64,
    min_live_offset: u64,
    handles: HashMap<u64, File>,
    handle_order: VecDeque<u64>,
}

#[derive(Clone, Copy, Debug)]
struct SegmentState {
    records: u64,
    sealed: bool,
}

impl SegmentedTimerLog {
    pub(crate) fn new(
        directory: impl AsRef<Path>,
        segment_size: usize,
        handle_limit: usize,
        metrics: Arc<TimerStorageMetrics>,
    ) -> Result<Self, SegmentedTimerLogError> {
        if segment_size < TIMER_LOG_V2_PHYSICAL_RECORD_SIZE * 2
            || !segment_size.is_multiple_of(TIMER_LOG_V2_PHYSICAL_RECORD_SIZE)
        {
            return Err(SegmentedTimerLogError::InvalidSegmentSize(segment_size));
        }
        let physical_slots = segment_size as u64 / TIMER_LOG_V2_PHYSICAL_RECORD_SIZE as u64;
        let records_per_segment = physical_slots - 1;
        Ok(Self {
            directory: directory.as_ref().to_path_buf(),
            segment_size: segment_size as u64,
            records_per_segment,
            logical_segment_span: records_per_segment * TIMER_LOG_V2_LOGICAL_RECORD_SIZE,
            handle_limit: handle_limit.max(1),
            metrics,
            state: Mutex::new(SegmentedTimerLogState::default()),
        })
    }

    pub(crate) fn load(&self) -> Result<(), SegmentedTimerLogError> {
        std::fs::create_dir_all(&self.directory)?;
        let manifest = self.read_manifest()?;
        let mut segment_starts = Vec::new();
        for entry in std::fs::read_dir(&self.directory)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if name.len() == 20 && name.bytes().all(|byte| byte.is_ascii_digit()) {
                segment_starts.push(
                    name.parse::<u64>()
                        .map_err(|_| SegmentedTimerLogError::InvalidSegmentName(name))?,
                );
            }
        }
        segment_starts.sort_unstable();
        if segment_starts.is_empty() {
            segment_starts.push(0);
            self.open_writer(0)?;
        }
        for pair in segment_starts.windows(2) {
            if pair[1] != pair[0] + self.logical_segment_span {
                return Err(SegmentedTimerLogError::SegmentHole {
                    expected: pair[0] + self.logical_segment_span,
                    actual: pair[1],
                });
            }
        }

        let mut state = SegmentedTimerLogState::default();
        for (index, start) in segment_starts.iter().copied().enumerate() {
            let last = index + 1 == segment_starts.len();
            let path = self.segment_path(start);
            let length = path.metadata()?.len();
            if !last {
                self.validate_sealed_segment(start, length)?;
                state.segments.insert(
                    start,
                    SegmentState {
                        records: self.records_per_segment,
                        sealed: true,
                    },
                );
                continue;
            }

            let (records, sealed, replayed) = self.recover_active_segment(start, length)?;
            self.metrics.record_recovery_replay(replayed);
            state.segments.insert(start, SegmentState { records, sealed });
            if sealed {
                let next_start = start + self.logical_segment_span;
                self.open_writer(next_start)?;
                state.segments.insert(
                    next_start,
                    SegmentState {
                        records: 0,
                        sealed: false,
                    },
                );
                state.active_start = next_start;
                state.active_records = 0;
                state.next_offset = next_start;
            } else {
                state.active_start = start;
                state.active_records = records;
                state.next_offset = start + records * TIMER_LOG_V2_LOGICAL_RECORD_SIZE;
            }
        }
        state.durable_length = manifest
            .map(|value| value.durable_length.min(state.next_offset))
            .unwrap_or_default();
        state.min_live_offset = manifest.map(|value| value.min_live_offset).unwrap_or_default();
        self.metrics.set_segment_count(state.segments.len() as u64);
        self.update_log_byte_metrics(&state);
        *self.state.lock() = state;
        Ok(())
    }

    pub(crate) fn append(&self, record: TimerLogV2Record) -> Result<TimerLogOffset, SegmentedTimerLogError> {
        let offsets = self.append_batch(&[record])?;
        Ok(offsets[0])
    }

    pub(crate) fn append_batch(
        &self,
        records: &[TimerLogV2Record],
    ) -> Result<Vec<TimerLogOffset>, SegmentedTimerLogError> {
        let mut state = self.state.lock();
        let mut offsets = Vec::with_capacity(records.len());
        let mut index = 0usize;
        while index < records.len() {
            if state.active_records == self.records_per_segment {
                self.seal_and_rotate(&mut state)?;
            }
            let available = (self.records_per_segment - state.active_records) as usize;
            let count = available.min(records.len() - index);
            let mut bytes = Vec::with_capacity(count * TIMER_LOG_V2_PHYSICAL_RECORD_SIZE);
            for record in &records[index..index + count] {
                offsets.push(TimerLogOffset::new(state.next_offset));
                bytes.extend_from_slice(&record.encode());
                state.active_records += 1;
                state.next_offset += TIMER_LOG_V2_LOGICAL_RECORD_SIZE;
            }
            let mut writer = self.open_writer(state.active_start)?;
            writer.seek(SeekFrom::Start(
                (state.active_records - count as u64) * TIMER_LOG_V2_PHYSICAL_RECORD_SIZE as u64,
            ))?;
            writer.write_all(&bytes)?;
            let active_start = state.active_start;
            let active_records = state.active_records;
            if let Some(segment) = state.segments.get_mut(&active_start) {
                segment.records = active_records;
            }
            self.metrics
                .record_logical_write((count as u64) * TIMER_LOG_V2_LOGICAL_RECORD_SIZE);
            self.metrics.record_physical_write(bytes.len() as u64);
            index += count;
        }
        self.update_log_byte_metrics(&state);
        Ok(offsets)
    }

    pub(crate) fn read(&self, offset: TimerLogOffset) -> Result<TimerLogV2Record, SegmentedTimerLogError> {
        let mut state = self.state.lock();
        self.read_locked(&mut state, offset)
    }

    pub(crate) fn read_batch(
        &self,
        cursor: TimerLogOffset,
        max_messages: usize,
        max_bytes: usize,
    ) -> Result<TimerLogReadBatch, SegmentedTimerLogError> {
        let mut state = self.state.lock();
        let mut offset = cursor.get();
        let byte_limit = max_bytes / TimerLogRecord::SIZE;
        let count_limit = max_messages
            .min(byte_limit)
            .max(usize::from(max_messages > 0 && max_bytes >= TimerLogRecord::SIZE));
        let mut entries = Vec::with_capacity(count_limit);
        while offset < state.next_offset && entries.len() < count_limit {
            let typed_offset = TimerLogOffset::new(offset);
            entries.push(TimerLogBatchEntry {
                offset: typed_offset,
                record: self.read_locked(&mut state, typed_offset)?,
            });
            offset += TIMER_LOG_V2_LOGICAL_RECORD_SIZE;
        }
        Ok(TimerLogReadBatch {
            entries,
            next_cursor: TimerLogOffset::new(offset),
            end_of_log: offset >= state.next_offset,
        })
    }

    pub(crate) fn flush_up_to(&self, offset: TimerLogOffset) -> Result<(), SegmentedTimerLogError> {
        let mut state = self.state.lock();
        if offset.get() > state.next_offset {
            return Err(SegmentedTimerLogError::FlushBeyondEnd {
                requested: offset.get(),
                end: state.next_offset,
            });
        }
        let started = Instant::now();
        self.open_writer(state.active_start)?.sync_data()?;
        state.durable_length = offset.get();
        self.write_manifest(&state)?;
        self.metrics.record_fsync(started.elapsed().as_nanos() as u64);
        Ok(())
    }

    pub(crate) fn flush(&self) -> Result<(), SegmentedTimerLogError> {
        self.flush_up_to(TimerLogOffset::new(self.len()))
    }

    pub fn len(&self) -> u64 {
        self.state.lock().next_offset
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn durable_length(&self) -> u64 {
        self.state.lock().durable_length
    }

    pub fn min_live_offset(&self) -> TimerLogOffset {
        TimerLogOffset::new(self.state.lock().min_live_offset)
    }

    pub fn segment_ids(&self) -> Vec<TimerSegmentId> {
        self.state
            .lock()
            .segments
            .keys()
            .copied()
            .map(TimerSegmentId::new)
            .collect()
    }

    pub(crate) fn truncate(&self, length: TimerLogOffset) -> Result<(), SegmentedTimerLogError> {
        if !length.get().is_multiple_of(TIMER_LOG_V2_LOGICAL_RECORD_SIZE) {
            return Err(SegmentedTimerLogError::UnalignedOffset(length.get()));
        }
        let mut state = self.state.lock();
        if length.get() > state.next_offset {
            return Err(SegmentedTimerLogError::TruncateBeyondEnd {
                requested: length.get(),
                end: state.next_offset,
            });
        }
        let target_start = (length.get() / self.logical_segment_span) * self.logical_segment_span;
        let remove: Vec<_> = state
            .segments
            .range((target_start + 1)..)
            .map(|(start, _)| *start)
            .collect();
        for start in remove {
            let path = self.segment_path(start);
            if path.exists() {
                std::fs::remove_file(path)?;
            }
            state.segments.remove(&start);
            state.handles.remove(&start);
            state.handle_order.retain(|cached| *cached != start);
        }
        if let std::collections::btree_map::Entry::Vacant(entry) = state.segments.entry(target_start) {
            self.open_writer(target_start)?;
            entry.insert(SegmentState {
                records: 0,
                sealed: false,
            });
        }
        let records = (length.get() - target_start) / TIMER_LOG_V2_LOGICAL_RECORD_SIZE;
        self.open_writer(target_start)?
            .set_len(records * TIMER_LOG_V2_PHYSICAL_RECORD_SIZE as u64)?;
        state.active_start = target_start;
        state.active_records = records;
        state.next_offset = length.get();
        state.durable_length = state.durable_length.min(length.get());
        if let Some(segment) = state.segments.get_mut(&target_start) {
            *segment = SegmentState { records, sealed: false };
        }
        self.write_manifest(&state)?;
        self.metrics.set_segment_count(state.segments.len() as u64);
        self.update_log_byte_metrics(&state);
        Ok(())
    }

    pub(crate) fn gc(
        &self,
        min_live_offset: TimerLogOffset,
        checkpoint_watermark: TimerLogOffset,
        snapshot_watermark: TimerLogOffset,
    ) -> Result<usize, SegmentedTimerLogError> {
        let safe_watermark = min_live_offset
            .get()
            .min(checkpoint_watermark.get())
            .min(snapshot_watermark.get());
        let mut state = self.state.lock();
        let removable: Vec<_> = state
            .segments
            .iter()
            .filter_map(|(start, segment)| {
                (segment.sealed && *start + self.logical_segment_span <= safe_watermark).then_some(*start)
            })
            .collect();
        for start in &removable {
            std::fs::remove_file(self.segment_path(*start))?;
            state.segments.remove(start);
            state.handles.remove(start);
            state.handle_order.retain(|cached| cached != start);
        }
        state.min_live_offset = state.min_live_offset.max(min_live_offset.get());
        self.write_manifest(&state)?;
        self.metrics.set_segment_count(state.segments.len() as u64);
        self.update_log_byte_metrics(&state);
        Ok(removable.len())
    }

    pub fn metrics(&self) -> &Arc<TimerStorageMetrics> {
        &self.metrics
    }

    fn read_locked(
        &self,
        state: &mut SegmentedTimerLogState,
        offset: TimerLogOffset,
    ) -> Result<TimerLogV2Record, SegmentedTimerLogError> {
        if !offset.get().is_multiple_of(TIMER_LOG_V2_LOGICAL_RECORD_SIZE) {
            return Err(SegmentedTimerLogError::UnalignedOffset(offset.get()));
        }
        if offset.get() >= state.next_offset {
            return Err(SegmentedTimerLogError::OffsetOutOfRange {
                offset: offset.get(),
                end: state.next_offset,
            });
        }
        let segment_start = (offset.get() / self.logical_segment_span) * self.logical_segment_span;
        if !state.segments.contains_key(&segment_start) {
            return Err(SegmentedTimerLogError::MissingSegment(segment_start));
        }
        let record_index = (offset.get() - segment_start) / TIMER_LOG_V2_LOGICAL_RECORD_SIZE;
        let mut file = self.cached_reader(state, segment_start)?;
        file.seek(SeekFrom::Start(record_index * TIMER_LOG_V2_PHYSICAL_RECORD_SIZE as u64))?;
        let mut bytes = [0u8; TIMER_LOG_V2_PHYSICAL_RECORD_SIZE];
        file.read_exact(&mut bytes)?;
        TimerLogV2Record::decode(&bytes)
    }

    fn seal_and_rotate(&self, state: &mut SegmentedTimerLogState) -> Result<(), SegmentedTimerLogError> {
        let mut writer = self.open_writer(state.active_start)?;
        writer.seek(SeekFrom::Start(
            self.records_per_segment * TIMER_LOG_V2_PHYSICAL_RECORD_SIZE as u64,
        ))?;
        writer.write_all(&blank_record())?;
        writer.sync_data()?;
        self.metrics
            .record_physical_write(TIMER_LOG_V2_PHYSICAL_RECORD_SIZE as u64);
        if let Some(segment) = state.segments.get_mut(&state.active_start) {
            segment.sealed = true;
        }
        state.active_start += self.logical_segment_span;
        state.active_records = 0;
        state.next_offset = state.active_start;
        self.open_writer(state.active_start)?;
        state.segments.insert(
            state.active_start,
            SegmentState {
                records: 0,
                sealed: false,
            },
        );
        self.metrics.set_segment_count(state.segments.len() as u64);
        self.write_manifest(state)
    }

    fn validate_sealed_segment(&self, start: u64, length: u64) -> Result<(), SegmentedTimerLogError> {
        if length != self.segment_size {
            return Err(SegmentedTimerLogError::InvalidSealedLength {
                segment: start,
                length,
                expected: self.segment_size,
            });
        }
        let mut file = OpenOptions::new().read(true).open(self.segment_path(start))?;
        file.seek(SeekFrom::Start(
            self.records_per_segment * TIMER_LOG_V2_PHYSICAL_RECORD_SIZE as u64,
        ))?;
        let mut bytes = [0u8; TIMER_LOG_V2_PHYSICAL_RECORD_SIZE];
        file.read_exact(&mut bytes)?;
        if !is_blank_record(&bytes) {
            return Err(SegmentedTimerLogError::InvalidBlankMarker(start));
        }
        Ok(())
    }

    fn recover_active_segment(&self, start: u64, length: u64) -> Result<(u64, bool, u64), SegmentedTimerLogError> {
        let mut file = self.open_writer(start)?;
        let complete_records = length / TIMER_LOG_V2_PHYSICAL_RECORD_SIZE as u64;
        let mut valid_data = 0u64;
        let mut sealed = false;
        for index in 0..complete_records.min(self.records_per_segment + 1) {
            file.seek(SeekFrom::Start(index * TIMER_LOG_V2_PHYSICAL_RECORD_SIZE as u64))?;
            let mut bytes = [0u8; TIMER_LOG_V2_PHYSICAL_RECORD_SIZE];
            file.read_exact(&mut bytes)?;
            if is_blank_record(&bytes) {
                sealed = index == self.records_per_segment;
                break;
            }
            if TimerLogV2Record::decode(&bytes).is_err() {
                break;
            }
            valid_data += 1;
        }
        let target_len = if sealed {
            self.segment_size
        } else {
            valid_data * TIMER_LOG_V2_PHYSICAL_RECORD_SIZE as u64
        };
        if length != target_len {
            file.set_len(target_len)?;
            file.sync_data()?;
        }
        Ok((valid_data, sealed, complete_records))
    }

    fn cached_reader(&self, state: &mut SegmentedTimerLogState, start: u64) -> Result<File, SegmentedTimerLogError> {
        if let Some(file) = state.handles.get(&start) {
            let file = file.try_clone()?;
            state.handle_order.retain(|cached| *cached != start);
            state.handle_order.push_back(start);
            return Ok(file);
        }
        let file = OpenOptions::new().read(true).open(self.segment_path(start))?;
        state.handles.insert(start, file.try_clone()?);
        state.handle_order.push_back(start);
        while state.handles.len() > self.handle_limit {
            if let Some(evicted) = state.handle_order.pop_front() {
                state.handles.remove(&evicted);
            }
        }
        Ok(file)
    }

    fn open_writer(&self, start: u64) -> Result<File, SegmentedTimerLogError> {
        Ok(OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(self.segment_path(start))?)
    }

    fn segment_path(&self, start: u64) -> PathBuf {
        self.directory.join(format!("{start:020}"))
    }

    fn read_manifest(&self) -> Result<Option<Manifest>, SegmentedTimerLogError> {
        let path = self.directory.join(MANIFEST_FILE);
        if !path.exists() {
            return Ok(None);
        }
        let mut bytes = Vec::new();
        OpenOptions::new().read(true).open(path)?.read_to_end(&mut bytes)?;
        Ok(Some(Manifest::decode(&bytes)?))
    }

    fn write_manifest(&self, state: &SegmentedTimerLogState) -> Result<(), SegmentedTimerLogError> {
        let manifest = Manifest {
            active_start: state.active_start,
            durable_length: state.durable_length,
            min_live_offset: state.min_live_offset,
        };
        let temporary_path = self.directory.join("manifest.tmp");
        let final_path = self.directory.join(MANIFEST_FILE);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temporary_path)?;
        file.write_all(&manifest.encode())?;
        file.sync_data()?;
        drop(file);
        std::fs::rename(temporary_path, final_path)?;
        Ok(())
    }

    fn update_log_byte_metrics(&self, state: &SegmentedTimerLogState) {
        let live = state.next_offset.saturating_sub(state.min_live_offset);
        let garbage = state.min_live_offset.min(state.next_offset);
        self.metrics.set_log_bytes(live, garbage);
    }
}

#[derive(Clone, Copy)]
struct Manifest {
    active_start: u64,
    durable_length: u64,
    min_live_offset: u64,
}

impl Manifest {
    fn encode(self) -> [u8; MANIFEST_SIZE] {
        let mut bytes = [0u8; MANIFEST_SIZE];
        bytes[0..4].copy_from_slice(&MANIFEST_MAGIC.to_be_bytes());
        bytes[4..6].copy_from_slice(&RECORD_VERSION.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.active_start.to_be_bytes());
        bytes[16..24].copy_from_slice(&self.durable_length.to_be_bytes());
        bytes[24..32].copy_from_slice(&self.min_live_offset.to_be_bytes());
        let checksum = crc32c(&bytes[..36]);
        bytes[36..40].copy_from_slice(&checksum.to_be_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self, SegmentedTimerLogError> {
        if bytes.len() != MANIFEST_SIZE {
            return Err(SegmentedTimerLogError::InvalidManifest);
        }
        if read_u32(bytes, 0) != MANIFEST_MAGIC
            || read_u16(bytes, 4) != RECORD_VERSION
            || crc32c(&bytes[..36]) != read_u32(bytes, 36)
        {
            return Err(SegmentedTimerLogError::InvalidManifest);
        }
        Ok(Self {
            active_start: read_u64(bytes, 8),
            durable_length: read_u64(bytes, 16),
            min_live_offset: read_u64(bytes, 24),
        })
    }
}

#[derive(Debug, Error)]
pub(crate) enum SegmentedTimerLogError {
    #[error("timer log I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("timer log segment size {0} is invalid")]
    InvalidSegmentSize(usize),
    #[error("timer log record length {0} is invalid")]
    InvalidRecordLength(usize),
    #[error("timer log record magic 0x{0:08x} is invalid")]
    InvalidRecordMagic(u32),
    #[error("timer log record version {0} is unsupported")]
    UnsupportedRecordVersion(u16),
    #[error("timer log record checksum does not match")]
    RecordChecksumMismatch,
    #[error("timer log segment name {0} is invalid")]
    InvalidSegmentName(String),
    #[error("timer log has a segment hole: expected {expected}, found {actual}")]
    SegmentHole { expected: u64, actual: u64 },
    #[error("sealed segment {segment} has length {length}, expected {expected}")]
    InvalidSealedLength { segment: u64, length: u64, expected: u64 },
    #[error("timer log segment {0} has an invalid blank marker")]
    InvalidBlankMarker(u64),
    #[error("timer log manifest is invalid")]
    InvalidManifest,
    #[error("timer log offset {0} is not aligned")]
    UnalignedOffset(u64),
    #[error("timer log offset {offset} is beyond end {end}")]
    OffsetOutOfRange { offset: u64, end: u64 },
    #[error("timer log segment starting at {0} is missing")]
    MissingSegment(u64),
    #[error("cannot flush timer log to {requested}; current end is {end}")]
    FlushBeyondEnd { requested: u64, end: u64 },
    #[error("cannot truncate timer log to {requested}; current end is {end}")]
    TruncateBeyondEnd { requested: u64, end: u64 },
}

fn blank_record() -> [u8; TIMER_LOG_V2_PHYSICAL_RECORD_SIZE] {
    let mut bytes = [0u8; TIMER_LOG_V2_PHYSICAL_RECORD_SIZE];
    bytes[0..4].copy_from_slice(&BLANK_MAGIC.to_be_bytes());
    bytes[4..6].copy_from_slice(&RECORD_VERSION.to_be_bytes());
    bytes[6..8].copy_from_slice(&(TIMER_LOG_V2_PHYSICAL_RECORD_SIZE as u16).to_be_bytes());
    let checksum = crc32c(&bytes[..76]);
    bytes[76..80].copy_from_slice(&checksum.to_be_bytes());
    bytes
}

fn is_blank_record(bytes: &[u8]) -> bool {
    bytes.len() == TIMER_LOG_V2_PHYSICAL_RECORD_SIZE
        && read_u32(bytes, 0) == BLANK_MAGIC
        && read_u16(bytes, 4) == RECORD_VERSION
        && read_u16(bytes, 6) as usize == TIMER_LOG_V2_PHYSICAL_RECORD_SIZE
        && crc32c(&bytes[..76]) == read_u32(bytes, 76)
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes(bytes[offset..offset + 2].try_into().expect("fixed u16 field"))
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(bytes[offset..offset + 4].try_into().expect("fixed u32 field"))
}

fn read_i32(bytes: &[u8], offset: usize) -> i32 {
    i32::from_be_bytes(bytes[offset..offset + 4].try_into().expect("fixed i32 field"))
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(bytes[offset..offset + 8].try_into().expect("fixed u64 field"))
}

fn read_i64(bytes: &[u8], offset: usize) -> i64 {
    i64::from_be_bytes(bytes[offset..offset + 8].try_into().expect("fixed i64 field"))
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    fn record(queue_offset: i64) -> TimerLogV2Record {
        TimerLogV2Record {
            previous_offset: if queue_offset == 0 { -1 } else { (queue_offset - 1) * 40 },
            source_physical_offset: queue_offset * 100,
            source_size: 64,
            timer_magic: 1,
            deliver_time_ms: 1_000,
            slot_time_ms: 1_000,
            generation: 7,
            source_queue_offset: queue_offset,
        }
    }

    #[test]
    fn codec_rejects_unknown_version_and_bad_crc() {
        let encoded = record(1).encode();
        assert_eq!(TimerLogV2Record::decode(&encoded).unwrap(), record(1));
        let mut unknown = encoded;
        unknown[5] = 3;
        assert!(matches!(
            TimerLogV2Record::decode(&unknown),
            Err(SegmentedTimerLogError::UnsupportedRecordVersion(3))
        ));
        let mut corrupt = encoded;
        corrupt[20] ^= 1;
        assert!(matches!(
            TimerLogV2Record::decode(&corrupt),
            Err(SegmentedTimerLogError::RecordChecksumMismatch)
        ));
    }

    #[test]
    fn append_crosses_segments_without_changing_logical_address_unit() {
        let directory = tempdir().unwrap();
        let log = SegmentedTimerLog::new(
            directory.path(),
            TIMER_LOG_V2_PHYSICAL_RECORD_SIZE * 4,
            2,
            Arc::new(TimerStorageMetrics::default()),
        )
        .unwrap();
        log.load().unwrap();
        let offsets = log.append_batch(&(0..7).map(record).collect::<Vec<_>>()).unwrap();
        assert_eq!(
            offsets.iter().map(|offset| offset.get()).collect::<Vec<_>>(),
            vec![0, 40, 80, 120, 160, 200, 240]
        );
        log.flush().unwrap();
        assert_eq!(log.read(TimerLogOffset::new(160)).unwrap(), record(4));
        assert!(directory.path().join("00000000000000000120").exists());
        assert!(directory.path().join("00000000000000000240").exists());
    }

    #[test]
    fn recovery_truncates_a_short_or_corrupt_active_tail() {
        let directory = tempdir().unwrap();
        let metrics = Arc::new(TimerStorageMetrics::default());
        let log = SegmentedTimerLog::new(
            directory.path(),
            TIMER_LOG_V2_PHYSICAL_RECORD_SIZE * 4,
            2,
            Arc::clone(&metrics),
        )
        .unwrap();
        log.load().unwrap();
        log.append(record(0)).unwrap();
        let path = directory.path().join("00000000000000000000");
        OpenOptions::new()
            .append(true)
            .open(path)
            .unwrap()
            .write_all(&[1, 2, 3])
            .unwrap();

        let reloaded =
            SegmentedTimerLog::new(directory.path(), TIMER_LOG_V2_PHYSICAL_RECORD_SIZE * 4, 2, metrics).unwrap();
        reloaded.load().unwrap();
        assert_eq!(reloaded.len(), 40);
    }
}
