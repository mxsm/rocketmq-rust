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

//! Payload copies: for a timer due later than the CommitLog keeps its files, the whole source
//! record is copied once, as a self-checking `TimerPayloadRecordV1`, into a segment of the
//! timer's UTC due day. Segments are appended like bucket files, synced by the group commit,
//! and cut back to the checkpoint on open.

use std::collections::BTreeMap;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::ErrorKind;
use std::path::Path;
use std::path::PathBuf;

use tracing::warn;

use super::cut_back;
use super::LogFile;
use super::NativeStorageError;
use super::NativeTimerStorage;
use super::StorageState;
use crate::timer::native_format::NativePayloadSegment;
use crate::timer::native_format::NativePayloadSegmentLength;
use crate::timer::native_format::NATIVE_PAYLOAD_DIR;
use crate::timer::payload_record::TimerPayloadRecordV1;
use crate::timer::positional_io::read_exact_at;
use crate::timer::positional_io::write_all_at;

/// Where a payload copy was written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativePayloadLocation {
    pub segment: NativePayloadSegment,
    pub offset: u32,
    pub length: u32,
}

impl NativeTimerStorage {
    /// Appends encoded payload records of timers due on `due_day_utc` and returns where each
    /// landed. A record goes to the day's last segment, or starts the next one when it would
    /// pass the segment size. Nothing is synced.
    ///
    /// # Errors
    ///
    /// Returns [`NativeStorageError::PayloadTooLarge`] for a record larger than a segment, and
    /// an I/O error when a segment cannot be created or written. Records written before the
    /// failure stay in their segments unreferenced, and are removed with their day.
    pub fn append_payloads(
        &self,
        due_day_utc: i32,
        records: &[Vec<u8>],
    ) -> Result<Vec<NativePayloadLocation>, NativeStorageError> {
        if let Some(record) = records
            .iter()
            .find(|record| record.len() as u64 > self.payload_segment_bytes)
        {
            return Err(NativeStorageError::PayloadTooLarge {
                length: record.len(),
                segment_bytes: self.payload_segment_bytes,
            });
        }
        let mut state = self.state.lock();
        let mut segment = state
            .payloads
            .range(day_range(due_day_utc))
            .next_back()
            .map(|(segment, _)| *segment)
            .unwrap_or(NativePayloadSegment {
                due_day_utc,
                segment_id: 0,
            });
        let mut start = state.payloads.get(&segment).map_or(0, |file| file.length);
        let mut length = start;
        let mut pending = Vec::new();
        let mut locations = Vec::with_capacity(records.len());
        for record in records {
            if length + record.len() as u64 > self.payload_segment_bytes {
                self.write_payloads(&mut state, segment, start, &pending)?;
                segment.segment_id += 1;
                start = 0;
                length = 0;
                pending.clear();
            }
            locations.push(NativePayloadLocation {
                segment,
                // Below the segment size, which fits 32 bits.
                offset: length as u32,
                length: record.len() as u32,
            });
            pending.extend_from_slice(record);
            length += record.len() as u64;
        }
        self.write_payloads(&mut state, segment, start, &pending)?;
        Ok(locations)
    }

    /// Reads the payload copy at `location`.
    ///
    /// # Errors
    ///
    /// Returns [`NativeStorageError::CorruptPayload`] when nothing was written there or the bytes
    /// do not form a valid record, and an I/O error when the segment cannot be read.
    pub fn read_payload(&self, location: NativePayloadLocation) -> Result<TimerPayloadRecordV1, NativeStorageError> {
        let corrupt = NativeStorageError::CorruptPayload { location };
        let file = {
            let mut state = self.state.lock();
            let written = state.payloads.get(&location.segment).map_or(0, |file| file.length);
            if u64::from(location.offset) + u64::from(location.length) > written {
                return Err(corrupt);
            }
            self.payload_handle(&mut state, location.segment)?.try_clone()?
        };
        let mut bytes = vec![0u8; location.length as usize];
        read_exact_at(&file, &mut bytes, u64::from(location.offset))?;
        TimerPayloadRecordV1::decode(&bytes).ok_or(corrupt)
    }

    /// Payload segments written since the last sync, with their lengths and handles to sync.
    pub(super) fn unsynced_payloads(
        &self,
        state: &mut StorageState,
    ) -> Result<Vec<(NativePayloadSegment, u64, File)>, NativeStorageError> {
        let pending: Vec<_> = state
            .payloads
            .iter()
            .filter(|(_, file)| file.length > file.durable)
            .map(|(segment, file)| (*segment, file.length))
            .collect();
        let mut segments = Vec::with_capacity(pending.len());
        for (segment, length) in pending {
            segments.push((segment, length, self.payload_handle(state, segment)?.try_clone()?));
        }
        Ok(segments)
    }

    /// Writes `bytes` to `segment` at `start`, creating the segment when it has no file.
    fn write_payloads(
        &self,
        state: &mut StorageState,
        segment: NativePayloadSegment,
        start: u64,
        bytes: &[u8],
    ) -> Result<(), NativeStorageError> {
        if bytes.is_empty() {
            return Ok(());
        }
        if !state.payloads.contains_key(&segment) {
            let path = self.root.join(segment.relative_path());
            if let Some(day_dir) = path.parent() {
                if !day_dir.exists() {
                    fs::create_dir_all(day_dir)?;
                    state.changed_dirs.insert(self.root.clone());
                    state.changed_dirs.insert(self.root.join(NATIVE_PAYLOAD_DIR));
                }
                state.changed_dirs.insert(day_dir.to_path_buf());
            }
            let file = OpenOptions::new().create_new(true).read(true).write(true).open(path)?;
            state.payload_handles.insert(segment, file);
            state.payloads.insert(segment, LogFile { length: 0, durable: 0 });
        }
        write_all_at(self.payload_handle(state, segment)?, bytes, start)?;
        if let Some(file) = state.payloads.get_mut(&segment) {
            file.length = start + bytes.len() as u64;
        }
        Ok(())
    }

    fn payload_handle<'a>(
        &self,
        state: &'a mut StorageState,
        segment: NativePayloadSegment,
    ) -> Result<&'a File, NativeStorageError> {
        let path = self.root.join(segment.relative_path());
        Ok(state
            .payload_handles
            .get_or_open(segment, || OpenOptions::new().read(true).write(true).open(path))?)
    }
}

/// Cuts the payload segments back to the lengths `recorded` by the checkpoint, and deletes
/// those it does not record; with no checkpoint, every segment.
pub(super) fn recover(
    root: &Path,
    recorded: Option<&[NativePayloadSegmentLength]>,
) -> Result<BTreeMap<NativePayloadSegment, LogFile>, NativeStorageError> {
    let mut durable: BTreeMap<NativePayloadSegment, u64> = recorded
        .unwrap_or_default()
        .iter()
        .map(|recorded| {
            (
                NativePayloadSegment {
                    due_day_utc: recorded.due_day_utc,
                    segment_id: recorded.segment_id,
                },
                recorded.durable_length,
            )
        })
        .collect();
    let mut segments = BTreeMap::new();
    for (segment, path) in scan(root)? {
        let length = match durable.remove(&segment) {
            Some(length) if length > 0 => length,
            // Written after the checkpoint; the replay copies it again.
            _ => {
                fs::remove_file(&path)?;
                continue;
            }
        };
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        cut_back(&file, &path, length)?;
        segments.insert(
            segment,
            LogFile {
                length,
                durable: length,
            },
        );
    }
    if let Some((segment, length)) = durable.into_iter().find(|(_, length)| *length > 0) {
        return Err(NativeStorageError::ShorterThanCheckpoint {
            path: root.join(segment.relative_path()),
            durable: length,
            actual: 0,
        });
    }
    Ok(segments)
}

/// The non-empty segments with the length `length` picks.
pub(super) fn lengths(
    payloads: &BTreeMap<NativePayloadSegment, LogFile>,
    length: impl Fn(&LogFile) -> u64,
) -> Vec<NativePayloadSegmentLength> {
    payloads
        .iter()
        .filter(|(_, file)| length(file) > 0)
        .map(|(segment, file)| NativePayloadSegmentLength {
            due_day_utc: segment.due_day_utc,
            segment_id: segment.segment_id,
            durable_length: length(file),
        })
        .collect()
}

fn day_range(due_day_utc: i32) -> std::ops::RangeInclusive<NativePayloadSegment> {
    NativePayloadSegment {
        due_day_utc,
        segment_id: 0,
    }..=NativePayloadSegment {
        due_day_utc,
        segment_id: u32::MAX,
    }
}

/// The payload segments on disk. Names that are not segments are left alone.
fn scan(root: &Path) -> Result<Vec<(NativePayloadSegment, PathBuf)>, NativeStorageError> {
    let days = match fs::read_dir(root.join(NATIVE_PAYLOAD_DIR)) {
        Ok(days) => days,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut segments = Vec::new();
    for day in days {
        let day = day?;
        if !day.file_type()?.is_dir() {
            warn!(path = %day.path().display(), "skip a file that is not a native payload day");
            continue;
        }
        let day_name = day.file_name();
        for entry in fs::read_dir(day.path())? {
            let entry = entry?;
            let segment = day_name
                .to_str()
                .zip(entry.file_name().to_str())
                .and_then(|(day, file)| NativePayloadSegment::parse(day, file));
            match segment {
                Some(segment) if entry.file_type()?.is_file() => segments.push((segment, entry.path())),
                _ => warn!(path = %entry.path().display(), "skip a file that is not a native payload segment"),
            }
        }
    }
    Ok(segments)
}
