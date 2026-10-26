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

//! The Apache RocketMQ 5.x `timerlog` directory: files named after their first offset, each
//! exactly one file size long, holding 52-byte units and closed by a blank.
//!
//! Files are written with positional I/O instead of a memory map, but the bytes on disk are
//! the ones Java's `TimerLog` writes, so either Broker can open the other's directory.

use std::collections::hash_map::Entry;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fs::File;
use std::fs::OpenOptions;
use std::path::Path;
use std::path::PathBuf;

use parking_lot::Mutex;
use thiserror::Error;

use crate::timer::java_format::encode_java_timer_log_blank;
use crate::timer::java_format::java_timer_log_last_unit_offset;
use crate::timer::java_format::java_timer_log_needs_next_file;
use crate::timer::java_format::JavaTimerFormatError;
use crate::timer::java_format::JavaTimerLogRecord;
use crate::timer::java_format::JavaTimerLogUnit;
use crate::timer::java_format::JAVA_TIMER_LOG_MIN_BLANK_LEN;
use crate::timer::java_format::JAVA_TIMER_LOG_UNIT_SIZE;
use crate::timer::positional_io::read_exact_at;
use crate::timer::positional_io::write_all_at;

const UNIT_SIZE: u64 = JAVA_TIMER_LOG_UNIT_SIZE as u64;
const SCAN_CHUNK_UNITS: u64 = 1_024;
const HANDLE_LIMIT: usize = 32;

/// How [`JavaTimerLog::recover`] treats the units it scans (`checkTimerLog` in Java's
/// `TimerMessageStore.recoverAndRevise`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JavaTimerLogScan {
    /// Scans whole files, stops at the first unit that is not valid and cuts the log there.
    Repair,
    /// Scans up to the current end, skipping units that are not valid; the log is unchanged.
    Revise,
}

/// A Java-format TimerLog that cannot be opened, read or written.
#[derive(Debug, Error)]
pub enum JavaTimerLogError {
    #[error("timer log I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("timer log file size {0} must hold a unit and a blank and fit in a Java int")]
    InvalidFileSize(u64),
    #[error("timer log file {start:020} has {length} bytes, expected {expected}")]
    FileLength { start: u64, length: u64, expected: u64 },
    #[error("timer log file {0:020} does not start at a multiple of the file size")]
    UnalignedFile(u64),
    #[error("timer log files are not contiguous: expected {expected:020}, found {actual:020}")]
    FileHole { expected: u64, actual: u64 },
    #[error("timer log offset {offset} is outside the log [{min}, {end})")]
    OffsetOutOfRange { offset: u64, min: u64, end: u64 },
    #[error("timer log offset {offset} does not hold a unit")]
    NotAUnit {
        offset: u64,
        #[source]
        source: Option<JavaTimerFormatError>,
    },
}

/// Offset a unit appended at `end` lands at: `end` itself, or the start of the next file when
/// the unit and the blank no longer fit (`TimerLog.append`). Callers chain the units of one
/// batch with it before the batch is written.
pub const fn java_timer_log_unit_offset(file_size: u64, end: u64) -> u64 {
    if file_size == 0 {
        return end;
    }
    let position = end % file_size;
    if position != 0 && java_timer_log_needs_next_file(file_size as usize, position as usize, JAVA_TIMER_LOG_UNIT_SIZE)
    {
        end - position + file_size
    } else {
        end
    }
}

/// A `timerlog` directory in the Apache RocketMQ 5.x format.
pub struct JavaTimerLog {
    directory: PathBuf,
    file_size: u64,
    state: Mutex<JavaTimerLogState>,
}

#[derive(Default)]
struct JavaTimerLogState {
    /// First offsets of the files, ascending and contiguous.
    files: BTreeSet<u64>,
    /// Offset the next byte is written at.
    end: u64,
    flushed: u64,
    /// Files written since their last flush.
    dirty: BTreeSet<u64>,
    handles: HashMap<u64, File>,
    handle_order: VecDeque<u64>,
}

impl JavaTimerLog {
    /// Creates a log over `directory` whose files are `file_size` bytes, Java's
    /// `mappedFileSizeTimerLog`. Nothing is read until [`load`](Self::load).
    ///
    /// # Errors
    ///
    /// Returns [`JavaTimerLogError::InvalidFileSize`] when a file cannot hold a unit and the
    /// blank, or is larger than a Java `int`.
    pub fn new(directory: impl AsRef<Path>, file_size: u64) -> Result<Self, JavaTimerLogError> {
        if file_size < (JAVA_TIMER_LOG_UNIT_SIZE + JAVA_TIMER_LOG_MIN_BLANK_LEN) as u64 || file_size > i32::MAX as u64 {
            return Err(JavaTimerLogError::InvalidFileSize(file_size));
        }
        Ok(Self {
            directory: directory.as_ref().to_path_buf(),
            file_size,
            state: Mutex::new(JavaTimerLogState::default()),
        })
    }

    /// Finds the files as Java's `MappedFileQueue.load` does: an empty last file is deleted,
    /// and every other file must be exactly one file size. Subdirectories and names that are
    /// not 20 digits are skipped. Like Java, the end is the end of the last file until
    /// [`recover`](Self::recover) finds the real one.
    ///
    /// # Errors
    ///
    /// Returns [`JavaTimerLogError`] when the directory cannot be read, or when a file has the
    /// wrong length, is not aligned to the file size, or leaves a hole.
    pub fn load(&self) -> Result<(), JavaTimerLogError> {
        std::fs::create_dir_all(&self.directory)?;
        let mut starts = Vec::new();
        for entry in std::fs::read_dir(&self.directory)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let Some(start) = entry.file_name().to_str().and_then(parse_file_name) else {
                continue;
            };
            starts.push((start, entry.metadata()?.len()));
        }
        starts.sort_unstable();
        if let Some(&(start, 0)) = starts.last() {
            std::fs::remove_file(self.file_path(start))?;
            starts.pop();
        }
        let mut files = BTreeSet::new();
        for (start, length) in starts {
            if length != self.file_size {
                return Err(JavaTimerLogError::FileLength {
                    start,
                    length,
                    expected: self.file_size,
                });
            }
            if start % self.file_size != 0 {
                return Err(JavaTimerLogError::UnalignedFile(start));
            }
            if let Some(last) = files.last() {
                let expected = last + self.file_size;
                if start != expected {
                    return Err(JavaTimerLogError::FileHole {
                        expected,
                        actual: start,
                    });
                }
            }
            files.insert(start);
        }
        let end = files.last().map_or(0, |last| last + self.file_size);
        *self.state.lock() = JavaTimerLogState {
            files,
            end,
            flushed: end,
            ..JavaTimerLogState::default()
        };
        Ok(())
    }

    /// Scans the log from the file that holds `begin_offset`, as Java's `recoverAndRevise`
    /// does, and calls `revise` with the offset of every valid unit. A blank ends a file.
    /// Returns the offset where the scan stopped.
    ///
    /// With [`JavaTimerLogScan::Repair`] the scan stops at the first unit that is not valid and
    /// the log ends there: later files are deleted and the rest of the file is zeroed, so units
    /// left behind a torn write cannot come back on the next recovery. Java leaves those bytes.
    ///
    /// `revise` runs while the log is locked and must not call back into it.
    ///
    /// # Errors
    ///
    /// Returns [`JavaTimerLogError::Io`] when a file cannot be read, cut or deleted.
    pub fn recover(
        &self,
        begin_offset: u64,
        scan: JavaTimerLogScan,
        mut revise: impl FnMut(u64, &JavaTimerLogUnit),
    ) -> Result<u64, JavaTimerLogError> {
        let mut state = self.state.lock();
        let Some(&first) = state.files.first() else {
            return Ok(0);
        };
        let begin_file = state.files.range(..=begin_offset).next_back().copied().unwrap_or(first);
        let files: Vec<u64> = state.files.range(begin_file..).copied().collect();
        let mut check_offset = begin_file;
        let mut buffer = vec![0u8; (SCAN_CHUNK_UNITS * UNIT_SIZE) as usize];
        for start in files {
            let limit = match scan {
                JavaTimerLogScan::Repair => self.file_size,
                JavaTimerLogScan::Revise => state.end.saturating_sub(start).min(self.file_size),
            };
            let (position, stopped) = self.scan_file(&mut state, start, limit, scan, &mut buffer, &mut revise)?;
            check_offset = start + position;
            if stopped {
                break;
            }
        }
        if scan == JavaTimerLogScan::Repair {
            self.cut(&mut state, check_offset)?;
        }
        Ok(check_offset)
    }

    /// Appends one unit and returns its offset.
    ///
    /// # Errors
    ///
    /// Returns [`JavaTimerLogError::Io`] when a file cannot be created or written.
    pub fn append(&self, unit: &JavaTimerLogUnit) -> Result<u64, JavaTimerLogError> {
        let offsets = self.append_batch(std::slice::from_ref(unit))?;
        Ok(offsets.first().copied().unwrap_or_default())
    }

    /// Appends units in order and returns their offsets. When a unit and the blank no longer
    /// fit, the file is closed with a blank and the unit goes to the next file, as in Java's
    /// `TimerLog.append`. Units that land in one file are written together.
    ///
    /// # Errors
    ///
    /// Returns [`JavaTimerLogError::Io`] when a file cannot be created or written. Units before
    /// the failed write stay in the log.
    pub fn append_batch(&self, units: &[JavaTimerLogUnit]) -> Result<Vec<u64>, JavaTimerLogError> {
        let mut state = self.state.lock();
        let mut offsets = Vec::with_capacity(units.len());
        let mut pending = units;
        while !pending.is_empty() {
            let start = self.writable_file(&mut state)?;
            let position = state.end - start;
            if java_timer_log_needs_next_file(self.file_size as usize, position as usize, JAVA_TIMER_LOG_UNIT_SIZE) {
                let blank = encode_java_timer_log_blank(self.file_size as usize, position as usize);
                write_all_at(self.handle(&mut state, start)?, &blank, position)?;
                state.dirty.insert(start);
                state.end = start + self.file_size;
                continue;
            }
            let fitting = (self.file_size - position - JAVA_TIMER_LOG_MIN_BLANK_LEN as u64) / UNIT_SIZE;
            let count = (fitting as usize).min(pending.len());
            let mut bytes = Vec::with_capacity(count * JAVA_TIMER_LOG_UNIT_SIZE);
            for unit in &pending[..count] {
                bytes.extend_from_slice(&unit.encode());
            }
            write_all_at(self.handle(&mut state, start)?, &bytes, position)?;
            state.dirty.insert(start);
            // The end moves only after the units are in the file.
            offsets.extend((0..count as u64).map(|index| state.end + index * UNIT_SIZE));
            state.end += count as u64 * UNIT_SIZE;
            pending = &pending[count..];
        }
        Ok(offsets)
    }

    /// Reads the unit at `offset`.
    ///
    /// # Errors
    ///
    /// Returns [`JavaTimerLogError::OffsetOutOfRange`] when no unit ends before the end of the
    /// log at `offset`, [`JavaTimerLogError::NotAUnit`] when the bytes there are a blank or not
    /// a valid unit, and [`JavaTimerLogError::Io`] when the file cannot be read.
    pub fn read_unit(&self, offset: u64) -> Result<JavaTimerLogUnit, JavaTimerLogError> {
        let mut state = self.state.lock();
        let start = offset - offset % self.file_size;
        let min = state.files.first().copied().unwrap_or_default();
        if !state.files.contains(&start)
            || offset % self.file_size + UNIT_SIZE > self.file_size
            || offset + UNIT_SIZE > state.end
        {
            return Err(JavaTimerLogError::OffsetOutOfRange {
                offset,
                min,
                end: state.end,
            });
        }
        let mut bytes = [0u8; JAVA_TIMER_LOG_UNIT_SIZE];
        read_exact_at(self.handle(&mut state, start)?, &mut bytes, offset - start)?;
        match JavaTimerLogRecord::decode(&bytes) {
            Ok(JavaTimerLogRecord::Unit(unit)) => Ok(unit),
            Ok(JavaTimerLogRecord::Blank { .. }) => Err(JavaTimerLogError::NotAUnit { offset, source: None }),
            Err(error) => Err(JavaTimerLogError::NotAUnit {
                offset,
                source: Some(error),
            }),
        }
    }

    /// Makes every write so far durable and returns the flushed offset. Appends can continue
    /// while the files sync.
    ///
    /// # Errors
    ///
    /// Returns [`JavaTimerLogError::Io`] when a file cannot be synced; the flushed offset then
    /// stays where it was.
    pub fn flush(&self) -> Result<u64, JavaTimerLogError> {
        let (end, files) = {
            let mut state = self.state.lock();
            let dirty = std::mem::take(&mut state.dirty);
            let mut files = Vec::with_capacity(dirty.len());
            for start in &dirty {
                match self.handle(&mut state, *start).and_then(|file| Ok(file.try_clone()?)) {
                    Ok(file) => files.push((*start, file)),
                    Err(error) => {
                        state.dirty.extend(dirty);
                        return Err(error);
                    }
                }
            }
            (state.end, files)
        };
        for (index, (_, file)) in files.iter().enumerate() {
            if let Err(error) = file.sync_data() {
                self.state
                    .lock()
                    .dirty
                    .extend(files[index..].iter().map(|(start, _)| *start));
                return Err(error.into());
            }
        }
        let mut state = self.state.lock();
        state.flushed = state.flushed.max(end);
        Ok(state.flushed)
    }

    /// Deletes leading files whose last unit points below `commit_log_min_offset`, as Java's
    /// `MappedFileQueue.deleteExpiredFileByOffsetForTimerLog`. Every message such a file refers
    /// to has left the CommitLog. Stops at the first file it keeps and never deletes the last
    /// file. Returns how many files were deleted.
    ///
    /// # Errors
    ///
    /// Returns [`JavaTimerLogError::Io`] when a file cannot be read or deleted.
    pub fn delete_expired_files(&self, commit_log_min_offset: i64) -> Result<usize, JavaTimerLogError> {
        let Some(last_unit) = java_timer_log_last_unit_offset(self.file_size as usize) else {
            return Ok(0);
        };
        let mut state = self.state.lock();
        let older = state.files.len().saturating_sub(1);
        let candidates: Vec<u64> = state.files.iter().take(older).copied().collect();
        let mut deleted = 0;
        for start in candidates {
            let mut bytes = [0u8; JAVA_TIMER_LOG_UNIT_SIZE];
            read_exact_at(self.handle(&mut state, start)?, &mut bytes, last_unit as u64)?;
            let expired = matches!(
                JavaTimerLogRecord::decode(&bytes),
                Ok(JavaTimerLogRecord::Unit(unit)) if unit.commit_log_offset < commit_log_min_offset
            );
            if !expired {
                break;
            }
            self.close(&mut state, start);
            std::fs::remove_file(self.file_path(start))?;
            state.files.remove(&start);
            state.dirty.remove(&start);
            deleted += 1;
        }
        Ok(deleted)
    }

    /// Bytes of each file.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// First offset of the oldest file, or `0` before the first file exists.
    pub fn min_offset(&self) -> u64 {
        self.state.lock().files.first().copied().unwrap_or_default()
    }

    /// Offset the next write starts at.
    pub fn end_offset(&self) -> u64 {
        self.state.lock().end
    }

    /// Offset up to which the log is durable.
    pub fn flushed_offset(&self) -> u64 {
        self.state.lock().flushed
    }

    /// Scans one file from position 0 up to `limit`, returning where the scan stopped and
    /// whether the whole recovery stops there.
    fn scan_file(
        &self,
        state: &mut JavaTimerLogState,
        start: u64,
        limit: u64,
        scan: JavaTimerLogScan,
        buffer: &mut [u8],
        revise: &mut impl FnMut(u64, &JavaTimerLogUnit),
    ) -> Result<(u64, bool), JavaTimerLogError> {
        let mut position = 0;
        while position < limit {
            let chunk_len = (limit - position).min(buffer.len() as u64) as usize;
            let chunk = &mut buffer[..chunk_len];
            read_exact_at(self.handle(state, start)?, chunk, position)?;
            for record in chunk.chunks(JAVA_TIMER_LOG_UNIT_SIZE) {
                match JavaTimerLogRecord::decode(record) {
                    Ok(JavaTimerLogRecord::Blank { length }) => {
                        let length = u64::try_from(length).unwrap_or_default();
                        return Ok(((position + length).clamp(position, self.file_size), false));
                    }
                    Ok(JavaTimerLogRecord::Unit(unit)) => revise(start + position, &unit),
                    Err(JavaTimerFormatError::Truncated { .. }) => return Ok((position, true)),
                    Err(_) if scan == JavaTimerLogScan::Repair => return Ok((position, true)),
                    Err(_) => {}
                }
                position += UNIT_SIZE;
            }
        }
        Ok((position.min(limit), false))
    }

    /// Ends the log at `offset`: later files are deleted and the rest of the file that holds
    /// `offset` is zeroed (`MappedFileQueue.truncateDirtyFiles`).
    fn cut(&self, state: &mut JavaTimerLogState, offset: u64) -> Result<(), JavaTimerLogError> {
        let later: Vec<u64> = state.files.range(offset + 1..).copied().collect();
        for start in later {
            self.close(state, start);
            std::fs::remove_file(self.file_path(start))?;
            state.files.remove(&start);
            state.dirty.remove(&start);
        }
        let start = offset - offset % self.file_size;
        if state.files.contains(&start) {
            // Shrinking the file and growing it again zero-fills the dirty tail.
            let file = self.handle(state, start)?;
            file.set_len(offset - start)?;
            file.set_len(self.file_size)?;
            file.sync_data()?;
        }
        state.end = offset;
        state.flushed = offset;
        Ok(())
    }

    /// First offset of the file the next write goes to, creating it when the log has no
    /// file yet or the last one is full.
    fn writable_file(&self, state: &mut JavaTimerLogState) -> Result<u64, JavaTimerLogError> {
        let start = match state.files.last() {
            Some(last) if state.end < last + self.file_size => return Ok(*last),
            Some(last) => last + self.file_size,
            None => 0,
        };
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(self.file_path(start))?;
        file.set_len(self.file_size)?;
        self.cache(state, start, file);
        state.files.insert(start);
        state.end = start;
        Ok(start)
    }

    fn handle<'a>(&self, state: &'a mut JavaTimerLogState, start: u64) -> Result<&'a File, JavaTimerLogError> {
        if !state.handles.contains_key(&start) {
            let file = OpenOptions::new().read(true).write(true).open(self.file_path(start))?;
            self.cache(state, start, file);
        } else if state.handle_order.back() != Some(&start) {
            state.handle_order.retain(|cached| *cached != start);
            state.handle_order.push_back(start);
        }
        state.handles.get(&start).ok_or_else(|| {
            JavaTimerLogError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("timer log file {start:020} has no open handle"),
            ))
        })
    }

    fn cache(&self, state: &mut JavaTimerLogState, start: u64, file: File) {
        match state.handles.entry(start) {
            Entry::Occupied(mut entry) => {
                entry.insert(file);
            }
            Entry::Vacant(entry) => {
                entry.insert(file);
                state.handle_order.push_back(start);
            }
        }
        while state.handles.len() > HANDLE_LIMIT {
            let Some(evicted) = state.handle_order.pop_front() else {
                break;
            };
            state.handles.remove(&evicted);
        }
    }

    /// Drops the handle of one file, so the file can be deleted on every platform.
    fn close(&self, state: &mut JavaTimerLogState, start: u64) {
        state.handles.remove(&start);
        state.handle_order.retain(|cached| *cached != start);
    }

    fn file_path(&self, start: u64) -> PathBuf {
        self.directory.join(format!("{start:020}"))
    }
}

fn parse_file_name(name: &str) -> Option<u64> {
    if name.len() == 20 && name.bytes().all(|byte| byte.is_ascii_digit()) {
        name.parse().ok()
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::timer::java_format::JAVA_TIMER_LOG_BLANK_MAGIC;

    // Three units fit in a 200-byte file: the fourth would leave 44 bytes, less than a unit
    // and a blank, so the file ends with a 44-byte blank at 156.
    const FILE_SIZE: u64 = 200;

    fn unit(commit_log_offset: i64) -> JavaTimerLogUnit {
        JavaTimerLogUnit {
            prev_pos: -1,
            magic: 1,
            curr_write_time_ms: 1_700_000_000_000,
            delay_ms: 5_000,
            commit_log_offset,
            size: 128,
            topic_hash: 7,
            reserved: 0,
        }
    }

    fn opened(directory: &Path) -> JavaTimerLog {
        let log = JavaTimerLog::new(directory, FILE_SIZE).unwrap();
        log.load().unwrap();
        log
    }

    fn recovered(log: &JavaTimerLog, begin: u64, scan: JavaTimerLogScan) -> (u64, Vec<(u64, i64)>) {
        let mut units = Vec::new();
        let end = log
            .recover(begin, scan, |offset, unit| units.push((offset, unit.commit_log_offset)))
            .unwrap();
        (end, units)
    }

    #[test]
    fn appends_fill_a_file_then_close_it_with_a_blank_like_java() {
        let directory = tempdir().unwrap();
        let log = opened(directory.path());
        let units: Vec<_> = (0..5).map(|index| unit(index * 100)).collect();

        assert_eq!(log.append_batch(&units).unwrap(), vec![0, 52, 104, 200, 252]);
        assert_eq!(log.end_offset(), 304);
        assert_eq!(java_timer_log_unit_offset(FILE_SIZE, 156), 200);
        assert_eq!(java_timer_log_unit_offset(FILE_SIZE, 304), 304);

        let first = std::fs::read(directory.path().join("00000000000000000000")).unwrap();
        assert_eq!(first.len(), FILE_SIZE as usize);
        assert_eq!(&first[0..52], &units[0].encode());
        assert_eq!(&first[104..156], &units[2].encode());
        assert_eq!(&first[156..160], &44i32.to_be_bytes());
        assert_eq!(&first[160..168], &[0; 8]);
        assert_eq!(&first[168..172], &JAVA_TIMER_LOG_BLANK_MAGIC.to_be_bytes());
        let second = std::fs::read(directory.path().join("00000000000000000200")).unwrap();
        assert_eq!(second.len(), FILE_SIZE as usize);
        assert_eq!(&second[52..104], &units[4].encode());
        assert!(second[104..].iter().all(|byte| *byte == 0));

        assert_eq!(log.read_unit(252).unwrap(), units[4]);
        assert!(matches!(
            log.read_unit(156),
            Err(JavaTimerLogError::OffsetOutOfRange { .. })
        ));
        assert!(matches!(
            log.read_unit(304),
            Err(JavaTimerLogError::OffsetOutOfRange { .. })
        ));
        assert_eq!(log.flush().unwrap(), 304);
        assert_eq!(log.flushed_offset(), 304);
    }

    #[test]
    fn repair_finds_the_end_java_would_and_appends_continue_there() {
        let directory = tempdir().unwrap();
        let log = opened(directory.path());
        log.append_batch(&(0..5).map(|index| unit(index * 100)).collect::<Vec<_>>())
            .unwrap();
        drop(log);

        let reopened = opened(directory.path());
        // Java loads every file as full until recovery finds the end.
        assert_eq!(reopened.end_offset(), 400);
        let (end, units) = recovered(&reopened, 0, JavaTimerLogScan::Repair);
        assert_eq!(end, 304);
        assert_eq!(units, vec![(0, 0), (52, 100), (104, 200), (200, 300), (252, 400)]);
        assert_eq!(reopened.end_offset(), 304);
        assert_eq!(reopened.append(&unit(500)).unwrap(), 304);
        assert_eq!(reopened.append(&unit(600)).unwrap(), 400);
    }

    #[test]
    fn repair_cuts_at_a_torn_unit_and_stale_units_behind_it_stay_gone() {
        let directory = tempdir().unwrap();
        let log = opened(directory.path());
        log.append_batch(&(0..5).map(|index| unit(index * 100)).collect::<Vec<_>>())
            .unwrap();
        drop(log);
        // Tear the unit at 52: its magic now sets a bit Java rejects.
        let path = directory.path().join("00000000000000000000");
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[52 + 15] = 0x10;
        std::fs::write(&path, &bytes).unwrap();

        let reopened = opened(directory.path());
        let (end, units) = recovered(&reopened, 0, JavaTimerLogScan::Repair);
        assert_eq!(end, 52);
        assert_eq!(units, vec![(0, 0)]);
        assert!(!directory.path().join("00000000000000000200").exists());
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes.len(), FILE_SIZE as usize);
        assert!(bytes[52..].iter().all(|byte| *byte == 0));

        // A new unit overwrites the torn one; the old unit at 104 does not come back.
        reopened.append(&unit(900)).unwrap();
        drop(reopened);
        let again = opened(directory.path());
        let (end, units) = recovered(&again, 0, JavaTimerLogScan::Repair);
        assert_eq!((end, units), (104, vec![(0, 0), (52, 900)]));
    }

    #[test]
    fn recovery_starts_at_the_file_holding_the_begin_offset() {
        let directory = tempdir().unwrap();
        let log = opened(directory.path());
        log.append_batch(&(0..5).map(|index| unit(index * 100)).collect::<Vec<_>>())
            .unwrap();

        let (end, units) = recovered(&log, 230, JavaTimerLogScan::Repair);
        assert_eq!((end, units), (304, vec![(200, 300), (252, 400)]));
    }

    #[test]
    fn revise_skips_invalid_units_up_to_the_end_and_leaves_the_log_alone() {
        let directory = tempdir().unwrap();
        let log = opened(directory.path());
        log.append_batch(&(0..3).map(|index| unit(index * 100)).collect::<Vec<_>>())
            .unwrap();
        let path = directory.path().join("00000000000000000000");
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[52 + 3] = 40;
        std::fs::write(&path, &bytes).unwrap();

        let (end, units) = recovered(&log, 0, JavaTimerLogScan::Revise);
        assert_eq!((end, units), (156, vec![(0, 0), (104, 200)]));
        assert_eq!(log.end_offset(), 156);
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn expired_files_go_once_their_last_unit_leaves_the_commit_log() {
        let directory = tempdir().unwrap();
        let log = opened(directory.path());
        log.append_batch(&(0..7).map(|index| unit(index * 100)).collect::<Vec<_>>())
            .unwrap();
        // Files at 0 and 200 end with units for 200 and 500; the file at 400 is the last one.
        assert_eq!(log.delete_expired_files(200).unwrap(), 0);
        assert_eq!(log.delete_expired_files(201).unwrap(), 1);
        assert_eq!(log.min_offset(), 200);
        assert_eq!(log.delete_expired_files(i64::MAX).unwrap(), 1);
        assert_eq!(log.min_offset(), 400);
        assert!(!directory.path().join("00000000000000000000").exists());
        assert!(!directory.path().join("00000000000000000200").exists());
        assert_eq!(log.read_unit(400).unwrap(), unit(600));
    }

    #[test]
    fn load_checks_files_like_java_and_skips_the_rust_v2_directory() {
        let directory = tempdir().unwrap();
        std::fs::create_dir_all(directory.path().join("v2").join("log")).unwrap();
        std::fs::write(directory.path().join("manifest"), [1u8; 8]).unwrap();
        std::fs::write(
            directory.path().join("00000000000000000000"),
            vec![0u8; FILE_SIZE as usize],
        )
        .unwrap();
        std::fs::write(directory.path().join("00000000000000000200"), b"").unwrap();
        let log = opened(directory.path());
        assert_eq!(log.end_offset(), 200);
        assert!(!directory.path().join("00000000000000000200").exists());

        std::fs::write(directory.path().join("00000000000000000200"), [0u8; 40]).unwrap();
        std::fs::write(
            directory.path().join("00000000000000000400"),
            vec![0u8; FILE_SIZE as usize],
        )
        .unwrap();
        let log = JavaTimerLog::new(directory.path(), FILE_SIZE).unwrap();
        assert!(matches!(
            log.load(),
            Err(JavaTimerLogError::FileLength {
                start: 200,
                length: 40,
                ..
            })
        ));
        std::fs::remove_file(directory.path().join("00000000000000000200")).unwrap();
        assert!(matches!(
            log.load(),
            Err(JavaTimerLogError::FileHole {
                expected: 200,
                actual: 400
            })
        ));
        assert!(matches!(
            JavaTimerLog::new(directory.path(), 67),
            Err(JavaTimerLogError::InvalidFileSize(67))
        ));
    }
}
