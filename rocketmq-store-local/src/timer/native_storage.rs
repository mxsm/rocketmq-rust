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

//! The files of the `native` timer engine under `timer-native/`: the FORMAT record, bucket
//! files, the cancel log and the A/B checkpoints (see [`crate::timer::native_format`]).
//!
//! Appends are buffered writes with no fsync. [`NativeTimerStorage::sync`] makes everything
//! written so far durable and reports how far; a checkpoint records those lengths. Opening the
//! directory cuts every file back to its last checkpoint and deletes the bucket files written
//! after it, so the engine can replay the Timer CQ from the checkpoint's source offset without
//! indexing a timer twice. Payload copies live in the payload store, which keeps its own files.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::ErrorKind;
use std::path::Path;
use std::path::PathBuf;

use parking_lot::Mutex;
use thiserror::Error;
use tracing::warn;

use crate::timer::native_format::decode_native_frame;
use crate::timer::native_format::encode_native_frames;
use crate::timer::native_format::NativeBucketId;
use crate::timer::native_format::NativeBucketKind;
use crate::timer::native_format::NativeBucketLength;
use crate::timer::native_format::NativeCancelRecord;
use crate::timer::native_format::NativeCheckpoint;
use crate::timer::native_format::NativeCheckpointSlot;
use crate::timer::native_format::NativeFileHeader;
use crate::timer::native_format::NativeFormat;
use crate::timer::native_format::NativeFormatError;
use crate::timer::native_format::NativeFrameOrder;
use crate::timer::native_format::NativeIndexEntry;
use crate::timer::native_format::NATIVE_CANCEL_LOG_NAME;
use crate::timer::native_format::NATIVE_FILE_HEADER_SIZE;
use crate::timer::native_format::NATIVE_FORMAT_FILE_NAME;
use crate::timer::native_format::NATIVE_TIMER_DIR;
use crate::timer::positional_io::read_exact_at;
use crate::timer::positional_io::write_all_at;

/// Bucket files kept open at once.
const HANDLE_LIMIT: usize = 64;
/// Bytes a bucket read takes from the file at a time.
const READ_CHUNK_BYTES: u64 = 1 << 20;
const HEADER_LEN: u64 = NATIVE_FILE_HEADER_SIZE as u64;

/// A `native` timer directory that cannot be opened, read or written.
#[derive(Debug, Error)]
pub enum NativeStorageError {
    #[error("native timer storage I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Format(#[from] NativeFormatError),
    #[error("native timer directory has {found} lanes, configured {configured}")]
    LaneCountMismatch { found: u16, configured: u16 },
    #[error("native timer directory {0} holds timer files but no FORMAT")]
    MissingFormat(PathBuf),
    #[error("native timer file {path} has {actual} bytes, fewer than the {durable} its checkpoint made durable")]
    ShorterThanCheckpoint { path: PathBuf, durable: u64, actual: u64 },
    #[error("native bucket file {0} holds another bucket")]
    MisplacedBucket(PathBuf),
    #[error("native bucket {0:?} has no file")]
    UnknownBucket(NativeBucketId),
    #[error("native checkpoint {sequence} does not follow checkpoint {last}")]
    StaleCheckpoint { sequence: u64, last: u64 },
    #[error("native checkpoint {sequence} records more than the files hold durably")]
    CheckpointAheadOfFiles { sequence: u64 },
}

/// The lengths [`NativeTimerStorage::sync`] made durable, for the next checkpoint.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NativeSyncedLengths {
    pub buckets: Vec<NativeBucketLength>,
    pub cancel_log_length: u64,
}

/// An append-only file and how much of it is written and durable.
struct LogFile {
    /// Bytes written; a bucket file's header included.
    length: u64,
    /// Bytes the last sync made durable.
    durable: u64,
}

struct StorageState {
    buckets: BTreeMap<NativeBucketId, LogFile>,
    handles: HashMap<NativeBucketId, File>,
    handle_order: VecDeque<NativeBucketId>,
    cancel_file: File,
    cancel: LogFile,
    /// Directories whose entries changed since the last sync.
    changed_dirs: BTreeSet<PathBuf>,
}

/// The slot and sequence of the last checkpoint written or loaded.
#[derive(Clone, Copy)]
struct LastCheckpoint {
    slot: NativeCheckpointSlot,
    sequence: u64,
}

/// The `native` engine's files under one store root.
pub struct NativeTimerStorage {
    root: PathBuf,
    format: NativeFormat,
    state: Mutex<StorageState>,
    /// Serializes checkpoint writes; kept apart from `state` so appends go on while one syncs.
    checkpoint: Mutex<Option<LastCheckpoint>>,
}

impl NativeTimerStorage {
    /// Opens `timer-native/` under `store_root`, creating it with `lane_count` lanes when it is
    /// new, and cuts every file back to the last checkpoint, which it returns. With no
    /// checkpoint, nothing is durable yet: every bucket file is deleted and the cancel log
    /// emptied, and the engine replays the Timer CQ from its start.
    ///
    /// # Errors
    ///
    /// Returns [`NativeStorageError`] when the directory was written with another lane count,
    /// has timer files but no FORMAT, holds a corrupt record, or lost bytes its checkpoint made
    /// durable, and on I/O failure.
    pub fn open(
        store_root: impl AsRef<Path>,
        lane_count: u16,
        now_ms: i64,
    ) -> Result<(Self, Option<NativeCheckpoint>), NativeStorageError> {
        let root = store_root.as_ref().join(NATIVE_TIMER_DIR);
        fs::create_dir_all(&root)?;
        let format = open_format(&root, lane_count, now_ms)?;
        let checkpoint = NativeCheckpoint::select(
            read_if_present(&root.join(NativeCheckpointSlot::A.file_name()))?.as_deref(),
            read_if_present(&root.join(NativeCheckpointSlot::B.file_name()))?.as_deref(),
        )?;
        let recorded = checkpoint.as_ref().map(|(_, checkpoint)| checkpoint);

        let mut durable: BTreeMap<NativeBucketId, u64> = recorded
            .map(|checkpoint| {
                checkpoint
                    .buckets
                    .iter()
                    .map(|bucket| (bucket.bucket, bucket.durable_length))
                    .collect()
            })
            .unwrap_or_default();
        let mut buckets = BTreeMap::new();
        for (bucket, path) in scan_buckets(&root)? {
            let length = match durable.remove(&bucket) {
                Some(length) if length >= HEADER_LEN => length,
                // Written after the checkpoint, or with not even its header durable; the replay
                // writes it again.
                _ => {
                    fs::remove_file(&path)?;
                    continue;
                }
            };
            let file = OpenOptions::new().read(true).write(true).open(&path)?;
            cut_back(&file, &path, length)?;
            let mut header = [0u8; NATIVE_FILE_HEADER_SIZE];
            read_exact_at(&file, &mut header, 0)?;
            if NativeFileHeader::decode(&header)?.bucket != bucket {
                return Err(NativeStorageError::MisplacedBucket(path));
            }
            buckets.insert(
                bucket,
                LogFile {
                    length,
                    durable: length,
                },
            );
        }
        if let Some((bucket, length)) = durable.into_iter().find(|(_, length)| *length >= HEADER_LEN) {
            return Err(NativeStorageError::ShorterThanCheckpoint {
                path: root.join(bucket.relative_path()),
                durable: length,
                actual: 0,
            });
        }

        let cancel_path = root.join(NATIVE_CANCEL_LOG_NAME);
        let cancel_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&cancel_path)?;
        let cancel_length = recorded.map_or(0, |checkpoint| checkpoint.cancel_log_length);
        cut_back(&cancel_file, &cancel_path, cancel_length)?;

        let last = checkpoint.as_ref().map(|(slot, checkpoint)| LastCheckpoint {
            slot: *slot,
            sequence: checkpoint.sequence,
        });
        let storage = Self {
            format,
            state: Mutex::new(StorageState {
                buckets,
                handles: HashMap::new(),
                handle_order: VecDeque::new(),
                cancel_file,
                cancel: LogFile {
                    length: cancel_length,
                    durable: cancel_length,
                },
                changed_dirs: BTreeSet::from([root.clone()]),
            }),
            root,
            checkpoint: Mutex::new(last),
        };
        Ok((storage, checkpoint.map(|(_, checkpoint)| checkpoint)))
    }

    /// The FORMAT record the directory was written with.
    pub fn format(&self) -> NativeFormat {
        self.format
    }

    /// The `timer-native/` directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Every bucket file with its written length.
    pub fn buckets(&self) -> Vec<NativeBucketLength> {
        self.state
            .lock()
            .buckets
            .iter()
            .map(|(bucket, file)| NativeBucketLength {
                bucket: *bucket,
                durable_length: file.length,
            })
            .collect()
    }

    /// Appends `entries` to `bucket`, creating its file when it has none, and returns the new
    /// length of the file. Minute runs are written as sorted frames, the other kinds in arrival
    /// order; keeping a minute run sorted across appends is the writer's part.
    ///
    /// # Errors
    ///
    /// Returns [`NativeStorageError`] for an invalid entry, an unsorted minute run, or an I/O
    /// failure; the file's length is then unchanged.
    pub fn append_entries(
        &self,
        bucket: NativeBucketId,
        entries: &[NativeIndexEntry],
    ) -> Result<u64, NativeStorageError> {
        let order = match bucket.kind() {
            NativeBucketKind::Minute => NativeFrameOrder::Sorted,
            NativeBucketKind::Hour | NativeBucketKind::Day => NativeFrameOrder::Arrival,
        };
        let mut bytes = Vec::new();
        encode_native_frames(entries, order, &mut bytes)?;

        let mut state = self.state.lock();
        if !state.buckets.contains_key(&bucket) {
            self.create_bucket(&mut state, bucket)?;
        }
        let length = state.buckets.get(&bucket).map_or(HEADER_LEN, |file| file.length);
        if bytes.is_empty() {
            return Ok(length);
        }
        write_all_at(self.handle(&mut state, bucket)?, &bytes, length)?;
        let length = length + bytes.len() as u64;
        if let Some(file) = state.buckets.get_mut(&bucket) {
            file.length = length;
        }
        Ok(length)
    }

    /// Passes the entries of `bucket` to `visit` in file order, a run of frames at a time, so a
    /// large bucket is never read whole.
    ///
    /// # Errors
    ///
    /// Returns [`NativeStorageError::UnknownBucket`] when the bucket has no file, the error of
    /// `visit`, or the error of the first damaged frame.
    pub fn visit_entries(
        &self,
        bucket: NativeBucketId,
        mut visit: impl FnMut(&[NativeIndexEntry]) -> Result<(), NativeStorageError>,
    ) -> Result<(), NativeStorageError> {
        let (file, length) = {
            let mut state = self.state.lock();
            let length = state
                .buckets
                .get(&bucket)
                .map(|file| file.length)
                .ok_or(NativeStorageError::UnknownBucket(bucket))?;
            (self.handle(&mut state, bucket)?.try_clone()?, length)
        };
        let mut header = [0u8; NATIVE_FILE_HEADER_SIZE];
        read_exact_at(&file, &mut header, 0)?;
        if NativeFileHeader::decode(&header)?.bucket != bucket {
            return Err(NativeStorageError::MisplacedBucket(
                self.root.join(bucket.relative_path()),
            ));
        }

        let mut position = HEADER_LEN;
        let mut pending = Vec::new();
        let mut entries = Vec::new();
        while position < length {
            let chunk = READ_CHUNK_BYTES.min(length - position) as usize;
            let start = pending.len();
            pending.resize(start + chunk, 0);
            read_exact_at(&file, &mut pending[start..], position)?;
            position += chunk as u64;

            let mut consumed = 0;
            while consumed < pending.len() {
                match decode_native_frame(&pending[consumed..]) {
                    Ok((frame, frame_len)) => {
                        entries.extend(frame.entries);
                        consumed += frame_len;
                    }
                    // The rest of this frame is in the next chunk.
                    Err(NativeFormatError::Truncated { .. }) if position < length => break,
                    Err(error) => return Err(error.into()),
                }
            }
            pending.drain(..consumed);
            if !entries.is_empty() {
                visit(&entries)?;
                entries.clear();
            }
        }
        Ok(())
    }

    /// Deletes the file of `bucket` and returns whether it had one. The next checkpoint must no
    /// longer name the bucket.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the file cannot be deleted.
    pub fn delete_bucket(&self, bucket: NativeBucketId) -> Result<bool, NativeStorageError> {
        let mut state = self.state.lock();
        if !state.buckets.contains_key(&bucket) {
            return Ok(false);
        }
        // An open handle keeps a file from being deleted on Windows.
        state.handles.remove(&bucket);
        state.handle_order.retain(|cached| *cached != bucket);
        let path = self.root.join(bucket.relative_path());
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        state.buckets.remove(&bucket);
        state.changed_dirs.insert(self.root.join(bucket.kind().dir_name()));
        Ok(true)
    }

    /// Appends recalls to the cancel log and returns its new length.
    ///
    /// # Errors
    ///
    /// Returns [`NativeStorageError`] for an invalid record or an I/O failure; the log's length
    /// is then unchanged.
    pub fn append_cancels(&self, records: &[NativeCancelRecord]) -> Result<u64, NativeStorageError> {
        let mut bytes = Vec::new();
        for record in records {
            record.encode_into(&mut bytes)?;
        }
        let mut state = self.state.lock();
        let length = state.cancel.length;
        write_all_at(&state.cancel_file, &bytes, length)?;
        state.cancel.length = length + bytes.len() as u64;
        Ok(state.cancel.length)
    }

    /// Reads every recall in the cancel log.
    ///
    /// # Errors
    ///
    /// Returns an I/O error, or the error of the first damaged record.
    pub fn read_cancels(&self) -> Result<Vec<NativeCancelRecord>, NativeStorageError> {
        let (file, length) = {
            let state = self.state.lock();
            (state.cancel_file.try_clone()?, state.cancel.length)
        };
        let mut bytes = vec![0u8; length as usize];
        read_exact_at(&file, &mut bytes, 0)?;
        let mut records = Vec::new();
        let mut rest = bytes.as_slice();
        while !rest.is_empty() {
            let (record, record_len) = NativeCancelRecord::decode(rest)?;
            records.push(record);
            rest = &rest[record_len..];
        }
        Ok(records)
    }

    /// Makes everything written so far durable, new directory entries included, and returns
    /// the durable lengths. Appends can go on while the files sync; what they add is left for
    /// the next sync.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when a file or directory cannot be synced; durable lengths then
    /// stay where they were.
    pub fn sync(&self) -> Result<NativeSyncedLengths, NativeStorageError> {
        let (buckets, cancel, dirs) = {
            let mut state = self.state.lock();
            let pending: Vec<_> = state
                .buckets
                .iter()
                .filter(|(_, file)| file.length > file.durable)
                .map(|(bucket, file)| (*bucket, file.length))
                .collect();
            let mut buckets = Vec::with_capacity(pending.len());
            for (bucket, length) in pending {
                buckets.push((bucket, length, self.handle(&mut state, bucket)?.try_clone()?));
            }
            let cancel = (state.cancel.length > state.cancel.durable)
                .then(|| state.cancel_file.try_clone().map(|file| (file, state.cancel.length)))
                .transpose()?;
            (buckets, cancel, std::mem::take(&mut state.changed_dirs))
        };
        let synced = buckets
            .iter()
            .try_for_each(|(_, _, file)| file.sync_data())
            .and_then(|()| cancel.as_ref().map_or(Ok(()), |(file, _)| file.sync_data()))
            .and_then(|()| dirs.iter().try_for_each(|dir| sync_directory(dir)));

        let mut state = self.state.lock();
        if let Err(error) = synced {
            state.changed_dirs.extend(dirs);
            return Err(error.into());
        }
        for (bucket, length, _) in buckets {
            if let Some(file) = state.buckets.get_mut(&bucket) {
                file.durable = file.durable.max(length);
            }
        }
        if let Some((_, length)) = cancel {
            state.cancel.durable = state.cancel.durable.max(length);
        }
        Ok(NativeSyncedLengths {
            // A bucket created while the files synced has nothing durable to record yet.
            buckets: state
                .buckets
                .iter()
                .filter(|(_, file)| file.durable >= HEADER_LEN)
                .map(|(bucket, file)| NativeBucketLength {
                    bucket: *bucket,
                    durable_length: file.durable,
                })
                .collect(),
            cancel_log_length: state.cancel.durable,
        })
    }

    /// The sequence of the last checkpoint written or loaded, `0` before the first.
    pub fn last_checkpoint_sequence(&self) -> u64 {
        self.checkpoint.lock().map_or(0, |last| last.sequence)
    }

    /// Writes `checkpoint` to the slot the last one is not in and syncs it, so a torn write
    /// leaves the last one to recover from.
    ///
    /// # Errors
    ///
    /// Returns [`NativeStorageError::StaleCheckpoint`] unless the sequence is higher than the
    /// last one's, [`NativeStorageError::CheckpointAheadOfFiles`] when it records a bucket file
    /// or cancel log length that is not durable, and an I/O error when the slot cannot be
    /// written or synced.
    pub fn write_checkpoint(&self, checkpoint: &NativeCheckpoint) -> Result<(), NativeStorageError> {
        let bytes = checkpoint.encode()?;
        let mut last = self.checkpoint.lock();
        let last_sequence = last.map_or(0, |last| last.sequence);
        if checkpoint.sequence <= last_sequence {
            return Err(NativeStorageError::StaleCheckpoint {
                sequence: checkpoint.sequence,
                last: last_sequence,
            });
        }
        {
            let state = self.state.lock();
            let covered = checkpoint.cancel_log_length <= state.cancel.durable
                && checkpoint.buckets.iter().all(|recorded| {
                    state
                        .buckets
                        .get(&recorded.bucket)
                        .is_some_and(|file| recorded.durable_length <= file.durable)
                });
            if !covered {
                return Err(NativeStorageError::CheckpointAheadOfFiles {
                    sequence: checkpoint.sequence,
                });
            }
        }
        let slot = last.map_or(NativeCheckpointSlot::A, |last| last.slot.other());
        let path = self.root.join(slot.file_name());
        let created = !path.exists();
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&path)?;
        write_all_at(&file, &bytes, 0)?;
        file.set_len(bytes.len() as u64)?;
        file.sync_all()?;
        if created {
            sync_directory(&self.root)?;
        }
        *last = Some(LastCheckpoint {
            slot,
            sequence: checkpoint.sequence,
        });
        Ok(())
    }

    fn create_bucket(&self, state: &mut StorageState, bucket: NativeBucketId) -> Result<(), NativeStorageError> {
        let dir = self.root.join(bucket.kind().dir_name());
        if !dir.exists() {
            fs::create_dir_all(&dir)?;
            state.changed_dirs.insert(self.root.clone());
        }
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(self.root.join(bucket.relative_path()))?;
        write_all_at(&file, &NativeFileHeader { bucket }.encode(), 0)?;
        state.changed_dirs.insert(dir);
        state.buckets.insert(
            bucket,
            LogFile {
                length: HEADER_LEN,
                durable: 0,
            },
        );
        self.cache(state, bucket, file);
        Ok(())
    }

    fn handle<'a>(&self, state: &'a mut StorageState, bucket: NativeBucketId) -> Result<&'a File, NativeStorageError> {
        if !state.handles.contains_key(&bucket) {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(self.root.join(bucket.relative_path()))?;
            self.cache(state, bucket, file);
        } else if state.handle_order.back() != Some(&bucket) {
            state.handle_order.retain(|cached| *cached != bucket);
            state.handle_order.push_back(bucket);
        }
        state
            .handles
            .get(&bucket)
            .ok_or(NativeStorageError::UnknownBucket(bucket))
    }

    fn cache(&self, state: &mut StorageState, bucket: NativeBucketId, file: File) {
        if state.handles.insert(bucket, file).is_none() {
            state.handle_order.push_back(bucket);
        }
        while state.handles.len() > HANDLE_LIMIT {
            let Some(evicted) = state.handle_order.pop_front() else {
                break;
            };
            state.handles.remove(&evicted);
        }
    }
}

/// Reads the FORMAT record, or writes it for a new directory.
fn open_format(root: &Path, lane_count: u16, now_ms: i64) -> Result<NativeFormat, NativeStorageError> {
    let path = root.join(NATIVE_FORMAT_FILE_NAME);
    if let Some(bytes) = read_if_present(&path)? {
        let format = NativeFormat::decode(&bytes)?;
        if format.lane_count != lane_count {
            return Err(NativeStorageError::LaneCountMismatch {
                found: format.lane_count,
                configured: lane_count,
            });
        }
        return Ok(format);
    }
    if holds_timer_files(root)? {
        return Err(NativeStorageError::MissingFormat(root.to_path_buf()));
    }
    let format = NativeFormat {
        lane_count,
        created_ms: now_ms,
    };
    // Written aside and renamed, so a crash never leaves a torn FORMAT.
    let staged = root.join(format!("{NATIVE_FORMAT_FILE_NAME}.tmp"));
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&staged)?;
    write_all_at(&file, &format.encode()?, 0)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&staged, &path)?;
    sync_directory(root)?;
    Ok(format)
}

fn holds_timer_files(root: &Path) -> Result<bool, NativeStorageError> {
    for name in [
        NativeCheckpointSlot::A.file_name(),
        NativeCheckpointSlot::B.file_name(),
        NATIVE_CANCEL_LOG_NAME,
    ] {
        if root.join(name).exists() {
            return Ok(true);
        }
    }
    Ok(!scan_buckets(root)?.is_empty())
}

/// The bucket files on disk. Names that are not buckets are left alone.
fn scan_buckets(root: &Path) -> Result<Vec<(NativeBucketId, PathBuf)>, NativeStorageError> {
    let mut buckets = Vec::new();
    for kind in [NativeBucketKind::Hour, NativeBucketKind::Day, NativeBucketKind::Minute] {
        let dir = root.join(kind.dir_name());
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            let bucket = entry
                .file_name()
                .to_str()
                .and_then(|name| NativeBucketId::parse(kind, name));
            match bucket {
                Some(bucket) if entry.file_type()?.is_file() => buckets.push((bucket, path)),
                _ => warn!(path = %path.display(), "skip a file that is not a native timer bucket"),
            }
        }
    }
    Ok(buckets)
}

/// Cuts `file` back to `durable` bytes.
fn cut_back(file: &File, path: &Path, durable: u64) -> Result<(), NativeStorageError> {
    let actual = file.metadata()?.len();
    if actual < durable {
        return Err(NativeStorageError::ShorterThanCheckpoint {
            path: path.to_path_buf(),
            durable,
            actual,
        });
    }
    if actual > durable {
        file.set_len(durable)?;
    }
    Ok(())
}

fn read_if_present(path: &Path) -> Result<Option<Vec<u8>>, NativeStorageError> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Makes the entries of `dir` durable. Windows has no directory sync; NTFS journals metadata.
#[cfg(unix)]
fn sync_directory(dir: &Path) -> std::io::Result<()> {
    File::open(dir).and_then(|dir| dir.sync_all())
}

#[cfg(not(unix))]
fn sync_directory(_dir: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests;
