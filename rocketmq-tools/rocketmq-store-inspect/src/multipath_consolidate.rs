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

//! Offline consolidation of a multipath CommitLog into a new single root.

use std::collections::BTreeMap;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;

use bytes::Bytes;
use rocketmq_store::decode_commit_log_record;
use rocketmq_store::CommitLogRecordBodyMode;
use rocketmq_store::CommitLogRecordChecksum;
use rocketmq_store::CommitLogRecordOutcome;
use serde::Serialize;

/// Immutable request for one offline consolidation attempt.
#[derive(Debug, Clone)]
pub struct ConsolidationRequest {
    /// Existing CommitLog roots. The source tree is never modified.
    pub source_roots: Vec<PathBuf>,
    /// New single-root destination. It must not already exist.
    pub target: PathBuf,
    /// Configured CommitLog segment size.
    pub mapped_file_size: u64,
    /// Store root whose `lock` file fences a running Broker.
    pub store_root: PathBuf,
}

impl ConsolidationRequest {
    /// Creates a request and derives the Store root from the target parent.
    pub fn new(source_roots: Vec<PathBuf>, target: PathBuf, mapped_file_size: u64) -> Self {
        let store_root = target.parent().unwrap_or_else(|| Path::new(".")).to_path_buf();
        Self {
            source_roots,
            target,
            mapped_file_size,
            store_root,
        }
    }

    /// Overrides the Store root used for the exclusive offline lock.
    pub fn with_store_root(mut self, store_root: PathBuf) -> Self {
        self.store_root = store_root;
        self
    }
}

/// Successful consolidation summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsolidationReport {
    /// Number of copied CommitLog segments.
    pub segment_count: usize,
    /// Total bytes copied and compared.
    pub copied_bytes: u64,
    /// Number of structurally valid message frames copied.
    pub record_count: usize,
}

#[derive(Debug)]
struct Segment {
    offset: u64,
    path: PathBuf,
    size: u64,
    record_count: usize,
}

/// Consolidates all source segments and atomically publishes the target directory.
pub fn consolidate_multipath(request: &ConsolidationRequest) -> io::Result<ConsolidationReport> {
    consolidate_multipath_with_environment(request, |path| fs2::available_space(path), |_| Ok(()))
}

/// Consolidates with a deterministic post-copy hook used by interruption tests.
#[doc(hidden)]
pub fn consolidate_multipath_with_hook(
    request: &ConsolidationRequest,
    after_copy: impl FnMut(usize) -> io::Result<()>,
) -> io::Result<ConsolidationReport> {
    consolidate_multipath_with_environment(request, |path| fs2::available_space(path), after_copy)
}

/// Consolidates with deterministic environment hooks used by capacity and interruption tests.
#[doc(hidden)]
pub fn consolidate_multipath_with_environment(
    request: &ConsolidationRequest,
    available_space: impl FnOnce(&Path) -> io::Result<u64>,
    mut after_copy: impl FnMut(usize) -> io::Result<()>,
) -> io::Result<ConsolidationReport> {
    validate_request(request)?;
    let _lock = OfflineLock::acquire(&request.store_root)?;
    let segments = scan_segments(&request.source_roots, request.mapped_file_size)?;
    let record_count = segments.iter().try_fold(0_usize, |total, segment| {
        total
            .checked_add(segment.record_count)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "CommitLog record count overflow"))
    })?;
    let copied_bytes = segments.iter().try_fold(0_u64, |total, segment| {
        total
            .checked_add(segment.size)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "CommitLog byte count overflow"))
    })?;
    let target_parent = request.target.parent().unwrap_or_else(|| Path::new("."));
    if available_space(target_parent)? < copied_bytes {
        return Err(io::Error::new(
            io::ErrorKind::StorageFull,
            "target filesystem has insufficient free space",
        ));
    }
    let target_name = request
        .target
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "target has no UTF-8 file name"))?;
    let staging = target_parent.join(format!(".{target_name}.staging-{}", std::process::id()));
    if staging.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("staging directory already exists: {}", staging.display()),
        ));
    }
    fs::create_dir(&staging)?;
    let mut cleanup = StagingCleanup::new(staging.clone());
    for (index, segment) in segments.iter().enumerate() {
        let destination = staging.join(format!("{:020}", segment.offset));
        fs::copy(&segment.path, &destination)?;
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(&destination)?
            .sync_all()?;
        compare_files(&segment.path, &destination)?;
        if validate_segment_frames(&destination, segment.offset)? != segment.record_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "copied segment record count differs from source {}",
                    segment.path.display()
                ),
            ));
        }
        after_copy(index + 1)?;
    }
    fs::rename(&staging, &request.target)?;
    cleanup.disarm();
    Ok(ConsolidationReport {
        segment_count: segments.len(),
        copied_bytes,
        record_count,
    })
}

struct IgnoredChecksum;

impl CommitLogRecordChecksum for IgnoredChecksum {
    fn checksum(&self, _bytes: &[u8]) -> u32 {
        0
    }
}

fn validate_segment_frames(path: &Path, segment_offset: u64) -> io::Result<usize> {
    let bytes = fs::read(path)?;
    let mut position = 0_usize;
    let mut records = 0_usize;
    while position < bytes.len() {
        if bytes[position..].iter().all(|byte| *byte == 0) {
            return Ok(records);
        }
        let size_prefix = bytes.get(position..position + 4).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "truncated CommitLog size prefix in {} at byte {position}",
                    path.display()
                ),
            )
        })?;
        let declared_size = i32::from_be_bytes(size_prefix.try_into().unwrap_or_default());
        let declared_size = usize::try_from(declared_size).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("negative CommitLog frame size in {} at byte {position}", path.display()),
            )
        })?;
        let end = position
            .checked_add(declared_size)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "CommitLog frame offset overflow"))?;
        let frame = bytes.get(position..end).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("truncated CommitLog frame in {} at byte {position}", path.display()),
            )
        })?;
        match decode_commit_log_record(
            &Bytes::copy_from_slice(frame),
            CommitLogRecordBodyMode::Skip,
            &IgnoredChecksum,
        )
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "invalid CommitLog frame in {} at byte {position}: {error:?}",
                    path.display()
                ),
            )
        })? {
            CommitLogRecordOutcome::Message(record) => {
                let expected_offset = segment_offset
                    .checked_add(position as u64)
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "CommitLog physical offset overflow"))?;
                if u64::try_from(record.physical_offset).ok() != Some(expected_offset) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "CommitLog physical offset mismatch in {} at byte {position}: expected {expected_offset}, found {}",
                            path.display(),
                            record.physical_offset
                        ),
                    ));
                }
                records += 1;
            }
            CommitLogRecordOutcome::Blank { .. } => {
                if bytes[end..].iter().any(|byte| *byte != 0) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("non-zero bytes follow CommitLog blank marker in {}", path.display()),
                    ));
                }
                return Ok(records);
            }
        }
        position = end;
    }
    Ok(records)
}

fn validate_request(request: &ConsolidationRequest) -> io::Result<()> {
    if request.source_roots.is_empty() || request.mapped_file_size == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "at least one source root and a non-zero segment size are required",
        ));
    }
    if request.target.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("target already exists: {}", request.target.display()),
        ));
    }
    let parent = request.target.parent().unwrap_or_else(|| Path::new("."));
    if !parent.is_dir() || !request.store_root.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "target parent and Store root must already exist",
        ));
    }
    let target_parent = fs::canonicalize(parent)?;
    for source in &request.source_roots {
        let source = fs::canonicalize(source)?;
        if target_parent.starts_with(&source) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "target must not be created inside a source CommitLog root",
            ));
        }
    }
    Ok(())
}

fn scan_segments(roots: &[PathBuf], mapped_file_size: u64) -> io::Result<Vec<Segment>> {
    let mut segments = BTreeMap::<u64, Segment>::new();
    for root in roots {
        let canonical_root = fs::canonicalize(root)?;
        for entry in fs::read_dir(&canonical_root)? {
            let path = entry?.path();
            let metadata = fs::symlink_metadata(&path)?;
            if !metadata.file_type().is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("non-file CommitLog entry: {}", path.display()),
                ));
            }
            let offset = parse_offset(&path)?;
            if metadata.len() > mapped_file_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("segment {} exceeds configured size", path.display()),
                ));
            }
            if let Some(existing) = segments.get(&offset) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "duplicate CommitLog owner for offset {offset}: {} and {}",
                        existing.path.display(),
                        path.display()
                    ),
                ));
            }
            let segment = Segment {
                offset,
                path: path.clone(),
                size: metadata.len(),
                record_count: validate_segment_frames(&path, offset)?,
            };
            segments.insert(offset, segment);
        }
    }
    let segments = segments.into_values().collect::<Vec<_>>();
    for (index, pair) in segments.windows(2).enumerate() {
        if pair[0].size != mapped_file_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("non-tail segment {} is not full", pair[0].path.display()),
            ));
        }
        let expected = pair[0]
            .offset
            .checked_add(mapped_file_size)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "CommitLog segment offset overflow"))?;
        if pair[1].offset != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "CommitLog is not contiguous after segment {index}: expected offset {expected}, found {}",
                    pair[1].offset
                ),
            ));
        }
    }
    Ok(segments)
}

fn parse_offset(path: &Path) -> io::Result<u64> {
    let name = path.file_name().and_then(|name| name.to_str()).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("CommitLog entry has no UTF-8 name: {}", path.display()),
        )
    })?;
    if name.len() != 20 || !name.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown CommitLog entry: {}", path.display()),
        ));
    }
    name.parse().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid CommitLog offset {}: {error}", path.display()),
        )
    })
}

fn compare_files(source: &Path, destination: &Path) -> io::Result<()> {
    let mut left = File::open(source)?;
    let mut right = File::open(destination)?;
    let mut left_buffer = [0_u8; 64 * 1024];
    let mut right_buffer = [0_u8; 64 * 1024];
    loop {
        let left_len = left.read(&mut left_buffer)?;
        let right_len = right.read(&mut right_buffer)?;
        if left_len != right_len || left_buffer[..left_len] != right_buffer[..right_len] {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("copied segment differs from source {}", source.display()),
            ));
        }
        if left_len == 0 {
            return Ok(());
        }
    }
}

struct OfflineLock {
    file: File,
}

impl OfflineLock {
    fn acquire(store_root: &Path) -> io::Result<Self> {
        let path = store_root.join("lock");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        fs2::FileExt::try_lock_exclusive(&file).map_err(|error| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                format!(
                    "Broker must be stopped; Store lock {} is unavailable: {error}",
                    path.display()
                ),
            )
        })?;
        Ok(Self { file })
    }
}

impl Drop for OfflineLock {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.file);
    }
}

struct StagingCleanup {
    path: Option<PathBuf>,
}

impl StagingCleanup {
    fn new(path: PathBuf) -> Self {
        Self { path: Some(path) }
    }

    fn disarm(&mut self) {
        self.path = None;
    }
}

impl Drop for StagingCleanup {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = fs::remove_dir_all(path);
        }
    }
}
