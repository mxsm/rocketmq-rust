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
use std::fs::File;

use thiserror::Error;

use super::super::identity::IdentityError;
use super::super::sidecar::SidecarError;
use super::super::sidecar::StoreMeta;
use super::proof::BootstrapInventoryEvidence;

#[cfg(any(target_os = "linux", windows))]
use sha2::Digest;
#[cfg(any(target_os = "linux", windows))]
use sha2::Sha256;
#[cfg(any(target_os = "linux", windows))]
use std::collections::BTreeSet;

#[cfg(any(target_os = "linux", windows))]
use super::super::codec::crc32;
#[cfg(any(target_os = "linux", windows))]
use super::super::identity::FileIncarnationId;
#[cfg(any(target_os = "linux", windows))]
use super::super::identity::PhysicalFileKey;
#[cfg(any(target_os = "linux", windows))]
use super::super::identity::StoreRelativePath;
#[cfg(any(target_os = "linux", windows))]
use super::super::replay::discovery::platform::EntryKind;
#[cfg(any(target_os = "linux", windows))]
use super::super::replay::discovery::platform::FileStamp;
#[cfg(any(target_os = "linux", windows))]
use super::super::replay::discovery::platform::InventoryEntry;
#[cfg(any(target_os = "linux", windows))]
use super::super::replay::discovery::platform::LifecycleDirectory;
#[cfg(any(target_os = "linux", windows))]
use super::super::replay::discovery::platform::OpenedEntry;
use super::super::replay::discovery::platform::PlatformError;
#[cfg(any(target_os = "linux", windows))]
use super::super::sidecar::decode_snapshot;
#[cfg(any(target_os = "linux", windows))]
use super::super::sidecar::encode_snapshot;
#[cfg(any(target_os = "linux", windows))]
use super::super::sidecar::IncarnationPhase;
#[cfg(any(target_os = "linux", windows))]
use super::super::sidecar::IncarnationSnapshotEntry;
#[cfg(any(target_os = "linux", windows))]
use super::super::sidecar::LifecycleSnapshot;
#[cfg(any(target_os = "linux", windows))]
use super::super::sidecar::SnapshotEntry;
#[cfg(any(target_os = "linux", windows))]
use super::super::sidecar::SnapshotMode;

const LIFECYCLE_DIRECTORY: &str = ".rocketmq-lifecycle";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct BootstrapInventoryLimits {
    max_directories: usize,
    max_entries: usize,
    max_depth: usize,
}

impl Default for BootstrapInventoryLimits {
    fn default() -> Self {
        Self {
            max_directories: 16_384,
            max_entries: 1_048_576,
            max_depth: 64,
        }
    }
}

impl BootstrapInventoryLimits {
    fn validate(self) -> Result<Self, BootstrapInventoryError> {
        if self.max_directories == 0 || self.max_entries == 0 || self.max_depth == 0 {
            return Err(BootstrapInventoryError::invalid_limits());
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BootstrapInventoryErrorKind {
    UnsupportedPlatform,
    LimitExceeded,
    UnsafeNamespace,
    InventoryChanged,
    InvalidSegment,
    InvalidIdentity,
    InvalidSnapshot,
}

#[derive(Debug, Error)]
enum BootstrapInventorySource {
    #[error("this platform has no qualified managed lifecycle writer")]
    UnsupportedPlatform,
    #[error("bootstrap inventory limits are invalid")]
    InvalidLimits,
    #[error("bootstrap inventory limit exceeded: {0}")]
    Limit(&'static str),
    #[error("unsafe bootstrap namespace: {0}")]
    UnsafeNamespace(&'static str),
    #[error("bootstrap inventory changed between complete scans")]
    InventoryChanged,
    #[error("invalid numeric segment: {0}")]
    InvalidSegment(&'static str),
    #[error("bootstrap inventory path or incarnation is invalid")]
    Identity(#[source] IdentityError),
    #[error("bootstrap inventory snapshot is invalid")]
    Sidecar(#[source] SidecarError),
    #[error("handle-relative bootstrap inventory failed")]
    Platform(#[source] PlatformError),
}

#[derive(Debug, Error)]
#[error("bootstrap inventory failed ({kind:?}): {source}")]
pub(super) struct BootstrapInventoryError {
    kind: BootstrapInventoryErrorKind,
    #[source]
    source: BootstrapInventorySource,
}

impl BootstrapInventoryError {
    pub(super) const fn kind(&self) -> BootstrapInventoryErrorKind {
        self.kind
    }

    fn unsupported() -> Self {
        Self {
            kind: BootstrapInventoryErrorKind::UnsupportedPlatform,
            source: BootstrapInventorySource::UnsupportedPlatform,
        }
    }

    fn invalid_limits() -> Self {
        Self {
            kind: BootstrapInventoryErrorKind::LimitExceeded,
            source: BootstrapInventorySource::InvalidLimits,
        }
    }

    fn limit(detail: &'static str) -> Self {
        Self {
            kind: BootstrapInventoryErrorKind::LimitExceeded,
            source: BootstrapInventorySource::Limit(detail),
        }
    }

    fn unsafe_namespace(detail: &'static str) -> Self {
        Self {
            kind: BootstrapInventoryErrorKind::UnsafeNamespace,
            source: BootstrapInventorySource::UnsafeNamespace(detail),
        }
    }

    fn changed() -> Self {
        Self {
            kind: BootstrapInventoryErrorKind::InventoryChanged,
            source: BootstrapInventorySource::InventoryChanged,
        }
    }

    fn invalid_segment(detail: &'static str) -> Self {
        Self {
            kind: BootstrapInventoryErrorKind::InvalidSegment,
            source: BootstrapInventorySource::InvalidSegment(detail),
        }
    }

    fn identity(source: IdentityError) -> Self {
        Self {
            kind: BootstrapInventoryErrorKind::InvalidIdentity,
            source: BootstrapInventorySource::Identity(source),
        }
    }

    fn sidecar(source: SidecarError) -> Self {
        Self {
            kind: BootstrapInventoryErrorKind::InvalidSnapshot,
            source: BootstrapInventorySource::Sidecar(source),
        }
    }

    fn platform(source: PlatformError) -> Self {
        let kind = match source.kind() {
            super::super::replay::discovery::platform::PlatformErrorKind::Changed => {
                BootstrapInventoryErrorKind::InventoryChanged
            }
            super::super::replay::discovery::platform::PlatformErrorKind::Limit => {
                BootstrapInventoryErrorKind::LimitExceeded
            }
            super::super::replay::discovery::platform::PlatformErrorKind::Unsupported => {
                BootstrapInventoryErrorKind::UnsupportedPlatform
            }
            super::super::replay::discovery::platform::PlatformErrorKind::Io
            | super::super::replay::discovery::platform::PlatformErrorKind::UnsafeNamespace => {
                BootstrapInventoryErrorKind::UnsafeNamespace
            }
        };
        Self {
            kind,
            source: BootstrapInventorySource::Platform(source),
        }
    }
}

pub(super) struct StableBootstrapInventory {
    evidence: BootstrapInventoryEvidence,
    retained_files: BTreeMap<super::super::identity::StoreRelativePath, File>,
}

impl std::fmt::Debug for StableBootstrapInventory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StableBootstrapInventory")
            .field("entries", &self.evidence.inventory_count)
            .field("retained_files", &self.retained_files.len())
            .finish_non_exhaustive()
    }
}

impl StableBootstrapInventory {
    pub(super) const fn snapshot(&self) -> &super::super::sidecar::LifecycleSnapshot {
        &self.evidence.snapshot
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        BootstrapInventoryEvidence,
        BTreeMap<super::super::identity::StoreRelativePath, File>,
    ) {
        (self.evidence, self.retained_files)
    }

    pub(super) fn retained_file_count_for_reconciliation(&self) -> usize {
        self.retained_files.len()
    }

    #[cfg(test)]
    pub(super) const fn snapshot_for_test(&self) -> &super::super::sidecar::LifecycleSnapshot {
        self.snapshot()
    }

    #[cfg(test)]
    pub(super) fn retained_file_count_for_test(&self) -> usize {
        self.retained_files.len()
    }
}

pub(super) fn scan_bootstrap_inventory(
    root: &File,
    meta: &StoreMeta,
    limits: BootstrapInventoryLimits,
) -> Result<StableBootstrapInventory, BootstrapInventoryError> {
    let limits = limits.validate()?;
    #[cfg(any(target_os = "linux", windows))]
    {
        scan_native(root, meta, limits)
    }
    #[cfg(not(any(target_os = "linux", windows)))]
    {
        let _ = (root, meta, limits);
        Err(BootstrapInventoryError::unsupported())
    }
}

pub(super) fn preflight_bootstrap_namespace(
    root: &File,
    limits: BootstrapInventoryLimits,
) -> Result<(), BootstrapInventoryError> {
    let limits = limits.validate()?;
    #[cfg(any(target_os = "linux", windows))]
    {
        scan_tree(root, limits, false).map(|_| ())
    }
    #[cfg(not(any(target_os = "linux", windows)))]
    {
        let _ = (root, limits);
        Err(BootstrapInventoryError::unsupported())
    }
}

#[cfg(any(target_os = "linux", windows))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct TreeSnapshot {
    directories: Vec<(String, FileStamp)>,
    entries: Vec<(String, InventoryEntry)>,
}

#[cfg(any(target_os = "linux", windows))]
struct NumericSegment {
    path: StoreRelativePath,
    offset: u64,
    expected_length: u64,
    physical_key: PhysicalFileKey,
    expected_entry: InventoryEntry,
    opened: OpenedEntry,
}

#[cfg(any(target_os = "linux", windows))]
struct TreeScan {
    snapshot: TreeSnapshot,
    numeric_segments: Vec<NumericSegment>,
    queue_lengths: BTreeMap<String, u64>,
}

#[cfg(any(target_os = "linux", windows))]
fn scan_native(
    root: &File,
    meta: &StoreMeta,
    limits: BootstrapInventoryLimits,
) -> Result<StableBootstrapInventory, BootstrapInventoryError> {
    let first = scan_tree(root, limits, true)?;
    let second = scan_tree(root, limits, false)?;
    if first.snapshot != second.snapshot {
        return Err(BootstrapInventoryError::changed());
    }
    verify_numeric_handles(&first.numeric_segments)?;
    let third = scan_tree(root, limits, false)?;
    if first.snapshot != third.snapshot {
        return Err(BootstrapInventoryError::changed());
    }
    verify_numeric_handles(&first.numeric_segments)?;
    materialize_inventory(meta, first.numeric_segments)
}

#[cfg(any(target_os = "linux", windows))]
fn scan_tree(
    root: &File,
    limits: BootstrapInventoryLimits,
    retain_numeric: bool,
) -> Result<TreeScan, BootstrapInventoryError> {
    let directory = LifecycleDirectory::open(root, "")
        .map_err(BootstrapInventoryError::platform)?
        .ok_or_else(|| BootstrapInventoryError::unsafe_namespace("retained Store root disappeared"))?;
    let inventory = directory
        .enumerate(limits.max_entries)
        .map_err(BootstrapInventoryError::platform)?;
    let mut output = TreeScan {
        snapshot: TreeSnapshot {
            directories: vec![(String::new(), inventory.directory_stamp.clone())],
            entries: Vec::new(),
        },
        numeric_segments: Vec::new(),
        queue_lengths: BTreeMap::new(),
    };
    walk_root_directory(
        &directory,
        "",
        inventory.entries,
        0,
        limits,
        retain_numeric,
        &mut output,
    )?;
    output.snapshot.directories.sort();
    output.snapshot.entries.sort();
    output
        .numeric_segments
        .sort_by(|left, right| left.path.cmp(&right.path));
    Ok(output)
}

#[cfg(any(target_os = "linux", windows))]
fn walk_root_directory(
    directory: &LifecycleDirectory,
    parent: &str,
    entries: Vec<InventoryEntry>,
    depth: usize,
    limits: BootstrapInventoryLimits,
    retain_numeric: bool,
    output: &mut TreeScan,
) -> Result<(), BootstrapInventoryError> {
    for entry in entries {
        let path = join_path(parent, &entry.name);
        record_entry(&path, &entry, limits, output)?;
        if parent.is_empty() && entry.name.eq_ignore_ascii_case(LIFECYCLE_DIRECTORY) {
            if entry.name != LIFECYCLE_DIRECTORY || entry.kind != EntryKind::Directory {
                return Err(BootstrapInventoryError::unsafe_namespace(
                    "lifecycle directory has a case-fold collision or wrong type",
                ));
            }
            continue;
        }
        let opened = directory
            .open_entry(&entry)
            .map_err(BootstrapInventoryError::platform)?;
        walk_opened_entry(path, entry, opened, depth, limits, retain_numeric, output)?;
    }
    Ok(())
}

#[cfg(any(target_os = "linux", windows))]
fn walk_opened_entry(
    path: String,
    entry: InventoryEntry,
    opened: OpenedEntry,
    depth: usize,
    limits: BootstrapInventoryLimits,
    retain_numeric: bool,
    output: &mut TreeScan,
) -> Result<(), BootstrapInventoryError> {
    match entry.kind {
        EntryKind::Directory => {
            let next_depth = depth
                .checked_add(1)
                .ok_or_else(|| BootstrapInventoryError::limit("depth"))?;
            if next_depth > limits.max_depth {
                return Err(BootstrapInventoryError::limit("directory depth"));
            }
            if output.snapshot.directories.len() >= limits.max_directories {
                return Err(BootstrapInventoryError::limit("directory count"));
            }
            let remaining = limits
                .max_entries
                .checked_sub(output.snapshot.entries.len())
                .ok_or_else(|| BootstrapInventoryError::limit("entry count"))?;
            let children = opened.enumerate(remaining).map_err(BootstrapInventoryError::platform)?;
            output
                .snapshot
                .directories
                .push((path.clone(), children.directory_stamp));
            for child in children.entries {
                let child_path = join_path(&path, &child.name);
                record_entry(&child_path, &child, limits, output)?;
                let child_opened = opened.open_entry(&child).map_err(BootstrapInventoryError::platform)?;
                walk_opened_entry(
                    child_path,
                    child,
                    child_opened,
                    next_depth,
                    limits,
                    retain_numeric,
                    output,
                )?;
            }
        }
        EntryKind::File => {
            if is_numeric_segment_name(&entry.name) {
                let queue_directory = validate_supported_segment_path(&path)?;
                match output.queue_lengths.entry(queue_directory.to_owned()) {
                    std::collections::btree_map::Entry::Vacant(length) => {
                        length.insert(entry.stamp.length);
                    }
                    std::collections::btree_map::Entry::Occupied(length) if *length.get() != entry.stamp.length => {
                        return Err(BootstrapInventoryError::invalid_segment(
                            "one mapped-file queue contains mixed segment lengths",
                        ));
                    }
                    std::collections::btree_map::Entry::Occupied(_) => {}
                }
                if entry.stamp.link_count != 1 {
                    return Err(BootstrapInventoryError::unsafe_namespace(
                        "numeric segment has an external hardlink alias",
                    ));
                }
                if entry.stamp.length == 0 {
                    return Err(BootstrapInventoryError::invalid_segment(
                        "numeric segment has zero length",
                    ));
                }
                if retain_numeric {
                    let offset = entry
                        .name
                        .parse::<u64>()
                        .map_err(|_| BootstrapInventoryError::invalid_segment("numeric offset overflow"))?;
                    let canonical_path = StoreRelativePath::new(&path).map_err(BootstrapInventoryError::identity)?;
                    canonical_path
                        .validate_segment_binding(offset)
                        .map_err(BootstrapInventoryError::identity)?;
                    let physical_key = entry
                        .stamp
                        .physical_key()
                        .ok_or_else(|| BootstrapInventoryError::invalid_segment("physical key is unavailable"))?;
                    output.numeric_segments.push(NumericSegment {
                        path: canonical_path,
                        offset,
                        expected_length: entry.stamp.length,
                        physical_key,
                        expected_entry: entry,
                        opened,
                    });
                }
            }
        }
        EntryKind::Reparse | EntryKind::Other => {
            return Err(BootstrapInventoryError::unsafe_namespace(
                "Store tree contains a symbolic link, reparse point, or special file",
            ));
        }
    }
    Ok(())
}

#[cfg(any(target_os = "linux", windows))]
fn record_entry(
    path: &str,
    entry: &InventoryEntry,
    limits: BootstrapInventoryLimits,
    output: &mut TreeScan,
) -> Result<(), BootstrapInventoryError> {
    if output.snapshot.entries.len() >= limits.max_entries {
        return Err(BootstrapInventoryError::limit("entry count"));
    }
    output.snapshot.entries.push((path.to_owned(), entry.clone()));
    Ok(())
}

#[cfg(any(target_os = "linux", windows))]
fn verify_numeric_handles(segments: &[NumericSegment]) -> Result<(), BootstrapInventoryError> {
    for segment in segments {
        segment
            .opened
            .verify(&segment.expected_entry)
            .map_err(BootstrapInventoryError::platform)?;
    }
    Ok(())
}

#[cfg(any(target_os = "linux", windows))]
fn materialize_inventory(
    meta: &StoreMeta,
    segments: Vec<NumericSegment>,
) -> Result<StableBootstrapInventory, BootstrapInventoryError> {
    let mut physical_keys = BTreeSet::new();
    let mut entries = Vec::new();
    let mut retained_files = BTreeMap::new();
    entries
        .try_reserve_exact(segments.len())
        .map_err(|_| BootstrapInventoryError::limit("snapshot allocation"))?;
    for (index, segment) in segments.into_iter().enumerate() {
        if !physical_keys.insert(segment.physical_key) {
            return Err(BootstrapInventoryError::unsafe_namespace(
                "two numeric paths resolve to one physical file",
            ));
        }
        let create_sequence = u64::try_from(index)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| BootstrapInventoryError::limit("create sequence"))?;
        let incarnation =
            FileIncarnationId::new(meta.store_uuid, create_sequence).map_err(BootstrapInventoryError::identity)?;
        let create_nonce = derive_create_nonce(meta, &segment, create_sequence);
        let create_file_path = segment
            .path
            .create_file_path(incarnation, segment.offset, &create_nonce)
            .map_err(BootstrapInventoryError::identity)?;
        let path = segment.path.clone();
        entries.push(SnapshotEntry::Incarnation(IncarnationSnapshotEntry {
            incarnation,
            phase: IncarnationPhase::Published,
            segment_offset: segment.offset,
            expected_file_length: segment.expected_length,
            create_nonce,
            physical_key: Some(segment.physical_key),
            canonical_path: segment.path,
            create_file_path,
        }));
        if retained_files.insert(path, segment.opened.into_file()).is_some() {
            return Err(BootstrapInventoryError::unsafe_namespace(
                "duplicate canonical numeric segment path",
            ));
        }
    }
    let create_high_water =
        u64::try_from(entries.len()).map_err(|_| BootstrapInventoryError::limit("create high-water"))?;
    let snapshot = LifecycleSnapshot {
        mode: SnapshotMode::BootstrapInventory,
        store_uuid: meta.store_uuid,
        generation: 0,
        log_generation: 0,
        predecessor_log_generation: u64::MAX,
        base_sequence: 1,
        create_high_water,
        ticket_high_water: 0,
        entries,
    };
    let canonical_snapshot = encode_snapshot(&snapshot).map_err(BootstrapInventoryError::sidecar)?;
    let decoded = decode_snapshot(&canonical_snapshot).map_err(BootstrapInventoryError::sidecar)?;
    if decoded != snapshot {
        return Err(BootstrapInventoryError::invalid_segment(
            "snapshot canonical round trip changed inventory",
        ));
    }
    let inventory_count =
        u64::try_from(decoded.entries.len()).map_err(|_| BootstrapInventoryError::limit("inventory count"))?;
    Ok(StableBootstrapInventory {
        evidence: BootstrapInventoryEvidence {
            store_uuid: decoded.store_uuid,
            snapshot_crc32: crc32(&canonical_snapshot),
            canonical_snapshot,
            inventory_count,
            create_high_water: decoded.create_high_water,
            ticket_high_water: decoded.ticket_high_water,
            snapshot: decoded,
        },
        retained_files,
    })
}

#[cfg(any(target_os = "linux", windows))]
fn derive_create_nonce(meta: &StoreMeta, segment: &NumericSegment, create_sequence: u64) -> [u8; 16] {
    let mut digest = Sha256::new();
    digest.update(b"rocketmq-bootstrap-incarnation-v1\0");
    digest.update(meta.store_uuid.as_bytes());
    digest.update(meta.bootstrap_id);
    digest.update(create_sequence.to_le_bytes());
    digest.update(segment.offset.to_le_bytes());
    digest.update(segment.expected_length.to_le_bytes());
    digest.update(segment.path.as_bytes());
    match segment.physical_key {
        PhysicalFileKey::Unix(key) => {
            digest.update([1]);
            digest.update(key.device().to_le_bytes());
            digest.update(key.inode().to_le_bytes());
        }
        PhysicalFileKey::Windows(key) => {
            digest.update([2]);
            digest.update(key.volume_serial().to_le_bytes());
            digest.update(key.file_id());
        }
    }
    let digest = digest.finalize();
    let mut nonce = [0_u8; 16];
    nonce.copy_from_slice(&digest[..16]);
    if nonce == [0; 16] {
        nonce[15] = 1;
    }
    nonce
}

#[cfg(any(target_os = "linux", windows))]
fn is_numeric_segment_name(name: &str) -> bool {
    name.len() == 20 && name.bytes().all(|byte| byte.is_ascii_digit())
}

#[cfg(any(target_os = "linux", windows))]
fn validate_supported_segment_path(path: &str) -> Result<&str, BootstrapInventoryError> {
    StoreRelativePath::new(path).map_err(BootstrapInventoryError::identity)?;
    let components = path.split('/').collect::<Vec<_>>();
    let queue_id = match components.as_slice() {
        ["commitlog", _file_name] => return Ok("commitlog"),
        ["consumequeue" | "consumequeue_ext" | "batchconsumequeue", topic, queue_id, _file_name]
            if !topic.is_empty() =>
        {
            *queue_id
        }
        _ => {
            return Err(BootstrapInventoryError::invalid_segment(
                "numeric file is outside a supported mapped-file queue",
            ));
        }
    };
    let parsed = queue_id
        .parse::<i32>()
        .map_err(|_| BootstrapInventoryError::invalid_segment("queue id is not a non-negative integer"))?;
    if parsed < 0 || parsed.to_string() != queue_id {
        return Err(BootstrapInventoryError::invalid_segment("queue id is not canonical"));
    }
    path.rsplit_once('/')
        .map(|(directory, _)| directory)
        .ok_or_else(|| BootstrapInventoryError::invalid_segment("mapped-file segment has no queue directory"))
}

#[cfg(any(target_os = "linux", windows))]
fn join_path(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_owned()
    } else {
        format!("{parent}/{name}")
    }
}
