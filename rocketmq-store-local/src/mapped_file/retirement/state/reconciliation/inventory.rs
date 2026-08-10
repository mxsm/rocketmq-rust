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
use std::collections::BTreeSet;
use std::fs::File;
use std::io;

use thiserror::Error;

use super::collect_known_paths;
use super::parent_directory;
use super::NamespaceObject;
use super::RecoveredLedgerState;
use super::StableNamespaceInventory;
use crate::mapped_file::retirement::codec::ContentFingerprint;
use crate::mapped_file::retirement::identity::IdentityError;
use crate::mapped_file::retirement::identity::StoreRelativePath;
use crate::mapped_file::retirement::identity::StoreUuid;
use crate::mapped_file::retirement::replay::discovery::platform::EntryKind;
use crate::mapped_file::retirement::replay::discovery::platform::InventoryEntry;
use crate::mapped_file::retirement::replay::discovery::platform::LifecycleDirectory;
use crate::mapped_file::retirement::replay::discovery::platform::OpenedEntry;
use crate::mapped_file::retirement::replay::discovery::platform::PlatformError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReconciliationInventoryLimits {
    pub max_directories: usize,
    pub max_entries: usize,
    pub max_fingerprint_bytes: u64,
}

impl Default for ReconciliationInventoryLimits {
    fn default() -> Self {
        Self {
            max_directories: 16_384,
            max_entries: 1_048_576,
            max_fingerprint_bytes: 64 * 1024 * 1024,
        }
    }
}

impl ReconciliationInventoryLimits {
    fn validate(self) -> Result<Self, ReconciliationInventoryError> {
        if self.max_directories == 0 || self.max_entries == 0 || self.max_fingerprint_bytes == 0 {
            return Err(ReconciliationInventoryError::InvalidLimits);
        }
        Ok(self)
    }
}

#[derive(Debug, Error)]
pub(crate) enum ReconciliationInventoryError {
    #[error("reconciliation inventory limits must all be nonzero")]
    InvalidLimits,
    #[error("reconciliation requires {actual} directories, exceeding limit {maximum}")]
    DirectoryLimit { actual: usize, maximum: usize },
    #[error("reconciliation directory {directory:?} is absent")]
    MissingDirectory { directory: Box<str> },
    #[error("reconciliation inventory allocation exceeded its declared limit")]
    AllocationLimit,
    #[error("entry {path:?} has {links} physical links; exactly one is required")]
    HardLinkAlias { path: StoreRelativePath, links: u64 },
    #[error("entry {path:?} is too large to fingerprint: {length} bytes exceeds {maximum}")]
    FingerprintLimit {
        path: StoreRelativePath,
        length: u64,
        maximum: u64,
    },
    #[error("entry {path:?} changed length while its fingerprint was read")]
    FingerprintLengthChanged { path: StoreRelativePath },
    #[error("managed segment directory {directory:?} changed between inventory phases")]
    InventoryChanged { directory: Box<str> },
    #[error("cannot represent a physical identity on this target")]
    UnsupportedPhysicalIdentity,
    #[error("invalid store-relative inventory path")]
    Identity(#[source] IdentityError),
    #[error("handle-relative namespace inventory failed")]
    Platform(#[source] PlatformError),
    #[error("positional namespace read failed")]
    Io(#[source] io::Error),
}

impl From<PlatformError> for ReconciliationInventoryError {
    fn from(source: PlatformError) -> Self {
        Self::Platform(source)
    }
}

pub(super) fn scan(
    root: &File,
    store_uuid: StoreUuid,
    recovered: &RecoveredLedgerState,
    limits: ReconciliationInventoryLimits,
) -> Result<StableNamespaceInventory, ReconciliationInventoryError> {
    let limits = limits.validate()?;
    let known_paths = collect_known_paths(recovered).map_err(|_| ReconciliationInventoryError::AllocationLimit)?;
    let complete_directories = known_paths
        .iter()
        .map(parent_directory)
        .map(Box::<str>::from)
        .collect::<BTreeSet<_>>();
    if complete_directories.len() > limits.max_directories {
        return Err(ReconciliationInventoryError::DirectoryLimit {
            actual: complete_directories.len(),
            maximum: limits.max_directories,
        });
    }

    let fingerprints = recovered
        .quarantines
        .values()
        .filter_map(|entry| {
            entry.content_fingerprint.map(|fingerprint| {
                (
                    entry.destination_path.as_ref().unwrap_or(&entry.source_path).clone(),
                    fingerprint,
                )
            })
        })
        .collect::<BTreeMap<_, _>>();
    let mut entries = BTreeMap::new();
    let mut retained_files = BTreeMap::new();
    for directory_path in &complete_directories {
        scan_directory(
            root,
            directory_path,
            &fingerprints,
            limits,
            &mut entries,
            &mut retained_files,
        )?;
    }
    Ok(StableNamespaceInventory {
        store_uuid,
        complete_directories,
        entries,
        retained_files,
        requires_retained_files: true,
    })
}

fn scan_directory(
    root: &File,
    directory_path: &str,
    fingerprints: &BTreeMap<StoreRelativePath, ContentFingerprint>,
    limits: ReconciliationInventoryLimits,
    output: &mut BTreeMap<StoreRelativePath, NamespaceObject>,
    retained_files: &mut BTreeMap<StoreRelativePath, File>,
) -> Result<(), ReconciliationInventoryError> {
    let directory = LifecycleDirectory::open(root, directory_path)?.ok_or_else(|| {
        ReconciliationInventoryError::MissingDirectory {
            directory: directory_path.into(),
        }
    })?;
    let remaining = limits
        .max_entries
        .checked_sub(output.len())
        .ok_or(ReconciliationInventoryError::AllocationLimit)?;
    let first = directory.enumerate(remaining)?;
    let mut opened = Vec::new();
    opened
        .try_reserve_exact(first.entries.len())
        .map_err(|_| ReconciliationInventoryError::AllocationLimit)?;
    for entry in &first.entries {
        let path = join_inventory_path(directory_path, &entry.name)?;
        let handle = directory.open_entry(entry)?;
        if entry.kind == EntryKind::File && handle.stamp().link_count != 1 {
            return Err(ReconciliationInventoryError::HardLinkAlias {
                path,
                links: handle.stamp().link_count,
            });
        }
        opened.push((path, entry.clone(), handle));
    }

    let second = directory.enumerate(remaining)?;
    if second != first {
        return Err(ReconciliationInventoryError::InventoryChanged {
            directory: directory_path.into(),
        });
    }
    verify_opened(&opened)?;

    for (path, entry, handle) in &opened {
        let content_fingerprint = match fingerprints.get(path) {
            Some(_) => Some(fingerprint(handle.file(), path, handle.stamp().length, limits)?),
            None => None,
        };
        let object = match entry.kind {
            EntryKind::File => NamespaceObject::RegularFile {
                physical_key: handle
                    .stamp()
                    .physical_key()
                    .ok_or(ReconciliationInventoryError::UnsupportedPhysicalIdentity)?,
                length: handle.stamp().length,
                content_fingerprint,
            },
            EntryKind::Directory => NamespaceObject::Directory,
            EntryKind::Reparse => NamespaceObject::ReparsePoint,
            EntryKind::Other => NamespaceObject::Other,
        };
        if output.insert(path.clone(), object).is_some() {
            return Err(ReconciliationInventoryError::AllocationLimit);
        }
    }

    let third = directory.enumerate(remaining)?;
    if third != first {
        return Err(ReconciliationInventoryError::InventoryChanged {
            directory: directory_path.into(),
        });
    }
    verify_opened(&opened)?;
    for (path, entry, handle) in opened {
        if entry.kind == EntryKind::File && retained_files.insert(path, handle.into_file()).is_some() {
            return Err(ReconciliationInventoryError::AllocationLimit);
        }
    }
    Ok(())
}

fn verify_opened(
    opened: &[(StoreRelativePath, InventoryEntry, OpenedEntry)],
) -> Result<(), ReconciliationInventoryError> {
    for (_, expected, handle) in opened {
        handle.verify(expected)?;
    }
    Ok(())
}

fn join_inventory_path(directory: &str, name: &str) -> Result<StoreRelativePath, ReconciliationInventoryError> {
    let joined = if directory.is_empty() {
        name.to_owned()
    } else {
        format!("{directory}/{name}")
    };
    StoreRelativePath::new(&joined).map_err(ReconciliationInventoryError::Identity)
}

fn fingerprint(
    file: &File,
    path: &StoreRelativePath,
    length: u64,
    limits: ReconciliationInventoryLimits,
) -> Result<ContentFingerprint, ReconciliationInventoryError> {
    if length > limits.max_fingerprint_bytes {
        return Err(ReconciliationInventoryError::FingerprintLimit {
            path: path.clone(),
            length,
            maximum: limits.max_fingerprint_bytes,
        });
    }
    let mut offset = 0_u64;
    let mut crc = u32::MAX;
    let mut buffer = [0_u8; 16 * 1024];
    while offset < length {
        let remaining = length - offset;
        let requested = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| ReconciliationInventoryError::AllocationLimit)?;
        let read = positional_read(file, &mut buffer[..requested], offset).map_err(ReconciliationInventoryError::Io)?;
        if read == 0 {
            return Err(ReconciliationInventoryError::FingerprintLengthChanged { path: path.clone() });
        }
        update_crc32(&mut crc, &buffer[..read]);
        offset = offset
            .checked_add(u64::try_from(read).map_err(|_| ReconciliationInventoryError::AllocationLimit)?)
            .ok_or(ReconciliationInventoryError::AllocationLimit)?;
    }
    Ok(ContentFingerprint { length, crc32: !crc })
}

fn update_crc32(crc: &mut u32, bytes: &[u8]) {
    for byte in bytes {
        *crc ^= u32::from(*byte);
        for _ in 0..8 {
            *crc = (*crc >> 1) ^ (0xedb8_8320 & (0_u32.wrapping_sub(*crc & 1)));
        }
    }
}

#[cfg(unix)]
fn positional_read(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;

    file.read_at(buffer, offset)
}

#[cfg(windows)]
fn positional_read(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
    use std::os::windows::fs::FileExt;

    file.seek_read(buffer, offset)
}

#[cfg(not(any(unix, windows)))]
fn positional_read(_file: &File, _buffer: &mut [u8], _offset: u64) -> io::Result<usize> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "positional lifecycle reads are unsupported on this target",
    ))
}

#[cfg(test)]
mod tests;
