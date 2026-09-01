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

use std::fs::File;

use thiserror::Error;

use crate::mapped_file::retirement::identity::PhysicalFileKey;

#[cfg(target_os = "linux")]
#[path = "platform/linux.rs"]
mod imp;
#[cfg(windows)]
#[path = "platform/windows.rs"]
mod imp;
#[cfg(not(any(target_os = "linux", windows)))]
#[path = "platform/unsupported.rs"]
mod imp;

pub(in crate::mapped_file::retirement) use imp::LifecycleDirectory;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::mapped_file::retirement) enum EntryKind {
    File,
    Directory,
    Reparse,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::mapped_file::retirement) struct FileStamp {
    pub(in crate::mapped_file::retirement) volume: u64,
    pub(in crate::mapped_file::retirement) file_id: [u8; 16],
    pub(in crate::mapped_file::retirement) link_count: u64,
    pub(in crate::mapped_file::retirement) length: u64,
    pub(in crate::mapped_file::retirement) allocation_size: u64,
    pub(in crate::mapped_file::retirement) created: [i64; 2],
    pub(in crate::mapped_file::retirement) modified: [i64; 2],
    pub(in crate::mapped_file::retirement) changed: [i64; 2],
    pub(in crate::mapped_file::retirement) attributes: u32,
    pub(in crate::mapped_file::retirement) reparse_tag: u32,
    pub(in crate::mapped_file::retirement) kind: EntryKind,
}

impl FileStamp {
    pub(in crate::mapped_file::retirement) fn physical_key(&self) -> Option<PhysicalFileKey> {
        #[cfg(target_os = "linux")]
        {
            let mut inode = [0_u8; 8];
            inode.copy_from_slice(&self.file_id[..8]);
            Some(PhysicalFileKey::unix(self.volume, u64::from_le_bytes(inode)))
        }
        #[cfg(windows)]
        {
            Some(PhysicalFileKey::windows(self.volume, self.file_id))
        }
        #[cfg(not(any(target_os = "linux", windows)))]
        {
            None
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::mapped_file::retirement) struct InventoryEntry {
    pub(in crate::mapped_file::retirement) name: String,
    pub(in crate::mapped_file::retirement) kind: EntryKind,
    pub(in crate::mapped_file::retirement) stamp: FileStamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::mapped_file::retirement) struct InventorySnapshot {
    pub(in crate::mapped_file::retirement) directory_stamp: FileStamp,
    pub(in crate::mapped_file::retirement) entries: Vec<InventoryEntry>,
}

pub(in crate::mapped_file::retirement) struct OpenedEntry {
    pub(in crate::mapped_file::retirement) file: File,
    stamp: FileStamp,
}

impl OpenedEntry {
    pub(super) const fn new(file: File, stamp: FileStamp) -> Self {
        Self { file, stamp }
    }

    pub(super) const fn length(&self) -> u64 {
        self.stamp.length
    }

    pub(in crate::mapped_file::retirement) const fn stamp(&self) -> &FileStamp {
        &self.stamp
    }

    pub(in crate::mapped_file::retirement) const fn file(&self) -> &File {
        &self.file
    }

    pub(in crate::mapped_file::retirement) fn into_file(self) -> File {
        self.file
    }

    pub(in crate::mapped_file::retirement) fn verify(&self, expected: &InventoryEntry) -> Result<(), PlatformError> {
        let actual = imp::stamp(&self.file)?;
        if actual != self.stamp || actual != expected.stamp {
            return Err(PlatformError::changed(format!(
                "entry {:?} changed after its retained handle was opened",
                expected.name
            )));
        }
        Ok(())
    }

    pub(in crate::mapped_file::retirement) fn enumerate(
        &self,
        maximum: usize,
    ) -> Result<InventorySnapshot, PlatformError> {
        if self.stamp.kind != EntryKind::Directory {
            return Err(PlatformError::unsafe_namespace(
                "retained lifecycle entry is not a directory",
            ));
        }
        let before = imp::stamp(&self.file)?;
        if before != self.stamp {
            return Err(PlatformError::changed(
                "retained lifecycle directory changed before enumeration",
            ));
        }
        let mut entries = imp::enumerate_directory(&self.file, maximum)?;
        let after = imp::stamp(&self.file)?;
        if after != before {
            return Err(PlatformError::changed(
                "retained lifecycle directory changed during one complete enumeration",
            ));
        }
        entries.sort();
        Ok(InventorySnapshot {
            directory_stamp: after,
            entries,
        })
    }

    pub(in crate::mapped_file::retirement) fn open_entry(&self, entry: &InventoryEntry) -> Result<Self, PlatformError> {
        if self.stamp.kind != EntryKind::Directory {
            return Err(PlatformError::unsafe_namespace(
                "retained lifecycle entry is not a directory",
            ));
        }
        imp::open_entry(&self.file, entry)
    }
}

#[derive(Debug, Error)]
pub(crate) enum PlatformError {
    #[error("{context}: {source}")]
    Io {
        context: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[cfg(windows)]
    #[error("{context}: {source}")]
    Windows {
        context: &'static str,
        #[source]
        source: windows::core::Error,
    },
    #[error("unsafe lifecycle namespace: {detail}")]
    UnsafeNamespace { detail: String },
    #[error("lifecycle inventory changed: {detail}")]
    Changed { detail: String },
    #[error("lifecycle discovery limit exceeded: {detail}")]
    Limit { detail: String },
    #[error("handle-relative lifecycle discovery is unsupported on this target")]
    Unsupported,
}

impl PlatformError {
    pub(super) fn io(context: &'static str, error: std::io::Error) -> Self {
        Self::Io { context, source: error }
    }

    #[cfg(windows)]
    pub(super) fn windows(context: &'static str, source: windows::core::Error) -> Self {
        Self::Windows { context, source }
    }

    pub(super) fn unsafe_namespace(detail: impl Into<String>) -> Self {
        Self::UnsafeNamespace { detail: detail.into() }
    }

    pub(super) fn changed(detail: impl Into<String>) -> Self {
        Self::Changed { detail: detail.into() }
    }

    pub(super) fn limit(detail: impl Into<String>) -> Self {
        Self::Limit { detail: detail.into() }
    }

    pub(super) fn unsupported() -> Self {
        Self::Unsupported
    }
}
