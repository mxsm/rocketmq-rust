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

use std::ffi::CStr;
use std::ffi::CString;
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::os::unix::fs::MetadataExt;

use super::EntryKind;
use super::FileStamp;
use super::InventoryEntry;
use super::InventorySnapshot;
use super::OpenedEntry;
use super::PlatformError;

const STRICT_RESOLVE: u64 =
    libc::RESOLVE_BENEATH | libc::RESOLVE_NO_SYMLINKS | libc::RESOLVE_NO_MAGICLINKS | libc::RESOLVE_NO_XDEV;

pub(in crate::mapped_file::retirement) struct LifecycleDirectory {
    root: File,
    file: File,
    name: String,
    initial_stamp: FileStamp,
}

impl LifecycleDirectory {
    pub(in crate::mapped_file::retirement) fn open(root: &File, name: &str) -> Result<Option<Self>, PlatformError> {
        let root_stamp = stamp(root)?;
        if root_stamp.kind != EntryKind::Directory {
            return Err(PlatformError::unsafe_namespace(
                "retained Store root is not a directory",
            ));
        }
        let retained_root = root
            .try_clone()
            .map_err(|source| PlatformError::io("duplicate retained Store-root handle", source))?;
        let Some(file) = open_directory_path(&retained_root, name, true)? else {
            return Ok(None);
        };
        let opened = stamp(&file)?;
        if opened.kind == EntryKind::Reparse {
            return Err(PlatformError::unsafe_namespace(
                ".rocketmq-lifecycle is a symbolic link",
            ));
        }
        if opened.kind != EntryKind::Directory {
            return Err(PlatformError::unsafe_namespace(
                ".rocketmq-lifecycle is not a directory",
            ));
        }
        Ok(Some(Self {
            root: retained_root,
            file,
            name: name.to_owned(),
            initial_stamp: opened,
        }))
    }

    pub(in crate::mapped_file::retirement) fn enumerate(
        &self,
        maximum: usize,
    ) -> Result<InventorySnapshot, PlatformError> {
        self.verify_parent_binding()?;
        let before = stamp(&self.file)?;
        let mut entries = enumerate_directory(&self.file, maximum)?;
        let after = stamp(&self.file)?;
        if before != after {
            return Err(PlatformError::changed(
                "lifecycle directory changed during one complete enumeration",
            ));
        }
        self.verify_parent_binding()?;
        entries.sort();
        Ok(InventorySnapshot {
            directory_stamp: after,
            entries,
        })
    }

    pub(in crate::mapped_file::retirement) fn open_entry(
        &self,
        entry: &InventoryEntry,
    ) -> Result<OpenedEntry, PlatformError> {
        self.verify_parent_binding()?;
        open_entry(&self.file, entry)
    }

    fn verify_parent_binding(&self) -> Result<(), PlatformError> {
        let reopened = open_directory_path(&self.root, &self.name, false)?
            .ok_or_else(|| PlatformError::changed("lifecycle directory binding disappeared"))?;
        let actual = stamp(&reopened)?;
        if actual.kind == EntryKind::Reparse {
            return Err(PlatformError::unsafe_namespace(
                ".rocketmq-lifecycle was rebound to a symbolic link",
            ));
        }
        if actual != self.initial_stamp {
            return Err(PlatformError::changed(
                ".rocketmq-lifecycle parent binding changed during discovery",
            ));
        }
        Ok(())
    }
}

pub(super) fn open_entry(parent: &File, entry: &InventoryEntry) -> Result<OpenedEntry, PlatformError> {
    let flags = match entry.kind {
        EntryKind::Directory => libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY,
        EntryKind::File => libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NONBLOCK,
        EntryKind::Reparse => {
            return Err(PlatformError::unsafe_namespace(format!(
                "entry {:?} is a symbolic link",
                entry.name
            )))
        }
        EntryKind::Other => {
            return Err(PlatformError::unsafe_namespace(format!(
                "entry {:?} is not a regular file or directory",
                entry.name
            )))
        }
    };
    let file = open_relative(
        parent,
        &entry.name,
        u64::try_from(flags).map_err(|_| PlatformError::limit("Linux open flags"))?,
        false,
    )?
    .ok_or_else(|| PlatformError::changed(format!("entry {:?} disappeared", entry.name)))?;
    let actual = stamp(&file)?;
    if actual != entry.stamp {
        return Err(PlatformError::changed(format!(
            "entry {:?} was replaced between enumeration and open",
            entry.name
        )));
    }
    Ok(OpenedEntry::new(file, actual))
}

pub(super) fn stamp(file: &File) -> Result<FileStamp, PlatformError> {
    let metadata = file
        .metadata()
        .map_err(|source| PlatformError::io("fstat retained lifecycle handle", source))?;
    let kind = if metadata.file_type().is_symlink() {
        EntryKind::Reparse
    } else if metadata.is_dir() {
        EntryKind::Directory
    } else if metadata.is_file() {
        EntryKind::File
    } else {
        EntryKind::Other
    };
    let mut file_id = [0_u8; 16];
    file_id[..8].copy_from_slice(&metadata.ino().to_le_bytes());
    Ok(FileStamp {
        volume: metadata.dev(),
        file_id,
        link_count: metadata.nlink(),
        length: metadata.size(),
        allocation_size: metadata.blocks().saturating_mul(512),
        created: [0, 0],
        modified: [metadata.mtime(), metadata.mtime_nsec()],
        changed: [metadata.ctime(), metadata.ctime_nsec()],
        attributes: metadata.mode(),
        reparse_tag: 0,
        kind,
    })
}

pub(super) fn enumerate_directory(directory: &File, maximum: usize) -> Result<Vec<InventoryEntry>, PlatformError> {
    // SAFETY: fcntl duplicates a live directory descriptor; ownership of the returned descriptor is
    // transferred immediately to fdopendir below or closed on the fdopendir failure path.
    let duplicate = unsafe { libc::fcntl(directory.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
    if duplicate < 0 {
        return Err(PlatformError::io(
            "duplicate lifecycle directory handle",
            io::Error::last_os_error(),
        ));
    }
    // SAFETY: `duplicate` is a newly owned directory descriptor and fdopendir consumes it on success.
    let raw_stream = unsafe { libc::fdopendir(duplicate) };
    if raw_stream.is_null() {
        let source = io::Error::last_os_error();
        // SAFETY: fdopendir failed and therefore did not consume the duplicate descriptor.
        unsafe { libc::close(duplicate) };
        return Err(PlatformError::io("fdopendir lifecycle directory", source));
    }
    let stream = DirectoryStream(raw_stream);
    // SAFETY: the DIR pointer is valid and exclusively owned by `stream`.
    unsafe { libc::rewinddir(stream.0) };

    let mut entries = Vec::new();
    entries
        .try_reserve_exact(maximum.min(16))
        .map_err(|_| PlatformError::limit("directory inventory allocation failed"))?;
    loop {
        // SAFETY: errno is thread-local on Linux and may be cleared before readdir to distinguish EOF.
        unsafe { *libc::__errno_location() = 0 };
        // SAFETY: stream owns a live DIR pointer and no concurrent calls use it.
        let raw_entry = unsafe { libc::readdir(stream.0) };
        if raw_entry.is_null() {
            let source = io::Error::last_os_error();
            if source.raw_os_error().unwrap_or(0) == 0 {
                break;
            }
            return Err(PlatformError::io("readdir lifecycle directory", source));
        }
        // SAFETY: readdir returned a live dirent whose nul-terminated d_name is valid until next call.
        let name_bytes = unsafe { CStr::from_ptr((*raw_entry).d_name.as_ptr()) }.to_bytes();
        if matches!(name_bytes, b"." | b"..") {
            continue;
        }
        if !name_bytes.is_ascii() {
            return Err(PlatformError::unsafe_namespace(
                "lifecycle directory contains a non-ASCII name",
            ));
        }
        if entries.len() >= maximum {
            return Err(PlatformError::limit(format!("directory entry count exceeds {maximum}")));
        }
        let name = std::str::from_utf8(name_bytes)
            .map_err(|_| PlatformError::unsafe_namespace("lifecycle name is not UTF-8"))?
            .to_owned();
        let probe = open_relative(
            directory,
            &name,
            u64::try_from(libc::O_PATH | libc::O_CLOEXEC | libc::O_NOFOLLOW)
                .map_err(|_| PlatformError::limit("Linux probe flags"))?,
            false,
        )?
        .ok_or_else(|| PlatformError::changed(format!("entry {name:?} disappeared during enumeration")))?;
        let entry_stamp = stamp(&probe)?;
        entries.push(InventoryEntry {
            name,
            kind: entry_stamp.kind,
            stamp: entry_stamp,
        });
    }
    Ok(entries)
}

fn open_directory_path(root: &File, path: &str, absent_ok: bool) -> Result<Option<File>, PlatformError> {
    if path.is_empty() {
        return root
            .try_clone()
            .map(Some)
            .map_err(|source| PlatformError::io("duplicate retained Store-root handle", source));
    }
    let flags = u64::try_from(libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY)
        .map_err(|_| PlatformError::limit("Linux open flags"))?;
    let mut current = root
        .try_clone()
        .map_err(|source| PlatformError::io("duplicate retained Store-root handle", source))?;
    for component in path.split('/') {
        let Some(next) = open_relative(&current, component, flags, absent_ok)? else {
            return Ok(None);
        };
        current = next;
    }
    Ok(Some(current))
}

fn open_relative(parent: &File, name: &str, flags: u64, absent_ok: bool) -> Result<Option<File>, PlatformError> {
    if name.is_empty() || matches!(name, "." | "..") || name.contains(['/', '\\']) {
        return Err(PlatformError::unsafe_namespace(
            "relative component is not canonical UTF-8",
        ));
    }
    let name = CString::new(name).map_err(|_| PlatformError::unsafe_namespace("relative component contains NUL"))?;
    // SAFETY: `open_how` contains only integer fields, so its all-zero bit pattern is valid.
    let mut how: libc::open_how = unsafe { std::mem::zeroed() };
    how.flags = flags;
    how.resolve = STRICT_RESOLVE;
    // SAFETY: the parent descriptor is retained, `name` is one live nul-terminated component, and
    // `how` requests a read/query-only open with strict beneath/no-link/no-magic-link/no-xdev resolution.
    let result = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            parent.as_raw_fd(),
            name.as_ptr(),
            &how,
            std::mem::size_of::<libc::open_how>(),
        )
    };
    if result < 0 {
        let source = io::Error::last_os_error();
        return match source.raw_os_error() {
            Some(libc::ENOENT) if absent_ok => Ok(None),
            Some(libc::ENOENT) => Err(PlatformError::changed(format!("relative entry {name:?} disappeared"))),
            Some(libc::ELOOP) | Some(libc::EXDEV) => Err(PlatformError::unsafe_namespace(format!(
                "openat2 rejected relative entry {name:?}: {source}"
            ))),
            Some(libc::ENOSYS) => Err(PlatformError::unsupported()),
            _ => Err(PlatformError::io("openat2 lifecycle entry", source)),
        };
    }
    let descriptor = i32::try_from(result).map_err(|_| PlatformError::limit("openat2 descriptor"))?;
    // SAFETY: successful openat2 returned unique ownership of this descriptor.
    Ok(Some(unsafe { File::from_raw_fd(descriptor) }))
}

struct DirectoryStream(*mut libc::DIR);

impl Drop for DirectoryStream {
    fn drop(&mut self) {
        // SAFETY: this type exclusively owns a non-null DIR pointer returned by fdopendir.
        unsafe { libc::closedir(self.0) };
    }
}
