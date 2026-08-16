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

use std::ffi::c_void;
use std::fs::File;
use std::io;
use std::mem::size_of;
use std::mem::MaybeUninit;
use std::os::windows::io::AsRawHandle;
use std::os::windows::io::FromRawHandle;
use std::ptr;

use windows::core::PWSTR;
use windows::Wdk::Foundation::OBJECT_ATTRIBUTES;
use windows::Wdk::Storage::FileSystem::FileIdExtdDirectoryInformation;
use windows::Wdk::Storage::FileSystem::NtCreateFile;
use windows::Wdk::Storage::FileSystem::NtQueryDirectoryFile;
use windows::Wdk::Storage::FileSystem::FILE_DIRECTORY_FILE;
use windows::Wdk::Storage::FileSystem::FILE_ID_EXTD_DIR_INFORMATION;
use windows::Wdk::Storage::FileSystem::FILE_NON_DIRECTORY_FILE;
use windows::Wdk::Storage::FileSystem::FILE_OPEN;
use windows::Wdk::Storage::FileSystem::FILE_OPEN_REPARSE_POINT;
use windows::Wdk::Storage::FileSystem::FILE_SYNCHRONOUS_IO_NONALERT;
use windows::Win32::Foundation::RtlNtStatusToDosError;
use windows::Win32::Foundation::HANDLE;
use windows::Win32::Foundation::NTSTATUS;
use windows::Win32::Foundation::OBJECT_ATTRIBUTE_FLAGS;
use windows::Win32::Foundation::STATUS_NO_MORE_FILES;
use windows::Win32::Foundation::STATUS_OBJECT_NAME_NOT_FOUND;
use windows::Win32::Foundation::UNICODE_STRING;
use windows::Win32::Storage::FileSystem::FileAttributeTagInfo;
use windows::Win32::Storage::FileSystem::FileBasicInfo;
use windows::Win32::Storage::FileSystem::FileIdInfo;
use windows::Win32::Storage::FileSystem::FileStandardInfo;
use windows::Win32::Storage::FileSystem::GetFileInformationByHandleEx;
use windows::Win32::Storage::FileSystem::FILE_ACCESS_RIGHTS;
use windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_DIRECTORY;
use windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
use windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_TAG_INFO;
use windows::Win32::Storage::FileSystem::FILE_BASIC_INFO;
use windows::Win32::Storage::FileSystem::FILE_FLAGS_AND_ATTRIBUTES;
use windows::Win32::Storage::FileSystem::FILE_ID_INFO;
use windows::Win32::Storage::FileSystem::FILE_LIST_DIRECTORY;
use windows::Win32::Storage::FileSystem::FILE_READ_ATTRIBUTES;
use windows::Win32::Storage::FileSystem::FILE_READ_DATA;
use windows::Win32::Storage::FileSystem::FILE_SHARE_DELETE;
use windows::Win32::Storage::FileSystem::FILE_SHARE_MODE;
use windows::Win32::Storage::FileSystem::FILE_SHARE_READ;
use windows::Win32::Storage::FileSystem::FILE_SHARE_WRITE;
use windows::Win32::Storage::FileSystem::FILE_STANDARD_INFO;
use windows::Win32::System::IO::IO_STATUS_BLOCK;

use super::EntryKind;
use super::FileStamp;
use super::InventoryEntry;
use super::InventorySnapshot;
use super::OpenedEntry;
use super::PlatformError;

const SYNCHRONIZE_ACCESS: u32 = 0x0010_0000;
const DIRECTORY_BUFFER_LENGTH: usize = 64 * 1024;
const DIRECTORY_ENTRY_HEADER_LENGTH: usize = std::mem::offset_of!(FILE_ID_EXTD_DIR_INFORMATION, FileName);

pub(in crate::mapped_file::retirement) struct LifecycleDirectory {
    root: File,
    file: File,
    name: String,
    initial_stamp: FileStamp,
}

impl LifecycleDirectory {
    pub(in crate::mapped_file::retirement) fn open(root: &File, name: &str) -> Result<Option<Self>, PlatformError> {
        let root_stamp = stamp(root)?;
        if root_stamp.kind == EntryKind::Reparse {
            return Err(PlatformError::unsafe_namespace(
                "retained Store root is a reparse point",
            ));
        }
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
                ".rocketmq-lifecycle is a reparse point",
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
                ".rocketmq-lifecycle was rebound to a reparse point",
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
    let expected_kind = match entry.kind {
        EntryKind::File => EntryKind::File,
        EntryKind::Directory => EntryKind::Directory,
        EntryKind::Reparse => {
            return Err(PlatformError::unsafe_namespace(format!(
                "entry {:?} is a reparse point",
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
    let file = open_relative(parent, &entry.name, expected_kind, false)?
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
    let handle = HANDLE(file.as_raw_handle());
    let id: FILE_ID_INFO = query_handle(handle, FileIdInfo, "query file identity")?;
    let basic: FILE_BASIC_INFO = query_handle(handle, FileBasicInfo, "query basic file information")?;
    let standard: FILE_STANDARD_INFO = query_handle(handle, FileStandardInfo, "query standard file information")?;
    let tag: FILE_ATTRIBUTE_TAG_INFO = query_handle(handle, FileAttributeTagInfo, "query reparse attributes")?;
    let kind = kind_from_attributes(tag.FileAttributes);
    let length =
        u64::try_from(standard.EndOfFile).map_err(|_| PlatformError::unsafe_namespace("file has a negative length"))?;
    let allocation_size = u64::try_from(standard.AllocationSize)
        .map_err(|_| PlatformError::unsafe_namespace("file has a negative allocation size"))?;
    Ok(FileStamp {
        volume: id.VolumeSerialNumber,
        file_id: id.FileId.Identifier,
        link_count: u64::from(standard.NumberOfLinks),
        length,
        allocation_size,
        created: [basic.CreationTime, 0],
        modified: [basic.LastWriteTime, 0],
        changed: [basic.ChangeTime, 0],
        attributes: tag.FileAttributes,
        reparse_tag: tag.ReparseTag,
        kind,
    })
}

fn query_handle<T: Copy>(
    handle: HANDLE,
    class: windows::Win32::Storage::FileSystem::FILE_INFO_BY_HANDLE_CLASS,
    context: &'static str,
) -> Result<T, PlatformError> {
    let mut value = MaybeUninit::<T>::uninit();
    // SAFETY: `value` is aligned writable storage for exactly `T`; the handle is borrowed for the
    // call, and each requested information class is paired with its documented fixed structure.
    unsafe {
        GetFileInformationByHandleEx(
            handle,
            class,
            value.as_mut_ptr().cast::<c_void>(),
            u32::try_from(size_of::<T>()).map_err(|_| PlatformError::limit("handle info size"))?,
        )
        .map_err(|error| PlatformError::windows(context, error))?;
        Ok(value.assume_init())
    }
}

fn open_directory_path(root: &File, path: &str, absent_ok: bool) -> Result<Option<File>, PlatformError> {
    if path.is_empty() {
        return root
            .try_clone()
            .map(Some)
            .map_err(|source| PlatformError::io("duplicate retained Store-root handle", source));
    }
    let mut current = root
        .try_clone()
        .map_err(|source| PlatformError::io("duplicate retained Store-root handle", source))?;
    for component in path.split('/') {
        let Some(next) = open_relative(&current, component, EntryKind::Directory, absent_ok)? else {
            return Ok(None);
        };
        current = next;
    }
    Ok(Some(current))
}

fn open_relative(parent: &File, name: &str, kind: EntryKind, absent_ok: bool) -> Result<Option<File>, PlatformError> {
    if name.is_empty() || matches!(name, "." | "..") || name.contains(['/', '\\', '\0']) {
        return Err(PlatformError::unsafe_namespace(
            "relative component is not canonical UTF-8",
        ));
    }
    let mut wide = name.encode_utf16().collect::<Vec<_>>();
    let byte_length = wide
        .len()
        .checked_mul(2)
        .and_then(|value| u16::try_from(value).ok())
        .ok_or_else(|| PlatformError::limit("relative component is too long"))?;
    let unicode = UNICODE_STRING {
        Length: byte_length,
        MaximumLength: byte_length,
        Buffer: PWSTR(wide.as_mut_ptr()),
    };
    let attributes = OBJECT_ATTRIBUTES {
        Length: u32::try_from(size_of::<OBJECT_ATTRIBUTES>())
            .map_err(|_| PlatformError::limit("OBJECT_ATTRIBUTES size"))?,
        RootDirectory: HANDLE(parent.as_raw_handle()),
        ObjectName: &unicode,
        Attributes: OBJECT_ATTRIBUTE_FLAGS(0),
        SecurityDescriptor: ptr::null(),
        SecurityQualityOfService: ptr::null(),
    };
    let desired = match kind {
        EntryKind::Directory => FILE_ACCESS_RIGHTS(FILE_LIST_DIRECTORY.0 | FILE_READ_ATTRIBUTES.0 | SYNCHRONIZE_ACCESS),
        EntryKind::File => FILE_ACCESS_RIGHTS(FILE_READ_DATA.0 | FILE_READ_ATTRIBUTES.0 | SYNCHRONIZE_ACCESS),
        EntryKind::Reparse | EntryKind::Other => {
            return Err(PlatformError::unsafe_namespace("unsupported relative entry kind"))
        }
    };
    let options = match kind {
        EntryKind::Directory => FILE_DIRECTORY_FILE.0,
        EntryKind::File => FILE_NON_DIRECTORY_FILE.0,
        EntryKind::Reparse | EntryKind::Other => {
            return Err(PlatformError::unsafe_namespace("unsupported relative entry kind"))
        }
    } | FILE_OPEN_REPARSE_POINT.0
        | FILE_SYNCHRONOUS_IO_NONALERT.0;
    let share = FILE_SHARE_MODE(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0 | FILE_SHARE_DELETE.0);
    let mut handle = HANDLE(ptr::null_mut());
    let mut io_status = IO_STATUS_BLOCK::default();
    // SAFETY: every pointer references live fixed storage for the duration of the synchronous call;
    // `RootDirectory` is a retained parent handle, the name is one validated component, and the
    // access/disposition/options request only an existing object for read/query without following
    // a reparse point.
    let status = unsafe {
        NtCreateFile(
            &mut handle,
            desired,
            &attributes,
            &mut io_status,
            None,
            FILE_FLAGS_AND_ATTRIBUTES(0),
            share,
            FILE_OPEN,
            windows::Wdk::Storage::FileSystem::NTCREATEFILE_CREATE_OPTIONS(options),
            None,
            0,
        )
    };
    if status == STATUS_OBJECT_NAME_NOT_FOUND {
        if absent_ok {
            return Ok(None);
        }
        return Err(PlatformError::changed(format!("relative entry {name:?} disappeared")));
    }
    if !nt_success(status) {
        return Err(PlatformError::io("NtCreateFile", status_error(status)));
    }
    if handle.is_invalid() {
        return Err(PlatformError::io(
            "NtCreateFile",
            io::Error::other("successful call returned an invalid handle"),
        ));
    }
    // SAFETY: successful NtCreateFile returned unique ownership of a valid kernel handle.
    Ok(Some(unsafe { File::from_raw_handle(handle.0) }))
}

pub(super) fn enumerate_directory(directory: &File, maximum: usize) -> Result<Vec<InventoryEntry>, PlatformError> {
    let volume = stamp(directory)?.volume;
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(maximum.min(16))
        .map_err(|_| PlatformError::limit("directory inventory allocation failed"))?;
    let mut storage = [0_u64; DIRECTORY_BUFFER_LENGTH / size_of::<u64>()];
    let mut restart = true;
    loop {
        let mut io_status = IO_STATUS_BLOCK::default();
        // SAFETY: the directory handle is synchronous and retained, `storage` is aligned writable
        // memory of the declared length, and the information class has the parsed documented layout.
        let status = unsafe {
            NtQueryDirectoryFile(
                HANDLE(directory.as_raw_handle()),
                None,
                None,
                None,
                &mut io_status,
                storage.as_mut_ptr().cast::<c_void>(),
                DIRECTORY_BUFFER_LENGTH as u32,
                FileIdExtdDirectoryInformation,
                false,
                None,
                restart,
            )
        };
        restart = false;
        if status == STATUS_NO_MORE_FILES {
            break;
        }
        if !nt_success(status) {
            return Err(PlatformError::io("NtQueryDirectoryFile", status_error(status)));
        }
        let used = io_status.Information;
        if used == 0 || used > DIRECTORY_BUFFER_LENGTH {
            return Err(PlatformError::unsafe_namespace(
                "NtQueryDirectoryFile returned an invalid byte count",
            ));
        }
        let bytes = &storage.as_slice()[..used.div_ceil(size_of::<u64>())];
        let base = bytes.as_ptr().cast::<u8>();
        let mut offset = 0_usize;
        loop {
            if offset > used.saturating_sub(DIRECTORY_ENTRY_HEADER_LENGTH) {
                return Err(PlatformError::unsafe_namespace("directory entry header is truncated"));
            }
            // SAFETY: bounds above cover the fixed prefix; Windows permits aligned offsets but
            // `read_unaligned` also handles a defensive non-aligned result.
            let info = unsafe { ptr::read_unaligned(base.add(offset).cast::<FILE_ID_EXTD_DIR_INFORMATION>()) };
            let name_bytes = usize::try_from(info.FileNameLength)
                .map_err(|_| PlatformError::limit("directory entry name length"))?;
            if name_bytes % 2 != 0 {
                return Err(PlatformError::unsafe_namespace(
                    "directory entry name has odd UTF-16 length",
                ));
            }
            let name_start = offset
                .checked_add(DIRECTORY_ENTRY_HEADER_LENGTH)
                .ok_or_else(|| PlatformError::limit("directory entry offset overflow"))?;
            let name_end = name_start
                .checked_add(name_bytes)
                .ok_or_else(|| PlatformError::limit("directory entry name overflow"))?;
            if name_end > used {
                return Err(PlatformError::unsafe_namespace("directory entry name is truncated"));
            }
            let mut name = String::with_capacity(name_bytes / 2);
            for index in 0..name_bytes / 2 {
                // SAFETY: `name_end <= used` validates every two-byte code unit in this range;
                // `read_unaligned` avoids imposing alignment requirements on the directory buffer.
                let code = unsafe { ptr::read_unaligned(base.add(name_start + index * 2).cast::<u16>()) };
                if code > 0x7f {
                    return Err(PlatformError::unsafe_namespace(
                        "lifecycle directory contains a non-ASCII name",
                    ));
                }
                name.push(char::from(code as u8));
            }
            if name != "." && name != ".." {
                if entries.len() >= maximum {
                    return Err(PlatformError::limit(format!("directory entry count exceeds {maximum}")));
                }
                let kind = kind_from_attributes(info.FileAttributes);
                let length = u64::try_from(info.EndOfFile)
                    .map_err(|_| PlatformError::unsafe_namespace("directory entry has a negative length"))?;
                let allocation_size = u64::try_from(info.AllocationSize)
                    .map_err(|_| PlatformError::unsafe_namespace("directory entry has a negative allocation size"))?;
                let listed_stamp = FileStamp {
                    volume,
                    file_id: info.FileId.Identifier,
                    link_count: 0,
                    length,
                    allocation_size,
                    created: [info.CreationTime, 0],
                    modified: [info.LastWriteTime, 0],
                    changed: [info.ChangeTime, 0],
                    attributes: info.FileAttributes,
                    reparse_tag: info.ReparsePointTag,
                    kind,
                };
                let stamp = if kind == EntryKind::Reparse {
                    listed_stamp
                } else {
                    let opened = open_relative(directory, &name, kind, false)?.ok_or_else(|| {
                        PlatformError::changed(format!("entry {name:?} disappeared during enumeration"))
                    })?;
                    let actual = stamp(&opened)?;
                    if !directory_query_matches_handle(&listed_stamp, &actual) {
                        return Err(PlatformError::changed(format!(
                            "entry {name:?} changed between directory query and handle verification"
                        )));
                    }
                    actual
                };
                entries.push(InventoryEntry { name, kind, stamp });
            }
            if info.NextEntryOffset == 0 {
                break;
            }
            let next = usize::try_from(info.NextEntryOffset)
                .map_err(|_| PlatformError::limit("directory next-entry offset"))?;
            if next < DIRECTORY_ENTRY_HEADER_LENGTH || offset.checked_add(next).is_none_or(|value| value >= used) {
                return Err(PlatformError::unsafe_namespace(
                    "directory next-entry offset is invalid",
                ));
            }
            offset += next;
        }
    }
    Ok(entries)
}

fn directory_query_matches_handle(listed: &FileStamp, actual: &FileStamp) -> bool {
    listed.volume == actual.volume
        && listed.file_id == actual.file_id
        && listed.kind == actual.kind
        && listed.attributes == actual.attributes
        && listed.reparse_tag == actual.reparse_tag
        && (listed.kind != EntryKind::File
            || (listed.length == actual.length && listed.allocation_size == actual.allocation_size))
}

fn kind_from_attributes(attributes: u32) -> EntryKind {
    if attributes & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0 {
        EntryKind::Reparse
    } else if attributes & FILE_ATTRIBUTE_DIRECTORY.0 != 0 {
        EntryKind::Directory
    } else {
        EntryKind::File
    }
}

const fn nt_success(status: NTSTATUS) -> bool {
    status.0 >= 0
}

fn status_error(status: NTSTATUS) -> io::Error {
    // SAFETY: RtlNtStatusToDosError accepts every NTSTATUS value and has no pointer arguments.
    let code = unsafe { RtlNtStatusToDosError(status) };
    io::Error::from_raw_os_error(code as i32)
}
