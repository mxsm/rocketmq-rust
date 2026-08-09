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

use std::ffi::OsStr;
use std::ffi::OsString;
use std::fs::File;
use std::io;
use std::io::Write;
use std::mem::MaybeUninit;
use std::os::windows::ffi::OsStrExt;
use std::os::windows::fs::MetadataExt;
use std::os::windows::io::AsRawHandle;
use std::os::windows::io::FromRawHandle;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;
use std::path::Prefix;
use std::ptr;

use windows_sys::Wdk::Foundation::OBJECT_ATTRIBUTES;
use windows_sys::Wdk::Storage::FileSystem::NtCreateFile;
use windows_sys::Wdk::Storage::FileSystem::FILE_DIRECTORY_FILE;
use windows_sys::Wdk::Storage::FileSystem::FILE_NON_DIRECTORY_FILE;
use windows_sys::Wdk::Storage::FileSystem::FILE_OPEN;
use windows_sys::Wdk::Storage::FileSystem::FILE_OPEN_IF;
use windows_sys::Wdk::Storage::FileSystem::FILE_OPEN_REPARSE_POINT;
use windows_sys::Wdk::Storage::FileSystem::FILE_SYNCHRONOUS_IO_NONALERT;
use windows_sys::Win32::Foundation::RtlNtStatusToDosError;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
use windows_sys::Win32::Foundation::OBJ_CASE_INSENSITIVE;
use windows_sys::Win32::Foundation::STATUS_OBJECT_NAME_NOT_FOUND;
use windows_sys::Win32::Foundation::UNICODE_STRING;
use windows_sys::Win32::Storage::FileSystem::CreateFileW;
use windows_sys::Win32::Storage::FileSystem::FileDispositionInfo;
use windows_sys::Win32::Storage::FileSystem::FileIdInfo;
use windows_sys::Win32::Storage::FileSystem::FileStandardInfo;
use windows_sys::Win32::Storage::FileSystem::GetFileInformationByHandleEx;
use windows_sys::Win32::Storage::FileSystem::SetFileInformationByHandle;
use windows_sys::Win32::Storage::FileSystem::DELETE;
use windows_sys::Win32::Storage::FileSystem::FILE_APPEND_DATA;
use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_DIRECTORY;
use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_NORMAL;
use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
use windows_sys::Win32::Storage::FileSystem::FILE_DISPOSITION_INFO;
use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS;
use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;
use windows_sys::Win32::Storage::FileSystem::FILE_ID_INFO;
use windows_sys::Win32::Storage::FileSystem::FILE_LIST_DIRECTORY;
use windows_sys::Win32::Storage::FileSystem::FILE_READ_ATTRIBUTES;
use windows_sys::Win32::Storage::FileSystem::FILE_READ_DATA;
use windows_sys::Win32::Storage::FileSystem::FILE_SHARE_DELETE;
use windows_sys::Win32::Storage::FileSystem::FILE_SHARE_READ;
use windows_sys::Win32::Storage::FileSystem::FILE_SHARE_WRITE;
use windows_sys::Win32::Storage::FileSystem::FILE_STANDARD_INFO;
use windows_sys::Win32::Storage::FileSystem::FILE_TRAVERSE;
use windows_sys::Win32::Storage::FileSystem::FILE_WRITE_ATTRIBUTES;
use windows_sys::Win32::Storage::FileSystem::FILE_WRITE_DATA;
use windows_sys::Win32::Storage::FileSystem::OPEN_EXISTING;
use windows_sys::Win32::Storage::FileSystem::SYNCHRONIZE;
use windows_sys::Win32::System::IO::IO_STATUS_BLOCK;

use super::FileIdentity;
use super::ABORT_FILE_NAME;
use super::LOCK_FILE_NAME;

const DIRECTORY_ACCESS: u32 = FILE_LIST_DIRECTORY | FILE_TRAVERSE | FILE_READ_ATTRIBUTES | SYNCHRONIZE;
const LOCK_ACCESS: u32 =
    FILE_READ_DATA | FILE_WRITE_DATA | FILE_APPEND_DATA | FILE_READ_ATTRIBUTES | FILE_WRITE_ATTRIBUTES | SYNCHRONIZE;
const SHARE_ALL: u32 = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;

pub(super) fn open_root(path: &Path, create: bool) -> io::Result<File> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let (anchor_path, components) = split_anchor(&absolute)?;
    let mut current = open_anchor(&anchor_path)?;
    for component in components {
        current = open_relative(
            &current,
            &component,
            DIRECTORY_ACCESS,
            if create { FILE_OPEN_IF } else { FILE_OPEN },
            FILE_ATTRIBUTE_DIRECTORY,
            FILE_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_NONALERT | FILE_OPEN_REPARSE_POINT,
        )?
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Store root component is absent"))?;
        reject_reparse_point(&current)?;
        verify_root_directory(&current)?;
    }
    Ok(current)
}

pub(super) fn verify_root_directory(file: &File) -> io::Result<()> {
    let metadata = file.metadata()?;
    if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 || !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Store root handle is not a real non-reparse directory",
        ));
    }
    Ok(())
}

pub(super) fn open_lock_file(root: &File, create: bool) -> io::Result<File> {
    open_relative(
        root,
        OsStr::new(LOCK_FILE_NAME),
        LOCK_ACCESS,
        if create { FILE_OPEN_IF } else { FILE_OPEN },
        FILE_ATTRIBUTE_NORMAL,
        FILE_NON_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_NONALERT | FILE_OPEN_REPARSE_POINT,
    )?
    .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Store lock file is absent"))
}

pub(super) fn verify_lock_file(file: &File) -> io::Result<()> {
    let metadata = file.metadata()?;
    if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 || !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Store lock handle is not an ordinary non-reparse file",
        ));
    }
    let mut info = MaybeUninit::<FILE_STANDARD_INFO>::uninit();
    // SAFETY: the borrowed handle remains live for the call and info is a correctly sized,
    // aligned writable FILE_STANDARD_INFO buffer initialized completely on success.
    let result = unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle() as HANDLE,
            FileStandardInfo,
            info.as_mut_ptr().cast(),
            std::mem::size_of::<FILE_STANDARD_INFO>() as u32,
        )
    };
    if result == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: GetFileInformationByHandleEx succeeded and initialized the complete buffer.
    if unsafe { info.assume_init() }.NumberOfLinks != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Store lock handle must be singly linked",
        ));
    }
    Ok(())
}

pub(super) fn file_identity(file: &File) -> io::Result<FileIdentity> {
    let mut info = MaybeUninit::<FILE_ID_INFO>::uninit();
    // SAFETY: the borrowed handle remains live for the call and info is a correctly sized,
    // aligned writable FILE_ID_INFO buffer initialized completely on success.
    let result = unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle() as HANDLE,
            FileIdInfo,
            info.as_mut_ptr().cast(),
            std::mem::size_of::<FILE_ID_INFO>() as u32,
        )
    };
    if result == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: GetFileInformationByHandleEx succeeded and initialized the complete buffer.
    let info = unsafe { info.assume_init() };
    Ok(FileIdentity {
        volume: info.VolumeSerialNumber,
        file_id: info.FileId.Identifier,
    })
}

pub(super) fn abort_marker_present(root: &File) -> io::Result<bool> {
    entry_present(root, ABORT_FILE_NAME)
}

pub(super) fn create_abort_marker(root: &File, contents: &[u8]) -> io::Result<()> {
    let mut marker = open_relative(
        root,
        OsStr::new(ABORT_FILE_NAME),
        FILE_WRITE_DATA | FILE_READ_ATTRIBUTES | FILE_WRITE_ATTRIBUTES | SYNCHRONIZE,
        FILE_OPEN_IF,
        FILE_ATTRIBUTE_NORMAL,
        FILE_NON_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_NONALERT | FILE_OPEN_REPARSE_POINT,
    )?
    .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Store abort marker could not be created"))?;
    verify_abort_marker_file(&marker)?;
    marker.set_len(0)?;
    marker.write_all(contents)?;
    marker.sync_all()
}

fn entry_present(root: &File, entry_name: &str) -> io::Result<bool> {
    open_relative(
        root,
        OsStr::new(entry_name),
        FILE_READ_ATTRIBUTES | SYNCHRONIZE,
        FILE_OPEN,
        FILE_ATTRIBUTE_NORMAL,
        FILE_SYNCHRONOUS_IO_NONALERT | FILE_OPEN_REPARSE_POINT,
    )
    .map(|entry| entry.is_some())
}

fn verify_abort_marker_file(file: &File) -> io::Result<()> {
    let metadata = file.metadata()?;
    if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 || !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Store abort marker is not an ordinary non-reparse file",
        ));
    }
    let mut info = MaybeUninit::<FILE_STANDARD_INFO>::uninit();
    // SAFETY: the borrowed handle remains live for the call and info is a correctly sized,
    // aligned writable FILE_STANDARD_INFO buffer initialized completely on success.
    let result = unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle() as HANDLE,
            FileStandardInfo,
            info.as_mut_ptr().cast(),
            std::mem::size_of::<FILE_STANDARD_INFO>() as u32,
        )
    };
    if result == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: GetFileInformationByHandleEx succeeded and initialized the complete buffer.
    if unsafe { info.assume_init() }.NumberOfLinks != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Store abort marker must be singly linked",
        ));
    }
    Ok(())
}

pub(super) fn remove_abort_marker(root: &File) -> io::Result<()> {
    let Some(marker) = open_relative(
        root,
        OsStr::new(ABORT_FILE_NAME),
        DELETE | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
        FILE_OPEN,
        FILE_ATTRIBUTE_NORMAL,
        FILE_NON_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_NONALERT | FILE_OPEN_REPARSE_POINT,
    )?
    else {
        return Ok(());
    };
    let metadata = marker.metadata()?;
    if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 || !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Store abort marker is not an ordinary non-reparse file",
        ));
    }

    let disposition = FILE_DISPOSITION_INFO { DeleteFile: true };
    // SAFETY: marker is a live handle opened with DELETE access. disposition is a correctly
    // sized immutable FILE_DISPOSITION_INFO buffer that remains valid for the call.
    let result = unsafe {
        SetFileInformationByHandle(
            marker.as_raw_handle() as HANDLE,
            FileDispositionInfo,
            (&disposition as *const FILE_DISPOSITION_INFO).cast(),
            std::mem::size_of::<FILE_DISPOSITION_INFO>() as u32,
        )
    };
    if result == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

pub(super) fn is_unsafe_path_error(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::InvalidData || error.kind() == io::ErrorKind::InvalidInput
}

fn split_anchor(path: &Path) -> io::Result<(PathBuf, Vec<OsString>)> {
    let mut components = path.components();
    let prefix = match components.next() {
        Some(Component::Prefix(prefix)) => prefix.kind(),
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Windows Store root must have an absolute disk or UNC prefix",
            ));
        }
    };
    if !matches!(components.next(), Some(Component::RootDir)) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Windows Store root must be absolute",
        ));
    }

    let anchor = match prefix {
        Prefix::Disk(letter) | Prefix::VerbatimDisk(letter) => PathBuf::from(format!(r"\\?\{}:\", char::from(letter))),
        Prefix::UNC(server, share) | Prefix::VerbatimUNC(server, share) => {
            let mut anchor = PathBuf::from(r"\\?\UNC");
            anchor.push(server);
            anchor.push(share);
            anchor
        }
        Prefix::Verbatim(_) | Prefix::DeviceNS(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Windows Store root uses an unsupported device or verbatim prefix",
            ));
        }
    };

    let mut names = Vec::new();
    for component in components {
        match component {
            Component::Normal(name) => names.push(name.to_os_string()),
            Component::CurDir => {}
            Component::ParentDir | Component::Prefix(_) | Component::RootDir => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Windows Store root must not contain parent or repeated root components",
                ));
            }
        }
    }
    Ok((anchor, names))
}

fn open_anchor(path: &Path) -> io::Result<File> {
    let wide = wide_string(path.as_os_str())?;
    // SAFETY: wide is nul terminated and lives through the call; null security/template
    // pointers are permitted. On success the returned owned handle is transferred once.
    let handle = unsafe {
        CreateFileW(
            wide.as_ptr(),
            DIRECTORY_ACCESS,
            SHARE_ALL,
            ptr::null(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
            ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: handle is newly returned and uniquely transferred to File.
    let file = unsafe { File::from_raw_handle(handle) };
    reject_reparse_point(&file)?;
    verify_root_directory(&file)?;
    Ok(file)
}

fn open_relative(
    root: &File,
    name: &OsStr,
    desired_access: u32,
    disposition: u32,
    attributes: u32,
    options: u32,
) -> io::Result<Option<File>> {
    let mut name = name.encode_wide().collect::<Vec<_>>();
    let byte_length = name
        .len()
        .checked_mul(2)
        .and_then(|length| u16::try_from(length).ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "relative Store name is too long"))?;
    let unicode_name = UNICODE_STRING {
        Length: byte_length,
        MaximumLength: byte_length,
        Buffer: name.as_mut_ptr(),
    };
    let object_attributes = OBJECT_ATTRIBUTES {
        Length: std::mem::size_of::<OBJECT_ATTRIBUTES>() as u32,
        RootDirectory: root.as_raw_handle() as HANDLE,
        ObjectName: &unicode_name,
        Attributes: OBJ_CASE_INSENSITIVE,
        SecurityDescriptor: ptr::null(),
        SecurityQualityOfService: ptr::null(),
    };
    let mut io_status = IO_STATUS_BLOCK::default();
    let mut handle: HANDLE = ptr::null_mut();
    // SAFETY: root remains live; object_attributes and unicode_name borrow buffers that remain
    // valid for the call; output pointers reference initialized writable storage. NtCreateFile
    // returns a new owned handle only on success.
    let status = unsafe {
        NtCreateFile(
            &mut handle,
            desired_access,
            &object_attributes,
            &mut io_status,
            ptr::null(),
            attributes,
            SHARE_ALL,
            disposition,
            options,
            ptr::null(),
            0,
        )
    };
    if status == STATUS_OBJECT_NAME_NOT_FOUND {
        return Ok(None);
    }
    if status < 0 {
        // SAFETY: RtlNtStatusToDosError accepts every NTSTATUS and has no pointer arguments.
        let windows_error = unsafe { RtlNtStatusToDosError(status) };
        return Err(io::Error::from_raw_os_error(windows_error as i32));
    }
    // SAFETY: successful NtCreateFile returned a new owned handle transferred exactly once.
    Ok(Some(unsafe { File::from_raw_handle(handle) }))
}

fn reject_reparse_point(file: &File) -> io::Result<()> {
    if file.metadata()?.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Store path component is a reparse point",
        ));
    }
    Ok(())
}

fn wide_string(value: &OsStr) -> io::Result<Vec<u16>> {
    let mut wide = value.encode_wide().collect::<Vec<_>>();
    if wide.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Windows Store path contains an embedded nul",
        ));
    }
    wide.push(0);
    Ok(wide)
}
