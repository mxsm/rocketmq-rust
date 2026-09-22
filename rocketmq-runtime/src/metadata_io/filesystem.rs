// Copyright 2023 The RocketMQ Rust Authors
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

//! Local atomic replacement and durability operations.

use super::metadata_io_failure;
use super::MetadataIoOperation;
use crate::RuntimeResult;
#[cfg(unix)]
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
#[cfg(windows)]
use std::iter;
#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;
use std::path::Path;
use uuid::Uuid;

pub(super) fn persist_atomic_local(target: &Path, bytes: &[u8]) -> RuntimeResult<()> {
    let parent = target
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)
        .map_err(|source| metadata_io_failure(MetadataIoOperation::CreateParent, parent, source))?;

    let file_name = target.file_name().and_then(|name| name.to_str()).unwrap_or("metadata");
    let temporary = parent.join(format!(".{file_name}.{}.tmp", Uuid::new_v4()));
    let result = persist_temporary_and_replace(&temporary, target, parent, bytes);
    if result.is_err() {
        if let Err(source) = std::fs::remove_file(&temporary) {
            if source.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(
                    path = %temporary.display(),
                    error = %source,
                    "failed to remove metadata temporary file after persistence failure"
                );
            }
        }
    }
    result
}

fn persist_temporary_and_replace(temporary: &Path, target: &Path, parent: &Path, bytes: &[u8]) -> RuntimeResult<()> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(temporary)
        .map_err(|source| metadata_io_failure(MetadataIoOperation::CreateTemporary, temporary, source))?;
    file.write_all(bytes)
        .map_err(|source| metadata_io_failure(MetadataIoOperation::WriteTemporary, temporary, source))?;
    file.sync_all()
        .map_err(|source| metadata_io_failure(MetadataIoOperation::SyncTemporary, temporary, source))?;
    drop(file);

    replace_file(temporary, target)
        .map_err(|source| metadata_io_failure(MetadataIoOperation::ReplaceTarget, target, source))?;
    sync_directory(parent).map_err(|source| metadata_io_failure(MetadataIoOperation::SyncParent, parent, source))
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> std::io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(windows)]
fn sync_directory(path: &Path) -> std::io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt;

    let directory = OpenOptions::new()
        .read(true)
        .write(true)
        .share_mode(
            windows_sys::Win32::Storage::FileSystem::FILE_SHARE_READ
                | windows_sys::Win32::Storage::FileSystem::FILE_SHARE_WRITE
                | windows_sys::Win32::Storage::FileSystem::FILE_SHARE_DELETE,
        )
        .custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)?;
    directory.sync_all()
}

#[cfg(not(any(unix, windows)))]
fn sync_directory(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(not(windows))]
fn replace_file(source: &Path, destination: &Path) -> std::io::Result<()> {
    std::fs::rename(source, destination)
}

#[cfg(windows)]
fn replace_file(source: &Path, destination: &Path) -> std::io::Result<()> {
    if !destination.exists() {
        let source = wide_path(source);
        let destination = wide_path(destination);
        // SAFETY: Both UTF-16 buffers are NUL-terminated and remain alive for the call.
        let moved = unsafe {
            windows_sys::Win32::Storage::FileSystem::MoveFileExW(
                source.as_ptr(),
                destination.as_ptr(),
                windows_sys::Win32::Storage::FileSystem::MOVEFILE_WRITE_THROUGH,
            )
        };
        return if moved == 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        };
    }

    let source = wide_path(source);
    let destination = wide_path(destination);
    // SAFETY: Both UTF-16 buffers are NUL-terminated and remain alive for the call; optional
    // backup and reserved pointers are null as allowed by ReplaceFileW.
    let replaced = unsafe {
        windows_sys::Win32::Storage::FileSystem::ReplaceFileW(
            destination.as_ptr(),
            source.as_ptr(),
            std::ptr::null(),
            windows_sys::Win32::Storage::FileSystem::REPLACEFILE_WRITE_THROUGH,
            std::ptr::null(),
            std::ptr::null(),
        )
    };
    if replaced == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(windows)]
fn wide_path(path: &Path) -> Vec<u16> {
    path.as_os_str().encode_wide().chain(iter::once(0)).collect()
}
