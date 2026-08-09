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
use std::io;

use crate::mapped_file::retirement::identity::PhysicalFileKey;

pub(super) fn capture(file: &File) -> io::Result<PhysicalFileKey> {
    capture_impl(file)
}

#[cfg(unix)]
fn capture_impl(file: &File) -> io::Result<PhysicalFileKey> {
    use std::os::unix::fs::MetadataExt;

    let metadata = file.metadata()?;
    Ok(PhysicalFileKey::unix(metadata.dev(), metadata.ino()))
}

#[cfg(windows)]
fn capture_impl(file: &File) -> io::Result<PhysicalFileKey> {
    use std::mem::size_of;
    use std::mem::MaybeUninit;
    use std::os::windows::io::AsRawHandle;

    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::Foundation::WIN32_ERROR;
    use windows::Win32::Storage::FileSystem::FileIdInfo;
    use windows::Win32::Storage::FileSystem::GetFileInformationByHandleEx;
    use windows::Win32::Storage::FileSystem::FILE_ID_INFO;

    let mut info = MaybeUninit::<FILE_ID_INFO>::uninit();
    let handle = HANDLE(file.as_raw_handle());
    // SAFETY: `handle` remains borrowed and valid for this call. `info` is aligned writable storage
    // of exactly the requested fixed information structure and is initialized completely on success.
    unsafe {
        GetFileInformationByHandleEx(
            handle,
            FileIdInfo,
            info.as_mut_ptr().cast(),
            size_of::<FILE_ID_INFO>() as u32,
        )
        .map_err(|error| {
            WIN32_ERROR::from_error(&error)
                .map(|code| io::Error::from_raw_os_error(code.0 as i32))
                .unwrap_or_else(|| io::Error::other(error))
        })?;
        let info = info.assume_init();
        Ok(PhysicalFileKey::windows(
            info.VolumeSerialNumber,
            info.FileId.Identifier,
        ))
    }
}

#[cfg(not(any(unix, windows)))]
fn capture_impl(_file: &File) -> io::Result<PhysicalFileKey> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "physical file identity is unsupported on this platform",
    ))
}
