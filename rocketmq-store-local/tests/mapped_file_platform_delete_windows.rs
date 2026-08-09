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

#![cfg(windows)]

use std::fs::OpenOptions;
use std::os::windows::fs::OpenOptionsExt;

use cheetah_string::CheetahString;
use rocketmq_store_local::mapped_file::DefaultMappedFile;
use rocketmq_store_local::mapped_file::MappedFile;
use rocketmq_store_local::mapped_file::MappedFileDestroyOutcome;
use windows::Win32::Storage::FileSystem::FILE_SHARE_READ;
use windows::Win32::Storage::FileSystem::FILE_SHARE_WRITE;

#[test]
fn sharing_violation_retains_the_namespace_until_an_explicit_retry() {
    let root = tempfile::tempdir().expect("temporary segment directory");
    let path = root.path().join("00000000000000000000");
    let mapped: DefaultMappedFile =
        DefaultMappedFile::try_new(CheetahString::from_string(path.to_string_lossy().into_owned()), 64)
            .expect("create mapped file");
    assert!(mapped.append_message_bytes(b"windows-retirement"));
    let blocker = OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0)
        .open(&path)
        .expect("open an external handle without delete sharing");

    MappedFile::shutdown(&mapped, 0);
    assert!(matches!(
        mapped.try_destroy(0),
        MappedFileDestroyOutcome::DeleteFailed { .. }
    ));
    assert!(path.exists());

    drop(blocker);
    assert_eq!(mapped.try_destroy(0), MappedFileDestroyOutcome::NamespaceRemoved);
    assert!(!path.exists());
}

#[test]
fn managed_windows_writer_uses_only_verified_handle_relative_ntfs_primitives() {
    let source = include_str!("../src/mapped_file/retirement/platform/windows.rs").replace("\r\n", "\n");
    assert!(source.contains("NtSetInformationFile"));
    assert!(source.contains("GetVolumeInformationByHandleW"));
    assert!(source.contains("eq_ignore_ascii_case(\"NTFS\")"));
    assert!(!source.contains("writer_qualified: false"));
    assert!(!source.contains("MoveFileExW"));
}
