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

use std::fs;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

use rocketmq_store_local::mapped_file::file::classify_file_preallocate_result;
use rocketmq_store_local::mapped_file::file::FilePreallocateOutcome;
use rocketmq_store_local::mapped_file::file::MappedFileStorage;
use rocketmq_store_local::mapped_file::file::PREALLOCATE_UNSUPPORTED_ERRNO;

#[test]
fn numeric_segment_name_opens_with_canonical_path_offset_and_size() {
    let directory = tempfile::tempdir().expect("create temporary directory");
    let path = directory.path().join("00000000000000000123");

    let (storage, preallocation) = MappedFileStorage::open(path.clone(), 32).expect("open storage");

    assert_eq!(storage.path(), path);
    assert_eq!(storage.file_from_offset(), 123);
    assert_eq!(storage.file().metadata().expect("file metadata").len(), 32);
    assert!(preallocation.is_some());
}

#[test]
fn invalid_segment_names_keep_legacy_error_text() {
    let directory = tempfile::tempdir().expect("create temporary directory");
    let path = directory.path().join("not-an-offset");

    let error = MappedFileStorage::open(path.clone(), 16).expect_err("invalid segment name must fail");

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(
        error.to_string(),
        format!("file name parse to offset is invalid: {}", path.display())
    );
}

#[test]
fn reopen_resize_preserves_prefix_and_skips_preallocation_at_existing_size() {
    let directory = tempfile::tempdir().expect("create temporary directory");
    let path = directory.path().join("7");
    fs::write(&path, b"prefix").expect("seed file");

    let (storage, grown) = MappedFileStorage::open(path, 12).expect("grow storage");
    assert!(grown.is_some());
    storage.file().seek(SeekFrom::Start(0)).expect("seek file");
    let mut prefix = [0_u8; 6];
    storage.file().read_exact(&mut prefix).expect("read prefix");
    assert_eq!(&prefix, b"prefix");

    let path = storage.path().to_path_buf();
    drop(storage);
    let (storage, unchanged) = MappedFileStorage::open(path, 12).expect("reopen storage");
    assert_eq!(storage.file().metadata().expect("file metadata").len(), 12);
    assert_eq!(unchanged, None);
}

#[test]
fn shrink_truncates_tail_without_preallocation() {
    let directory = tempfile::tempdir().expect("create temporary directory");
    let path = directory.path().join("8");
    fs::write(&path, b"prefix-tail").expect("seed file");

    let (storage, preallocation) = MappedFileStorage::open(path, 6).expect("shrink storage");

    assert_eq!(preallocation, None);
    assert_eq!(storage.file().metadata().expect("file metadata").len(), 6);
    storage.file().seek(SeekFrom::Start(0)).expect("seek file");
    let mut bytes = Vec::new();
    storage.file().read_to_end(&mut bytes).expect("read file");
    assert_eq!(bytes, b"prefix");
}

#[test]
fn zero_length_file_skips_preallocation() {
    let directory = tempfile::tempdir().expect("create temporary directory");
    let path = directory.path().join("9");

    let (storage, preallocation) = MappedFileStorage::open(path, 0).expect("open zero-length storage");

    assert_eq!(storage.file().metadata().expect("file metadata").len(), 0);
    assert_eq!(preallocation, None);
}

#[test]
fn preallocation_results_keep_legacy_classification() {
    assert_eq!(
        classify_file_preallocate_result(0, 123),
        FilePreallocateOutcome::Allocated
    );
    assert_eq!(
        classify_file_preallocate_result(-1, PREALLOCATE_UNSUPPORTED_ERRNO),
        FilePreallocateOutcome::Unsupported {
            errno: PREALLOCATE_UNSUPPORTED_ERRNO,
        }
    );
    assert_eq!(
        classify_file_preallocate_result(-1, 28),
        FilePreallocateOutcome::Failed { errno: 28 }
    );
}

#[test]
fn failed_rename_keeps_canonical_path_and_open_handle() {
    let directory = tempfile::tempdir().expect("create temporary directory");
    let original = directory.path().join("10");
    let missing_parent = directory.path().join("missing").join("11");
    let (mut storage, _) = MappedFileStorage::open(original.clone(), 4).expect("open storage");

    assert!(storage.rename(&missing_parent).is_err());
    assert_eq!(storage.path(), original);
    assert!(storage.file().metadata().is_ok());
}

#[test]
fn successful_rename_updates_path_before_reopen_failure() {
    let directory = tempfile::tempdir().expect("create temporary directory");
    let original = directory.path().join("12");
    let renamed = directory.path().join("13");
    let (mut storage, _) = MappedFileStorage::open(original.clone(), 4).expect("open storage");

    storage.rename(&renamed).expect("rename storage");
    assert_eq!(storage.path(), renamed);
    assert!(!original.exists());
    fs::remove_file(&renamed).expect("remove renamed path before reopen");

    assert!(storage.reopen().is_err());
    assert_eq!(storage.path(), renamed);
    assert!(storage.file().metadata().is_ok(), "old handle must remain open");
}

#[test]
fn delete_reports_success_then_missing_file_failure() {
    let directory = tempfile::tempdir().expect("create temporary directory");
    let path = directory.path().join("14");
    let (storage, _) = MappedFileStorage::open(path.clone(), 4).expect("open storage");

    storage.delete().expect("delete storage path");
    assert!(!path.exists());
    assert!(storage.delete().is_err());
}

#[test]
fn file_handle_remains_usable_for_store_owned_mmap_and_io() {
    let directory = tempfile::tempdir().expect("create temporary directory");
    let path = directory.path().join("15");
    let (storage, _) = MappedFileStorage::open(path, 8).expect("open storage");

    storage.file().write_all(b"bytes").expect("write through file handle");
    storage.file().seek(SeekFrom::Start(0)).expect("seek file");
    let mut bytes = [0_u8; 5];
    storage.file().read_exact(&mut bytes).expect("read through file handle");
    assert_eq!(&bytes, b"bytes");
}
