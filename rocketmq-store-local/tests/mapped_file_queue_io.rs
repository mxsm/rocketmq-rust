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

use std::fs::File;
use std::path::Path;

use rocketmq_store_local::mapped_file::queue_io::create_mapped_file_for_queue;
use rocketmq_store_local::mapped_file::queue_io::load_mapped_file_queue_files;
use rocketmq_store_local::mapped_file::queue_io::load_mapped_file_queue_path;
use rocketmq_store_local::mapped_file::MappedFile;
use tempfile::tempdir;

fn create_sized_file(path: &Path, size: u64) {
    File::create(path)
        .expect("create fixture")
        .set_len(size)
        .expect("size fixture");
}

#[test]
fn missing_queue_directory_preserves_legacy_success() {
    let temp_dir = tempdir().expect("temp dir");
    let missing = temp_dir.path().join("missing");

    let outcome = load_mapped_file_queue_path(missing.to_string_lossy().as_ref(), 16);

    assert!(outcome.is_success());
    assert!(outcome.into_mapped_files().is_empty());
}

#[test]
fn queue_load_sorts_files_initializes_positions_and_removes_empty_tail() {
    let temp_dir = tempdir().expect("temp dir");
    let first = temp_dir.path().join("00000000000000000000");
    let second = temp_dir.path().join("00000000000000000016");
    let empty_tail = temp_dir.path().join("00000000000000000032");
    create_sized_file(&second, 16);
    create_sized_file(&first, 16);
    create_sized_file(&empty_tail, 0);

    let outcome = load_mapped_file_queue_path(temp_dir.path().to_string_lossy().as_ref(), 16);
    assert!(outcome.is_success());
    let files = outcome.into_mapped_files();
    assert_eq!(files.len(), 2);
    assert_eq!(files[0].get_file_from_offset(), 0);
    assert_eq!(files[1].get_file_from_offset(), 16);
    for file in &files {
        assert_eq!(file.get_wrote_position(), 16);
        assert_eq!(file.get_flushed_position(), 16);
        assert_eq!(file.get_committed_position(), 16);
    }
    assert!(!empty_tail.exists());
}

#[test]
fn queue_load_returns_files_loaded_before_a_size_failure() {
    let temp_dir = tempdir().expect("temp dir");
    let valid = temp_dir.path().join("00000000000000000000");
    let invalid = temp_dir.path().join("00000000000000000016");
    create_sized_file(&valid, 16);
    create_sized_file(&invalid, 8);

    let outcome = load_mapped_file_queue_files(vec![invalid, valid], 16);

    assert!(!outcome.is_success());
    let files = outcome.into_mapped_files();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].get_file_from_offset(), 0);
}

#[test]
fn queue_creation_uses_local_sync_fallback_and_marks_the_first_file() {
    let temp_dir = tempdir().expect("temp dir");
    let file_path = temp_dir.path().join("00000000000000000000");
    let next_file_path = temp_dir.path().join("00000000000000000016");

    let mapped_file =
        create_mapped_file_for_queue(None, &file_path, &next_file_path, 16, true).expect("create queue mapped file");

    assert!(file_path.exists());
    assert_eq!(mapped_file.get_file_size(), 16);
    assert!(mapped_file.is_first_create_in_queue());
}
