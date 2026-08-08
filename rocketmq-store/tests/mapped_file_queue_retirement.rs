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

use rocketmq_store::test_support::MappedFileQueueTestHarness;
use rocketmq_store_local::mapped_file::MappedFile;

fn queue(temp_dir: &tempfile::TempDir) -> MappedFileQueueTestHarness {
    MappedFileQueueTestHarness::new(temp_dir.path().to_string_lossy().into_owned(), 1024, None)
}

fn create_three_files(
    queue: &MappedFileQueueTestHarness,
) -> Vec<std::sync::Arc<rocketmq_store_local::mapped_file::DefaultMappedFile>> {
    (0..3)
        .map(|index| {
            let file = queue.try_create_mapped_file(index * 1024).expect("create mapped file");
            file.set_wrote_position(1024);
            file.set_committed_position(1024);
            file.set_flushed_position(1024);
            file
        })
        .collect()
}

#[test]
fn truncate_keeps_a_live_tail_file_until_destroy_retry_succeeds() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let mut queue = queue(&temp_dir);
    let files = create_three_files(&queue);
    assert!(files[2].hold());

    assert!(!queue.try_truncate_dirty_files(1536));

    assert_eq!(queue.get_mapped_file_count(), 3);
    assert!(!files[2].is_available());
    assert!(!files[2].append_message_bytes(b"must-not-publish"));
    assert!(queue.get_last_mapped_file_mut_start_offset(3072, true).is_none());
    assert_eq!(queue.get_mapped_file_count(), 3);

    files[2].release();
    assert!(queue.try_truncate_dirty_files(1536));
    assert_eq!(queue.get_mapped_file_count(), 2);
    assert!(!temp_dir.path().join(format!("{:020}", 2048)).exists());
    queue.destroy();
}

#[test]
fn reset_returns_false_and_keeps_a_live_tail_file_for_retry() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let mut queue = queue(&temp_dir);
    let files = create_three_files(&queue);
    assert!(files[2].hold());

    assert!(!queue.reset_offset(1536));
    assert_eq!(queue.get_mapped_file_count(), 3);
    assert_eq!(files[1].get_wrote_position(), 1024);

    files[2].release();
    assert!(queue.reset_offset(1536));
    assert_eq!(queue.get_mapped_file_count(), 2);
    assert_eq!(files[1].get_wrote_position(), 512);
    assert!(!files[2].is_available());
    queue.destroy();
}

#[test]
fn delete_last_keeps_a_live_file_in_the_generation() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let mut queue = queue(&temp_dir);
    let first = queue.try_create_mapped_file(0).expect("first file");
    let last = queue.try_create_mapped_file(1024).expect("last file");
    assert!(last.hold());

    assert!(!queue.try_delete_last_mapped_file());
    assert_eq!(queue.get_mapped_file_count(), 2);

    last.release();
    assert!(queue.try_delete_last_mapped_file());
    assert_eq!(queue.get_mapped_file_count(), 1);
    assert!(std::sync::Arc::ptr_eq(
        &queue.get_first_mapped_file().expect("first remains"),
        &first
    ));
    queue.destroy();
}

#[test]
fn whole_destroy_keeps_the_generation_when_the_oldest_file_is_live() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let mut queue = queue(&temp_dir);
    let first = queue.try_create_mapped_file(0).expect("first file");
    let second = queue.try_create_mapped_file(1024).expect("second file");
    assert!(first.hold());

    assert!(!queue.destroy_with_outcome());
    assert_eq!(queue.get_mapped_file_count(), 2);
    assert!(!second.is_available());
    assert!(queue.get_last_mapped_file_mut_start_offset(2048, true).is_none());
    assert!(temp_dir.path().exists());

    first.release();
    assert!(queue.destroy_with_outcome());
    assert_eq!(queue.get_mapped_file_count(), 0);
}

#[test]
fn whole_destroy_does_not_remove_unknown_directory_entries() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let mut queue = queue(&temp_dir);
    queue.try_create_mapped_file(0).expect("mapped file");
    let sentinel = temp_dir.path().join("unknown-entry");
    std::fs::write(&sentinel, b"preserve").expect("sentinel");

    assert!(!queue.destroy_with_outcome());

    assert_eq!(queue.get_mapped_file_count(), 0);
    assert_eq!(std::fs::read(&sentinel).expect("sentinel remains"), b"preserve");
}
