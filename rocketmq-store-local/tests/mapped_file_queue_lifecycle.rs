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

use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_store_local::mapped_file::queue_lifecycle::delete_expired_mapped_files_by_offset;
use rocketmq_store_local::mapped_file::queue_lifecycle::delete_expired_mapped_files_by_time;
use rocketmq_store_local::mapped_file::queue_lifecycle::delete_expired_mapped_files_by_time_before;
use rocketmq_store_local::mapped_file::queue_lifecycle::destroy_last_mapped_file;
use rocketmq_store_local::mapped_file::queue_lifecycle::destroy_mapped_file_queue;
use rocketmq_store_local::mapped_file::queue_lifecycle::mapped_files_after_removal;
use rocketmq_store_local::mapped_file::queue_lifecycle::retry_delete_first_mapped_file;
use rocketmq_store_local::mapped_file::queue_lifecycle::shutdown_mapped_file_queue;
use rocketmq_store_local::mapped_file::queue_lifecycle::swap_mapped_file_queue;
use rocketmq_store_local::mapped_file::DefaultMappedFile;
use rocketmq_store_local::mapped_file::MappedFile;
use tempfile::TempDir;

fn mapped_file(temp_dir: &TempDir, offset: u64, size: u64) -> Arc<DefaultMappedFile> {
    let path = temp_dir.path().join(format!("{offset:020}"));
    Arc::new(
        DefaultMappedFile::try_new(CheetahString::from_string(path.to_string_lossy().into_owned()), size)
            .expect("mapped file"),
    )
}

#[test]
fn removal_filters_only_candidates_present_in_the_current_snapshot() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let first = mapped_file(&temp_dir, 0, 16);
    let second = mapped_file(&temp_dir, 16, 16);
    let absent = mapped_file(&temp_dir, 32, 16);

    let retained = mapped_files_after_removal(&[first.clone(), second.clone()], &[first, absent]);

    assert_eq!(retained.len(), 1);
    assert!(Arc::ptr_eq(&retained[0], &second));
}

#[test]
fn destroy_last_returns_the_destroyed_newest_file() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let first = mapped_file(&temp_dir, 0, 16);
    let second = mapped_file(&temp_dir, 16, 16);

    let destroyed = destroy_last_mapped_file(&[first, second.clone()]).expect("last file");

    assert!(Arc::ptr_eq(&destroyed, &second));
    assert!(!destroyed.is_available());
}

#[test]
fn time_deletion_keeps_the_newest_file_and_honors_the_batch_limit() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let files = vec![
        mapped_file(&temp_dir, 0, 16),
        mapped_file(&temp_dir, 16, 16),
        mapped_file(&temp_dir, 32, 16),
    ];

    let deletion = delete_expired_mapped_files_by_time(&files, 0, 0, 1000, true, 1, || 0);

    assert_eq!(deletion.deleted_count(), 1);
    let deleted = deletion.into_mapped_files();
    assert!(Arc::ptr_eq(&deleted[0], &files[0]));
    assert!(files[2].is_available());
}

#[test]
fn time_deletion_stops_before_the_pinned_wal_segment() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let files = vec![
        mapped_file(&temp_dir, 0, 16),
        mapped_file(&temp_dir, 16, 16),
        mapped_file(&temp_dir, 32, 16),
        mapped_file(&temp_dir, 48, 16),
    ];

    let deletion = delete_expired_mapped_files_by_time_before(&files, 0, 0, 1000, true, 10, Some(16), || 0);

    assert_eq!(deletion.deleted_count(), 1);
    assert!(!files[0].is_available());
    assert!(files[1..].iter().all(|file| file.is_available()));
}

#[test]
fn offset_deletion_stops_when_the_selected_file_cannot_finish_destroy() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let files = vec![
        mapped_file(&temp_dir, 0, 20),
        mapped_file(&temp_dir, 20, 20),
        mapped_file(&temp_dir, 40, 20),
    ];
    let mut first_unit = vec![0; 20];
    first_unit[0..8].copy_from_slice(&5_i64.to_be_bytes());
    let mut second_unit = vec![0; 20];
    second_unit[0..8].copy_from_slice(&15_i64.to_be_bytes());
    assert!(files[0].append_message_bytes(&first_unit));
    assert!(files[1].append_message_bytes(&second_unit));

    let deletion = delete_expired_mapped_files_by_offset(&files, 20, 10, 20);

    assert_eq!(deletion.deleted_count(), 0);
    assert!(deletion.into_mapped_files().is_empty());
    assert!(!files[0].is_available());
    assert!(files[1].is_available());
    assert!(files[2].is_available());
}

#[test]
fn swap_reserves_three_newest_files_and_shutdown_releases_every_file() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let files: Vec<_> = (0..5).map(|index| mapped_file(&temp_dir, index * 16, 16)).collect();

    swap_mapped_file_queue(&files, 1, 0, 0, || i64::MAX);
    assert_eq!(files[0].get_metrics().expect("metrics").swap_operations(), 1);
    assert_eq!(files[1].get_metrics().expect("metrics").swap_operations(), 1);
    assert!(files[2..]
        .iter()
        .all(|file| file.get_metrics().expect("metrics").swap_operations() == 0));
    assert_eq!(retry_delete_first_mapped_file(files.first(), 1000).deleted_count(), 0);

    shutdown_mapped_file_queue(&files, 0);
    assert!(files.iter().all(|file| !file.is_available()));
}

#[test]
fn destroy_removes_every_file_and_the_queue_directory() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let path = temp_dir.path().to_path_buf();
    let files = vec![mapped_file(&temp_dir, 0, 16), mapped_file(&temp_dir, 16, 16)];

    destroy_mapped_file_queue(&files, path.to_string_lossy().as_ref());

    assert!(!path.exists());
    assert!(files.iter().all(|file| !file.is_available()));
}
