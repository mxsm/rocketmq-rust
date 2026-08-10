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
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use cheetah_string::CheetahString;
use rocketmq_store_local::mapped_file::queue_lifecycle::clean_swapped_mapped_file_queue;
use rocketmq_store_local::mapped_file::queue_lifecycle::delete_expired_mapped_files_by_offset;
use rocketmq_store_local::mapped_file::queue_lifecycle::delete_expired_mapped_files_by_time;
use rocketmq_store_local::mapped_file::queue_lifecycle::delete_expired_mapped_files_by_time_before;
use rocketmq_store_local::mapped_file::queue_lifecycle::destroy_last_mapped_file;
use rocketmq_store_local::mapped_file::queue_lifecycle::destroy_mapped_file_queue;
use rocketmq_store_local::mapped_file::queue_lifecycle::is_expired;
use rocketmq_store_local::mapped_file::queue_lifecycle::mapped_files_after_removal;
use rocketmq_store_local::mapped_file::queue_lifecycle::retry_delete_first_mapped_file;
use rocketmq_store_local::mapped_file::queue_lifecycle::select_expired_mapped_files_by_offset;
use rocketmq_store_local::mapped_file::queue_lifecycle::select_expired_mapped_files_by_time_before;
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
fn destroy_last_keeps_a_live_newest_file_tracked_for_retry() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let first = mapped_file(&temp_dir, 0, 16);
    let second = mapped_file(&temp_dir, 16, 16);
    assert!(second.hold());

    assert!(destroy_last_mapped_file(&[first, second.clone()]).is_none());
    assert!(!second.is_available());

    second.release();
    let destroyed = destroy_last_mapped_file(std::slice::from_ref(&second)).expect("retry last file");
    assert!(Arc::ptr_eq(&destroyed, &second));
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
fn fresh_file_survives_a_nonzero_retention_period() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let files = vec![mapped_file(&temp_dir, 0, 16), mapped_file(&temp_dir, 16, 16)];
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_millis() as i64;

    let deletion = delete_expired_mapped_files_by_time(&files, 60_000, 0, 1000, false, 10, || now);

    assert_eq!(deletion.deleted_count(), 0);
    assert!(files.iter().all(|file| file.is_available()));
}

#[test]
fn expiration_uses_duration_and_treats_future_mtime_as_fresh() {
    let modified = UNIX_EPOCH + Duration::from_secs(100);

    assert!(is_expired(
        modified,
        UNIX_EPOCH + Duration::from_secs(160),
        Duration::from_secs(60)
    ));
    assert!(!is_expired(
        modified,
        UNIX_EPOCH + Duration::from_secs(99),
        Duration::ZERO
    ));
}

#[test]
fn negative_retention_never_deletes_a_file() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let files = vec![mapped_file(&temp_dir, 0, 16), mapped_file(&temp_dir, 16, 16)];

    let deletion = delete_expired_mapped_files_by_time(&files, -1, 0, 1000, false, 10, || i64::MAX);

    assert_eq!(deletion.deleted_count(), 0);
    assert!(files.iter().all(|file| file.is_available()));
}

#[test]
fn negative_delete_interval_never_deletes_a_file() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let files = vec![mapped_file(&temp_dir, 0, 16), mapped_file(&temp_dir, 16, 16)];

    let deletion = delete_expired_mapped_files_by_time(&files, 0, -1, 1000, true, 10, || i64::MAX);

    assert_eq!(deletion.deleted_count(), 0);
    assert!(files.iter().all(|file| file.is_available()));
}

#[test]
fn compatibility_last_modified_timestamp_is_unix_epoch_millis() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let before = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_millis() as u64;
    let file = mapped_file(&temp_dir, 0, 16);
    let after = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_millis() as u64;

    let modified = file.get_last_modified_timestamp();

    assert!(modified <= after);
    assert!(before.saturating_sub(modified) <= 5_000);
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
fn offset_deletion_releases_selection_before_destroy() {
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

    assert_eq!(deletion.deleted_count(), 1);
    let deleted_files = deletion.into_mapped_files();
    assert_eq!(deleted_files.len(), 1);
    assert!(Arc::ptr_eq(&deleted_files[0], &files[0]));
    assert!(!files[0].is_available());
    assert!(files[1].is_available());
    assert!(files[2].is_available());
}

#[test]
fn managed_cleanup_selection_is_non_mutating_and_rejects_invalid_unit_sizes() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let files = vec![mapped_file(&temp_dir, 0, 20), mapped_file(&temp_dir, 20, 20)];
    let mut unit = vec![0; 20];
    unit[0..8].copy_from_slice(&5_i64.to_be_bytes());
    assert!(files[0].append_message_bytes(&unit));

    let selected = select_expired_mapped_files_by_offset(&files, 20, 10, 20);
    assert_eq!(selected.len(), 1);
    assert!(Arc::ptr_eq(&selected[0], &files[0]));
    assert!(files.iter().all(|file| file.is_available()));

    for unit_size in [-1, 0, 21] {
        assert!(select_expired_mapped_files_by_offset(&files, 20, 10, unit_size).is_empty());
    }

    let selected = select_expired_mapped_files_by_time_before(&files, 0, true, 1, None, || 0);
    assert_eq!(selected.len(), 1);
    assert!(Arc::ptr_eq(&selected[0], &files[0]));
    assert!(files.iter().all(|file| file.is_available()));
}

#[test]
fn retry_deletion_succeeds_after_the_last_reader_releases_the_first_file() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let first = mapped_file(&temp_dir, 0, 16);

    assert!(first.hold());
    assert!(!first.destroy(1000));
    assert!(!first.is_available());

    first.release();
    let deletion = retry_delete_first_mapped_file(Some(&first), 0);

    assert_eq!(deletion.deleted_count(), 1);
    assert!(Arc::ptr_eq(&deletion.into_mapped_files()[0], &first));
}

#[test]
fn swap_reserves_three_newest_files_and_shutdown_releases_every_file() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let files: Vec<_> = (0..5).map(|index| mapped_file(&temp_dir, index * 16, 16)).collect();
    for file in &files {
        assert!(file.try_seal_readable().expect("queue segment seals"));
    }

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
fn clean_swapped_queue_only_removes_dropped_retired_observations() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let files: Vec<_> = (0..5).map(|index| mapped_file(&temp_dir, index * 16, 16)).collect();
    for file in &files {
        assert!(file.try_seal_readable().expect("queue segment seals"));
    }

    swap_mapped_file_queue(&files, 1, 0, 0, || i64::MAX);

    clean_swapped_mapped_file_queue(&files, 0, || i64::MAX);
    clean_swapped_mapped_file_queue(&files, 0, || i64::MAX);

    for file in &files[..2] {
        let metrics = file.get_metrics().expect("metrics");
        assert_eq!(metrics.clean_swap_operations(), 1);
        assert_eq!(metrics.swap_operations(), 1);
    }
    for file in &files[2..] {
        let metrics = file.get_metrics().expect("metrics");
        assert_eq!(metrics.clean_swap_operations(), 0);
        assert_eq!(metrics.swap_operations(), 0);
    }

    files[0].swap_map();
    clean_swapped_mapped_file_queue(&files, 0, || i64::MAX);
    assert_eq!(files[0].get_metrics().expect("metrics").clean_swap_operations(), 2);
    assert_eq!(files[1].get_metrics().expect("metrics").clean_swap_operations(), 1);
}

#[test]
fn negative_swap_intervals_do_not_trigger_maintenance() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let files: Vec<_> = (0..5).map(|index| mapped_file(&temp_dir, index * 16, 16)).collect();

    swap_mapped_file_queue(&files, 1, -1, -1, || i64::MAX);
    clean_swapped_mapped_file_queue(&files, -1, || i64::MAX);

    assert!(files.iter().all(|file| {
        let metrics = file.get_metrics().expect("metrics");
        metrics.swap_operations() == 0 && metrics.clean_swap_operations() == 0
    }));
}

#[test]
fn destroy_removes_every_file_and_the_queue_directory() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let path = temp_dir.path().to_path_buf();
    let files = vec![mapped_file(&temp_dir, 0, 16), mapped_file(&temp_dir, 16, 16)];

    let deletion = destroy_mapped_file_queue(&files, path.to_string_lossy().as_ref());

    assert_eq!(deletion.deleted_count(), 2);
    assert!(!path.exists());
    assert!(files.iter().all(|file| !file.is_available()));
}

#[test]
fn whole_destroy_stops_at_a_live_holder_and_keeps_retry_identity() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let path = temp_dir.path().to_path_buf();
    let files = vec![mapped_file(&temp_dir, 0, 16), mapped_file(&temp_dir, 16, 16)];
    assert!(files[0].hold());

    let first_attempt = destroy_mapped_file_queue(&files, path.to_string_lossy().as_ref());

    assert_eq!(first_attempt.deleted_count(), 0);
    assert!(path.exists());
    assert!(!files[1].is_available());

    files[0].release();
    let retry = destroy_mapped_file_queue(&files, path.to_string_lossy().as_ref());
    assert_eq!(retry.deleted_count(), 2);
    assert!(!path.exists());
}

#[test]
fn whole_destroy_preserves_unknown_directory_entries() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let path = temp_dir.path().to_path_buf();
    let sentinel = path.join("do-not-delete.txt");
    std::fs::write(&sentinel, b"untracked").expect("create sentinel");
    let files = vec![mapped_file(&temp_dir, 0, 16), mapped_file(&temp_dir, 16, 16)];

    let deletion = destroy_mapped_file_queue(&files, path.to_string_lossy().as_ref());

    assert_eq!(deletion.deleted_count(), 2);
    assert!(path.exists());
    assert_eq!(std::fs::read(&sentinel).expect("sentinel remains"), b"untracked");
}

#[test]
fn drained_shutdown_marks_logical_cleanup_and_detaches_physical_mapping() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let file = mapped_file(&temp_dir, 0, 16);
    let path = temp_dir.path().join("00000000000000000000");
    let metrics = file.get_metrics().expect("mapped-file metrics");
    assert!(file.is_mapped());
    assert_eq!(metrics.mapped_generations_live(), 1);
    assert_eq!(metrics.file_owners_live(), 1);

    file.shutdown(u64::MAX);

    assert!(rocketmq_store_local::mapped_file::kernel::ReferenceResource::is_logical_cleanup_marked(file.as_ref()));
    assert!(!file.is_mapped());
    assert!(path.exists(), "normal shutdown must preserve the namespace");
    assert_eq!(metrics.mapped_generations_live(), 0);
    assert_eq!(metrics.file_owners_live(), 0);
    assert_eq!(metrics.lifecycle_detach_total(), 1);
}
