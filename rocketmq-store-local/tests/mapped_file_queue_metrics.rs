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
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_store_local::mapped_file::queue_metrics::mapped_file_queue_available_memory_size;
use rocketmq_store_local::mapped_file::queue_metrics::mapped_file_queue_fall_behind;
use rocketmq_store_local::mapped_file::queue_metrics::mapped_file_queue_lazy_mmap_stats;
use rocketmq_store_local::mapped_file::queue_metrics::mapped_file_queue_max_offset;
use rocketmq_store_local::mapped_file::queue_metrics::mapped_file_queue_max_wrote_position;
use rocketmq_store_local::mapped_file::queue_metrics::mapped_file_queue_min_offset;
use rocketmq_store_local::mapped_file::queue_metrics::mapped_file_queue_should_roll;
use rocketmq_store_local::mapped_file::queue_metrics::mapped_file_queue_total_size;
use rocketmq_store_local::mapped_file::queue_metrics::mapped_file_queue_warmup_stats;
use rocketmq_store_local::mapped_file::DefaultMappedFile;
use rocketmq_store_local::mapped_file::LazyMmapStats;
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
fn empty_queue_queries_preserve_legacy_sentinels() {
    assert_eq!(mapped_file_queue_max_offset(None), 0);
    assert_eq!(mapped_file_queue_max_wrote_position(None), 0);
    assert_eq!(mapped_file_queue_min_offset(None), -1);
    assert!(mapped_file_queue_should_roll(None, 1));
    assert_eq!(mapped_file_queue_fall_behind(None, 10), 0);
    assert_eq!(mapped_file_queue_total_size(0, 1024), 0);
}

#[test]
fn queue_position_and_roll_queries_use_the_last_file_snapshot() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let file = mapped_file(&temp_dir, 100, 100);
    assert!(file.append_message_bytes(&[0; 30]));

    assert_eq!(mapped_file_queue_max_offset(Some(&file)), 130);
    assert_eq!(mapped_file_queue_max_wrote_position(Some(&file)), 130);
    assert_eq!(mapped_file_queue_min_offset(Some(&file)), 100);
    assert!(!mapped_file_queue_should_roll(Some(&file), 70));
    assert!(mapped_file_queue_should_roll(Some(&file), 71));
    assert_eq!(mapped_file_queue_fall_behind(Some(&file), 110), 20);
    assert_eq!(mapped_file_queue_fall_behind(Some(&file), 0), 0);
}

#[test]
fn available_memory_and_total_size_keep_distinct_accounting() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let first = mapped_file(&temp_dir, 0, 16);
    let second = mapped_file(&temp_dir, 16, 16);
    assert!(first.destroy(0));

    assert_eq!(mapped_file_queue_available_memory_size(&[first, second], 16), 16);
    assert_eq!(mapped_file_queue_total_size(2, 16), 32);
}

#[test]
fn warmup_metrics_aggregate_with_saturating_totals_and_last_file_latency() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let first = mapped_file(&temp_dir, 0, 16);
    let second = mapped_file(&temp_dir, 16, 16);
    first
        .get_metrics()
        .expect("metrics")
        .record_warm_with_latency(16, Duration::from_millis(3));
    second
        .get_metrics()
        .expect("metrics")
        .record_warm_with_latency(32, Duration::from_millis(5));

    let stats = mapped_file_queue_warmup_stats(&[first, second]);

    assert_eq!(stats.operations, 2);
    assert_eq!(stats.bytes, 48);
    assert_eq!(stats.total_millis, 8);
    assert_eq!(stats.last_millis, 5);
}

#[test]
fn lazy_mmap_metrics_aggregate_mapped_and_unmapped_files() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let first_path = temp_dir.path().join("00000000000000000000");
    let second_path = temp_dir.path().join("00000000000000000016");
    File::create(&first_path)
        .expect("first file")
        .set_len(16)
        .expect("first size");
    File::create(&second_path)
        .expect("second file")
        .set_len(16)
        .expect("second size");
    let first = Arc::new(
        DefaultMappedFile::try_new_lazy_read_only(
            CheetahString::from_string(first_path.to_string_lossy().into_owned()),
            16,
        )
        .expect("first lazy file"),
    );
    let second = Arc::new(
        DefaultMappedFile::try_new_lazy_read_only(
            CheetahString::from_string(second_path.to_string_lossy().into_owned()),
            16,
        )
        .expect("second lazy file"),
    );
    assert_eq!(first.get_mapped_byte_buffer().len(), 16);

    let stats = mapped_file_queue_lazy_mmap_stats(&[first, second]);

    assert_eq!(
        stats,
        LazyMmapStats {
            eligible_files: 2,
            mapped_files: 1,
            map_operations: 1,
            map_failures: 0,
            total_millis: stats.total_millis,
            last_millis: stats.last_millis,
        }
    );
}
