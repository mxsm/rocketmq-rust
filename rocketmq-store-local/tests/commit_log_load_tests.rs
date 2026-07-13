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

use std::error::Error;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::time::Duration;

use memmap2::MmapMut;
use memmap2::MmapOptions;
use rocketmq_store_local::commit_log::load::apply_recovery_file_prefetch;
use rocketmq_store_local::commit_log::load::apply_recovery_mmap_advice;
use rocketmq_store_local::commit_log::load::collect_commit_log_metadata;
use rocketmq_store_local::commit_log::load::record_file_prefetch;
use rocketmq_store_local::commit_log::load::record_mmap_advice;
use rocketmq_store_local::commit_log::load::validate_commit_log_file;
use rocketmq_store_local::commit_log::load::CommitLogFileLoadDecision;
use rocketmq_store_local::commit_log::load::CommitLogFileMetadata;
use rocketmq_store_local::commit_log::load::CommitLogMappingExecution;
use rocketmq_store_local::commit_log::load::CommitLogMappingMode;
use rocketmq_store_local::commit_log::load::CommitLogMappingOptions;
use rocketmq_store_local::commit_log::load::CommitLogMappingPlan;
use rocketmq_store_local::commit_log::load::CommitLogMetadataCollectionOptions;
use rocketmq_store_local::commit_log::load::HintOutcome;
use rocketmq_store_local::commit_log::load::LoadStatistics;
use rocketmq_store_local::commit_log::load::RecoveryFilePrefetch;
use rocketmq_store_local::commit_log::load::RecoveryMmapAdvice;
use tempfile::NamedTempFile;
use tempfile::TempDir;

const EXPECTED_FILE_SIZE: u64 = 8;

fn mutable_mapping() -> (NamedTempFile, MmapMut) {
    let file = NamedTempFile::new().unwrap();
    file.as_file().set_len(4096).unwrap();
    // SAFETY: The temporary file remains alive for the returned mapping and is not
    // accessed through another mapping while this test owns it.
    let mmap = unsafe { MmapOptions::new().map_mut(file.as_file()).unwrap() };
    (file, mmap)
}

fn metadata(path: &str, size: u64) -> CommitLogFileMetadata {
    CommitLogFileMetadata {
        path: PathBuf::from(path),
        size,
    }
}

fn collection_options(parallel_enabled: bool) -> CommitLogMetadataCollectionOptions {
    CommitLogMetadataCollectionOptions {
        expected_file_size: EXPECTED_FILE_SIZE,
        parallel_enabled,
    }
}

fn create_commit_log_files(directory: &TempDir, count: usize, size: u64) -> Vec<PathBuf> {
    (0..count)
        .map(|index| {
            let path = directory.path().join(format!("{index:020}"));
            fs::write(&path, vec![index as u8; size as usize]).unwrap();
            path
        })
        .collect()
}

#[test]
fn metadata_collection_accepts_zero_paths() {
    let metadata = collect_commit_log_metadata(&[], collection_options(true)).unwrap();

    assert!(metadata.is_empty());
}

#[test]
fn metadata_collection_accepts_one_path() {
    let directory = TempDir::new().unwrap();
    let paths = create_commit_log_files(&directory, 1, EXPECTED_FILE_SIZE);

    let metadata = collect_commit_log_metadata(&paths, collection_options(true)).unwrap();

    assert_eq!(
        metadata,
        vec![CommitLogFileMetadata {
            path: paths[0].clone(),
            size: EXPECTED_FILE_SIZE,
        }]
    );
}

#[test]
fn metadata_collection_accepts_four_paths() {
    let directory = TempDir::new().unwrap();
    let paths = create_commit_log_files(&directory, 4, EXPECTED_FILE_SIZE);

    let metadata = collect_commit_log_metadata(&paths, collection_options(true)).unwrap();

    assert_eq!(
        metadata.iter().map(|item| &item.path).collect::<Vec<_>>(),
        paths.iter().collect::<Vec<_>>()
    );
}

#[test]
fn metadata_collection_accepts_five_paths() {
    let directory = TempDir::new().unwrap();
    let paths = create_commit_log_files(&directory, 5, EXPECTED_FILE_SIZE);

    let metadata = collect_commit_log_metadata(&paths, collection_options(true)).unwrap();

    assert_eq!(
        metadata.iter().map(|item| &item.path).collect::<Vec<_>>(),
        paths.iter().collect::<Vec<_>>()
    );
}

#[test]
fn disabled_parallel_collection_accepts_five_paths() {
    let directory = TempDir::new().unwrap();
    let paths = create_commit_log_files(&directory, 5, EXPECTED_FILE_SIZE);

    let metadata = collect_commit_log_metadata(&paths, collection_options(false)).unwrap();

    assert_eq!(
        metadata.iter().map(|item| &item.path).collect::<Vec<_>>(),
        paths.iter().collect::<Vec<_>>()
    );
}

#[test]
fn raw_five_path_threshold_filters_the_empty_final_path_to_four() {
    let directory = TempDir::new().unwrap();
    let mut paths = create_commit_log_files(&directory, 4, EXPECTED_FILE_SIZE);
    let empty_last = directory.path().join("00000000000000000004");
    fs::write(&empty_last, []).unwrap();
    paths.push(empty_last.clone());

    let metadata = collect_commit_log_metadata(&paths, collection_options(true)).unwrap();

    assert_eq!(metadata.len(), 4);
    assert!(!empty_last.exists());
}

#[test]
fn parallel_metadata_collection_preserves_input_order() {
    let directory = TempDir::new().unwrap();
    let paths = create_commit_log_files(&directory, 7, EXPECTED_FILE_SIZE);

    let metadata = collect_commit_log_metadata(&paths, collection_options(true)).unwrap();

    assert_eq!(metadata.into_iter().map(|item| item.path).collect::<Vec<_>>(), paths);
}

#[test]
fn sequential_first_error_does_not_delete_a_later_empty_path() {
    let directory = TempDir::new().unwrap();
    let paths = create_commit_log_files(&directory, 5, EXPECTED_FILE_SIZE);
    fs::write(&paths[0], vec![0; (EXPECTED_FILE_SIZE - 1) as usize]).unwrap();
    let empty_last = paths[4].clone();
    fs::write(&empty_last, []).unwrap();

    let error = collect_commit_log_metadata(&paths, collection_options(false)).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(empty_last.exists());
}

#[test]
fn empty_final_path_is_deleted_and_filtered() {
    let directory = TempDir::new().unwrap();
    let mut paths = create_commit_log_files(&directory, 1, EXPECTED_FILE_SIZE);
    let empty_last = directory.path().join("00000000000000000001");
    fs::write(&empty_last, []).unwrap();
    paths.push(empty_last.clone());

    let metadata = collect_commit_log_metadata(&paths, collection_options(false)).unwrap();

    assert_eq!(metadata.len(), 1);
    assert!(!empty_last.exists());
}

#[test]
fn parallel_metadata_error_preserves_kind_and_exact_context() {
    let directory = TempDir::new().unwrap();
    let paths = create_commit_log_files(&directory, 5, EXPECTED_FILE_SIZE);
    let missing = paths[1].clone();
    fs::remove_file(&missing).unwrap();
    let source_error = fs::metadata(&missing).unwrap_err();

    let error = collect_commit_log_metadata(&paths, collection_options(true)).unwrap_err();

    assert_eq!(error.kind(), source_error.kind());
    assert_eq!(
        error.to_string(),
        format!("Failed to get metadata for {:?}: {}", missing, source_error)
    );
}

#[test]
fn metadata_validation_error_is_mapped_to_invalid_data() {
    let directory = TempDir::new().unwrap();
    let paths = create_commit_log_files(&directory, 1, EXPECTED_FILE_SIZE - 1);

    let error = collect_commit_log_metadata(&paths, collection_options(false)).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        format!(
            "{} length {} not matched expected size {}, please check it manually",
            paths[0].display(),
            EXPECTED_FILE_SIZE - 1,
            EXPECTED_FILE_SIZE
        )
    );
}

#[test]
fn exact_sized_file_is_loaded() {
    let metadata = metadata("commitlog/00000000000000000000", 1024);

    assert_eq!(
        validate_commit_log_file(&metadata, 1024, false),
        Ok(CommitLogFileLoadDecision::Load)
    );
}

#[test]
fn empty_last_file_is_removed() {
    let metadata = metadata("commitlog/00000000000000001024", 0);

    assert_eq!(
        validate_commit_log_file(&metadata, 1024, true),
        Ok(CommitLogFileLoadDecision::RemoveEmptyLast)
    );
}

#[test]
fn empty_non_last_file_is_rejected() {
    let metadata = metadata("commitlog/00000000000000000000", 0);

    let error = validate_commit_log_file(&metadata, 1024, false).unwrap_err();
    assert_eq!(error.actual, 0);
    assert_eq!(error.expected, 1024);
}

#[test]
fn short_file_is_rejected() {
    let metadata = metadata("commitlog/00000000000000000000", 1023);

    let error = validate_commit_log_file(&metadata, 1024, false).unwrap_err();
    assert_eq!(error.actual, 1023);
    assert_eq!(error.expected, 1024);
}

#[test]
fn long_file_is_rejected() {
    let metadata = metadata("commitlog/00000000000000000000", 1025);

    let error = validate_commit_log_file(&metadata, 1024, false).unwrap_err();
    assert_eq!(error.actual, 1025);
    assert_eq!(error.expected, 1024);
}

#[test]
fn zero_expected_size_keeps_last_empty_special_case() {
    let empty = metadata("commitlog/00000000000000000000", 0);
    let non_empty = metadata("commitlog/00000000000000000001", 1);

    assert_eq!(
        validate_commit_log_file(&empty, 0, true),
        Ok(CommitLogFileLoadDecision::RemoveEmptyLast)
    );
    assert_eq!(
        validate_commit_log_file(&empty, 0, false),
        Ok(CommitLogFileLoadDecision::Load)
    );
    assert!(validate_commit_log_file(&non_empty, 0, false).is_err());
}

#[test]
fn validation_error_exposes_fields_and_exact_message() {
    let metadata = metadata("commitlog/00000000000000000000", 7);

    let error = validate_commit_log_file(&metadata, 8, false).unwrap_err();
    assert_eq!(error.path, metadata.path);
    assert_eq!(error.actual, 7);
    assert_eq!(error.expected, 8);
    assert_eq!(
        error.to_string(),
        "commitlog/00000000000000000000 length 7 not matched expected size 8, please check it manually"
    );
    let error_trait: &dyn Error = &error;
    assert!(error_trait.source().is_none());
}

fn mapping_plan(file_count: usize, parallel_enabled: bool, lazy_mmap_enabled: bool) -> CommitLogMappingPlan {
    let metadata = (0..file_count)
        .map(|index| metadata(&format!("commitlog/{index:020}"), 1024 + index as u64))
        .collect();
    CommitLogMappingPlan::new(
        metadata,
        CommitLogMappingOptions {
            parallel_enabled,
            lazy_mmap_enabled,
        },
    )
}

#[test]
fn empty_mapping_plan_is_sequential() {
    let plan = mapping_plan(0, true, false);

    assert_eq!(plan.execution(), CommitLogMappingExecution::Sequential);
    assert!(plan.entries().is_empty());
}

#[test]
fn one_mapping_entry_is_sequential() {
    let plan = mapping_plan(1, true, false);

    assert_eq!(plan.execution(), CommitLogMappingExecution::Sequential);
    assert_eq!(plan.entries().len(), 1);
}

#[test]
fn four_mapping_entries_are_sequential() {
    let plan = mapping_plan(4, true, false);

    assert_eq!(plan.execution(), CommitLogMappingExecution::Sequential);
}

#[test]
fn five_mapping_entries_are_parallel() {
    let plan = mapping_plan(5, true, false);

    assert_eq!(plan.execution(), CommitLogMappingExecution::Parallel);
}

#[test]
fn disabled_parallel_option_keeps_five_entries_sequential() {
    let plan = mapping_plan(5, false, false);

    assert_eq!(plan.execution(), CommitLogMappingExecution::Sequential);
}

#[test]
fn mapping_plan_preserves_metadata_order() {
    let plan = mapping_plan(5, true, false);
    let paths: Vec<_> = plan
        .entries()
        .iter()
        .map(|entry| entry.metadata().path.clone())
        .collect();

    assert_eq!(
        paths,
        (0..5)
            .map(|index| PathBuf::from(format!("commitlog/{index:020}")))
            .collect::<Vec<_>>()
    );
}

#[test]
fn mapping_entry_keeps_the_whole_metadata_value() {
    let plan = mapping_plan(3, false, false);

    for (index, entry) in plan.entries().iter().enumerate() {
        assert_eq!(
            entry.metadata(),
            &metadata(&format!("commitlog/{index:020}"), 1024 + index as u64)
        );
    }
}

#[test]
fn lazy_mapping_marks_only_non_final_entries_read_only() {
    let plan = mapping_plan(4, false, true);
    let modes: Vec<_> = plan.entries().iter().map(|entry| entry.mode()).collect();

    assert_eq!(
        modes,
        vec![
            CommitLogMappingMode::LazyReadOnly,
            CommitLogMappingMode::LazyReadOnly,
            CommitLogMappingMode::LazyReadOnly,
            CommitLogMappingMode::Eager,
        ]
    );
}

fn mmap_counters(statistics: &LoadStatistics) -> (u64, u64, u64, u64) {
    (
        statistics.mmap_advice_attempts,
        statistics.mmap_advice_successes,
        statistics.mmap_advice_failures,
        statistics.mmap_advice_elapsed_ms,
    )
}

fn prefetch_counters(statistics: &LoadStatistics) -> (u64, u64, u64, u64) {
    (
        statistics.file_prefetch_attempts,
        statistics.file_prefetch_successes,
        statistics.file_prefetch_failures,
        statistics.file_prefetch_elapsed_ms,
    )
}

#[test]
fn disabled_recovery_hint_adapters_are_not_attempted() {
    let (_file, mmap) = mutable_mapping();
    let mut statistics = LoadStatistics::default();

    record_mmap_advice(
        &mut statistics,
        apply_recovery_mmap_advice(RecoveryMmapAdvice::Disabled, &mmap, "commitlog/0"),
    );
    record_file_prefetch(
        &mut statistics,
        apply_recovery_file_prefetch(RecoveryFilePrefetch::Disabled, &mmap, "commitlog/0"),
    );

    assert_eq!(mmap_counters(&statistics), (0, 0, 0, 0));
    assert_eq!(prefetch_counters(&statistics), (0, 0, 0, 0));
}

#[cfg(not(unix))]
#[test]
fn sequential_mmap_advice_is_not_attempted_when_unsupported() {
    let (_file, mmap) = mutable_mapping();
    let mut statistics = LoadStatistics::default();

    record_mmap_advice(
        &mut statistics,
        apply_recovery_mmap_advice(RecoveryMmapAdvice::Sequential, &mmap, "commitlog/0"),
    );

    assert_eq!(mmap_counters(&statistics), (0, 0, 0, 0));
}

#[cfg(not(windows))]
#[test]
fn sequential_file_prefetch_is_not_attempted_when_unsupported() {
    let (_file, mmap) = mutable_mapping();
    let mut statistics = LoadStatistics::default();

    record_file_prefetch(
        &mut statistics,
        apply_recovery_file_prefetch(RecoveryFilePrefetch::Sequential, &mmap, "commitlog/0"),
    );

    assert_eq!(prefetch_counters(&statistics), (0, 0, 0, 0));
}

#[test]
fn not_attempted_outcomes_leave_both_hint_families_unchanged() {
    let mut statistics = LoadStatistics {
        mmap_advice_attempts: 11,
        mmap_advice_successes: 12,
        mmap_advice_failures: 13,
        mmap_advice_elapsed_ms: 14,
        file_prefetch_attempts: 21,
        file_prefetch_successes: 22,
        file_prefetch_failures: 23,
        file_prefetch_elapsed_ms: 24,
        ..LoadStatistics::default()
    };

    record_mmap_advice(&mut statistics, HintOutcome::not_attempted());
    record_file_prefetch(&mut statistics, HintOutcome::not_attempted());

    assert_eq!(mmap_counters(&statistics), (11, 12, 13, 14));
    assert_eq!(prefetch_counters(&statistics), (21, 22, 23, 24));
}

#[test]
fn mmap_success_records_only_mmap_attempt_success_and_whole_milliseconds() {
    let mut statistics = LoadStatistics::default();

    record_mmap_advice(&mut statistics, HintOutcome::success(Duration::from_micros(1_999)));

    assert_eq!(mmap_counters(&statistics), (1, 1, 0, 1));
    assert_eq!(prefetch_counters(&statistics), (0, 0, 0, 0));
}

#[test]
fn mmap_failure_records_zero_for_submillisecond_elapsed_time() {
    let mut statistics = LoadStatistics::default();

    record_mmap_advice(&mut statistics, HintOutcome::failure(Duration::from_nanos(999_999)));

    assert_eq!(mmap_counters(&statistics), (1, 0, 1, 0));
    assert_eq!(prefetch_counters(&statistics), (0, 0, 0, 0));
}

#[test]
fn prefetch_success_and_failure_record_only_prefetch_fields() {
    let mut statistics = LoadStatistics::default();

    record_file_prefetch(&mut statistics, HintOutcome::success(Duration::from_millis(7)));
    record_file_prefetch(&mut statistics, HintOutcome::failure(Duration::from_millis(9)));

    assert_eq!(mmap_counters(&statistics), (0, 0, 0, 0));
    assert_eq!(prefetch_counters(&statistics), (2, 1, 1, 16));
}

#[test]
fn duration_max_is_clamped_to_u64_max_milliseconds() {
    let mut statistics = LoadStatistics::default();

    record_mmap_advice(&mut statistics, HintOutcome::success(Duration::MAX));

    assert_eq!(mmap_counters(&statistics), (1, 1, 0, u64::MAX));
}

#[test]
fn mmap_success_saturates_attempt_success_and_elapsed_fields() {
    let mut statistics = LoadStatistics {
        mmap_advice_attempts: u64::MAX,
        mmap_advice_successes: u64::MAX,
        mmap_advice_elapsed_ms: u64::MAX,
        ..LoadStatistics::default()
    };

    record_mmap_advice(&mut statistics, HintOutcome::success(Duration::from_millis(1)));

    assert_eq!(mmap_counters(&statistics), (u64::MAX, u64::MAX, 0, u64::MAX));
}

#[test]
fn mmap_failure_saturates_attempt_failure_and_elapsed_fields() {
    let mut statistics = LoadStatistics {
        mmap_advice_attempts: u64::MAX,
        mmap_advice_failures: u64::MAX,
        mmap_advice_elapsed_ms: u64::MAX,
        ..LoadStatistics::default()
    };

    record_mmap_advice(&mut statistics, HintOutcome::failure(Duration::from_millis(1)));

    assert_eq!(mmap_counters(&statistics), (u64::MAX, 0, u64::MAX, u64::MAX));
}

#[test]
fn prefetch_counters_and_elapsed_fields_saturate() {
    let mut statistics = LoadStatistics {
        file_prefetch_attempts: u64::MAX,
        file_prefetch_successes: u64::MAX,
        file_prefetch_failures: u64::MAX,
        file_prefetch_elapsed_ms: u64::MAX,
        ..LoadStatistics::default()
    };

    record_file_prefetch(&mut statistics, HintOutcome::success(Duration::from_millis(1)));
    record_file_prefetch(&mut statistics, HintOutcome::failure(Duration::from_millis(1)));

    assert_eq!(prefetch_counters(&statistics), (u64::MAX, u64::MAX, u64::MAX, u64::MAX));
}
