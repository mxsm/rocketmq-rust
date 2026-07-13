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
use std::path::PathBuf;

use rocketmq_store_local::commit_log::load::validate_commit_log_file;
use rocketmq_store_local::commit_log::load::CommitLogFileLoadDecision;
use rocketmq_store_local::commit_log::load::CommitLogFileMetadata;
use rocketmq_store_local::commit_log::load::CommitLogMappingExecution;
use rocketmq_store_local::commit_log::load::CommitLogMappingMode;
use rocketmq_store_local::commit_log::load::CommitLogMappingOptions;
use rocketmq_store_local::commit_log::load::CommitLogMappingPlan;

fn metadata(path: &str, size: u64) -> CommitLogFileMetadata {
    CommitLogFileMetadata {
        path: PathBuf::from(path),
        size,
    }
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
