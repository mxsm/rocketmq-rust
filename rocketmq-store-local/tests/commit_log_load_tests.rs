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
