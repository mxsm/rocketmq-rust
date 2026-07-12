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

use rocketmq_store::log_file::commit_log_recovery::plan_abnormal_recovery_window_from_ranges as legacy_plan;
use rocketmq_store::log_file::commit_log_recovery::AbnormalRecoveryFileRange as LegacyFileRange;
use rocketmq_store::log_file::commit_log_recovery::AbnormalRecoveryWindow as LegacyWindow;
use rocketmq_store::log_file::commit_log_recovery::RecoveryStatistics as LegacyRecoveryStatistics;
use rocketmq_store_local::commit_log::recovery::AbnormalRecoveryFileRange as CanonicalFileRange;
use rocketmq_store_local::commit_log::recovery::AbnormalRecoveryWindow as CanonicalWindow;
use rocketmq_store_local::commit_log::recovery::RecoveryStatistics as CanonicalRecoveryStatistics;

type RecoveryPlanner = fn(&[CanonicalFileRange], Option<usize>, usize, i64, i64, i64, i64) -> CanonicalWindow;

fn canonical_recovery_statistics(value: LegacyRecoveryStatistics) -> CanonicalRecoveryStatistics {
    value
}

fn canonical_file_range(value: LegacyFileRange) -> CanonicalFileRange {
    value
}

fn canonical_window(value: LegacyWindow) -> CanonicalWindow {
    value
}

#[test]
fn recovery_value_objects_preserve_type_identity() {
    let recovery_stats = canonical_recovery_statistics(LegacyRecoveryStatistics::default());
    assert_eq!(recovery_stats.files_processed, 0);
    assert_eq!(
        canonical_file_range(LegacyFileRange::new(100, 100)),
        CanonicalFileRange::new(100, 100)
    );
}

#[test]
fn recovery_window_fixed_golden_preserves_all_planner_outcomes() {
    let ranges: Vec<_> = (0..6)
        .map(|index| CanonicalFileRange::new(i64::from(index) * 100, 100))
        .collect();
    let planner: RecoveryPlanner = legacy_plan;

    let cases = [
        (
            "empty",
            planner(&[], Some(0), 2, 0, 0, 0, 0),
            CanonicalWindow {
                start_index: 0,
                checkpoint_index: Some(0),
                dispatch_progress_index: None,
                confirm_offset_index: None,
                file_count_limit: Some(2),
                expanded_files: 0,
                scanned_file_count: 0,
                scanned_bytes: 0,
                end_offset: None,
                fallback_reason: Some("empty_commitlog"),
            },
        ),
        (
            "checkpoint-only",
            planner(&ranges, Some(4), 0, 100, 200, 0, 600),
            CanonicalWindow {
                start_index: 4,
                checkpoint_index: Some(4),
                dispatch_progress_index: None,
                confirm_offset_index: None,
                file_count_limit: None,
                expanded_files: 0,
                scanned_file_count: 2,
                scanned_bytes: 200,
                end_offset: Some(600),
                fallback_reason: None,
            },
        ),
        (
            "bounded-checkpoint-missing",
            planner(&ranges, None, 2, 200, 300, 0, 600),
            CanonicalWindow {
                start_index: 0,
                checkpoint_index: None,
                dispatch_progress_index: None,
                confirm_offset_index: None,
                file_count_limit: Some(2),
                expanded_files: 0,
                scanned_file_count: 6,
                scanned_bytes: 600,
                end_offset: Some(600),
                fallback_reason: Some("checkpoint_not_matched"),
            },
        ),
        (
            "invalid-range",
            planner(&ranges, Some(4), 2, 200, 300, 600, 0),
            CanonicalWindow {
                start_index: 0,
                checkpoint_index: Some(4),
                dispatch_progress_index: None,
                confirm_offset_index: None,
                file_count_limit: Some(2),
                expanded_files: 4,
                scanned_file_count: 6,
                scanned_bytes: 600,
                end_offset: None,
                fallback_reason: Some("invalid_commitlog_range"),
            },
        ),
        (
            "dispatch-out-of-range",
            planner(&ranges, Some(4), 2, 700, 300, 0, 600),
            CanonicalWindow {
                start_index: 0,
                checkpoint_index: Some(4),
                dispatch_progress_index: None,
                confirm_offset_index: None,
                file_count_limit: Some(2),
                expanded_files: 4,
                scanned_file_count: 6,
                scanned_bytes: 600,
                end_offset: Some(600),
                fallback_reason: Some("dispatch_progress_out_of_range"),
            },
        ),
        (
            "confirm-out-of-range",
            planner(&ranges, Some(4), 2, 300, -1, 0, 600),
            CanonicalWindow {
                start_index: 0,
                checkpoint_index: Some(4),
                dispatch_progress_index: None,
                confirm_offset_index: None,
                file_count_limit: Some(2),
                expanded_files: 4,
                scanned_file_count: 6,
                scanned_bytes: 600,
                end_offset: Some(600),
                fallback_reason: Some("confirm_offset_out_of_range"),
            },
        ),
        (
            "normal-bounded",
            planner(&ranges, Some(5), 2, 400, 500, 0, 600),
            CanonicalWindow {
                start_index: 3,
                checkpoint_index: Some(5),
                dispatch_progress_index: Some(4),
                confirm_offset_index: Some(5),
                file_count_limit: Some(2),
                expanded_files: 2,
                scanned_file_count: 3,
                scanned_bytes: 300,
                end_offset: Some(600),
                fallback_reason: None,
            },
        ),
    ];

    for (name, actual, expected) in cases {
        assert_eq!(canonical_window(actual), expected, "{name}");
    }
}
