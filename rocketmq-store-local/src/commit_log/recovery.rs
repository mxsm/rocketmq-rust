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

use tracing::info;

const MAX_SIGNED_OFFSET: u64 = 9_223_372_036_854_775_807;

/// Compatibility policy for normal CommitLog recovery paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NormalRecoveryPolicy {
    /// Sequential recovery retains the start of the last valid message and stops at invalid input.
    Standard,
    /// Batched recovery retains the end of the last valid message and continues with the next
    /// segment.
    Optimized,
}

/// Runtime-neutral event consumed by the normal recovery offset state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NormalRecoveryEvent {
    /// Begins scanning a CommitLog segment at its absolute base offset.
    SegmentStarted { base_offset: u64 },
    /// Accepts one validated message frame.
    MessageAccepted {
        /// Absolute base offset of the containing segment.
        segment_base: u64,
        /// Frame start relative to the containing segment.
        relative_start: u64,
        /// Validated frame size.
        size: u64,
    },
    /// Encounters an end-of-segment blank marker.
    Blank,
    /// Encounters an invalid record.
    InvalidRecord,
    /// Reaches the end of the current frame source.
    SourceEnded,
}

/// Control action returned after applying a normal recovery event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NormalRecoveryAction {
    /// Continue scanning records in the current segment.
    ContinueRecord,
    /// Continue recovery at the next segment.
    ContinueNextSegment,
    /// Stop recovery at the current watermarks.
    StopRecovery,
}

/// Checked offset failure while applying a normal recovery event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NormalRecoveryOffsetError {
    /// Segment base plus relative frame start overflowed `u64`.
    #[error("segment base {base_offset} plus relative start {relative_start} overflowed")]
    BaseRelativeOverflow {
        /// Segment base offset.
        base_offset: u64,
        /// Relative frame start.
        relative_start: u64,
    },
    /// Message start plus size overflowed `u64`.
    #[error("message start {start_offset} plus size {size} overflowed")]
    MessageEndOverflow {
        /// Absolute message start.
        start_offset: u64,
        /// Message frame size.
        size: u64,
    },
    /// A resulting recovery watermark cannot be represented by Store's signed offsets.
    #[error("recovery offset {offset} exceeds i64::MAX")]
    OffsetExceedsI64 {
        /// Out-of-range offset.
        offset: u64,
    },
}

/// Immutable normal recovery watermarks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NormalRecoverySummary {
    /// Policy-specific last valid message watermark.
    pub last_valid_offset: u64,
    /// Physical truncation watermark.
    pub truncate_offset: u64,
}

/// Runtime-neutral normal recovery offset state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NormalRecoveryState {
    last_valid_offset: u64,
    truncate_offset: u64,
    policy: NormalRecoveryPolicy,
}

impl NormalRecoveryState {
    /// Creates a state machine with both watermarks at the supplied confirmed offset.
    ///
    /// # Errors
    ///
    /// Returns [`NormalRecoveryOffsetError::OffsetExceedsI64`] when the initial offset cannot be
    /// represented by Store's signed offsets.
    pub const fn try_new(initial_offset: u64, policy: NormalRecoveryPolicy) -> Result<Self, NormalRecoveryOffsetError> {
        if initial_offset > MAX_SIGNED_OFFSET {
            return Err(NormalRecoveryOffsetError::OffsetExceedsI64 { offset: initial_offset });
        }
        Ok(Self {
            last_valid_offset: initial_offset,
            truncate_offset: initial_offset,
            policy,
        })
    }

    /// Applies one event transactionally and returns the next scan action.
    ///
    /// # Errors
    ///
    /// Returns [`NormalRecoveryOffsetError`] when checked offset arithmetic fails or a resulting
    /// watermark exceeds `i64::MAX`. The state is unchanged on error.
    pub fn apply(&mut self, event: NormalRecoveryEvent) -> Result<NormalRecoveryAction, NormalRecoveryOffsetError> {
        let (action, next_last_valid, next_truncate) = match event {
            NormalRecoveryEvent::SegmentStarted { base_offset } => match self.policy {
                NormalRecoveryPolicy::Standard => (
                    NormalRecoveryAction::ContinueRecord,
                    self.last_valid_offset,
                    base_offset,
                ),
                NormalRecoveryPolicy::Optimized => (
                    NormalRecoveryAction::ContinueRecord,
                    self.last_valid_offset,
                    self.truncate_offset,
                ),
            },
            NormalRecoveryEvent::MessageAccepted {
                segment_base,
                relative_start,
                size,
            } => {
                let start_offset = segment_base.checked_add(relative_start).ok_or(
                    NormalRecoveryOffsetError::BaseRelativeOverflow {
                        base_offset: segment_base,
                        relative_start,
                    },
                )?;
                let end_offset = start_offset
                    .checked_add(size)
                    .ok_or(NormalRecoveryOffsetError::MessageEndOverflow { start_offset, size })?;
                match self.policy {
                    NormalRecoveryPolicy::Standard => (NormalRecoveryAction::ContinueRecord, start_offset, end_offset),
                    NormalRecoveryPolicy::Optimized => (NormalRecoveryAction::ContinueRecord, end_offset, end_offset),
                }
            }
            NormalRecoveryEvent::Blank => (
                NormalRecoveryAction::ContinueNextSegment,
                self.last_valid_offset,
                self.truncate_offset,
            ),
            NormalRecoveryEvent::InvalidRecord | NormalRecoveryEvent::SourceEnded => {
                let action = match self.policy {
                    NormalRecoveryPolicy::Standard => NormalRecoveryAction::StopRecovery,
                    NormalRecoveryPolicy::Optimized => NormalRecoveryAction::ContinueNextSegment,
                };
                (action, self.last_valid_offset, self.truncate_offset)
            }
        };

        for offset in [next_last_valid, next_truncate] {
            if offset > MAX_SIGNED_OFFSET {
                return Err(NormalRecoveryOffsetError::OffsetExceedsI64 { offset });
            }
        }

        self.last_valid_offset = next_last_valid;
        self.truncate_offset = next_truncate;
        Ok(action)
    }

    /// Returns the current last-valid and truncation watermarks.
    pub const fn summary(&self) -> NormalRecoverySummary {
        NormalRecoverySummary {
            last_valid_offset: self.last_valid_offset,
            truncate_offset: self.truncate_offset,
        }
    }
}

/// Statistics for recovery operations.
#[derive(Debug, Default, Clone)]
pub struct RecoveryStatistics {
    pub files_processed: usize,
    pub messages_recovered: u64,
    pub bytes_processed: u64,
    pub invalid_messages: u64,
    pub recovery_time_ms: u128,
}

impl RecoveryStatistics {
    pub fn log_summary(&self, recovery_type: &str) {
        info!(
            target: "rocketmq_store::log_file::commit_log_recovery",
            "{} recovery completed: {} files, {} messages, {:.2} MB, {} invalid, {}ms",
            recovery_type,
            self.files_processed,
            self.messages_recovered,
            self.bytes_processed as f64 / 1024.0 / 1024.0,
            self.invalid_messages,
            self.recovery_time_ms
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbnormalRecoveryWindow {
    pub start_index: usize,
    pub checkpoint_index: Option<usize>,
    pub dispatch_progress_index: Option<usize>,
    pub confirm_offset_index: Option<usize>,
    pub file_count_limit: Option<usize>,
    pub expanded_files: usize,
    pub scanned_file_count: usize,
    pub scanned_bytes: u64,
    pub end_offset: Option<i64>,
    pub fallback_reason: Option<&'static str>,
}

impl AbnormalRecoveryWindow {
    fn new(
        file_ranges: &[AbnormalRecoveryFileRange],
        start_index: usize,
        checkpoint_index: Option<usize>,
        file_count_limit: Option<usize>,
        end_offset: Option<i64>,
        fallback_reason: Option<&'static str>,
    ) -> Self {
        let expanded_files = checkpoint_index
            .map(|checkpoint_index| checkpoint_index.saturating_sub(start_index))
            .unwrap_or_default();
        let scanned_file_count = file_ranges.len().saturating_sub(start_index);
        let scanned_bytes = planned_scanned_bytes(file_ranges, start_index, end_offset);

        Self {
            start_index,
            checkpoint_index,
            dispatch_progress_index: None,
            confirm_offset_index: None,
            file_count_limit,
            expanded_files,
            scanned_file_count,
            scanned_bytes,
            end_offset,
            fallback_reason,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AbnormalRecoveryFileRange {
    pub start_offset: i64,
    pub file_size: u64,
}

impl AbnormalRecoveryFileRange {
    pub const fn new(start_offset: i64, file_size: u64) -> Self {
        Self {
            start_offset,
            file_size,
        }
    }
}

pub fn plan_abnormal_recovery_window_from_ranges(
    file_ranges: &[AbnormalRecoveryFileRange],
    checkpoint_index: Option<usize>,
    max_recovery_commit_log_files: usize,
    dispatch_progress_offset: i64,
    confirm_offset: i64,
    commit_log_min_offset: i64,
    commit_log_max_offset: i64,
) -> AbnormalRecoveryWindow {
    if file_ranges.is_empty() {
        return AbnormalRecoveryWindow::new(
            file_ranges,
            0,
            checkpoint_index,
            configured_file_count_limit(max_recovery_commit_log_files),
            None,
            Some("empty_commitlog"),
        );
    }

    let end_offset =
        valid_commit_log_range(commit_log_min_offset, commit_log_max_offset).then_some(commit_log_max_offset);

    if max_recovery_commit_log_files == 0 {
        return AbnormalRecoveryWindow::new(
            file_ranges,
            checkpoint_index.unwrap_or_default(),
            checkpoint_index,
            None,
            end_offset,
            checkpoint_index.is_none().then_some("checkpoint_not_matched"),
        );
    }

    let file_count_limit = configured_file_count_limit(max_recovery_commit_log_files);
    let Some(checkpoint_index) = checkpoint_index else {
        return AbnormalRecoveryWindow::new(
            file_ranges,
            0,
            None,
            file_count_limit,
            end_offset,
            Some("checkpoint_not_matched"),
        );
    };

    if !valid_commit_log_range(commit_log_min_offset, commit_log_max_offset) {
        return AbnormalRecoveryWindow::new(
            file_ranges,
            0,
            Some(checkpoint_index),
            file_count_limit,
            None,
            Some("invalid_commitlog_range"),
        );
    }

    let Some(dispatch_progress_index) = file_index_for_offset(
        file_ranges,
        dispatch_progress_offset,
        commit_log_min_offset,
        commit_log_max_offset,
    ) else {
        return AbnormalRecoveryWindow::new(
            file_ranges,
            0,
            Some(checkpoint_index),
            file_count_limit,
            end_offset,
            Some("dispatch_progress_out_of_range"),
        );
    };

    let Some(confirm_offset_index) = file_index_for_offset(
        file_ranges,
        confirm_offset,
        commit_log_min_offset,
        commit_log_max_offset,
    ) else {
        return AbnormalRecoveryWindow::new(
            file_ranges,
            0,
            Some(checkpoint_index),
            file_count_limit,
            end_offset,
            Some("confirm_offset_out_of_range"),
        );
    };

    let checkpoint_window_start = checkpoint_index.saturating_sub(max_recovery_commit_log_files);
    let start_index = checkpoint_window_start
        .min(dispatch_progress_index)
        .min(confirm_offset_index);
    let mut window = AbnormalRecoveryWindow::new(
        file_ranges,
        start_index,
        Some(checkpoint_index),
        file_count_limit,
        end_offset,
        None,
    );
    window.dispatch_progress_index = Some(dispatch_progress_index);
    window.confirm_offset_index = Some(confirm_offset_index);
    window
}

fn configured_file_count_limit(max_recovery_commit_log_files: usize) -> Option<usize> {
    (max_recovery_commit_log_files != 0).then_some(max_recovery_commit_log_files)
}

fn valid_commit_log_range(commit_log_min_offset: i64, commit_log_max_offset: i64) -> bool {
    commit_log_min_offset >= 0 && commit_log_max_offset >= commit_log_min_offset
}

fn file_index_for_offset(
    file_ranges: &[AbnormalRecoveryFileRange],
    offset: i64,
    commit_log_min_offset: i64,
    commit_log_max_offset: i64,
) -> Option<usize> {
    if offset < commit_log_min_offset || offset > commit_log_max_offset {
        return None;
    }

    let mut selected_index = None;
    for (index, file_range) in file_ranges.iter().enumerate() {
        if offset >= file_range.start_offset {
            selected_index = Some(index);
        } else {
            break;
        }
    }
    selected_index
}

fn planned_scanned_bytes(
    file_ranges: &[AbnormalRecoveryFileRange],
    start_index: usize,
    end_offset: Option<i64>,
) -> u64 {
    let Some(start_file) = file_ranges.get(start_index) else {
        return 0;
    };
    if let Some(end_offset) = end_offset {
        if end_offset >= start_file.start_offset {
            return (end_offset - start_file.start_offset) as u64;
        }
    }

    file_ranges[start_index..]
        .iter()
        .map(|file_range| file_range.file_size)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file_ranges(count: usize) -> Vec<AbnormalRecoveryFileRange> {
        (0..count)
            .map(|index| AbnormalRecoveryFileRange::new((index as i64) * 100, 100))
            .collect()
    }

    #[test]
    fn recovery_statistics_preserve_fields() {
        let stats = RecoveryStatistics {
            messages_recovered: 1000,
            bytes_processed: 1024 * 1024,
            files_processed: 5,
            ..Default::default()
        };
        assert_eq!(stats.messages_recovered, 1000);
        assert_eq!(stats.bytes_processed, 1024 * 1024);
    }

    #[test]
    fn abnormal_recovery_window_fixed_golden() {
        let ranges = file_ranges(6);
        let cases = [
            (
                "empty",
                plan_abnormal_recovery_window_from_ranges(&[], Some(0), 2, 0, 0, 0, 0),
                "AbnormalRecoveryWindow { start_index: 0, checkpoint_index: Some(0), dispatch_progress_index: None, \
                 confirm_offset_index: None, file_count_limit: Some(2), expanded_files: 0, scanned_file_count: 0, \
                 scanned_bytes: 0, end_offset: None, fallback_reason: Some(\"empty_commitlog\") }",
            ),
            (
                "checkpoint-only",
                plan_abnormal_recovery_window_from_ranges(&ranges, Some(4), 0, 100, 200, 0, 600),
                "AbnormalRecoveryWindow { start_index: 4, checkpoint_index: Some(4), dispatch_progress_index: None, \
                 confirm_offset_index: None, file_count_limit: None, expanded_files: 0, scanned_file_count: 2, \
                 scanned_bytes: 200, end_offset: Some(600), fallback_reason: None }",
            ),
            (
                "bounded-checkpoint-missing",
                plan_abnormal_recovery_window_from_ranges(&ranges, None, 2, 200, 300, 0, 600),
                "AbnormalRecoveryWindow { start_index: 0, checkpoint_index: None, dispatch_progress_index: None, \
                 confirm_offset_index: None, file_count_limit: Some(2), expanded_files: 0, scanned_file_count: 6, \
                 scanned_bytes: 600, end_offset: Some(600), fallback_reason: Some(\"checkpoint_not_matched\") }",
            ),
            (
                "invalid-range",
                plan_abnormal_recovery_window_from_ranges(&ranges, Some(4), 2, 200, 300, 600, 0),
                "AbnormalRecoveryWindow { start_index: 0, checkpoint_index: Some(4), dispatch_progress_index: None, \
                 confirm_offset_index: None, file_count_limit: Some(2), expanded_files: 4, scanned_file_count: 6, \
                 scanned_bytes: 600, end_offset: None, fallback_reason: Some(\"invalid_commitlog_range\") }",
            ),
            (
                "dispatch-out-of-range",
                plan_abnormal_recovery_window_from_ranges(&ranges, Some(4), 2, 700, 300, 0, 600),
                "AbnormalRecoveryWindow { start_index: 0, checkpoint_index: Some(4), dispatch_progress_index: None, \
                 confirm_offset_index: None, file_count_limit: Some(2), expanded_files: 4, scanned_file_count: 6, \
                 scanned_bytes: 600, end_offset: Some(600), fallback_reason: Some(\"dispatch_progress_out_of_range\") \
                 }",
            ),
            (
                "confirm-out-of-range",
                plan_abnormal_recovery_window_from_ranges(&ranges, Some(4), 2, 300, -1, 0, 600),
                "AbnormalRecoveryWindow { start_index: 0, checkpoint_index: Some(4), dispatch_progress_index: None, \
                 confirm_offset_index: None, file_count_limit: Some(2), expanded_files: 4, scanned_file_count: 6, \
                 scanned_bytes: 600, end_offset: Some(600), fallback_reason: Some(\"confirm_offset_out_of_range\") }",
            ),
            (
                "normal-bounded",
                plan_abnormal_recovery_window_from_ranges(&ranges, Some(5), 2, 400, 500, 0, 600),
                "AbnormalRecoveryWindow { start_index: 3, checkpoint_index: Some(5), dispatch_progress_index: \
                 Some(4), confirm_offset_index: Some(5), file_count_limit: Some(2), expanded_files: 2, \
                 scanned_file_count: 3, scanned_bytes: 300, end_offset: Some(600), fallback_reason: None }",
            ),
        ];

        for (name, actual, expected) in cases {
            assert_eq!(format!("{actual:?}"), expected, "{name}");
        }
    }

    #[test]
    fn abnormal_recovery_window_preserves_checkpoint_start_when_limit_disabled() {
        let ranges = file_ranges(6);
        let window = plan_abnormal_recovery_window_from_ranges(&ranges, Some(4), 0, 100, 200, 0, 600);
        assert_eq!(window.start_index, 4);
        assert_eq!(window.checkpoint_index, Some(4));
        assert_eq!(window.file_count_limit, None);
        assert_eq!(window.expanded_files, 0);
        assert_eq!(window.fallback_reason, None);
    }

    #[test]
    fn abnormal_recovery_window_expands_by_configured_file_limit() {
        let ranges = file_ranges(6);
        let window = plan_abnormal_recovery_window_from_ranges(&ranges, Some(5), 2, 400, 500, 0, 600);
        assert_eq!(window.start_index, 3);
        assert_eq!(window.file_count_limit, Some(2));
        assert_eq!(window.expanded_files, 2);
        assert_eq!(window.scanned_file_count, 3);
        assert_eq!(window.scanned_bytes, 300);
        assert_eq!(window.fallback_reason, None);
    }

    #[test]
    fn abnormal_recovery_window_expands_to_dispatch_progress() {
        let ranges = file_ranges(6);
        let window = plan_abnormal_recovery_window_from_ranges(&ranges, Some(5), 1, 200, 500, 0, 600);
        assert_eq!(window.start_index, 2);
        assert_eq!(window.dispatch_progress_index, Some(2));
        assert_eq!(window.confirm_offset_index, Some(5));
        assert_eq!(window.expanded_files, 3);
    }

    #[test]
    fn abnormal_recovery_window_expands_to_confirm_offset() {
        let ranges = file_ranges(6);
        let window = plan_abnormal_recovery_window_from_ranges(&ranges, Some(5), 1, 500, 100, 0, 600);
        assert_eq!(window.start_index, 1);
        assert_eq!(window.dispatch_progress_index, Some(5));
        assert_eq!(window.confirm_offset_index, Some(1));
        assert_eq!(window.expanded_files, 4);
    }

    #[test]
    fn abnormal_recovery_window_falls_back_when_checkpoint_missing() {
        let ranges = file_ranges(6);
        let window = plan_abnormal_recovery_window_from_ranges(&ranges, None, 2, 200, 300, 0, 600);
        assert_eq!(window.start_index, 0);
        assert_eq!(window.checkpoint_index, None);
        assert_eq!(window.fallback_reason, Some("checkpoint_not_matched"));
    }

    #[test]
    fn abnormal_recovery_window_falls_back_when_dispatch_progress_is_untrusted() {
        let ranges = file_ranges(6);
        let window = plan_abnormal_recovery_window_from_ranges(&ranges, Some(4), 2, 700, 300, 0, 600);
        assert_eq!(window.start_index, 0);
        assert_eq!(window.fallback_reason, Some("dispatch_progress_out_of_range"));
    }

    #[test]
    fn abnormal_recovery_window_falls_back_when_confirm_offset_is_untrusted() {
        let ranges = file_ranges(6);
        let window = plan_abnormal_recovery_window_from_ranges(&ranges, Some(4), 2, 300, -1, 0, 600);
        assert_eq!(window.start_index, 0);
        assert_eq!(window.fallback_reason, Some("confirm_offset_out_of_range"));
    }
}
