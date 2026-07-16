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

use super::consume_queue::should_truncate_recovery_consume_queue;
use super::AbnormalRecoveryState;
use super::NormalRecoveryPolicy;
use super::NormalRecoveryState;

/// Runtime-neutral decision produced after CommitLog recovery finishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitLogRecoveryCompletion {
    /// No CommitLog segment remains, so persisted recovery progress and logical queues must reset.
    Empty,
    /// Recovery produced a valid physical boundary that Store must apply to its compatibility
    /// state.
    Recovered {
        /// Confirm offset used outside controller mode.
        confirm_offset: i64,
        /// Upper bound used while clamping controller-mode confirm progress.
        controller_confirm_offset: i64,
        /// Physical offset applied to flush, commit, and dirty-file truncation progress.
        process_offset: i64,
        /// Whether ConsumeQueue contains data at or beyond `process_offset`.
        truncate_consume_queue: bool,
    },
}

#[inline]
const fn signed_offset(offset: u64) -> i64 {
    // Normal and abnormal recovery constructors and every state transition reject offsets above
    // i64::MAX, so every stored watermark preserves its value through this cast.
    offset as i64
}

impl NormalRecoveryState {
    /// Resolves the final normal-recovery watermarks and ConsumeQueue action.
    pub fn completion(&self, max_phy_offset_of_consume_queue: i64) -> CommitLogRecoveryCompletion {
        let summary = self.summary();
        let confirm_offset = signed_offset(summary.last_valid_offset);
        let process_offset = signed_offset(summary.truncate_offset);
        let controller_confirm_offset = match self.policy {
            NormalRecoveryPolicy::Standard => process_offset,
            NormalRecoveryPolicy::Optimized => confirm_offset,
        };
        CommitLogRecoveryCompletion::Recovered {
            confirm_offset,
            controller_confirm_offset,
            process_offset,
            truncate_consume_queue: should_truncate_recovery_consume_queue(
                max_phy_offset_of_consume_queue,
                summary.truncate_offset,
            ),
        }
    }
}

impl AbnormalRecoveryState {
    /// Resolves the final abnormal-recovery watermarks and ConsumeQueue action.
    pub fn completion(&self, max_phy_offset_of_consume_queue: i64) -> CommitLogRecoveryCompletion {
        let summary = self.summary();
        CommitLogRecoveryCompletion::Recovered {
            confirm_offset: signed_offset(summary.last_valid_offset),
            controller_confirm_offset: signed_offset(summary.confirm_valid_offset),
            process_offset: signed_offset(summary.truncate_offset),
            truncate_consume_queue: should_truncate_recovery_consume_queue(
                max_phy_offset_of_consume_queue,
                summary.truncate_offset,
            ),
        }
    }
}
