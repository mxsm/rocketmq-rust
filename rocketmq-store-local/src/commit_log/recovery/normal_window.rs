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

const DEFAULT_NORMAL_RECOVERY_COMMIT_LOG_FILES: usize = 3;

/// File window selected for normal CommitLog recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NormalRecoveryFileWindow {
    /// First mapped-file index included in recovery.
    pub start_index: usize,
    /// Effective configured file-count limit reported by Store diagnostics.
    pub file_count_limit: usize,
}

/// Plans the bounded mapped-file window used by normal CommitLog recovery.
#[inline]
pub fn plan_normal_recovery_file_window(
    mapped_file_count: usize,
    max_recovery_commit_log_files: usize,
) -> NormalRecoveryFileWindow {
    let file_count_limit = if max_recovery_commit_log_files == 0 {
        DEFAULT_NORMAL_RECOVERY_COMMIT_LOG_FILES
    } else {
        max_recovery_commit_log_files
    };
    NormalRecoveryFileWindow {
        start_index: mapped_file_count.saturating_sub(file_count_limit),
        file_count_limit,
    }
}
