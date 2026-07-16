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

//! Runtime-neutral outer route selection for CommitLog recovery.

/// One CommitLog recovery implementation requested from the Store adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitLogRecoveryStep {
    /// Run the optimized recovery implementation.
    Optimized,
    /// Run the compatibility recovery implementation.
    Standard,
}

/// Preserves the legacy optimized-recovery environment value semantics.
///
/// A missing or malformed value enables optimized recovery. Only the exact value `false` selects
/// the standard implementation because Rust's boolean parser is intentionally case-sensitive.
pub fn optimized_recovery_requested(value: Option<&str>) -> bool {
    value.and_then(|value| value.parse::<bool>().ok()).unwrap_or(true)
}

/// Selects exactly one recovery implementation and returns the adapter output unchanged.
pub fn drive_commit_log_recovery<Execute, Output>(use_optimized: bool, execute: Execute) -> Output
where
    Execute: FnOnce(CommitLogRecoveryStep) -> Output,
{
    execute(if use_optimized {
        CommitLogRecoveryStep::Optimized
    } else {
        CommitLogRecoveryStep::Standard
    })
}
