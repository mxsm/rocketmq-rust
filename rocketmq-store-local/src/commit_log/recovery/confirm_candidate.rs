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

/// Checked-arithmetic failure while deriving an abnormal-recovery confirm candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AbnormalRecoveryConfirmCandidateError {
    /// The decoded CommitLog offset was negative.
    #[error("commitlog offset {offset} is negative")]
    NegativeCommitLogOffset {
        /// Invalid decoded CommitLog offset.
        offset: i64,
    },
    /// The raw input-frame size cannot be represented by the signed CommitLog offset type.
    #[error("input frame size {size} exceeds i64::MAX")]
    InputSizeExceedsI64 {
        /// Raw input-frame size.
        size: usize,
    },
    /// The decoded offset plus the raw input-frame size overflowed `i64`.
    #[error("commitlog offset {offset} plus input frame size {size} overflowed")]
    ConfirmCandidateOverflow {
        /// Non-negative decoded CommitLog offset.
        offset: i64,
        /// Raw input-frame size after checked conversion to `i64`.
        size: i64,
    },
}

/// Derives the absolute end offset used by abnormal-recovery confirm gating.
///
/// # Errors
///
/// Returns [`AbnormalRecoveryConfirmCandidateError`] when the decoded offset is negative, the raw
/// input-frame size cannot be represented by `i64`, or their sum overflows `i64`.
pub fn abnormal_confirm_candidate_end(
    commit_log_offset: i64,
    input_size: usize,
) -> Result<i64, AbnormalRecoveryConfirmCandidateError> {
    if commit_log_offset < 0 {
        return Err(AbnormalRecoveryConfirmCandidateError::NegativeCommitLogOffset {
            offset: commit_log_offset,
        });
    }
    let input_size = i64::try_from(input_size)
        .map_err(|_| AbnormalRecoveryConfirmCandidateError::InputSizeExceedsI64 { size: input_size })?;
    commit_log_offset
        .checked_add(input_size)
        .ok_or(AbnormalRecoveryConfirmCandidateError::ConfirmCandidateOverflow {
            offset: commit_log_offset,
            size: input_size,
        })
}
