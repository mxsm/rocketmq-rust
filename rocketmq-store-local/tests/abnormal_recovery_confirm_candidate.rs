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

use rocketmq_store_local::commit_log::recovery::abnormal_confirm_candidate_end;
use rocketmq_store_local::commit_log::recovery::AbnormalRecoveryConfirmCandidateError;

#[test]
fn confirm_candidate_preserves_checked_offset_semantics() {
    assert_eq!(abnormal_confirm_candidate_end(7, 5), Ok(12));
    assert_eq!(abnormal_confirm_candidate_end(i64::MAX, 0), Ok(i64::MAX));
    assert_eq!(abnormal_confirm_candidate_end(0, i64::MAX as usize), Ok(i64::MAX));

    for offset in [-1, i64::MIN] {
        assert_eq!(
            abnormal_confirm_candidate_end(offset, 1),
            Err(AbnormalRecoveryConfirmCandidateError::NegativeCommitLogOffset { offset })
        );
    }
    assert_eq!(
        abnormal_confirm_candidate_end(i64::MAX, 1),
        Err(AbnormalRecoveryConfirmCandidateError::ConfirmCandidateOverflow {
            offset: i64::MAX,
            size: 1,
        })
    );
}

#[cfg(target_pointer_width = "64")]
#[test]
fn confirm_candidate_rejects_input_size_above_i64_max() {
    let size = (i64::MAX as usize) + 1;
    assert_eq!(
        abnormal_confirm_candidate_end(0, size),
        Err(AbnormalRecoveryConfirmCandidateError::InputSizeExceedsI64 { size })
    );
}

#[test]
fn confirm_candidate_errors_preserve_store_visible_messages() {
    let cases = [
        (
            AbnormalRecoveryConfirmCandidateError::NegativeCommitLogOffset { offset: -7 },
            "commitlog offset -7 is negative",
        ),
        (
            AbnormalRecoveryConfirmCandidateError::InputSizeExceedsI64 { size: 11 },
            "input frame size 11 exceeds i64::MAX",
        ),
        (
            AbnormalRecoveryConfirmCandidateError::ConfirmCandidateOverflow {
                offset: i64::MAX,
                size: 1,
            },
            "commitlog offset 9223372036854775807 plus input frame size 1 overflowed",
        ),
    ];

    for (error, expected) in cases {
        assert_eq!(error.to_string(), expected);
    }
}
