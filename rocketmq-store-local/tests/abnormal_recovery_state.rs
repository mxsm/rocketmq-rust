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

use rocketmq_store_local::commit_log::recovery::AbnormalRecoveryAction;
use rocketmq_store_local::commit_log::recovery::AbnormalRecoveryDispatchGate;
use rocketmq_store_local::commit_log::recovery::AbnormalRecoveryEvent;
use rocketmq_store_local::commit_log::recovery::AbnormalRecoveryOffsetError;
use rocketmq_store_local::commit_log::recovery::AbnormalRecoveryPolicy;
use rocketmq_store_local::commit_log::recovery::AbnormalRecoveryState;
use rocketmq_store_local::commit_log::recovery::AbnormalRecoverySummary;

const I64_MAX_U64: u64 = 9_223_372_036_854_775_807;

fn state(policy: AbnormalRecoveryPolicy, initial_offset: u64) -> AbnormalRecoveryState {
    AbnormalRecoveryState::try_new(initial_offset, policy).expect("test recovery offset must fit i64")
}

fn message(
    relative_start: u64,
    validated_size: u64,
    confirm_candidate_end: i64,
    dispatch_gate: AbnormalRecoveryDispatchGate,
) -> AbnormalRecoveryEvent {
    AbnormalRecoveryEvent::MessageAccepted {
        segment_base: 100,
        relative_start,
        validated_size,
        confirm_candidate_end,
        dispatch_gate,
    }
}

#[test]
fn constructor_freezes_standard_and_windowed_optimized_seed_model() {
    let standard = state(AbnormalRecoveryPolicy::Standard, 512);
    let optimized_first = state(AbnormalRecoveryPolicy::Optimized, 512);
    let optimized_window = state(AbnormalRecoveryPolicy::Optimized, 0);

    for recovery in [standard, optimized_first] {
        assert_eq!(
            recovery.summary(),
            AbnormalRecoverySummary {
                last_valid_offset: 512,
                confirm_valid_offset: 512,
                truncate_offset: 512,
            }
        );
    }
    assert_eq!(
        optimized_window.summary(),
        AbnormalRecoverySummary {
            last_valid_offset: 0,
            confirm_valid_offset: 0,
            truncate_offset: 0,
        }
    );

    assert_eq!(
        AbnormalRecoveryState::try_new(I64_MAX_U64 + 1, AbnormalRecoveryPolicy::Standard),
        Err(AbnormalRecoveryOffsetError::OffsetExceedsI64 {
            offset: I64_MAX_U64 + 1,
        })
    );
    assert!(AbnormalRecoveryState::try_new(I64_MAX_U64, AbnormalRecoveryPolicy::Optimized).is_ok());
}

#[test]
fn segment_start_preserves_policy_specific_watermarks() {
    let mut standard = state(AbnormalRecoveryPolicy::Standard, 5);
    assert_eq!(
        standard.apply(AbnormalRecoveryEvent::SegmentStarted { base_offset: 100 }),
        Ok(AbnormalRecoveryAction::ContinueRecord)
    );
    assert_eq!(
        standard.summary(),
        AbnormalRecoverySummary {
            last_valid_offset: 5,
            confirm_valid_offset: 5,
            truncate_offset: 100,
        }
    );

    let mut optimized = state(AbnormalRecoveryPolicy::Optimized, 5);
    assert_eq!(
        optimized.apply(AbnormalRecoveryEvent::SegmentStarted { base_offset: 100 }),
        Ok(AbnormalRecoveryAction::ContinueRecord)
    );
    assert_eq!(
        optimized.summary(),
        AbnormalRecoverySummary {
            last_valid_offset: 5,
            confirm_valid_offset: 5,
            truncate_offset: 5,
        }
    );
}

#[test]
fn message_gate_matrix_keeps_input_candidate_distinct_from_validated_size() {
    let cases = [
        (
            AbnormalRecoveryPolicy::Standard,
            AbnormalRecoveryDispatchGate::Ungated,
            AbnormalRecoveryAction::DispatchMessage,
            10,
            100,
            107,
        ),
        (
            AbnormalRecoveryPolicy::Standard,
            AbnormalRecoveryDispatchGate::ConfirmBounded { confirm_offset: 149 },
            AbnormalRecoveryAction::SkipMessageDispatch,
            10,
            100,
            107,
        ),
        (
            AbnormalRecoveryPolicy::Standard,
            AbnormalRecoveryDispatchGate::ConfirmBounded { confirm_offset: 150 },
            AbnormalRecoveryAction::DispatchMessage,
            150,
            100,
            107,
        ),
        (
            AbnormalRecoveryPolicy::Optimized,
            AbnormalRecoveryDispatchGate::Ungated,
            AbnormalRecoveryAction::DispatchMessage,
            150,
            147,
            147,
        ),
        (
            AbnormalRecoveryPolicy::Optimized,
            AbnormalRecoveryDispatchGate::ConfirmBounded { confirm_offset: 149 },
            AbnormalRecoveryAction::SkipMessageDispatch,
            10,
            147,
            147,
        ),
        (
            AbnormalRecoveryPolicy::Optimized,
            AbnormalRecoveryDispatchGate::ConfirmBounded { confirm_offset: 150 },
            AbnormalRecoveryAction::DispatchMessage,
            150,
            147,
            147,
        ),
    ];

    for (policy, gate, expected_action, expected_confirm, expected_last, expected_truncate) in cases {
        let mut recovery = state(policy, 10);
        recovery
            .apply(AbnormalRecoveryEvent::SegmentStarted { base_offset: 100 })
            .expect("segment start must succeed");
        assert_eq!(recovery.apply(message(40, 7, 150, gate)), Ok(expected_action));
        assert_eq!(
            recovery.summary(),
            AbnormalRecoverySummary {
                last_valid_offset: expected_last,
                confirm_valid_offset: expected_confirm,
                truncate_offset: expected_truncate,
            }
        );
    }
}

#[test]
fn standard_uses_validated_accumulator_while_optimized_uses_relative_start() {
    let mut standard = state(AbnormalRecoveryPolicy::Standard, 100);
    standard
        .apply(AbnormalRecoveryEvent::SegmentStarted { base_offset: 100 })
        .expect("segment start must succeed");
    assert_eq!(
        standard.apply(message(30, 7, 37, AbnormalRecoveryDispatchGate::Ungated)),
        Ok(AbnormalRecoveryAction::DispatchMessage)
    );
    assert_eq!(
        standard.apply(message(200, 5, 205, AbnormalRecoveryDispatchGate::Ungated)),
        Ok(AbnormalRecoveryAction::DispatchMessage)
    );
    assert_eq!(
        standard.summary(),
        AbnormalRecoverySummary {
            last_valid_offset: 107,
            confirm_valid_offset: 100,
            truncate_offset: 112,
        }
    );

    let mut optimized = state(AbnormalRecoveryPolicy::Optimized, 100);
    assert_eq!(
        optimized.apply(message(30, 7, 137, AbnormalRecoveryDispatchGate::Ungated)),
        Ok(AbnormalRecoveryAction::DispatchMessage)
    );
    assert_eq!(
        optimized.apply(message(200, 5, 305, AbnormalRecoveryDispatchGate::Ungated)),
        Ok(AbnormalRecoveryAction::DispatchMessage)
    );
    assert_eq!(
        optimized.summary(),
        AbnormalRecoverySummary {
            last_valid_offset: 305,
            confirm_valid_offset: 305,
            truncate_offset: 305,
        }
    );
}

#[test]
fn fresh_confirm_limit_is_applied_for_every_message() {
    for policy in [AbnormalRecoveryPolicy::Standard, AbnormalRecoveryPolicy::Optimized] {
        let mut recovery = state(policy, 100);
        recovery
            .apply(AbnormalRecoveryEvent::SegmentStarted { base_offset: 100 })
            .expect("segment start must succeed");
        assert_eq!(
            recovery.apply(message(
                0,
                10,
                110,
                AbnormalRecoveryDispatchGate::ConfirmBounded { confirm_offset: 110 },
            )),
            Ok(AbnormalRecoveryAction::DispatchMessage)
        );
        assert_eq!(
            recovery.apply(message(
                10,
                10,
                120,
                AbnormalRecoveryDispatchGate::ConfirmBounded { confirm_offset: 119 },
            )),
            Ok(AbnormalRecoveryAction::SkipMessageDispatch)
        );
        assert_eq!(recovery.summary().confirm_valid_offset, 110);
    }
}

#[test]
fn blank_invalid_and_source_end_action_matrix_is_stable() {
    for policy in [AbnormalRecoveryPolicy::Standard, AbnormalRecoveryPolicy::Optimized] {
        let expected_terminal = match policy {
            AbnormalRecoveryPolicy::Standard => AbnormalRecoveryAction::StopRecovery,
            AbnormalRecoveryPolicy::Optimized => AbnormalRecoveryAction::ContinueNextSegment,
        };
        for event in [AbnormalRecoveryEvent::InvalidRecord, AbnormalRecoveryEvent::SourceEnded] {
            let mut recovery = state(policy, 17);
            assert_eq!(recovery.apply(event), Ok(expected_terminal));
            assert_eq!(
                recovery.summary(),
                AbnormalRecoverySummary {
                    last_valid_offset: 17,
                    confirm_valid_offset: 17,
                    truncate_offset: 17,
                }
            );
        }

        let mut recovery = state(policy, 17);
        assert_eq!(
            recovery.apply(AbnormalRecoveryEvent::Blank),
            Ok(AbnormalRecoveryAction::NotifyFileEndAndContinueNextSegment)
        );
        assert_eq!(
            recovery.summary(),
            AbnormalRecoverySummary {
                last_valid_offset: 17,
                confirm_valid_offset: 17,
                truncate_offset: 17,
            }
        );
    }
}

#[test]
fn offset_errors_are_transactional_for_both_policies() {
    for policy in [AbnormalRecoveryPolicy::Standard, AbnormalRecoveryPolicy::Optimized] {
        let mut cases = vec![
            (
                AbnormalRecoveryEvent::MessageAccepted {
                    segment_base: I64_MAX_U64,
                    relative_start: 0,
                    validated_size: 1,
                    confirm_candidate_end: 0,
                    dispatch_gate: AbnormalRecoveryDispatchGate::Ungated,
                },
                AbnormalRecoveryOffsetError::OffsetExceedsI64 {
                    offset: I64_MAX_U64 + 1,
                },
            ),
            (
                message(0, 1, -1, AbnormalRecoveryDispatchGate::Ungated),
                AbnormalRecoveryOffsetError::NegativeConfirmCandidate { candidate: -1 },
            ),
        ];
        if policy == AbnormalRecoveryPolicy::Optimized {
            cases.insert(
                0,
                (
                    AbnormalRecoveryEvent::MessageAccepted {
                        segment_base: u64::MAX,
                        relative_start: 1,
                        validated_size: 0,
                        confirm_candidate_end: 0,
                        dispatch_gate: AbnormalRecoveryDispatchGate::Ungated,
                    },
                    AbnormalRecoveryOffsetError::BaseRelativeOverflow {
                        base_offset: u64::MAX,
                        relative_start: 1,
                    },
                ),
            );
        }

        for (event, expected_error) in cases {
            let mut recovery = state(policy, 23);
            if policy == AbnormalRecoveryPolicy::Standard {
                recovery
                    .apply(AbnormalRecoveryEvent::SegmentStarted {
                        base_offset: I64_MAX_U64,
                    })
                    .expect("segment start at i64::MAX must succeed");
            }
            let before = recovery.summary();
            assert_eq!(recovery.apply(event), Err(expected_error));
            assert_eq!(recovery.summary(), before);
        }
    }
}
