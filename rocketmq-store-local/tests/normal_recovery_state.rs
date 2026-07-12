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

use rocketmq_store_local::commit_log::recovery::NormalRecoveryAction;
use rocketmq_store_local::commit_log::recovery::NormalRecoveryEvent;
use rocketmq_store_local::commit_log::recovery::NormalRecoveryOffsetError;
use rocketmq_store_local::commit_log::recovery::NormalRecoveryPolicy;
use rocketmq_store_local::commit_log::recovery::NormalRecoveryState;
use rocketmq_store_local::commit_log::recovery::NormalRecoverySummary;

const I64_MAX_U64: u64 = 9_223_372_036_854_775_807;

fn state(policy: NormalRecoveryPolicy, initial_offset: u64) -> NormalRecoveryState {
    NormalRecoveryState::new(initial_offset, policy)
}

#[test]
fn policy_event_matrix_is_stable() {
    let cases = [
        (
            NormalRecoveryPolicy::Standard,
            NormalRecoveryEvent::MessageAccepted {
                segment_base: 100,
                relative_start: 7,
                size: 11,
            },
            NormalRecoveryAction::ContinueRecord,
            NormalRecoverySummary {
                last_valid_offset: 107,
                truncate_offset: 118,
            },
        ),
        (
            NormalRecoveryPolicy::Standard,
            NormalRecoveryEvent::Blank,
            NormalRecoveryAction::ContinueNextSegment,
            NormalRecoverySummary {
                last_valid_offset: 5,
                truncate_offset: 5,
            },
        ),
        (
            NormalRecoveryPolicy::Standard,
            NormalRecoveryEvent::InvalidRecord,
            NormalRecoveryAction::StopRecovery,
            NormalRecoverySummary {
                last_valid_offset: 5,
                truncate_offset: 5,
            },
        ),
        (
            NormalRecoveryPolicy::Standard,
            NormalRecoveryEvent::SourceEnded,
            NormalRecoveryAction::StopRecovery,
            NormalRecoverySummary {
                last_valid_offset: 5,
                truncate_offset: 5,
            },
        ),
        (
            NormalRecoveryPolicy::Optimized,
            NormalRecoveryEvent::MessageAccepted {
                segment_base: 100,
                relative_start: 7,
                size: 11,
            },
            NormalRecoveryAction::ContinueRecord,
            NormalRecoverySummary {
                last_valid_offset: 118,
                truncate_offset: 118,
            },
        ),
        (
            NormalRecoveryPolicy::Optimized,
            NormalRecoveryEvent::Blank,
            NormalRecoveryAction::ContinueNextSegment,
            NormalRecoverySummary {
                last_valid_offset: 5,
                truncate_offset: 5,
            },
        ),
        (
            NormalRecoveryPolicy::Optimized,
            NormalRecoveryEvent::InvalidRecord,
            NormalRecoveryAction::ContinueNextSegment,
            NormalRecoverySummary {
                last_valid_offset: 5,
                truncate_offset: 5,
            },
        ),
        (
            NormalRecoveryPolicy::Optimized,
            NormalRecoveryEvent::SourceEnded,
            NormalRecoveryAction::ContinueNextSegment,
            NormalRecoverySummary {
                last_valid_offset: 5,
                truncate_offset: 5,
            },
        ),
    ];

    for (policy, event, expected_action, expected_summary) in cases {
        let mut recovery = state(policy, 5);
        assert_eq!(recovery.apply(event), Ok(expected_action));
        assert_eq!(recovery.summary(), expected_summary);
    }
}

#[test]
fn consecutive_messages_preserve_start_end_policy() {
    for (policy, expected) in [
        (
            NormalRecoveryPolicy::Standard,
            NormalRecoverySummary {
                last_valid_offset: 110,
                truncate_offset: 117,
            },
        ),
        (
            NormalRecoveryPolicy::Optimized,
            NormalRecoverySummary {
                last_valid_offset: 117,
                truncate_offset: 117,
            },
        ),
    ] {
        let mut recovery = state(policy, 3);
        assert_eq!(
            recovery.apply(NormalRecoveryEvent::MessageAccepted {
                segment_base: 100,
                relative_start: 0,
                size: 10,
            }),
            Ok(NormalRecoveryAction::ContinueRecord)
        );
        assert_eq!(
            recovery.apply(NormalRecoveryEvent::MessageAccepted {
                segment_base: 100,
                relative_start: 10,
                size: 7,
            }),
            Ok(NormalRecoveryAction::ContinueRecord)
        );
        assert_eq!(recovery.summary(), expected);
    }
}

#[test]
fn first_empty_segment_preserves_policy_watermarks() {
    let mut standard = state(NormalRecoveryPolicy::Standard, 11);
    assert_eq!(
        standard.apply(NormalRecoveryEvent::SegmentStarted { base_offset: 100 }),
        Ok(NormalRecoveryAction::ContinueRecord)
    );
    assert_eq!(
        standard.apply(NormalRecoveryEvent::SourceEnded),
        Ok(NormalRecoveryAction::StopRecovery)
    );
    assert_eq!(
        standard.summary(),
        NormalRecoverySummary {
            last_valid_offset: 11,
            truncate_offset: 100,
        }
    );

    let mut optimized = state(NormalRecoveryPolicy::Optimized, 11);
    assert_eq!(
        optimized.apply(NormalRecoveryEvent::SegmentStarted { base_offset: 100 }),
        Ok(NormalRecoveryAction::ContinueRecord)
    );
    assert_eq!(
        optimized.apply(NormalRecoveryEvent::SourceEnded),
        Ok(NormalRecoveryAction::ContinueNextSegment)
    );
    assert_eq!(
        optimized.summary(),
        NormalRecoverySummary {
            last_valid_offset: 11,
            truncate_offset: 11,
        }
    );
}

#[test]
fn blank_then_empty_next_segment_preserves_last_valid_offset() {
    let mut standard = state(NormalRecoveryPolicy::Standard, 9);
    assert_eq!(
        standard.apply(NormalRecoveryEvent::Blank),
        Ok(NormalRecoveryAction::ContinueNextSegment)
    );
    assert_eq!(
        standard.apply(NormalRecoveryEvent::SegmentStarted { base_offset: 200 }),
        Ok(NormalRecoveryAction::ContinueRecord)
    );
    assert_eq!(
        standard.apply(NormalRecoveryEvent::SourceEnded),
        Ok(NormalRecoveryAction::StopRecovery)
    );
    assert_eq!(
        standard.summary(),
        NormalRecoverySummary {
            last_valid_offset: 9,
            truncate_offset: 200,
        }
    );

    let mut optimized = state(NormalRecoveryPolicy::Optimized, 9);
    assert_eq!(
        optimized.apply(NormalRecoveryEvent::Blank),
        Ok(NormalRecoveryAction::ContinueNextSegment)
    );
    assert_eq!(
        optimized.apply(NormalRecoveryEvent::SegmentStarted { base_offset: 200 }),
        Ok(NormalRecoveryAction::ContinueRecord)
    );
    assert_eq!(
        optimized.apply(NormalRecoveryEvent::SourceEnded),
        Ok(NormalRecoveryAction::ContinueNextSegment)
    );
    assert_eq!(
        optimized.summary(),
        NormalRecoverySummary {
            last_valid_offset: 9,
            truncate_offset: 9,
        }
    );
}

#[test]
fn invalid_first_segment_stops_standard_but_optimized_accepts_second() {
    let mut standard = state(NormalRecoveryPolicy::Standard, 0);
    assert_eq!(
        standard.apply(NormalRecoveryEvent::InvalidRecord),
        Ok(NormalRecoveryAction::StopRecovery)
    );
    assert_eq!(
        standard.summary(),
        NormalRecoverySummary {
            last_valid_offset: 0,
            truncate_offset: 0,
        }
    );

    let mut optimized = state(NormalRecoveryPolicy::Optimized, 0);
    assert_eq!(
        optimized.apply(NormalRecoveryEvent::InvalidRecord),
        Ok(NormalRecoveryAction::ContinueNextSegment)
    );
    assert_eq!(
        optimized.apply(NormalRecoveryEvent::SegmentStarted { base_offset: 100 }),
        Ok(NormalRecoveryAction::ContinueRecord)
    );
    assert_eq!(
        optimized.apply(NormalRecoveryEvent::MessageAccepted {
            segment_base: 100,
            relative_start: 0,
            size: 8,
        }),
        Ok(NormalRecoveryAction::ContinueRecord)
    );
    assert_eq!(
        optimized.summary(),
        NormalRecoverySummary {
            last_valid_offset: 108,
            truncate_offset: 108,
        }
    );
}

#[test]
fn initial_confirm_and_last_blank_are_preserved() {
    for policy in [NormalRecoveryPolicy::Standard, NormalRecoveryPolicy::Optimized] {
        let mut recovery = state(policy, 123);
        assert_eq!(
            recovery.summary(),
            NormalRecoverySummary {
                last_valid_offset: 123,
                truncate_offset: 123,
            }
        );
        assert_eq!(
            recovery.apply(NormalRecoveryEvent::MessageAccepted {
                segment_base: 200,
                relative_start: 0,
                size: 8,
            }),
            Ok(NormalRecoveryAction::ContinueRecord)
        );
        let before_blank = recovery.summary();
        assert_eq!(
            recovery.apply(NormalRecoveryEvent::Blank),
            Ok(NormalRecoveryAction::ContinueNextSegment)
        );
        assert_eq!(recovery.summary(), before_blank);
    }
}

#[test]
fn checked_offset_errors_are_transactional_for_both_policies() {
    for policy in [NormalRecoveryPolicy::Standard, NormalRecoveryPolicy::Optimized] {
        let cases = [
            (
                NormalRecoveryEvent::MessageAccepted {
                    segment_base: u64::MAX,
                    relative_start: 1,
                    size: 0,
                },
                NormalRecoveryOffsetError::BaseRelativeOverflow {
                    base_offset: u64::MAX,
                    relative_start: 1,
                },
            ),
            (
                NormalRecoveryEvent::MessageAccepted {
                    segment_base: 0,
                    relative_start: u64::MAX,
                    size: 1,
                },
                NormalRecoveryOffsetError::MessageEndOverflow {
                    start_offset: u64::MAX,
                    size: 1,
                },
            ),
            (
                NormalRecoveryEvent::MessageAccepted {
                    segment_base: I64_MAX_U64,
                    relative_start: 0,
                    size: 1,
                },
                NormalRecoveryOffsetError::OffsetExceedsI64 {
                    offset: I64_MAX_U64 + 1,
                },
            ),
        ];

        for (event, expected_error) in cases {
            let mut recovery = state(policy, 17);
            let before = recovery.summary();
            assert_eq!(recovery.apply(event), Err(expected_error));
            assert_eq!(recovery.summary(), before);
        }
    }
}

#[test]
fn i64_upper_bound_is_accepted() {
    for policy in [NormalRecoveryPolicy::Standard, NormalRecoveryPolicy::Optimized] {
        let mut recovery = state(policy, 0);
        assert_eq!(
            recovery.apply(NormalRecoveryEvent::MessageAccepted {
                segment_base: I64_MAX_U64 - 1,
                relative_start: 0,
                size: 1,
            }),
            Ok(NormalRecoveryAction::ContinueRecord)
        );
        assert_eq!(recovery.summary().truncate_offset, I64_MAX_U64);
    }
}
