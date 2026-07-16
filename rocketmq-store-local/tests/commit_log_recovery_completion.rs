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

use rocketmq_store_local::commit_log::recovery::AbnormalRecoveryDispatchGate;
use rocketmq_store_local::commit_log::recovery::AbnormalRecoveryEvent;
use rocketmq_store_local::commit_log::recovery::AbnormalRecoveryPolicy;
use rocketmq_store_local::commit_log::recovery::AbnormalRecoveryState;
use rocketmq_store_local::commit_log::recovery::CommitLogRecoveryCompletion;
use rocketmq_store_local::commit_log::recovery::NormalRecoveryEvent;
use rocketmq_store_local::commit_log::recovery::NormalRecoveryPolicy;
use rocketmq_store_local::commit_log::recovery::NormalRecoveryState;

#[test]
fn normal_completion_preserves_policy_specific_controller_boundaries() {
    let mut standard = NormalRecoveryState::try_new(10, NormalRecoveryPolicy::Standard).unwrap();
    standard
        .apply(NormalRecoveryEvent::SegmentStarted { base_offset: 20 })
        .unwrap();
    standard
        .apply(NormalRecoveryEvent::MessageAccepted {
            segment_base: 20,
            relative_start: 2,
            size: 3,
        })
        .unwrap();
    assert_eq!(
        standard.completion(25),
        CommitLogRecoveryCompletion::Recovered {
            confirm_offset: 22,
            controller_confirm_offset: 25,
            process_offset: 25,
            truncate_consume_queue: true,
        }
    );

    let mut optimized = NormalRecoveryState::try_new(10, NormalRecoveryPolicy::Optimized).unwrap();
    optimized
        .apply(NormalRecoveryEvent::SegmentStarted { base_offset: 20 })
        .unwrap();
    optimized
        .apply(NormalRecoveryEvent::MessageAccepted {
            segment_base: 20,
            relative_start: 2,
            size: 3,
        })
        .unwrap();
    assert_eq!(
        optimized.completion(24),
        CommitLogRecoveryCompletion::Recovered {
            confirm_offset: 25,
            controller_confirm_offset: 25,
            process_offset: 25,
            truncate_consume_queue: false,
        }
    );
}

#[test]
fn abnormal_completion_preserves_confirm_and_truncation_watermarks() {
    let mut standard = AbnormalRecoveryState::try_new(10, AbnormalRecoveryPolicy::Standard).unwrap();
    standard
        .apply(AbnormalRecoveryEvent::SegmentStarted { base_offset: 20 })
        .unwrap();
    standard
        .apply(AbnormalRecoveryEvent::MessageAccepted {
            segment_base: 20,
            relative_start: 2,
            validated_size: 3,
            confirm_candidate_end: 99,
            dispatch_gate: AbnormalRecoveryDispatchGate::Ungated,
        })
        .unwrap();
    assert_eq!(
        standard.completion(-1),
        CommitLogRecoveryCompletion::Recovered {
            confirm_offset: 20,
            controller_confirm_offset: 10,
            process_offset: 23,
            truncate_consume_queue: true,
        }
    );

    let mut optimized = AbnormalRecoveryState::try_new(10, AbnormalRecoveryPolicy::Optimized).unwrap();
    optimized
        .apply(AbnormalRecoveryEvent::SegmentStarted { base_offset: 20 })
        .unwrap();
    optimized
        .apply(AbnormalRecoveryEvent::MessageAccepted {
            segment_base: 20,
            relative_start: 2,
            validated_size: 3,
            confirm_candidate_end: 99,
            dispatch_gate: AbnormalRecoveryDispatchGate::Ungated,
        })
        .unwrap();
    assert_eq!(
        optimized.completion(22),
        CommitLogRecoveryCompletion::Recovered {
            confirm_offset: 25,
            controller_confirm_offset: 99,
            process_offset: 25,
            truncate_consume_queue: false,
        }
    );
}
