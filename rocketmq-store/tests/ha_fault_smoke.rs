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

//! Deterministic, process-level HA decision and recovery smoke tests.

use rocketmq_store_api::decide_replication;
use rocketmq_store_api::AckPolicy;
use rocketmq_store_api::AppendReceipt;
use rocketmq_store_api::AppendReceiptError;
use rocketmq_store_api::AppendStatus;
use rocketmq_store_api::Durability;
use rocketmq_store_api::HaRejectReason;
use rocketmq_store_api::MasterEpoch;
use rocketmq_store_api::ReplicaAck;
use rocketmq_store_api::ReplicaCount;
use rocketmq_store_api::ReplicationDecision;
use rocketmq_store_api::ReplicationObservation;
use rocketmq_store_api::SyncStateSet;
use rocketmq_store_api::WriteAuthority;
use rocketmq_store_local::commit_log::recovery::AbnormalRecoveryAction;
use rocketmq_store_local::commit_log::recovery::AbnormalRecoveryDispatchGate;
use rocketmq_store_local::commit_log::recovery::AbnormalRecoveryEvent;
use rocketmq_store_local::commit_log::recovery::AbnormalRecoveryPolicy;
use rocketmq_store_local::commit_log::recovery::AbnormalRecoveryState;

fn authority(broker_id: i64, epoch: i32) -> WriteAuthority {
    WriteAuthority::try_new(broker_id, MasterEpoch::try_from(epoch).expect("positive epoch"))
        .expect("non-negative broker id")
}

fn decision(
    current: WriteAuthority,
    requested: WriteAuthority,
    policy: AckPolicy,
    local_durable_watermark: i64,
    replica_acks: Vec<ReplicaAck>,
    members: &[i64],
) -> ReplicationDecision {
    let observation = ReplicationObservation::try_new(
        current,
        requested,
        policy,
        128,
        local_durable_watermark,
        replica_acks,
        SyncStateSet::try_new(members.iter().copied()).expect("non-empty ISR"),
    )
    .expect("valid fault observation");
    decide_replication(&observation)
}

#[test]
fn stopped_replica_before_ack_never_produces_replicated_success() {
    let current = authority(1, 9);
    assert_eq!(
        decision(
            current,
            current,
            AckPolicy::ReplicaCount(ReplicaCount::try_new(2).expect("replicated policy")),
            128,
            Vec::new(),
            &[1, 2],
        ),
        ReplicationDecision::Wait { required_offset: 128 }
    );
}

#[test]
fn stale_master_write_is_fenced_after_new_epoch_is_installed() {
    assert_eq!(
        decision(
            authority(2, 10),
            authority(1, 9),
            AckPolicy::LocalDurable,
            128,
            Vec::new(),
            &[1, 2],
        ),
        ReplicationDecision::Reject(HaRejectReason::StaleAuthority)
    );
}

#[test]
fn unavailable_controller_cannot_pre_authorize_competing_future_leaders() {
    let installed = authority(1, 10);
    for proposed in [authority(1, 11), authority(2, 11)] {
        assert_eq!(
            decision(installed, proposed, AckPolicy::LocalDurable, 128, Vec::new(), &[1, 2],),
            ReplicationDecision::Reject(HaRejectReason::AuthorityMismatch)
        );
    }
}

#[test]
fn slow_duplicate_and_removed_replica_acks_do_not_form_a_quorum() {
    let current = authority(1, 10);
    let policy = AckPolicy::ReplicaCount(ReplicaCount::try_new(3).expect("replicated policy"));
    let acks = vec![
        ReplicaAck::try_new(2, 128).expect("replica ACK"),
        ReplicaAck::try_new(2, 128).expect("duplicate ACK"),
        ReplicaAck::try_new(3, 127).expect("slow ACK"),
        ReplicaAck::try_new(4, 128).expect("removed replica ACK"),
    ];

    assert_eq!(
        decision(current, current, policy, 128, acks, &[1, 2, 3]),
        ReplicationDecision::Wait { required_offset: 128 }
    );
}

#[test]
fn local_fsync_failure_cannot_be_reported_as_replicated_durability() {
    let current = authority(1, 10);
    let replica_ack = ReplicaAck::try_new(2, 128).expect("replica ACK");
    assert_eq!(
        decision(
            current,
            current,
            AckPolicy::ReplicaCount(ReplicaCount::try_new(2).expect("replicated policy")),
            127,
            vec![replica_ack],
            &[1, 2],
        ),
        ReplicationDecision::Wait { required_offset: 128 }
    );
    assert_eq!(
        AppendReceipt::try_new(AppendStatus::PutOk, 0..128, 128, 127, Durability::Replicated)
            .expect_err("local fsync failure cannot produce a replicated receipt"),
        AppendReceiptError::ReplicatedDurabilityRequiresDecision
    );
}

#[test]
fn truncated_segment_tail_recovers_only_the_last_complete_record() {
    let mut recovery =
        AbnormalRecoveryState::try_new(0, AbnormalRecoveryPolicy::Optimized).expect("valid recovery state");
    recovery
        .apply(AbnormalRecoveryEvent::SegmentStarted { base_offset: 0 })
        .expect("start segment");
    assert_eq!(
        recovery
            .apply(AbnormalRecoveryEvent::MessageAccepted {
                segment_base: 0,
                relative_start: 0,
                validated_size: 64,
                confirm_candidate_end: 64,
                dispatch_gate: AbnormalRecoveryDispatchGate::Ungated,
            })
            .expect("accept complete record"),
        AbnormalRecoveryAction::DispatchMessage
    );
    assert_eq!(
        recovery
            .apply(AbnormalRecoveryEvent::InvalidRecord)
            .expect("reject truncated tail"),
        AbnormalRecoveryAction::ContinueNextSegment
    );
    assert_eq!(recovery.summary().truncate_offset, 64);
}

#[test]
fn restored_fixture_accepts_a_new_complete_record_after_recovery() {
    let mut restored = AbnormalRecoveryState::try_new(64, AbnormalRecoveryPolicy::Optimized)
        .expect("restore complete-record watermark");
    restored
        .apply(AbnormalRecoveryEvent::SegmentStarted { base_offset: 64 })
        .expect("start restored segment");
    assert_eq!(
        restored
            .apply(AbnormalRecoveryEvent::MessageAccepted {
                segment_base: 64,
                relative_start: 0,
                validated_size: 32,
                confirm_candidate_end: 96,
                dispatch_gate: AbnormalRecoveryDispatchGate::Ungated,
            })
            .expect("append complete record after restore"),
        AbnormalRecoveryAction::DispatchMessage
    );
    assert_eq!(restored.summary().truncate_offset, 96);
}
