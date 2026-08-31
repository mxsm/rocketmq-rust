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

use rocketmq_store_api::decide_replication;
use rocketmq_store_api::AckPolicy;
use rocketmq_store_api::AppendReceipt;
use rocketmq_store_api::AppendStatus;
use rocketmq_store_api::Durability;
use rocketmq_store_api::HaRejectReason;
use rocketmq_store_api::MasterEpoch;
use rocketmq_store_api::ReplicaAck;
use rocketmq_store_api::ReplicaCount;
use rocketmq_store_api::ReplicationDecision;
use rocketmq_store_api::ReplicationObservation;
use rocketmq_store_api::StoreContractViolation;
use rocketmq_store_api::SyncStateSet;
use rocketmq_store_api::SyncStateSetEpoch;
use rocketmq_store_api::WriteAuthority;

fn authority(broker_id: i64, epoch: i32) -> WriteAuthority {
    WriteAuthority::try_new(broker_id, MasterEpoch::try_from(epoch).expect("positive epoch"))
        .expect("non-negative broker id")
}

fn observation(
    current: WriteAuthority,
    requested: WriteAuthority,
    policy: AckPolicy,
    local_durable_watermark: i64,
    replica_acks: Vec<ReplicaAck>,
    members: &[i64],
) -> ReplicationObservation {
    ReplicationObservation::try_new(
        current,
        requested,
        policy,
        100,
        local_durable_watermark,
        replica_acks,
        SyncStateSet::try_new(members.iter().copied()).expect("non-empty sync-state set"),
    )
    .expect("valid observation")
}

#[test]
fn epochs_and_authority_reject_invalid_values() {
    for epoch in [i32::MIN, -1, 0] {
        assert_eq!(
            Err(StoreContractViolation::HaInvalidMasterEpoch(epoch)),
            MasterEpoch::try_from(epoch)
        );
        assert_eq!(
            Err(StoreContractViolation::HaInvalidSyncStateSetEpoch(epoch)),
            SyncStateSetEpoch::try_from(epoch)
        );
    }

    assert_eq!(
        Err(StoreContractViolation::HaInvalidBrokerId(-1)),
        WriteAuthority::try_new(-1, MasterEpoch::try_from(1).expect("positive epoch"))
    );
}

#[test]
fn ack_policy_rejects_unknown_or_local_only_replica_counts() {
    assert_eq!(
        Err(StoreContractViolation::HaInvalidAckPolicy(0)),
        AckPolicy::try_from_legacy(0, -1)
    );
    assert_eq!(
        Err(StoreContractViolation::HaInvalidAckPolicy(-2)),
        AckPolicy::try_from_legacy(-2, -1)
    );
    assert_eq!(
        Err(StoreContractViolation::HaInvalidReplicaCount(1)),
        ReplicaCount::try_new(1)
    );
    assert_eq!(Ok(AckPolicy::AllInSyncSet), AckPolicy::try_from_legacy(-1, -1));
    assert_eq!(Ok(AckPolicy::LocalDurable), AckPolicy::try_from_legacy(1, -1));
}

#[test]
fn sync_state_set_is_non_empty_and_requires_the_current_leader() {
    assert_eq!(
        Err(StoreContractViolation::HaEmptySyncStateSet),
        SyncStateSet::try_new(std::iter::empty())
    );
    let current = authority(1, 7);
    assert_eq!(
        Err(StoreContractViolation::HaLeaderMissingFromSyncStateSet(1)),
        ReplicationObservation::try_new(
            current,
            current,
            AckPolicy::LocalDurable,
            100,
            100,
            Vec::new(),
            SyncStateSet::try_new([2]).expect("non-empty set"),
        )
    );
}

#[test]
fn stale_and_uninstalled_authorities_fail_closed() {
    let current = authority(1, 8);
    let stale = observation(current, authority(1, 7), AckPolicy::LocalDurable, 100, Vec::new(), &[1]);
    assert_eq!(
        ReplicationDecision::Reject(HaRejectReason::StaleAuthority),
        decide_replication(&stale)
    );

    for requested in [authority(2, 8), authority(1, 9)] {
        let not_installed = observation(current, requested, AckPolicy::LocalDurable, 100, Vec::new(), &[1, 2]);
        assert_eq!(
            ReplicationDecision::Reject(HaRejectReason::AuthorityMismatch),
            decide_replication(&not_installed)
        );
    }
}

#[test]
fn replicated_ack_requires_local_durability_before_replica_progress() {
    let current = authority(1, 8);
    let decision = decide_replication(&observation(
        current,
        current,
        AckPolicy::ReplicaCount(ReplicaCount::try_new(2).expect("replicated policy")),
        99,
        vec![ReplicaAck::try_new(2, 100).expect("replica ACK")],
        &[1, 2],
    ));

    assert_eq!(ReplicationDecision::Wait { required_offset: 100 }, decision);
}

#[test]
fn duplicate_slow_and_non_member_acks_never_satisfy_replica_policy() {
    let current = authority(1, 8);
    let policy = AckPolicy::ReplicaCount(ReplicaCount::try_new(3).expect("replicated policy"));
    for replica_acks in [
        vec![
            ReplicaAck::try_new(2, 100).expect("replica ACK"),
            ReplicaAck::try_new(2, 100).expect("duplicate replica ACK"),
        ],
        vec![
            ReplicaAck::try_new(2, 100).expect("replica ACK"),
            ReplicaAck::try_new(3, 99).expect("slow replica ACK"),
        ],
        vec![
            ReplicaAck::try_new(2, 100).expect("replica ACK"),
            ReplicaAck::try_new(4, 100).expect("non-member ACK"),
        ],
    ] {
        assert_eq!(
            ReplicationDecision::Wait { required_offset: 100 },
            decide_replication(&observation(current, current, policy, 100, replica_acks, &[1, 2, 3]))
        );
    }
}

#[test]
fn missing_members_and_progress_wait_without_downgrade() {
    let current = authority(1, 8);
    let policy = AckPolicy::ReplicaCount(ReplicaCount::try_new(3).expect("replicated policy"));
    assert_eq!(
        ReplicationDecision::Wait { required_offset: 100 },
        decide_replication(&observation(current, current, policy, 100, Vec::new(), &[1, 2]))
    );

    assert_eq!(
        ReplicationDecision::Wait { required_offset: 100 },
        decide_replication(&observation(
            current,
            current,
            AckPolicy::AllInSyncSet,
            100,
            vec![ReplicaAck::try_new(2, 100).expect("replica ACK")],
            &[1, 2, 3],
        ))
    );
}

#[test]
fn acknowledgement_durability_matches_the_policy_and_membership() {
    let current = authority(1, 8);
    let local = decide_replication(&observation(
        current,
        current,
        AckPolicy::AllInSyncSet,
        100,
        Vec::new(),
        &[1],
    ));
    let replicated = decide_replication(&observation(
        current,
        current,
        AckPolicy::AllInSyncSet,
        100,
        vec![ReplicaAck::try_new(2, 100).expect("replica ACK")],
        &[1, 2],
    ));

    assert!(matches!(
        local,
        ReplicationDecision::Acknowledge(ack) if ack.durability() == Durability::Local
    ));
    assert!(matches!(
        replicated,
        ReplicationDecision::Acknowledge(ack) if ack.durability() == Durability::Replicated
    ));

    let ReplicationDecision::Acknowledge(acknowledgement) = replicated else {
        panic!("replica policy should produce an acknowledgement");
    };
    let receipt = AppendReceipt::try_new_with_replication(AppendStatus::PutOk, 0..100, 100, 100, acknowledgement)
        .expect("canonical decision should authorize a replicated receipt");
    assert_eq!(receipt.durability(), Durability::Replicated);
}

#[test]
fn replica_order_does_not_change_the_decision() {
    let current = authority(1, 8);
    let policy = AckPolicy::ReplicaCount(ReplicaCount::try_new(3).expect("replicated policy"));
    let permutations = [
        [(2, 100), (3, 100), (4, 99)],
        [(4, 99), (2, 100), (3, 100)],
        [(3, 100), (4, 99), (2, 100)],
    ];

    for permutation in permutations {
        let acks = permutation
            .into_iter()
            .map(|(broker_id, offset)| ReplicaAck::try_new(broker_id, offset).expect("replica ACK"))
            .collect();
        assert!(matches!(
            decide_replication(&observation(current, current, policy, 100, acks, &[1, 2, 3, 4])),
            ReplicationDecision::Acknowledge(ack) if ack.durability() == Durability::Replicated
        ));
    }
}
