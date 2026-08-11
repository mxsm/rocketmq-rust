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

use std::collections::BTreeMap;
use std::collections::HashSet;

use rocketmq_store_api::decide_replication;
use rocketmq_store_api::AckPolicy;
use rocketmq_store_api::HaRejectReason;
use rocketmq_store_api::ReplicaAck;
use rocketmq_store_api::ReplicationDecision;
use rocketmq_store_api::ReplicationObservation;
use rocketmq_store_api::SyncStateSet;
use rocketmq_store_api::WriteAuthority;

use crate::ha::ha_service::HAAckedReplicaSnapshot;

/// One immutable view of authority, membership, and durable progress used to resolve a batch of
/// group-transfer requests.
pub(crate) enum AckFrontier {
    Ready {
        authority: WriteAuthority,
        local_durable_watermark: i64,
        replica_acks: Vec<ReplicaAck>,
        replica_frontiers: BTreeMap<i64, i64>,
        sync_state_set: SyncStateSet,
        members: HashSet<i64>,
    },
    Rejected(HaRejectReason),
}

impl AckFrontier {
    pub(crate) fn from_snapshots(
        authority: Option<WriteAuthority>,
        configured_sync_state_set: Option<HashSet<i64>>,
        local_durable_watermark: i64,
        snapshots: &[HAAckedReplicaSnapshot],
        require_controller_ids: bool,
    ) -> Self {
        let Some(authority) = authority else {
            return Self::Rejected(HaRejectReason::AuthorityMismatch);
        };
        let mut members = configured_sync_state_set.unwrap_or_else(|| HashSet::from([authority.broker_id()]));
        let mut replica_frontiers = BTreeMap::<i64, i64>::new();
        for (index, snapshot) in snapshots.iter().enumerate() {
            let broker_id = match snapshot.slave_broker_id {
                Some(broker_id) => broker_id,
                None if !require_controller_ids => {
                    match i64::try_from(index).ok().and_then(|value| value.checked_add(1)) {
                        Some(broker_id) => broker_id,
                        None => continue,
                    }
                }
                None => continue,
            };
            let Ok(replica_ack) = ReplicaAck::try_new(broker_id, snapshot.slave_ack_offset) else {
                continue;
            };
            members.insert(broker_id);
            replica_frontiers
                .entry(broker_id)
                .and_modify(|offset| *offset = (*offset).max(replica_ack.durable_offset()))
                .or_insert(replica_ack.durable_offset());
        }
        if !members.contains(&authority.broker_id()) {
            return Self::Rejected(HaRejectReason::AuthorityMismatch);
        }
        let Ok(sync_state_set) = SyncStateSet::try_new(members.iter().copied()) else {
            return Self::Rejected(HaRejectReason::AuthorityMismatch);
        };
        let replica_acks = replica_frontiers
            .iter()
            .filter_map(|(broker_id, offset)| ReplicaAck::try_new(*broker_id, *offset).ok())
            .collect();

        Self::Ready {
            authority,
            local_durable_watermark: local_durable_watermark.max(0),
            replica_acks,
            replica_frontiers,
            sync_state_set,
            members,
        }
    }

    pub(crate) fn decide(
        &self,
        requested_authority: Option<WriteAuthority>,
        policy: AckPolicy,
        required_offset: i64,
    ) -> ReplicationDecision {
        let (authority, local_durable_watermark, replica_acks, sync_state_set) = match self {
            Self::Ready {
                authority,
                local_durable_watermark,
                replica_acks,
                sync_state_set,
                ..
            } => (authority, local_durable_watermark, replica_acks, sync_state_set),
            Self::Rejected(reason) => return ReplicationDecision::Reject(*reason),
        };
        let requested_authority = requested_authority.unwrap_or(*authority);
        if requested_authority.master_epoch() < authority.master_epoch() {
            return ReplicationDecision::Reject(HaRejectReason::StaleAuthority);
        }
        if requested_authority != *authority || required_offset < 0 {
            return ReplicationDecision::Reject(HaRejectReason::AuthorityMismatch);
        }
        if required_offset > self.policy_frontier(policy) {
            return ReplicationDecision::Wait { required_offset };
        }

        let observation = ReplicationObservation::try_new(
            *authority,
            requested_authority,
            policy,
            required_offset,
            *local_durable_watermark,
            replica_acks.clone(),
            sync_state_set.clone(),
        );
        match observation {
            Ok(observation) => decide_replication(&observation),
            Err(_) => ReplicationDecision::Reject(HaRejectReason::AuthorityMismatch),
        }
    }

    fn policy_frontier(&self, policy: AckPolicy) -> i64 {
        let Self::Ready {
            authority,
            local_durable_watermark,
            replica_frontiers,
            members,
            ..
        } = self
        else {
            return -1;
        };
        let remote_frontier = match policy {
            AckPolicy::LocalDurable => *local_durable_watermark,
            AckPolicy::ReplicaCount(required) => {
                let remote_required = required.get().saturating_sub(1);
                let mut offsets = members
                    .iter()
                    .filter(|broker_id| **broker_id != authority.broker_id())
                    .filter_map(|broker_id| replica_frontiers.get(broker_id).copied())
                    .collect::<Vec<_>>();
                if offsets.len() < remote_required {
                    -1
                } else {
                    offsets.sort_unstable_by(|left, right| right.cmp(left));
                    offsets[remote_required - 1]
                }
            }
            AckPolicy::AllInSyncSet => members
                .iter()
                .filter(|broker_id| **broker_id != authority.broker_id())
                .map(|broker_id| replica_frontiers.get(broker_id).copied().unwrap_or(-1))
                .min()
                .unwrap_or(*local_durable_watermark),
        };
        (*local_durable_watermark).min(remote_frontier)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rocketmq_store_api::MasterEpoch;
    use rocketmq_store_api::ReplicaCount;

    use super::*;

    fn authority(epoch: i32) -> WriteAuthority {
        WriteAuthority::try_new(0, MasterEpoch::try_from(epoch).expect("positive epoch")).expect("valid authority")
    }

    #[test]
    fn replica_count_frontier_uses_the_required_highest_unique_replica() {
        let frontier = AckFrontier::from_snapshots(
            Some(authority(3)),
            Some(HashSet::from([0, 1, 2])),
            200,
            &[
                HAAckedReplicaSnapshot {
                    slave_broker_id: Some(1),
                    slave_ack_offset: 180,
                },
                HAAckedReplicaSnapshot {
                    slave_broker_id: Some(2),
                    slave_ack_offset: 120,
                },
            ],
            true,
        );

        assert!(matches!(
            frontier.decide(
                Some(authority(3)),
                AckPolicy::ReplicaCount(ReplicaCount::try_new(2).expect("two replicas")),
                180,
            ),
            ReplicationDecision::Acknowledge(_)
        ));
        assert!(matches!(
            frontier.decide(
                Some(authority(3)),
                AckPolicy::ReplicaCount(ReplicaCount::try_new(3).expect("three replicas")),
                180,
            ),
            ReplicationDecision::Wait { .. }
        ));
    }

    #[test]
    fn all_in_sync_frontier_waits_for_the_slowest_member() {
        let frontier = AckFrontier::from_snapshots(
            Some(authority(3)),
            Some(HashSet::from([0, 1, 2])),
            200,
            &[
                HAAckedReplicaSnapshot {
                    slave_broker_id: Some(1),
                    slave_ack_offset: 180,
                },
                HAAckedReplicaSnapshot {
                    slave_broker_id: Some(2),
                    slave_ack_offset: 120,
                },
            ],
            true,
        );

        assert!(matches!(
            frontier.decide(Some(authority(3)), AckPolicy::AllInSyncSet, 120),
            ReplicationDecision::Acknowledge(_)
        ));
        assert!(matches!(
            frontier.decide(Some(authority(3)), AckPolicy::AllInSyncSet, 121),
            ReplicationDecision::Wait { .. }
        ));
    }

    #[test]
    fn authority_change_rejects_a_request_from_the_previous_epoch() {
        let frontier = AckFrontier::from_snapshots(Some(authority(4)), None, 200, &[], false);

        assert_eq!(
            frontier.decide(Some(authority(3)), AckPolicy::LocalDurable, 100),
            ReplicationDecision::Reject(HaRejectReason::StaleAuthority)
        );
    }
}
