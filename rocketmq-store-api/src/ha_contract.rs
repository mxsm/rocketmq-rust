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

//! Backend-neutral high-availability value types and acknowledgement decisions.

use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use crate::Durability;
use crate::StoreContractViolation;

/// A positive Controller-issued master epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MasterEpoch(i32);

impl MasterEpoch {
    /// Returns the wire-compatible epoch value.
    pub const fn get(self) -> i32 {
        self.0
    }
}

impl TryFrom<i32> for MasterEpoch {
    type Error = StoreContractViolation;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        if value <= 0 {
            return Err(StoreContractViolation::HaInvalidMasterEpoch(value));
        }
        Ok(Self(value))
    }
}

/// A positive Controller-issued sync-state-set epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SyncStateSetEpoch(i32);

impl SyncStateSetEpoch {
    /// Returns the wire-compatible epoch value.
    pub const fn get(self) -> i32 {
        self.0
    }
}

impl TryFrom<i32> for SyncStateSetEpoch {
    type Error = StoreContractViolation;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        if value <= 0 {
            return Err(StoreContractViolation::HaInvalidSyncStateSetEpoch(value));
        }
        Ok(Self(value))
    }
}

/// A replication acknowledgement count greater than the local leader alone.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ReplicaCount(NonZeroUsize);

impl ReplicaCount {
    /// Creates a replica count that necessarily requires at least one remote acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns [`StoreContractViolation::HaInvalidReplicaCount`] when `value` is less than two.
    pub fn try_new(value: usize) -> Result<Self, StoreContractViolation> {
        if value < 2 {
            return Err(StoreContractViolation::HaInvalidReplicaCount(value));
        }
        Ok(Self(NonZeroUsize::new(value).expect("value is proven non-zero")))
    }

    /// Returns the number of acknowledgements, including the local leader.
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

/// The explicit durability condition requested by a synchronous append.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AckPolicy {
    /// Require only the local durable watermark.
    LocalDurable,
    /// Require the local leader and the configured number of unique in-sync members.
    ReplicaCount(ReplicaCount),
    /// Require the local leader and every member of the current sync-state set.
    AllInSyncSet,
}

impl AckPolicy {
    /// Converts a legacy ACK count or sentinel at a wire/configuration boundary.
    ///
    /// # Errors
    ///
    /// Returns [`StoreContractViolation::HaInvalidAckPolicy`] for zero, negative values other than
    /// `all_in_sync_state_set`, or values that cannot be represented as `usize`.
    pub fn try_from_legacy(value: i32, all_in_sync_state_set: i32) -> Result<Self, StoreContractViolation> {
        if value == all_in_sync_state_set {
            return Ok(Self::AllInSyncSet);
        }
        if value == 1 {
            return Ok(Self::LocalDurable);
        }
        if value <= 0 {
            return Err(StoreContractViolation::HaInvalidAckPolicy(value));
        }
        let count = usize::try_from(value).map_err(|_| StoreContractViolation::HaInvalidAckPolicy(value))?;
        Ok(Self::ReplicaCount(ReplicaCount::try_new(count)?))
    }
}

/// The Controller authority required to accept a write or role transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct WriteAuthority {
    broker_id: i64,
    master_epoch: MasterEpoch,
}

/// A generation-scoped Controller write lease token.
///
/// The token deliberately excludes an expiry timestamp. A Broker converts the
/// Controller-provided duration into a process-local monotonic deadline before
/// installing it in Store.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct WriteLeaseToken {
    authority: WriteAuthority,
    generation: u64,
}

impl WriteLeaseToken {
    /// Creates a token for a non-zero Controller lease generation.
    ///
    /// # Errors
    ///
    /// Returns [`StoreContractViolation::HaInvalidLeaseGeneration`] when `generation` is zero.
    pub fn try_new(authority: WriteAuthority, generation: u64) -> Result<Self, StoreContractViolation> {
        if generation == 0 {
            return Err(StoreContractViolation::HaInvalidLeaseGeneration(generation));
        }
        Ok(Self { authority, generation })
    }

    /// Returns the Controller authority carried by this lease.
    pub const fn authority(self) -> WriteAuthority {
        self.authority
    }

    /// Returns the monotonically increasing lease generation.
    pub const fn generation(self) -> u64 {
        self.generation
    }
}

impl WriteAuthority {
    /// Creates an authority for a non-negative broker identifier and positive epoch.
    ///
    /// # Errors
    ///
    /// Returns [`StoreContractViolation::HaInvalidBrokerId`] when `broker_id` is negative.
    pub fn try_new(broker_id: i64, master_epoch: MasterEpoch) -> Result<Self, StoreContractViolation> {
        if broker_id < 0 {
            return Err(StoreContractViolation::HaInvalidBrokerId(broker_id));
        }
        Ok(Self {
            broker_id,
            master_epoch,
        })
    }

    /// Creates an authority from a wire broker identifier.
    ///
    /// # Errors
    ///
    /// Returns [`StoreContractViolation::HaBrokerIdOutOfRange`] when the identifier cannot be represented
    /// by the canonical signed broker-id type.
    pub fn try_from_u64(broker_id: u64, master_epoch: MasterEpoch) -> Result<Self, StoreContractViolation> {
        let broker_id =
            i64::try_from(broker_id).map_err(|_| StoreContractViolation::HaBrokerIdOutOfRange(broker_id))?;
        Self::try_new(broker_id, master_epoch)
    }

    /// Returns the authorized broker identifier.
    pub const fn broker_id(self) -> i64 {
        self.broker_id
    }

    /// Returns the authorized master epoch.
    pub const fn master_epoch(self) -> MasterEpoch {
        self.master_epoch
    }
}

/// One remote replica's durable acknowledgement.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ReplicaAck {
    broker_id: i64,
    durable_offset: i64,
}

impl ReplicaAck {
    /// Creates a validated remote acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns a typed error for a negative broker identifier or durable offset.
    pub fn try_new(broker_id: i64, durable_offset: i64) -> Result<Self, StoreContractViolation> {
        if broker_id < 0 {
            return Err(StoreContractViolation::HaInvalidBrokerId(broker_id));
        }
        if durable_offset < 0 {
            return Err(StoreContractViolation::HaInvalidOffset(durable_offset));
        }
        Ok(Self {
            broker_id,
            durable_offset,
        })
    }

    /// Returns the acknowledging broker identifier.
    pub const fn broker_id(self) -> i64 {
        self.broker_id
    }

    /// Returns the exclusive durable offset reported by the replica.
    pub const fn durable_offset(self) -> i64 {
        self.durable_offset
    }
}

/// A non-empty set of non-negative broker identifiers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncStateSet(BTreeSet<i64>);

impl SyncStateSet {
    /// Creates a validated, de-duplicated sync-state set.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the set is empty or contains a negative broker identifier.
    pub fn try_new(members: impl IntoIterator<Item = i64>) -> Result<Self, StoreContractViolation> {
        let members = members.into_iter().collect::<BTreeSet<_>>();
        if members.is_empty() {
            return Err(StoreContractViolation::HaEmptySyncStateSet);
        }
        if let Some(broker_id) = members.iter().copied().find(|broker_id| *broker_id < 0) {
            return Err(StoreContractViolation::HaInvalidBrokerId(broker_id));
        }
        Ok(Self(members))
    }

    /// Returns whether the set contains a broker identifier.
    pub fn contains(&self, broker_id: i64) -> bool {
        self.0.contains(&broker_id)
    }

    /// Returns the number of unique members.
    fn len(&self) -> usize {
        self.0.len()
    }

    fn iter(&self) -> impl Iterator<Item = i64> + '_ {
        self.0.iter().copied()
    }
}

/// A complete, validated observation used for one acknowledgement decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicationObservation {
    current_authority: WriteAuthority,
    requested_authority: WriteAuthority,
    policy: AckPolicy,
    required_offset: i64,
    local_durable_watermark: i64,
    replica_acks: Vec<ReplicaAck>,
    sync_state_set: SyncStateSet,
}

impl ReplicationObservation {
    /// Creates a complete replication observation.
    ///
    /// # Errors
    ///
    /// Returns a typed error for negative offsets or when the current leader is absent from the
    /// supplied sync-state set.
    pub fn try_new(
        current_authority: WriteAuthority,
        requested_authority: WriteAuthority,
        policy: AckPolicy,
        required_offset: i64,
        local_durable_watermark: i64,
        replica_acks: Vec<ReplicaAck>,
        sync_state_set: SyncStateSet,
    ) -> Result<Self, StoreContractViolation> {
        if required_offset < 0 {
            return Err(StoreContractViolation::HaInvalidOffset(required_offset));
        }
        if local_durable_watermark < 0 {
            return Err(StoreContractViolation::HaInvalidOffset(local_durable_watermark));
        }
        if !sync_state_set.contains(current_authority.broker_id()) {
            return Err(StoreContractViolation::HaLeaderMissingFromSyncStateSet(
                current_authority.broker_id(),
            ));
        }
        Ok(Self {
            current_authority,
            requested_authority,
            policy,
            required_offset,
            local_durable_watermark,
            replica_acks,
            sync_state_set,
        })
    }
}

/// A proof produced only after the canonical acknowledgement decision succeeds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicationAcknowledgement {
    durability: Durability,
    acknowledged_offset: i64,
}

impl ReplicationAcknowledgement {
    /// Returns the durability proven by the decision.
    pub const fn durability(self) -> Durability {
        self.durability
    }

    /// Returns the exclusive offset covered by the decision.
    pub const fn acknowledged_offset(self) -> i64 {
        self.acknowledged_offset
    }
}

/// Why a write authority or replica policy was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HaRejectReason {
    /// The request belongs to an older Controller epoch.
    StaleAuthority,
    /// The request does not exactly match the currently installed authority.
    AuthorityMismatch,
}

/// Canonical outcome of evaluating authority, local durability, and replica acknowledgements.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplicationDecision {
    /// The requested durability was proven.
    Acknowledge(ReplicationAcknowledgement),
    /// The authority and membership are valid, but progress has not reached this offset.
    Wait {
        /// Exclusive offset required from local storage and applicable replicas.
        required_offset: i64,
    },
    /// The request must fail closed rather than wait or downgrade durability.
    Reject(HaRejectReason),
}

/// Evaluates one append without I/O, mutation, retry, or implicit durability downgrade.
pub fn decide_replication(observation: &ReplicationObservation) -> ReplicationDecision {
    let current = observation.current_authority;
    let requested = observation.requested_authority;
    if requested.master_epoch() < current.master_epoch() {
        return ReplicationDecision::Reject(HaRejectReason::StaleAuthority);
    }
    if requested != current {
        return ReplicationDecision::Reject(HaRejectReason::AuthorityMismatch);
    }
    if observation.local_durable_watermark < observation.required_offset {
        return ReplicationDecision::Wait {
            required_offset: observation.required_offset,
        };
    }

    let acknowledge = |durability| {
        ReplicationDecision::Acknowledge(ReplicationAcknowledgement {
            durability,
            acknowledged_offset: observation.required_offset,
        })
    };
    match observation.policy {
        AckPolicy::LocalDurable => acknowledge(Durability::Local),
        AckPolicy::ReplicaCount(required) => {
            let mut acknowledged = BTreeSet::from([current.broker_id()]);
            for replica in &observation.replica_acks {
                if replica.broker_id() != current.broker_id()
                    && observation.sync_state_set.contains(replica.broker_id())
                    && replica.durable_offset() >= observation.required_offset
                {
                    acknowledged.insert(replica.broker_id());
                }
            }
            if acknowledged.len() >= required.get() {
                acknowledge(Durability::Replicated)
            } else {
                ReplicationDecision::Wait {
                    required_offset: observation.required_offset,
                }
            }
        }
        AckPolicy::AllInSyncSet => {
            if observation.sync_state_set.len() == 1 {
                return acknowledge(Durability::Local);
            }
            let acknowledged = observation
                .replica_acks
                .iter()
                .filter(|replica| {
                    replica.broker_id() != current.broker_id()
                        && observation.sync_state_set.contains(replica.broker_id())
                        && replica.durable_offset() >= observation.required_offset
                })
                .map(|replica| replica.broker_id())
                .collect::<BTreeSet<_>>();
            if observation
                .sync_state_set
                .iter()
                .filter(|broker_id| *broker_id != current.broker_id())
                .all(|broker_id| acknowledged.contains(&broker_id))
            {
                acknowledge(Durability::Replicated)
            } else {
                ReplicationDecision::Wait {
                    required_offset: observation.required_offset,
                }
            }
        }
    }
}
