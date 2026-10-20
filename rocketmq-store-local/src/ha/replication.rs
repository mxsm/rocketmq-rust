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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use parking_lot::Mutex;
use parking_lot::MutexGuard;
use rocketmq_store_api::MasterEpoch;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HAAckedReplicaSnapshot {
    pub slave_broker_id: Option<i64>,
    pub slave_ack_offset: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HAReplicaRuntimeSnapshot {
    pub slave_ack_offset: i64,
    pub in_sync: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GroupTransferRuntimeInfo {
    pub pending_request_count: u64,
    pub pending_request_oldest_wait_millis: u64,
    pub ack_notify_count: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EpochTransition {
    Rejected,
    Unchanged,
    Advanced { epoch: MasterEpoch },
}

#[derive(Default)]
struct SyncStateTracker {
    local_sync_state_set: HashSet<i64>,
    remote_sync_state_set: HashSet<i64>,
    connection_caught_up_time_table: HashMap<i64, u64>,
}

/// Canonical controller/replica state. Store adapters apply network and CommitLog side effects.
pub struct ReplicationStateRoot {
    is_master: AtomicBool,
    tracker: Mutex<SyncStateTracker>,
    is_synchronizing_sync_state_set: AtomicBool,
    local_broker_id: AtomicI64,
    current_master_epoch: AtomicI32,
}

#[derive(Default)]
pub struct ReplicationProgress {
    max_ack_offset: AtomicU64,
}

impl ReplicationProgress {
    pub fn record_ack(&self, offset: i64) -> bool {
        let Ok(offset) = u64::try_from(offset) else {
            return false;
        };
        let mut current = self.max_ack_offset.load(Ordering::Relaxed);
        while offset > current {
            match self
                .max_ack_offset
                .compare_exchange_weak(current, offset, Ordering::SeqCst, Ordering::Relaxed)
            {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
        false
    }

    pub fn max_ack_offset(&self) -> i64 {
        i64::try_from(self.max_ack_offset.load(Ordering::Relaxed)).unwrap_or(i64::MAX)
    }
}

impl ReplicationStateRoot {
    pub fn new(is_master: bool) -> Self {
        Self {
            is_master: AtomicBool::new(is_master),
            tracker: Mutex::new(SyncStateTracker::default()),
            is_synchronizing_sync_state_set: AtomicBool::new(false),
            local_broker_id: AtomicI64::new(-1),
            current_master_epoch: AtomicI32::new(0),
        }
    }

    pub fn is_master(&self) -> bool {
        self.is_master.load(Ordering::SeqCst)
    }

    pub fn set_master(&self, is_master: bool) {
        self.is_master.store(is_master, Ordering::SeqCst);
    }

    pub fn local_broker_id(&self) -> i64 {
        self.local_broker_id.load(Ordering::SeqCst)
    }

    pub fn set_local_broker_id(&self, local_broker_id: i64) {
        self.local_broker_id.store(local_broker_id, Ordering::SeqCst);
        if self.is_master() && local_broker_id >= 0 {
            let mut tracker = self.tracker();
            if tracker.local_sync_state_set.is_empty() {
                tracker.local_sync_state_set.insert(local_broker_id);
            }
        }
    }

    pub fn local_sync_state_set(&self) -> HashSet<i64> {
        self.tracker().local_sync_state_set.clone()
    }

    pub fn sync_state_set(&self) -> HashSet<i64> {
        let tracker = self.tracker();
        if self.is_synchronizing_sync_state_set() {
            tracker
                .local_sync_state_set
                .union(&tracker.remote_sync_state_set)
                .copied()
                .collect()
        } else {
            tracker.local_sync_state_set.clone()
        }
    }

    /// Installs the sync state set committed by the Controller.
    ///
    /// Re-applying the current membership keeps a pending proposal: the Broker refreshes the
    /// Controller view periodically, possibly while its proposal is still being reported. Catch-up
    /// times always survive, because shrinking compares them against the not-caught-up timeout.
    pub fn replace_sync_state_set(&self, sync_state_set: HashSet<i64>) {
        let mut tracker = self.tracker();
        if tracker.local_sync_state_set == sync_state_set {
            return;
        }
        self.is_synchronizing_sync_state_set.store(false, Ordering::SeqCst);
        tracker.local_sync_state_set = sync_state_set;
        tracker.remote_sync_state_set.clear();
    }

    pub fn is_synchronizing_sync_state_set(&self) -> bool {
        self.is_synchronizing_sync_state_set.load(Ordering::SeqCst)
    }

    pub fn record_caught_up(&self, slave_broker_id: i64, last_caught_up_time_ms: u64) {
        let mut tracker = self.tracker();
        let previous = tracker
            .connection_caught_up_time_table
            .get(&slave_broker_id)
            .copied()
            .unwrap_or_default();
        tracker
            .connection_caught_up_time_table
            .insert(slave_broker_id, previous.max(last_caught_up_time_ms));
    }

    /// Computes the membership this master should propose to the Controller.
    ///
    /// The proposal keeps the local broker, the committed members, and pending additions whose
    /// last catch-up is within `timeout_millis`; a member without a catch-up time never
    /// connected. A proposal that differs from the committed set stays pending until the
    /// Controller commits a new set.
    pub fn maybe_shrink_sync_state_set(&self, now_millis: u64, timeout_millis: u64) -> HashSet<i64> {
        let local_broker_id = self.local_broker_id();
        let mut tracker = self.tracker();
        let mut next = tracker.local_sync_state_set.clone();
        if self.is_synchronizing_sync_state_set() {
            next.extend(tracker.remote_sync_state_set.iter().copied());
        }
        next.retain(|broker_id| {
            *broker_id == local_broker_id
                || tracker
                    .connection_caught_up_time_table
                    .get(broker_id)
                    .is_some_and(|caught_up_at| now_millis.saturating_sub(*caught_up_at) <= timeout_millis)
        });
        if next != tracker.local_sync_state_set {
            self.is_synchronizing_sync_state_set.store(true, Ordering::SeqCst);
            tracker.remote_sync_state_set = next.clone();
        } else if self.is_synchronizing_sync_state_set() {
            self.is_synchronizing_sync_state_set.store(false, Ordering::SeqCst);
            tracker.remote_sync_state_set.clear();
        }
        next
    }

    /// Adds a caught-up replica to the pending proposal.
    ///
    /// `min_offset` is the offset the replica must have reached. Returns the new proposal, or
    /// `None` when the replica is behind, already committed, or already proposed. Pending
    /// additions accumulate, so the union that guards acknowledgements covers every proposed
    /// member until the Controller answers.
    pub fn maybe_expand_sync_state_set(
        &self,
        slave_broker_id: i64,
        slave_max_offset: i64,
        min_offset: i64,
    ) -> Option<HashSet<i64>> {
        if slave_max_offset < min_offset {
            return None;
        }
        let mut tracker = self.tracker();
        if tracker.local_sync_state_set.contains(&slave_broker_id) {
            return None;
        }
        let mut next = if self.is_synchronizing_sync_state_set() {
            if tracker.remote_sync_state_set.contains(&slave_broker_id) {
                return None;
            }
            tracker.remote_sync_state_set.clone()
        } else {
            tracker.local_sync_state_set.clone()
        };
        next.insert(slave_broker_id);
        self.is_synchronizing_sync_state_set.store(true, Ordering::SeqCst);
        tracker.remote_sync_state_set = next.clone();
        Some(next)
    }

    /// Forgets a disconnected replica. Returns whether it was committed or proposed.
    pub fn remove_replica(&self, slave_broker_id: i64) -> bool {
        let mut tracker = self.tracker();
        let removed = tracker.local_sync_state_set.remove(&slave_broker_id);
        let unproposed = tracker.remote_sync_state_set.remove(&slave_broker_id);
        tracker.connection_caught_up_time_table.remove(&slave_broker_id);
        removed || unproposed
    }

    pub fn tracked_sync_state_set_size(&self) -> Option<usize> {
        let tracker = self.tracker();
        if self.is_synchronizing_sync_state_set() {
            Some(
                tracker
                    .local_sync_state_set
                    .len()
                    .max(tracker.remote_sync_state_set.len()),
            )
        } else if tracker.local_sync_state_set.is_empty() {
            None
        } else {
            Some(tracker.local_sync_state_set.len())
        }
    }

    pub fn clear_pending_sync_state_tracking(&self) {
        let mut tracker = self.tracker();
        self.is_synchronizing_sync_state_set.store(false, Ordering::SeqCst);
        tracker.remote_sync_state_set.clear();
        tracker.connection_caught_up_time_table.clear();
    }

    pub fn ensure_local_member(&self) {
        let local_broker_id = self.local_broker_id();
        if local_broker_id < 0 {
            return;
        }
        let mut tracker = self.tracker();
        if tracker.local_sync_state_set.is_empty() {
            tracker.local_sync_state_set.insert(local_broker_id);
        }
    }

    pub fn apply_epoch_transition(&self, next_epoch: MasterEpoch) -> EpochTransition {
        let next_epoch_value = next_epoch.get();
        loop {
            let current_epoch = self.current_master_epoch.load(Ordering::SeqCst);
            if next_epoch_value < current_epoch {
                return EpochTransition::Rejected;
            }
            if next_epoch_value == current_epoch {
                return EpochTransition::Unchanged;
            }
            if self
                .current_master_epoch
                .compare_exchange(current_epoch, next_epoch_value, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return EpochTransition::Advanced { epoch: next_epoch };
            }
        }
    }

    pub fn current_master_epoch(&self) -> Option<MasterEpoch> {
        MasterEpoch::try_from(self.current_master_epoch.load(Ordering::SeqCst)).ok()
    }

    fn tracker(&self) -> MutexGuard<'_, SyncStateTracker> {
        self.tracker.lock()
    }
}

pub fn compute_confirm_offset(
    current_confirm_offset: i64,
    max_phy_offset: i64,
    expected_sync_state_set_size: usize,
    replicas: &[HAReplicaRuntimeSnapshot],
) -> i64 {
    let in_sync_offsets = replicas
        .iter()
        .filter(|replica| replica.in_sync && replica.slave_ack_offset > 0)
        .map(|replica| replica.slave_ack_offset)
        .collect::<Vec<_>>();
    let candidate_offsets = if in_sync_offsets.is_empty() {
        replicas
            .iter()
            .filter(|replica| replica.slave_ack_offset > 0)
            .map(|replica| replica.slave_ack_offset)
            .collect::<Vec<_>>()
    } else {
        in_sync_offsets
    };

    if expected_sync_state_set_size.saturating_sub(1) > candidate_offsets.len() {
        return current_confirm_offset;
    }
    candidate_offsets.into_iter().min().unwrap_or(max_phy_offset)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replication_root_tracks_pending_membership_and_epoch_monotonically() {
        let root = ReplicationStateRoot::new(true);
        root.set_local_broker_id(1);
        root.replace_sync_state_set(HashSet::from([1]));
        assert_eq!(root.maybe_expand_sync_state_set(2, 64, 64), Some(HashSet::from([1, 2])));
        assert!(root.is_synchronizing_sync_state_set());
        assert_eq!(root.sync_state_set(), HashSet::from([1, 2]));
        let epoch_three = MasterEpoch::try_from(3).expect("positive epoch");
        assert_eq!(
            root.apply_epoch_transition(epoch_three),
            EpochTransition::Advanced { epoch: epoch_three }
        );
        assert_eq!(
            root.apply_epoch_transition(MasterEpoch::try_from(2).expect("positive epoch")),
            EpochTransition::Rejected
        );
    }

    #[test]
    fn reapplied_controller_view_keeps_pending_proposal_and_catch_up_times() {
        let root = ReplicationStateRoot::new(true);
        root.set_local_broker_id(1);
        root.replace_sync_state_set(HashSet::from([1]));
        assert_eq!(root.maybe_expand_sync_state_set(2, 64, 64), Some(HashSet::from([1, 2])));
        root.record_caught_up(2, 10_000);

        root.replace_sync_state_set(HashSet::from([1]));

        assert!(root.is_synchronizing_sync_state_set());
        assert_eq!(root.sync_state_set(), HashSet::from([1, 2]));
        assert_eq!(root.maybe_shrink_sync_state_set(12_000, 15_000), HashSet::from([1, 2]));

        root.replace_sync_state_set(HashSet::from([1, 2]));

        assert!(!root.is_synchronizing_sync_state_set());
        assert_eq!(root.maybe_shrink_sync_state_set(12_000, 15_000), HashSet::from([1, 2]));
        assert!(!root.is_synchronizing_sync_state_set());
    }

    #[test]
    fn pending_additions_accumulate_into_one_proposal() {
        let root = ReplicationStateRoot::new(true);
        root.set_local_broker_id(1);
        root.replace_sync_state_set(HashSet::from([1]));

        assert_eq!(root.maybe_expand_sync_state_set(2, 64, 64), Some(HashSet::from([1, 2])));
        assert_eq!(
            root.maybe_expand_sync_state_set(3, 64, 64),
            Some(HashSet::from([1, 2, 3]))
        );
        assert_eq!(root.maybe_expand_sync_state_set(2, 96, 64), None);
        assert_eq!(root.maybe_expand_sync_state_set(4, 32, 64), None);
        assert_eq!(root.sync_state_set(), HashSet::from([1, 2, 3]));

        assert!(root.remove_replica(3));
        assert_eq!(root.sync_state_set(), HashSet::from([1, 2]));
    }

    #[test]
    fn shrink_keeps_fresh_pending_additions_and_drops_stale_members() {
        let root = ReplicationStateRoot::new(true);
        root.set_local_broker_id(1);
        root.replace_sync_state_set(HashSet::from([1, 2]));
        root.record_caught_up(2, 10_000);
        assert_eq!(
            root.maybe_expand_sync_state_set(3, 64, 64),
            Some(HashSet::from([1, 2, 3]))
        );
        root.record_caught_up(3, 10_000);

        assert_eq!(
            root.maybe_shrink_sync_state_set(20_000, 15_000),
            HashSet::from([1, 2, 3])
        );
        assert!(root.is_synchronizing_sync_state_set());

        root.record_caught_up(2, 20_000);
        assert_eq!(root.maybe_shrink_sync_state_set(30_000, 15_000), HashSet::from([1, 2]));
        assert!(!root.is_synchronizing_sync_state_set());
        assert_eq!(root.sync_state_set(), HashSet::from([1, 2]));

        assert_eq!(root.maybe_shrink_sync_state_set(40_000, 15_000), HashSet::from([1]));
        assert!(root.is_synchronizing_sync_state_set());
        assert_eq!(root.sync_state_set(), HashSet::from([1, 2]));
    }

    #[test]
    fn confirm_offset_requires_expected_replica_coverage() {
        let replicas = [HAReplicaRuntimeSnapshot {
            slave_ack_offset: 128,
            in_sync: true,
        }];
        assert_eq!(compute_confirm_offset(64, 256, 2, &replicas), 128);
        assert_eq!(compute_confirm_offset(64, 256, 3, &replicas), 64);
    }

    #[test]
    fn progress_is_monotonic_and_rejects_negative_offsets() {
        let progress = ReplicationProgress::default();
        assert!(!progress.record_ack(-1));
        assert!(progress.record_ack(64));
        assert!(!progress.record_ack(32));
        assert_eq!(progress.max_ack_offset(), 64);
    }

    #[test]
    fn replication_state_remains_available_after_a_panicking_holder() {
        let root = ReplicationStateRoot::new(true);
        let panic_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _tracker = root.tracker();
            panic!("injected replication-state panic");
        }));

        assert!(panic_result.is_err());
        root.set_local_broker_id(7);
        assert_eq!(root.local_sync_state_set(), HashSet::from([7]));
    }
}
