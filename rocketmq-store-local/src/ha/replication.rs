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
use std::sync::Mutex;

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
    Advanced { epoch: i32 },
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
            let mut tracker = self.tracker.lock().expect("lock Local replication state");
            if tracker.local_sync_state_set.is_empty() {
                tracker.local_sync_state_set.insert(local_broker_id);
            }
        }
    }

    pub fn local_sync_state_set(&self) -> HashSet<i64> {
        self.tracker
            .lock()
            .expect("lock Local replication state")
            .local_sync_state_set
            .clone()
    }

    pub fn sync_state_set(&self) -> HashSet<i64> {
        let tracker = self.tracker.lock().expect("lock Local replication state");
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

    pub fn replace_sync_state_set(&self, sync_state_set: HashSet<i64>) {
        let mut tracker = self.tracker.lock().expect("lock Local replication state");
        self.is_synchronizing_sync_state_set.store(false, Ordering::SeqCst);
        tracker.local_sync_state_set = sync_state_set;
        tracker.remote_sync_state_set.clear();
        tracker.connection_caught_up_time_table.clear();
    }

    pub fn is_synchronizing_sync_state_set(&self) -> bool {
        self.is_synchronizing_sync_state_set.load(Ordering::SeqCst)
    }

    pub fn record_caught_up(&self, slave_broker_id: i64, last_caught_up_time_ms: u64) {
        let mut tracker = self.tracker.lock().expect("lock Local replication state");
        let previous = tracker
            .connection_caught_up_time_table
            .get(&slave_broker_id)
            .copied()
            .unwrap_or_default();
        tracker
            .connection_caught_up_time_table
            .insert(slave_broker_id, previous.max(last_caught_up_time_ms));
    }

    pub fn maybe_shrink_sync_state_set(&self, now_millis: u64, timeout_millis: u64) -> HashSet<i64> {
        let local_broker_id = self.local_broker_id();
        let mut tracker = self.tracker.lock().expect("lock Local replication state");
        let mut next = tracker.local_sync_state_set.clone();
        let mut changed = false;
        next.retain(|broker_id| {
            if *broker_id == local_broker_id {
                return true;
            }
            match tracker.connection_caught_up_time_table.get(broker_id).copied() {
                Some(caught_up_at) if now_millis.saturating_sub(caught_up_at) <= timeout_millis => true,
                _ => {
                    changed = true;
                    false
                }
            }
        });
        if changed {
            self.is_synchronizing_sync_state_set.store(true, Ordering::SeqCst);
            tracker.remote_sync_state_set = next.clone();
        }
        next
    }

    pub fn maybe_expand_sync_state_set(
        &self,
        slave_broker_id: i64,
        slave_max_offset: i64,
        confirm_offset: i64,
    ) -> Option<HashSet<i64>> {
        if slave_max_offset < confirm_offset {
            return None;
        }
        let mut tracker = self.tracker.lock().expect("lock Local replication state");
        if tracker.local_sync_state_set.contains(&slave_broker_id) {
            return None;
        }
        let mut next = tracker.local_sync_state_set.clone();
        next.insert(slave_broker_id);
        self.is_synchronizing_sync_state_set.store(true, Ordering::SeqCst);
        tracker.remote_sync_state_set = next.clone();
        Some(next)
    }

    pub fn remove_replica(&self, slave_broker_id: i64) -> bool {
        let mut tracker = self.tracker.lock().expect("lock Local replication state");
        let removed = tracker.local_sync_state_set.remove(&slave_broker_id);
        tracker.remote_sync_state_set.remove(&slave_broker_id);
        tracker.connection_caught_up_time_table.remove(&slave_broker_id);
        removed
    }

    pub fn tracked_sync_state_set_size(&self) -> Option<usize> {
        let tracker = self.tracker.lock().expect("lock Local replication state");
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
        let mut tracker = self.tracker.lock().expect("lock Local replication state");
        self.is_synchronizing_sync_state_set.store(false, Ordering::SeqCst);
        tracker.remote_sync_state_set.clear();
        tracker.connection_caught_up_time_table.clear();
    }

    pub fn ensure_local_member(&self) {
        let local_broker_id = self.local_broker_id();
        if local_broker_id < 0 {
            return;
        }
        let mut tracker = self.tracker.lock().expect("lock Local replication state");
        if tracker.local_sync_state_set.is_empty() {
            tracker.local_sync_state_set.insert(local_broker_id);
        }
    }

    pub fn apply_epoch_transition(&self, next_epoch: i32) -> EpochTransition {
        loop {
            let current_epoch = self.current_master_epoch.load(Ordering::SeqCst);
            if next_epoch < current_epoch {
                return EpochTransition::Rejected;
            }
            if next_epoch == current_epoch {
                return EpochTransition::Unchanged;
            }
            if self
                .current_master_epoch
                .compare_exchange(current_epoch, next_epoch, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return EpochTransition::Advanced { epoch: next_epoch };
            }
        }
    }

    pub fn current_master_epoch(&self) -> i32 {
        self.current_master_epoch.load(Ordering::SeqCst)
    }
}

pub fn has_required_sync_state_set_acks(
    sync_state_set: &HashSet<i64>,
    acked_replicas: &[HAAckedReplicaSnapshot],
    next_offset: i64,
) -> bool {
    if sync_state_set.len() <= 1 {
        return true;
    }
    let mut ack_count = 1;
    for replica in acked_replicas {
        if replica
            .slave_broker_id
            .is_some_and(|broker_id| sync_state_set.contains(&broker_id))
            && replica.slave_ack_offset >= next_offset
        {
            ack_count += 1;
        }
        if ack_count >= sync_state_set.len() {
            return true;
        }
    }
    false
}

pub fn has_required_acks(required_acks: i32, acked_replicas: &[HAAckedReplicaSnapshot], next_offset: i64) -> bool {
    if required_acks <= 1 {
        return true;
    }
    let mut ack_count = 1;
    for replica in acked_replicas {
        if replica.slave_ack_offset >= next_offset {
            ack_count += 1;
        }
        if ack_count >= required_acks {
            return true;
        }
    }
    false
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
    fn ack_policy_counts_master_and_filters_sync_state_members() {
        let replicas = [
            HAAckedReplicaSnapshot {
                slave_broker_id: Some(2),
                slave_ack_offset: 100,
            },
            HAAckedReplicaSnapshot {
                slave_broker_id: Some(3),
                slave_ack_offset: 50,
            },
        ];
        assert!(has_required_acks(2, &replicas, 100));
        assert!(has_required_sync_state_set_acks(&HashSet::from([1, 2]), &replicas, 100));
        assert!(!has_required_sync_state_set_acks(
            &HashSet::from([1, 2, 3]),
            &replicas,
            100
        ));
    }

    #[test]
    fn replication_root_tracks_pending_membership_and_epoch_monotonically() {
        let root = ReplicationStateRoot::new(true);
        root.set_local_broker_id(1);
        root.replace_sync_state_set(HashSet::from([1]));
        assert_eq!(root.maybe_expand_sync_state_set(2, 64, 64), Some(HashSet::from([1, 2])));
        assert!(root.is_synchronizing_sync_state_set());
        assert_eq!(root.sync_state_set(), HashSet::from([1, 2]));
        assert_eq!(root.apply_epoch_transition(3), EpochTransition::Advanced { epoch: 3 });
        assert_eq!(root.apply_epoch_transition(2), EpochTransition::Rejected);
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
}
