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
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::sync::LazyLock;

use parking_lot::RwLock;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::TimeUtils::get_current_millis;
use tracing::info;
use tracing::warn;

pub static REBALANCE_LOCK_MAX_LIVE_TIME: LazyLock<i64> = LazyLock::new(|| {
    std::env::var("rocketmq.broker.rebalance.lockMaxLiveTime")
        .unwrap_or("60000".to_string())
        .parse::<i64>()
        .unwrap_or(60000)
});

type MessageQueueLockTable = HashMap<String, HashMap<MessageQueue, LockEntry>>;

#[derive(Clone, Default)]
pub struct RebalanceLockManager {
    mq_lock_table: Arc<RwLock<MessageQueueLockTable>>,
}

impl RebalanceLockManager {
    pub fn is_lock_all_expired(&self, group: &str) -> bool {
        let lock_table = self.mq_lock_table.read();
        match lock_table.get(group) {
            None => true,
            Some(lock_entry) => lock_entry.values().all(|entry| entry.is_expired()),
        }
    }

    /// Try to lock a single message queue for the specified client.
    /// Returns true if the lock was successfully acquired.
    pub fn try_lock(&self, group: &str, mq: &MessageQueue, client_id: &str) -> bool {
        // Fast path: check if already locked by this client
        if self.is_locked(group, mq, client_id) {
            return true;
        }

        // Slow path: need to acquire write lock
        let mut write_guard = self.mq_lock_table.write();
        let group_value = write_guard
            .entry(group.to_string())
            .or_insert_with(|| HashMap::with_capacity(32));

        match group_value.get_mut(mq) {
            None => {
                // No lock entry exists, create new one
                group_value.insert(
                    mq.clone(),
                    LockEntry {
                        client_id: client_id.to_string(),
                        last_update_timestamp: AtomicI64::new(get_current_millis() as i64),
                    },
                );
                info!(
                    "RebalanceLockManager#tryLock: lock a message queue which has not been locked yet, group={}, \
                     clientId={}, mq={:?}",
                    group, client_id, mq
                );
                true
            }
            Some(lock_entry) => {
                if lock_entry.is_locked(client_id) {
                    lock_entry
                        .last_update_timestamp
                        .store(get_current_millis() as i64, std::sync::atomic::Ordering::Relaxed);
                    return true;
                }

                let old_client_id = lock_entry.client_id.clone();

                if lock_entry.is_expired() {
                    lock_entry.client_id = client_id.to_string();
                    lock_entry
                        .last_update_timestamp
                        .store(get_current_millis() as i64, std::sync::atomic::Ordering::Relaxed);
                    warn!(
                        "RebalanceLockManager#tryLock: try to lock a expired message queue, group={}, mq={:?}, old \
                         client id={}, new client id={}",
                        group, mq, old_client_id, client_id
                    );
                    return true;
                }

                warn!(
                    "RebalanceLockManager#tryLock: message queue has been locked by other client, group={}, mq={:?}, \
                     locked client id={}, current client id={}",
                    group, mq, old_client_id, client_id
                );
                false
            }
        }
    }

    pub fn try_lock_batch(&self, group: &str, mqs: &HashSet<MessageQueue>, client_id: &str) -> HashSet<MessageQueue> {
        let mut lock_mqs = HashSet::with_capacity(mqs.len());
        let mut not_locked_mqs = HashSet::with_capacity(mqs.len());
        for mq in mqs.iter() {
            if self.is_locked(group, mq, client_id) {
                lock_mqs.insert(mq.clone());
            } else {
                not_locked_mqs.insert(mq.clone());
            }
        }
        if !not_locked_mqs.is_empty() {
            let mut write_guard = self.mq_lock_table.write();
            let group_value = write_guard
                .entry(group.to_string())
                .or_insert(HashMap::with_capacity(32));

            for mq in not_locked_mqs {
                let lock_entry = group_value.entry(mq.clone()).or_insert_with(|| {
                    info!(
                        "RebalanceLockManager#tryLockBatch: lock a message which has not been locked yet, group={}, \
                         clientId={}, mq={:?}",
                        group, client_id, mq
                    );
                    LockEntry {
                        client_id: client_id.to_string(),
                        last_update_timestamp: AtomicI64::new(get_current_millis() as i64),
                    }
                });
                if lock_entry.is_locked(client_id) {
                    lock_entry
                        .last_update_timestamp
                        .store(get_current_millis() as i64, std::sync::atomic::Ordering::Relaxed);
                    lock_mqs.insert(mq);
                    continue;
                }
                let old_client_id = lock_entry.client_id.as_str().to_string();
                if lock_entry.is_expired() {
                    lock_entry.client_id = client_id.to_string();
                    lock_entry
                        .last_update_timestamp
                        .store(get_current_millis() as i64, std::sync::atomic::Ordering::Relaxed);
                    warn!(
                        "RebalanceLockManager#tryLockBatch: try to lock a expired message queue, group={} mq={:?}, \
                         old client id={}, new client id={}",
                        group, mq, old_client_id, client_id
                    );
                    lock_mqs.insert(mq);
                    continue;
                }
                warn!(
                    "RebalanceLockManager#tryLockBatch: message queue has been locked by other client, group={}, \
                     mq={:?}, locked client id={}, current client id={}",
                    group, mq, old_client_id, client_id
                );
            }
        }
        lock_mqs
    }

    pub fn unlock_batch(&self, group: &str, mqs: &HashSet<MessageQueue>, client_id: &str) {
        let mut write_guard = self.mq_lock_table.write();
        let Some(group_value) = write_guard.get_mut(group) else {
            warn!(
                "RebalanceLockManager#unlockBatch: group not exist, group={}, clientId={}, mqs={:?}",
                group, client_id, mqs
            );
            return;
        };

        for mq in mqs.iter() {
            match group_value.get(mq) {
                None => {
                    warn!(
                        "RebalanceLockManager#unlockBatch: mq not locked, group={}, clientId={}, mq={}",
                        group, client_id, mq
                    );
                }
                Some(lock_entry) if lock_entry.client_id == client_id => {
                    group_value.remove(mq);
                    info!(
                        "RebalanceLockManager#unlockBatch: unlock mq, group={}, clientId={}, mq={:?}",
                        group, client_id, mq
                    );
                }
                Some(lock_entry) => {
                    warn!(
                        "RebalanceLockManager#unlockBatch: mq locked by other client, group={}, locked clientId={}, \
                         current clientId={}, mq={:?}",
                        group, lock_entry.client_id, client_id, mq
                    );
                }
            }
        }
    }

    fn is_locked(&self, group: &str, mq: &MessageQueue, client_id: &str) -> bool {
        let lock_table = self.mq_lock_table.read();
        if let Some(group_value) = lock_table.get(group) {
            if let Some(lock_entry) = group_value.get(mq) {
                let locked = lock_entry.is_locked(client_id);
                if locked {
                    lock_entry
                        .last_update_timestamp
                        .store(get_current_millis() as i64, std::sync::atomic::Ordering::Relaxed);
                }
                return locked;
            }
        }
        false
    }
}

struct LockEntry {
    client_id: String,
    last_update_timestamp: AtomicI64,
}

impl LockEntry {
    #[inline]
    pub fn is_expired(&self) -> bool {
        let now = get_current_millis() as i64;
        let last_update_timestamp = self.last_update_timestamp.load(std::sync::atomic::Ordering::Relaxed);
        (now - last_update_timestamp) > *REBALANCE_LOCK_MAX_LIVE_TIME
    }

    #[inline]
    pub fn is_locked(&self, client_id: &str) -> bool {
        self.client_id == client_id && !self.is_expired()
    }
}

#[cfg(test)]
mod rebalance_lock_manager_tests {
    use rocketmq_common::common::message::message_queue::MessageQueue;

    use super::*;

    #[test]
    fn lock_all_expired_returns_true_when_no_locks_exist() {
        let manager = RebalanceLockManager::default();
        assert!(manager.is_lock_all_expired("test_group"));
    }

    #[test]
    fn lock_all_expired_returns_false_when_active_locks_exist() {
        let manager = RebalanceLockManager::default();
        let mq = MessageQueue::default();
        let mut set = HashSet::new();
        set.insert(mq.clone());
        manager.try_lock_batch("test_group", &set, "client_1");
        assert!(!manager.is_lock_all_expired("test_group"));
    }

    #[test]
    fn try_lock_batch_locks_message_queues_for_new_group() {
        let manager = RebalanceLockManager::default();
        let mq = MessageQueue::default();
        let mut set = HashSet::new();
        set.insert(mq.clone());
        let locked_mqs = manager.try_lock_batch("test_group", &set, "client_1");
        assert_eq!(locked_mqs.len(), 1);
    }

    #[test]
    fn try_lock_batch_does_not_lock_already_locked_message_queues() {
        let manager = RebalanceLockManager::default();
        let mq = MessageQueue::default();
        let mut set = HashSet::new();
        set.insert(mq.clone());
        manager.try_lock_batch("test_group", &set, "client_1");
        let locked_mqs = manager.try_lock_batch("test_group", &set, "client_2");
        assert!(locked_mqs.is_empty());
    }

    #[test]
    fn unlock_batch_unlocks_message_queues_locked_by_client() {
        let manager = RebalanceLockManager::default();
        let mq = MessageQueue::default();
        let mut set = HashSet::new();
        set.insert(mq.clone());
        manager.try_lock_batch("test_group", &set, "client_1");
        manager.unlock_batch("test_group", &set, "client_1");
        let locked_mqs = manager.try_lock_batch("test_group", &set, "client_2");
        assert_eq!(locked_mqs.len(), 1);
    }

    #[test]
    fn unlock_batch_does_not_unlock_message_queues_locked_by_other_clients() {
        let manager = RebalanceLockManager::default();
        let mq = MessageQueue::default();
        let mut set = HashSet::new();
        set.insert(mq.clone());
        manager.try_lock_batch("test_group", &set, "client_1");
        manager.unlock_batch("test_group", &set, "client_2");
        assert!(!manager.is_lock_all_expired("test_group"));
    }

    #[test]
    fn is_locked_returns_true_for_locked_message_queue() {
        let manager = RebalanceLockManager::default();
        let mq = MessageQueue::default();
        let mut set = HashSet::new();
        set.insert(mq.clone());
        manager.try_lock_batch("test_group", &set, "client_1");
        assert!(manager.is_locked("test_group", &mq, "client_1"));
    }

    #[test]
    fn is_locked_returns_false_for_unlocked_message_queue() {
        let manager = RebalanceLockManager::default();
        let mq = MessageQueue::default();
        assert!(!manager.is_locked("test_group", &mq, "client_1"));
    }

    #[test]
    fn try_lock_locks_single_message_queue() {
        let manager = RebalanceLockManager::default();
        let mq = MessageQueue::default();
        assert!(manager.try_lock("test_group", &mq, "client_1"));
        // Same client can re-lock
        assert!(manager.try_lock("test_group", &mq, "client_1"));
    }

    #[test]
    fn try_lock_fails_when_locked_by_other_client() {
        let manager = RebalanceLockManager::default();
        let mq = MessageQueue::default();
        assert!(manager.try_lock("test_group", &mq, "client_1"));
        // Different client cannot lock
        assert!(!manager.try_lock("test_group", &mq, "client_2"));
    }

    #[test]
    fn try_lock_and_try_lock_batch_interoperate() {
        let manager = RebalanceLockManager::default();
        let mq = MessageQueue::default();
        // Lock via try_lock
        assert!(manager.try_lock("test_group", &mq, "client_1"));
        // try_lock_batch should recognize the lock
        let mut set = HashSet::new();
        set.insert(mq.clone());
        let locked = manager.try_lock_batch("test_group", &set, "client_1");
        assert_eq!(locked.len(), 1);
        // Other client cannot lock via batch
        let locked = manager.try_lock_batch("test_group", &set, "client_2");
        assert!(locked.is_empty());
    }
}
