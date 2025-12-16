//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

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
        let lock_entry = lock_table.get(group);
        if lock_entry.is_none() {
            return true;
        }
        let lock_entry = lock_entry.unwrap();
        for (_, entry) in lock_entry.iter() {
            if !entry.is_expired() {
                return false;
            }
        }
        true
    }

    pub fn try_lock_batch(
        &self,
        group: &str,
        mqs: &HashSet<MessageQueue>,
        client_id: &str,
    ) -> HashSet<MessageQueue> {
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
                        "RebalanceLockManager#tryLockBatch: lock a message which has not been \
                         locked yet, group={}, clientId={}, mq={:?}",
                        group, client_id, mq
                    );
                    LockEntry {
                        client_id: client_id.to_string(),
                        last_update_timestamp: AtomicI64::new(get_current_millis() as i64),
                    }
                });
                if lock_entry.is_locked(client_id) {
                    lock_entry.last_update_timestamp.store(
                        get_current_millis() as i64,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    lock_mqs.insert(mq);
                    continue;
                }
                let old_client_id = lock_entry.client_id.as_str().to_string();
                if lock_entry.is_expired() {
                    lock_entry.client_id = client_id.to_string();
                    lock_entry.last_update_timestamp.store(
                        get_current_millis() as i64,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    warn!(
                        "RebalanceLockManager#tryLockBatch: try to lock a expired message queue, \
                         group={} mq={:?}, old client id={}, new client id={}",
                        group, mq, old_client_id, client_id
                    );
                    lock_mqs.insert(mq);
                    continue;
                }
                warn!(
                    "RebalanceLockManager#tryLockBatch: message queue has been locked by other \
                     group={}, mq={:?}, locked client id={}, current client id={}",
                    group, mq, old_client_id, client_id
                );
            }
        }
        lock_mqs
    }

    pub fn unlock_batch(&self, group: &str, mqs: &HashSet<MessageQueue>, client_id: &str) {
        let mut write_guard = self.mq_lock_table.write();
        let group_value = write_guard.get_mut(group);
        if group_value.is_none() {
            warn!(
                "RebalanceLockManager#unlockBatch: group not exist, group={}, clientId={}, \
                 mqs={:?}",
                group, client_id, mqs
            );
            return;
        }
        let group_value = group_value.unwrap();
        for mq in mqs.iter() {
            let lock_entry = group_value.get(mq);
            if lock_entry.is_none() {
                warn!(
                    "RebalanceLockManager#unlockBatch: mq not locked, group={}, clientId={}, mq={}",
                    group, client_id, mq
                );
                continue;
            }
            let lock_entry = lock_entry.unwrap();
            if lock_entry.client_id == *client_id {
                group_value.remove(mq);
                info!(
                    "RebalanceLockManager#unlockBatch: unlock a message queue, group={}, \
                     clientId={}, mq={:?}",
                    group, client_id, mq
                );
            } else {
                warn!(
                    "RebalanceLockManager#unlockBatch: unlock a message queue, but the client id \
                     is not matched, group={}, clientId={}, mq={:?}",
                    group, client_id, mq
                );
            }
        }
    }

    fn is_locked(&self, group: &str, mq: &MessageQueue, client_id: &str) -> bool {
        let lock_table = self.mq_lock_table.read();
        let group_value = lock_table.get(group);
        if group_value.is_none() {
            return false;
        }
        let group_value = group_value.unwrap();
        let lock_entry = group_value.get(mq);
        if lock_entry.is_none() {
            return false;
        }
        let lock_entry = lock_entry.unwrap();
        let locked = lock_entry.is_locked(client_id);
        if locked {
            lock_entry.last_update_timestamp.store(
                get_current_millis() as i64,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        locked
    }
}

struct LockEntry {
    client_id: String,
    last_update_timestamp: AtomicI64,
}

impl LockEntry {
    pub fn new() -> LockEntry {
        Self {
            client_id: "".to_string(),
            last_update_timestamp: AtomicI64::new(get_current_millis() as i64),
        }
    }

    #[inline]
    pub fn is_expired(&self) -> bool {
        let now = get_current_millis() as i64;
        let last_update_timestamp = self
            .last_update_timestamp
            .load(std::sync::atomic::Ordering::Relaxed);
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
}
