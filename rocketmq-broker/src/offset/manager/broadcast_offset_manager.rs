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
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rocketmq_common::TimeUtils::current_millis;

const TOPIC_GROUP_SEPARATOR: &str = "@";
const BROADCAST_GROUP_SUFFIX: &str = "broadcast";
pub(crate) const SCAN_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_OFFLINE_EXPIRY: Duration = Duration::from_secs(5 * 60);
const DEFAULT_ONLINE_EXPIRY: Duration = Duration::from_secs(24 * 60 * 60);

type QueryPersistedOffset = Arc<dyn Fn(&str, &str, i32) -> i64 + Send + Sync>;
type CommitPersistedOffset = Arc<dyn Fn(&str, &str, i32, i64) + Send + Sync>;
type QueryInitialOffset = Arc<dyn Fn(&str, i32) -> i64 + Send + Sync>;
type IsClientOnline = Arc<dyn Fn(&str, &str) -> bool + Send + Sync>;
type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

#[derive(Clone)]
struct BroadcastOffsetPorts {
    query_persisted: QueryPersistedOffset,
    commit_persisted: CommitPersistedOffset,
    query_initial: QueryInitialOffset,
    is_client_online: IsClientOnline,
    now_millis: Clock,
}

impl Default for BroadcastOffsetPorts {
    fn default() -> Self {
        Self {
            query_persisted: Arc::new(|_, _, _| -1),
            commit_persisted: Arc::new(|_, _, _, _| {}),
            query_initial: Arc::new(|_, _| -1),
            is_client_online: Arc::new(|_, _| false),
            now_millis: Arc::new(current_millis),
        }
    }
}

#[derive(Debug)]
struct BroadcastTimedOffsetStore {
    timestamp: u64,
    from_proxy: bool,
    queue_offsets: HashMap<i32, i64>,
}

impl BroadcastTimedOffsetStore {
    fn new(from_proxy: bool, timestamp: u64) -> Self {
        Self {
            timestamp,
            from_proxy,
            queue_offsets: HashMap::new(),
        }
    }

    fn update_monotonic(&mut self, queue_id: i32, offset: i64, timestamp: u64, from_proxy: bool) {
        self.timestamp = timestamp;
        self.from_proxy = from_proxy;
        self.queue_offsets
            .entry(queue_id)
            .and_modify(|current| *current = (*current).max(offset))
            .or_insert(offset);
    }
}

#[derive(Debug)]
struct BroadcastOffsetData {
    topic: String,
    group: String,
    clients: HashMap<String, BroadcastTimedOffsetStore>,
}

impl BroadcastOffsetData {
    fn new(topic: &str, group: &str) -> Self {
        Self {
            topic: topic.to_owned(),
            group: group.to_owned(),
            clients: HashMap::new(),
        }
    }
}

#[derive(Default)]
struct BroadcastOffsetState {
    offsets: HashMap<String, BroadcastOffsetData>,
}

#[derive(Clone)]
pub struct BroadcastOffsetManager {
    state: Arc<Mutex<BroadcastOffsetState>>,
    ports: BroadcastOffsetPorts,
    offline_expiry: Duration,
    online_expiry: Duration,
}

impl std::fmt::Debug for BroadcastOffsetManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BroadcastOffsetManager")
            .field("offline_expiry", &self.offline_expiry)
            .field("online_expiry", &self.online_expiry)
            .finish_non_exhaustive()
    }
}

impl Default for BroadcastOffsetManager {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(BroadcastOffsetState::default())),
            ports: BroadcastOffsetPorts::default(),
            offline_expiry: DEFAULT_OFFLINE_EXPIRY,
            online_expiry: DEFAULT_ONLINE_EXPIRY,
        }
    }
}

impl BroadcastOffsetManager {
    pub(crate) fn new(
        query_persisted: impl Fn(&str, &str, i32) -> i64 + Send + Sync + 'static,
        commit_persisted: impl Fn(&str, &str, i32, i64) + Send + Sync + 'static,
        query_initial: impl Fn(&str, i32) -> i64 + Send + Sync + 'static,
        is_client_online: impl Fn(&str, &str) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            ports: BroadcastOffsetPorts {
                query_persisted: Arc::new(query_persisted),
                commit_persisted: Arc::new(commit_persisted),
                query_initial: Arc::new(query_initial),
                is_client_online: Arc::new(is_client_online),
                now_millis: Arc::new(current_millis),
            },
            ..Self::default()
        }
    }

    pub(crate) fn pull_capability(&self) -> BroadcastOffsetCapability {
        BroadcastOffsetCapability { manager: self.clone() }
    }

    pub(crate) fn query_init_offset(
        &self,
        topic: &str,
        group_id: &str,
        queue_id: i32,
        client_id: &str,
        request_offset: i64,
        from_proxy: bool,
    ) -> i64 {
        let key = build_key(topic, group_id);
        let now = (self.ports.now_millis)();
        let mut state = self.state.lock();
        let Some(data) = state.offsets.get_mut(&key) else {
            drop(state);
            return if from_proxy && request_offset < 0 {
                self.resolve_offset(None, topic, group_id, queue_id)
            } else {
                -1
            };
        };
        let offset_store = data
            .clients
            .entry(client_id.to_owned())
            .or_insert_with(|| BroadcastTimedOffsetStore::new(from_proxy, now));
        let should_initialize =
            (offset_store.from_proxy && request_offset < 0) || offset_store.from_proxy != from_proxy;
        if !should_initialize {
            return -1;
        }
        let client_offset = offset_store.queue_offsets.get(&queue_id).copied();
        drop(state);
        self.resolve_offset(client_offset, topic, group_id, queue_id)
    }

    pub(crate) fn update_offset(
        &self,
        topic: &str,
        group: &str,
        queue_id: i32,
        offset: i64,
        client_id: &str,
        from_proxy: bool,
    ) {
        let now = (self.ports.now_millis)();
        let mut state = self.state.lock();
        let data = state
            .offsets
            .entry(build_key(topic, group))
            .or_insert_with(|| BroadcastOffsetData::new(topic, group));
        data.clients
            .entry(client_id.to_owned())
            .or_insert_with(|| BroadcastTimedOffsetStore::new(from_proxy, now))
            .update_monotonic(queue_id, offset, now, from_proxy);
    }

    pub(crate) fn scan_offset_data(&self) {
        let now = (self.ports.now_millis)();
        let offline_expiry = self.offline_expiry.as_millis() as u64;
        let online_expiry = self.online_expiry.as_millis() as u64;
        let mut commits = Vec::new();
        {
            let mut state = self.state.lock();
            state.offsets.retain(|_, data| {
                let mut queue_min_offsets = HashMap::<i32, i64>::new();
                data.clients.retain(|client_id, offset_store| {
                    let elapsed = now.saturating_sub(offset_store.timestamp);
                    let online = (self.ports.is_client_online)(&data.group, client_id);
                    let include = online || elapsed < offline_expiry;
                    if include {
                        for (queue_id, offset) in &offset_store.queue_offsets {
                            queue_min_offsets
                                .entry(*queue_id)
                                .and_modify(|current| *current = (*current).min(*offset))
                                .or_insert(*offset);
                        }
                    }
                    if online {
                        elapsed < online_expiry
                    } else {
                        elapsed < offline_expiry
                    }
                });
                commits.extend(
                    queue_min_offsets
                        .into_iter()
                        .map(|(queue_id, offset)| (data.topic.clone(), data.group.clone(), queue_id, offset)),
                );
                !data.clients.is_empty()
            });
        }
        for (topic, group, queue_id, offset) in commits {
            (self.ports.commit_persisted)(&topic, &group, queue_id, offset);
        }
    }

    pub(crate) fn shutdown(&self) {
        self.scan_offset_data();
    }

    fn resolve_offset(&self, client_offset: Option<i64>, topic: &str, group: &str, queue_id: i32) -> i64 {
        if let Some(offset) = client_offset.filter(|offset| *offset >= 0) {
            return offset;
        }
        let persisted = (self.ports.query_persisted)(topic, &broadcast_group_id(group), queue_id);
        if persisted >= 0 {
            persisted
        } else {
            (self.ports.query_initial)(topic, queue_id)
        }
    }

    #[cfg(test)]
    fn with_clock_and_expiry(
        mut self,
        now_millis: impl Fn() -> u64 + Send + Sync + 'static,
        offline_expiry: Duration,
        online_expiry: Duration,
    ) -> Self {
        self.ports.now_millis = Arc::new(now_millis);
        self.offline_expiry = offline_expiry;
        self.online_expiry = online_expiry;
        self
    }
}

#[derive(Clone, Debug)]
pub(crate) struct BroadcastOffsetCapability {
    manager: BroadcastOffsetManager,
}

impl BroadcastOffsetCapability {
    pub(crate) fn query_init_offset(
        &self,
        topic: &str,
        group_id: &str,
        queue_id: i32,
        client_id: &str,
        request_offset: i64,
        from_proxy: bool,
    ) -> i64 {
        self.manager
            .query_init_offset(topic, group_id, queue_id, client_id, request_offset, from_proxy)
    }

    pub(crate) fn update_offset(
        &self,
        topic: &str,
        group: &str,
        queue_id: i32,
        offset: i64,
        client_id: &str,
        from_proxy: bool,
    ) {
        self.manager
            .update_offset(topic, group, queue_id, offset, client_id, from_proxy);
    }
}

fn build_key(topic: &str, group: &str) -> String {
    format!("{topic}{TOPIC_GROUP_SEPARATOR}{group}")
}

fn broadcast_group_id(group: &str) -> String {
    format!("{group}{TOPIC_GROUP_SEPARATOR}{BROADCAST_GROUP_SUFFIX}")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;

    use super::*;

    #[test]
    fn capability_shares_monotonic_offsets_with_manager() {
        let manager = BroadcastOffsetManager::default();
        let capability = manager.pull_capability();
        capability.update_offset("topic-a", "group-a", 0, 12, "client-a", false);
        capability.update_offset("topic-a", "group-a", 0, 7, "client-a", true);

        assert_eq!(
            manager.query_init_offset("topic-a", "group-a", 0, "client-a", -1, true),
            12
        );
    }

    #[test]
    fn query_falls_back_to_persisted_then_store_offset() {
        let persisted = BroadcastOffsetManager::new(
            |_, group, _| {
                if group == "group-a@broadcast" {
                    41
                } else {
                    -1
                }
            },
            |_, _, _, _| {},
            |_, _| 99,
            |_, _| false,
        );
        assert_eq!(
            persisted.query_init_offset("topic-a", "group-a", 0, "proxy-a", -1, true),
            41
        );

        let store = BroadcastOffsetManager::new(|_, _, _| -1, |_, _, _, _| {}, |_, _| 99, |_, _| false);
        assert_eq!(
            store.query_init_offset("topic-a", "group-a", 0, "proxy-a", -1, true),
            99
        );
    }

    #[test]
    fn scan_expires_offline_clients_and_persists_queue_minimum() {
        let now = Arc::new(AtomicU64::new(1_000));
        let commits = Arc::new(Mutex::new(Vec::new()));
        let commits_for_port = Arc::clone(&commits);
        let now_for_clock = Arc::clone(&now);
        let manager = BroadcastOffsetManager::new(
            |_, _, _| -1,
            move |topic, group, queue_id, offset| {
                commits_for_port
                    .lock()
                    .push((topic.to_owned(), group.to_owned(), queue_id, offset));
            },
            |_, _| -1,
            |_, client| client == "online",
        )
        .with_clock_and_expiry(
            move || now_for_clock.load(Ordering::Acquire),
            Duration::from_millis(100),
            Duration::from_millis(500),
        );
        manager.update_offset("topic-a", "group-a", 0, 20, "offline", false);
        manager.update_offset("topic-a", "group-a", 0, 30, "online", false);
        now.store(1_150, Ordering::Release);

        manager.scan_offset_data();

        assert_eq!(
            commits.lock().as_slice(),
            &[("topic-a".to_owned(), "group-a".to_owned(), 0, 30)]
        );
        assert_eq!(
            manager.query_init_offset("topic-a", "group-a", 0, "offline", -1, true),
            -1
        );
    }

    #[test]
    fn concurrent_updates_preserve_the_highest_offset() {
        let manager = BroadcastOffsetManager::default();
        let workers = (0..8)
            .map(|worker| {
                let capability = manager.pull_capability();
                std::thread::spawn(move || {
                    for sequence in 0..100 {
                        capability.update_offset("topic-a", "group-a", 0, worker * 100 + sequence, "client-a", false);
                    }
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().expect("broadcast offset worker should not panic");
        }

        let capability = manager.pull_capability();
        capability.update_offset("topic-a", "group-a", 0, 0, "client-a", true);
        assert_eq!(
            capability.query_init_offset("topic-a", "group-a", 0, "client-a", -1, true),
            799
        );
    }
}
