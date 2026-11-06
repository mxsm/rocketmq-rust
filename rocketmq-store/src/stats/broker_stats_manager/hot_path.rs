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

//! Statistics handles for the per-message send and pull paths.
//!
//! Every update used to look up its item set by name and then its item by key, both in shared
//! `DashMap`s, so all cores writing one topic contended on the same shard locks. The sets are now
//! resolved once, the cluster-wide items are kept, and a send's topic items are cached per thread.
//! A cached item removed from its set (topic or group deletion) is retired, and the next update
//! resolves it again, so updates are reported exactly as through the sets.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Write;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use dashmap::DashMap;
use rocketmq_observability::stats::stats_item::StatsItem;
use rocketmq_observability::stats::stats_item_set::StatsItemSet;
use rocketmq_observability::stats::with_stats_key;
use rocketmq_observability::stats::Stats;

use super::BrokerStatsManager;

/// Topics whose put items one thread keeps; the cache is emptied when it would grow past this.
const CACHED_TOPICS_PER_THREAD: usize = 256;
/// Queues per topic whose put-latency item is cached; higher queue ids update through the set.
const CACHED_LATENCY_QUEUES: usize = 64;

static NEXT_MANAGER_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static PUT_ITEMS: RefCell<PutItemCache> = RefCell::new(PutItemCache::default());
}

/// Identifies a manager in the per-thread cache, which serves one manager at a time.
pub(super) fn next_manager_id() -> u64 {
    NEXT_MANAGER_ID.fetch_add(1, Ordering::Relaxed)
}

/// Converts a delta the way [`StatsItemSet::add_value`] does.
#[inline]
fn counter(value: i32) -> u64 {
    i64::from(value) as u64
}

/// Item sets updated by every send or pull, resolved by [`BrokerStatsManager::init`]. Each one
/// shares its items with the same-named set of the stats table, so the table reports every update.
#[derive(Default)]
pub(super) struct HotStatsSets {
    pub(super) topic_put_nums: Option<StatsItemSet>,
    pub(super) topic_put_size: Option<StatsItemSet>,
    pub(super) topic_put_latency: Option<StatsItemSet>,
    pub(super) broker_put_nums: Option<StatsItemSet>,
    pub(super) broker_put_nums_without_system_topic: Option<StatsItemSet>,
    pub(super) group_get_nums: Option<StatsItemSet>,
    pub(super) group_get_size: Option<StatsItemSet>,
    pub(super) group_get_latency: Option<StatsItemSet>,
    pub(super) broker_get_nums: Option<StatsItemSet>,
    pub(super) broker_get_nums_without_system_topic: Option<StatsItemSet>,
    pub(super) queue_put_nums: Option<StatsItemSet>,
    pub(super) queue_put_size: Option<StatsItemSet>,
    pub(super) queue_get_nums: Option<StatsItemSet>,
    pub(super) queue_get_size: Option<StatsItemSet>,
}

impl HotStatsSets {
    pub(super) fn resolve(table: &DashMap<String, StatsItemSet>) -> Self {
        let set = |name: &str| table.get(name).map(|set| set.clone());
        Self {
            topic_put_nums: set(Stats::TOPIC_PUT_NUMS),
            topic_put_size: set(Stats::TOPIC_PUT_SIZE),
            topic_put_latency: set(BrokerStatsManager::TOPIC_PUT_LATENCY),
            broker_put_nums: set(Stats::BROKER_PUT_NUMS),
            broker_put_nums_without_system_topic: set(BrokerStatsManager::BROKER_PUT_NUMS_WITHOUT_SYSTEM_TOPIC),
            group_get_nums: set(Stats::GROUP_GET_NUMS),
            group_get_size: set(Stats::GROUP_GET_SIZE),
            group_get_latency: set(Stats::GROUP_GET_LATENCY),
            broker_get_nums: set(Stats::BROKER_GET_NUMS),
            broker_get_nums_without_system_topic: set(BrokerStatsManager::BROKER_GET_NUMS_WITHOUT_SYSTEM_TOPIC),
            queue_put_nums: set(Stats::QUEUE_PUT_NUMS),
            queue_put_size: set(Stats::QUEUE_PUT_SIZE),
            queue_get_nums: set(Stats::QUEUE_GET_NUMS),
            queue_get_size: set(Stats::QUEUE_GET_SIZE),
        }
    }
}

/// Items keyed by the cluster name, which every send or pull of the Broker updates.
#[derive(Default)]
pub(super) struct ClusterStatsItems {
    pub(super) put_nums: Option<Arc<StatsItem>>,
    pub(super) put_nums_without_system_topic: Option<Arc<StatsItem>>,
    pub(super) get_nums: Option<Arc<StatsItem>>,
    pub(super) get_nums_without_system_topic: Option<Arc<StatsItem>>,
}

impl ClusterStatsItems {
    pub(super) fn resolve(sets: &HotStatsSets, cluster_name: &str) -> Self {
        let item = |set: &Option<StatsItemSet>| set.as_ref().map(|set| set.get_or_create_stats_item(cluster_name));
        Self {
            put_nums: item(&sets.broker_put_nums),
            put_nums_without_system_topic: item(&sets.broker_put_nums_without_system_topic),
            get_nums: item(&sets.broker_get_nums),
            get_nums_without_system_topic: item(&sets.broker_get_nums_without_system_topic),
        }
    }
}

/// Adds to a kept item, or through its set when the item has been removed since it was kept.
#[inline]
pub(super) fn add_kept(item: Option<&Arc<StatsItem>>, set: Option<&StatsItemSet>, key: &str, value: i32) {
    match item {
        Some(item) if !item.is_retired() => item.add(counter(value), 1),
        _ => {
            if let Some(set) = set {
                set.add_value(key, value, 1);
            }
        }
    }
}

fn topic_put_latency_item(set: &StatsItemSet, topic: &str, queue_id: i32) -> Arc<StatsItem> {
    with_stats_key(
        |key| {
            let _ = write!(key, "{queue_id}@{topic}");
        },
        |key| set.get_or_create_stats_item(key),
    )
}

/// One successful put, as passed to [`BrokerStatsManager::record_put_success`].
struct PutSample<'a> {
    topic: &'a str,
    queue_id: i32,
    msg_num: i32,
    wrote_bytes: i32,
    latency_ms: i32,
}

/// The sets a put updates through its cached items.
#[derive(Clone, Copy)]
struct TopicPutSets<'a> {
    nums: &'a StatsItemSet,
    size: &'a StatsItemSet,
    latency: &'a StatsItemSet,
}

struct TopicPutItems {
    nums: Arc<StatsItem>,
    size: Arc<StatsItem>,
    /// Put-latency items by queue id, resolved on first use.
    latency: Vec<Option<Arc<StatsItem>>>,
}

impl TopicPutItems {
    fn resolve(sets: TopicPutSets<'_>, topic: &str) -> Self {
        Self {
            nums: sets.nums.get_or_create_stats_item(topic),
            size: sets.size.get_or_create_stats_item(topic),
            latency: Vec::new(),
        }
    }

    fn is_stale(&self) -> bool {
        self.nums.is_retired() || self.size.is_retired()
    }

    fn add(&mut self, sets: TopicPutSets<'_>, sample: &PutSample<'_>) {
        self.nums.add(counter(sample.msg_num), 1);
        self.size.add(counter(sample.wrote_bytes), 1);
        let Some(index) = usize::try_from(sample.queue_id)
            .ok()
            .filter(|index| *index < CACHED_LATENCY_QUEUES)
        else {
            topic_put_latency_item(sets.latency, sample.topic, sample.queue_id).add(counter(sample.latency_ms), 1);
            return;
        };
        if self.latency.len() <= index {
            self.latency.resize(index + 1, None);
        }
        let slot = &mut self.latency[index];
        let item = match slot {
            Some(item) if !item.is_retired() => item,
            _ => slot.insert(topic_put_latency_item(sets.latency, sample.topic, sample.queue_id)),
        };
        item.add(counter(sample.latency_ms), 1);
    }
}

#[derive(Default)]
struct PutItemCache {
    manager_id: u64,
    topics: HashMap<Box<str>, TopicPutItems>,
}

impl PutItemCache {
    fn record(&mut self, manager_id: u64, sets: TopicPutSets<'_>, sample: &PutSample<'_>) {
        if self.manager_id != manager_id {
            self.topics.clear();
            self.manager_id = manager_id;
        }
        if self.topics.get(sample.topic).is_none_or(TopicPutItems::is_stale) {
            if self.topics.len() >= CACHED_TOPICS_PER_THREAD {
                self.topics.clear();
            }
            self.topics
                .insert(Box::from(sample.topic), TopicPutItems::resolve(sets, sample.topic));
        }
        if let Some(items) = self.topics.get_mut(sample.topic) {
            items.add(sets, sample);
        }
    }
}

impl BrokerStatsManager {
    /// Records one successful put of `msg_num` messages and `wrote_bytes` bytes to `queue_id` of
    /// `topic`, which took `latency_ms`.
    ///
    /// The statistics are the same as those of `inc_topic_put_nums(topic, msg_num, 1)`,
    /// `inc_topic_put_size`, `inc_broker_put_nums` and `inc_topic_put_latency`. The topic's items
    /// are cached per thread, so a put normally takes no lock and no shared lookup.
    pub fn record_put_success(&self, topic: &str, queue_id: i32, msg_num: i32, wrote_bytes: i32, latency_ms: i32) {
        let hot = &self.hot_sets;
        match (
            hot.topic_put_nums.as_ref(),
            hot.topic_put_size.as_ref(),
            hot.topic_put_latency.as_ref(),
        ) {
            (Some(nums), Some(size), Some(latency)) => {
                let sets = TopicPutSets { nums, size, latency };
                let sample = PutSample {
                    topic,
                    queue_id,
                    msg_num,
                    wrote_bytes,
                    latency_ms,
                };
                PUT_ITEMS.with(|cache| cache.borrow_mut().record(self.manager_id, sets, &sample));
            }
            _ => {
                self.inc_topic_put_nums(topic, msg_num, 1);
                self.inc_topic_put_size(topic, wrote_bytes);
                self.inc_topic_put_latency(topic, queue_id, latency_ms);
            }
        }
        self.inc_broker_put_nums(topic, msg_num);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;

    use cheetah_string::CheetahString;
    use rocketmq_model::common::topic::TopicValidator;
    use rocketmq_runtime::RuntimeOwner;

    use super::*;
    use crate::config::store_runtime_config::StoreRuntimeConfig;

    fn manager() -> BrokerStatsManager {
        static OWNER: OnceLock<RuntimeOwner> = OnceLock::new();
        let task_group = OWNER
            .get_or_init(|| RuntimeOwner::new().expect("test runtime owner should start"))
            .root_context()
            .component("broker-stats-hot-path-test")
            .task_group()
            .clone();
        BrokerStatsManager::new(Arc::new(StoreRuntimeConfig::default()), task_group)
    }

    fn totals(manager: &BrokerStatsManager, stats_name: &str, key: &str) -> (u64, u64) {
        manager
            .get_stats_item(stats_name, key)
            .map(|item| (item.get_value(), item.get_times()))
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn record_put_success_matches_the_individual_updates() {
        let cached = manager();
        let direct = manager();
        let system = TopicValidator::RMQ_SYS_SCHEDULE_TOPIC;
        let puts = [
            ("TopicA", 0, 1, 1_024, 3),
            ("TopicA", 1, 32, 40_960, 5),
            ("TopicA", 0, 1, 512, 7),
            ("TopicB", 200, 2, 2_048, 1),
            (system, 0, 1, 100, 2),
        ];
        for (topic, queue_id, msg_num, wrote_bytes, latency_ms) in puts {
            cached.record_put_success(topic, queue_id, msg_num, wrote_bytes, latency_ms);
            direct.inc_topic_put_nums(topic, msg_num, 1);
            direct.inc_topic_put_size(topic, wrote_bytes);
            direct.inc_broker_put_nums(topic, msg_num);
            direct.inc_topic_put_latency(topic, queue_id, latency_ms);
        }

        let cluster = direct.get_cluster_name();
        let expected = [
            (Stats::TOPIC_PUT_NUMS, "TopicA", (34, 3)),
            (Stats::TOPIC_PUT_SIZE, "TopicA", (42_496, 3)),
            (Stats::TOPIC_PUT_NUMS, "TopicB", (2, 1)),
            (Stats::TOPIC_PUT_NUMS, system, (1, 1)),
            (BrokerStatsManager::TOPIC_PUT_LATENCY, "0@TopicA", (10, 2)),
            (BrokerStatsManager::TOPIC_PUT_LATENCY, "1@TopicA", (5, 1)),
            (BrokerStatsManager::TOPIC_PUT_LATENCY, "200@TopicB", (1, 1)),
            (Stats::BROKER_PUT_NUMS, cluster, (37, 5)),
            (
                BrokerStatsManager::BROKER_PUT_NUMS_WITHOUT_SYSTEM_TOPIC,
                cluster,
                (36, 4),
            ),
        ];
        for (stats_name, key, totals_expected) in expected {
            assert_eq!(totals_expected, totals(&direct, stats_name, key), "{stats_name} {key}");
            assert_eq!(totals_expected, totals(&cached, stats_name, key), "{stats_name} {key}");
        }
    }

    #[tokio::test]
    async fn cached_items_follow_topic_deletion() {
        let manager = manager();
        manager.record_put_success("DeletedTopic", 0, 2, 200, 9);
        manager.on_topic_deleted(&CheetahString::from_static_str("DeletedTopic"));
        assert!(manager.get_stats_item(Stats::TOPIC_PUT_NUMS, "DeletedTopic").is_none());

        manager.record_put_success("DeletedTopic", 0, 3, 300, 4);

        assert_eq!((3, 1), totals(&manager, Stats::TOPIC_PUT_NUMS, "DeletedTopic"));
        assert_eq!((300, 1), totals(&manager, Stats::TOPIC_PUT_SIZE, "DeletedTopic"));
        assert_eq!(
            (4, 1),
            totals(&manager, BrokerStatsManager::TOPIC_PUT_LATENCY, "0@DeletedTopic")
        );
    }

    #[tokio::test]
    async fn managers_sharing_a_thread_keep_their_own_statistics() {
        let first = manager();
        let second = manager();
        first.record_put_success("TopicA", 0, 1, 10, 1);
        second.record_put_success("TopicA", 0, 2, 20, 1);
        first.record_put_success("TopicA", 0, 4, 40, 1);

        assert_eq!((5, 2), totals(&first, Stats::TOPIC_PUT_NUMS, "TopicA"));
        assert_eq!((2, 1), totals(&second, Stats::TOPIC_PUT_NUMS, "TopicA"));
    }

    #[tokio::test]
    async fn cache_eviction_keeps_every_update() {
        let manager = manager();
        let topics = (0..CACHED_TOPICS_PER_THREAD + 10)
            .map(|index| format!("Topic{index}"))
            .collect::<Vec<_>>();
        for topic in &topics {
            manager.record_put_success(topic, 0, 1, 1, 1);
        }
        manager.record_put_success(&topics[0], 0, 1, 1, 1);

        assert_eq!((2, 2), totals(&manager, Stats::TOPIC_PUT_NUMS, &topics[0]));
        assert_eq!(
            (1, 1),
            totals(&manager, Stats::TOPIC_PUT_NUMS, &topics[CACHED_TOPICS_PER_THREAD + 9])
        );
    }

    #[tokio::test]
    async fn a_removed_cluster_item_is_updated_through_its_set() {
        let manager = manager();
        manager.inc_broker_put_nums("TopicA", 1);
        if let Some(set) = manager.get_stats_table().get(Stats::BROKER_PUT_NUMS) {
            set.clear();
        }

        manager.inc_broker_put_nums("TopicA", 2);

        assert_eq!(
            (2, 1),
            totals(&manager, Stats::BROKER_PUT_NUMS, manager.get_cluster_name())
        );
    }
}
