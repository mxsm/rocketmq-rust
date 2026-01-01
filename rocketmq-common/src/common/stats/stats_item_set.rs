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

use std::sync::Arc;

use dashmap::DashMap;

use crate::common::stats::stats_item::StatsItem;
use crate::common::stats::stats_snapshot::StatsSnapshot;

/// Manages a collection of StatsItem instances indexed by key
/// Each StatsItemSet represents one statistics category (e.g., TOPIC_PUT_NUMS)
#[derive(Debug)]
pub struct StatsItemSet {
    stats_name: String,
    items: Arc<DashMap<String, Arc<StatsItem>>>,
}

impl StatsItemSet {
    pub fn new(stats_name: String) -> Self {
        StatsItemSet {
            stats_name,
            items: Arc::new(DashMap::new()),
        }
    }

    /// Get or create a StatsItem for the given key
    pub fn get_or_create_stats_item(&self, key: &str) -> Arc<StatsItem> {
        self.items
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(StatsItem::new(&self.stats_name, key)))
            .clone()
    }

    /// Add value to the stats item identified by key
    pub fn add_value(&self, stats_key: &str, inc_value: i32, inc_times: i32) {
        let item = self.get_or_create_stats_item(stats_key);
        for _ in 0..inc_times {
            item.increment(inc_value as u64);
        }
    }

    /// Add RT (Response Time) value for latency metrics
    pub fn add_rt_value(&self, stats_key: &str, inc_value: i32, inc_times: i32) {
        self.add_value(stats_key, inc_value, inc_times);
    }

    /// Get statistics snapshot for the last minute
    pub fn get_stats_data_in_minute(&self, stats_key: &str) -> StatsSnapshot {
        self.items
            .get(stats_key)
            .map(|item| item.get_stats_data_in_minute())
            .unwrap_or_default()
    }

    /// Get a StatsItem by key
    pub fn get_stats_item(&self, stats_key: &str) -> Option<Arc<StatsItem>> {
        self.items.get(stats_key).map(|entry| entry.clone())
    }

    /// Sample all items in this set
    pub fn sampling_in_minutes(&self) {
        for entry in self.items.iter() {
            let _snapshot = entry.value().get_stats_data_in_minute();
        }
    }

    /// Delete a specific stats item by key
    pub fn del_value(&self, key: &str) {
        self.items.remove(key);
    }

    /// Delete all items whose key starts with prefix followed by separator
    /// e.g., delValueByPrefixKey("TopicA", "@") deletes "TopicA@queue0", "TopicA@queue1"
    pub fn del_value_by_prefix_key(&self, prefix: &str, separator: &str) {
        self.items.retain(|k, _| {
            if let Some(pos) = k.find(separator) {
                !k[..pos].starts_with(prefix)
            } else {
                true
            }
        });
    }

    /// Delete all items whose key ends with suffix preceded by separator
    /// e.g., delValueBySuffixKey("GroupA", "@") deletes "TopicA@GroupA", "TopicB@GroupA"
    pub fn del_value_by_suffix_key(&self, suffix: &str, separator: &str) {
        self.items.retain(|k, _| {
            if let Some(pos) = k.rfind(separator) {
                !k[pos + separator.len()..].starts_with(suffix)
            } else {
                true
            }
        });
    }

    /// Delete all items whose key contains infix surrounded by separator
    /// e.g., delValueByInfixKey("TopicA", "@") deletes "queue0@TopicA@GroupA"
    pub fn del_value_by_infix_key(&self, infix: &str, separator: &str) {
        let pattern = format!("{}{}{}", separator, infix, separator);
        self.items.retain(|k, _| !k.contains(&pattern));
    }

    /// Get the number of items in this set
    pub fn size(&self) -> usize {
        self.items.len()
    }

    /// Clear all items
    pub fn clear(&self) {
        self.items.clear();
    }
}
