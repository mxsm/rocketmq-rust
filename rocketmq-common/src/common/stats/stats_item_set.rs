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
use std::time::SystemTime;

use dashmap::DashMap;

use crate::common::stats::stats_item::StatsItem;
use crate::common::stats::stats_snapshot::StatsSnapshot;

/// Manages a collection of StatsItem instances indexed by key
/// Each StatsItemSet represents one statistics category (e.g., TOPIC_PUT_NUMS)
#[derive(Clone, Debug)]
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
        if let Some(item) = self.items.get(key) {
            return Arc::clone(&*item);
        }
        self.items
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(StatsItem::new(&self.stats_name, key)))
            .clone()
    }

    /// Add value to the stats item identified by key.
    ///
    /// Equivalent to Java `addValue(key, incValue, incTimes)`.
    pub fn add_value(&self, stats_key: &str, inc_value: impl Into<i64>, inc_times: impl Into<i64>) {
        let item = self.get_or_create_stats_item(stats_key);
        item.add(inc_value.into() as u64, inc_times.into() as u64);
    }

    /// Add RT (Response Time) value for latency metrics.
    ///
    /// Equivalent to Java `addRTValue(key, incValue, incTimes)`.
    pub fn add_rt_value(&self, stats_key: &str, inc_value: impl Into<i64>, inc_times: impl Into<i64>) {
        self.add_value(stats_key, inc_value.into(), inc_times.into());
    }

    /// Get statistics snapshot for the last minute
    pub fn get_stats_data_in_minute(&self, stats_key: &str) -> StatsSnapshot {
        self.items
            .get(stats_key)
            .map(|item| item.get_stats_data_in_minute())
            .unwrap_or_default()
    }

    /// Get statistics snapshot for the last hour
    pub fn get_stats_data_in_hour(&self, stats_key: &str) -> StatsSnapshot {
        self.items
            .get(stats_key)
            .map(|item| item.get_stats_data_in_hour())
            .unwrap_or_default()
    }

    /// Get a StatsItem by key
    pub fn get_stats_item(&self, stats_key: &str) -> Option<Arc<StatsItem>> {
        self.items.get(stats_key).map(|entry| entry.clone())
    }

    /// Sample all items into their second-level window (`cs_list_minute`).
    ///
    /// Must be called every ~10 seconds by a background task.
    pub fn sampling_in_seconds(&self) {
        for entry in self.items.iter() {
            entry.value().sampling_in_seconds();
        }
    }

    /// Sample all items into their minute-level window (`cs_list_hour`).
    ///
    /// Must be called every ~10 minutes by a background task.
    pub fn sampling_in_minutes(&self) {
        for entry in self.items.iter() {
            entry.value().sampling_in_minutes_level();
        }
    }

    /// Sample all items into their hour-level window (`cs_list_day`).
    ///
    /// Must be called every ~1 hour by a background task.
    pub fn sampling_in_hours(&self) {
        for entry in self.items.iter() {
            entry.value().sampling_in_hour_level();
        }
    }

    /// Delete a specific stats item by key
    pub fn del_value(&self, key: &str) {
        self.items.remove(key);
    }

    /// Delete all items whose key starts with `prefix` followed immediately by `separator`.
    ///
    /// Equivalent to Java `delValueByPrefixKey(statsKey, separator)`: retains only keys where
    /// `key.starts_with(statsKey + separator)` is false.
    pub fn del_value_by_prefix_key(&self, prefix: &str, separator: &str) {
        let match_prefix = format!("{}{}", prefix, separator);
        self.items.retain(|k, _| !k.starts_with(&match_prefix));
    }

    /// Delete all items whose key ends with `separator` followed immediately by `suffix`.
    ///
    /// Equivalent to Java `delValueBySuffixKey(statsKey, separator)`: retains only keys where
    /// `key.endsWith(separator + statsKey)` is false.
    pub fn del_value_by_suffix_key(&self, suffix: &str, separator: &str) {
        let match_suffix = format!("{}{}", separator, suffix);
        self.items.retain(|k, _| !k.ends_with(&match_suffix));
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

    /// Evict items that have not been updated for longer than `max_idle_minutes`.
    ///
    /// Equivalent to Java `cleanResource(maxStatsIdleTimeInMinutes)`.
    pub fn clean_resource(&self, max_idle_minutes: u64) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let threshold_ms = max_idle_minutes * 60 * 1000;
        self.items
            .retain(|_, v| now.saturating_sub(v.get_last_update_timestamp()) <= threshold_ms);
    }
}
