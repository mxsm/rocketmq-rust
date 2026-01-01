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
use std::sync::Mutex;

use dashmap::DashMap;
use tokio::task;
use tokio::time::interval;
use tokio::time::Duration;

use crate::common::stats::moment_stats_item::MomentStatsItem;
use crate::TimeUtils::get_current_millis;
use crate::UtilAll::compute_next_minutes_time_millis;

#[derive(Clone)]
pub struct MomentStatsItemSet {
    stats_item_table: Arc<DashMap<String, MomentStatsItem>>,
    stats_name: String,
    scheduled_task: Arc<Mutex<Option<task::JoinHandle<()>>>>,
}

impl MomentStatsItemSet {
    pub fn new(stats_name: String) -> Self {
        let stats_item_table = Arc::new(DashMap::new());
        let scheduled_task = Arc::new(Mutex::new(None));
        let set = MomentStatsItemSet {
            stats_item_table,
            stats_name,
            scheduled_task,
        };
        set.init();
        set
    }

    pub fn get_stats_item_table(&self) -> Arc<DashMap<String, MomentStatsItem>> {
        Arc::clone(&self.stats_item_table)
    }

    pub fn get_stats_name(&self) -> &str {
        &self.stats_name
    }

    pub fn init(&self) {
        let stats_item_table = Arc::clone(&self.stats_item_table);
        let initial_delay = Duration::from_millis(
            (compute_next_minutes_time_millis() as i64 - get_current_millis() as i64).unsigned_abs(),
        );

        let mut interval = tokio::time::interval(Duration::from_secs(300));

        task::spawn(async move {
            tokio::time::sleep(initial_delay).await;
            loop {
                interval.tick().await;
                MomentStatsItemSet::print_at_minutes(&stats_item_table);
            }
        });
    }

    fn print_at_minutes(stats_item_table: &DashMap<String, MomentStatsItem>) {
        for entry in stats_item_table.iter() {
            entry.value().print_at_minutes();
        }
    }

    pub fn set_value(&self, stats_key: &str, value: i32) {
        let stats_item = self.get_and_create_stats_item(stats_key.to_string());
        stats_item
            .get_value()
            .store(value as i64, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn del_value_by_infix_key(&self, stats_key: &str, separator: &str) {
        let to_remove: Vec<String> = self
            .stats_item_table
            .iter()
            .filter(|entry| entry.key().contains(&format!("{separator}{stats_key}{separator}")))
            .map(|entry| entry.key().clone())
            .collect();
        for key in to_remove {
            self.stats_item_table.remove(&key);
        }
    }

    pub fn del_value_by_suffix_key(&self, stats_key: &str, separator: &str) {
        let to_remove: Vec<String> = self
            .stats_item_table
            .iter()
            .filter(|entry| entry.key().ends_with(&format!("{separator}{stats_key}")))
            .map(|entry| entry.key().clone())
            .collect();
        for key in to_remove {
            self.stats_item_table.remove(&key);
        }
    }

    pub fn get_and_create_stats_item(&self, stats_key: String) -> MomentStatsItem {
        if let Some(stats_item) = self.stats_item_table.get(&stats_key) {
            return stats_item.clone();
        }

        let new_item = MomentStatsItem::new(self.stats_name.clone(), stats_key.clone());
        self.stats_item_table.insert(stats_key, new_item.clone());
        new_item
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use dashmap::DashMap;

    use super::*;

    #[tokio::test]
    async fn moment_stats_item_set_initializes_with_empty_table() {
        let stats_set = MomentStatsItemSet::new("TestName".to_string());
        assert!(stats_set.get_stats_item_table().is_empty());
    }

    #[tokio::test]
    async fn moment_stats_item_set_returns_correct_stats_name() {
        let stats_set = MomentStatsItemSet::new("TestName".to_string());
        assert_eq!(stats_set.get_stats_name(), "TestName");
    }

    #[tokio::test]
    async fn moment_stats_item_set_creates_and_returns_stats_item() {
        let stats_set = MomentStatsItemSet::new("TestName".to_string());
        let stats_item = stats_set.get_and_create_stats_item("TestKey".to_string());
        assert_eq!(stats_item.get_stats_name(), "TestName");
        assert_eq!(stats_item.get_stats_key(), "TestKey");
    }

    #[tokio::test]
    async fn moment_stats_item_set_sets_and_gets_value() {
        let stats_set = MomentStatsItemSet::new("TestName".to_string());
        stats_set.set_value("TestKey", 10);
        let stats_item = stats_set.get_and_create_stats_item("TestKey".to_string());
        assert_eq!(stats_item.get_value().load(std::sync::atomic::Ordering::Relaxed), 10);
    }

    #[tokio::test]
    async fn moment_stats_item_set_deletes_value_by_infix_key() {
        let stats_set = MomentStatsItemSet::new("TestName".to_string());
        stats_set.set_value("_TestKey_", 10);
        stats_set.del_value_by_infix_key("TestKey", "_");
        assert!(stats_set.get_stats_item_table().is_empty());
    }

    #[tokio::test]
    async fn moment_stats_item_set_deletes_value_by_suffix_key() {
        let stats_set = MomentStatsItemSet::new("TestName".to_string());
        stats_set.set_value("_TestKey", 10);
        stats_set.del_value_by_suffix_key("TestKey", "_");
        assert!(stats_set.get_stats_item_table().is_empty());
    }
}
