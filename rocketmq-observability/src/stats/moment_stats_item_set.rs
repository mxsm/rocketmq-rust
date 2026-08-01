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
use parking_lot::Mutex;
use rocketmq_runtime::OperationContext;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskKind;
use tokio::time::Duration;
use tracing::warn;

use crate::stats::moment_stats_item::MomentStatsItem;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::common::util_all::compute_next_minutes_time_millis;

#[derive(Clone)]
pub struct MomentStatsItemSet {
    stats_item_table: Arc<DashMap<String, MomentStatsItem>>,
    stats_name: String,
    task_owner: Arc<Mutex<Option<StatsSetTaskOwner>>>,
    parent_task_group: TaskGroup,
}

struct StatsSetTaskOwner {
    task_group: TaskGroup,
    operation: OperationContext,
}

impl MomentStatsItemSet {
    pub fn new(stats_name: String, parent_task_group: TaskGroup) -> Self {
        let stats_item_table = Arc::new(DashMap::new());
        let task_owner = Arc::new(Mutex::new(None));
        let set = MomentStatsItemSet {
            stats_item_table,
            stats_name,
            task_owner,
            parent_task_group,
        };
        set.init();
        set
    }

    pub fn new_with_task_group(stats_name: String, parent_task_group: TaskGroup) -> Self {
        Self::new(stats_name, parent_task_group)
    }

    pub fn get_stats_item_table(&self) -> Arc<DashMap<String, MomentStatsItem>> {
        Arc::clone(&self.stats_item_table)
    }

    pub fn get_stats_name(&self) -> &str {
        &self.stats_name
    }

    pub fn init(&self) {
        if self.task_owner.lock().is_some() {
            return;
        }

        let stats_item_table = Arc::clone(&self.stats_item_table);
        let initial_delay =
            Duration::from_millis((compute_next_minutes_time_millis() as i64 - current_millis() as i64).unsigned_abs());

        let task_group = self.parent_task_group.clone();
        let operation = OperationContext::without_deadline(TaskKind::ScheduledDriver);
        let scheduled_tasks = ScheduledTaskGroup::new(task_group.clone());
        let mut config =
            ScheduledTaskConfig::fixed_rate_no_overlap("common.moment-stats-set.print", Duration::from_secs(300));
        config.initial_delay = initial_delay;

        if let Err(error) = scheduled_tasks.schedule_fixed_rate_no_overlap_operation(&operation, config, move || {
            let stats_item_table = stats_item_table.clone();
            async move {
                MomentStatsItemSet::print_at_minutes(&stats_item_table);
            }
        }) {
            warn!(
                "[{}] failed to spawn MomentStatsItemSet task: {}",
                self.stats_name, error
            );
            return;
        }

        *self.task_owner.lock() = Some(StatsSetTaskOwner { task_group, operation });
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

        let new_item = MomentStatsItem::new(
            self.stats_name.clone(),
            stats_key.clone(),
            self.parent_task_group.clone(),
        );
        self.stats_item_table.insert(stats_key, new_item.clone());
        new_item
    }

    pub async fn shutdown(&self) {
        let task_owner = { self.task_owner.lock().take() };
        if let Some(task_owner) = task_owner {
            match task_owner
                .operation
                .cancel_and_wait(&task_owner.task_group, Duration::from_secs(5))
                .await
            {
                Ok(true) => {}
                Ok(false) => warn!(
                    "[{}] MomentStatsItemSet shutdown exceeded its deadline",
                    self.stats_name
                ),
                Err(error) => warn!(
                    "[{}] MomentStatsItemSet used an invalid task owner: {}",
                    self.stats_name, error
                ),
            }
        }

        let stats_items = self
            .stats_item_table
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        for stats_item in stats_items {
            stats_item.shutdown().await;
        }
    }
}

#[cfg(test)]
mod tests {

    use rocketmq_runtime::RuntimeContext;

    use super::*;

    fn test_parent(name: &'static str) -> TaskGroup {
        RuntimeContext::from_current(name)
            .service_context("moment-stats-set-service")
            .task_group()
            .clone()
    }

    #[tokio::test]
    async fn moment_stats_item_set_initializes_with_empty_table() {
        let stats_set = MomentStatsItemSet::new("TestName".to_string(), test_parent("moment-stats-set-empty-test"));
        assert!(stats_set.get_stats_item_table().is_empty());
    }

    #[tokio::test]
    async fn moment_stats_item_set_returns_correct_stats_name() {
        let stats_set = MomentStatsItemSet::new("TestName".to_string(), test_parent("moment-stats-set-name-test"));
        assert_eq!(stats_set.get_stats_name(), "TestName");
    }

    #[tokio::test]
    async fn moment_stats_item_set_with_task_group_is_parented() {
        let context = RuntimeContext::from_current("moment-stats-set-parent-test");
        let service = context.service_context("common-stats-service");
        let stats_set =
            MomentStatsItemSet::new_with_task_group("ParentedName".to_string(), service.task_group().clone());

        assert_eq!(service.task_group().task_count(), 1);
        stats_set.shutdown().await;
        let report = service.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
        assert_eq!(report.completed, 1, "{}", report.to_json());
        assert!(report.children.is_empty(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn moment_stats_item_set_creates_and_returns_stats_item() {
        let stats_set = MomentStatsItemSet::new("TestName".to_string(), test_parent("moment-stats-set-create-test"));
        let stats_item = stats_set.get_and_create_stats_item("TestKey".to_string());
        assert_eq!(stats_item.get_stats_name(), "TestName");
        assert_eq!(stats_item.get_stats_key(), "TestKey");
    }

    #[tokio::test]
    async fn moment_stats_item_set_sets_and_gets_value() {
        let stats_set = MomentStatsItemSet::new("TestName".to_string(), test_parent("moment-stats-set-value-test"));
        stats_set.set_value("TestKey", 10);
        let stats_item = stats_set.get_and_create_stats_item("TestKey".to_string());
        assert_eq!(stats_item.get_value().load(std::sync::atomic::Ordering::Relaxed), 10);
    }

    #[tokio::test]
    async fn moment_stats_item_set_deletes_value_by_infix_key() {
        let stats_set = MomentStatsItemSet::new("TestName".to_string(), test_parent("moment-stats-set-infix-test"));
        stats_set.set_value("_TestKey_", 10);
        stats_set.del_value_by_infix_key("TestKey", "_");
        assert!(stats_set.get_stats_item_table().is_empty());
    }

    #[tokio::test]
    async fn moment_stats_item_set_deletes_value_by_suffix_key() {
        let stats_set = MomentStatsItemSet::new("TestName".to_string(), test_parent("moment-stats-set-suffix-test"));
        stats_set.set_value("_TestKey", 10);
        stats_set.del_value_by_suffix_key("TestKey", "_");
        assert!(stats_set.get_stats_item_table().is_empty());
    }
}
