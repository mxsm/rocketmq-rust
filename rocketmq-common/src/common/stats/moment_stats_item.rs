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

use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use parking_lot::Mutex;
use rocketmq_runtime::RuntimeHandle;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::TaskGroup;
use tracing::info;
use tracing::warn;

use crate::TimeUtils::current_millis;
use crate::UtilAll::compute_next_minutes_time_millis;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct MomentStatsItem {
    value: Arc<AtomicI64>,
    stats_name: String,
    stats_key: String,
    task_group: Arc<Mutex<Option<TaskGroup>>>,
    parent_task_group: Option<TaskGroup>,
}

impl MomentStatsItem {
    pub fn new(stats_name: String, stats_key: String) -> Self {
        Self::new_with_optional_task_group(stats_name, stats_key, None)
    }

    pub fn new_with_task_group(stats_name: String, stats_key: String, parent_task_group: TaskGroup) -> Self {
        Self::new_with_optional_task_group(stats_name, stats_key, Some(parent_task_group))
    }

    fn new_with_optional_task_group(
        stats_name: String,
        stats_key: String,
        parent_task_group: Option<TaskGroup>,
    ) -> Self {
        MomentStatsItem {
            value: Arc::new(AtomicI64::new(0)),
            stats_name,
            stats_key,
            task_group: Arc::new(Mutex::new(None)),
            parent_task_group,
        }
    }

    pub fn init(self: Arc<Self>) {
        if self.task_group.lock().is_some() {
            warn!(
                "[{}] [{}] MomentStatsItem already initialized",
                self.stats_name, self.stats_key
            );
            return;
        }

        let group_name = format!("rocketmq-common.moment-stats.{}.{}", self.stats_name, self.stats_key);
        let task_group = if let Some(parent_task_group) = self.parent_task_group.as_ref() {
            parent_task_group.child(group_name)
        } else {
            let runtime = match tokio::runtime::Handle::try_current() {
                Ok(handle) => RuntimeHandle::new(handle),
                Err(error) => {
                    warn!(
                        "[{}] [{}] failed to initialize MomentStatsItem outside Tokio runtime: {}",
                        self.stats_name, self.stats_key, error
                    );
                    return;
                }
            };
            TaskGroup::root(group_name, runtime)
        };
        let scheduled_tasks = ScheduledTaskGroup::new(task_group.child("scheduled"));
        let self_clone = self.clone();
        let mut config =
            ScheduledTaskConfig::fixed_rate_no_overlap("common.moment-stats.print", Duration::from_secs(300));
        config.initial_delay =
            Duration::from_millis((compute_next_minutes_time_millis() as i64 - current_millis() as i64).unsigned_abs());

        if let Err(error) = scheduled_tasks.schedule_fixed_rate_no_overlap(config, move || {
            let self_clone = self_clone.clone();
            async move {
                self_clone.print_at_minutes();
                self_clone.value.store(0, Ordering::Relaxed);
            }
        }) {
            warn!(
                "[{}] [{}] failed to spawn MomentStatsItem task: {}",
                self.stats_name, self.stats_key, error
            );
            return;
        }
        *self.task_group.lock() = Some(task_group);
    }

    pub async fn shutdown(&self) {
        let task_group = { self.task_group.lock().take() };
        if let Some(task_group) = task_group {
            let report = task_group.shutdown(SHUTDOWN_TIMEOUT).await;
            if !report.is_healthy() {
                warn!(
                    report = %report.to_json(),
                    "[{}] [{}] MomentStatsItem shutdown report is unhealthy",
                    self.stats_name,
                    self.stats_key
                );
            }
        }
    }

    pub fn print_at_minutes(&self) {
        info!(
            "[{}] [{}] Stats Every 5 Minutes, Value: {}",
            self.stats_name,
            self.stats_key,
            self.value.load(Ordering::Relaxed)
        );
    }

    pub fn get_value(&self) -> Arc<AtomicI64> {
        self.value.clone()
    }

    pub fn get_stats_key(&self) -> &str {
        &self.stats_key
    }

    pub fn get_stats_name(&self) -> &str {
        &self.stats_name
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;

    #[tokio::test]
    async fn moment_stats_item_initializes_with_zero_value() {
        let stats_item = MomentStatsItem::new("TestName".to_string(), "TestKey".to_string());
        assert_eq!(stats_item.get_value().load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn moment_stats_item_returns_correct_stats_name() {
        let stats_item = MomentStatsItem::new("TestName".to_string(), "TestKey".to_string());
        assert_eq!(stats_item.get_stats_name(), "TestName");
    }

    #[tokio::test]
    async fn moment_stats_item_returns_correct_stats_key() {
        let stats_item = MomentStatsItem::new("TestName".to_string(), "TestKey".to_string());
        assert_eq!(stats_item.get_stats_key(), "TestKey");
    }
}
