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
use std::time::SystemTime;

use parking_lot::Mutex;
use parking_lot::RwLock;
use rocketmq_runtime::RuntimeHandle;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::TaskGroup;

use crate::common::statistics::statistics_item::StatisticsItem;
use crate::common::statistics::statistics_item_state_getter::StatisticsItemStateGetter;
use crate::common::statistics::statistics_kind_meta::StatisticsKindMeta;
use crate::TimeUtils::current_millis;

type StatsTable = Arc<RwLock<HashMap<String, HashMap<String, Arc<StatisticsItem>>>>>;

const CLEANUP_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

struct CleanupTask {
    task_group: TaskGroup,
}

impl CleanupTask {
    async fn shutdown(self, timeout: Duration) {
        let report = self.task_group.shutdown(timeout).await;
        if let Err(error) = report.assert_no_task_leak() {
            tracing::warn!("statistics cleanup task shutdown report is unhealthy: {error}");
        }
    }

    fn abort(self) {
        let report = self.task_group.shutdown_now();
        if !report.is_healthy() {
            tracing::warn!(
                report = %report.to_json(),
                "statistics cleanup task abort report is unhealthy"
            );
        }
    }
}

pub struct StatisticsManager {
    kind_meta_map: Arc<RwLock<HashMap<String, Arc<StatisticsKindMeta>>>>,
    brief_metas: Option<Vec<(String, Vec<Vec<i64>>)>>,
    stats_table: StatsTable,
    statistics_item_state_getter: Arc<RwLock<Option<Arc<dyn StatisticsItemStateGetter + Send + Sync>>>>,
    cleanup_task: Arc<Mutex<Option<CleanupTask>>>,
    parent_task_group: Option<TaskGroup>,
}

impl Default for StatisticsManager {
    fn default() -> Self {
        Self::new()
    }
}

impl StatisticsManager {
    const MAX_IDLE_TIME: u64 = 10 * 60 * 1000;

    pub fn new() -> Self {
        Self::new_with_optional_kind_meta_and_task_group(HashMap::new(), None)
    }

    pub fn new_with_task_group(parent_task_group: TaskGroup) -> Self {
        Self::new_with_optional_kind_meta_and_task_group(HashMap::new(), Some(parent_task_group))
    }

    pub fn with_kind_meta(kind_meta: HashMap<String, Arc<StatisticsKindMeta>>) -> Self {
        Self::new_with_optional_kind_meta_and_task_group(kind_meta, None)
    }

    pub fn with_kind_meta_and_task_group(
        kind_meta: HashMap<String, Arc<StatisticsKindMeta>>,
        parent_task_group: TaskGroup,
    ) -> Self {
        Self::new_with_optional_kind_meta_and_task_group(kind_meta, Some(parent_task_group))
    }

    fn new_with_optional_kind_meta_and_task_group(
        kind_meta: HashMap<String, Arc<StatisticsKindMeta>>,
        parent_task_group: Option<TaskGroup>,
    ) -> Self {
        let manager = Self {
            kind_meta_map: Arc::new(RwLock::new(kind_meta)),
            brief_metas: None,
            stats_table: Arc::new(RwLock::new(HashMap::new())),
            statistics_item_state_getter: Arc::new(RwLock::new(None)),
            cleanup_task: Arc::new(Mutex::new(None)),
            parent_task_group,
        };
        manager.start();
        manager
    }

    pub fn add_statistics_kind_meta(&self, kind_meta: Arc<StatisticsKindMeta>) {
        let mut kind_meta_map = self.kind_meta_map.write();
        kind_meta_map.insert(kind_meta.get_name().to_string(), kind_meta.clone());
        let mut stats_table = self.stats_table.write();
        stats_table.entry(kind_meta.get_name().to_string()).or_default();
    }

    pub fn set_brief_meta(&mut self, brief_metas: Vec<(String, Vec<Vec<i64>>)>) {
        self.brief_metas = Some(brief_metas);
    }

    fn start(&self) {
        let mut cleanup_task = self.cleanup_task.lock();
        if cleanup_task.is_some() {
            return;
        }

        let task_group = if let Some(parent_task_group) = self.parent_task_group.as_ref() {
            parent_task_group.child("rocketmq-common.statistics")
        } else {
            let runtime = match tokio::runtime::Handle::try_current() {
                Ok(handle) => RuntimeHandle::new(handle),
                Err(error) => {
                    tracing::debug!(%error, "StatisticsManager cleanup task deferred until a Tokio runtime is available");
                    return;
                }
            };
            TaskGroup::root("rocketmq-common.statistics", runtime)
        };
        let scheduled_tasks = ScheduledTaskGroup::new(task_group.child("scheduled"));
        let stats_table = self.stats_table.clone();
        let kind_meta_map = self.kind_meta_map.clone();
        let statistics_item_state_getter = self.statistics_item_state_getter.clone();

        if let Err(error) = scheduled_tasks.schedule_fixed_rate_no_overlap(
            ScheduledTaskConfig::fixed_rate_no_overlap(
                "common.statistics.cleanup",
                Duration::from_millis(Self::MAX_IDLE_TIME / 3),
            ),
            move || {
                let stats_table = stats_table.clone();
                let kind_meta_map = kind_meta_map.clone();
                let statistics_item_state_getter = statistics_item_state_getter.clone();
                async move {
                    let statistics_item_state_getter = statistics_item_state_getter.read().clone();
                    let expired_items =
                        expired_statistics_items(&stats_table, statistics_item_state_getter, current_millis());
                    for item in expired_items {
                        remove(item.as_ref(), &stats_table, &kind_meta_map);
                    }
                }
            },
        ) {
            tracing::warn!(%error, "failed to spawn StatisticsManager cleanup task");
            return;
        }
        *cleanup_task = Some(CleanupTask { task_group });
    }

    pub async fn shutdown(&self) {
        let cleanup_task = self.cleanup_task.lock().take();
        if let Some(cleanup_task) = cleanup_task {
            cleanup_task.shutdown(CLEANUP_TASK_SHUTDOWN_TIMEOUT).await;
        }
    }

    pub async fn inc(&self, kind: &str, key: &str, item_accumulates: Vec<i64>) -> bool {
        self.start();
        if let Some(item_map) = self.stats_table.write().get_mut(kind) {
            if let Some(item) = item_map.get(key) {
                item.inc_items(item_accumulates);
                return true;
            } else {
                let kind_meta_map = self.kind_meta_map.read();
                if let Some(kind_meta) = kind_meta_map.get(kind) {
                    let item_names = kind_meta.get_item_names().iter().map(|item| item.as_str()).collect();
                    let Ok(new_item) = StatisticsItem::new(kind, key, item_names) else {
                        return false;
                    };
                    let new_item = Arc::new(new_item);
                    item_map.insert(key.to_string(), new_item.clone());
                    new_item.inc_items(item_accumulates);
                    self.schedule_statistics_item(new_item);
                    return true;
                }
            }
        }
        false
    }

    fn schedule_statistics_item(&self, item: Arc<StatisticsItem>) {
        let kind_meta_map = self.kind_meta_map.read();
        if let Some(kind_meta) = kind_meta_map.get(item.stat_kind()) {
            kind_meta.get_scheduled_printer().schedule(item.as_ref());
        }
    }

    pub fn set_statistics_item_state_getter(&mut self, getter: Arc<dyn StatisticsItemStateGetter + Send + Sync>) {
        *self.statistics_item_state_getter.write() = Some(getter);
        self.start();
    }
}

impl Drop for StatisticsManager {
    fn drop(&mut self) {
        if let Some(cleanup_task) = self.cleanup_task.lock().take() {
            cleanup_task.abort();
        }
    }
}

fn expired_statistics_items(
    stats_table: &StatsTable,
    statistics_item_state_getter: Option<Arc<dyn StatisticsItemStateGetter + Send + Sync>>,
    now_millis: u64,
) -> Vec<Arc<StatisticsItem>> {
    let stats_table = stats_table.read();
    let mut expired_items = Vec::new();
    for item_map in stats_table.values() {
        for item in item_map.values() {
            let expired = now_millis.saturating_sub(item.last_timestamp()) > StatisticsManager::MAX_IDLE_TIME;
            let offline = statistics_item_state_getter
                .as_ref()
                .is_none_or(|getter| !getter.online(item));
            if expired && offline {
                expired_items.push(item.clone());
            }
        }
    }
    expired_items
}

pub fn remove(
    item: &StatisticsItem,
    stats_table: &StatsTable,
    kind_meta_map: &Arc<RwLock<HashMap<String, Arc<StatisticsKindMeta>>>>,
) {
    let stat_kind = item.stat_kind();
    let stat_object = item.stat_object();
    if let Some(item_map) = stats_table.write().get_mut(stat_kind) {
        item_map.remove(stat_object);
    }

    // Remove from scheduled printer
    let kind_meta_map = kind_meta_map.write();
    if let Some(kind_meta) = kind_meta_map.get(stat_kind) {
        kind_meta.get_scheduled_printer().remove(item);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    use rocketmq_runtime::RuntimeContext;

    use super::*;
    use crate::common::statistics::statistics_item_scheduled_printer::StatisticsItemScheduledPrinter;

    #[tokio::test]
    async fn inc_rejects_empty_item_names_without_panicking() {
        let manager = StatisticsManager::new();
        manager.add_statistics_kind_meta(Arc::new(StatisticsKindMeta::new(
            "kind".to_string(),
            Vec::new(),
            StatisticsItemScheduledPrinter,
        )));

        assert!(!manager.inc("kind", "key", vec![1]).await);
    }

    #[tokio::test]
    async fn shutdown_stops_idle_cleanup_task() {
        let manager = StatisticsManager::new();
        assert!(
            manager.cleanup_task.lock().is_some(),
            "cleanup task should be running after manager creation"
        );

        manager.shutdown().await;
        assert!(manager.cleanup_task.lock().is_none());

        manager.shutdown().await;
        assert!(manager.cleanup_task.lock().is_none());
    }

    #[tokio::test]
    async fn new_with_task_group_parents_cleanup_task() {
        let context = RuntimeContext::from_current("statistics-manager-parent-test");
        let service = context.service_context("statistics-service");
        let manager = StatisticsManager::new_with_task_group(service.task_group().clone());

        assert!(
            manager.cleanup_task.lock().is_some(),
            "cleanup task should be running after parented manager creation"
        );
        manager.shutdown().await;

        let report = service.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
        assert!(
            report
                .children
                .iter()
                .any(|child| child.name == "rocketmq-common.statistics"),
            "{}",
            report.to_json()
        );
    }

    #[test]
    fn new_without_tokio_runtime_does_not_spawn_panic() {
        let manager = StatisticsManager::new();
        assert!(
            manager.cleanup_task.lock().is_none(),
            "cleanup task should not start without an ambient Tokio runtime"
        );
    }

    #[test]
    fn cleanup_task_starts_lazily_when_used_inside_runtime() {
        let manager = StatisticsManager::new();
        assert!(
            manager.cleanup_task.lock().is_none(),
            "cleanup task should not start without an ambient Tokio runtime"
        );
        manager.add_statistics_kind_meta(Arc::new(StatisticsKindMeta::new(
            "kind".to_string(),
            vec!["count".to_string()],
            StatisticsItemScheduledPrinter,
        )));

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            assert!(manager.inc("kind", "key", vec![1]).await);
            assert!(
                manager.cleanup_task.lock().is_some(),
                "cleanup task should start lazily once async use enters a Tokio runtime"
            );
            manager.shutdown().await;
        });
    }

    #[test]
    fn expired_items_are_collected_without_holding_remove_lock() {
        let item = Arc::new(StatisticsItem::new("kind", "key", vec!["count"]).expect("valid statistics item"));
        let stats_table: StatsTable = Arc::new(RwLock::new(HashMap::from([(
            "kind".to_string(),
            HashMap::from([("key".to_string(), item.clone())]),
        )])));
        let kind_meta_map = Arc::new(RwLock::new(HashMap::from([(
            "kind".to_string(),
            Arc::new(StatisticsKindMeta::new(
                "kind".to_string(),
                vec!["count".to_string()],
                StatisticsItemScheduledPrinter,
            )),
        )])));

        let expired_items = expired_statistics_items(
            &stats_table,
            None,
            item.last_timestamp() + StatisticsManager::MAX_IDLE_TIME + 1,
        );
        assert_eq!(expired_items.len(), 1);

        for item in expired_items {
            remove(item.as_ref(), &stats_table, &kind_meta_map);
        }

        assert!(stats_table.read().get("kind").is_some_and(HashMap::is_empty));
    }

    #[tokio::test]
    async fn cleanup_task_shutdown_cancels_worker_before_abort() {
        let observed_shutdown = Arc::new(AtomicBool::new(false));
        let observed_shutdown_in_task = observed_shutdown.clone();
        let task_group = TaskGroup::root(
            "statistics-cleanup-test",
            RuntimeHandle::new(tokio::runtime::Handle::current()),
        );
        let shutdown_token = task_group.cancellation_token();
        task_group
            .spawn_service("statistics-cleanup-test-worker", async move {
                shutdown_token.cancelled().await;
                observed_shutdown_in_task.store(true, Ordering::SeqCst);
            })
            .expect("cleanup task test worker should spawn");
        let cleanup_task = CleanupTask { task_group };

        cleanup_task.shutdown(Duration::from_secs(1)).await;

        assert!(observed_shutdown.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn drop_aborts_idle_cleanup_task_and_releases_state() {
        let stats_table = {
            let manager = StatisticsManager::new();
            Arc::downgrade(&manager.stats_table)
        };

        tokio::time::timeout(Duration::from_secs(1), async {
            while stats_table.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropping StatisticsManager should release cleanup task state");
    }
}
