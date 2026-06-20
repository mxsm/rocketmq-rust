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
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::interval;

use crate::common::statistics::statistics_item::StatisticsItem;
use crate::common::statistics::statistics_item_state_getter::StatisticsItemStateGetter;
use crate::common::statistics::statistics_kind_meta::StatisticsKindMeta;
use crate::TimeUtils::current_millis;

type StatsTable = Arc<RwLock<HashMap<String, HashMap<String, Arc<StatisticsItem>>>>>;

const CLEANUP_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

struct CleanupTask {
    shutdown_tx: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

impl CleanupTask {
    fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    async fn shutdown(self, timeout: Duration) {
        let _ = self.shutdown_tx.send(true);
        let mut handle = self.handle;
        match tokio::time::timeout(timeout, &mut handle).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) if error.is_cancelled() => {}
            Ok(Err(error)) => tracing::warn!(%error, "statistics cleanup task exited with join error"),
            Err(_) => {
                handle.abort();
                match handle.await {
                    Ok(()) => {}
                    Err(error) if error.is_cancelled() => {}
                    Err(error) => tracing::warn!(%error, "statistics cleanup task aborted with join error"),
                }
            }
        }
    }

    fn abort(self) {
        let _ = self.shutdown_tx.send(true);
        self.handle.abort();
    }
}

pub struct StatisticsManager {
    kind_meta_map: Arc<RwLock<HashMap<String, Arc<StatisticsKindMeta>>>>,
    brief_metas: Option<Vec<(String, Vec<Vec<i64>>)>>,
    stats_table: StatsTable,
    statistics_item_state_getter: Option<Arc<dyn StatisticsItemStateGetter + Send + Sync>>,
    cleanup_task: Arc<Mutex<Option<CleanupTask>>>,
}

impl Default for StatisticsManager {
    fn default() -> Self {
        Self::new()
    }
}

impl StatisticsManager {
    const MAX_IDLE_TIME: u64 = 10 * 60 * 1000;

    pub fn new() -> Self {
        let manager = Self {
            kind_meta_map: Arc::new(RwLock::new(HashMap::new())),
            brief_metas: None,
            stats_table: Arc::new(RwLock::new(HashMap::new())),
            statistics_item_state_getter: None,
            cleanup_task: Arc::new(Mutex::new(None)),
        };
        manager.start();
        manager
    }

    pub fn with_kind_meta(kind_meta: HashMap<String, Arc<StatisticsKindMeta>>) -> Self {
        let manager = Self {
            kind_meta_map: Arc::new(RwLock::new(kind_meta)),
            brief_metas: None,
            stats_table: Arc::new(RwLock::new(HashMap::new())),
            statistics_item_state_getter: None,
            cleanup_task: Arc::new(Mutex::new(None)),
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
        if cleanup_task.as_ref().is_some_and(|handle| !handle.is_finished()) {
            return;
        }

        let stats_table = self.stats_table.clone();
        let kind_meta_map = self.kind_meta_map.clone();
        let statistics_item_state_getter = self.statistics_item_state_getter.clone();
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let handle = tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(Self::MAX_IDLE_TIME / 3));
            let stats_table_clone = stats_table.clone();
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let stats_table = stats_table.read();
                        for item_map in stats_table.values() {
                            let tmp_item_map: HashMap<_, _> = item_map.clone().into_iter().collect();

                            for item in tmp_item_map.values() {
                                let last_time_stamp = item.last_timestamp();
                                let expired = current_millis() - last_time_stamp > Self::MAX_IDLE_TIME;
                                let offline = statistics_item_state_getter
                                    .as_ref()
                                    .is_none_or(|getter| !getter.online(item));
                                if expired && offline {
                                    remove(item, &stats_table_clone, &kind_meta_map);
                                }
                            }
                        }
                    }
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        });
        *cleanup_task = Some(CleanupTask { shutdown_tx, handle });
    }

    pub async fn shutdown(&self) {
        let cleanup_task = self.cleanup_task.lock().take();
        if let Some(cleanup_task) = cleanup_task {
            cleanup_task.shutdown(CLEANUP_TASK_SHUTDOWN_TIMEOUT).await;
        }
    }

    pub async fn inc(&self, kind: &str, key: &str, item_accumulates: Vec<i64>) -> bool {
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
        self.statistics_item_state_getter = Some(getter);
    }
}

impl Drop for StatisticsManager {
    fn drop(&mut self) {
        if let Some(cleanup_task) = self.cleanup_task.lock().take() {
            cleanup_task.abort();
        }
    }
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
            manager
                .cleanup_task
                .lock()
                .as_ref()
                .is_some_and(|handle| !handle.is_finished()),
            "cleanup task should be running after manager creation"
        );

        manager.shutdown().await;
        assert!(manager.cleanup_task.lock().is_none());

        manager.shutdown().await;
        assert!(manager.cleanup_task.lock().is_none());
    }

    #[tokio::test]
    async fn cleanup_task_shutdown_signals_worker_before_abort() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let observed_shutdown = Arc::new(AtomicBool::new(false));
        let observed_shutdown_in_task = observed_shutdown.clone();
        let cleanup_task = CleanupTask {
            shutdown_tx,
            handle: tokio::spawn(async move {
                loop {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                    if shutdown_rx.changed().await.is_err() {
                        break;
                    }
                }
                observed_shutdown_in_task.store(true, Ordering::SeqCst);
            }),
        };

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
