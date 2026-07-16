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

use std::fmt;
use std::ops::Deref;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rocketmq_common::common::broker::broker_config::BrokerIdentity;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::ScheduledTaskSnapshot;
use rocketmq_runtime::ShutdownReport;
pub use rocketmq_store_local::stats::CallSnapshot;
use rocketmq_store_local::stats::StoreStatsState;
use tokio::sync::Notify;
use tracing::info;
use tracing::warn;

const FREQUENCY_OF_SAMPLING: u64 = 1000;

/// Store-facing lifecycle adapter around the runtime-neutral local statistics state.
pub struct StoreStatsService {
    state: StoreStatsState,
    stopped: AtomicBool,
    shutdown_notify: Notify,
    worker_group: Mutex<Option<rocketmq_runtime::TaskGroup>>,
    scheduled_tasks: Mutex<Option<ScheduledTaskGroup>>,
    broker_identity: Option<BrokerIdentity>,
}

impl StoreStatsService {
    #[inline]
    pub fn new(broker_identity: Option<BrokerIdentity>) -> Self {
        Self {
            state: StoreStatsState::new(),
            stopped: AtomicBool::new(true),
            shutdown_notify: Notify::new(),
            worker_group: Mutex::new(None),
            scheduled_tasks: Mutex::new(None),
            broker_identity,
        }
    }

    pub fn start(self: &Arc<Self>) {
        let mut worker_group_guard = self.worker_group.lock();
        if worker_group_guard.is_some() {
            return;
        }

        self.stopped.store(false, Ordering::Release);
        let service = Arc::clone(self);
        let service_name = service.get_service_name();
        let worker_group = match crate::runtime::task_group("rocketmq-store.stats") {
            Ok(worker_group) => worker_group,
            Err(error) => {
                self.stopped.store(true, Ordering::Release);
                warn!("failed to create StoreStatsService task group: {error}");
                return;
            }
        };
        let scheduled_tasks = ScheduledTaskGroup::new(worker_group.child("scheduled"));
        if let Err(error) = scheduled_tasks.schedule_fixed_delay(
            ScheduledTaskConfig::fixed_delay(service_name.clone(), Duration::from_millis(FREQUENCY_OF_SAMPLING)),
            move || {
                let service = Arc::clone(&service);
                async move {
                    if service.stopped.load(Ordering::Acquire) {
                        return;
                    }
                    service.sampling();
                    service.print_tps();
                }
            },
        ) {
            self.stopped.store(true, Ordering::Release);
            warn!("failed to spawn StoreStatsService task: {error}");
            return;
        }
        info!("{} service started", service_name);
        *self.scheduled_tasks.lock() = Some(scheduled_tasks);
        *worker_group_guard = Some(worker_group);
    }

    pub fn shutdown(&self) {
        self.stopped.store(true, Ordering::Release);
        self.shutdown_notify.notify_waiters();
        self.scheduled_tasks.lock().take();
        if let Some(worker_group) = self.worker_group.lock().take() {
            worker_group.cancel();
        }
    }

    pub async fn shutdown_gracefully(&self) {
        let _ = self.shutdown_gracefully_with_report().await;
    }

    pub async fn shutdown_gracefully_with_report(&self) -> Option<ShutdownReport> {
        self.stopped.store(true, Ordering::Release);
        self.shutdown_notify.notify_waiters();
        self.scheduled_tasks.lock().take();
        let worker_group = self.worker_group.lock().take();
        if let Some(worker_group) = worker_group {
            let report = worker_group.shutdown(Duration::from_secs(5)).await;
            if let Err(error) = crate::runtime::shutdown_report_result("StoreStatsService", report.clone()) {
                warn!("StoreStatsService task failed during shutdown: {error}");
                return None;
            }
            return Some(report);
        }
        None
    }

    #[cfg(test)]
    pub(crate) fn has_worker_handle(&self) -> bool {
        self.worker_group.lock().is_some()
    }

    pub(crate) fn task_count(&self) -> usize {
        let root_count = self
            .worker_group
            .lock()
            .as_ref()
            .map(rocketmq_runtime::TaskGroup::task_count)
            .unwrap_or_default();
        let scheduled_count = self
            .scheduled_tasks
            .lock()
            .as_ref()
            .map(|scheduled_tasks| scheduled_tasks.group().task_count())
            .unwrap_or_default();
        root_count + scheduled_count
    }

    pub(crate) fn schedule_snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.scheduled_tasks
            .lock()
            .as_ref()
            .map(ScheduledTaskGroup::snapshot)
            .unwrap_or_default()
    }

    fn get_service_name(&self) -> String {
        if let Some(broker_identity) = &self.broker_identity {
            if broker_identity.is_in_broker_container {
                return format!("{}StoreStatsService", broker_identity.get_canonical_name());
            }
        }
        "StoreStatsService".to_string()
    }
}

impl Deref for StoreStatsService {
    type Target = StoreStatsState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl Drop for StoreStatsService {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        self.shutdown_notify.notify_waiters();
        self.scheduled_tasks.get_mut().take();
        if let Some(worker_group) = self.worker_group.get_mut().take() {
            worker_group.cancel();
        }
    }
}

impl fmt::Display for StoreStatsService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.state.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::task;

    #[test]
    fn preserves_call_snapshot_tps_contract() {
        let begin = CallSnapshot::new(1000, 10);
        let end = CallSnapshot::new(2000, 20);
        assert_eq!(CallSnapshot::get_tps(&begin, &end), 10.0);
    }

    #[test]
    fn exposes_local_stats_state_through_store_adapter() {
        let stats = StoreStatsService::new(None);
        stats.add_single_put_message_topic_times_total("topic-a", 4);
        stats.add_single_put_message_topic_size_total("topic-a", 100);
        assert_eq!(stats.get_put_message_times_total(), 4);
        assert_eq!(stats.get_put_message_size_total(), 100);
    }

    #[tokio::test]
    async fn start_shutdown_are_idempotent_and_restartable() {
        let stats = Arc::new(StoreStatsService::new(None));

        stats.start();
        stats.start();
        assert!(stats.has_worker_handle());
        assert_eq!(stats.task_count(), 1);
        assert!(!stats.stopped.load(Ordering::Acquire));

        task::yield_now().await;
        let report = stats
            .shutdown_gracefully_with_report()
            .await
            .expect("shutdown should return a report");
        assert!(report.is_healthy(), "{}", report.to_json());
        stats.shutdown_gracefully().await;
        assert!(!stats.has_worker_handle());
        assert_eq!(stats.task_count(), 0);
        assert!(stats.stopped.load(Ordering::Acquire));

        stats.start();
        assert!(stats.has_worker_handle());
        stats.shutdown_gracefully().await;
    }
}
