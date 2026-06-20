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

use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use crate::runtime::spawn_client_tracked_task;
use crate::runtime::ClientTrackedTaskHandle;
use parking_lot::Mutex;
use rocketmq_common::common::stats::stats_item_set::StatsItemSet;
use rocketmq_remoting::protocol::body::consume_status::ConsumeStatus;
use serde::Serialize;
use tokio::sync::Notify;
use tracing::warn;

const TOPIC_AND_GROUP_CONSUME_OK_TPS: &str = "CONSUME_OK_TPS";
const TOPIC_AND_GROUP_CONSUME_FAILED_TPS: &str = "CONSUME_FAILED_TPS";
const TOPIC_AND_GROUP_CONSUME_RT: &str = "CONSUME_RT";
const TOPIC_AND_GROUP_PULL_TPS: &str = "PULL_TPS";
const TOPIC_AND_GROUP_PULL_RT: &str = "PULL_RT";
const CONSUMER_STATS_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Tracks consumer-side statistics for each topic/group pair.
///
/// Maintains five time-windowed metric sets: consume OK TPS, consume failed
/// TPS, consume RT, pull TPS, and pull RT. All `inc_*` methods are
/// thread-safe and intended to be called on the hot consumption path.
pub struct ConsumerStatsManager {
    topic_and_group_consume_ok_tps: StatsItemSet,
    topic_and_group_consume_rt: StatsItemSet,
    topic_and_group_consume_failed_tps: StatsItemSet,
    topic_and_group_pull_tps: StatsItemSet,
    topic_and_group_pull_rt: StatsItemSet,
    shutdown_signal: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
    task_handles: Mutex<Vec<ConsumerStatsTask>>,
}

struct ConsumerStatsTask {
    handle: ClientTrackedTaskHandle,
}

impl ConsumerStatsTask {
    async fn shutdown_async(self, timeout: Duration) -> bool {
        let report = self.handle.shutdown(timeout).await;
        if !report.is_healthy() {
            warn!(
                report = %report.to_json(),
                "consumer stats task shutdown report is unhealthy"
            );
        }
        report.is_healthy()
    }

    fn shutdown_blocking(self, timeout: Duration) -> bool {
        let (report, completed) = self.handle.shutdown_blocking(timeout);
        if !report.is_healthy() {
            warn!(
                report = %report.to_json(),
                "consumer stats task blocking shutdown report is unhealthy"
            );
        }
        completed && report.is_healthy()
    }

    fn abort(self) {
        let report = self.handle.shutdown_now();
        if !report.is_healthy() {
            warn!(
                report = %report.to_json(),
                "consumer stats task immediate shutdown report is unhealthy"
            );
        }
    }

    fn task_count(&self) -> usize {
        self.handle.task_count()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ConsumerStatsManagerLifecycleProbe {
    pub healthy: bool,
    pub task_count_before_shutdown: usize,
    pub task_count_after_shutdown: usize,
    pub shutdown_elapsed_us: u128,
}

impl Default for ConsumerStatsManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ConsumerStatsManager {
    /// Creates a new `ConsumerStatsManager` with all metric sets initialised.
    pub fn new() -> Self {
        Self {
            topic_and_group_consume_ok_tps: StatsItemSet::new(TOPIC_AND_GROUP_CONSUME_OK_TPS.to_string()),
            topic_and_group_consume_rt: StatsItemSet::new(TOPIC_AND_GROUP_CONSUME_RT.to_string()),
            topic_and_group_consume_failed_tps: StatsItemSet::new(TOPIC_AND_GROUP_CONSUME_FAILED_TPS.to_string()),
            topic_and_group_pull_tps: StatsItemSet::new(TOPIC_AND_GROUP_PULL_TPS.to_string()),
            topic_and_group_pull_rt: StatsItemSet::new(TOPIC_AND_GROUP_PULL_RT.to_string()),
            shutdown_signal: Arc::new(AtomicBool::new(false)),
            shutdown_notify: Arc::new(Notify::new()),
            task_handles: Mutex::new(Vec::new()),
        }
    }

    /// Starts background sampling tasks.
    ///
    /// Spawns three background tasks that advance the sliding-window snapshots used
    /// by [`consume_status`](Self::consume_status):
    ///
    /// - every 10 s: `cs_list_minute` (drives per-minute stats)
    /// - every 10 min: `cs_list_hour` (drives per-hour stats)
    /// - every 1 h: `cs_list_day` (drives per-day stats)
    pub fn start(&self) {
        let mut task_handles = self.task_handles.lock();
        if !task_handles.is_empty() {
            warn!("ConsumerStatsManager already started; ignoring duplicate start");
            return;
        }

        self.shutdown_signal.store(false, Ordering::Release);

        // 10-second tick drives cs_list_minute on each StatsItem.
        let sets_sec = [
            self.topic_and_group_consume_ok_tps.clone(),
            self.topic_and_group_consume_rt.clone(),
            self.topic_and_group_consume_failed_tps.clone(),
            self.topic_and_group_pull_tps.clone(),
            self.topic_and_group_pull_rt.clone(),
        ];
        let shutdown_signal = self.shutdown_signal.clone();
        let shutdown_notify = self.shutdown_notify.clone();
        if let Some(task) = spawn_stats_task("rocketmq-client-consumer-stats-seconds", async move {
            loop {
                if shutdown_signal.load(Ordering::Acquire) {
                    break;
                }
                for set in &sets_sec {
                    set.sampling_in_seconds();
                }
                if !sleep_or_shutdown(&shutdown_signal, &shutdown_notify, Duration::from_secs(10)).await {
                    break;
                }
            }
        }) {
            task_handles.push(task);
        }

        // 10-minute tick drives cs_list_hour on each StatsItem.
        let sets_min = [
            self.topic_and_group_consume_ok_tps.clone(),
            self.topic_and_group_consume_rt.clone(),
            self.topic_and_group_consume_failed_tps.clone(),
            self.topic_and_group_pull_tps.clone(),
            self.topic_and_group_pull_rt.clone(),
        ];
        let shutdown_signal = self.shutdown_signal.clone();
        let shutdown_notify = self.shutdown_notify.clone();
        if let Some(task) = spawn_stats_task("rocketmq-client-consumer-stats-minutes", async move {
            loop {
                if shutdown_signal.load(Ordering::Acquire) {
                    break;
                }
                for set in &sets_min {
                    set.sampling_in_minutes();
                }
                if !sleep_or_shutdown(&shutdown_signal, &shutdown_notify, Duration::from_secs(10 * 60)).await {
                    break;
                }
            }
        }) {
            task_handles.push(task);
        }

        // 1-hour tick drives cs_list_day on each StatsItem.
        let sets_hour = [
            self.topic_and_group_consume_ok_tps.clone(),
            self.topic_and_group_consume_rt.clone(),
            self.topic_and_group_consume_failed_tps.clone(),
            self.topic_and_group_pull_tps.clone(),
            self.topic_and_group_pull_rt.clone(),
        ];
        let shutdown_signal = self.shutdown_signal.clone();
        let shutdown_notify = self.shutdown_notify.clone();
        if let Some(task) = spawn_stats_task("rocketmq-client-consumer-stats-hours", async move {
            loop {
                if shutdown_signal.load(Ordering::Acquire) {
                    break;
                }
                for set in &sets_hour {
                    set.sampling_in_hours();
                }
                if !sleep_or_shutdown(&shutdown_signal, &shutdown_notify, Duration::from_secs(3600)).await {
                    break;
                }
            }
        }) {
            task_handles.push(task);
        }
    }

    /// Shuts down the stats manager.
    pub fn shutdown(&self) {
        self.shutdown_signal.store(true, Ordering::Release);
        self.shutdown_notify.notify_waiters();
        let tasks = self.drain_task_handles();
        let timed_out = shutdown_consumer_stats_tasks_blocking(tasks, CONSUMER_STATS_TASK_SHUTDOWN_TIMEOUT);
        if timed_out > 0 {
            warn!(timed_out, "consumer stats tasks did not stop before timeout; aborted");
        }
    }

    /// Shuts down the stats manager without blocking the Tokio runtime worker.
    pub async fn shutdown_async(&self) {
        self.shutdown_signal.store(true, Ordering::Release);
        self.shutdown_notify.notify_waiters();
        let tasks = self.drain_task_handles();
        let timed_out = shutdown_consumer_stats_tasks_async(tasks, CONSUMER_STATS_TASK_SHUTDOWN_TIMEOUT).await;
        if timed_out > 0 {
            warn!(timed_out, "consumer stats tasks did not stop before timeout; aborted");
        }
    }

    fn drain_task_handles(&self) -> Vec<ConsumerStatsTask> {
        std::mem::take(&mut *self.task_handles.lock())
    }

    fn task_count(&self) -> usize {
        self.task_handles.lock().iter().map(ConsumerStatsTask::task_count).sum()
    }

    /// Records a single pull response-time observation in milliseconds.
    pub fn inc_pull_rt(&self, group: &str, topic: &str, rt: u64) {
        self.topic_and_group_pull_rt
            .add_rt_value(&stats_key(topic, group), rt as i64, 1);
    }

    /// Records `msgs` messages successfully pulled in one batch.
    pub fn inc_pull_tps(&self, group: &str, topic: &str, msgs: u64) {
        self.topic_and_group_pull_tps
            .add_value(&stats_key(topic, group), msgs as i64, 1);
    }

    /// Records a single consume response-time observation in milliseconds.
    pub fn inc_consume_rt(&self, group: &str, topic: &str, rt: u64) {
        self.topic_and_group_consume_rt
            .add_rt_value(&stats_key(topic, group), rt as i64, 1);
    }

    /// Records `msgs` messages consumed successfully in one batch.
    pub fn inc_consume_ok_tps(&self, group: &str, topic: &str, msgs: u64) {
        self.topic_and_group_consume_ok_tps
            .add_value(&stats_key(topic, group), msgs as i64, 1);
    }

    /// Java-compatible acronym spelling for `incConsumeOKTPS`.
    #[inline]
    pub fn inc_consume_oktps(&self, group: &str, topic: &str, msgs: u64) {
        self.inc_consume_ok_tps(group, topic, msgs);
    }

    /// Records `msgs` messages that failed consumption in one batch.
    pub fn inc_consume_failed_tps(&self, group: &str, topic: &str, msgs: u64) {
        self.topic_and_group_consume_failed_tps
            .add_value(&stats_key(topic, group), msgs as i64, 1);
    }

    /// Returns a point-in-time [`ConsumeStatus`] snapshot for the given
    /// consumer group and topic.
    ///
    /// - Pull / consume RT uses the per-minute average; consume RT falls back to the per-hour
    ///   average when the per-minute window is empty.
    /// - `consume_failed_msgs` accumulates the per-hour sum of failed messages.
    pub fn consume_status(&self, group: &str, topic: &str) -> ConsumeStatus {
        let key = stats_key(topic, group);

        let pull_rt = self.topic_and_group_pull_rt.get_stats_data_in_minute(&key).get_avgpt();

        let pull_tps = self.topic_and_group_pull_tps.get_stats_data_in_minute(&key).get_tps();

        let consume_rt = {
            let minute = self.topic_and_group_consume_rt.get_stats_data_in_minute(&key);
            if minute.get_sum() == 0 {
                self.topic_and_group_consume_rt.get_stats_data_in_hour(&key).get_avgpt()
            } else {
                minute.get_avgpt()
            }
        };

        let consume_ok_tps = self
            .topic_and_group_consume_ok_tps
            .get_stats_data_in_minute(&key)
            .get_tps();

        let consume_failed_tps = self
            .topic_and_group_consume_failed_tps
            .get_stats_data_in_minute(&key)
            .get_tps();

        let consume_failed_msgs = self
            .topic_and_group_consume_failed_tps
            .get_stats_data_in_hour(&key)
            .get_sum() as i64;

        ConsumeStatus {
            pull_rt,
            pull_tps,
            consume_rt,
            consume_ok_tps,
            consume_failed_tps,
            consume_failed_msgs,
        }
    }
}

/// Builds the canonical stats key `"topic@group"` used by all metric sets.
#[inline]
fn stats_key(topic: &str, group: &str) -> String {
    format!("{topic}@{group}")
}

fn spawn_stats_task<F>(thread_name: &'static str, task: F) -> Option<ConsumerStatsTask>
where
    F: Future<Output = ()> + Send + 'static,
{
    match spawn_client_tracked_task(thread_name, task) {
        Ok(handle) => Some(ConsumerStatsTask { handle }),
        Err(error) => {
            warn!("Failed to spawn {} background task: {}", thread_name, error);
            None
        }
    }
}

async fn shutdown_consumer_stats_tasks_async(tasks: Vec<ConsumerStatsTask>, timeout: Duration) -> usize {
    let deadline = Instant::now() + timeout;
    let mut timed_out = 0;

    for task in tasks {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            task.abort();
            timed_out += 1;
            continue;
        }

        if !task.shutdown_async(remaining).await {
            timed_out += 1;
        }
    }

    timed_out
}

fn shutdown_consumer_stats_tasks_blocking(tasks: Vec<ConsumerStatsTask>, timeout: Duration) -> usize {
    let deadline = Instant::now() + timeout;
    let mut timed_out = 0;

    for task in tasks {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            task.abort();
            timed_out += 1;
            continue;
        }

        if !task.shutdown_blocking(remaining) {
            timed_out += 1;
        }
    }

    timed_out
}

async fn sleep_or_shutdown(shutdown_signal: &Arc<AtomicBool>, shutdown_notify: &Arc<Notify>, delay: Duration) -> bool {
    if shutdown_signal.load(Ordering::Acquire) {
        return false;
    }

    tokio::time::timeout(delay, shutdown_notify.notified()).await.is_err() && !shutdown_signal.load(Ordering::Acquire)
}

#[doc(hidden)]
pub async fn run_consumer_stats_manager_lifecycle_probe() -> ConsumerStatsManagerLifecycleProbe {
    let manager = ConsumerStatsManager::new();
    manager.start();
    let task_count_before_shutdown = manager.task_count();

    let shutdown_started = Instant::now();
    manager.shutdown_async().await;
    let shutdown_elapsed_us = shutdown_started.elapsed().as_micros();
    let task_count_after_shutdown = manager.task_count();

    ConsumerStatsManagerLifecycleProbe {
        healthy: task_count_before_shutdown == 3 && task_count_after_shutdown == 0,
        task_count_before_shutdown,
        task_count_after_shutdown,
        shutdown_elapsed_us,
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use super::*;

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    fn make_manager() -> ConsumerStatsManager {
        ConsumerStatsManager::new()
    }

    #[test]
    fn stats_key_format() {
        assert_eq!(stats_key("TopicA", "GroupA"), "TopicA@GroupA");
    }

    #[test]
    fn smoke_inc_consume_ok_tps() {
        let mgr = make_manager();
        mgr.inc_consume_ok_tps("GroupA", "TopicA", 5);
    }

    #[test]
    fn java_acronym_alias_inc_consume_oktps_records_ok_tps() {
        let mgr = make_manager();
        mgr.inc_consume_oktps("GroupA", "TopicA", 5);
        let item = mgr
            .topic_and_group_consume_ok_tps
            .get_stats_item(&stats_key("TopicA", "GroupA"))
            .expect("alias should record the same stats item as inc_consume_ok_tps");

        assert_eq!(item.get_value(), 5);
        assert_eq!(item.get_times(), 1);
    }

    #[test]
    fn smoke_inc_consume_failed_tps() {
        let mgr = make_manager();
        mgr.inc_consume_failed_tps("GroupA", "TopicA", 3);
    }

    #[test]
    fn smoke_inc_consume_rt() {
        let mgr = make_manager();
        mgr.inc_consume_rt("GroupA", "TopicA", 42);
    }

    #[test]
    fn smoke_inc_pull_rt() {
        let mgr = make_manager();
        mgr.inc_pull_rt("GroupA", "TopicA", 10);
    }

    #[test]
    fn smoke_inc_pull_tps() {
        let mgr = make_manager();
        mgr.inc_pull_tps("GroupA", "TopicA", 100);
    }

    #[test]
    fn consume_status_returns_zero_for_empty_stats() {
        let mgr = make_manager();
        let status = mgr.consume_status("GroupA", "TopicA");
        assert_eq!(status.pull_rt, 0.0);
        assert_eq!(status.pull_tps, 0.0);
        assert_eq!(status.consume_rt, 0.0);
        assert_eq!(status.consume_ok_tps, 0.0);
        assert_eq!(status.consume_failed_tps, 0.0);
        assert_eq!(status.consume_failed_msgs, 0);
    }

    #[tokio::test]
    async fn start_launches_background_tasks() {
        let mgr = make_manager();
        mgr.start();
        assert_eq!(mgr.task_handles.lock().len(), 3);

        mgr.shutdown_async().await;

        assert!(mgr.task_handles.lock().is_empty());
    }

    #[tokio::test]
    async fn start_ignores_duplicate_background_tasks() {
        let mgr = make_manager();
        mgr.start();
        mgr.start();

        assert_eq!(mgr.task_handles.lock().len(), 3);

        mgr.shutdown_async().await;
    }

    #[tokio::test]
    async fn stats_task_shutdown_aborts_after_timeout() {
        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_in_task = dropped.clone();
        let task = spawn_stats_task("rocketmq-client-consumer-stats-test", async move {
            let _drop_flag = DropFlag(dropped_in_task);
            pending::<()>().await;
        })
        .expect("test task should spawn");

        assert!(!task.shutdown_async(Duration::from_millis(20)).await);
        assert!(dropped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn sleep_or_shutdown_wakes_on_notify() {
        let shutdown_signal = Arc::new(AtomicBool::new(false));
        let shutdown_notify = Arc::new(Notify::new());
        let signal = shutdown_signal.clone();
        let notify = shutdown_notify.clone();

        let wait = tokio::spawn(async move { sleep_or_shutdown(&signal, &notify, Duration::from_secs(60)).await });

        shutdown_signal.store(true, Ordering::Release);
        shutdown_notify.notify_waiters();

        assert!(!tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .expect("sleep_or_shutdown should wake promptly")
            .expect("sleep task should complete"));
    }

    #[tokio::test]
    async fn consumer_stats_manager_lifecycle_probe_reports_clean_shutdown() {
        let probe = run_consumer_stats_manager_lifecycle_probe().await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_before_shutdown, 3);
        assert_eq!(probe.task_count_after_shutdown, 0);
    }

    #[test]
    fn start_without_tokio_runtime_does_not_panic() {
        let mgr = make_manager();
        mgr.start();
        assert_eq!(mgr.task_handles.lock().len(), 3);

        mgr.shutdown();

        assert!(mgr.task_handles.lock().is_empty());
    }

    #[test]
    fn default_creates_valid_manager() {
        let mgr = ConsumerStatsManager::default();
        // Should not panic when querying uninitialised key.
        let _ = mgr.consume_status("G", "T");
    }
}
