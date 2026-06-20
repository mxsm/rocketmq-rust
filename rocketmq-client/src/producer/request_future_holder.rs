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
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_runtime::ScheduledTaskSnapshot;
use serde::Serialize;
use std::sync::LazyLock;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tracing::error;
use tracing::warn;

use crate::producer::request_response_future::RequestResponseFuture;
use crate::runtime::schedule_client_fixed_delay_task;
use crate::runtime::ClientScheduledTaskHandle;

const REQUEST_SCAN_INITIAL_DELAY: Duration = Duration::from_millis(3_000);
const REQUEST_SCAN_INTERVAL: Duration = Duration::from_millis(1_000);
const REQUEST_SCAN_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

pub static REQUEST_FUTURE_HOLDER: LazyLock<Arc<RequestFutureHolder>> =
    LazyLock::new(|| Arc::new(RequestFutureHolder::new()));

pub struct RequestFutureHolder {
    request_future_table: Arc<RwLock<HashMap<String, Arc<RequestResponseFuture>>>>,
    producer_set: Arc<Mutex<HashSet<String>>>,
    scheduled_task: Arc<Mutex<Option<ClientScheduledTaskHandle>>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RequestFutureHolderLifecycleProbe {
    pub task_count_before_shutdown: usize,
    pub task_count_after_shutdown: usize,
    pub scheduled_runs: u64,
    pub scheduled_skips: u64,
    pub scheduled_overlaps: u64,
    pub scheduled_failures: u64,
    pub shutdown_elapsed_us: u128,
    pub healthy: bool,
}

impl RequestFutureHolder {
    fn new() -> Self {
        Self {
            request_future_table: Arc::new(RwLock::new(HashMap::new())),
            producer_set: Arc::new(Mutex::new(HashSet::new())),
            scheduled_task: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn scan_expired_request(&self) {
        let mut rf_list = Vec::new();
        {
            let mut table = self.request_future_table.write().await;
            let mut expired_keys = Vec::new();

            for (key, future) in table.iter() {
                if future.is_timeout() {
                    expired_keys.push(key.clone());
                    rf_list.push(future.clone());
                }
            }

            for key in expired_keys {
                table.remove(&key);
            }
        }

        for rf in rf_list {
            let cause = Box::new(rocketmq_error::RocketMQError::Timeout {
                operation: "request_reply",
                timeout_ms: rf.get_timeout_millis(),
            });
            rf.set_cause(cause);
            rf.execute_request_callback();
        }
    }

    pub async fn start_scheduled_task(self: &Arc<Self>, producer_id: impl Into<String>) {
        self.start_scheduled_task_with_schedule(producer_id, REQUEST_SCAN_INITIAL_DELAY, REQUEST_SCAN_INTERVAL)
            .await;
    }

    async fn start_scheduled_task_with_schedule(
        self: &Arc<Self>,
        producer_id: impl Into<String>,
        initial_delay: Duration,
        scan_interval: Duration,
    ) {
        self.producer_set.lock().await.insert(producer_id.into());
        let mut scheduled_task = self.scheduled_task.lock().await;
        if scheduled_task
            .as_ref()
            .is_some_and(ClientScheduledTaskHandle::is_running)
        {
            return;
        }

        let holder = Arc::clone(self);
        let handle = match schedule_client_fixed_delay_task(
            "rocketmq-client-request-future-scan",
            initial_delay,
            scan_interval.max(Duration::from_millis(1)),
            REQUEST_SCAN_SHUTDOWN_TIMEOUT,
            move || {
                let holder = Arc::clone(&holder);
                async move {
                    holder.scan_expired_request().await;
                }
            },
        ) {
            Ok(handle) => handle,
            Err(error) => {
                error!(%error, "failed to spawn request future scan task");
                return;
            }
        };
        *scheduled_task = Some(handle);
    }

    pub async fn shutdown(&self, producer_id: &str) {
        let mut producers = self.producer_set.lock().await;
        producers.remove(producer_id);
        let should_stop = producers.is_empty();
        drop(producers);

        if should_stop {
            if let Some(task) = self.scheduled_task.lock().await.take() {
                let report = task.shutdown(REQUEST_SCAN_SHUTDOWN_TIMEOUT).await;
                if !report.is_healthy() {
                    warn!(
                        timeout_ms = REQUEST_SCAN_SHUTDOWN_TIMEOUT.as_millis(),
                        report = %report.to_json(),
                        "request future scan task did not stop before timeout and was aborted"
                    );
                }
            }
        }
    }

    pub async fn put_request(&self, correlation_id: String, request: Arc<RequestResponseFuture>) {
        let mut table = self.request_future_table.write().await;
        table.insert(correlation_id, request);
    }

    pub async fn remove_request(&self, correlation_id: &str) {
        let mut table = self.request_future_table.write().await;
        table.remove(correlation_id);
    }

    pub async fn remove_request_and_get(&self, correlation_id: &str) -> Option<Arc<RequestResponseFuture>> {
        let mut table = self.request_future_table.write().await;
        table.remove(correlation_id)
    }

    pub async fn get_request(&self, correlation_id: &str) -> Option<Arc<RequestResponseFuture>> {
        let table = self.request_future_table.read().await;
        table.get(correlation_id).cloned()
    }

    async fn task_count(&self) -> usize {
        self.scheduled_task
            .lock()
            .await
            .as_ref()
            .map(ClientScheduledTaskHandle::task_count)
            .unwrap_or_default()
    }

    async fn schedule_snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.scheduled_task
            .lock()
            .await
            .as_ref()
            .map(ClientScheduledTaskHandle::schedule_snapshot)
            .unwrap_or_default()
    }
}

#[doc(hidden)]
pub async fn run_request_future_holder_lifecycle_probe() -> RequestFutureHolderLifecycleProbe {
    let holder = Arc::new(RequestFutureHolder::new());
    holder
        .start_scheduled_task_with_schedule("producer-a", Duration::ZERO, Duration::from_millis(1))
        .await;

    let mut snapshots = holder.schedule_snapshot().await;
    for _ in 0..100 {
        if snapshots
            .iter()
            .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
        snapshots = holder.schedule_snapshot().await;
    }

    let scheduled_runs = snapshots.iter().map(|snapshot| snapshot.runs).sum();
    let scheduled_skips = snapshots.iter().map(|snapshot| snapshot.skips).sum();
    let scheduled_overlaps = snapshots.iter().map(|snapshot| snapshot.overlaps).sum();
    let scheduled_failures = snapshots.iter().map(|snapshot| snapshot.failures).sum();
    let task_count_before_shutdown = holder.task_count().await;
    let shutdown_started_at = std::time::Instant::now();
    holder.shutdown("producer-a").await;
    let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
    let task_count_after_shutdown = holder.task_count().await;
    let healthy = scheduled_runs > 0
        && scheduled_overlaps == 0
        && scheduled_failures == 0
        && task_count_before_shutdown > 0
        && task_count_after_shutdown == 0;

    RequestFutureHolderLifecycleProbe {
        task_count_before_shutdown,
        task_count_after_shutdown,
        scheduled_runs,
        scheduled_skips,
        scheduled_overlaps,
        scheduled_failures,
        shutdown_elapsed_us,
        healthy,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    use rocketmq_common::common::message::MessageTrait;

    use super::*;

    impl RequestFutureHolder {
        async fn producer_count(&self) -> usize {
            self.producer_set.lock().await.len()
        }

        async fn scheduled_task_active(&self) -> bool {
            self.scheduled_task
                .lock()
                .await
                .as_ref()
                .is_some_and(ClientScheduledTaskHandle::is_running)
        }
    }

    #[tokio::test]
    async fn scheduled_task_lifecycle_matches_java_reference_counting() {
        let holder = Arc::new(RequestFutureHolder::new());

        holder.start_scheduled_task("producer-a").await;
        assert_eq!(holder.producer_count().await, 1);
        assert!(holder.scheduled_task_active().await);

        holder.start_scheduled_task("producer-a").await;
        assert_eq!(holder.producer_count().await, 1);
        assert!(holder.scheduled_task_active().await);

        holder.start_scheduled_task("producer-b").await;
        assert_eq!(holder.producer_count().await, 2);
        assert!(holder.scheduled_task_active().await);

        holder.shutdown("producer-a").await;
        assert_eq!(holder.producer_count().await, 1);
        assert!(holder.scheduled_task_active().await);

        holder.shutdown("producer-b").await;
        assert_eq!(holder.producer_count().await, 0);
        assert!(!holder.scheduled_task_active().await);
    }

    #[tokio::test]
    async fn scheduled_task_shutdown_interrupts_initial_delay() {
        let holder = Arc::new(RequestFutureHolder::new());

        holder.start_scheduled_task("producer-a").await;
        assert!(holder.scheduled_task_active().await);

        let started = Instant::now();
        holder.shutdown("producer-a").await;

        assert!(
            started.elapsed() < Duration::from_millis(500),
            "shutdown should not wait for the full initial scan delay"
        );
        assert!(!holder.scheduled_task_active().await);
    }

    #[tokio::test]
    async fn scan_expired_request_removes_future_and_executes_timeout_callback() {
        let holder = RequestFutureHolder::new();
        let callback_called = Arc::new(AtomicBool::new(false));
        let callback_called_inner = Arc::clone(&callback_called);
        let callback = Arc::new(
            move |response: Option<&dyn MessageTrait>, error: Option<&dyn std::error::Error>| {
                assert!(response.is_none());
                let error = error.expect("timeout scan should pass timeout cause");
                assert!(error.to_string().contains("request_reply"));
                callback_called_inner.store(true, Ordering::SeqCst);
            },
        );
        let request = Arc::new(RequestResponseFuture::new("corr-timeout".into(), 0, Some(callback)));

        holder.put_request("corr-timeout".to_string(), request).await;
        tokio::time::sleep(Duration::from_millis(1)).await;
        holder.scan_expired_request().await;

        assert!(holder.get_request("corr-timeout").await.is_none());
        assert!(callback_called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn remove_request_and_get_returns_and_removes_future() {
        let holder = RequestFutureHolder::new();
        let request = Arc::new(RequestResponseFuture::new("corr-remove".into(), 3_000, None));

        holder.put_request("corr-remove".to_string(), request.clone()).await;

        let removed = holder
            .remove_request_and_get("corr-remove")
            .await
            .expect("request should be removed");

        assert!(Arc::ptr_eq(&removed, &request));
        assert!(holder.get_request("corr-remove").await.is_none());
    }

    #[tokio::test]
    async fn request_future_holder_lifecycle_probe_reports_clean_shutdown() {
        let probe = run_request_future_holder_lifecycle_probe().await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }
}
