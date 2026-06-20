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

use std::sync::LazyLock;
use tokio::sync::watch;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::interval;
use tracing::error;
use tracing::warn;

use crate::producer::request_response_future::RequestResponseFuture;
use crate::runtime::spawn_client_task;

const REQUEST_SCAN_INITIAL_DELAY: Duration = Duration::from_millis(3_000);
const REQUEST_SCAN_INTERVAL: Duration = Duration::from_millis(1_000);
const REQUEST_SCAN_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

pub static REQUEST_FUTURE_HOLDER: LazyLock<Arc<RequestFutureHolder>> =
    LazyLock::new(|| Arc::new(RequestFutureHolder::new()));

pub struct RequestFutureHolder {
    request_future_table: Arc<RwLock<HashMap<String, Arc<RequestResponseFuture>>>>,
    producer_set: Arc<Mutex<HashSet<String>>>,
    scheduled_task: Arc<Mutex<Option<RequestScanTask>>>,
}

struct RequestScanTask {
    shutdown_tx: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

impl RequestScanTask {
    fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    async fn shutdown(self, timeout: Duration) -> bool {
        let _ = self.shutdown_tx.send(true);
        let mut handle = self.handle;
        match tokio::time::timeout(timeout, &mut handle).await {
            Ok(Ok(())) => true,
            Ok(Err(error)) if error.is_cancelled() => true,
            Ok(Err(error)) => {
                warn!(%error, "request future scan task exited with join error");
                true
            }
            Err(_) => {
                handle.abort();
                match handle.await {
                    Ok(()) => {}
                    Err(error) if error.is_cancelled() => {}
                    Err(error) => warn!(%error, "request future scan task aborted with join error"),
                }
                false
            }
        }
    }
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
        self.producer_set.lock().await.insert(producer_id.into());

        let mut scheduled_task = self.scheduled_task.lock().await;
        if scheduled_task.as_ref().is_some_and(|handle| !handle.is_finished()) {
            return;
        }

        let holder = Arc::clone(self);
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let handle = match spawn_client_task("rocketmq-client-request-future-scan", async move {
            tokio::select! {
                _ = tokio::time::sleep(REQUEST_SCAN_INITIAL_DELAY) => {}
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        return;
                    }
                }
            }

            let mut interval = interval(REQUEST_SCAN_INTERVAL);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        holder.scan_expired_request().await;
                    }
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        }) {
            Ok(handle) => handle,
            Err(error) => {
                error!(%error, "failed to spawn request future scan task");
                return;
            }
        };
        *scheduled_task = Some(RequestScanTask { shutdown_tx, handle });
    }

    pub async fn shutdown(&self, producer_id: &str) {
        let mut producers = self.producer_set.lock().await;
        producers.remove(producer_id);
        let should_stop = producers.is_empty();
        drop(producers);

        if should_stop {
            if let Some(task) = self.scheduled_task.lock().await.take() {
                if !task.shutdown(REQUEST_SCAN_SHUTDOWN_TIMEOUT).await {
                    warn!(
                        timeout_ms = REQUEST_SCAN_SHUTDOWN_TIMEOUT.as_millis(),
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
                .is_some_and(|handle| !handle.is_finished())
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
    async fn request_scan_task_shutdown_waits_for_worker_completion() {
        let task = RequestScanTask {
            shutdown_tx: watch::channel(false).0,
            handle: tokio::spawn(async {}),
        };

        assert!(task.shutdown(Duration::from_secs(1)).await);
    }

    #[tokio::test]
    async fn request_scan_task_shutdown_aborts_after_timeout() {
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let task = RequestScanTask {
            shutdown_tx,
            handle: tokio::spawn(async {
                std::future::pending::<()>().await;
            }),
        };

        assert!(!task.shutdown(Duration::from_millis(20)).await);
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
}
