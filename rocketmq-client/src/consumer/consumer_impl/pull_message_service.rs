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

use rocketmq_common::common::message::message_enum::MessageRequestMode;
use rocketmq_error::RocketMQError;
use rocketmq_rust::ArcMut;
use rocketmq_rust::Shutdown;
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::client_config::ClientConfig;
use crate::consumer::consumer_impl::message_request::MessageRequest;
use crate::consumer::consumer_impl::pop_request::PopRequest;
use crate::consumer::consumer_impl::pull_request::PullRequest;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::runtime::spawn_client_task;
use crate::runtime::spawn_client_tracked_task;
use crate::runtime::ClientTrackedTaskHandle;

/// Default queue capacity for message requests
const DEFAULT_QUEUE_CAPACITY: usize = 4096;

/// Default shutdown timeout in milliseconds
const DEFAULT_SHUTDOWN_TIMEOUT_MS: u64 = 1000;

/// RocketMQ Consumer Pull Message Service
///
/// # Responsibilities
/// - Asynchronously schedules Pull/Pop message requests
/// - Supports both delayed and immediate scheduling
/// - Manages lifecycle of background pull tasks
///
/// # Thread Model
/// - Main loop: Single Tokio task when a runtime is available
/// - Delayed tasks: Tokio tasks, with a current-thread runtime fallback for synchronous callers
///
/// # Shutdown Semantics
/// - `shutdown()` sends stop signal and waits for graceful termination
/// - Main loop processes current request before exiting
/// - Delayed tasks check `is_stopped()` before execution
#[derive(Clone)]
pub struct PullMessageService {
    /// Message request channel sender
    tx: Option<tokio::sync::mpsc::Sender<Box<dyn MessageRequest + Send + 'static>>>,

    /// Shutdown signal broadcaster
    tx_shutdown: Option<tokio::sync::broadcast::Sender<()>>,

    /// Service stopped flag (for fast check)
    stopped: Arc<AtomicBool>,

    /// Queue capacity
    queue_capacity: usize,

    /// Main service loop task handle
    main_task_handle: Arc<tokio::sync::Mutex<Option<ClientTrackedTaskHandle>>>,

    /// Tracks delayed one-shot tasks submitted by execute_*_later APIs.
    scheduled_task_tracker: TaskTracker,

    /// Cancels delayed one-shot tasks during shutdown.
    scheduled_task_shutdown: CancellationToken,
}

#[derive(Debug, Clone, Serialize)]
pub struct PullMessageServiceLifecycleProbe {
    pub healthy: bool,
    pub task_count_before_shutdown: usize,
    pub task_count_after_shutdown: usize,
    pub shutdown_elapsed_us: u128,
}

impl PullMessageService {
    /// Creates a new PullMessageService instance
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_QUEUE_CAPACITY)
    }

    /// Creates a new PullMessageService instance with custom queue capacity
    pub fn with_capacity(queue_capacity: usize) -> Self {
        PullMessageService {
            tx: None,
            tx_shutdown: None,
            stopped: Arc::new(AtomicBool::new(false)),
            queue_capacity,
            main_task_handle: Arc::new(tokio::sync::Mutex::new(None)),
            scheduled_task_tracker: TaskTracker::new(),
            scheduled_task_shutdown: CancellationToken::new(),
        }
    }

    /// Checks if the service is stopped
    #[inline]
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    /// Gets service name
    #[inline]
    pub fn get_service_name(&self) -> &'static str {
        "PullMessageService"
    }

    /// Starts the pull message service
    ///
    /// # Arguments
    /// * `instance` - MQClientInstance for message processing
    ///
    /// # Errors
    /// Returns error if service is already started
    pub async fn start(&mut self, mut instance: ArcMut<MQClientInstance>) -> Result<(), RocketMQError> {
        if self.tx.is_some() {
            warn!("{} already started", self.get_service_name());
            return Ok(());
        }

        let (tx, mut rx) = tokio::sync::mpsc::channel::<Box<dyn MessageRequest + Send + 'static>>(self.queue_capacity);
        let (mut shutdown, tx_shutdown) = Shutdown::new(1);

        let handle = spawn_client_tracked_task("rocketmq-client-pull-message-service", async move {
            info!("{} service started", "PullMessageService");

            loop {
                tokio::select! {
                    _ = shutdown.recv() => {
                        info!("{} received shutdown signal", "PullMessageService");
                        break;
                    }
                    Some(request) = rx.recv() => {
                        // Process request with exception handling
                        if let Err(e) = Self::process_request(request, &mut instance).await {
                            error!("{} failed to process request: {:?}", "PullMessageService", e);
                        }
                    }
                }
            }

            info!("{} service end", "PullMessageService");
        })
        .map_err(|error| RocketMQError::Internal(format!("failed to spawn PullMessageService main loop: {error}")))?;

        self.tx = Some(tx);
        self.tx_shutdown = Some(tx_shutdown);
        self.stopped.store(false, Ordering::Release);
        *self.main_task_handle.lock().await = Some(handle);

        Ok(())
    }

    async fn main_task_count(&self) -> usize {
        self.main_task_handle
            .lock()
            .await
            .as_ref()
            .map(ClientTrackedTaskHandle::task_count)
            .unwrap_or_default()
    }

    /// Processes a message request (Pull or Pop)
    ///
    /// # Arguments
    /// * `request` - Message request to process
    /// * `instance` - MQClientInstance for consumer lookup
    ///
    /// # Errors
    /// Returns error if request processing fails
    async fn process_request(
        request: Box<dyn MessageRequest + Send + 'static>,
        instance: &mut MQClientInstance,
    ) -> Result<(), RocketMQError> {
        match request.get_message_request_mode() {
            MessageRequestMode::Pull => {
                // Safe downcast using Any trait
                if let Ok(pull_request) = request.into_any().downcast::<PullRequest>() {
                    Self::pull_message(*pull_request, instance).await;
                    Ok(())
                } else {
                    Err(RocketMQError::Internal("Failed to downcast to PullRequest".to_string()))
                }
            }
            MessageRequestMode::Pop => {
                if let Ok(pop_request) = request.into_any().downcast::<PopRequest>() {
                    Self::pop_message(*pop_request, instance).await;
                    Ok(())
                } else {
                    Err(RocketMQError::Internal("Failed to downcast to PopRequest".to_string()))
                }
            }
        }
    }

    /// Handles pull message request
    async fn pull_message(request: PullRequest, instance: &mut MQClientInstance) {
        if let Some(mut consumer) = instance.select_consumer(request.get_consumer_group()).await {
            consumer.pull_message(request).await;
        } else {
            warn!("No matched consumer for the PullRequest {}, drop it", request)
        }
    }

    /// Handles pop message request
    async fn pop_message(request: PopRequest, instance: &mut MQClientInstance) {
        if let Some(mut consumer) = instance.select_consumer(request.get_consumer_group()).await {
            consumer.pop_message(request).await;
        } else {
            warn!("No matched consumer for the PopRequest {}, drop it", request)
        }
    }

    /// Executes pull request with delay
    ///
    /// # Arguments
    /// * `pull_request` - The pull request to execute
    /// * `time_delay` - Delay in milliseconds before execution
    ///
    /// # Behavior
    /// - Returns immediately if service is stopped
    /// - Spawns a tokio task that sleeps then sends the request
    pub fn execute_pull_request_later(&self, pull_request: PullRequest, time_delay: u64) {
        if self.is_stopped() {
            warn!("{} has shutdown, cannot execute later task", self.get_service_name());
            return;
        }

        let this = self.clone();
        let request = pull_request.clone();

        spawn_scheduled_pull_message_task(
            "rocketmq-client-pull-request-delay",
            &self.scheduled_task_tracker,
            &self.scheduled_task_shutdown,
            async move {
                tokio::time::sleep(Duration::from_millis(time_delay)).await;

                if this.is_stopped() {
                    return;
                }

                if let Some(tx) = &this.tx {
                    if let Err(e) = tx.send(Box::new(request)).await {
                        warn!("Failed to send pull request: {:?}", e);
                    }
                }
            },
        );
    }

    /// Executes pull request immediately
    ///
    /// # Arguments
    /// * `pull_request` - The pull request to execute
    ///
    /// # Behavior
    /// Logs error but does not return error (aligned with Java implementation)
    pub async fn execute_pull_request_immediately(&self, pull_request: PullRequest) {
        if self.is_stopped() {
            warn!("PullMessageService has shutdown");
            return;
        }

        if let Some(tx) = &self.tx {
            if let Err(e) = tx.send(Box::new(pull_request)).await {
                error!("executePullRequestImmediately messageRequestQueue.put error: {:?}", e);
            }
        } else {
            warn!("PullMessageService not started");
        }
    }

    /// Executes pop request with delay
    pub fn execute_pop_pull_request_later(&self, pop_request: PopRequest, time_delay: u64) {
        if self.is_stopped() {
            warn!("{} has shutdown, cannot execute later task", self.get_service_name());
            return;
        }

        let this = self.clone();
        let request = pop_request.clone();

        spawn_scheduled_pull_message_task(
            "rocketmq-client-pop-request-delay",
            &self.scheduled_task_tracker,
            &self.scheduled_task_shutdown,
            async move {
                tokio::time::sleep(Duration::from_millis(time_delay)).await;

                if this.is_stopped() {
                    return;
                }

                if let Some(tx) = &this.tx {
                    if let Err(e) = tx.send(Box::new(request)).await {
                        warn!("Failed to send pop request: {:?}", e);
                    }
                }
            },
        );
    }

    /// Executes pop request immediately
    pub async fn execute_pop_pull_request_immediately(&self, pop_request: PopRequest) {
        if self.is_stopped() {
            warn!("PullMessageService has shutdown");
            return;
        }

        if let Some(tx) = &self.tx {
            if let Err(e) = tx.send(Box::new(pop_request)).await {
                error!(
                    "executePopPullRequestImmediately messageRequestQueue.put error: {:?}",
                    e
                );
            }
        } else {
            warn!("PullMessageService not started");
        }
    }

    /// Executes a generic task with delay (equivalent to Java's executeTaskLater)
    ///
    /// # Arguments
    /// * `task` - Task function to execute
    /// * `time_delay` - Delay in milliseconds before execution
    pub fn execute_task_later<F>(&self, task: F, time_delay: u64)
    where
        F: FnOnce() + Send + 'static,
    {
        if self.is_stopped() {
            warn!("{} has shutdown, cannot execute task", self.get_service_name());
            return;
        }

        let stopped = self.stopped.clone();
        spawn_scheduled_pull_message_task(
            "rocketmq-client-pull-task-delay",
            &self.scheduled_task_tracker,
            &self.scheduled_task_shutdown,
            async move {
                tokio::time::sleep(Duration::from_millis(time_delay)).await;
                if stopped.load(Ordering::Acquire) {
                    return;
                }
                task();
            },
        );
    }

    /// Executes a generic task immediately (equivalent to Java's executeTask)
    pub fn execute_task<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        if self.is_stopped() {
            warn!("{} has shutdown, cannot execute task", self.get_service_name());
            return;
        }

        spawn_scheduled_pull_message_task(
            "rocketmq-client-pull-task",
            &self.scheduled_task_tracker,
            &self.scheduled_task_shutdown,
            async move {
                task();
            },
        );
    }

    /// Gracefully shuts down the service
    ///
    /// # Arguments
    /// * `timeout_ms` - Maximum time to wait for shutdown (milliseconds)
    ///
    /// # Behavior
    /// - Sets stopped flag
    /// - Sends shutdown signal to main loop
    /// - Cancels all scheduled tasks
    /// - Waits for main loop to finish (with timeout)
    pub async fn shutdown(&self, timeout_ms: u64) -> Result<(), RocketMQError> {
        if self.is_stopped() {
            warn!("{} already stopped", self.get_service_name());
            return Ok(());
        }

        info!("{} shutting down...", self.get_service_name());

        // 1. Set stopped flag
        self.stopped.store(true, Ordering::Release);
        self.scheduled_task_shutdown.cancel();

        // 2. Send shutdown signal
        if let Some(tx_shutdown) = &self.tx_shutdown {
            tx_shutdown
                .send(())
                .map_err(|_| RocketMQError::Internal("Failed to send shutdown signal".to_string()))?;
        }

        // 3. Wait for main loop to exit (with timeout)
        let timeout = Duration::from_millis(timeout_ms);
        let main_task = self.main_task_handle.lock().await.take();
        if let Some(handle) = main_task {
            let report = handle.shutdown(timeout).await;
            if !report.is_healthy() {
                warn!(
                    report = %report.to_json(),
                    "{} main loop shutdown report is unhealthy",
                    self.get_service_name()
                );
            }
        }

        self.scheduled_task_tracker.close();
        if tokio::time::timeout(timeout, self.scheduled_task_tracker.wait())
            .await
            .is_err()
        {
            warn!(
                "{} scheduled task shutdown timeout after {}ms",
                self.get_service_name(),
                timeout_ms
            );
        }

        info!("{} shutdown completed", self.get_service_name());
        Ok(())
    }

    /// Shuts down with default timeout
    pub async fn shutdown_default(&self) -> Result<(), RocketMQError> {
        self.shutdown(DEFAULT_SHUTDOWN_TIMEOUT_MS).await
    }
}

impl Default for PullMessageService {
    fn default() -> Self {
        Self::new()
    }
}

#[doc(hidden)]
pub async fn run_pull_message_service_lifecycle_probe() -> PullMessageServiceLifecycleProbe {
    let mut service = PullMessageService::new();
    let mut instance = MQClientInstance::new_arc(ClientConfig::default(), 0, "pull-message-service-probe", None);

    let start_result = service.start(instance.clone()).await;
    let task_count_before_shutdown = service.main_task_count().await;

    let shutdown_started = Instant::now();
    let shutdown_result = service.shutdown(1_000).await;
    let shutdown_elapsed_us = shutdown_started.elapsed().as_micros();
    let task_count_after_shutdown = service.main_task_count().await;

    instance.shutdown().await;

    PullMessageServiceLifecycleProbe {
        healthy: start_result.is_ok()
            && shutdown_result.is_ok()
            && task_count_before_shutdown == 1
            && task_count_after_shutdown == 0,
        task_count_before_shutdown,
        task_count_after_shutdown,
        shutdown_elapsed_us,
    }
}

fn spawn_scheduled_pull_message_task<F>(
    thread_name: &'static str,
    tracker: &TaskTracker,
    shutdown_token: &CancellationToken,
    task: F,
) where
    F: Future<Output = ()> + Send + 'static,
{
    if shutdown_token.is_cancelled() {
        return;
    }

    let shutdown_token = shutdown_token.clone();
    let tracked_task = tracker.track_future(async move {
        tokio::select! {
            biased;
            _ = shutdown_token.cancelled() => {},
            _ = task => {},
        }
    });

    if let Err(error) = spawn_client_task(thread_name, tracked_task) {
        error!("Failed to spawn {} background task: {}", thread_name, error);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execute_task_without_tokio_runtime_does_not_spawn_panic() {
        let service = PullMessageService::new();
        let ran = Arc::new(AtomicBool::new(false));
        let ran_clone = ran.clone();

        service.execute_task(move || ran_clone.store(true, Ordering::Release));

        std::thread::sleep(Duration::from_millis(50));
        assert!(ran.load(Ordering::Acquire));
    }

    #[test]
    fn execute_task_later_without_tokio_runtime_does_not_spawn_panic() {
        let service = PullMessageService::new();
        let ran = Arc::new(AtomicBool::new(false));
        let ran_clone = ran.clone();

        service.execute_task_later(move || ran_clone.store(true, Ordering::Release), 1);

        std::thread::sleep(Duration::from_millis(50));
        assert!(ran.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn execute_task_is_tracked_until_completion() {
        let service = PullMessageService::new();
        let ran = Arc::new(AtomicBool::new(false));
        let ran_clone = ran.clone();

        service.execute_task(move || ran_clone.store(true, Ordering::Release));
        service.scheduled_task_tracker.close();

        tokio::time::timeout(Duration::from_secs(1), service.scheduled_task_tracker.wait())
            .await
            .expect("scheduled task tracker should finish completed immediate task");

        assert!(ran.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn shutdown_cancels_delayed_tasks_and_waits_for_tracker() {
        let service = PullMessageService::new();
        let ran = Arc::new(AtomicBool::new(false));
        let ran_clone = ran.clone();

        service.execute_task_later(move || ran_clone.store(true, Ordering::Release), 60_000);
        tokio::task::yield_now().await;

        service.shutdown(100).await.expect("shutdown should succeed");

        tokio::time::timeout(Duration::from_secs(1), service.scheduled_task_tracker.wait())
            .await
            .expect("shutdown should release delayed task tracker");

        assert!(!ran.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn pull_message_service_lifecycle_probe_reports_clean_shutdown() {
        let probe = run_pull_message_service_lifecycle_probe().await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_before_shutdown, 1);
        assert_eq!(probe.task_count_after_shutdown, 0);
    }
}
