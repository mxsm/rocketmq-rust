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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_common::common::message::message_enum::MessageRequestMode;
use rocketmq_error::RocketMQError;
use rocketmq_rust::ArcMut;
use rocketmq_rust::Shutdown;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::consumer::consumer_impl::message_request::MessageRequest;
use crate::consumer::consumer_impl::pop_request::PopRequest;
use crate::consumer::consumer_impl::pull_request::PullRequest;
use crate::factory::mq_client_instance::MQClientInstance;

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
/// - Main loop: Single Tokio task
/// - Delayed tasks: Individual tokio::spawn tasks
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

        self.tx = Some(tx);
        self.tx_shutdown = Some(tx_shutdown);
        self.stopped.store(false, Ordering::Release);

        tokio::spawn(async move {
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
        });

        Ok(())
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
        mut request: Box<dyn MessageRequest + Send + 'static>,
        instance: &mut MQClientInstance,
    ) -> Result<(), RocketMQError> {
        match request.get_message_request_mode() {
            MessageRequestMode::Pull => {
                // Safe downcast using Any trait
                if let Some(pull_request) = request.as_any_mut().downcast_ref::<PullRequest>() {
                    Self::pull_message(pull_request.clone(), instance).await;
                    Ok(())
                } else {
                    Err(RocketMQError::Internal("Failed to downcast to PullRequest".to_string()))
                }
            }
            MessageRequestMode::Pop => {
                if let Some(pop_request) = request.as_any_mut().downcast_ref::<PopRequest>() {
                    Self::pop_message(pop_request.clone(), instance).await;
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

        // Use a one-shot scheduled task
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(time_delay)).await;

            if this.is_stopped() {
                return;
            }

            if let Some(tx) = &this.tx {
                if let Err(e) = tx.send(Box::new(request)).await {
                    warn!("Failed to send pull request: {:?}", e);
                }
            }
        });
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

        // Use a one-shot scheduled task
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(time_delay)).await;

            if this.is_stopped() {
                return;
            }

            if let Some(tx) = &this.tx {
                if let Err(e) = tx.send(Box::new(request)).await {
                    warn!("Failed to send pop request: {:?}", e);
                }
            }
        });
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

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(time_delay)).await;
            task();
        });
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

        tokio::spawn(async move {
            task();
        });
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

        // 2. Send shutdown signal
        if let Some(tx_shutdown) = &self.tx_shutdown {
            tx_shutdown
                .send(())
                .map_err(|_| RocketMQError::Internal("Failed to send shutdown signal".to_string()))?;
        }

        // 3. Wait for main loop to exit (with timeout)
        let wait_result = tokio::time::timeout(Duration::from_millis(timeout_ms), async {
            // Wait for tx to be dropped (main loop exited)
            while self.tx.as_ref().map(|tx| !tx.is_closed()).unwrap_or(false) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        if wait_result.is_err() {
            warn!(
                "{} shutdown timeout after {}ms, forcing exit",
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
