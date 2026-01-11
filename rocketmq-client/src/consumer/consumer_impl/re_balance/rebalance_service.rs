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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_rust::ArcMut;
use rocketmq_rust::Shutdown;
use tokio::select;
use tokio::sync::Notify;
use tokio::time::Instant;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::factory::mq_client_instance::MQClientInstance;

/// Configuration for RebalanceService.
///
/// This struct contains all configurable parameters for the rebalance service,
/// allowing for flexible configuration in different environments (production, testing, etc.).
#[derive(Debug, Clone)]
pub struct RebalanceConfig {
    /// Wait interval when rebalance is balanced (in milliseconds).
    /// Default: 20000ms (20 seconds)
    pub wait_interval_ms: u64,

    /// Minimum interval between rebalance operations (in milliseconds).
    /// Default: 1000ms (1 second)
    pub min_interval_ms: u64,

    /// Whether to enable dynamic interval adjustment.
    /// If true, uses wait_interval when balanced, min_interval when not balanced.
    /// Default: true
    pub enable_dynamic_interval: bool,
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        Self {
            wait_interval_ms: 20000,
            min_interval_ms: 1000,
            enable_dynamic_interval: true,
        }
    }
}

impl RebalanceConfig {
    /// Create a new RebalanceConfig with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a RebalanceConfig from environment variables.
    ///
    /// Environment variables:
    /// - `rocketmq.client.rebalance.waitInterval`: Wait interval in milliseconds
    /// - `rocketmq.client.rebalance.minInterval`: Minimum interval in milliseconds
    pub fn from_env() -> Self {
        let wait_interval_ms = std::env::var("rocketmq.client.rebalance.waitInterval")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(20000);

        let min_interval_ms = std::env::var("rocketmq.client.rebalance.minInterval")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000);

        Self {
            wait_interval_ms,
            min_interval_ms,
            enable_dynamic_interval: true,
        }
    }

    /// Get wait interval as Duration.
    pub fn wait_interval(&self) -> Duration {
        Duration::from_millis(self.wait_interval_ms)
    }

    /// Get minimum interval as Duration.
    pub fn min_interval(&self) -> Duration {
        Duration::from_millis(self.min_interval_ms)
    }

    /// Validate configuration values.
    ///
    /// # Returns
    ///
    /// `Ok(())` if valid, `Err(String)` with error message if invalid.
    pub fn validate(&self) -> Result<(), String> {
        if self.wait_interval_ms == 0 {
            return Err("wait_interval_ms must be greater than 0".to_string());
        }
        if self.min_interval_ms == 0 {
            return Err("min_interval_ms must be greater than 0".to_string());
        }
        if self.min_interval_ms > self.wait_interval_ms {
            return Err(format!(
                "min_interval_ms ({}) must be <= wait_interval_ms ({})",
                self.min_interval_ms, self.wait_interval_ms
            ));
        }
        Ok(())
    }
}

///`RebalanceService` is a crucial struct in Apache RocketMQ-Rust, responsible for coordinating
/// load balancing among the consumers of a message queue. Its primary function is to ensure that
/// consumer instances can reasonably distribute message queues among multiple consumers, achieving
/// efficient message processing and balanced load. Specifically, the role of `RebalanceService`
/// includes the following aspects:
///
/// 1. **Consumer Load Balancing** When consumers in a consumer group start, stop, or fail,
///    `RebalanceService` dynamically adjusts the message queue distribution between consumers to
///    ensure each consumer processes a reasonable number of queues, avoiding situations where some
///    consumers are overburdened or underutilized.
/// 2. **Queue Allocation and Revocation** `RebalanceService` triggers reallocation of consumer
///    queues periodically or when certain events occur. It decides which queues should be processed
///    by which consumers based on the number of consumers, the state of consumer instances, and the
///    number of message queues.
/// 3. **Consumer Failure Recovery** If a consumer instance fails or goes offline,
///    `RebalanceService` triggers a rebalancing operation, redistributing the queues it was
///    responsible for to other online consumers, ensuring that messages are not lost and the load
///    is evenly distributed.
/// 4. **Consumer Rejoining** When a new consumer joins the consumer group, `RebalanceService`
///    initiates a rebalancing process, adding the new consumer to the queue allocation and
///    adjusting the queues for each consumer to ensure the load is balanced across the entire
///    consumer group.
/// 5. **Listening for Queue State Changes** `RebalanceService` listens for changes in the state of
///    consumers and queues in RocketMQ, adjusting queue allocations based on these changes.
///
/// This service is integral to ensuring that the consumers in a RocketMQ cluster maintain high
/// availability, optimal resource utilization, and fault tolerance while processing messages
/// efficiently.

#[derive(Clone)]
pub struct RebalanceService {
    config: RebalanceConfig,
    notify: Arc<Notify>,
    tx_shutdown: Option<tokio::sync::broadcast::Sender<()>>,
    started: Arc<AtomicBool>,
    task_abort_handle: Option<Arc<tokio::task::AbortHandle>>,
    // Health check and metrics
    last_success_timestamp: Arc<AtomicU64>,
    total_rebalance_count: Arc<AtomicU64>,
    success_count: Arc<AtomicU64>,
    fail_count: Arc<AtomicU64>,
}

impl RebalanceService {
    /// Create a new RebalanceService with default configuration from environment variables.
    pub fn new() -> Self {
        Self::with_config(RebalanceConfig::from_env())
    }

    /// Create a new RebalanceService with custom configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - RebalanceConfig to use
    ///
    /// # Panics
    ///
    /// Panics if the configuration is invalid.
    pub fn with_config(config: RebalanceConfig) -> Self {
        if let Err(e) = config.validate() {
            panic!("Invalid RebalanceConfig: {}", e);
        }

        RebalanceService {
            config,
            notify: Arc::new(Notify::new()),
            tx_shutdown: None,
            started: Arc::new(AtomicBool::new(false)),
            task_abort_handle: None,
            last_success_timestamp: Arc::new(AtomicU64::new(0)),
            total_rebalance_count: Arc::new(AtomicU64::new(0)),
            success_count: Arc::new(AtomicU64::new(0)),
            fail_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Get a reference to the configuration.
    pub fn config(&self) -> &RebalanceConfig {
        &self.config
    }

    pub async fn start(
        &mut self,
        mut instance: ArcMut<MQClientInstance>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Prevent duplicate starts
        if self.started.swap(true, Ordering::SeqCst) {
            warn!("RebalanceService already started, ignoring duplicate start call");
            return Ok(());
        }

        let notify = self.notify.clone();
        let (mut shutdown, tx_shutdown) = Shutdown::new(1);
        self.tx_shutdown = Some(tx_shutdown);

        let started_clone = self.started.clone();
        let last_success_ts = self.last_success_timestamp.clone();
        let total_count = self.total_rebalance_count.clone();
        let success_count_clone = self.success_count.clone();
        let fail_count_clone = self.fail_count.clone();

        // Clone config for use in spawned task
        let config = self.config.clone();

        let handle = tokio::spawn(async move {
            let mut last_rebalance_timestamp = tokio::time::Instant::now();
            let min_interval = config.min_interval();
            let mut real_wait_interval = config.wait_interval();
            info!("RebalanceService started, timestamp={}", get_current_millis());
            loop {
                select! {
                    _ = notify.notified() => {
                        info!("RebalanceService wakeup triggered");
                    }
                    _ = shutdown.recv() => {
                        info!("RebalanceService shutdown signal received, timestamp={}", get_current_millis());
                        started_clone.store(false, Ordering::SeqCst);
                        return;
                    }
                    _ = tokio::time::sleep(real_wait_interval) => {}
                }
                if shutdown.is_shutdown() {
                    started_clone.store(false, Ordering::SeqCst);
                    return;
                }
                let now = tokio::time::Instant::now();
                let interval = now - last_rebalance_timestamp;
                if interval < min_interval {
                    real_wait_interval = min_interval - interval;
                } else {
                    // Rebalance operation with exception handling and metrics
                    let start_time = tokio::time::Instant::now();
                    total_count.fetch_add(1, Ordering::Relaxed);

                    let balanced = match instance.do_rebalance().await {
                        Ok(result) => {
                            success_count_clone.fetch_add(1, Ordering::Relaxed);
                            last_success_ts.store(get_current_millis(), Ordering::Relaxed);
                            result
                        }
                        Err(e) => {
                            fail_count_clone.fetch_add(1, Ordering::Relaxed);
                            error!(
                                "RebalanceService: do_rebalance error: {:?}, timestamp={}",
                                e,
                                get_current_millis()
                            );
                            false // Treat as unbalanced on exception, retry with min interval
                        }
                    };

                    let duration_ms = start_time.elapsed().as_millis();
                    let next_interval = if config.enable_dynamic_interval {
                        if balanced {
                            config.wait_interval()
                        } else {
                            min_interval
                        }
                    } else {
                        config.wait_interval()
                    };

                    info!(
                        "RebalanceService: do_rebalance completed, balanced={}, duration={}ms, next_interval={}ms, \
                         timestamp={}",
                        balanced,
                        duration_ms,
                        next_interval.as_millis(),
                        get_current_millis()
                    );

                    real_wait_interval = next_interval;
                    last_rebalance_timestamp = now;
                }
            }
        });

        self.task_abort_handle = Some(Arc::new(handle.abort_handle()));
        Ok(())
    }

    pub fn wakeup(&self) {
        self.notify.notify_waiters();
    }

    pub async fn shutdown(&self, timeout_ms: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // If not started, return directly
        if !self.started.load(Ordering::SeqCst) {
            info!("RebalanceService not started, shutdown skipped");
            return Ok(());
        }

        // Send shutdown signal
        if let Some(tx_shutdown) = &self.tx_shutdown {
            if let Err(e) = tx_shutdown.send(()) {
                warn!("Failed to send shutdown signal to RebalanceService, error: {:?}", e);
            }
        } else {
            warn!("Shutdown called but no shutdown channel available");
            return Ok(());
        }

        // Wait for task to exit (polling with timeout)
        let start = Instant::now();
        let timeout = Duration::from_millis(timeout_ms);
        while self.started.load(Ordering::SeqCst) {
            if start.elapsed() >= timeout {
                warn!("RebalanceService shutdown timeout after {}ms", timeout_ms);
                // Force abort task
                if let Some(abort_handle) = &self.task_abort_handle {
                    abort_handle.abort();
                }
                return Err("Shutdown timeout".into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        info!("RebalanceService shutdown successfully");
        Ok(())
    }

    /// Check if the rebalance service is healthy.
    ///
    /// A service is considered healthy if:
    /// - It has been started
    /// - The last successful rebalance was within the specified idle time
    ///
    /// # Arguments
    ///
    /// * `max_idle_ms` - Maximum idle time in milliseconds since last successful rebalance
    ///
    /// # Returns
    ///
    /// `true` if the service is healthy, `false` otherwise
    pub fn is_healthy(&self, max_idle_ms: u64) -> bool {
        if !self.started.load(Ordering::SeqCst) {
            return false;
        }

        let last_success = self.last_success_timestamp.load(Ordering::Relaxed);
        if last_success == 0 {
            // Service started but no successful rebalance yet
            // This is OK if the service just started
            return true;
        }

        let now = get_current_millis();
        now - last_success < max_idle_ms
    }

    /// Get the service name.
    ///
    /// # Returns
    ///
    /// The name of this service: "RebalanceService"
    pub fn get_service_name(&self) -> &'static str {
        "RebalanceService"
    }

    /// Get rebalance metrics.
    ///
    /// # Returns
    ///
    /// A tuple of (total_count, success_count, fail_count, last_success_timestamp)
    pub fn get_metrics(&self) -> (u64, u64, u64, u64) {
        (
            self.total_rebalance_count.load(Ordering::Relaxed),
            self.success_count.load(Ordering::Relaxed),
            self.fail_count.load(Ordering::Relaxed),
            self.last_success_timestamp.load(Ordering::Relaxed),
        )
    }

    /// Check if the service is running.
    ///
    /// # Returns
    ///
    /// `true` if the service is currently running, `false` otherwise
    pub fn is_running(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rebalance_config_default() {
        let config = RebalanceConfig::default();
        assert_eq!(config.wait_interval_ms, 20000);
        assert_eq!(config.min_interval_ms, 1000);
        assert!(config.enable_dynamic_interval);
    }

    #[test]
    fn test_rebalance_config_new() {
        let config = RebalanceConfig::new();
        assert_eq!(config.wait_interval_ms, 20000);
        assert_eq!(config.min_interval_ms, 1000);
    }

    #[test]
    fn test_rebalance_config_custom() {
        let config = RebalanceConfig {
            wait_interval_ms: 10000,
            min_interval_ms: 500,
            enable_dynamic_interval: false,
        };
        assert_eq!(config.wait_interval_ms, 10000);
        assert_eq!(config.min_interval_ms, 500);
        assert!(!config.enable_dynamic_interval);
    }

    #[test]
    fn test_rebalance_config_duration_conversion() {
        let config = RebalanceConfig {
            wait_interval_ms: 5000,
            min_interval_ms: 100,
            enable_dynamic_interval: true,
        };
        assert_eq!(config.wait_interval(), Duration::from_millis(5000));
        assert_eq!(config.min_interval(), Duration::from_millis(100));
    }

    #[test]
    fn test_rebalance_config_validate_success() {
        let config = RebalanceConfig {
            wait_interval_ms: 5000,
            min_interval_ms: 1000,
            enable_dynamic_interval: true,
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_rebalance_config_validate_zero_wait_interval() {
        let config = RebalanceConfig {
            wait_interval_ms: 0,
            min_interval_ms: 1000,
            enable_dynamic_interval: true,
        };
        assert!(config.validate().is_err());
        assert_eq!(
            config.validate().unwrap_err(),
            "wait_interval_ms must be greater than 0"
        );
    }

    #[test]
    fn test_rebalance_config_validate_zero_min_interval() {
        let config = RebalanceConfig {
            wait_interval_ms: 5000,
            min_interval_ms: 0,
            enable_dynamic_interval: true,
        };
        assert!(config.validate().is_err());
        assert_eq!(config.validate().unwrap_err(), "min_interval_ms must be greater than 0");
    }

    #[test]
    fn test_rebalance_config_validate_min_greater_than_wait() {
        let config = RebalanceConfig {
            wait_interval_ms: 1000,
            min_interval_ms: 5000,
            enable_dynamic_interval: true,
        };
        assert!(config.validate().is_err());
        let err = config.validate().unwrap_err();
        assert!(err.contains("min_interval_ms"));
        assert!(err.contains("wait_interval_ms"));
    }

    #[test]
    fn test_rebalance_config_validate_equal_intervals() {
        let config = RebalanceConfig {
            wait_interval_ms: 5000,
            min_interval_ms: 5000,
            enable_dynamic_interval: true,
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_rebalance_service_new() {
        let service = RebalanceService::new();
        assert!(!service.is_running());
        assert_eq!(service.get_service_name(), "RebalanceService");
        assert_eq!(service.config().wait_interval_ms, 20000);
        assert_eq!(service.config().min_interval_ms, 1000);
    }

    #[test]
    fn test_rebalance_service_with_config() {
        let config = RebalanceConfig {
            wait_interval_ms: 10000,
            min_interval_ms: 500,
            enable_dynamic_interval: false,
        };
        let service = RebalanceService::with_config(config.clone());
        assert_eq!(service.config().wait_interval_ms, 10000);
        assert_eq!(service.config().min_interval_ms, 500);
        assert!(!service.config().enable_dynamic_interval);
    }

    #[test]
    #[should_panic(expected = "Invalid RebalanceConfig")]
    fn test_rebalance_service_with_invalid_config() {
        let config = RebalanceConfig {
            wait_interval_ms: 0,
            min_interval_ms: 1000,
            enable_dynamic_interval: true,
        };
        RebalanceService::with_config(config);
    }

    #[test]
    fn test_rebalance_service_initial_state() {
        let service = RebalanceService::new();
        assert!(!service.is_running());

        let (total, success, fail, last_ts) = service.get_metrics();
        assert_eq!(total, 0);
        assert_eq!(success, 0);
        assert_eq!(fail, 0);
        assert_eq!(last_ts, 0);
    }

    #[test]
    fn test_rebalance_service_is_healthy_not_started() {
        let service = RebalanceService::new();
        // Service not started should be unhealthy
        assert!(!service.is_healthy(60000));
    }

    #[test]
    fn test_rebalance_service_config_access() {
        let config = RebalanceConfig {
            wait_interval_ms: 15000,
            min_interval_ms: 750,
            enable_dynamic_interval: true,
        };
        let service = RebalanceService::with_config(config);

        let retrieved_config = service.config();
        assert_eq!(retrieved_config.wait_interval_ms, 15000);
        assert_eq!(retrieved_config.min_interval_ms, 750);
        assert!(retrieved_config.enable_dynamic_interval);
    }

    #[test]
    fn test_rebalance_config_clone() {
        let config1 = RebalanceConfig {
            wait_interval_ms: 8000,
            min_interval_ms: 400,
            enable_dynamic_interval: false,
        };
        let config2 = config1.clone();

        assert_eq!(config1.wait_interval_ms, config2.wait_interval_ms);
        assert_eq!(config1.min_interval_ms, config2.min_interval_ms);
        assert_eq!(config1.enable_dynamic_interval, config2.enable_dynamic_interval);
    }

    #[test]
    fn test_rebalance_config_debug() {
        let config = RebalanceConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("RebalanceConfig"));
        assert!(debug_str.contains("20000"));
        assert!(debug_str.contains("1000"));
    }

    #[test]
    fn test_rebalance_service_get_service_name() {
        let service = RebalanceService::new();
        assert_eq!(service.get_service_name(), "RebalanceService");
    }

    #[test]
    fn test_rebalance_config_from_env_fallback() {
        // When env vars are not set, should use defaults
        std::env::remove_var("rocketmq.client.rebalance.waitInterval");
        std::env::remove_var("rocketmq.client.rebalance.minInterval");

        let config = RebalanceConfig::from_env();
        assert_eq!(config.wait_interval_ms, 20000);
        assert_eq!(config.min_interval_ms, 1000);
    }

    #[test]
    fn test_rebalance_config_edge_cases() {
        // Test with very small intervals
        let config = RebalanceConfig {
            wait_interval_ms: 1,
            min_interval_ms: 1,
            enable_dynamic_interval: true,
        };
        assert!(config.validate().is_ok());

        // Test with very large intervals
        let config = RebalanceConfig {
            wait_interval_ms: u64::MAX - 1,
            min_interval_ms: 1,
            enable_dynamic_interval: true,
        };
        assert!(config.validate().is_ok());
    }
}
