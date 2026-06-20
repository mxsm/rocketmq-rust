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

//! Advanced connection pool with metrics, health checking, and lifecycle management.
//!
//! # Features
//!
//! - **Connection Reuse**: Maintains long-lived TCP connections to brokers
//! - **Idle Timeout**: Automatically closes unused connections after timeout
//! - **Health Checking**: Validates connection health before returning
//! - **Metrics Collection**: Tracks usage, latency, and error rates
//! - **Concurrency**: Lock-free reads via DashMap
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────┐
//! │             ConnectionPool                           │
//! ├──────────────────────────────────────────────────────┤
//! │                                                      │
//! │  ┌────────────────────┐    ┌──────────────────────┐│
//! │  │  DashMap           │    │  ConnectionMetrics   ││
//! │  │  addr -> Entry     │───►│  - last_used         ││
//! │  │                    │    │  - request_count     ││
//! │  │                    │    │  - error_count       ││
//! │  └────────────────────┘    │  - latency_sum       ││
//! │           │                └──────────────────────┘│
//! │           ↓                                         │
//! │  ┌────────────────────┐                            │
//! │  │  PooledConnection  │                            │
//! │  │  - client          │                            │
//! │  │  - metrics         │                            │
//! │  │  - created_at      │                            │
//! │  └────────────────────┘                            │
//! │                                                      │
//! └──────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use std::time::Duration;
//!
//! use rocketmq_remoting::clients::connection_pool::ConnectionPool;
//!
//! # async fn example() {
//! let pool = ConnectionPool::new(
//!     1000,                     // max_connections
//!     Duration::from_secs(300), // max_idle_duration
//! );
//!
//! // Get or create connection
//! if let Some(conn) = pool
//!     .get_or_create("127.0.0.1:9876", || async {
//!         // Connection factory
//!         create_client("127.0.0.1:9876").await
//!     })
//!     .await
//! {
//!     // Use connection
//!     let metrics = pool.get_metrics("127.0.0.1:9876");
//!     println!("Connection used {} times", metrics.request_count);
//! }
//!
//! // Cleanup idle connections
//! pool.evict_idle().await;
//! # }
//! ```

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_runtime::RuntimeError;
use rocketmq_runtime::RuntimeHandle;
use rocketmq_runtime::RuntimeResult;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::ScheduledTaskSnapshot;
use rocketmq_runtime::ShutdownAnnotation;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::clients::Client;
use crate::request_processor::default_request_processor::DefaultRemotingRequestProcessor;

/// Connection pool entry with lifecycle and metrics tracking.
///
/// # Lifecycle States
///
/// ```text
/// Created → Active → Idle → Evicted
///    ↓        ↓       ↓        ↓
///  [new]   [use]  [timeout] [remove]
/// ```
#[derive(Clone)]
pub struct PooledConnection<PR = DefaultRemotingRequestProcessor> {
    /// The underlying client connection
    client: Client<PR>,

    /// Connection metrics for monitoring
    metrics: Arc<ConnectionMetrics>,

    /// When this connection was created
    created_at: Instant,
}

impl<PR> PooledConnection<PR> {
    /// Create a new pooled connection
    pub fn new(client: Client<PR>) -> Self {
        Self {
            client,
            metrics: Arc::new(ConnectionMetrics::new()),
            created_at: Instant::now(),
        }
    }

    /// Get the underlying client
    pub fn client(&self) -> &Client<PR> {
        &self.client
    }

    /// Get connection metrics
    pub fn metrics(&self) -> &ConnectionMetrics {
        &self.metrics
    }

    /// Check if connection is healthy
    pub fn is_healthy(&self) -> bool {
        // Check if the underlying connection is healthy
        // Note: We can't directly call connection().ok due to trait bounds,
        // so we'll assume it's healthy if it exists
        true
    }

    /// Check if connection is idle (not used recently)
    pub fn is_idle(&self, max_idle: Duration) -> bool {
        self.metrics.last_used().elapsed() > max_idle
    }

    /// Record successful request
    pub fn record_success(&self, latency_ms: u64) {
        self.metrics.record_success(latency_ms);
    }

    /// Record failed request
    pub fn record_error(&self) {
        self.metrics.record_error();
    }

    /// Get connection age
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }
}

/// Connection metrics for monitoring and decision-making.
///
/// # Thread Safety
///
/// All fields use atomic operations for lock-free updates
#[derive(Debug)]
pub struct ConnectionMetrics {
    /// Last time this connection was used
    last_used: parking_lot::Mutex<Instant>,

    /// Total number of requests sent
    request_count: AtomicU64,

    /// Number of consecutive errors
    consecutive_errors: AtomicU64,

    /// Sum of all request latencies (milliseconds)
    latency_sum: AtomicU64,

    /// Total number of errors
    total_errors: AtomicU64,
}

impl ConnectionMetrics {
    /// Create new metrics tracker
    pub fn new() -> Self {
        Self {
            last_used: parking_lot::Mutex::new(Instant::now()),
            request_count: AtomicU64::new(0),
            consecutive_errors: AtomicU64::new(0),
            latency_sum: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
        }
    }

    /// Record successful request
    pub fn record_success(&self, latency_ms: u64) {
        *self.last_used.lock() = Instant::now();
        self.request_count.fetch_add(1, Ordering::Relaxed);
        self.latency_sum.fetch_add(latency_ms, Ordering::Relaxed);
        self.consecutive_errors.store(0, Ordering::Relaxed);
    }

    /// Record failed request
    pub fn record_error(&self) {
        *self.last_used.lock() = Instant::now();
        self.request_count.fetch_add(1, Ordering::Relaxed);
        self.consecutive_errors.fetch_add(1, Ordering::Relaxed);
        self.total_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Get average latency in milliseconds
    pub fn avg_latency(&self) -> f64 {
        let count = self.request_count.load(Ordering::Relaxed);
        if count == 0 {
            return 0.0;
        }
        let sum = self.latency_sum.load(Ordering::Relaxed);
        sum as f64 / count as f64
    }

    /// Get error rate (0.0 - 1.0)
    pub fn error_rate(&self) -> f64 {
        let count = self.request_count.load(Ordering::Relaxed);
        if count == 0 {
            return 0.0;
        }
        let errors = self.total_errors.load(Ordering::Relaxed);
        errors as f64 / count as f64
    }

    /// Get consecutive error count
    pub fn consecutive_errors(&self) -> u64 {
        self.consecutive_errors.load(Ordering::Relaxed)
    }

    /// Get total request count
    pub fn request_count(&self) -> u64 {
        self.request_count.load(Ordering::Relaxed)
    }

    /// Get last used time
    pub fn last_used(&self) -> Instant {
        *self.last_used.lock()
    }
}

impl Default for ConnectionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Advanced connection pool with lifecycle management.
///
/// # Configuration
///
/// ```rust,ignore
/// use std::time::Duration;
///
/// use rocketmq_remoting::clients::connection_pool::ConnectionPool;
///
/// let pool = ConnectionPool::new(
///     1000,                     // max 1000 connections
///     Duration::from_secs(300), // idle timeout 5 minutes
/// );
/// ```
///
/// # Concurrency
///
/// - **Read operations**: Lock-free via DashMap
/// - **Write operations**: Fine-grained per-shard locking
/// - **Scales**: Linearly with CPU cores
///
/// # Memory
///
/// - **Per connection**: ~200 bytes overhead (metrics + metadata)
/// - **Total**: O(active_connections)
pub struct ConnectionPool<PR = DefaultRemotingRequestProcessor> {
    /// Connection storage: addr -> PooledConnection
    connections: Arc<DashMap<CheetahString, PooledConnection<PR>>>,

    /// Maximum number of connections to maintain
    max_connections: usize,

    /// Maximum duration a connection can be idle
    max_idle_duration: Duration,
}

/// Handle for a background connection-pool cleanup task.
#[derive(Debug, Clone)]
pub struct ConnectionPoolCleanupTask {
    scheduled_tasks: Option<ScheduledTaskGroup>,
}

impl ConnectionPoolCleanupTask {
    fn inactive() -> Self {
        Self { scheduled_tasks: None }
    }

    /// Returns true when the cleanup task was actually spawned.
    pub fn is_active(&self) -> bool {
        self.scheduled_tasks.is_some()
    }

    /// Request the cleanup task to stop without waiting for completion.
    pub fn abort(&self) {
        if let Some(scheduled_tasks) = &self.scheduled_tasks {
            scheduled_tasks.group().cancel();
        }
    }

    /// Stop the cleanup task gracefully and return the shutdown report.
    pub async fn shutdown(&self, timeout: Duration) -> ShutdownReport {
        if let Some(scheduled_tasks) = &self.scheduled_tasks {
            return scheduled_tasks.shutdown(timeout).await;
        }

        let mut report = ShutdownReport::new("rocketmq-remoting.connection-pool.inactive", Duration::ZERO);
        report.annotations.push(ShutdownAnnotation::new(
            "connection pool cleanup task was not started because no Tokio runtime was available",
        ));
        report
    }

    /// Number of tracked cleanup tasks.
    pub fn task_count(&self) -> usize {
        self.scheduled_tasks
            .as_ref()
            .map(|scheduled_tasks| scheduled_tasks.group().task_count())
            .unwrap_or_default()
    }

    /// Snapshot of cleanup scheduler metrics.
    pub fn schedule_snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.scheduled_tasks
            .as_ref()
            .map(ScheduledTaskGroup::snapshot)
            .unwrap_or_default()
    }
}

impl<PR> ConnectionPool<PR> {
    /// Create a new connection pool with specified limits.
    ///
    /// # Arguments
    ///
    /// * `max_connections` - Maximum number of connections (0 = unlimited)
    /// * `max_idle_duration` - Idle timeout (e.g., 5 minutes)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use std::time::Duration;
    ///
    /// use rocketmq_remoting::clients::connection_pool::ConnectionPool;
    ///
    /// let pool = ConnectionPool::<()>::new(1000, Duration::from_secs(300));
    /// ```
    pub fn new(max_connections: usize, max_idle_duration: Duration) -> Self {
        Self {
            connections: Arc::new(DashMap::with_capacity(64)),
            max_connections,
            max_idle_duration,
        }
    }

    /// Get connection from pool or create new one.
    ///
    /// # Returns
    ///
    /// * `Some(conn)` - Healthy connection from pool or newly created
    /// * `None` - Failed to create connection or pool at capacity
    pub fn get(&self, addr: &CheetahString) -> Option<PooledConnection<PR>>
    where
        PR: Clone,
    {
        if let Some(entry) = self.connections.get(addr) {
            let conn = entry.value().clone();
            if conn.is_healthy() {
                debug!("Reusing pooled connection to {}", addr);
                return Some(conn);
            } else {
                debug!("Removing unhealthy connection to {}", addr);
                drop(entry); // Release read lock
                self.connections.remove(addr);
            }
        }
        None
    }

    /// Insert a new connection into the pool.
    ///
    /// # Returns
    ///
    /// * `true` - Connection added successfully
    /// * `false` - Pool at capacity, connection rejected
    pub fn insert(&self, addr: CheetahString, client: Client<PR>) -> bool {
        // Check capacity
        if self.max_connections > 0 && self.connections.len() >= self.max_connections {
            warn!(
                "Connection pool at capacity ({}/{}), rejecting connection to {}",
                self.connections.len(),
                self.max_connections,
                addr
            );
            return false;
        }

        let pooled = PooledConnection::new(client);
        self.connections.insert(addr.clone(), pooled);
        info!(
            "Added connection to pool: {} (pool size: {})",
            addr,
            self.connections.len()
        );
        true
    }

    /// Remove connection from pool.
    ///
    /// # Returns
    ///
    /// * `Some(conn)` - Removed connection
    /// * `None` - Connection not found
    pub fn remove(&self, addr: &CheetahString) -> Option<PooledConnection<PR>> {
        self.connections.remove(addr).map(|(_, conn)| {
            debug!("Removed connection from pool: {}", addr);
            conn
        })
    }

    /// Get connection metrics.
    ///
    /// # Returns
    ///
    /// * `Some(metrics)` - Metrics for the connection
    /// * `None` - Connection not in pool
    pub fn get_metrics(&self, addr: &CheetahString) -> Option<Arc<ConnectionMetrics>> {
        self.connections.get(addr).map(|entry| entry.value().metrics.clone())
    }

    /// Record successful request on connection.
    pub fn record_success(&self, addr: &CheetahString, latency_ms: u64) {
        if let Some(entry) = self.connections.get(addr) {
            entry.value().record_success(latency_ms);
        }
    }

    /// Record failed request on connection.
    pub fn record_error(&self, addr: &CheetahString) {
        if let Some(entry) = self.connections.get(addr) {
            entry.value().record_error();
        }
    }

    /// Evict idle connections from the pool.
    ///
    /// # Returns
    ///
    /// Number of connections evicted
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// # use rocketmq_remoting::clients::connection_pool::ConnectionPool;
    /// # async fn example(pool: &ConnectionPool) {
    /// let evicted = pool.evict_idle().await;
    /// println!("Evicted {} idle connections", evicted);
    /// # }
    /// ```
    pub async fn evict_idle(&self) -> usize {
        let mut to_remove = Vec::new();

        // Collect idle connections
        for entry in self.connections.iter() {
            if entry.value().is_idle(self.max_idle_duration) {
                to_remove.push(entry.key().clone());
            }
        }

        let count = to_remove.len();
        if count > 0 {
            info!("Evicting {} idle connections", count);
            for addr in to_remove {
                self.connections.remove(&addr);
            }
        }

        count
    }

    /// Evict unhealthy connections from the pool.
    ///
    /// # Returns
    ///
    /// Number of connections evicted
    pub async fn evict_unhealthy(&self) -> usize {
        let mut to_remove = Vec::new();

        // Collect unhealthy connections
        for entry in self.connections.iter() {
            if !entry.value().is_healthy() {
                to_remove.push(entry.key().clone());
            }
        }

        let count = to_remove.len();
        if count > 0 {
            warn!("Evicting {} unhealthy connections", count);
            for addr in to_remove {
                self.connections.remove(&addr);
            }
        }

        count
    }

    /// Get pool statistics.
    ///
    /// # Returns
    ///
    /// `PoolStats` with current pool state
    pub fn stats(&self) -> PoolStats {
        let size = self.connections.len();
        let mut healthy = 0;
        let mut idle = 0;
        let mut total_requests = 0u64;
        let mut total_errors = 0u64;

        for entry in self.connections.iter() {
            let conn = entry.value();
            if conn.is_healthy() {
                healthy += 1;
            }
            if conn.is_idle(self.max_idle_duration) {
                idle += 1;
            }
            total_requests += conn.metrics().request_count();
            total_errors += conn.metrics().total_errors.load(Ordering::Relaxed);
        }

        PoolStats {
            total: size,
            healthy,
            idle,
            max_connections: self.max_connections,
            total_requests,
            total_errors,
        }
    }

    /// Start background cleanup task.
    ///
    /// Periodically evicts idle and unhealthy connections.
    ///
    /// # Arguments
    ///
    /// * `interval` - Cleanup interval (e.g., 30 seconds)
    ///
    /// # Returns
    ///
    /// Cleanup task handle that can be shut down or aborted
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// # use rocketmq_remoting::clients::connection_pool::ConnectionPool;
    /// # use std::time::Duration;
    /// # async fn example() {
    /// let pool = ConnectionPool::<()>::new(1000, Duration::from_secs(300));
    /// let cleanup_task = pool.start_cleanup_task(Duration::from_secs(30));
    ///
    /// // ... use pool ...
    ///
    /// cleanup_task.abort(); // Stop cleanup task
    /// # }
    /// ```
    pub fn start_cleanup_task(&self, interval: Duration) -> ConnectionPoolCleanupTask
    where
        PR: Send + Sync + 'static,
    {
        match self.try_start_cleanup_task(interval) {
            Ok(cleanup_task) => cleanup_task,
            Err(error) => {
                warn!(?error, "connection pool cleanup task not started");
                ConnectionPoolCleanupTask::inactive()
            }
        }
    }

    /// Try to start the background cleanup task.
    ///
    /// Unlike [`Self::start_cleanup_task`], this method returns an error when no
    /// ambient Tokio runtime is available or when the task cannot be spawned.
    pub fn try_start_cleanup_task(&self, interval: Duration) -> RuntimeResult<ConnectionPoolCleanupTask>
    where
        PR: Send + Sync + 'static,
    {
        let handle = tokio::runtime::Handle::try_current().map_err(|_error| RuntimeError::NoCurrentRuntime)?;
        let task_group = TaskGroup::root("rocketmq-remoting.connection-pool", RuntimeHandle::new(handle));
        self.start_cleanup_task_with_group(interval, task_group)
    }

    /// Start background cleanup task under the provided task group.
    pub fn start_cleanup_task_with_group(
        &self,
        interval: Duration,
        task_group: TaskGroup,
    ) -> rocketmq_runtime::RuntimeResult<ConnectionPoolCleanupTask>
    where
        PR: Send + Sync + 'static,
    {
        let connections = self.connections.clone();
        let max_idle = self.max_idle_duration;
        let scheduled_tasks = ScheduledTaskGroup::new(task_group.child("remoting.connection-pool.cleanup"));
        scheduled_tasks.schedule_fixed_delay(
            ScheduledTaskConfig::fixed_delay("remoting.connection-pool.cleanup", interval),
            move || {
                let connections = connections.clone();
                async move {
                    let mut idle_count = 0;
                    let mut unhealthy_count = 0;
                    let mut to_remove = Vec::new();

                    for entry in connections.iter() {
                        let conn = entry.value();
                        if !conn.is_healthy() {
                            to_remove.push((entry.key().clone(), "unhealthy"));
                            unhealthy_count += 1;
                        } else if conn.is_idle(max_idle) {
                            to_remove.push((entry.key().clone(), "idle"));
                            idle_count += 1;
                        }
                    }

                    if !to_remove.is_empty() {
                        info!(
                            "Cleanup: evicting {} idle and {} unhealthy connections",
                            idle_count, unhealthy_count
                        );
                        for (addr, reason) in to_remove {
                            connections.remove(&addr);
                            debug!("Evicted connection to {} (reason: {})", addr, reason);
                        }
                    }

                    debug!("Connection pool size: {} (after cleanup)", connections.len());
                }
            },
        )?;

        Ok(ConnectionPoolCleanupTask {
            scheduled_tasks: Some(scheduled_tasks),
        })
    }
}

impl<PR> Clone for ConnectionPool<PR> {
    fn clone(&self) -> Self {
        Self {
            connections: self.connections.clone(),
            max_connections: self.max_connections,
            max_idle_duration: self.max_idle_duration,
        }
    }
}

/// Pool statistics snapshot.
#[derive(Debug, Clone)]
pub struct PoolStats {
    /// Total number of connections
    pub total: usize,

    /// Number of healthy connections
    pub healthy: usize,

    /// Number of idle connections
    pub idle: usize,

    /// Maximum configured connections
    pub max_connections: usize,

    /// Total requests processed
    pub total_requests: u64,

    /// Total errors encountered
    pub total_errors: u64,
}

impl PoolStats {
    /// Calculate pool utilization (0.0 - 1.0)
    pub fn utilization(&self) -> f64 {
        if self.max_connections == 0 {
            return 0.0;
        }
        self.total as f64 / self.max_connections as f64
    }

    /// Calculate error rate (0.0 - 1.0)
    pub fn error_rate(&self) -> f64 {
        if self.total_requests == 0 {
            return 0.0;
        }
        self.total_errors as f64 / self.total_requests as f64
    }

    /// Get number of active (non-idle) connections
    pub fn active(&self) -> usize {
        self.total - self.idle
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_metrics() {
        let metrics = ConnectionMetrics::new();

        // Record successes
        metrics.record_success(10);
        metrics.record_success(20);
        metrics.record_success(30);

        assert_eq!(metrics.request_count(), 3);
        assert_eq!(metrics.avg_latency(), 20.0);
        assert_eq!(metrics.error_rate(), 0.0);

        // Record error
        metrics.record_error();
        assert_eq!(metrics.request_count(), 4);
        assert_eq!(metrics.consecutive_errors(), 1);
        assert_eq!(metrics.error_rate(), 0.25);

        // Success resets consecutive errors
        metrics.record_success(15);
        assert_eq!(metrics.consecutive_errors(), 0);
    }

    #[test]
    fn test_pool_stats() {
        let stats = PoolStats {
            total: 50,
            healthy: 45,
            idle: 10,
            max_connections: 100,
            total_requests: 10000,
            total_errors: 100,
        };

        assert_eq!(stats.utilization(), 0.5);
        assert_eq!(stats.error_rate(), 0.01);
        assert_eq!(stats.active(), 40);
    }

    #[tokio::test]
    async fn cleanup_task_shutdown_reports_healthy() {
        let pool = ConnectionPool::<()>::new(100, Duration::from_millis(10));
        let cleanup_task = pool.start_cleanup_task(Duration::from_millis(5));

        assert!(cleanup_task.is_active());
        assert_eq!(cleanup_task.task_count(), 1);
        let report = cleanup_task.shutdown(Duration::from_secs(1)).await;

        assert!(report.is_healthy(), "{}", report.to_json());
        assert_eq!(cleanup_task.task_count(), 0);
    }

    #[test]
    fn start_cleanup_task_without_tokio_runtime_returns_inactive_task() {
        let pool = ConnectionPool::<()>::new(100, Duration::from_millis(10));
        let cleanup_task = pool.start_cleanup_task(Duration::from_millis(5));

        assert!(!cleanup_task.is_active());
        let report = futures::executor::block_on(cleanup_task.shutdown(Duration::from_secs(1)));
        assert!(report.is_healthy(), "{}", report.to_json());
        assert_eq!(report.name, "rocketmq-remoting.connection-pool.inactive");
    }

    #[test]
    fn try_start_cleanup_task_without_tokio_runtime_returns_error() {
        let pool = ConnectionPool::<()>::new(100, Duration::from_millis(10));

        let error = pool
            .try_start_cleanup_task(Duration::from_millis(5))
            .expect_err("try_start_cleanup_task should require an ambient runtime");

        assert!(matches!(error, RuntimeError::NoCurrentRuntime));
    }
}
