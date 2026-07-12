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

use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use parking_lot::Mutex;
use rocketmq_runtime::RuntimeHandle;
use rocketmq_runtime::RuntimeResult;
use rocketmq_runtime::ServiceContext;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskGroupLifecycleState;
use rocketmq_runtime::TaskId;
use rocketmq_rust::ArcMut;
use rocketmq_rust::WeakArcMut;
use serde::Serialize;
use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::connection_net_event::ConnectionNetEvent;
use crate::base::pending_request_table::PendingRequestTable;
use crate::clients::connection_pool::ConnectionPool;
use crate::clients::connection_pool::ConnectionPoolCleanupTask;
use crate::clients::nameserver_selector::LatencyTracker;
use crate::clients::reconnect::CircuitBreaker;
use crate::clients::Client;
use crate::clients::RemotingClient;
use crate::protocol::remoting_command::RemotingCommand;
use crate::remoting::inner::RemotingGeneralHandler;
use crate::remoting::RemotingService;
use crate::request_processor::default_request_processor::DefaultRemotingRequestProcessor;
use crate::runtime::config::client_config::TokioClientConfig;
use crate::runtime::processor::RequestProcessor;
use crate::runtime::RPCHook;
use crate::tls::TlsConfig;
use rocketmq_transport::security::TransportSecurity;

/// High-performance async RocketMQ client with connection pooling and auto-reconnection.
///
/// # Architecture
///
/// ```text
/// ┌─────────────────────────────────────────────────────────┐
/// │            RocketmqDefaultClient<PR>                    │
/// ├─────────────────────────────────────────────────────────┤
/// │                                                         │
/// │  ┌────────────────┐      ┌──────────────────┐         │
/// │  │ Connection Pool│ ───► │NameServer Router │         │
/// │  │  (DashMap)     │      │  (Health-based)  │         │
/// │  └────────────────┘      └──────────────────┘         │
/// │         │                         │                    │
/// │         ↓                         ↓                    │
/// │  ┌────────────────┐      ┌──────────────────┐         │
/// │  │ Request Handler│ ───► │  Response Table  │         │
/// │  │  (async tasks) │      │   (oneshot rx)   │         │
/// │  └────────────────┘      └──────────────────┘         │
/// │                                                         │
/// └─────────────────────────────────────────────────────────┘
/// ```
///
/// # Key Features
///
/// - **Connection Pooling**: Reuses TCP connections to brokers/nameservers
/// - **Auto-Reconnection**: Exponential backoff retry on connection failures
/// - **Smart Routing**: Selects healthiest nameserver based on latency/errors
/// - **Request Multiplexing**: Multiple concurrent requests per connection
/// - **Graceful Shutdown**: Drains in-flight requests before closing
///
/// # Performance Characteristics
///
/// - **Concurrency**: Uses `DashMap` for lock-free reads on connection pool
/// - **Memory**: O(N) where N = number of unique broker addresses
/// - **Latency**: Single async hop for cached connections, 2-3 hops for new
///
/// # Type Parameters
///
/// * `PR` - Request processor type (default: `DefaultRemotingRequestProcessor`)
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
///
/// use rocketmq_remoting::clients::RocketmqDefaultClient;
/// use rocketmq_remoting::runtime::config::client_config::TokioClientConfig;
///
/// # async fn example() -> rocketmq_error::RocketMQResult<()> {
/// let config = Arc::new(TokioClientConfig::default());
/// let processor = Default::default();
/// let client = RocketmqDefaultClient::new(config, processor);
///
/// // Update nameserver list
/// client
///     .update_name_server_address_list(vec!["127.0.0.1:9876".into()])
///     .await;
///
/// // Send request
/// let response = client
///     .invoke_request(
///         None, // use default nameserver
///         request, 3000, // 3s timeout
///     )
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct RocketmqDefaultClient<PR = DefaultRemotingRequestProcessor> {
    /// Client configuration (timeouts, buffer sizes, etc.)
    ///
    /// Shared across all connections to avoid duplication
    tokio_client_config: Arc<TokioClientConfig>,

    /// Connection pool: `addr -> Client` mapping
    ///
    /// **Lock-Free Design**: Uses `DashMap` for concurrent access without Mutex
    /// - Read operations (get): Zero-lock overhead
    /// - Write operations (insert/remove): Fine-grained per-shard locking
    /// - Concurrency: Scales linearly with CPU cores (typically 16-64 shards)
    ///
    /// Invariant: Only contains healthy connections (unhealthy removed on error)
    connection_tables: Arc<DashMap<CheetahString /* ip:port */, Client<PR>>>,

    /// List of all nameserver addresses (in priority order)
    ///
    /// Updated via `update_name_server_address_list()`
    namesrv_addr_list: ArcMut<Vec<CheetahString>>,

    /// Currently selected nameserver (cached for fast path)
    ///
    /// May be `None` if no nameserver available or all unhealthy
    namesrv_addr_choosed: ArcMut<Option<CheetahString>>,

    /// Set of healthy/reachable nameservers
    ///
    /// Updated asynchronously by health check task (`scan_available_name_srv`)
    available_namesrv_addr_set: ArcMut<HashSet<CheetahString>>,

    /// Latency tracker for smart nameserver selection
    ///
    /// Tracks P99 latency and error rates to select the best nameserver
    latency_tracker: LatencyTracker,

    /// Circuit breakers per address to prevent cascading failures
    ///
    /// Maps address to circuit breaker state for auto-reconnection
    circuit_breakers: Arc<DashMap<CheetahString, CircuitBreaker>>,

    /// Advanced connection pool with metrics and lifecycle management
    ///
    /// **Optional Feature**: Provides enhanced connection tracking:
    /// - Idle timeout and automatic cleanup
    /// - Per-connection metrics (latency, error rate)
    /// - Pool-level statistics (utilization, health)
    ///
    /// **Usage**: Call `enable_connection_pool()` to activate
    connection_pool: Option<ConnectionPool<PR>>,

    /// Token used to signal graceful shutdown of background tasks.
    ///
    /// Cancelling this token stops the nameserver scan and idle connection
    /// scan loops spawned in [`start()`].
    shutdown_token: CancellationToken,

    /// Task group that owns long-lived background maintenance tasks.
    background_task_group: Arc<Mutex<Option<TaskGroup>>>,

    /// Task group that owns short-lived client worker tasks.
    worker_task_group: Arc<Mutex<Option<TaskGroup>>>,

    /// Optional parent service context for structured client task ownership.
    service_context: Option<ServiceContext>,

    /// Shared command handler (processor + response table)
    ///
    /// Arc-wrapped to share across all `Client` instances
    cmd_handler: ArcMut<RemotingGeneralHandler<PR>>,

    /// Optional connection event broadcaster
    ///
    /// Used for monitoring and metrics collection
    tx: Option<tokio::sync::broadcast::Sender<ConnectionNetEvent>>,

    /// Optional signer applied by each canonical transport session before sending.
    transport_security: Option<Arc<TransportSecurity>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RemotingClientConnectionShutdownReport {
    pub addr: CheetahString,
    pub report: ShutdownReport,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RemotingClientShutdownReport {
    pub background: Option<ShutdownReport>,
    pub workers: Option<ShutdownReport>,
    pub connections: Vec<RemotingClientConnectionShutdownReport>,
}

impl RemotingClientShutdownReport {
    pub fn is_healthy(&self) -> bool {
        self.background.as_ref().is_none_or(ShutdownReport::is_healthy)
            && self.workers.as_ref().is_none_or(ShutdownReport::is_healthy)
            && self.connections.iter().all(|connection| connection.report.is_healthy())
    }
}

impl<PR> Clone for RocketmqDefaultClient<PR> {
    fn clone(&self) -> Self {
        Self {
            tokio_client_config: self.tokio_client_config.clone(),
            connection_tables: self.connection_tables.clone(),
            namesrv_addr_list: self.namesrv_addr_list.clone(),
            namesrv_addr_choosed: self.namesrv_addr_choosed.clone(),
            available_namesrv_addr_set: self.available_namesrv_addr_set.clone(),
            latency_tracker: self.latency_tracker.clone(),
            circuit_breakers: self.circuit_breakers.clone(),
            connection_pool: self.connection_pool.clone(),
            shutdown_token: self.shutdown_token.clone(),
            background_task_group: self.background_task_group.clone(),
            worker_task_group: self.worker_task_group.clone(),
            service_context: self.service_context.clone(),
            cmd_handler: self.cmd_handler.clone(),
            tx: self.tx.clone(),
            transport_security: self.transport_security.clone(),
        }
    }
}

impl<PR: RequestProcessor + Sync + Clone + 'static> RocketmqDefaultClient<PR> {
    pub fn new(tokio_client_config: Arc<TokioClientConfig>, processor: PR) -> Self {
        Self::new_with_cl(tokio_client_config, processor, None)
    }

    pub fn new_with_service_context(
        tokio_client_config: Arc<TokioClientConfig>,
        processor: PR,
        service_context: ServiceContext,
    ) -> Self {
        Self::new_with_cl_and_optional_service_context(tokio_client_config, processor, None, Some(service_context))
    }

    pub fn new_with_cl(
        tokio_client_config: Arc<TokioClientConfig>,
        processor: PR,
        tx: Option<tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
    ) -> Self {
        Self::new_with_cl_and_optional_service_context(tokio_client_config, processor, tx, None)
    }

    pub fn new_with_cl_and_service_context(
        tokio_client_config: Arc<TokioClientConfig>,
        processor: PR,
        tx: Option<tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
        service_context: ServiceContext,
    ) -> Self {
        Self::new_with_cl_and_optional_service_context(tokio_client_config, processor, tx, Some(service_context))
    }

    fn new_with_cl_and_optional_service_context(
        tokio_client_config: Arc<TokioClientConfig>,
        processor: PR,
        tx: Option<tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
        service_context: Option<ServiceContext>,
    ) -> Self {
        let handler = RemotingGeneralHandler {
            request_processor: processor,
            rpc_hooks: vec![],
            response_table: PendingRequestTable::with_capacity(512),
        };
        Self {
            tokio_client_config,
            connection_tables: Arc::new(DashMap::with_capacity(64)),
            namesrv_addr_list: ArcMut::new(Default::default()),
            namesrv_addr_choosed: ArcMut::new(Default::default()),
            available_namesrv_addr_set: ArcMut::new(Default::default()),
            latency_tracker: LatencyTracker::new(),
            circuit_breakers: Arc::new(DashMap::with_capacity(64)),
            connection_pool: None,
            shutdown_token: CancellationToken::new(),
            background_task_group: Arc::new(Mutex::new(None)),
            worker_task_group: Arc::new(Mutex::new(None)),
            service_context,
            cmd_handler: ArcMut::new(handler),
            tx,
            transport_security: None,
        }
    }

    /// Installs an optional transport signer for newly created outbound sessions.
    pub fn with_transport_security(mut self, transport_security: Arc<TransportSecurity>) -> Self {
        self.transport_security = Some(transport_security);
        self
    }

    /// Returns whether newly created outbound connections use TLS.
    #[inline]
    pub fn is_use_tls(&self) -> bool {
        self.tokio_client_config.use_tls
    }

    /// Returns the TLS configuration used when creating new outbound connections.
    #[inline]
    pub fn tls_config(&self) -> &TlsConfig {
        &self.tokio_client_config.tls_config
    }

    pub fn update_name_server_address_list_sync(&self, addrs: Vec<CheetahString>) {
        if addrs.is_empty() {
            return;
        }

        let mut update = false;

        {
            let current: &Vec<CheetahString> = &self.namesrv_addr_list;
            if current.is_empty() || addrs.len() != current.len() {
                update = true;
            } else {
                for addr in &addrs {
                    if !current.contains(addr) {
                        update = true;
                        break;
                    }
                }
            }
        }

        if !update {
            return;
        }

        info!(
            "name server address updated. NEW : {:?} , OLD: {:?}",
            addrs,
            self.namesrv_addr_list.as_ref() as &Vec<CheetahString>
        );

        use rand::seq::SliceRandom;
        let mut shuffled = addrs.clone();
        shuffled.shuffle(&mut rand::rng());

        let list = self.namesrv_addr_list.mut_from_ref();
        list.clear();
        list.extend(shuffled);

        let stale_addr = self.namesrv_addr_choosed.as_ref().clone();
        if let Some(namesrv_addr) = stale_addr {
            if !addrs.contains(&namesrv_addr) {
                self.namesrv_addr_choosed.mut_from_ref().take();

                self.connection_tables.remove(&namesrv_addr);
            }
        }
    }

    fn get_or_create_worker_task_group(&self) -> Option<TaskGroup> {
        if self.shutdown_token.is_cancelled() {
            return None;
        }

        let mut task_group_guard = self.worker_task_group.lock();
        if let Some(task_group) = task_group_guard.as_ref() {
            if task_group.lifecycle_state() == TaskGroupLifecycleState::Open {
                return Some(task_group.clone());
            }
        }

        let task_group = if let Some(service_context) = self.service_context.as_ref() {
            service_context.task_group().child("rocketmq-remoting.client.workers")
        } else {
            let runtime = match tokio::runtime::Handle::try_current() {
                Ok(handle) => RuntimeHandle::new(handle),
                Err(error) => {
                    error!(
                        ?error,
                        "failed to create RemotingClient worker task group outside Tokio runtime"
                    );
                    return None;
                }
            };
            TaskGroup::root("rocketmq-remoting.client.workers", runtime)
        };
        *task_group_guard = Some(task_group.clone());
        Some(task_group)
    }

    pub(crate) fn worker_task_group(&self) -> Option<TaskGroup> {
        self.worker_task_group.lock().as_ref().cloned()
    }

    pub(crate) fn spawn_worker_task<F>(&self, name: impl Into<Arc<str>>, future: F) -> Option<TaskId>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let name = name.into();
        let task_group = self.get_or_create_worker_task_group()?;
        match task_group.spawn_service(name.clone(), future) {
            Ok(task_id) => Some(task_id),
            Err(error) => {
                warn!(
                    ?error,
                    task = %name,
                    "failed to spawn RemotingClient worker task"
                );
                None
            }
        }
    }
}

impl<PR: RequestProcessor + Sync + Clone + 'static> RocketmqDefaultClient<PR> {
    /// Enable advanced connection pool with metrics and automatic cleanup.
    ///
    /// # Arguments
    ///
    /// * `max_connections` - Maximum number of connections (0 = unlimited)
    /// * `max_idle_duration` - Idle timeout (e.g., 5 minutes)
    /// * `cleanup_interval` - Cleanup task interval (e.g., 30 seconds)
    ///
    /// # Returns
    ///
    /// Cleanup task handle for the background task (can be aborted)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// # use rocketmq_remoting::clients::RocketmqDefaultClient;
    /// # use rocketmq_remoting::runtime::config::client_config::TokioClientConfig;
    /// # use std::sync::Arc;
    /// # use std::time::Duration;
    /// # async fn example() {
    /// let client =
    ///     RocketmqDefaultClient::new(Arc::new(TokioClientConfig::default()), Default::default());
    ///
    /// // Enable connection pool with:
    /// // - Max 1000 connections
    /// // - 5 minute idle timeout
    /// // - 30 second cleanup interval
    /// let cleanup_task =
    ///     client.enable_connection_pool(1000, Duration::from_secs(300), Duration::from_secs(30));
    ///
    /// // ... use client ...
    ///
    /// // Stop cleanup when shutting down
    /// cleanup_task.abort();
    /// # }
    /// ```
    pub fn enable_connection_pool(
        &mut self,
        max_connections: usize,
        max_idle_duration: Duration,
        cleanup_interval: Duration,
    ) -> ConnectionPoolCleanupTask {
        let pool = ConnectionPool::new(max_connections, max_idle_duration);
        let cleanup_task = match self.service_context.as_ref() {
            Some(service_context) => pool.start_cleanup_task_with_service_context(service_context, cleanup_interval),
            None => pool.start_cleanup_task(cleanup_interval),
        };
        self.connection_pool = Some(pool);
        info!(
            "Connection pool enabled: max={}, idle_timeout={:?}, cleanup_interval={:?}",
            max_connections, max_idle_duration, cleanup_interval
        );
        cleanup_task
    }

    /// Enable advanced connection pool and return an error if cleanup cannot be spawned.
    pub fn try_enable_connection_pool(
        &mut self,
        max_connections: usize,
        max_idle_duration: Duration,
        cleanup_interval: Duration,
    ) -> RuntimeResult<ConnectionPoolCleanupTask> {
        let pool = ConnectionPool::new(max_connections, max_idle_duration);
        let cleanup_task = match self.service_context.as_ref() {
            Some(service_context) => {
                pool.try_start_cleanup_task_with_service_context(service_context, cleanup_interval)?
            }
            None => pool.try_start_cleanup_task(cleanup_interval)?,
        };
        self.connection_pool = Some(pool);
        info!(
            "Connection pool enabled: max={}, idle_timeout={:?}, cleanup_interval={:?}",
            max_connections, max_idle_duration, cleanup_interval
        );
        Ok(cleanup_task)
    }

    /// Get connection pool statistics (if enabled).
    ///
    /// # Returns
    ///
    /// * `Some(stats)` - Pool statistics
    /// * `None` - Connection pool not enabled
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// # use rocketmq_remoting::clients::RocketmqDefaultClient;
    /// # fn example(client: &RocketmqDefaultClient) {
    /// if let Some(stats) = client.get_pool_stats() {
    ///     println!(
    ///         "Pool: {}/{} connections ({:.1}% util)",
    ///         stats.active(),
    ///         stats.max_connections,
    ///         stats.utilization() * 100.0
    ///     );
    ///     println!("Error rate: {:.2}%", stats.error_rate() * 100.0);
    /// }
    /// # }
    /// ```
    pub fn get_pool_stats(&self) -> Option<crate::clients::connection_pool::PoolStats> {
        self.connection_pool.as_ref().map(|pool| pool.stats())
    }

    /// Get or create connection to a healthy nameserver using smart latency-based selection.
    ///
    /// # Selection Strategy
    ///
    /// **Latency-based**: Selects lowest P99 latency nameserver
    /// ```text
    /// namesrv_list = [ns1, ns2, ns3]
    /// Metrics:
    ///   ns1: P99=5ms,  errors=0
    ///   ns2: P99=50ms, errors=0
    ///   ns3: P99=10ms, errors=3 (unhealthy)
    ///
    /// Selection: ns1 (lowest latency + healthy)
    /// ```
    ///
    /// **Scoring Formula**:
    /// ```text
    /// score = P99_latency_ms + (consecutive_errors × 100)
    /// ```
    ///
    /// **Fallback**: If no metrics available, uses first nameserver
    ///
    /// # Performance Notes
    ///
    /// - **Lock Minimization**: Drops lock before expensive `create_client()`
    /// - **Smart Selection**: O(N) where N = nameserver count (typically <10)
    /// - **Caching**: Reuses `namesrv_addr_choosed` for fast path
    ///
    /// # Returns
    ///
    /// * `Some(client)` - Connected to healthy nameserver
    /// * `None` - No nameservers available or all unhealthy
    async fn get_and_create_nameserver_client(&self) -> Option<Client<PR>> {
        let cached_addr = self.namesrv_addr_choosed.as_ref().clone();

        if let Some(ref addr) = cached_addr {
            // Quick lookup in connection pool (lock-free with DashMap)
            if let Some(client) = self.connection_tables.get(addr) {
                if client.connection().is_healthy() && self.latency_tracker.is_healthy(addr) {
                    // Fast path: Cached nameserver is healthy
                    return Some(client.value().clone());
                }
                debug!("Cached nameserver {} is unhealthy, selecting new one", addr);
            }
        }

        let addr_list = self.namesrv_addr_list.as_ref();

        if addr_list.is_empty() {
            warn!("No nameservers configured in namesrv_addr_list");
            return None;
        }

        // Use latency tracker to select best nameserver
        let selected_addr = match self.latency_tracker.select_best(addr_list) {
            Some(addr) => addr,
            None => {
                error!(
                    "Failed to select healthy nameserver. Available list: {:?}, Available set: {:?}",
                    addr_list,
                    self.available_namesrv_addr_set.as_ref()
                );
                return None;
            }
        };

        info!(
            "Selected nameserver: {} (P99: {:?}, errors: {})",
            selected_addr,
            self.latency_tracker
                .get_p99(selected_addr)
                .unwrap_or(Duration::from_secs(0)),
            self.latency_tracker.get_error_count(selected_addr)
        );

        // Update cached selection
        self.namesrv_addr_choosed.mut_from_ref().replace(selected_addr.clone());

        self.create_client(
            selected_addr,
            Duration::from_millis(self.tokio_client_config.connect_timeout_millis as u64),
        )
        .await
    }

    /// Get existing healthy client or create new connection.
    ///
    /// # Flow
    /// 1. If `addr` is `None` or empty, route to nameserver
    /// 2. Check connection pool for existing client
    /// 3. Verify client health (connection.is_healthy() == true)
    /// 4. If unhealthy or missing, create new connection
    ///
    /// # Performance
    /// - **Fast path**: Single lock acquire + HashMap lookup + health check (< 100ns)
    /// - **Slow path**: Lock + TCP handshake + TLS (if enabled) (10-50ms)
    async fn get_and_create_client(&self, addr: Option<&CheetahString>) -> Option<Client<PR>> {
        // Route empty addresses to nameserver
        let target_addr = match addr {
            None => return self.get_and_create_nameserver_client().await,
            Some(addr) if addr.is_empty() => return self.get_and_create_nameserver_client().await,
            Some(addr) => addr,
        };

        // Fast path: Check connection pool (lock-free with DashMap)
        if let Some(client_ref) = self.connection_tables.get(target_addr) {
            let client = client_ref.value().clone();
            if client.connection().is_healthy() {
                return Some(client); // Return healthy cached client
            }
            // Client unhealthy - will create new connection
            debug!("Cached client for {} is unhealthy, reconnecting...", target_addr);
        }

        // Slow path: Create new connection
        self.create_client(
            target_addr,
            Duration::from_millis(self.tokio_client_config.connect_timeout_millis as u64),
        )
        .await
    }

    /// Create new client connection with double-checked locking pattern.
    ///
    /// # Concurrency Strategy
    ///
    /// Uses **double-checked locking** to prevent thundering herd:
    /// 1. **Check 1**: Quick lookup before TCP connect (avoids redundant connects)
    /// 2. **Release lock**: Perform TCP connect WITHOUT holding lock
    /// 3. **Check 2**: Re-acquire lock and verify no other task created connection
    /// 4. **Insert**: Store new client in pool
    ///
    /// # Performance
    ///
    /// **Before (holding lock during connect)**:
    /// ```text
    /// Thread 1: [====== LOCK ======][==== CONNECT (50ms) ====][==== INSERT ====]
    /// Thread 2:                      [waiting.....................][LOCK]
    /// Thread 3:                      [waiting.....................][LOCK]
    /// Total: ~50ms * 3 = 150ms wasted
    /// ```
    ///
    /// **After (lock-free connect)**:
    /// ```text
    /// Thread 1: [== LOCK ==][RELEASE]→[CONNECT 50ms]→[LOCK][INSERT]
    /// Thread 2: [== LOCK ==][RELEASE]→[CONNECT 50ms]→[LOCK][cached!]
    /// Thread 3: [== LOCK ==][RELEASE]→[CONNECT 50ms]→[LOCK][cached!]
    /// Total: ~50ms + small lock overhead
    /// ```
    ///
    /// # Arguments
    ///
    /// * `addr` - Target address (e.g., "127.0.0.1:10911")
    /// * `duration` - Connection timeout
    ///
    /// # Returns
    ///
    /// * `Some(client)` - Successfully connected (either new or cached)
    /// * `None` - Connection failed or timed out (or circuit breaker OPEN)
    async fn create_client(&self, addr: &CheetahString, duration: Duration) -> Option<Client<PR>> {
        if let Some(ref pool) = self.connection_pool {
            if let Some(pooled_conn) = pool.get(addr) {
                if pooled_conn.is_healthy() {
                    debug!("Reusing pooled connection to {}", addr);
                    return Some(pooled_conn.client().clone());
                }
                // Unhealthy connection - remove from pool
                pool.remove(addr);
            }
        }

        // Check if healthy client already exists
        if let Some(client_ref) = self.connection_tables.get(addr) {
            let client = client_ref.value().clone();
            if client.connection().is_healthy() {
                return Some(client);
            }
            // Client unhealthy - remove it immediately (DashMap allows concurrent removal)
            drop(client_ref); // Release read guard before removal
            self.connection_tables.remove(addr);
        }

        // Check circuit breaker for this address
        let mut breaker = self
            .circuit_breakers
            .entry(addr.clone())
            .or_insert_with(CircuitBreaker::default_breaker)
            .clone();

        // Check if request allowed (CLOSED or HALF_OPEN)
        if !breaker.allow_request() {
            warn!("Circuit breaker OPEN for {}, rejecting connection attempt", addr);
            return None;
        }

        let addr_inner = addr.to_string();
        let mut tls_config = self.tokio_client_config.tls_config.clone();
        tls_config.enable = self.tokio_client_config.use_tls;

        let service_context = self.service_context.clone();
        let transport_security = self.transport_security.clone();
        let connect_result = time::timeout(duration, async move {
            let result = if let Some(service_context) = service_context.as_ref() {
                Client::connect_with_service_context(
                    service_context,
                    addr_inner,
                    self.cmd_handler.clone(),
                    self.tx.as_ref(),
                    tls_config,
                )
                .await
            } else {
                Client::connect(addr_inner, self.cmd_handler.clone(), self.tx.as_ref(), tls_config).await
            };
            match transport_security {
                Some(transport_security) => result.map(|client| client.with_transport_security(transport_security)),
                None => result,
            }
        })
        .await;

        match connect_result {
            Ok(Ok(new_client)) => {
                // Connection successful - record success in circuit breaker
                breaker.record_success();
                self.circuit_breakers.insert(addr.clone(), breaker);

                if let Some(ref pool) = self.connection_pool {
                    if pool.insert(addr.clone(), new_client.clone()) {
                        info!("Added connection to pool: {} (pool size: {})", addr, pool.stats().total);
                    } else {
                        warn!("Connection pool at capacity, falling back to DashMap");
                    }
                }

                match self.connection_tables.entry(addr.clone()) {
                    dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                        // Check if existing is still healthy
                        if entry.get().connection().is_healthy() {
                            info!("Race condition: {} already connected by another task", addr);
                            return Some(entry.get().clone());
                        }
                        // Replace unhealthy with new client
                        entry.insert(new_client.clone());
                    }
                    dashmap::mapref::entry::Entry::Vacant(entry) => {
                        entry.insert(new_client.clone());
                    }
                }

                info!("Successfully created client for {}", addr);
                Some(new_client)
            }
            Ok(Err(e)) => {
                // Connection failed - record failure in circuit breaker
                error!("Failed to connect to {}: {:?}", addr, e);
                breaker.record_failure();
                self.circuit_breakers.insert(addr.clone(), breaker);
                None
            }
            Err(_) => {
                // Timeout - record failure in circuit breaker
                error!("Connection to {} timed out after {:?}", addr, duration);
                breaker.record_failure();
                self.circuit_breakers.insert(addr.clone(), breaker);
                None
            }
        }
    }

    /// Creates a client with automatic retry using exponential backoff.
    ///
    /// # Arguments
    ///
    /// * `addr` - Target address
    /// * `duration` - Connection timeout per attempt
    /// * `max_attempts` - Maximum retry attempts (0 = use circuit breaker only)
    ///
    /// # Returns
    ///
    /// * `Some(client)` - Successfully connected (possibly after retries)
    /// * `None` - All attempts failed or circuit breaker blocked
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Retry up to 3 times with exponential backoff (1s, 2s, 4s)
    /// let client = self.create_client_with_retry(addr, Duration::from_secs(5), 3).await;
    /// ```
    async fn create_client_with_retry(
        &self,
        addr: &CheetahString,
        duration: Duration,
        max_attempts: u32,
    ) -> Option<Client<PR>> {
        use crate::clients::reconnect::ExponentialBackoff;

        let mut backoff = ExponentialBackoff::new(
            Duration::from_secs(1),  // Initial delay
            Duration::from_secs(10), // Max delay
            max_attempts,
        );

        loop {
            // Try to create client
            if let Some(client) = self.create_client(addr, duration).await {
                return Some(client);
            }

            // Check if should retry
            if let Some(delay) = backoff.next_delay() {
                debug!(
                    "Connection to {} failed, retrying in {:?} (attempt {}/{})",
                    addr,
                    delay,
                    backoff.current_attempt(),
                    max_attempts
                );
                time::sleep(delay).await;
            } else {
                warn!(
                    "Connection to {} failed after {} attempts",
                    addr,
                    backoff.current_attempt()
                );
                return None;
            }
        }
    }

    /// Background task: Continuously scan nameservers to update availability set.
    ///
    /// # Purpose
    ///
    /// Maintains `available_namesrv_addr_set` by probing all configured nameservers
    /// and marking them as available/unavailable based on connection health.
    ///
    /// # Algorithm
    ///
    /// ```text
    /// 1. Cleanup phase: Remove stale entries not in namesrv_addr_list
    /// 2. Probe phase: Test connection to each nameserver
    /// 3. Update phase: Add/remove from available_namesrv_addr_set
    /// ```
    ///
    /// # Performance
    ///
    /// - **Frequency**: Called every `connect_timeout_millis` (typically 3s)
    /// - **Concurrency**: Parallel probes via `futures::future::join_all`
    /// - **Overhead**: O(N) where N = number of nameservers (typically < 10)
    ///
    /// # Example Timeline
    ///
    /// ```text
    /// T+0s:  Start scan
    /// T+0s:  Cleanup: Remove ["old-ns:9876"]
    /// T+0s:  Probe ns1 → Success (mark available)
    /// T+50ms: Probe ns2 → Timeout (mark unavailable)
    /// T+100ms: Probe ns3 → Success (mark available)
    /// T+100ms: Scan complete
    /// T+3s:  Next scan begins...
    /// ```
    async fn scan_available_name_srv(&self) {
        let addr_list = self.namesrv_addr_list.as_ref();

        if addr_list.is_empty() {
            debug!("No nameservers configured, skipping availability scan");
            return;
        }

        // Collect addresses to remove (avoid holding borrow during mutation)
        let stale_addrs: Vec<CheetahString> = self
            .available_namesrv_addr_set
            .as_ref()
            .iter()
            .filter(|addr| !addr_list.contains(addr))
            .cloned()
            .collect();

        for stale_addr in stale_addrs {
            warn!("Removing stale nameserver from available set: {}", stale_addr);
            self.available_namesrv_addr_set.mut_from_ref().remove(&stale_addr);
        }

        // Parallel probing reduces total scan time
        use futures::future::join_all;

        let probe_futures: Vec<_> = addr_list
            .iter()
            .map(|addr| {
                let addr_clone = addr.clone();
                async move {
                    let result = self.get_and_create_client(Some(&addr_clone)).await;
                    (addr_clone, result.is_some())
                }
            })
            .collect();

        // Execute all probes concurrently
        let results = join_all(probe_futures).await;

        // Update availability set based on probe results
        for (namesrv_addr, is_available) in results {
            if is_available {
                // Connection successful - mark as available
                if self
                    .available_namesrv_addr_set
                    .mut_from_ref()
                    .insert(namesrv_addr.clone())
                {
                    info!("Nameserver {} is now available", namesrv_addr);
                }
            } else {
                // Connection failed - mark as unavailable
                if self.available_namesrv_addr_set.mut_from_ref().remove(&namesrv_addr) {
                    warn!("Nameserver {} is now unavailable", namesrv_addr);
                }
            }
        }

        debug!(
            "Availability scan complete: {}/{} nameservers available",
            self.available_namesrv_addr_set.as_ref().len(),
            addr_list.len()
        );
    }

    /// Scans connections and removes those that are unhealthy or idle.
    ///
    /// Unhealthy connections (state != `Healthy`) are removed unconditionally.
    /// When the connection pool is enabled, idle connections (exceeding
    /// `channel_not_active_interval` since last use) are also evicted.
    fn scan_idle_connections(&self) {
        let interval_ms = self.tokio_client_config.channel_not_active_interval;
        if interval_ms <= 0 {
            return;
        }

        let idle_threshold = Duration::from_millis(interval_ms as u64);
        let mut stale_addrs = Vec::new();

        for entry in self.connection_tables.iter() {
            let addr = entry.key().clone();
            let client = entry.value();

            // Remove connections that are no longer healthy
            if !client.connection().is_healthy() {
                stale_addrs.push(addr);
                continue;
            }

            // Remove connections idle beyond the threshold (pool metrics required)
            if let Some(ref pool) = self.connection_pool {
                if let Some(pooled) = pool.get(&addr) {
                    if pooled.is_idle(idle_threshold) {
                        stale_addrs.push(addr);
                    }
                }
            }
        }

        for addr in &stale_addrs {
            if self.connection_tables.remove(addr).is_some() {
                warn!("[SCAN] Removed idle/unhealthy connection: {}", addr);

                if let Some(ref pool) = self.connection_pool {
                    pool.remove(addr);
                }
            }
        }
    }

    pub async fn shutdown_with_report(&mut self, timeout: Duration) -> RemotingClientShutdownReport {
        let deadline = ShutdownDeadline::after(timeout);
        self.shutdown_token.cancel();

        let background_task_group = { self.background_task_group.lock().take() };
        let worker_task_group = { self.worker_task_group.lock().take() };

        let background = match background_task_group {
            Some(task_group) => Some(task_group.shutdown_until(deadline).await),
            None => None,
        };
        let workers = match worker_task_group {
            Some(task_group) => Some(task_group.shutdown_until(deadline).await),
            None => None,
        };

        if let Some(pool) = self.connection_pool.take() {
            pool.clear();
        }

        let addrs: Vec<_> = self.connection_tables.iter().map(|entry| entry.key().clone()).collect();
        let mut clients = Vec::with_capacity(addrs.len());
        for addr in addrs {
            if let Some((addr, client)) = self.connection_tables.remove(&addr) {
                clients.push((addr, client));
            }
        }

        let mut connections = Vec::with_capacity(clients.len());
        for (addr, client) in clients {
            let report = client.close_with_report(deadline.remaining()).await;
            connections.push(RemotingClientConnectionShutdownReport { addr, report });
        }

        self.namesrv_addr_list.clear();
        self.available_namesrv_addr_set.clear();

        RemotingClientShutdownReport {
            background,
            workers,
            connections,
        }
    }
}

#[allow(unused_variables)]
impl<PR: RequestProcessor + Sync + Clone + 'static> RemotingService for RocketmqDefaultClient<PR> {
    async fn start(&self, this: WeakArcMut<Self>) {
        if let Some(client) = this.upgrade() {
            let task_group = {
                let mut task_group_guard = self.background_task_group.lock();
                if let Some(task_group) = task_group_guard.as_ref() {
                    if task_group.lifecycle_state() == TaskGroupLifecycleState::Open {
                        debug!("RemotingClient background tasks are already running");
                        return;
                    }
                }

                let task_group = if let Some(service_context) = self.service_context.as_ref() {
                    service_context.task_group().child("rocketmq-remoting.client")
                } else {
                    let runtime = match tokio::runtime::Handle::try_current() {
                        Ok(handle) => RuntimeHandle::new(handle),
                        Err(error) => {
                            error!(
                                ?error,
                                "failed to start RemotingClient background tasks outside Tokio runtime"
                            );
                            return;
                        }
                    };
                    TaskGroup::root("rocketmq-remoting.client", runtime)
                };
                *task_group_guard = Some(task_group.clone());
                task_group
            };

            let connect_timeout_millis = self.tokio_client_config.connect_timeout_millis as u64;
            let token = self.shutdown_token.clone();

            let client_for_scan = client.clone();
            let scan_token = token.clone();
            if let Err(error) = task_group.spawn_service("remoting.client.namesrv-scan", async move {
                loop {
                    tokio::select! {
                        () = scan_token.cancelled() => break,
                        () = async {
                            client_for_scan.scan_available_name_srv().await;
                            time::sleep(Duration::from_millis(connect_timeout_millis)).await;
                        } => {}
                    }
                }
            }) {
                error!(?error, "failed to spawn RemotingClient nameserver scan task");
            }

            let channel_not_active_interval = self.tokio_client_config.channel_not_active_interval as u64;
            if channel_not_active_interval > 0 {
                let idle_token = token.clone();
                if let Err(error) = task_group.spawn_service("remoting.client.idle-scan", async move {
                    loop {
                        tokio::select! {
                            () = idle_token.cancelled() => break,
                            () = time::sleep(Duration::from_millis(channel_not_active_interval)) => {
                                client.scan_idle_connections();
                            }
                        }
                    }
                }) {
                    error!(?error, "failed to spawn RemotingClient idle connection scan task");
                }
            }
        }
    }

    fn shutdown(&mut self) {
        self.shutdown_token.cancel();
        if let Some(task_group) = self.background_task_group.lock().take() {
            let report = task_group.shutdown_now();
            if !report.is_healthy() {
                warn!(
                    report = %report.to_json(),
                    "RemotingClient background task shutdown report is unhealthy"
                );
            }
        }
        if let Some(task_group) = self.worker_task_group.lock().take() {
            let report = task_group.shutdown_now();
            if !report.is_healthy() {
                warn!(
                    report = %report.to_json(),
                    "RemotingClient worker task shutdown report is unhealthy"
                );
            }
        }
        if let Some(pool) = self.connection_pool.take() {
            pool.clear();
        }
        self.connection_tables.clear();
        self.namesrv_addr_list.clear();
        self.available_namesrv_addr_set.clear();

        info!("RemotingClient shutdown complete");
    }

    fn register_rpc_hook(&mut self, hook: Arc<dyn RPCHook>) {
        self.cmd_handler.register_rpc_hook(hook);
    }

    fn clear_rpc_hook(&mut self) {
        self.cmd_handler.clear_rpc_hook();
    }
}

#[allow(unused_variables)]
impl<PR: RequestProcessor + Sync + Clone + 'static> RemotingClient for RocketmqDefaultClient<PR> {
    async fn update_name_server_address_list(&self, addrs: Vec<CheetahString>) {
        self.update_name_server_address_list_sync(addrs);
    }

    fn get_name_server_address_list(&self) -> &[CheetahString] {
        self.namesrv_addr_list.as_ref()
    }

    fn get_available_name_srv_list(&self) -> Vec<CheetahString> {
        self.available_namesrv_addr_set.as_ref().clone().into_iter().collect()
    }

    /// Send request and wait for response with timeout.
    ///
    /// # Flow
    /// ```text
    /// 1. Get/create client connection         (~100ns fast path, ~50ms slow)
    /// 2. Send request with timeout            (network RTT + processing)
    /// 3. Record latency / error metrics       (~10ns)
    /// ```
    ///
    /// # Error Handling
    ///
    /// Returns `RocketMQError` for all failures:
    /// - Client unavailable (no connection)
    /// - Network I/O error (send/recv failure)
    /// - Timeout (no response within deadline)
    ///
    /// # Arguments
    ///
    /// * `addr` - Target address (None = use nameserver)
    /// * `request` - Command to send
    /// * `timeout_millis` - Max wait time for response
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// # use rocketmq_remoting::clients::RocketmqDefaultClient;
    /// # use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
    /// # async fn example(client: &RocketmqDefaultClient) -> rocketmq_error::RocketMQResult<()> {
    /// let request = RemotingCommand::create_request_command(/* ... */);
    /// let response = client.invoke_request(
    ///     Some(&"127.0.0.1:10911".into()),
    ///     request,
    ///     3000 // 3 second timeout
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn invoke_request(
        &self,
        addr: Option<&CheetahString>,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        // Record start time for latency tracking
        let start = time::Instant::now();

        // Determine target address (for metrics recording)
        let target_addr = addr.cloned().or_else(|| self.namesrv_addr_choosed.as_ref().clone());

        let mut client = self.get_and_create_client(addr).await.ok_or_else(|| {
            let target = addr.map(|a| a.as_str()).unwrap_or("<nameserver>");

            if target == "<nameserver>" {
                error!(
                    "Failed to get client for <nameserver>. Diagnostics: configured_list={:?}, available_set={:?}, \
                     cached_choice={:?}, connections={}",
                    self.namesrv_addr_list.as_ref(),
                    self.available_namesrv_addr_set.as_ref(),
                    self.namesrv_addr_choosed.as_ref(),
                    self.connection_tables.len()
                );
            } else {
                error!("Failed to get client for {}", target);
            }

            // Record connection error
            if let Some(ref addr) = target_addr {
                self.latency_tracker.record_error(addr);
            }

            rocketmq_error::RocketMQError::network_connection_failed(target.to_string(), "Failed to connect")
        })?;

        if self.shutdown_token.is_cancelled() {
            return Err(rocketmq_error::RocketMQError::ClientNotStarted);
        }

        let mut request = request;
        let remote_address = client.remote_address();
        let request_for_after = if self.cmd_handler.has_rpc_hooks() {
            request.make_custom_header_to_net();
            self.cmd_handler
                .do_before_rpc_hooks_with_addr(remote_address, Some(&mut request))?;
            Some(request.clone())
        } else {
            None
        };

        let send_result = time::timeout(
            Duration::from_millis(timeout_millis),
            client.send_read(request, timeout_millis),
        )
        .await;

        let latency = start.elapsed();

        match send_result {
            Ok(Ok(mut response)) => {
                if let Some(request) = request_for_after.as_ref() {
                    self.cmd_handler
                        .do_after_rpc_hooks_with_addr(remote_address, request, Some(&mut response))?;
                }

                if let Some(ref addr) = target_addr {
                    let latency_ms = latency.as_millis() as u64;
                    self.latency_tracker.record_success(addr, latency);

                    if let Some(ref pool) = self.connection_pool {
                        pool.record_success(addr, latency_ms);
                    }

                    debug!(
                        remote_addr = %addr,
                        elapsed_ms = latency.as_millis() as u64,
                        "request completed"
                    );
                }
                Ok(response)
            }
            Ok(Err(err)) => {
                if matches!(err, rocketmq_error::RocketMQError::Timeout { .. }) {
                    client.retire_after_timeout();
                    if let Some(ref addr) = target_addr {
                        self.connection_tables.remove(addr);
                        if let Some(ref pool) = self.connection_pool {
                            pool.remove(addr);
                        }
                    }
                }
                if let Some(ref addr) = target_addr {
                    self.latency_tracker.record_error(addr);

                    if let Some(ref pool) = self.connection_pool {
                        pool.record_error(addr);
                    }

                    warn!(
                        remote_addr = %addr,
                        elapsed_ms = latency.as_millis() as u64,
                        error = ?err,
                        "request failed"
                    );
                }
                Err(err)
            }
            Err(_) => {
                client.retire_after_timeout();
                if let Some(ref addr) = target_addr {
                    self.connection_tables.remove(addr);
                    if let Some(ref pool) = self.connection_pool {
                        pool.remove(addr);
                    }
                    self.latency_tracker.record_error(addr);

                    if let Some(ref pool) = self.connection_pool {
                        pool.record_error(addr);
                    }
                }
                Err(rocketmq_error::RocketMQError::Timeout {
                    operation: "send_request",
                    timeout_ms: timeout_millis,
                })
            }
        }
    }

    async fn invoke_request_oneway(&self, addr: &CheetahString, request: RemotingCommand, timeout_millis: u64) {
        let client = self.get_and_create_client(Some(addr)).await;
        match client {
            None => {
                error!(remote_addr = %addr, "invoke oneway client unavailable");
            }
            Some(mut client) => {
                let mut request = request;
                if self.cmd_handler.has_rpc_hooks() {
                    let remote_address = client.remote_address();
                    request.make_custom_header_to_net();
                    if let Err(error) = self
                        .cmd_handler
                        .do_before_rpc_hooks_with_addr(remote_address, Some(&mut request))
                    {
                        warn!(
                            remote_addr = %addr,
                            error = ?error,
                            "invoke oneway before RPC hook failed"
                        );
                        return;
                    }
                }
                let addr_clone = addr.clone();
                let Some(task_group) = self.get_or_create_worker_task_group() else {
                    warn!(remote_addr = %addr, "invoke oneway skipped because client is shut down");
                    return;
                };
                if let Err(error) = task_group.spawn_service("remoting.client.oneway-send", async move {
                    match time::timeout(Duration::from_millis(timeout_millis), async move {
                        let mut request = request;
                        request.mark_oneway_rpc_ref();
                        client.send(request).await
                    })
                    .await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            warn!(
                                remote_addr = %addr_clone,
                                error = ?e,
                                "invoke oneway send failed"
                            );
                        }
                        Err(_) => {
                            warn!(
                                remote_addr = %addr_clone,
                                timeout_ms = timeout_millis,
                                "invoke oneway send timed out"
                            );
                        }
                    }
                }) {
                    warn!(
                        remote_addr = %addr,
                        error = ?error,
                        "invoke oneway send task spawn failed"
                    );
                }
            }
        }
    }

    fn invoke_oneway_unbounded(&self, addr: CheetahString, request: RemotingCommand) {
        let client_owner = self.clone();
        let addr_for_spawn_error = addr.clone();
        let Some(task_group) = self.get_or_create_worker_task_group() else {
            tracing::debug!(
                "invoke_oneway_unbounded: client is shut down, skipping send to {}",
                addr
            );
            return;
        };

        if let Err(error) = task_group.spawn_service("remoting.client.oneway-unbounded", async move {
            if client_owner.shutdown_token.is_cancelled() {
                tracing::debug!(
                    "invoke_oneway_unbounded: client is shut down, skipping send to {}",
                    addr
                );
                return;
            }

            let Some(mut client) = client_owner.get_and_create_client(Some(&addr)).await else {
                tracing::warn!("invoke_oneway_unbounded: failed to get or create client for {}", addr);
                return;
            };

            let mut request = request;
            request.mark_oneway_rpc_ref();
            if client_owner.cmd_handler.has_rpc_hooks() {
                let remote_address = client.remote_address();
                request.make_custom_header_to_net();
                if let Err(error) = client_owner
                    .cmd_handler
                    .do_before_rpc_hooks_with_addr(remote_address, Some(&mut request))
                {
                    tracing::warn!(
                        "invoke_oneway_unbounded: before RPC hook failed for {}: {:?}",
                        addr,
                        error
                    );
                    return;
                }
            }

            if let Err(error) = client.send(request).await {
                tracing::warn!("invoke_oneway_unbounded: send request to {} failed: {:?}", addr, error);
            }
        }) {
            tracing::warn!(
                "invoke_oneway_unbounded: failed to spawn send task for {}: {:?}",
                addr_for_spawn_error,
                error
            );
        }
    }

    fn is_address_reachable(&mut self, addr: &CheetahString) {
        if let Some(client_ref) = self.connection_tables.get(addr) {
            if client_ref.value().connection().is_healthy() {
                return;
            }
            // Connection exists but is unhealthy; drop the guard before removal
            drop(client_ref);
            self.connection_tables.remove(addr);
            warn!("Removed unhealthy connection for {}", addr);
        } else {
            debug!("No connection found for {}", addr);
        }
    }

    fn close_clients(&mut self, addrs: Vec<String>) {
        for addr in &addrs {
            let key = CheetahString::from(addr.as_str());
            if let Some((_, _client)) = self.connection_tables.remove(&key) {
                info!("Closed client connection for {}", addr);
            }
        }
    }

    fn register_processor(&mut self, processor: impl RequestProcessor + Sync) {
        let _ = &processor;
        warn!("dynamic request processor registration is not supported by RocketmqDefaultClient after construction");
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use rocketmq_error::RocketMQResult;
    use rocketmq_runtime::RuntimeContext;
    use tokio::net::TcpListener;

    use super::*;
    use crate::code::request_code::RequestCode;
    use crate::code::response_code::ResponseCode;
    use crate::connection::Connection;
    use crate::request_processor::default_request_processor::DefaultRemotingRequestProcessor;
    use crate::runtime::config::client_config::TokioClientConfig;

    #[derive(Default)]
    struct CountingHook {
        before_count: AtomicUsize,
        after_count: AtomicUsize,
    }

    impl RPCHook for CountingHook {
        fn do_before_request(&self, _remote_addr: SocketAddr, request: &mut RemotingCommand) -> RocketMQResult<()> {
            self.before_count.fetch_add(1, Ordering::SeqCst);
            request.ensure_ext_fields_initialized();
            request.add_ext_field("hooked", "true");
            Ok(())
        }

        fn do_after_response(
            &self,
            _remote_addr: SocketAddr,
            _request: &RemotingCommand,
            response: &mut RemotingCommand,
        ) -> RocketMQResult<()> {
            self.after_count.fetch_add(1, Ordering::SeqCst);
            response.ensure_ext_fields_initialized();
            response.add_ext_field("afterHook", "true");
            Ok(())
        }
    }

    #[test]
    fn is_use_tls_reflects_client_config() {
        let config = TokioClientConfig {
            use_tls: true,
            tls_config: TlsConfig {
                enable: true,
                ..TlsConfig::default()
            },
            ..Default::default()
        };
        let client = RocketmqDefaultClient::new(Arc::new(config), DefaultRemotingRequestProcessor);

        assert!(client.is_use_tls());
        assert!(client.tls_config().enable);
    }

    #[tokio::test]
    async fn start_tracks_background_tasks_with_task_group() {
        let config = TokioClientConfig {
            connect_timeout_millis: 10,
            channel_not_active_interval: 10,
            ..Default::default()
        };
        let client = ArcMut::new(RocketmqDefaultClient::new(
            Arc::new(config),
            DefaultRemotingRequestProcessor,
        ));
        let weak_client = ArcMut::downgrade(&client);

        client.start(weak_client).await;

        let task_group = client
            .background_task_group
            .lock()
            .as_ref()
            .cloned()
            .expect("background task group");
        assert_eq!(task_group.lifecycle_state(), TaskGroupLifecycleState::Open);
        assert_eq!(task_group.task_count(), 2);

        client.mut_from_ref().shutdown();

        assert_eq!(task_group.lifecycle_state(), TaskGroupLifecycleState::ShutdownCompleted);
    }

    #[tokio::test]
    async fn service_context_parents_background_and_worker_tasks() {
        let context = RuntimeContext::from_current("remoting-default-client-parent-test");
        let service = context.service_context("remoting-client-service");
        let config = TokioClientConfig {
            connect_timeout_millis: 10,
            channel_not_active_interval: 10,
            ..Default::default()
        };
        let client = ArcMut::new(RocketmqDefaultClient::new_with_service_context(
            Arc::new(config),
            DefaultRemotingRequestProcessor,
            service.clone(),
        ));
        let weak_client = ArcMut::downgrade(&client);

        client.start(weak_client).await;
        client
            .spawn_worker_task("remoting.client.parent-test-worker", async {})
            .expect("worker task should spawn");

        let background_task_group = client
            .background_task_group
            .lock()
            .as_ref()
            .cloned()
            .expect("background task group");
        let worker_task_group = client
            .worker_task_group
            .lock()
            .as_ref()
            .cloned()
            .expect("worker task group");

        assert_eq!(background_task_group.parent_id(), Some(service.task_group().id()));
        assert_eq!(worker_task_group.parent_id(), Some(service.task_group().id()));

        client.mut_from_ref().shutdown();
        let report = service.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn invoke_request_runs_outbound_rpc_hooks() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept client");
            let mut connection = Connection::new(socket);
            let request = connection
                .receive_command()
                .await
                .expect("request frame")
                .expect("request command");
            let hooked = request
                .ext_fields()
                .and_then(|fields| fields.get("hooked"))
                .map(|value| value.as_str());
            assert_eq!(hooked, Some("true"));

            let mut response = RemotingCommand::create_response_command_with_code(ResponseCode::Success);
            response.set_opaque_mut(request.opaque());
            connection.send_command(response).await.expect("send response");
        });

        let hook = Arc::new(CountingHook::default());
        let mut client =
            RocketmqDefaultClient::new(Arc::new(TokioClientConfig::default()), DefaultRemotingRequestProcessor);
        client.register_rpc_hook(hook.clone());

        let target = CheetahString::from_string(addr.to_string());
        let request = RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo);
        let response = client
            .invoke_request(Some(&target), request, 3_000)
            .await
            .expect("invoke request");

        assert_eq!(hook.before_count.load(Ordering::SeqCst), 1);
        assert_eq!(hook.after_count.load(Ordering::SeqCst), 1);
        assert_eq!(
            response
                .ext_fields()
                .and_then(|fields| fields.get("afterHook"))
                .map(|value| value.as_str()),
            Some("true")
        );

        server.await.expect("server task");
        client.shutdown();
    }

    #[tokio::test]
    async fn timed_out_request_retires_the_pooled_connection_before_the_next_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let server = tokio::spawn(async move {
            let (first_socket, _) = listener.accept().await.expect("accept first client");
            let mut first = Connection::new(first_socket);
            let _ = first
                .receive_command()
                .await
                .expect("first request frame")
                .expect("first request");

            let (second_socket, _) = time::timeout(Duration::from_secs(1), listener.accept())
                .await
                .expect("timeout must force a reconnect")
                .expect("accept replacement client");
            let mut second = Connection::new(second_socket);
            let request = second
                .receive_command()
                .await
                .expect("replacement request frame")
                .expect("replacement request");
            second
                .send_command(
                    RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                        .set_opaque(request.opaque()),
                )
                .await
                .expect("send replacement response");
        });

        let mut client =
            RocketmqDefaultClient::new(Arc::new(TokioClientConfig::default()), DefaultRemotingRequestProcessor);
        let target = CheetahString::from_string(addr.to_string());
        assert!(client
            .invoke_request(
                Some(&target),
                RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
                30,
            )
            .await
            .is_err());

        let response = client
            .invoke_request(
                Some(&target),
                RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
                500,
            )
            .await
            .expect("next request must use a new owner and connection");
        assert_eq!(response.code(), ResponseCode::Success.to_i32());

        server.await.expect("server task");
        client.shutdown();
    }

    #[tokio::test]
    async fn invoke_oneway_unbounded_creates_connection_before_sending() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let (received_tx, received_rx) = tokio::sync::oneshot::channel();

        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept client");
            let mut connection = Connection::new(socket);
            let request = time::timeout(Duration::from_secs(3), connection.receive_command())
                .await
                .expect("oneway request should arrive")
                .expect("request frame")
                .expect("request command");
            let hooked = request
                .ext_fields()
                .and_then(|fields| fields.get("hooked"))
                .map(|value| value.as_str() == "true")
                .unwrap_or(false);

            let _ = received_tx.send((request.code(), request.is_oneway_rpc(), hooked));
        });

        let hook = Arc::new(CountingHook::default());
        let mut client =
            RocketmqDefaultClient::new(Arc::new(TokioClientConfig::default()), DefaultRemotingRequestProcessor);
        client.register_rpc_hook(hook.clone());

        let target = CheetahString::from_string(addr.to_string());
        let request = RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo);
        client.invoke_oneway_unbounded(target, request);
        let worker_task_group = client
            .worker_task_group
            .lock()
            .as_ref()
            .cloned()
            .expect("worker task group");
        assert_eq!(worker_task_group.lifecycle_state(), TaskGroupLifecycleState::Open);

        let (code, is_oneway, hooked) = time::timeout(Duration::from_secs(3), received_rx)
            .await
            .expect("server should receive unbounded oneway request")
            .expect("server should report received request");

        assert_eq!(code, RequestCode::GetBrokerClusterInfo.to_i32());
        assert!(is_oneway);
        assert!(hooked);
        assert_eq!(hook.before_count.load(Ordering::SeqCst), 1);
        assert_eq!(hook.after_count.load(Ordering::SeqCst), 0);

        server.await.expect("server task");
        client.shutdown();
        assert_eq!(
            worker_task_group.lifecycle_state(),
            TaskGroupLifecycleState::ShutdownCompleted
        );
    }

    #[tokio::test]
    async fn shutdown_with_report_closes_connection_table_clients() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.expect("accept client");
            time::sleep(Duration::from_secs(5)).await;
        });
        let mut client =
            RocketmqDefaultClient::new(Arc::new(TokioClientConfig::default()), DefaultRemotingRequestProcessor);

        let target = CheetahString::from_string(addr.to_string());
        let created = client
            .create_client(&target, Duration::from_secs(3))
            .await
            .expect("client connection should be created");
        drop(created);
        assert_eq!(client.connection_tables.len(), 1);

        let report = client.shutdown_with_report(Duration::from_secs(1)).await;

        assert!(report.is_healthy(), "{report:?}");
        assert_eq!(report.connections.len(), 1);
        assert_eq!(report.connections[0].addr, target);
        assert!(client.connection_tables.is_empty());
        server.abort();
    }
}
