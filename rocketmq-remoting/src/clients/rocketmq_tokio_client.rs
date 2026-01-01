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
use std::sync::atomic::AtomicI32;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rand::Rng;
use rocketmq_runtime::RocketMQRuntime;
use rocketmq_rust::ArcMut;
use rocketmq_rust::WeakArcMut;
use tokio::time;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::connection_net_event::ConnectionNetEvent;
use crate::clients::connection_pool::ConnectionPool;
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

const LOCK_TIMEOUT_MILLIS: u64 = 3000;

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
/// - **Lock Contention**: Uses `Arc<Mutex<HashMap>>` for connection pool
///   - ⚠️ TODO: Migrate to `DashMap` for lock-free reads
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
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
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

    /// Round-robin index for nameserver selection
    ///
    /// ⚠️ Deprecated: Use `latency_tracker` for smart selection
    namesrv_index: Arc<AtomicI32>,

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

    /// Background task runtime for health checks and cleanup
    ///
    /// `None` after `shutdown()` is called
    client_runtime: Option<RocketMQRuntime>,

    /// Shared command handler (processor + response table)
    ///
    /// Arc-wrapped to share across all `Client` instances
    cmd_handler: ArcMut<RemotingGeneralHandler<PR>>,

    /// Optional connection event broadcaster
    ///
    /// Used for monitoring and metrics collection
    tx: Option<tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
}
impl<PR: RequestProcessor + Sync + Clone + 'static> RocketmqDefaultClient<PR> {
    pub fn new(tokio_client_config: Arc<TokioClientConfig>, processor: PR) -> Self {
        Self::new_with_cl(tokio_client_config, processor, None)
    }

    pub fn new_with_cl(
        tokio_client_config: Arc<TokioClientConfig>,
        processor: PR,
        tx: Option<tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
    ) -> Self {
        let handler = RemotingGeneralHandler {
            request_processor: processor,
            //shutdown: (),
            rpc_hooks: vec![],
            response_table: ArcMut::new(HashMap::with_capacity(512)),
        };
        Self {
            tokio_client_config,
            connection_tables: Arc::new(DashMap::with_capacity(64)),
            namesrv_addr_list: ArcMut::new(Default::default()),
            namesrv_addr_choosed: ArcMut::new(Default::default()),
            available_namesrv_addr_set: ArcMut::new(Default::default()),
            namesrv_index: Arc::new(AtomicI32::new(init_value_index())),
            latency_tracker: LatencyTracker::new(),
            circuit_breakers: Arc::new(DashMap::with_capacity(64)),
            connection_pool: None, // Disabled by default, enable via enable_connection_pool()
            client_runtime: Some(RocketMQRuntime::new_multi(10, "client-thread")),
            cmd_handler: ArcMut::new(handler),
            tx,
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
    /// Task handle for the cleanup background task (can be aborted)
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
    ) -> tokio::task::JoinHandle<()> {
        let pool = ConnectionPool::new(max_connections, max_idle_duration);
        let cleanup_task = pool.start_cleanup_task(cleanup_interval);
        self.connection_pool = Some(pool);
        info!(
            "Connection pool enabled: max={}, idle_timeout={:?}, cleanup_interval={:?}",
            max_connections, max_idle_duration, cleanup_interval
        );
        cleanup_task
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
    /// **Latency-based** (NEW): Selects lowest P99 latency nameserver
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
        // Try cached nameserver ===
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

        // Smart nameserver selection ===
        let addr_list = self.namesrv_addr_list.as_ref();

        if addr_list.is_empty() {
            warn!("No nameservers configured in namesrv_addr_list");
            return None;
        }

        // Use latency tracker to select best nameserver
        let selected_addr = self.latency_tracker.select_best(addr_list)?;

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

        //Create connection to selected nameserver ===
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
    ///
    /// # Lock Optimization
    /// Current: Hold lock during `get()` and `clone()`
    /// TODO: Use `DashMap` for lock-free read path:
    /// ```rust,ignore
    /// if let Some(client) = self.connection_tables.get(addr) {
    ///     if client.connection().is_healthy() {
    ///         return Some(client.clone());
    ///     }
    /// }
    /// ```
    async fn get_and_create_client(&self, addr: Option<&CheetahString>) -> Option<Client<PR>> {
        // HOT PATH: Most requests hit this method

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
        // Try connection pool first (if enabled) ===
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

        // Double-check before expensive connect (lock-free with DashMap) ===

        // Check if healthy client already exists (fallback to DashMap)
        if let Some(client_ref) = self.connection_tables.get(addr) {
            let client = client_ref.value().clone();
            if client.connection().is_healthy() {
                return Some(client);
            }
            // Client unhealthy - remove it immediately (DashMap allows concurrent removal)
            drop(client_ref); // Release read guard before removal
            self.connection_tables.remove(addr);
        }

        // check circuit breaker ===
        // Get or create circuit breaker for this address
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

        // Perform TCP connect WITHOUT holding lock ===
        let addr_inner = addr.to_string();

        let connect_result = time::timeout(duration, async {
            Client::connect(addr_inner, self.cmd_handler.clone(), self.tx.as_ref()).await
        })
        .await;

        // Handle connection result and update circuit breaker ===
        match connect_result {
            Ok(Ok(new_client)) => {
                // Connection successful - record success in circuit breaker
                breaker.record_success();
                self.circuit_breakers.insert(addr.clone(), breaker);

                // Insert into connection pool (if enabled) ===
                if let Some(ref pool) = self.connection_pool {
                    if pool.insert(addr.clone(), new_client.clone()) {
                        info!("Added connection to pool: {} (pool size: {})", addr, pool.stats().total);
                    } else {
                        warn!("Connection pool at capacity, falling back to DashMap");
                    }
                }

                // Insert into DashMap (fallback or dual-store) ===
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
    /// - **Concurrency**: Sequential probes (TODO: parallelize with `join_all`)
    /// - **Overhead**: O(N) where N = number of nameservers (typically < 10)
    ///
    /// # Optimization Opportunities
    ///
    /// 1. **Parallel probes**: Use `futures::future::join_all` to probe all nameservers
    ///    concurrently
    /// 2. **Smart backoff**: Skip probing recently-successful servers
    /// 3. **Metrics**: Track probe latency for latency-based selection
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

        // Cleanup - Remove stale entries ===
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

        // Parallel probe all configured nameservers ===
        // Parallel probing reduces scan time from O(N * 50ms) to O(max(50ms))
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
}

#[allow(unused_variables)]
impl<PR: RequestProcessor + Sync + Clone + 'static> RemotingService for RocketmqDefaultClient<PR> {
    async fn start(&self, this: WeakArcMut<Self>) {
        if let Some(client) = this.upgrade() {
            let connect_timeout_millis = self.tokio_client_config.connect_timeout_millis as u64;
            self.client_runtime.as_ref().unwrap().get_handle().spawn(async move {
                loop {
                    client.scan_available_name_srv().await;
                    time::sleep(Duration::from_millis(connect_timeout_millis)).await;
                }
            });
        }
    }

    fn shutdown(&mut self) {
        if let Some(rt) = self.client_runtime.take() {
            rt.shutdown();
        }
        // DashMap::clear() is already thread-safe, no need for async lock
        self.connection_tables.clear();
        self.namesrv_addr_list.clear();
        self.available_namesrv_addr_set.clear();

        info!(">>>>>>>>>>>>>>>RemotingClient shutdown success<<<<<<<<<<<<<<<<<");
    }

    fn register_rpc_hook(&mut self, hook: Arc<dyn RPCHook>) {
        self.cmd_handler.register_rpc_hook(hook);
    }

    fn clear_rpc_hook(&mut self) {
        todo!()
    }
}

#[allow(unused_variables)]
impl<PR: RequestProcessor + Sync + Clone + 'static> RemotingClient for RocketmqDefaultClient<PR> {
    async fn update_name_server_address_list(&self, addrs: Vec<CheetahString>) {
        let old = self.namesrv_addr_list.mut_from_ref();
        let mut update = false;

        if !addrs.is_empty() {
            if old.is_empty() || addrs.len() != old.len() {
                update = true;
            } else {
                for addr in &addrs {
                    if !old.contains(addr) {
                        update = true;
                        break;
                    }
                }
            }

            if update {
                // Shuffle the addresses
                // Shuffle logic is not implemented here as it is not available in standard library
                // You can implement it using various algorithms like Fisher-Yates shuffle

                info!(
                    "name remoting_server address updated. NEW : {:?} , OLD: {:?}",
                    addrs, old
                );
                /* let mut rng = thread_rng();
                addrs.shuffle(&mut rng);*/
                self.namesrv_addr_list.mut_from_ref().extend(addrs.clone());

                // should close the channel if choosed addr is not exist.
                if let Some(namesrv_addr) = self.namesrv_addr_choosed.as_ref() {
                    if !addrs.contains(namesrv_addr) {
                        // DashMap allows direct removal without collecting
                        self.connection_tables.remove(namesrv_addr);
                    }
                }
            }
        }
    }

    fn get_name_server_address_list(&self) -> &[CheetahString] {
        self.namesrv_addr_list.as_ref()
    }

    fn get_available_name_srv_list(&self) -> Vec<CheetahString> {
        self.available_namesrv_addr_set.as_ref().clone().into_iter().collect()
    }

    /// Send request and wait for response with timeout.
    ///
    /// # HOT PATH
    /// This is the primary client API - optimize for low latency and high throughput.
    ///
    /// # Flow
    /// ```text
    /// 1. Get/create client connection         (~100ns fast path, ~50ms slow)
    /// 2. Spawn send task on runtime           (~10μs)
    /// 3. Apply timeout wrapper                (~100ns)
    /// 4. Wait for response via oneshot        (network RTT + processing)
    /// 5. Unwrap nested Result layers          (~10ns)
    /// ```
    ///
    /// # Performance Optimizations
    ///
    /// - **Early validation**: Check client availability before expensive spawn
    /// - **Flat error handling**: Reduce nested `match` overhead
    /// - **Direct await**: Spawn only for timeout enforcement (could be optimized further)
    ///
    /// # Error Handling
    ///
    /// Returns `RocketmqError::RemoteError` for all failures:
    /// - Client unavailable (no connection)
    /// - Network I/O error (send/recv failure)
    /// - Timeout (no response within deadline)
    /// - Task spawn error (runtime shutdown)
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
    /// # async fn example(client: &RocketmqDefaultClient) -> Result<(), Box<dyn std::error::Error>> {
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

        // === Get client connection ===
        let mut client = self.get_and_create_client(addr).await.ok_or_else(|| {
            let target = addr.map(|a| a.as_str()).unwrap_or("<nameserver>");
            error!("Failed to get client for {}", target);

            // Record connection error
            if let Some(ref addr) = target_addr {
                self.latency_tracker.record_error(addr);
            }

            rocketmq_error::RocketMQError::network_connection_failed(target.to_string(), "Failed to connect")
        })?;

        // === Send request with timeout ===
        let runtime = self.client_runtime.as_ref().ok_or_else(|| {
            error!("Client runtime has been shut down");
            rocketmq_error::RocketMQError::ClientNotStarted
        })?;

        let send_task = runtime.get_handle().spawn(async move {
            // Apply timeout at the send_read level
            match time::timeout(
                Duration::from_millis(timeout_millis),
                client.send_read(request, timeout_millis),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => Err(rocketmq_error::RocketMQError::Timeout {
                    operation: "send_request",
                    timeout_ms: timeout_millis,
                }),
            }
        });

        // === Await result and record metrics ===
        match send_task.await {
            Ok(send_result) => {
                let latency = start.elapsed();

                match send_result {
                    Ok(response) => {
                        // Record successful request latency
                        if let Some(ref addr) = target_addr {
                            let latency_ms = latency.as_millis() as u64;
                            self.latency_tracker.record_success(addr, latency);

                            // Record in connection pool metrics (if enabled)
                            if let Some(ref pool) = self.connection_pool {
                                pool.record_success(addr, latency_ms);
                            }

                            debug!("Request to {} completed in {:?}", addr, latency);
                        }
                        Ok(response)
                    }
                    Err(err) => {
                        // Record error
                        if let Some(ref addr) = target_addr {
                            self.latency_tracker.record_error(addr);

                            // Record in connection pool metrics (if enabled)
                            if let Some(ref pool) = self.connection_pool {
                                pool.record_error(addr);
                            }

                            warn!("Request to {} failed after {:?}: {:?}", addr, latency, err);
                        }
                        Err(err)
                    }
                }
            }
            Err(join_err) => {
                // Task panic or cancellation
                error!("Send task failed: {:?}", join_err);

                // Record error
                if let Some(ref addr) = target_addr {
                    self.latency_tracker.record_error(addr);
                }

                Err(rocketmq_error::RocketMQError::Internal(format!(
                    "Send task error: {}",
                    join_err
                )))
            }
        }
    }

    async fn invoke_request_oneway(&self, addr: &CheetahString, request: RemotingCommand, timeout_millis: u64) {
        let client = self.get_and_create_client(Some(addr)).await;
        match client {
            None => {
                error!("get client failed");
            }
            Some(mut client) => {
                self.client_runtime.as_ref().unwrap().get_handle().spawn(async move {
                    match time::timeout(Duration::from_millis(timeout_millis), async move {
                        let mut request = request;
                        request.mark_oneway_rpc_ref();
                        client.send(request).await
                    })
                    .await
                    {
                        Ok(_) => Ok::<(), rocketmq_error::RocketMQError>(()),
                        Err(_) => Err(rocketmq_error::RocketMQError::Timeout {
                            operation: "send_oneway",
                            timeout_ms: timeout_millis,
                        }),
                    }
                });
            }
        }
    }

    fn is_address_reachable(&mut self, addr: &CheetahString) {
        todo!()
    }

    fn close_clients(&mut self, addrs: Vec<String>) {
        todo!()
    }

    fn register_processor(&mut self, processor: impl RequestProcessor + Sync) {
        todo!()
    }
}

fn init_value_index() -> i32 {
    let mut rng = rand::rng();
    rng.random_range(0..999)
}
