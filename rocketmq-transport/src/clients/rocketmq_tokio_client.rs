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
use std::ops::Deref;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use parking_lot::Mutex;
use parking_lot::RwLock;
use rocketmq_error::NetworkError;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_error::RpcClientError;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::ResourcePermit;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskGroupLifecycleState;
use rocketmq_runtime::TaskId;
use serde::Serialize;
use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::connection_net_event::ConnectionNetEvent;
use crate::base::pending_request_table::PendingRequestTable;
use crate::base::pending_request_table::PendingRequestUsage;
use crate::clients::client::SessionConnectTarget;
use crate::clients::nameserver_endpoint::diff_name_server_endpoints;
use crate::clients::nameserver_endpoint::ConnectTarget;
use crate::clients::nameserver_endpoint::NameServerEndpoint;
use crate::clients::nameserver_failover::build_nameserver_failover_candidates;
use crate::clients::nameserver_selector::LatencyTracker;
use crate::clients::reconnect::CircuitAdmission;
use crate::clients::reconnect::CircuitBreaker;
use crate::clients::reconnect::CircuitState;
use crate::clients::TransportSession;
use crate::codec::remoting_command_codec::FrameLimits;
use crate::deadline::RequestDeadline;
use crate::error_helpers::remote_error;
use crate::remoting::inner::RemotingGeneralHandler;
use crate::request_processor::default_request_processor::DefaultRequestProcessor;
#[cfg(test)]
use crate::runtime::config::client_config::ConnectConfig;
use crate::runtime::config::client_config::GoAwayPolicy;
#[cfg(test)]
use crate::runtime::config::client_config::MaintenanceConfig;
use crate::runtime::config::client_config::TransportClientConfig;
use crate::runtime::processor::RequestProcessor;
use crate::runtime::RPCHook;
use crate::security::TransportSecurity;
use crate::telemetry::TransportGoAwayOutcome;
use crate::telemetry::TransportNameServerFailoverReason;
use crate::telemetry::TransportTelemetry;
use crate::tls::TlsConfig;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

/// High-performance async RocketMQ client with persistent endpoint sessions and auto-reconnection.
///
/// # Architecture
///
/// ```text
/// ┌─────────────────────────────────────────────────────────┐
/// │            TransportClient<PR>                    │
/// ├─────────────────────────────────────────────────────────┤
/// │                                                         │
/// │  ┌────────────────┐      ┌──────────────────┐         │
/// │  │Session Registry│ ───► │NameServer Router │         │
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
/// - **Persistent Sessions**: Reuses one healthy TCP session per broker or nameserver endpoint
/// - **Auto-Reconnection**: Exponential backoff retry on connection failures
/// - **Smart Routing**: Selects healthiest nameserver based on latency/errors
/// - **Request Multiplexing**: Multiple concurrent requests per connection
/// - **Graceful Shutdown**: Drains in-flight requests before closing
///
/// # Performance Characteristics
///
/// - **Concurrency**: Uses `DashMap` for concurrent reads on the endpoint-session registry
/// - **Memory**: O(N) where N = number of unique broker addresses
/// - **Latency**: Single async hop for cached connections, 2-3 hops for new
///
/// # Type Parameters
///
/// * `PR` - Request processor type (default: `DefaultRequestProcessor`)
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
///
/// use crate::clients::TransportClient;
/// use crate::runtime::config::client_config::TransportClientConfig;
///
/// # async fn example() -> rocketmq_error::RocketMQResult<()> {
/// let config = Arc::new(TransportClientConfig::default());
/// let processor = Default::default();
/// let client = TransportClient::builder(config, processor, service_context).build()?;
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
pub struct TransportClient<PR = DefaultRequestProcessor> {
    /// Client configuration (timeouts, buffer sizes, etc.)
    ///
    /// Shared across all connections to avoid duplication
    tokio_client_config: Arc<TransportClientConfig>,

    /// Persistent endpoint-session registry: `addr -> Client`.
    ///
    /// **Lock-Free Design**: Uses `DashMap` for concurrent access without Mutex
    /// - Read operations (get): Zero-lock overhead
    /// - Write operations (insert/remove): Fine-grained per-shard locking
    /// - Concurrency: Scales linearly with CPU cores (typically 16-64 shards)
    ///
    /// Invariant: Only contains healthy connections (unhealthy removed on error)
    connection_tables: Arc<DashMap<CheetahString /* ip:port */, TransportSession<PR>>>,

    /// One lifecycle-owned connection attempt per endpoint during cold bursts.
    connect_flights: Arc<DashMap<CheetahString, Arc<ConnectFlight<PR>>>>,
    connect_attempts: Arc<AtomicU64>,
    namesrv_draining_count: Arc<AtomicUsize>,

    /// List of all nameserver addresses (in priority order)
    ///
    /// Updated via `update_name_server_address_list()`
    namesrv_endpoints: Arc<ArcSwap<Vec<NameServerEndpoint>>>,

    /// Currently selected nameserver (cached for fast path)
    ///
    /// May be `None` if no nameserver available or all unhealthy
    namesrv_addr_choosed: Arc<RwLock<Option<CheetahString>>>,

    /// Set of healthy/reachable nameservers
    ///
    /// Updated asynchronously by health check task (`scan_available_name_srv`)
    available_namesrv_addr_set: Arc<RwLock<HashSet<CheetahString>>>,

    /// Latency tracker for smart nameserver selection
    ///
    /// Tracks P99 latency and error rates to select the best nameserver
    latency_tracker: LatencyTracker,

    /// Circuit breakers per address to prevent cascading failures
    ///
    /// Maps address to circuit breaker state for auto-reconnection
    circuit_breakers: Arc<DashMap<CheetahString, CircuitBreaker>>,

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
    service_context: ChildServiceContext,

    /// Shared command handler (processor + response table)
    ///
    /// Arc-wrapped to share across all `Client` instances
    cmd_handler: Arc<RemotingGeneralHandler<PR>>,

    /// Optional connection event broadcaster
    ///
    /// Used for monitoring and metrics collection
    tx: Option<tokio::sync::broadcast::Sender<ConnectionNetEvent>>,

    /// Optional signer applied by each canonical transport session before sending.
    transport_security: Option<Arc<TransportSecurity>>,

    telemetry: TransportTelemetry,
    frame_limits: FrameLimits,
    go_away_policy: GoAwayPolicy,
}

/// Builds a persistent endpoint client without exposing positional optional capabilities.
pub struct TransportClientBuilder<PR> {
    config: Arc<TransportClientConfig>,
    processor: PR,
    service_context: ChildServiceContext,
    connection_events: Option<tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
    transport_security: Option<Arc<TransportSecurity>>,
    telemetry: TransportTelemetry,
    frame_limits: FrameLimits,
    go_away_policy: GoAwayPolicy,
}

impl<PR> TransportClientBuilder<PR>
where
    PR: RequestProcessor + Sync + Clone + 'static,
{
    pub fn connection_events(mut self, events: tokio::sync::broadcast::Sender<ConnectionNetEvent>) -> Self {
        self.connection_events = Some(events);
        self
    }

    pub fn transport_security(mut self, transport_security: Arc<TransportSecurity>) -> Self {
        self.transport_security = Some(transport_security);
        self
    }

    pub fn telemetry(mut self, telemetry: TransportTelemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Applies one validated frame profile to every connection created by this client.
    pub fn frame_limits(mut self, frame_limits: FrameLimits) -> RocketMQResult<Self> {
        frame_limits.validate()?;
        self.frame_limits = frame_limits;
        Ok(self)
    }

    /// Applies an explicit allowlist for one bounded `GO_AWAY` reconnect retry.
    #[must_use]
    pub fn go_away_policy(mut self, policy: GoAwayPolicy) -> Self {
        self.go_away_policy = policy;
        self
    }

    pub fn build(self) -> RocketMQResult<TransportClient<PR>> {
        let mut client = TransportClient::build_inner(
            self.config,
            self.processor,
            self.connection_events,
            self.service_context,
            self.telemetry,
            self.frame_limits,
            self.go_away_policy,
        )?;
        if let Some(transport_security) = self.transport_security {
            client = client.with_transport_security(transport_security);
        }
        Ok(client)
    }
}

/// Nameserver-aware remoting client.
///
/// This type composes the canonical persistent [`TransportClient`]. It never
/// owns a second connection registry, writer queue, or pending-request table.
#[derive(Clone)]
pub struct RemotingClient<PR = DefaultRequestProcessor> {
    transport: Arc<TransportClient<PR>>,
}

impl<PR> RemotingClient<PR>
where
    PR: RequestProcessor + Sync + Clone + 'static,
{
    pub fn builder(
        config: Arc<TransportClientConfig>,
        processor: PR,
        service_context: ChildServiceContext,
    ) -> RemotingClientBuilder<PR> {
        RemotingClientBuilder {
            transport: TransportClient::builder(config, processor, service_context),
        }
    }

    pub fn transport_client(&self) -> Arc<TransportClient<PR>> {
        Arc::clone(&self.transport)
    }

    pub async fn start(self: &Arc<Self>) -> RocketMQResult<ClientStartReport> {
        self.transport.start().await
    }

    pub async fn shutdown_until(&self, deadline: ShutdownDeadline) -> RocketMQResult<ClientShutdownReport> {
        Ok(self.transport.shutdown_with_report(deadline.remaining()).await)
    }
}

impl<PR> Deref for RemotingClient<PR> {
    type Target = TransportClient<PR>;

    fn deref(&self) -> &Self::Target {
        self.transport.as_ref()
    }
}

pub struct RemotingClientBuilder<PR> {
    transport: TransportClientBuilder<PR>,
}

impl<PR> RemotingClientBuilder<PR>
where
    PR: RequestProcessor + Sync + Clone + 'static,
{
    pub fn connection_events(mut self, events: tokio::sync::broadcast::Sender<ConnectionNetEvent>) -> Self {
        self.transport = self.transport.connection_events(events);
        self
    }

    pub fn transport_security(mut self, transport_security: Arc<TransportSecurity>) -> Self {
        self.transport = self.transport.transport_security(transport_security);
        self
    }

    pub fn telemetry(mut self, telemetry: TransportTelemetry) -> Self {
        self.transport = self.transport.telemetry(telemetry);
        self
    }

    /// Applies one validated frame profile to every connection created by this client.
    pub fn frame_limits(mut self, frame_limits: FrameLimits) -> RocketMQResult<Self> {
        self.transport = self.transport.frame_limits(frame_limits)?;
        Ok(self)
    }

    /// Applies an explicit allowlist for one bounded `GO_AWAY` reconnect retry.
    #[must_use]
    pub fn go_away_policy(mut self, policy: GoAwayPolicy) -> Self {
        self.transport = self.transport.go_away_policy(policy);
        self
    }

    pub fn build(self) -> RocketMQResult<RemotingClient<PR>> {
        Ok(RemotingClient {
            transport: Arc::new(self.transport.build()?),
        })
    }
}

enum ConnectFlightState<PR> {
    Connecting,
    Complete(Box<Result<Option<TransportSession<PR>>, Arc<str>>>),
}

struct ConnectFlight<PR> {
    state: Mutex<ConnectFlightState<PR>>,
    changed: tokio::sync::Notify,
}

struct DrainingEndpointGuard(Arc<AtomicUsize>);

impl Drop for DrainingEndpointGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl<PR> ConnectFlight<PR>
where
    PR: RequestProcessor + Sync + Clone + 'static,
{
    fn new() -> Self {
        Self {
            state: Mutex::new(ConnectFlightState::Connecting),
            changed: tokio::sync::Notify::new(),
        }
    }

    fn complete(&self, result: RocketMQResult<Option<TransportSession<PR>>>) {
        *self.state.lock() =
            ConnectFlightState::Complete(Box::new(result.map_err(|error| Arc::<str>::from(error.to_string()))));
        self.changed.notify_waiters();
    }

    async fn wait(
        &self,
        deadline: RequestDeadline,
        target: &CheetahString,
    ) -> RocketMQResult<Option<TransportSession<PR>>> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let ConnectFlightState::Complete(result) = &*self.state.lock() {
                return (**result).clone().map_err(|message| {
                    RocketMQError::network_connection_failed(target.to_string(), message.to_string())
                });
            }
            deadline
                .timeout(changed)
                .await
                .map_err(|_| RocketMQError::network_connection_timeout(target.to_string(), deadline.budget_millis()))?;
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize)]
pub struct ClientStartReport {
    pub background_tasks_started: usize,
    pub already_running: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConnectionShutdownReport {
    pub addr: CheetahString,
    pub report: ShutdownReport,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ClientShutdownReport {
    pub background: Option<ShutdownReport>,
    pub workers: Option<ShutdownReport>,
    pub connections: Vec<ConnectionShutdownReport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum RequestTarget {
    Endpoint(CheetahString),
    NameServer,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SendReceipt {
    pub endpoint: CheetahString,
    pub written_at_millis: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct PendingUsage {
    pub count: usize,
    pub retained_bytes: usize,
    pub rejected_count: usize,
    pub rejected_bytes: usize,
}

impl From<PendingRequestUsage> for PendingUsage {
    fn from(usage: PendingRequestUsage) -> Self {
        Self {
            count: usage.count,
            retained_bytes: usage.bytes,
            rejected_count: usage.rejected_count,
            rejected_bytes: usage.rejected_bytes,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ClientSnapshot {
    pub connection_count: usize,
    pub connect_flight_count: usize,
    pub configured_name_server_count: usize,
    pub available_name_server_count: usize,
    pub healthy_name_server_count: usize,
    pub probing_name_server_count: usize,
    pub draining_name_server_count: usize,
    pub circuit_open_name_server_count: usize,
    pub pending: PendingUsage,
}

impl ClientShutdownReport {
    pub fn is_healthy(&self) -> bool {
        self.background.as_ref().is_none_or(ShutdownReport::is_healthy)
            && self.workers.as_ref().is_none_or(ShutdownReport::is_healthy)
            && self.connections.iter().all(|connection| connection.report.is_healthy())
    }
}

impl<PR> Clone for TransportClient<PR> {
    fn clone(&self) -> Self {
        Self {
            tokio_client_config: self.tokio_client_config.clone(),
            connection_tables: self.connection_tables.clone(),
            connect_flights: self.connect_flights.clone(),
            connect_attempts: self.connect_attempts.clone(),
            namesrv_draining_count: self.namesrv_draining_count.clone(),
            namesrv_endpoints: self.namesrv_endpoints.clone(),
            namesrv_addr_choosed: self.namesrv_addr_choosed.clone(),
            available_namesrv_addr_set: self.available_namesrv_addr_set.clone(),
            latency_tracker: self.latency_tracker.clone(),
            circuit_breakers: self.circuit_breakers.clone(),
            shutdown_token: self.shutdown_token.clone(),
            background_task_group: self.background_task_group.clone(),
            worker_task_group: self.worker_task_group.clone(),
            service_context: self.service_context.clone(),
            cmd_handler: self.cmd_handler.clone(),
            tx: self.tx.clone(),
            transport_security: self.transport_security.clone(),
            telemetry: self.telemetry.clone(),
            frame_limits: self.frame_limits,
            go_away_policy: self.go_away_policy.clone(),
        }
    }
}

impl<PR: RequestProcessor + Sync + Clone + 'static> TransportClient<PR> {
    const NAMESERVER_SCAN_INTERVAL: Duration = Duration::from_secs(30);

    pub fn builder(
        tokio_client_config: Arc<TransportClientConfig>,
        processor: PR,
        service_context: ChildServiceContext,
    ) -> TransportClientBuilder<PR> {
        TransportClientBuilder {
            config: tokio_client_config,
            processor,
            service_context,
            connection_events: None,
            transport_security: None,
            telemetry: TransportTelemetry::noop(),
            frame_limits: FrameLimits::java_compatibility(),
            go_away_policy: GoAwayPolicy::default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(
        config: Arc<TransportClientConfig>,
        processor: PR,
        service_context: ChildServiceContext,
    ) -> Self {
        Self::builder(config, processor, service_context)
            .build()
            .expect("test transport client configuration must be valid")
    }

    fn build_inner(
        tokio_client_config: Arc<TransportClientConfig>,
        processor: PR,
        tx: Option<tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
        service_context: ChildServiceContext,
        telemetry: TransportTelemetry,
        frame_limits: FrameLimits,
        go_away_policy: GoAwayPolicy,
    ) -> RocketMQResult<Self> {
        frame_limits.validate()?;
        let process_budget = service_context.process_budget();
        let handler = RemotingGeneralHandler::new_with_telemetry(
            processor,
            vec![],
            PendingRequestTable::try_with_limits_and_budget(
                crate::base::pending_request_table::PendingRequestLimits {
                    max_count: 512,
                    ..Default::default()
                },
                &process_budget,
            )?,
            telemetry.clone(),
        );
        Ok(Self {
            tokio_client_config,
            connection_tables: Arc::new(DashMap::with_capacity(64)),
            connect_flights: Arc::new(DashMap::with_capacity(64)),
            connect_attempts: Arc::new(AtomicU64::new(0)),
            namesrv_draining_count: Arc::new(AtomicUsize::new(0)),
            namesrv_endpoints: Arc::new(ArcSwap::from_pointee(Vec::new())),
            namesrv_addr_choosed: Arc::new(RwLock::new(Default::default())),
            available_namesrv_addr_set: Arc::new(RwLock::new(Default::default())),
            latency_tracker: LatencyTracker::new(),
            circuit_breakers: Arc::new(DashMap::with_capacity(64)),
            shutdown_token: CancellationToken::new(),
            background_task_group: Arc::new(Mutex::new(None)),
            worker_task_group: Arc::new(Mutex::new(None)),
            service_context,
            cmd_handler: Arc::new(handler),
            tx,
            transport_security: None,
            telemetry,
            frame_limits,
            go_away_policy,
        })
    }

    /// Installs an optional transport signer for newly created outbound sessions.
    pub fn with_transport_security(mut self, transport_security: Arc<TransportSecurity>) -> Self {
        self.transport_security = Some(transport_security);
        self
    }

    /// Returns whether newly created outbound connections use TLS.
    #[inline]
    pub fn is_use_tls(&self) -> bool {
        self.tokio_client_config.tls.enable
    }

    /// Returns the TLS configuration used when creating new outbound connections.
    #[inline]
    pub fn tls_config(&self) -> &TlsConfig {
        &self.tokio_client_config.tls
    }

    #[must_use]
    pub fn snapshot(&self) -> ClientSnapshot {
        let endpoints = self.namesrv_endpoints.load();
        let identities = endpoints
            .iter()
            .map(|endpoint| endpoint.identity())
            .collect::<HashSet<_>>();
        let healthy_name_server_count = identities
            .iter()
            .filter(|identity| {
                self.connection_tables
                    .get(**identity)
                    .is_some_and(|session| session.connection().is_healthy())
                    && self.latency_tracker.is_healthy(identity)
            })
            .count();
        let (probing_name_server_count, circuit_open_name_server_count) = self
            .circuit_breakers
            .iter()
            .filter(|entry| identities.contains(entry.key()))
            .fold((0, 0), |(probing, open), entry| match entry.state() {
                CircuitState::Closed => (probing, open),
                CircuitState::HalfOpen => (probing + 1, open),
                CircuitState::Open => (probing, open + 1),
            });
        ClientSnapshot {
            connection_count: self.connection_tables.len(),
            connect_flight_count: self.connect_flights.len(),
            configured_name_server_count: self.namesrv_endpoints.load().len(),
            available_name_server_count: self.available_namesrv_addr_set.read().len(),
            healthy_name_server_count,
            probing_name_server_count,
            draining_name_server_count: self.namesrv_draining_count.load(Ordering::Acquire),
            circuit_open_name_server_count,
            pending: self.cmd_handler.response_table.usage().into(),
        }
    }

    pub fn update_name_server_address_list_sync(&self, addrs: Vec<CheetahString>) {
        if addrs.is_empty() {
            return;
        }

        use rand::seq::SliceRandom;
        let mut shuffled = addrs.clone();
        shuffled.shuffle(&mut rand::rng());
        let endpoints = shuffled
            .into_iter()
            .filter_map(|address| match NameServerEndpoint::legacy(address.clone()) {
                Ok(endpoint) => Some(endpoint),
                Err(error) => {
                    warn!(%address, ?error, "ignored invalid legacy NameServer endpoint");
                    None
                }
            })
            .collect();
        self.apply_name_server_endpoint_snapshot_sync(endpoints, Duration::from_secs(30));
    }

    /// Atomically applies resolved NameServer targets and starts bounded retirement for removals.
    pub fn update_name_server_connect_targets_sync(&self, targets: Vec<ConnectTarget>, drain_timeout: Duration) {
        let endpoints = targets.into_iter().map(NameServerEndpoint::resolved).collect();
        self.apply_name_server_endpoint_snapshot_sync(endpoints, drain_timeout);
    }

    /// Atomically publishes a complete selector snapshot.
    pub fn apply_name_server_endpoint_snapshot_sync(
        &self,
        endpoints: Vec<NameServerEndpoint>,
        drain_timeout: Duration,
    ) {
        let current = self.namesrv_endpoints.load_full();
        let diff = diff_name_server_endpoints(&current, &endpoints);
        if diff.added.is_empty() && diff.removed.is_empty() {
            return;
        }

        let old_count = current.len();
        self.namesrv_endpoints.store(Arc::new(endpoints));
        info!(
            added = diff.added.len(),
            unchanged = diff.unchanged.len(),
            removed = diff.removed.len(),
            old_count,
            new_count = self.namesrv_endpoints.load().len(),
            "NameServer endpoint snapshot updated"
        );

        for endpoint in diff.removed {
            let identity = endpoint.identity().clone();
            self.available_namesrv_addr_set.write().remove(&identity);
            if self.namesrv_addr_choosed.read().as_ref() == Some(&identity) {
                self.namesrv_addr_choosed.write().take();
            }
            self.connect_flights.remove(&identity);
            if let Some((_, session)) = self.connection_tables.remove(&identity) {
                self.start_removed_endpoint_drain(identity, session, drain_timeout);
            }
        }
    }

    fn name_server_identity_list(&self) -> Vec<CheetahString> {
        self.namesrv_endpoints
            .load()
            .iter()
            .map(|endpoint| endpoint.identity().clone())
            .collect()
    }

    fn name_server_endpoint(&self, identity: &CheetahString) -> Option<NameServerEndpoint> {
        self.namesrv_endpoints
            .load()
            .iter()
            .find(|endpoint| endpoint.identity() == identity)
            .cloned()
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

        let task_group = self
            .service_context
            .component("rocketmq-transport.client.workers")
            .task_group()
            .clone();
        *task_group_guard = Some(task_group.clone());
        Some(task_group)
    }

    #[cfg(test)]
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

    fn start_removed_endpoint_drain(
        &self,
        identity: CheetahString,
        session: TransportSession<PR>,
        drain_timeout: Duration,
    ) {
        session.begin_drain();
        self.namesrv_draining_count.fetch_add(1, Ordering::AcqRel);
        let drain_guard = DrainingEndpointGuard(self.namesrv_draining_count.clone());
        self.telemetry
            .record_nameserver_failover(TransportNameServerFailoverReason::Draining);
        let latency_tracker = self.latency_tracker.clone();
        let circuit_breakers = self.circuit_breakers.clone();
        let cleanup_identity = identity.clone();
        let task_name = format!("rocketmq.transport.nameserver-drain.{identity}");
        let spawned = self.spawn_worker_task(task_name, async move {
            let _drain_guard = drain_guard;
            let report = session.drain_and_close(drain_timeout).await;
            latency_tracker.remove(&cleanup_identity);
            circuit_breakers.remove(&cleanup_identity);
            if !report.is_healthy() {
                warn!(report = %report.to_json(), "removed NameServer endpoint drain was unhealthy");
            }
        });
        if spawned.is_none() {
            warn!("removed NameServer endpoint drain could not be scheduled because the client is shutting down");
        }
    }
}

impl<PR: RequestProcessor + Sync + Clone + 'static> TransportClient<PR> {
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
    async fn get_and_create_nameserver_client_until(
        &self,
        deadline: RequestDeadline,
    ) -> RocketMQResult<Option<TransportSession<PR>>> {
        deadline.ensure_before_send("<nameserver>")?;
        let cached_addr = self.namesrv_addr_choosed.read().clone();

        if let Some(ref addr) = cached_addr {
            // Quick lookup in the endpoint-session registry.
            if let Some(client) = self.connection_tables.get(addr) {
                if client.connection().is_healthy() && self.latency_tracker.is_healthy(addr) {
                    // Fast path: Cached nameserver is healthy
                    return Ok(Some(client.value().clone()));
                }
                debug!("Cached nameserver {} is unhealthy, selecting new one", addr);
                self.telemetry
                    .record_nameserver_failover(TransportNameServerFailoverReason::Unhealthy);
            }
        }

        let addr_list = self.name_server_identity_list();

        if addr_list.is_empty() {
            warn!("No nameservers configured in namesrv_addr_list");
            return Ok(None);
        }

        let available = self.available_namesrv_addr_set.read().clone();
        let mut half_open_probe_selected = false;
        let mut candidates =
            build_nameserver_failover_candidates(&addr_list, &available, &self.latency_tracker, |address| {
                let mut breaker = self
                    .circuit_breakers
                    .entry(address.clone())
                    .or_insert_with(CircuitBreaker::default_breaker);
                match breaker.connection_admission() {
                    CircuitAdmission::Regular => true,
                    CircuitAdmission::Probe if !half_open_probe_selected => {
                        half_open_probe_selected = true;
                        true
                    }
                    CircuitAdmission::Probe | CircuitAdmission::Rejected => {
                        self.telemetry
                            .record_nameserver_failover(TransportNameServerFailoverReason::CircuitOpen);
                        false
                    }
                }
            });
        candidates.sort_by_key(|address| {
            self.connection_tables
                .get(address)
                .is_none_or(|session| !session.connection().is_healthy())
        });
        if candidates.is_empty() {
            error!(
                "Failed to select healthy nameserver. Available list: {:?}, Available set: {:?}",
                addr_list, available
            );
            return Ok(None);
        }

        let mut last_error = None;
        for selected_addr in candidates {
            deadline.ensure_before_send(selected_addr.to_string())?;
            info!(
                "Selected nameserver: {} (P99: {:?}, errors: {})",
                selected_addr,
                self.latency_tracker
                    .get_p99(&selected_addr)
                    .unwrap_or(Duration::from_secs(0)),
                self.latency_tracker.get_error_count(&selected_addr)
            );
            match self.create_client_until(&selected_addr, deadline).await {
                Ok(Some(client)) => {
                    self.namesrv_addr_choosed.write().replace(selected_addr);
                    return Ok(Some(client));
                }
                Ok(None) => {}
                Err(error) => {
                    self.telemetry
                        .record_nameserver_failover(TransportNameServerFailoverReason::ConnectFailure);
                    self.latency_tracker.record_error(&selected_addr);
                    last_error = Some(error);
                }
            }
        }

        self.namesrv_addr_choosed.write().take();
        match last_error {
            Some(error) => Err(error),
            None => Ok(None),
        }
    }

    /// Get existing healthy client or create new connection.
    ///
    /// # Flow
    /// 1. If `addr` is `None` or empty, route to nameserver
    /// 2. Check the endpoint-session registry for an existing client
    /// 3. Verify client health (connection.is_healthy() == true)
    /// 4. If unhealthy or missing, create new connection
    ///
    /// # Performance
    /// - **Fast path**: Single lock acquire + HashMap lookup + health check (< 100ns)
    /// - **Slow path**: Lock + TCP handshake + TLS (if enabled) (10-50ms)
    async fn get_and_create_client(&self, addr: Option<&CheetahString>) -> Option<TransportSession<PR>> {
        let deadline = RequestDeadline::after(self.tokio_client_config.connect.timeout);
        match self.get_and_create_client_until(addr, deadline).await {
            Ok(client) => client,
            Err(error) => {
                warn!(error = ?error, "Failed to get or create remoting client");
                None
            }
        }
    }

    async fn get_and_create_client_until(
        &self,
        addr: Option<&CheetahString>,
        deadline: RequestDeadline,
    ) -> RocketMQResult<Option<TransportSession<PR>>> {
        // Route empty addresses to nameserver
        let target_addr = match addr {
            None => return self.get_and_create_nameserver_client_until(deadline).await,
            Some(addr) if addr.is_empty() => return self.get_and_create_nameserver_client_until(deadline).await,
            Some(addr) => addr,
        };
        deadline.ensure_before_send(target_addr.to_string())?;

        // Fast path: Check the persistent endpoint-session registry.
        if let Some(client_ref) = self.connection_tables.get(target_addr) {
            let client = client_ref.value().clone();
            if client.connection().is_healthy() {
                return Ok(Some(client)); // Return healthy cached client
            }
            // Client unhealthy - will create new connection
            debug!("Cached client for {} is unhealthy, reconnecting...", target_addr);
        }

        // Slow path: Create new connection
        self.create_client_until(target_addr, deadline).await
    }

    /// Create new client connection with double-checked locking pattern.
    ///
    /// # Concurrency Strategy
    ///
    /// Uses **double-checked locking** to prevent thundering herd:
    /// 1. **Check 1**: Quick lookup before TCP connect (avoids redundant connects)
    /// 2. **Release lock**: Perform TCP connect WITHOUT holding lock
    /// 3. **Check 2**: Re-acquire lock and verify no other task created connection
    /// 4. **Insert**: Store the new client in the endpoint-session registry
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
    #[cfg(test)]
    async fn create_client(&self, addr: &CheetahString, duration: Duration) -> Option<TransportSession<PR>> {
        match self.create_client_until(addr, RequestDeadline::after(duration)).await {
            Ok(client) => client,
            Err(error) => {
                error!(remote_addr = %addr, error = ?error, "Failed to create remoting client");
                None
            }
        }
    }

    async fn create_client_until(
        &self,
        addr: &CheetahString,
        deadline: RequestDeadline,
    ) -> RocketMQResult<Option<TransportSession<PR>>> {
        deadline.ensure_before_send(addr.to_string())?;
        if let Some(client) = self.connection_tables.get(addr) {
            if client.connection().is_healthy() {
                return Ok(Some(client.value().clone()));
            }
        }

        let (flight, leader) = match self.connect_flights.entry(addr.clone()) {
            dashmap::mapref::entry::Entry::Occupied(entry) => (entry.get().clone(), false),
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let flight = Arc::new(ConnectFlight::new());
                entry.insert(flight.clone());
                (flight, true)
            }
        };
        if leader {
            let client = self.clone();
            let flight_for_task = flight.clone();
            let target = addr.clone();
            let configured_nameserver = self.name_server_endpoint(addr);
            let connect_timeout = self.tokio_client_config.connect.timeout;
            let target_for_task = target.clone();
            let spawned = self.spawn_worker_task(format!("rocketmq.transport.connect.{target}"), async move {
                client.connect_attempts.fetch_add(1, Ordering::Relaxed);
                let result = client
                    .connect_endpoint_until(
                        &target_for_task,
                        configured_nameserver,
                        RequestDeadline::after(connect_timeout),
                    )
                    .await;
                flight_for_task.complete(result);
                client.connect_flights.remove(&target_for_task);
            });
            if spawned.is_none() {
                flight.complete(Err(RocketMQError::ClientNotStarted));
                self.connect_flights.remove(&target);
            }
        }
        flight.wait(deadline, addr).await
    }

    async fn connect_endpoint_until(
        &self,
        addr: &CheetahString,
        configured_nameserver: Option<NameServerEndpoint>,
        deadline: RequestDeadline,
    ) -> RocketMQResult<Option<TransportSession<PR>>> {
        deadline.ensure_before_send(addr.to_string())?;
        // Check if healthy client already exists
        if let Some(client_ref) = self.connection_tables.get(addr) {
            let client = client_ref.value().clone();
            if client.connection().is_healthy() {
                return Ok(Some(client));
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
            return Ok(None);
        }

        let session_target = match configured_nameserver.as_ref() {
            Some(endpoint) => match endpoint.connect_target() {
                Some(target) => SessionConnectTarget::Resolved(target.clone()),
                None => SessionConnectTarget::Legacy(endpoint.compatibility_address().to_string()),
            },
            None => SessionConnectTarget::Legacy(addr.to_string()),
        };
        let tls_config = self.tokio_client_config.tls.clone();
        let frame_limits = self.frame_limits;

        let transport_security = self.transport_security.clone();
        let connect_result = TransportSession::connect_target_with_service_context_until_and_telemetry(
            &self.service_context,
            session_target,
            self.cmd_handler.clone(),
            self.tx.as_ref(),
            tls_config,
            frame_limits,
            deadline,
            self.telemetry.clone(),
        )
        .await;
        let connect_result = match transport_security {
            Some(transport_security) => connect_result.map(|client| client.with_transport_security(transport_security)),
            None => connect_result,
        };

        match connect_result {
            Ok(new_client) => {
                if configured_nameserver.is_some() && self.name_server_endpoint(addr).is_none() {
                    new_client.begin_drain();
                    let _ = new_client.close_with_report(Duration::from_secs(1)).await;
                    return Ok(None);
                }
                // Connection successful - record success in circuit breaker
                breaker.record_success();
                self.circuit_breakers.insert(addr.clone(), breaker);

                match self.connection_tables.entry(addr.clone()) {
                    dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                        // Check if existing is still healthy
                        if entry.get().connection().is_healthy() {
                            info!("Race condition: {} already connected by another task", addr);
                            return Ok(Some(entry.get().clone()));
                        }
                        // Replace unhealthy with new client
                        entry.insert(new_client.clone());
                    }
                    dashmap::mapref::entry::Entry::Vacant(entry) => {
                        entry.insert(new_client.clone());
                    }
                }

                info!("Successfully created client for {}", addr);
                Ok(Some(new_client))
            }
            Err(error) => {
                // Connection failed - record failure in circuit breaker
                error!(remote_addr = %addr, error = ?error, "Failed to connect");
                breaker.record_failure();
                self.circuit_breakers.insert(addr.clone(), breaker);
                Err(error)
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
        let addr_list = self.name_server_identity_list();

        if addr_list.is_empty() {
            debug!("No nameservers configured, skipping availability scan");
            return;
        }

        // Collect addresses to remove (avoid holding borrow during mutation)
        let stale_addrs: Vec<CheetahString> = self
            .available_namesrv_addr_set
            .read()
            .iter()
            .filter(|addr| !addr_list.contains(addr))
            .cloned()
            .collect();

        for stale_addr in stale_addrs {
            warn!("Removing stale nameserver from available set: {}", stale_addr);
            self.available_namesrv_addr_set.write().remove(&stale_addr);
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
                if self.available_namesrv_addr_set.write().insert(namesrv_addr.clone()) {
                    info!("Nameserver {} is now available", namesrv_addr);
                }
            } else {
                // Connection failed - mark as unavailable
                if self.available_namesrv_addr_set.write().remove(&namesrv_addr) {
                    warn!("Nameserver {} is now unavailable", namesrv_addr);
                }
            }
        }

        debug!(
            "Availability scan complete: {}/{} nameservers available",
            self.available_namesrv_addr_set.read().len(),
            addr_list.len()
        );
    }

    /// Scans persistent sessions and removes those that are unhealthy or idle.
    fn scan_idle_connections(&self) {
        let idle_threshold = self.tokio_client_config.maintenance.idle_scan_interval;
        let now_millis = current_millis();
        let mut stale_addrs = Vec::new();

        for entry in self.connection_tables.iter() {
            let addr = entry.key().clone();
            let client = entry.value();

            // Remove connections that are no longer healthy
            if !client.connection().is_healthy() {
                stale_addrs.push(addr);
                continue;
            }

            if idle_threshold.is_some_and(|threshold| client.idle_for_at(now_millis) >= threshold) {
                stale_addrs.push(addr);
            }
        }

        for addr in &stale_addrs {
            if self.connection_tables.remove(addr).is_some() {
                warn!("[SCAN] Removed idle/unhealthy connection: {}", addr);
            }
        }
    }

    pub async fn shutdown_with_report(&self, timeout: Duration) -> ClientShutdownReport {
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
            connections.push(ConnectionShutdownReport { addr, report });
        }

        self.namesrv_endpoints.store(Arc::new(Vec::new()));
        self.namesrv_addr_choosed.write().take();
        self.available_namesrv_addr_set.write().clear();

        ClientShutdownReport {
            background,
            workers,
            connections,
        }
    }
}

#[allow(unused_variables)]
impl<PR: RequestProcessor + Sync + Clone + 'static> TransportClient<PR> {
    pub async fn start(self: &Arc<Self>) -> RocketMQResult<ClientStartReport> {
        let task_group = {
            let mut task_group_guard = self.background_task_group.lock();
            if let Some(task_group) = task_group_guard.as_ref() {
                if task_group.lifecycle_state() == TaskGroupLifecycleState::Open {
                    debug!("TransportClient background tasks are already running");
                    return Ok(ClientStartReport {
                        background_tasks_started: 0,
                        already_running: true,
                    });
                }
            }

            let task_group = self
                .service_context
                .component("rocketmq-transport.client")
                .task_group()
                .clone();
            *task_group_guard = Some(task_group.clone());
            task_group
        };

        let connect_scan_interval = Self::NAMESERVER_SCAN_INTERVAL;
        let token = self.shutdown_token.clone();
        let mut background_tasks_started = 0;

        let client_for_scan = Arc::clone(self);
        let scan_token = token.clone();
        task_group
            .spawn_service("remoting.client.namesrv-scan", async move {
                loop {
                    tokio::select! {
                        () = scan_token.cancelled() => break,
                        () = async {
                            client_for_scan.scan_available_name_srv().await;
                            time::sleep(connect_scan_interval).await;
                        } => {}
                    }
                }
            })
            .map_err(|error| remote_error(format!("failed to spawn nameserver scan task: {error}")))?;
        background_tasks_started += 1;

        if let Some(idle_scan_interval) = self.tokio_client_config.maintenance.idle_scan_interval {
            let idle_token = token.clone();
            let client = Arc::clone(self);
            task_group
                .spawn_service("remoting.client.idle-scan", async move {
                    loop {
                        tokio::select! {
                            () = idle_token.cancelled() => break,
                            () = time::sleep(idle_scan_interval) => {
                                client.scan_idle_connections();
                            }
                        }
                    }
                })
                .map_err(|error| remote_error(format!("failed to spawn idle connection scan task: {error}")))?;
            background_tasks_started += 1;
        }

        Ok(ClientStartReport {
            background_tasks_started,
            already_running: false,
        })
    }

    pub fn shutdown(&self) {
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
        self.connection_tables.clear();
        self.connect_flights.clear();
        self.namesrv_endpoints.store(Arc::new(Vec::new()));
        self.namesrv_addr_choosed.write().take();
        self.available_namesrv_addr_set.write().clear();

        info!("RemotingClient shutdown complete");
    }

    pub fn register_rpc_hook(&self, hook: Arc<dyn RPCHook>) {
        self.cmd_handler.register_rpc_hook(hook);
    }

    pub fn clear_rpc_hook(&self) {
        self.cmd_handler.clear_rpc_hook();
    }
}

impl<PR: RequestProcessor + Sync + Clone + 'static> TransportClient<PR> {
    const MAX_GO_AWAY_ATTEMPTS: usize = 2;

    fn session_cache_identity(
        &self,
        requested_addr: Option<&CheetahString>,
        session: &TransportSession<PR>,
    ) -> CheetahString {
        requested_addr
            .cloned()
            .or_else(|| {
                self.connection_tables
                    .iter()
                    .find(|entry| entry.value().is_same_registry_session(session))
                    .map(|entry| entry.key().clone())
            })
            .or_else(|| self.namesrv_addr_choosed.read().clone())
            .unwrap_or_else(|| CheetahString::from_string(session.remote_address().to_string()))
    }

    fn remove_cached_session_if_matches(&self, identity: &CheetahString, expected: &TransportSession<PR>) -> bool {
        self.connection_tables
            .remove_if(identity, |_, current| current.is_same_registry_session(expected))
            .is_some()
    }

    fn start_go_away_drain(&self, identity: CheetahString, session: TransportSession<PR>) {
        session.begin_drain();
        let drain_timeout = session.max_pending_request_age();
        let task_name = format!("rocketmq.transport.go-away-drain.{identity}");
        let spawned = self.spawn_worker_task(task_name, async move {
            let report = session.drain_and_close(drain_timeout).await;
            if !report.is_healthy() {
                warn!(report = %report.to_json(), "GO_AWAY session drain was unhealthy");
            }
        });
        if spawned.is_none() {
            warn!(%identity, "GO_AWAY session drain could not be scheduled because the client is shutting down");
        }
    }

    fn record_nameserver_outcome(&self, addr: Option<&CheetahString>, latency: Duration, success: bool) {
        let Some(addr) = addr else {
            return;
        };
        if success {
            self.latency_tracker.record_success(addr, latency);
        } else {
            self.latency_tracker.record_error(addr);
        }
    }

    async fn invoke_oneway_until(
        &self,
        addr: &CheetahString,
        request: RemotingCommand,
        deadline: RequestDeadline,
        permit: Option<ResourcePermit>,
    ) -> RocketMQResult<()> {
        self.invoke_oneway_until_inner(addr, request, deadline, permit).await
    }

    async fn invoke_oneway_until_inner(
        &self,
        addr: &CheetahString,
        request: RemotingCommand,
        deadline: RequestDeadline,
        permit: Option<ResourcePermit>,
    ) -> RocketMQResult<()> {
        deadline.ensure_before_send(addr.to_string())?;
        if self.shutdown_token.is_cancelled() {
            return Err(RocketMQError::ClientNotStarted);
        }
        let Some(mut client) = self.get_and_create_client_until(Some(addr), deadline).await? else {
            return Err(RocketMQError::network_connection_failed(
                addr.to_string(),
                "one-way client unavailable",
            ));
        };
        if self.shutdown_token.is_cancelled() {
            return Err(RocketMQError::ClientNotStarted);
        }

        let mut request = request;
        let remote_address = client.remote_address();
        if let Some(hooks) = self.cmd_handler.hook_snapshot() {
            request.make_custom_header_to_net();
            self.cmd_handler.do_before_rpc_hooks_with_snapshot(
                Some(hooks.as_ref()),
                remote_address,
                Some(&mut request),
            )?;
        }
        deadline.ensure_before_send(remote_address.to_string())?;
        request.mark_oneway_rpc_ref();
        match permit {
            Some(permit) => client.send_until_with_permit(request, deadline, permit).await,
            None => client.send_until(request, deadline).await,
        }
    }

    /// Sends a one-way command while transferring an existing process-budget
    /// reservation into the transport writer.
    pub async fn invoke_oneway_with_permit(
        &self,
        addr: &CheetahString,
        request: RemotingCommand,
        deadline: RequestDeadline,
        permit: ResourcePermit,
    ) -> RocketMQResult<()> {
        self.invoke_oneway_until(addr, request, deadline, Some(permit)).await
    }
}

impl<PR: RequestProcessor + Sync + Clone + 'static> TransportClient<PR> {
    pub async fn update_name_server_address_list(&self, addrs: Vec<CheetahString>) {
        self.update_name_server_address_list_sync(addrs);
    }

    pub fn get_name_server_address_list(&self) -> Vec<CheetahString> {
        self.namesrv_endpoints
            .load()
            .iter()
            .map(NameServerEndpoint::compatibility_address)
            .collect()
    }

    pub fn get_available_name_srv_list(&self) -> Vec<CheetahString> {
        self.available_namesrv_addr_set.read().iter().cloned().collect()
    }

    /// Sends one canonical request under an absolute deadline.
    pub async fn request(
        &self,
        target: RequestTarget,
        request: RemotingCommand,
        deadline: RequestDeadline,
    ) -> RocketMQResult<RemotingCommand> {
        match target {
            RequestTarget::Endpoint(endpoint) => {
                self.invoke_request_with_deadline(Some(&endpoint), request, deadline)
                    .await
            }
            RequestTarget::NameServer => self.invoke_request_with_deadline(None, request, deadline).await,
        }
    }

    /// Sends one command and resolves only after the sole writer has completed it.
    pub async fn send_oneway(
        &self,
        target: RequestTarget,
        request: RemotingCommand,
        deadline: RequestDeadline,
    ) -> RocketMQResult<SendReceipt> {
        match target {
            RequestTarget::Endpoint(endpoint) => {
                self.invoke_oneway_until(&endpoint, request, deadline, None).await?;
                Ok(SendReceipt {
                    endpoint,
                    written_at_millis: current_millis(),
                })
            }
            RequestTarget::NameServer => {
                let started_at = time::Instant::now();
                let result = async {
                    deadline.ensure_before_send("<nameserver>")?;
                    let Some(mut client) = self.get_and_create_client_until(None, deadline).await? else {
                        return Err(RocketMQError::network_connection_failed(
                            "<nameserver>",
                            "one-way nameserver client unavailable",
                        ));
                    };
                    let endpoint = CheetahString::from_string(client.remote_address().to_string());
                    let mut request = request;
                    if let Some(hooks) = self.cmd_handler.hook_snapshot() {
                        request.make_custom_header_to_net();
                        self.cmd_handler.do_before_rpc_hooks_with_snapshot(
                            Some(hooks.as_ref()),
                            client.remote_address(),
                            Some(&mut request),
                        )?;
                    }
                    request.mark_oneway_rpc_ref();
                    client.send_until(request, deadline).await?;
                    Ok(SendReceipt {
                        endpoint,
                        written_at_millis: current_millis(),
                    })
                }
                .await;
                let metric_addr = self
                    .namesrv_addr_choosed
                    .read()
                    .clone()
                    .or_else(|| result.as_ref().ok().map(|receipt| receipt.endpoint.clone()));
                self.record_nameserver_outcome(metric_addr.as_ref(), started_at.elapsed(), result.is_ok());
                result
            }
        }
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
    /// # use crate::clients::TransportClient;
    /// # use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    /// # async fn example(client: &TransportClient) -> rocketmq_error::RocketMQResult<()> {
    /// let request = RemotingCommand::create_request_command(/* ... */);
    /// let response = client.invoke_request(
    ///     Some(&"127.0.0.1:10911".into()),
    ///     request,
    ///     3000 // 3 second timeout
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn invoke_request(
        &self,
        addr: Option<&CheetahString>,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        self.invoke_request_with_deadline(addr, request, RequestDeadline::from_timeout_millis(timeout_millis))
            .await
    }

    pub async fn invoke_request_with_deadline(
        &self,
        addr: Option<&CheetahString>,
        request: RemotingCommand,
        deadline: RequestDeadline,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        let nameserver_request = addr.is_none();
        let start = time::Instant::now();
        let timeout_millis = deadline.budget_millis();
        let target = addr.map_or_else(|| "<nameserver>".to_string(), ToString::to_string);
        deadline.ensure_before_send(target.clone())?;

        let mut client = self.get_and_create_client_until(addr, deadline).await?.ok_or_else(|| {
            if target == "<nameserver>" {
                let configured_list = self.name_server_identity_list();
                let available_set = self.available_namesrv_addr_set.read().clone();
                let cached_choice = self.namesrv_addr_choosed.read().clone();
                error!(
                    "Failed to get client for <nameserver>. Diagnostics: configured_list={:?}, available_set={:?}, \
                     cached_choice={:?}, connections={}",
                    configured_list,
                    available_set,
                    cached_choice,
                    self.connection_tables.len()
                );
            } else {
                error!("Failed to get client for {}", target);
            }

            if nameserver_request {
                if let Some(addr) = self.namesrv_addr_choosed.read().as_ref() {
                    self.latency_tracker.record_error(addr);
                }
            }

            RocketMQError::network_connection_failed(target.clone(), "Failed to connect")
        })?;

        if self.shutdown_token.is_cancelled() {
            return Err(RocketMQError::ClientNotStarted);
        }

        let mut request = request;
        let initial_remote_address = client.remote_address();
        let initial_identity = self.session_cache_identity(addr, &client);
        let nameserver_metric_addr = nameserver_request.then(|| initial_identity.clone());
        deadline.ensure_before_send(initial_remote_address.to_string())?;
        let hooks = self.cmd_handler.hook_snapshot();
        let request_for_after = if let Some(hooks) = hooks {
            request.make_custom_header_to_net();
            self.cmd_handler.do_before_rpc_hooks_with_snapshot(
                Some(hooks.as_ref()),
                initial_remote_address,
                Some(&mut request),
            )?;
            deadline.ensure_before_send(initial_remote_address.to_string())?;
            Some((request.clone(), hooks))
        } else {
            None
        };
        let apply_final_hooks =
            |mut response: RemotingCommand, remote_address: std::net::SocketAddr| -> RocketMQResult<RemotingCommand> {
                if let Some((request, hooks)) = request_for_after.as_ref() {
                    self.cmd_handler.do_after_rpc_hooks_with_snapshot(
                        Some(hooks.as_ref()),
                        remote_address,
                        request,
                        Some(&mut response),
                    )?;
                }
                if deadline.is_expired() {
                    return Err(RocketMQError::network_response_timeout(
                        remote_address.to_string(),
                        timeout_millis,
                    ));
                }
                Ok(response)
            };
        let retry_allowed = self.go_away_policy.allows_request(request.code()) && !request.is_oneway_rpc();
        let retry_request = request.clone();
        let mut attempted_retry = false;

        for attempt in 0..Self::MAX_GO_AWAY_ATTEMPTS {
            let remote_address = client.remote_address();
            let identity = self.session_cache_identity(addr, &client);
            let mut attempt_request = retry_request.clone();
            if attempt > 0 {
                attempt_request.set_opaque_mut(RemotingCommand::get_and_add());
            }

            match client.send_read(attempt_request, deadline).await {
                Ok(response) if response.code() == ResponseCode::GoAway.to_i32() => {
                    self.telemetry.record_go_away(TransportGoAwayOutcome::Received);
                    if !retry_allowed {
                        let response = apply_final_hooks(response, remote_address)?;
                        let latency = start.elapsed();
                        self.record_nameserver_outcome(nameserver_metric_addr.as_ref(), latency, true);
                        debug!(
                            remote_addr = %identity,
                            elapsed_ms = latency.as_millis() as u64,
                            "request completed with GO_AWAY retry disabled"
                        );
                        return Ok(response);
                    }
                    self.remove_cached_session_if_matches(&identity, &client);
                    self.start_go_away_drain(identity.clone(), client);

                    if attempt + 1 == Self::MAX_GO_AWAY_ATTEMPTS {
                        self.telemetry.record_go_away(TransportGoAwayOutcome::RetryFailed);
                        self.record_nameserver_outcome(nameserver_metric_addr.as_ref(), start.elapsed(), false);
                        return Err(RpcClientError::unexpected_response_code(
                            response.code(),
                            "GO_AWAY after replacement-connection retry",
                        )
                        .into());
                    }

                    attempted_retry = true;
                    if let Err(error) = deadline.ensure_before_send(identity.to_string()) {
                        self.telemetry.record_go_away(TransportGoAwayOutcome::RetryFailed);
                        self.record_nameserver_outcome(nameserver_metric_addr.as_ref(), start.elapsed(), false);
                        return Err(error);
                    }
                    let replacement = match self.get_and_create_client_until(addr, deadline).await {
                        Ok(Some(replacement)) => Ok(replacement),
                        Ok(None) => Err(RocketMQError::network_connection_failed(
                            identity.to_string(),
                            "GO_AWAY replacement connection unavailable",
                        )),
                        Err(error) => Err(error),
                    };
                    client = match replacement {
                        Ok(replacement) => replacement,
                        Err(error) => {
                            self.telemetry.record_go_away(TransportGoAwayOutcome::RetryFailed);
                            self.record_nameserver_outcome(nameserver_metric_addr.as_ref(), start.elapsed(), false);
                            return Err(error);
                        }
                    };
                }
                Ok(response) => {
                    let response = match apply_final_hooks(response, remote_address) {
                        Ok(response) => response,
                        Err(error) => {
                            if attempted_retry {
                                self.telemetry.record_go_away(TransportGoAwayOutcome::RetryFailed);
                            }
                            self.record_nameserver_outcome(nameserver_metric_addr.as_ref(), start.elapsed(), false);
                            return Err(error);
                        }
                    };
                    if attempted_retry {
                        self.telemetry.record_go_away(TransportGoAwayOutcome::RetrySuccess);
                    }
                    let latency = start.elapsed();
                    self.record_nameserver_outcome(nameserver_metric_addr.as_ref(), latency, true);
                    debug!(
                        remote_addr = %identity,
                        elapsed_ms = latency.as_millis() as u64,
                        "request completed"
                    );
                    return Ok(response);
                }
                Err(error) => {
                    if matches!(
                        error,
                        RocketMQError::Network(
                            NetworkError::WriteTimeout { .. } | NetworkError::ResponseTimeout { .. }
                        )
                    ) {
                        client.retire_after_timeout().await;
                        self.remove_cached_session_if_matches(&identity, &client);
                    }
                    if attempted_retry {
                        self.telemetry.record_go_away(TransportGoAwayOutcome::RetryFailed);
                    }
                    let latency = start.elapsed();
                    self.record_nameserver_outcome(nameserver_metric_addr.as_ref(), latency, false);
                    warn!(
                        remote_addr = %identity,
                        elapsed_ms = latency.as_millis() as u64,
                        error = ?error,
                        "request failed"
                    );
                    return Err(error);
                }
            }
        }

        unreachable!("GO_AWAY attempt loop has a fixed non-zero bound")
    }

    pub async fn invoke_request_oneway_with_deadline(
        &self,
        addr: &CheetahString,
        request: RemotingCommand,
        deadline: RequestDeadline,
    ) -> RocketMQResult<()> {
        self.invoke_oneway_until(addr, request, deadline, None).await
    }

    pub async fn invoke_request_oneway(
        &self,
        addr: &CheetahString,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        self.invoke_request_oneway_with_deadline(addr, request, RequestDeadline::from_timeout_millis(timeout_millis))
            .await
    }

    pub fn is_address_reachable(&self, addr: &CheetahString) {
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

    pub fn close_clients(&self, addrs: Vec<String>) {
        for addr in &addrs {
            let key = CheetahString::from(addr.as_str());
            if let Some((_, _client)) = self.connection_tables.remove(&key) {
                info!("Closed client connection for {}", addr);
            }
        }
    }

    pub fn register_processor(&self, processor: impl RequestProcessor + Sync) {
        let _ = &processor;
        warn!("dynamic request processor registration is not supported by TransportClient after construction");
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
    use crate::connection::Connection;
    use crate::request_processor::default_request_processor::DefaultRequestProcessor;
    use crate::runtime::config::client_config::TransportClientConfig;
    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_protocol::code::response_code::ResponseCode;

    fn test_service_context(name: &'static str) -> ChildServiceContext {
        RuntimeContext::from_current(name).service_context("remoting-client-service")
    }

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

    #[tokio::test]
    async fn is_use_tls_reflects_client_config() {
        let config = TransportClientConfig {
            tls: TlsConfig {
                enable: true,
                ..TlsConfig::default()
            },
            ..Default::default()
        };
        let client = TransportClient::build_for_test(
            Arc::new(config),
            DefaultRequestProcessor,
            test_service_context("remoting-client-tls-test"),
        );

        assert!(client.is_use_tls());
        assert!(client.tls_config().enable);
    }

    #[tokio::test]
    async fn start_tracks_background_tasks_with_task_group() {
        let config = TransportClientConfig {
            connect: ConnectConfig {
                timeout: Duration::from_millis(10),
            },
            maintenance: MaintenanceConfig {
                idle_scan_interval: Some(Duration::from_millis(10)),
            },
            ..Default::default()
        };
        let client = Arc::new(TransportClient::build_for_test(
            Arc::new(config),
            DefaultRequestProcessor,
            test_service_context("remoting-client-background-test"),
        ));
        client.start().await.expect("start client background tasks");

        let task_group = client
            .background_task_group
            .lock()
            .as_ref()
            .cloned()
            .expect("background task group");
        assert_eq!(task_group.lifecycle_state(), TaskGroupLifecycleState::Open);
        assert_eq!(task_group.task_count(), 2);

        let repeated = client.start().await.expect("repeat client start");
        assert!(repeated.already_running);
        let repeated_start_group = client
            .background_task_group
            .lock()
            .as_ref()
            .cloned()
            .expect("background task group after repeated start");
        assert_eq!(repeated_start_group.id(), task_group.id());
        assert_eq!(repeated_start_group.task_count(), 2);

        client.shutdown();

        assert_eq!(task_group.lifecycle_state(), TaskGroupLifecycleState::ShutdownCompleted);
    }

    #[test]
    fn nameserver_scan_interval_is_independent_from_connect_timeout() {
        let config = TransportClientConfig {
            connect: ConnectConfig {
                timeout: Duration::from_millis(7),
            },
            ..Default::default()
        };

        assert_ne!(
            TransportClient::<DefaultRequestProcessor>::NAMESERVER_SCAN_INTERVAL,
            config.connect.timeout
        );
        assert_eq!(
            TransportClient::<DefaultRequestProcessor>::NAMESERVER_SCAN_INTERVAL,
            Duration::from_secs(30)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_nameserver_updates_publish_complete_owned_snapshots() {
        let client = Arc::new(TransportClient::build_for_test(
            Arc::new(TransportClientConfig::default()),
            DefaultRequestProcessor,
            test_service_context("remoting-client-update-test"),
        ));
        let first = client.clone();
        let second = client.clone();

        let first_updates = tokio::spawn(async move {
            for _ in 0..64 {
                first.update_name_server_address_list_sync(vec!["ns-a:9876".into(), "ns-b:9876".into()]);
                tokio::task::yield_now().await;
            }
        });
        let second_updates = tokio::spawn(async move {
            for _ in 0..64 {
                second.update_name_server_address_list_sync(vec!["ns-c:9876".into(), "ns-d:9876".into()]);
                tokio::task::yield_now().await;
            }
        });

        for _ in 0..128 {
            let snapshot = client.get_name_server_address_list();
            assert!(snapshot.is_empty() || snapshot.len() == 2);
            if let Some(first) = snapshot.first() {
                let first_group = first.as_str().starts_with("ns-a") || first.as_str().starts_with("ns-b");
                assert!(snapshot.iter().all(|address| {
                    let address = address.as_str();
                    if first_group {
                        address.starts_with("ns-a") || address.starts_with("ns-b")
                    } else {
                        address.starts_with("ns-c") || address.starts_with("ns-d")
                    }
                }));
            }
            tokio::task::yield_now().await;
        }

        first_updates.await.expect("first updater should finish");
        second_updates.await.expect("second updater should finish");
        assert_eq!(client.get_name_server_address_list().len(), 2);
        client.shutdown();
    }

    #[tokio::test]
    async fn service_context_parents_background_and_worker_tasks() {
        let context = RuntimeContext::from_current("remoting-default-client-parent-test");
        let service = context.service_context("remoting-client-service");
        let config = TransportClientConfig {
            connect: ConnectConfig {
                timeout: Duration::from_millis(10),
            },
            maintenance: MaintenanceConfig {
                idle_scan_interval: Some(Duration::from_millis(10)),
            },
            ..Default::default()
        };
        let client = Arc::new(TransportClient::build_for_test(
            Arc::new(config),
            DefaultRequestProcessor,
            service.clone(),
        ));
        client.start().await.expect("start client background tasks");
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

        client.shutdown();
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
        let client = TransportClient::build_for_test(
            Arc::new(TransportClientConfig::default()),
            DefaultRequestProcessor,
            test_service_context("remoting-client-hook-test"),
        );
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cold_endpoint_burst_uses_one_lifecycle_owned_connect_flight() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept client");
            let mut connection = Connection::new(socket);
            for _ in 0..32 {
                let request = connection
                    .receive_command()
                    .await
                    .expect("request frame")
                    .expect("request command");
                let mut response = RemotingCommand::create_response_command_with_code(ResponseCode::Success);
                response.set_opaque_mut(request.opaque());
                connection.send_command(response).await.expect("send response");
            }
        });
        let client = Arc::new(TransportClient::build_for_test(
            Arc::new(TransportClientConfig::default()),
            DefaultRequestProcessor,
            test_service_context("remoting-client-singleflight-test"),
        ));
        let target = CheetahString::from_string(addr.to_string());
        let requests = (0..32).map(|opaque| {
            let client = client.clone();
            let target = target.clone();
            async move {
                client
                    .invoke_request(
                        Some(&target),
                        RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo).set_opaque(opaque),
                        3_000,
                    )
                    .await
            }
        });

        let responses = tokio::time::timeout(Duration::from_secs(5), futures::future::join_all(requests))
            .await
            .expect("burst deadline");
        assert!(responses.into_iter().all(|response| response.is_ok()));
        assert_eq!(client.connect_attempts.load(Ordering::Relaxed), 1);
        assert_eq!(client.connection_tables.len(), 1);

        server.await.expect("server task");
        client.shutdown();
    }

    #[tokio::test]
    async fn timed_out_request_retires_the_cached_session_before_the_next_request() {
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

        let client = TransportClient::build_for_test(
            Arc::new(TransportClientConfig::default()),
            DefaultRequestProcessor,
            test_service_context("remoting-client-timeout-test"),
        );
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
    async fn registry_token_distinguishes_replacements_and_same_port_nameserver_identities() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (first, _) = listener.accept().await.expect("accept first client");
            let (second, _) = listener.accept().await.expect("accept replacement client");
            let _connections = (first, second);
            let _ = release_rx.await;
        });

        let client = TransportClient::build_for_test(
            Arc::new(TransportClientConfig::default()),
            DefaultRequestProcessor,
            test_service_context("remoting-client-registry-token-test"),
        );
        let target = CheetahString::from_string(addr.to_string());
        let first = client
            .create_client(&target, Duration::from_secs(1))
            .await
            .expect("first client");
        client.connection_tables.remove(&target);
        let replacement = client
            .create_client(&target, Duration::from_secs(1))
            .await
            .expect("replacement client");
        client.connection_tables.remove(&target);

        let first_identity = CheetahString::from_static_str("nameserver-a:9876");
        let replacement_identity = CheetahString::from_static_str("nameserver-b:9876");
        client.connection_tables.insert(first_identity.clone(), first.clone());
        client
            .connection_tables
            .insert(replacement_identity.clone(), replacement.clone());

        assert_eq!(client.session_cache_identity(None, &first), first_identity);
        assert_eq!(client.session_cache_identity(None, &replacement), replacement_identity);
        assert!(!client.remove_cached_session_if_matches(&replacement_identity, &first));
        assert!(client.connection_tables.contains_key(&replacement_identity));
        assert!(client.remove_cached_session_if_matches(&first_identity, &first));
        assert!(client.connection_tables.contains_key(&replacement_identity));

        client.shutdown();
        let _ = release_tx.send(());
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn invoke_oneway_waits_for_writer_completion() {
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
        let client = TransportClient::build_for_test(
            Arc::new(TransportClientConfig::default()),
            DefaultRequestProcessor,
            test_service_context("remoting-client-oneway-test"),
        );
        client.register_rpc_hook(hook.clone());

        let target = CheetahString::from_string(addr.to_string());
        let request = RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo);
        client
            .invoke_request_oneway(&target, request, 3_000)
            .await
            .expect("one-way send should complete");

        let (code, is_oneway, hooked) = time::timeout(Duration::from_secs(3), received_rx)
            .await
            .expect("server should receive oneway request")
            .expect("server should report received request");

        assert_eq!(code, RequestCode::GetBrokerClusterInfo.to_i32());
        assert!(is_oneway);
        assert!(hooked);
        assert_eq!(hook.before_count.load(Ordering::SeqCst), 1);
        assert_eq!(hook.after_count.load(Ordering::SeqCst), 0);

        server.await.expect("server task");
        client.shutdown();
    }

    #[tokio::test]
    async fn explicit_broker_requests_do_not_update_nameserver_latency() {
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
            connection
                .send_command(
                    RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                        .set_opaque(request.opaque()),
                )
                .await
                .expect("send response");
            connection
                .receive_command()
                .await
                .expect("oneway frame")
                .expect("oneway command");
        });
        let client = TransportClient::build_for_test(
            Arc::new(TransportClientConfig::default()),
            DefaultRequestProcessor,
            test_service_context("explicit-broker-latency-test"),
        );
        let target = CheetahString::from_string(addr.to_string());

        client
            .invoke_request(
                Some(&target),
                RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
                3_000,
            )
            .await
            .expect("explicit Broker request");
        client
            .invoke_request_oneway(
                &target,
                RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
                3_000,
            )
            .await
            .expect("explicit Broker oneway");

        assert_eq!(client.latency_tracker.get_p99(&target), None);
        assert_eq!(client.latency_tracker.get_error_count(&target), 0);
        server.await.expect("server task");
        client.shutdown();
    }

    #[tokio::test]
    async fn nameserver_request_updates_latency_and_failover_state() {
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
            connection
                .send_command(
                    RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                        .set_opaque(request.opaque()),
                )
                .await
                .expect("send response");
        });
        let client = TransportClient::build_for_test(
            Arc::new(TransportClientConfig::default()),
            DefaultRequestProcessor,
            test_service_context("nameserver-latency-test"),
        );
        let target = CheetahString::from_string(addr.to_string());
        client.update_name_server_address_list_sync(vec![target.clone()]);

        client
            .invoke_request(
                None,
                RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
                3_000,
            )
            .await
            .expect("NameServer request");

        assert!(client.latency_tracker.get_p99(&target).is_some());
        assert_eq!(client.latency_tracker.get_error_count(&target), 0);
        server.await.expect("server task");
        client.shutdown();
    }

    #[tokio::test]
    async fn unchanged_resolved_endpoint_reuses_connection_and_identity_state() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let socket_addr = listener.local_addr().expect("listener addr");
        let (release_server, server_release) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept client");
            let mut connection = Connection::new(socket);
            for _ in 0..2 {
                let request = connection
                    .receive_command()
                    .await
                    .expect("request frame")
                    .expect("request command");
                connection
                    .send_command(
                        RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                            .set_opaque(request.opaque()),
                    )
                    .await
                    .expect("send response");
            }
            server_release.await.expect("release server connection");
        });
        let client = TransportClient::build_for_test(
            Arc::new(TransportClientConfig::default()),
            DefaultRequestProcessor,
            test_service_context("nameserver-unchanged-endpoint-test"),
        );
        let target = ConnectTarget::new(socket_addr, "namesrv.default.svc:9876").unwrap();
        let identity = target.identity();
        client.update_name_server_connect_targets_sync(vec![target.clone()], Duration::from_secs(1));

        for _ in 0..2 {
            client
                .invoke_request(
                    None,
                    RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
                    3_000,
                )
                .await
                .expect("NameServer request");
            client.update_name_server_connect_targets_sync(vec![target.clone()], Duration::from_secs(1));
        }

        assert_eq!(client.connect_attempts.load(Ordering::Relaxed), 1);
        assert!(client.connection_tables.contains_key(&identity));
        assert!(client.latency_tracker.get_p99(&identity).is_some());
        let snapshot = client.snapshot();
        assert_eq!(snapshot.healthy_name_server_count, 1);
        assert_eq!(snapshot.probing_name_server_count, 0);
        assert_eq!(snapshot.draining_name_server_count, 0);
        assert_eq!(snapshot.circuit_open_name_server_count, 0);
        release_server.send(()).expect("release server");
        server.await.expect("server task");
        client.shutdown();
    }

    #[tokio::test]
    async fn same_socket_with_new_authority_does_not_reuse_the_old_session() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let socket_addr = listener.local_addr().expect("listener addr");
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (socket, _) = listener.accept().await.expect("accept client");
                let mut connection = Connection::new(socket);
                let request = connection
                    .receive_command()
                    .await
                    .expect("request frame")
                    .expect("request command");
                connection
                    .send_command(
                        RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                            .set_opaque(request.opaque()),
                    )
                    .await
                    .expect("send response");
            }
        });
        let client = TransportClient::build_for_test(
            Arc::new(TransportClientConfig::default()),
            DefaultRequestProcessor,
            test_service_context("nameserver-authority-isolation-test"),
        );
        let first = ConnectTarget::new(socket_addr, "namesrv-a.default.svc:9876").unwrap();
        let second = ConnectTarget::new(socket_addr, "namesrv-b.default.svc:9876").unwrap();
        client.update_name_server_connect_targets_sync(vec![first.clone()], Duration::from_secs(1));
        client
            .invoke_request(
                None,
                RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
                3_000,
            )
            .await
            .expect("first authority request");

        client.update_name_server_connect_targets_sync(vec![second.clone()], Duration::from_secs(1));
        client
            .invoke_request(
                None,
                RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
                3_000,
            )
            .await
            .expect("second authority request");

        assert_eq!(client.connect_attempts.load(Ordering::Relaxed), 2);
        assert!(!client.connection_tables.contains_key(&first.identity()));
        assert!(client.connection_tables.contains_key(&second.identity()));
        assert!(client.latency_tracker.get_p99(&second.identity()).is_some());
        server.await.expect("server task");
        client.shutdown();
    }

    #[tokio::test]
    async fn removed_endpoint_rejects_new_work_and_closes_after_drain_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let socket_addr = listener.local_addr().expect("listener addr");
        let (request_received, request_started) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept client");
            let mut connection = Connection::new(socket);
            connection
                .receive_command()
                .await
                .expect("request frame")
                .expect("request command");
            request_received.send(()).expect("signal request");
            let trailing = time::timeout(Duration::from_secs(1), connection.receive_command())
                .await
                .expect("drain timeout should close the socket");
            assert!(trailing.is_none(), "drained connection should reach EOF");
        });
        let client = Arc::new(TransportClient::build_for_test(
            Arc::new(TransportClientConfig::default()),
            DefaultRequestProcessor,
            test_service_context("nameserver-drain-timeout-test"),
        ));
        let target = ConnectTarget::new(socket_addr, "namesrv.default.svc:9876").unwrap();
        let identity = target.identity();
        client.update_name_server_connect_targets_sync(vec![target], Duration::from_millis(25));
        let request_client = client.clone();
        let request = tokio::spawn(async move {
            request_client
                .invoke_request(
                    None,
                    RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
                    3_000,
                )
                .await
        });
        request_started.await.expect("request should reach server");

        client.update_name_server_connect_targets_sync(Vec::new(), Duration::from_millis(25));

        assert!(client.get_name_server_address_list().is_empty());
        assert!(!client.connection_tables.contains_key(&identity));
        assert!(time::timeout(Duration::from_secs(1), request)
            .await
            .unwrap()
            .unwrap()
            .is_err());
        server.await.expect("server task");
        let report = client.shutdown_with_report(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{report:?}");
    }

    #[tokio::test]
    async fn nameserver_connect_failure_tries_next_candidate_within_deadline() {
        let closed_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind closed endpoint");
        let closed_addr = closed_listener.local_addr().expect("closed endpoint address");
        drop(closed_listener);

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind healthy endpoint");
        let healthy_addr = listener.local_addr().expect("healthy endpoint address");
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept fallback client");
            let mut connection = Connection::new(socket);
            let request = connection
                .receive_command()
                .await
                .expect("request frame")
                .expect("request command");
            connection
                .send_command(
                    RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                        .set_opaque(request.opaque()),
                )
                .await
                .expect("send response");
        });

        let client = TransportClient::build_for_test(
            Arc::new(TransportClientConfig::default()),
            DefaultRequestProcessor,
            test_service_context("nameserver-connect-failover-test"),
        );
        client.apply_name_server_endpoint_snapshot_sync(
            vec![
                NameServerEndpoint::legacy(closed_addr.to_string()).unwrap(),
                NameServerEndpoint::legacy(healthy_addr.to_string()).unwrap(),
            ],
            Duration::from_secs(30),
        );

        let response = client
            .invoke_request(
                None,
                RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
                3_000,
            )
            .await
            .expect("second NameServer should satisfy the request");

        assert_eq!(response.code(), ResponseCode::Success.to_i32());
        server.await.expect("fallback server task");
        client.shutdown();
    }

    #[tokio::test]
    async fn request_failure_after_write_does_not_retry_another_nameserver() {
        let first_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind first endpoint");
        let first_addr = first_listener.local_addr().expect("first endpoint address");
        let second_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind second endpoint");
        let second_addr = second_listener.local_addr().expect("second endpoint address");
        let first_server = tokio::spawn(async move {
            let (socket, _) = first_listener.accept().await.expect("accept first client");
            let mut connection = Connection::new(socket);
            connection
                .receive_command()
                .await
                .expect("request frame")
                .expect("request command");
        });

        let client = TransportClient::build_for_test(
            Arc::new(TransportClientConfig::default()),
            DefaultRequestProcessor,
            test_service_context("nameserver-no-write-retry-test"),
        );
        client.apply_name_server_endpoint_snapshot_sync(
            vec![
                NameServerEndpoint::legacy(first_addr.to_string()).unwrap(),
                NameServerEndpoint::legacy(second_addr.to_string()).unwrap(),
            ],
            Duration::from_secs(30),
        );

        assert!(client
            .invoke_request(
                None,
                RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
                1_000,
            )
            .await
            .is_err());
        assert!(
            time::timeout(Duration::from_millis(100), second_listener.accept())
                .await
                .is_err(),
            "the second NameServer must not receive a replay after request bytes were written"
        );

        first_server.await.expect("first server task");
        client.shutdown();
    }

    #[tokio::test]
    async fn shutdown_with_report_closes_connection_table_clients() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.expect("accept client");
            time::sleep(Duration::from_secs(5)).await;
        });
        let client = TransportClient::build_for_test(
            Arc::new(TransportClientConfig::default()),
            DefaultRequestProcessor,
            test_service_context("remoting-client-shutdown-test"),
        );

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

    #[tokio::test]
    async fn idle_scan_evicts_an_expired_persistent_session() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.expect("accept client");
            time::sleep(Duration::from_secs(5)).await;
        });
        let config = TransportClientConfig {
            maintenance: MaintenanceConfig {
                idle_scan_interval: Some(Duration::from_millis(1)),
            },
            ..TransportClientConfig::default()
        };
        let client = TransportClient::build_for_test(
            Arc::new(config),
            DefaultRequestProcessor,
            test_service_context("remoting-client-idle-eviction-test"),
        );
        let target = CheetahString::from_string(addr.to_string());
        let created = client
            .create_client(&target, Duration::from_secs(3))
            .await
            .expect("client connection should be created");
        created.set_last_used_millis_for_test(0);
        drop(created);

        client.scan_idle_connections();

        assert!(client.connection_tables.is_empty());
        client.shutdown();
        server.abort();
    }
}
