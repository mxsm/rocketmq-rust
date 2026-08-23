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

use std::ops::Deref;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_error::NetworkError;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_error::RpcClientError;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::ResourcePermit;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReport;
#[cfg(test)]
use rocketmq_runtime::TaskGroup;
#[cfg(test)]
use rocketmq_runtime::TaskGroupLifecycleState;
use serde::Serialize;
use tokio::time;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::connection_net_event::ConnectionNetEvent;
use crate::base::pending_request_table::PendingRequestTable;
use crate::base::pending_request_table::PendingRequestUsage;
use crate::clients::client::SessionConnectTarget;
use crate::clients::nameserver_endpoint::ConnectTarget;
use crate::clients::nameserver_endpoint::NameServerEndpoint;
use crate::clients::TransportSession;
use crate::codec::remoting_command_codec::FrameLimits;
use crate::deadline::RequestDeadline;
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
use crate::telemetry::TransportTelemetry;
use crate::tls::TlsConfig;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

mod connection_registry;
mod endpoint_state;
mod lifecycle;
mod nameserver;

use connection_registry::ConnectionRegistry;
use endpoint_state::EndpointLease;
use endpoint_state::EndpointStateStore;
use lifecycle::ClientLifecycle;
use lifecycle::ConnectionCommitFence;
use nameserver::NameServerHealth;

#[cfg(test)]
use lifecycle::LifecycleTestBarrier;

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct EndpointCompletionTestHook {
    entered: Arc<std::sync::atomic::AtomicBool>,
    entered_signal: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
impl EndpointCompletionTestHook {
    pub(crate) fn new() -> Self {
        Self {
            entered: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            entered_signal: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
        }
    }

    pub(crate) async fn wait_until_entered(&self) {
        while !self.entered.load(Ordering::Acquire) {
            self.entered_signal.notified().await;
        }
    }

    pub(crate) fn release(&self) {
        self.release.notify_one();
    }
}

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
    connection_registry: Arc<ConnectionRegistry<PR>>,
    connect_attempts: Arc<AtomicU64>,
    namesrv_draining_count: Arc<AtomicUsize>,

    /// List of all nameserver addresses (in priority order)
    ///
    /// Updated via `update_name_server_address_list()`
    endpoint_state: Arc<EndpointStateStore>,
    nameserver_health: Arc<NameServerHealth>,
    direct_circuit_breakers: Arc<dashmap::DashMap<CheetahString, crate::clients::reconnect::CircuitBreaker>>,
    #[cfg(test)]
    connect_completion_test_hook: Arc<Mutex<Option<EndpointCompletionTestHook>>>,
    #[cfg(test)]
    leader_before_worker_spawn_test_hook: Arc<Mutex<Option<LifecycleTestBarrier>>>,

    /// Serializes task ownership, startup, and shutdown generations.
    lifecycle: Arc<Mutex<ClientLifecycle>>,

    // Existing in-module tests inspect these task groups. Production ownership
    // stays exclusively inside `lifecycle`.
    #[cfg(test)]
    background_task_group: Arc<Mutex<Option<TaskGroup>>>,
    #[cfg(test)]
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

    /// Gracefully shuts down the canonical transport by the caller's absolute deadline.
    ///
    /// This forwards the same deadline without converting it to a new duration,
    /// so nested lifecycle owners share one drain budget.
    pub async fn shutdown_until(&self, deadline: ShutdownDeadline) -> RocketMQResult<ClientShutdownReport> {
        Ok(self.transport.shutdown_graceful(deadline).await)
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
            connection_registry: self.connection_registry.clone(),
            connect_attempts: self.connect_attempts.clone(),
            namesrv_draining_count: self.namesrv_draining_count.clone(),
            endpoint_state: self.endpoint_state.clone(),
            nameserver_health: self.nameserver_health.clone(),
            direct_circuit_breakers: self.direct_circuit_breakers.clone(),
            #[cfg(test)]
            connect_completion_test_hook: self.connect_completion_test_hook.clone(),
            #[cfg(test)]
            leader_before_worker_spawn_test_hook: self.leader_before_worker_spawn_test_hook.clone(),
            lifecycle: self.lifecycle.clone(),
            #[cfg(test)]
            background_task_group: self.background_task_group.clone(),
            #[cfg(test)]
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
            connection_registry: Arc::new(ConnectionRegistry::new()),
            connect_attempts: Arc::new(AtomicU64::new(0)),
            namesrv_draining_count: Arc::new(AtomicUsize::new(0)),
            endpoint_state: Arc::new(EndpointStateStore::new()),
            nameserver_health: Arc::new(NameServerHealth::new()),
            direct_circuit_breakers: Arc::new(dashmap::DashMap::with_capacity(64)),
            #[cfg(test)]
            connect_completion_test_hook: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            leader_before_worker_spawn_test_hook: Arc::new(Mutex::new(None)),
            lifecycle: Arc::new(Mutex::new(ClientLifecycle::new())),
            #[cfg(test)]
            background_task_group: Arc::new(Mutex::new(None)),
            #[cfg(test)]
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
        let state = self.endpoint_state.load();
        let healthy_name_server_count = state
            .endpoints()
            .iter()
            .filter(|endpoint| {
                state.lease_for(endpoint.identity()).is_some_and(|lease| {
                    self.connection_registry
                        .healthy_session(endpoint.identity(), Some(&lease))
                        .is_some_and(|session| session.connection().is_healthy())
                        && self.nameserver_health.is_healthy(endpoint.identity(), &lease)
                })
            })
            .count();
        let (probing_name_server_count, circuit_open_name_server_count) = state
            .endpoints()
            .iter()
            .filter_map(|endpoint| {
                state
                    .lease_for(endpoint.identity())
                    .and_then(|lease| self.nameserver_health.circuit_state(endpoint.identity(), &lease))
            })
            .fold((0, 0), |(probing, open), state| match state {
                crate::clients::reconnect::CircuitState::Closed => (probing, open),
                crate::clients::reconnect::CircuitState::HalfOpen => (probing + 1, open),
                crate::clients::reconnect::CircuitState::Open => (probing, open + 1),
            });
        ClientSnapshot {
            connection_count: self.connection_registry.len(),
            connect_flight_count: self.connection_registry.flight_count(),
            configured_name_server_count: state.endpoints().len(),
            available_name_server_count: state.available().len(),
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
        let endpoints = Self::name_server_connect_targets_to_endpoints(targets);
        self.apply_name_server_endpoint_snapshot_sync(endpoints, drain_timeout);
    }

    /// Atomically publishes a complete selector snapshot.
    pub fn apply_name_server_endpoint_snapshot_sync(
        &self,
        endpoints: Vec<NameServerEndpoint>,
        drain_timeout: Duration,
    ) {
        self.apply_name_server_endpoint_snapshot(endpoints, drain_timeout);
    }

    #[cfg(test)]
    pub(crate) fn install_connect_completion_test_hook(&self, hook: EndpointCompletionTestHook) {
        *self.connect_completion_test_hook.lock() = Some(hook);
    }

    #[cfg(test)]
    fn install_leader_before_worker_spawn_test_hook(&self, hook: LifecycleTestBarrier) {
        *self.leader_before_worker_spawn_test_hook.lock() = Some(hook);
    }

    #[cfg(test)]
    async fn wait_for_connect_completion_test_hook(&self) {
        let hook = self.connect_completion_test_hook.lock().clone();
        if let Some(hook) = hook {
            hook.entered.store(true, Ordering::Release);
            hook.entered_signal.notify_waiters();
            hook.release.notified().await;
        }
    }

    #[cfg(test)]
    async fn wait_for_leader_before_worker_spawn_test_hook(&self) {
        let hook = self.leader_before_worker_spawn_test_hook.lock().clone();
        if let Some(hook) = hook {
            hook.pause().await;
        }
    }
}

impl<PR: RequestProcessor + Sync + Clone + 'static> TransportClient<PR> {
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
    async fn get_and_create_client_until(
        &self,
        addr: Option<&CheetahString>,
        deadline: RequestDeadline,
    ) -> RocketMQResult<Option<TransportSession<PR>>> {
        // Route empty addresses to nameserver
        let target_addr = match addr {
            None => {
                return self
                    .get_and_create_nameserver_client_until(deadline)
                    .await
                    .map(|selection| selection.map(|selection| selection.session))
            }
            Some(addr) if addr.is_empty() => {
                return self
                    .get_and_create_nameserver_client_until(deadline)
                    .await
                    .map(|selection| selection.map(|selection| selection.session))
            }
            Some(addr) => addr,
        };
        deadline.ensure_before_send(target_addr.to_string())?;

        if let Some(client) = self.connection_registry.healthy_session(target_addr, None) {
            return Ok(Some(client));
        }
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
        self.create_client_with_lease_until(addr, None, None, deadline).await
    }

    async fn create_client_for_nameserver_until(
        &self,
        addr: &CheetahString,
        endpoint: NameServerEndpoint,
        lease: EndpointLease,
        deadline: RequestDeadline,
    ) -> RocketMQResult<Option<TransportSession<PR>>> {
        self.create_client_with_lease_until(addr, Some(endpoint), Some(lease), deadline)
            .await
    }

    async fn create_client_with_lease_until(
        &self,
        addr: &CheetahString,
        configured_nameserver: Option<NameServerEndpoint>,
        lease: Option<EndpointLease>,
        deadline: RequestDeadline,
    ) -> RocketMQResult<Option<TransportSession<PR>>> {
        deadline.ensure_before_send(addr.to_string())?;
        if !self.can_commit_endpoint_lease(lease.as_ref()) {
            return Ok(None);
        }
        if let Some(client) = self.connection_registry.healthy_session(addr, lease.as_ref()) {
            return Ok(Some(client));
        }

        // Capture the exact worker owner before publishing a connection flight.
        // A shutdown advances the worker epoch, so a request admitted before a
        // restart can never spawn or commit through a replacement worker group.
        let worker_owner = self
            .capture_worker_task_owner()
            .ok_or(RocketMQError::ClientNotStarted)?;
        let (flight, leader) = self.connection_registry.acquire_flight(addr.clone(), lease.clone());
        if leader {
            #[cfg(test)]
            self.wait_for_leader_before_worker_spawn_test_hook().await;
            let client = self.clone();
            let flight_for_task = flight.clone();
            let target = addr.clone();
            let connect_timeout = self.tokio_client_config.connect.timeout;
            let target_for_task = target.clone();
            let lease_for_task = lease.clone();
            let commit_fence = worker_owner.commit_fence();
            let spawned = self.spawn_worker_task_with_owner(
                &worker_owner,
                format!("rocketmq.transport.connect.{target}"),
                async move {
                    client.connect_attempts.fetch_add(1, Ordering::Relaxed);
                    let result = client
                        .connect_endpoint_until(
                            &target_for_task,
                            configured_nameserver,
                            lease_for_task,
                            RequestDeadline::after(connect_timeout),
                            &commit_fence,
                        )
                        .await;
                    let result = if client.matches_connection_commit_fence(&commit_fence) {
                        result
                    } else {
                        Err(RocketMQError::ClientNotStarted)
                    };
                    flight_for_task.complete(result);
                    client
                        .connection_registry
                        .remove_flight_if_matches(&target_for_task, &flight_for_task);
                },
            );
            if spawned.is_none() {
                flight.complete(Err(RocketMQError::ClientNotStarted));
                self.connection_registry.remove_flight_if_matches(&target, &flight);
            }
        }
        flight.wait(deadline, addr).await
    }

    async fn connect_endpoint_until(
        &self,
        addr: &CheetahString,
        configured_nameserver: Option<NameServerEndpoint>,
        lease: Option<EndpointLease>,
        deadline: RequestDeadline,
        commit_fence: &ConnectionCommitFence,
    ) -> RocketMQResult<Option<TransportSession<PR>>> {
        deadline.ensure_before_send(addr.to_string())?;
        if !self.matches_connection_commit_fence(commit_fence) {
            return Err(RocketMQError::ClientNotStarted);
        }
        if !self.can_commit_endpoint_lease(lease.as_ref()) {
            return Ok(None);
        }
        if let Some(client) = self.connection_registry.healthy_session(addr, lease.as_ref()) {
            return Ok(Some(client));
        }

        let allowed = match lease.as_ref() {
            Some(lease) => self
                .nameserver_health
                .connection_admission_if_current(addr, lease, || self.endpoint_state.is_current(lease))
                .is_some_and(|admission| admission != crate::clients::reconnect::CircuitAdmission::Rejected),
            None => self
                .direct_circuit_breakers
                .entry(addr.clone())
                .or_insert_with(crate::clients::reconnect::CircuitBreaker::default_breaker)
                .allow_request(),
        };
        if !allowed {
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
        let transport_config = (*self.tokio_client_config).clone();
        let frame_limits = self.frame_limits;

        let transport_security = self.transport_security.clone();
        let connect_result = TransportSession::connect_target_with_service_context_until_and_telemetry(
            &self.service_context,
            session_target,
            self.cmd_handler.clone(),
            self.tx.as_ref(),
            transport_config,
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
                #[cfg(test)]
                self.wait_for_connect_completion_test_hook().await;
                if !self.matches_connection_commit_fence(commit_fence) {
                    new_client.begin_drain();
                    let _ = new_client.close_with_report(Duration::from_secs(1)).await;
                    return Err(RocketMQError::ClientNotStarted);
                }
                if !self.can_commit_endpoint_lease(lease.as_ref()) {
                    new_client.begin_drain();
                    let _ = new_client.close_with_report(Duration::from_secs(1)).await;
                    return Ok(None);
                }
                match lease.as_ref() {
                    Some(lease) => self
                        .nameserver_health
                        .record_connection_success_if_current(addr, lease, || self.endpoint_state.is_current(lease)),
                    None => {
                        if let Some(mut breaker) = self.direct_circuit_breakers.get_mut(addr) {
                            breaker.record_success();
                        }
                    }
                }
                let session_lease = lease.clone();
                match self
                    .connection_registry
                    .insert_session(addr.clone(), new_client.clone(), lease, || {
                        self.matches_connection_commit_fence(commit_fence)
                            && self.can_commit_endpoint_lease(session_lease.as_ref())
                    }) {
                    Some(client) => {
                        info!("Successfully created client for {}", addr);
                        Ok(Some(client))
                    }
                    None => {
                        new_client.begin_drain();
                        let _ = new_client.close_with_report(Duration::from_secs(1)).await;
                        if self.matches_connection_commit_fence(commit_fence) {
                            Ok(None)
                        } else {
                            Err(RocketMQError::ClientNotStarted)
                        }
                    }
                }
            }
            Err(error) => {
                error!(remote_addr = %addr, error = ?error, "Failed to connect");
                if !self.matches_connection_commit_fence(commit_fence) {
                    return Err(RocketMQError::ClientNotStarted);
                }
                match lease.as_ref() {
                    Some(lease) => self
                        .nameserver_health
                        .record_connection_failure_if_current(addr, lease, || self.endpoint_state.is_current(lease)),
                    None => {
                        if let Some(mut breaker) = self.direct_circuit_breakers.get_mut(addr) {
                            breaker.record_failure();
                        }
                    }
                }
                Err(error)
            }
        }
    }

    fn can_commit_endpoint_lease(&self, lease: Option<&EndpointLease>) -> bool {
        lease.is_none_or(|lease| self.endpoint_state.is_current(lease))
    }

    /// Scans persistent sessions and removes those that are unhealthy or idle.
    fn scan_idle_connections(&self) {
        let idle_threshold = self.tokio_client_config.maintenance.idle_scan_interval;
        let now_millis = current_millis();
        for addr in self.connection_registry.remove_unhealthy_or_idle(|client| {
            idle_threshold.is_some_and(|threshold| client.idle_for_at(now_millis) >= threshold)
        }) {
            warn!("[SCAN] Removed idle/unhealthy connection: {}", addr);
        }
    }
}

impl<PR: RequestProcessor + Sync + Clone + 'static> TransportClient<PR> {
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
            .or_else(|| self.connection_registry.session_identity(session))
            .or_else(|| self.endpoint_state.load().chosen().cloned())
            .unwrap_or_else(|| CheetahString::from_string(session.remote_address().to_string()))
    }

    fn remove_cached_session_if_matches(&self, identity: &CheetahString, expected: &TransportSession<PR>) -> bool {
        self.connection_registry
            .remove_session_if_matches(identity, expected)
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

    fn record_nameserver_outcome(
        &self,
        addr: Option<&CheetahString>,
        lease: Option<&EndpointLease>,
        latency: Duration,
        success: bool,
    ) {
        let (Some(addr), Some(lease)) = (addr, lease) else {
            return;
        };
        self.nameserver_health
            .record_outcome_if_current(addr, lease, latency, success, || self.endpoint_state.is_current(lease));
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
        if self.is_stopping() {
            return Err(RocketMQError::ClientNotStarted);
        }
        let Some(mut client) = self.get_and_create_client_until(Some(addr), deadline).await? else {
            return Err(RocketMQError::network_connection_failed(
                addr.to_string(),
                "one-way client unavailable",
            ));
        };
        if self.is_stopping() {
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
        self.endpoint_state
            .load()
            .endpoints()
            .iter()
            .map(NameServerEndpoint::compatibility_address)
            .collect()
    }

    pub fn get_available_name_srv_list(&self) -> Vec<CheetahString> {
        self.endpoint_state.load().available().iter().cloned().collect()
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
                deadline.ensure_before_send("<nameserver>")?;
                let Some(selection) = self.get_and_create_nameserver_client_until(deadline).await? else {
                    return Err(RocketMQError::network_connection_failed(
                        "<nameserver>",
                        "one-way nameserver client unavailable",
                    ));
                };
                let metric_identity = selection.identity.clone();
                let metric_lease = selection.lease.clone();
                let selection_generation = selection.state.generation();
                let mut client = selection.session;
                debug!(
                    selected = %metric_identity,
                    generation = selection_generation,
                    "Sending one-way request to selected nameserver"
                );
                let result = async {
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
                self.record_nameserver_outcome(
                    Some(&metric_identity),
                    Some(&metric_lease),
                    started_at.elapsed(),
                    result.is_ok(),
                );
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
        let nameserver_request = addr.is_none_or(CheetahString::is_empty);
        let start = time::Instant::now();
        let timeout_millis = deadline.budget_millis();
        let target = if nameserver_request {
            "<nameserver>".to_string()
        } else {
            addr.map_or_else(|| "<nameserver>".to_string(), ToString::to_string)
        };
        deadline.ensure_before_send(target.clone())?;
        let nameserver_diagnostics = nameserver_request.then(|| self.endpoint_state.load());
        let nameserver_selection = if nameserver_request {
            self.get_and_create_nameserver_client_until(deadline).await?
        } else {
            None
        };
        let nameserver_metric_addr = nameserver_selection
            .as_ref()
            .map(|selection| selection.identity.clone());
        let nameserver_lease = nameserver_selection.as_ref().map(|selection| selection.lease.clone());
        let nameserver_generation = nameserver_selection
            .as_ref()
            .map(|selection| selection.state.generation());
        let mut client = match nameserver_selection {
            Some(selection) => Some(selection.session),
            None if nameserver_request => None,
            None => self.get_and_create_client_until(addr, deadline).await?,
        }
        .ok_or_else(|| {
            if target == "<nameserver>" {
                if let Some(state) = nameserver_diagnostics.as_ref() {
                    error!(
                        "Failed to get client for <nameserver>. Diagnostics: configured_list={:?}, available_set={:?}, \
                         cached_choice={:?}, connections={}",
                        state.endpoints(),
                        state.available(),
                        state.chosen(),
                        self.connection_registry.len()
                    );
                }
            } else {
                error!("Failed to get client for {}", target);
            }

            RocketMQError::network_connection_failed(target.clone(), "Failed to connect")
        })?;

        if self.is_stopping() {
            return Err(RocketMQError::ClientNotStarted);
        }

        let mut request = request;
        let initial_remote_address = client.remote_address();
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
            let identity = if nameserver_request {
                self.connection_registry
                    .session_identity(&client)
                    .or_else(|| nameserver_metric_addr.clone())
                    .unwrap_or_else(|| CheetahString::from_string(remote_address.to_string()))
            } else {
                self.session_cache_identity(addr, &client)
            };
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
                        self.record_nameserver_outcome(
                            nameserver_metric_addr.as_ref(),
                            nameserver_lease.as_ref(),
                            latency,
                            true,
                        );
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
                        self.record_nameserver_outcome(
                            nameserver_metric_addr.as_ref(),
                            nameserver_lease.as_ref(),
                            start.elapsed(),
                            false,
                        );
                        return Err(RpcClientError::unexpected_response_code(
                            response.code(),
                            "GO_AWAY after replacement-connection retry",
                        )
                        .into());
                    }

                    attempted_retry = true;
                    if let Err(error) = deadline.ensure_before_send(identity.to_string()) {
                        self.telemetry.record_go_away(TransportGoAwayOutcome::RetryFailed);
                        self.record_nameserver_outcome(
                            nameserver_metric_addr.as_ref(),
                            nameserver_lease.as_ref(),
                            start.elapsed(),
                            false,
                        );
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
                            self.record_nameserver_outcome(
                                nameserver_metric_addr.as_ref(),
                                nameserver_lease.as_ref(),
                                start.elapsed(),
                                false,
                            );
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
                            self.record_nameserver_outcome(
                                nameserver_metric_addr.as_ref(),
                                nameserver_lease.as_ref(),
                                start.elapsed(),
                                false,
                            );
                            return Err(error);
                        }
                    };
                    if attempted_retry {
                        self.telemetry.record_go_away(TransportGoAwayOutcome::RetrySuccess);
                    }
                    let latency = start.elapsed();
                    self.record_nameserver_outcome(
                        nameserver_metric_addr.as_ref(),
                        nameserver_lease.as_ref(),
                        latency,
                        true,
                    );
                    debug!(
                        remote_addr = %identity,
                        nameserver_generation = ?nameserver_generation,
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
                    self.record_nameserver_outcome(
                        nameserver_metric_addr.as_ref(),
                        nameserver_lease.as_ref(),
                        latency,
                        false,
                    );
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
        if self.connection_registry.healthy_session(addr, None).is_some() {
            return;
        }
        if self.connection_registry.remove_unhealthy_session(addr) {
            warn!("Removed unhealthy connection for {}", addr);
        } else {
            debug!("No connection found for {}", addr);
        }
    }

    pub fn close_clients(&self, addrs: Vec<String>) {
        for addr in &addrs {
            let key = CheetahString::from(addr.as_str());
            if !self.connection_registry.remove_sessions_by_identity(&key).is_empty() {
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
mod tests;
