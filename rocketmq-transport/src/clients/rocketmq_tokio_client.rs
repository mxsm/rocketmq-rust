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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_error::RocketMQResult;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::ChildServiceContext;
#[cfg(test)]
use rocketmq_runtime::TaskGroup;
#[cfg(test)]
use rocketmq_runtime::TaskGroupLifecycleState;
use tracing::warn;

use crate::base::connection_net_event::ConnectionNetEvent;
use crate::base::pending_request_table::PendingRequestTable;
use crate::clients::client::ClientInboundOwner;
use crate::clients::client::LegacyClientInboundOwner;
use crate::clients::client::V2ClientInboundOwner;
use crate::clients::nameserver_endpoint::ConnectTarget;
use crate::clients::nameserver_endpoint::NameServerEndpoint;
use crate::clients::LegacyDefaultRequestProcessor as DefaultRequestProcessor;
use crate::codec::remoting_command_codec::FrameLimits;
use crate::remoting::inner::RemotingGeneralHandler;
#[cfg(test)]
use crate::runtime::config::client_config::ConnectConfig;
use crate::runtime::config::client_config::GoAwayPolicy;
#[cfg(test)]
use crate::runtime::config::client_config::MaintenanceConfig;
use crate::runtime::config::client_config::TransportClientConfig;
use crate::runtime::processor::RequestProcessor;
use crate::runtime::processor_v2::RequestProcessorV2;
#[cfg(test)]
use crate::runtime::RPCHook;
use crate::security::TransportSecurity;
use crate::telemetry::TransportTelemetry;
#[cfg(test)]
use crate::tls::TlsConfig;

mod api;
mod compatibility;
mod connect_flight;
mod connection_registry;
mod endpoint_state;
mod lifecycle;
mod nameserver;
mod request;

pub use api::{
    ClientShutdownReport, ClientSnapshot, ClientStartReport, ConnectionShutdownReport, PendingUsage, RemotingClient,
    RemotingClientBuilder, RemotingClientV2Builder, RequestTarget, SendReceipt, TransportClientBuilder,
    TransportClientV2Builder,
};
pub use compatibility::CachedConnectionState;

use connection_registry::ConnectionRegistry;
use endpoint_state::EndpointStateStore;
use lifecycle::ClientLifecycle;
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
    cmd_handler: Arc<dyn ClientInboundOwner>,

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
        let pending_requests = PendingRequestTable::try_with_limits_and_budget(
            crate::base::pending_request_table::PendingRequestLimits {
                max_count: 512,
                ..Default::default()
            },
            &process_budget,
        )?;
        let handler =
            RemotingGeneralHandler::new_with_telemetry(processor, vec![], pending_requests, telemetry.clone());
        Self::build_with_inbound_owner(
            tokio_client_config,
            Arc::new(LegacyClientInboundOwner::new(handler)),
            tx,
            service_context,
            telemetry,
            frame_limits,
            go_away_policy,
        )
    }
}

impl<PR: RequestProcessorV2 + Sync + Clone + 'static> TransportClient<PR> {
    fn build_inner_v2(
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
        let pending_requests = PendingRequestTable::try_with_limits_and_budget(
            crate::base::pending_request_table::PendingRequestLimits {
                max_count: 512,
                ..Default::default()
            },
            &process_budget,
        )?;
        let owner = V2ClientInboundOwner::new(processor, pending_requests, &process_budget)?;
        Self::build_with_inbound_owner(
            tokio_client_config,
            Arc::new(owner),
            tx,
            service_context,
            telemetry,
            frame_limits,
            go_away_policy,
        )
    }
}

impl<PR: Send + Sync + Clone + 'static> TransportClient<PR> {
    const NAMESERVER_SCAN_INTERVAL: Duration = Duration::from_secs(30);

    fn build_with_inbound_owner(
        tokio_client_config: Arc<TransportClientConfig>,
        cmd_handler: Arc<dyn ClientInboundOwner>,
        tx: Option<tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
        service_context: ChildServiceContext,
        telemetry: TransportTelemetry,
        frame_limits: FrameLimits,
        go_away_policy: GoAwayPolicy,
    ) -> RocketMQResult<Self> {
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
            cmd_handler,
            tx,
            transport_security: None,
            telemetry,
            frame_limits,
            go_away_policy,
        })
    }

    fn snapshot_inner(&self) -> ClientSnapshot {
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
            pending: self.cmd_handler.pending_requests().usage().into(),
        }
    }

    fn update_name_server_address_list_sync_inner(&self, addrs: Vec<CheetahString>) {
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
        self.apply_name_server_endpoint_snapshot_sync_inner(endpoints, Duration::from_secs(30));
    }

    fn update_name_server_connect_targets_sync_inner(&self, targets: Vec<ConnectTarget>, drain_timeout: Duration) {
        let endpoints = Self::name_server_connect_targets_to_endpoints(targets);
        self.apply_name_server_endpoint_snapshot_sync_inner(endpoints, drain_timeout);
    }

    fn apply_name_server_endpoint_snapshot_sync_inner(
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

impl<PR: Send + Sync + Clone + 'static> TransportClient<PR> {
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

#[cfg(test)]
mod tests;
