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
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQResult;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::ResourcePermit;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReport;
use serde::Serialize;

use crate::base::connection_net_event::ConnectionNetEvent;
use crate::base::pending_request_table::PendingRequestUsage;
use crate::clients::nameserver_endpoint::ConnectTarget;
use crate::clients::nameserver_endpoint::NameServerEndpoint;
use crate::clients::LegacyDefaultRequestProcessor as DefaultRequestProcessor;
use crate::codec::remoting_command_codec::FrameLimits;
use crate::deadline::RequestDeadline;
use crate::runtime::config::client_config::GoAwayPolicy;
use crate::runtime::config::client_config::TransportClientConfig;
use crate::runtime::processor::RequestProcessor;
use crate::runtime::processor_v2::RequestProcessorV2;
use crate::runtime::RPCHook;
use crate::security::TransportSecurity;
use crate::telemetry::TransportTelemetry;
use crate::tls::TlsConfig;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

use super::CachedConnectionState;
use super::TransportClient;

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
    ///
    /// # Errors
    ///
    /// Returns an error when the frame limits are internally inconsistent or
    /// exceed the supported protocol envelope.
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

    /// Builds the legacy-processor transport client.
    ///
    /// # Errors
    ///
    /// Returns an error when the transport configuration, admission limits,
    /// frame limits, or owned runtime composition is invalid.
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

/// Builds a persistent endpoint client with an explicit V2 inbound processor.
pub struct TransportClientV2Builder<PR> {
    config: Arc<TransportClientConfig>,
    processor: PR,
    service_context: ChildServiceContext,
    connection_events: Option<tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
    transport_security: Option<Arc<TransportSecurity>>,
    telemetry: TransportTelemetry,
    frame_limits: FrameLimits,
    go_away_policy: GoAwayPolicy,
}

impl<PR> TransportClientV2Builder<PR>
where
    PR: RequestProcessorV2 + Sync + Clone + 'static,
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
    ///
    /// # Errors
    ///
    /// Returns an error when the frame limits are internally inconsistent or
    /// exceed the supported protocol envelope.
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

    /// Builds the V2 transport client.
    ///
    /// # Errors
    ///
    /// Returns an error when the transport configuration, admission limits,
    /// frame limits, or owned runtime composition is invalid.
    pub fn build(self) -> RocketMQResult<TransportClient<PR>> {
        let mut client = TransportClient::build_inner_v2(
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
}

impl<PR> RemotingClient<PR>
where
    PR: RequestProcessorV2 + Sync + Clone + 'static,
{
    pub fn builder_v2(
        config: Arc<TransportClientConfig>,
        processor: PR,
        service_context: ChildServiceContext,
    ) -> RemotingClientV2Builder<PR> {
        RemotingClientV2Builder {
            transport: TransportClient::builder_v2(config, processor, service_context),
        }
    }
}

impl<PR> RemotingClient<PR>
where
    PR: Send + Sync + Clone + 'static,
{
    pub fn transport_client(&self) -> Arc<TransportClient<PR>> {
        Arc::clone(&self.transport)
    }

    /// Starts the nameserver-aware client lifecycle.
    ///
    /// # Errors
    ///
    /// Returns an error when an owned background service cannot be started or
    /// the client lifecycle has already entered an incompatible terminal state.
    pub async fn start(self: &Arc<Self>) -> RocketMQResult<ClientStartReport> {
        self.transport.start().await
    }

    /// Gracefully shuts down the canonical transport by the caller's absolute deadline.
    ///
    /// This forwards the same deadline without converting it to a new duration,
    /// so nested lifecycle owners share one drain budget.
    ///
    /// # Errors
    ///
    /// Returns an error when the canonical transport cannot complete its
    /// lifecycle transition. Timeout and aborted work remain available in the
    /// returned report when shutdown itself completes successfully.
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
    ///
    /// # Errors
    ///
    /// Returns an error when the frame limits are internally inconsistent or
    /// exceed the supported protocol envelope.
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

    /// Builds the legacy-processor nameserver-aware remoting client.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying V2 transport configuration,
    /// admission limits, frame limits, or owned runtime composition is invalid.
    pub fn build(self) -> RocketMQResult<RemotingClient<PR>> {
        Ok(RemotingClient {
            transport: Arc::new(self.transport.build()?),
        })
    }
}

pub struct RemotingClientV2Builder<PR> {
    transport: TransportClientV2Builder<PR>,
}

impl<PR> RemotingClientV2Builder<PR>
where
    PR: RequestProcessorV2 + Sync + Clone + 'static,
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
    ///
    /// # Errors
    ///
    /// Returns an error when the frame limits are internally inconsistent or
    /// exceed the supported protocol envelope.
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

    /// Builds the V2 nameserver-aware remoting client.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying V2 transport configuration,
    /// admission limits, frame limits, or owned runtime composition is invalid.
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

impl<PR: RequestProcessor + Sync + Clone + 'static> TransportClient<PR> {
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
}

impl<PR: RequestProcessorV2 + Sync + Clone + 'static> TransportClient<PR> {
    pub fn builder_v2(
        tokio_client_config: Arc<TransportClientConfig>,
        processor: PR,
        service_context: ChildServiceContext,
    ) -> TransportClientV2Builder<PR> {
        TransportClientV2Builder {
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
}

impl<PR: Send + Sync + Clone + 'static> TransportClient<PR> {
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
        self.snapshot_inner()
    }

    pub fn update_name_server_address_list_sync(&self, addrs: Vec<CheetahString>) {
        self.update_name_server_address_list_sync_inner(addrs);
    }

    /// Atomically applies resolved NameServer targets and starts bounded retirement for removals.
    pub fn update_name_server_connect_targets_sync(&self, targets: Vec<ConnectTarget>, drain_timeout: Duration) {
        self.update_name_server_connect_targets_sync_inner(targets, drain_timeout);
    }

    /// Atomically publishes a complete selector snapshot.
    pub fn apply_name_server_endpoint_snapshot_sync(
        &self,
        endpoints: Vec<NameServerEndpoint>,
        drain_timeout: Duration,
    ) {
        self.apply_name_server_endpoint_snapshot_sync_inner(endpoints, drain_timeout);
    }

    pub fn register_rpc_hook(&self, hook: Arc<dyn RPCHook>) {
        self.cmd_handler.register_rpc_hook(hook);
    }

    pub fn clear_rpc_hook(&self) {
        self.cmd_handler.clear_rpc_hook();
    }

    pub async fn update_name_server_address_list(&self, addrs: Vec<CheetahString>) {
        self.update_name_server_address_list_sync_inner(addrs);
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
    ///
    /// # Errors
    ///
    /// Returns an error when the target cannot be resolved or connected, the
    /// deadline expires, request admission or signing fails, the writer cannot
    /// send the command, or response correlation terminates without a response.
    pub async fn request(
        &self,
        target: RequestTarget,
        request: RemotingCommand,
        deadline: RequestDeadline,
    ) -> RocketMQResult<RemotingCommand> {
        self.request_inner(target, request, deadline).await
    }

    /// Sends one command and resolves only after the sole writer has completed it.
    ///
    /// # Errors
    ///
    /// Returns an error when the target cannot be resolved or connected, the
    /// deadline expires, request admission or signing fails, or the sole writer
    /// cannot complete the command.
    pub async fn send_oneway(
        &self,
        target: RequestTarget,
        request: RemotingCommand,
        deadline: RequestDeadline,
    ) -> RocketMQResult<SendReceipt> {
        self.send_oneway_inner(target, request, deadline).await
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
    /// # Errors
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
    ) -> RocketMQResult<RemotingCommand> {
        self.invoke_request_with_deadline(addr, request, RequestDeadline::from_timeout_millis(timeout_millis))
            .await
    }

    /// Sends a one-way command while transferring an existing process-budget
    /// reservation into the transport writer.
    ///
    /// # Errors
    ///
    /// Returns an error when the address cannot be connected, the deadline
    /// expires, request signing fails, or the session writer rejects or fails
    /// the command. The supplied permit is consumed by the attempted send.
    pub async fn invoke_oneway_with_permit(
        &self,
        addr: &CheetahString,
        request: RemotingCommand,
        deadline: RequestDeadline,
        permit: ResourcePermit,
    ) -> RocketMQResult<()> {
        self.invoke_oneway_until(addr, request, deadline, Some(permit)).await
    }

    /// Sends a one-way request under the caller's absolute deadline.
    ///
    /// # Errors
    ///
    /// Returns an error when the address cannot be connected, the deadline
    /// expires, request admission or signing fails, or the session writer
    /// rejects or fails the command.
    pub async fn invoke_request_oneway_with_deadline(
        &self,
        addr: &CheetahString,
        request: RemotingCommand,
        deadline: RequestDeadline,
    ) -> RocketMQResult<()> {
        self.invoke_oneway_until(addr, request, deadline, None).await
    }

    /// Sends a one-way request under a timeout relative to the current instant.
    ///
    /// # Errors
    ///
    /// Returns an error when the address cannot be connected, request admission
    /// or signing fails, the timeout expires, or the session writer rejects or
    /// fails the command.
    pub async fn invoke_request_oneway(
        &self,
        addr: &CheetahString,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        self.invoke_oneway_until(
            addr,
            request,
            RequestDeadline::from_timeout_millis(timeout_millis),
            None,
        )
        .await
    }

    /// Reconciles one direct cached connection without performing network I/O.
    ///
    /// The returned state only describes the direct-session cache at this instant.
    /// It is not a DNS, socket, or network reachability probe.
    pub fn reconcile_cached_connection(&self, addr: &CheetahString) -> CachedConnectionState {
        self.reconcile_cached_connection_inner(addr)
    }

    /// Retained compatibility facade for direct cached-session cleanup.
    ///
    /// This method is not a DNS, socket, or network reachability probe. Use
    /// [`Self::reconcile_cached_connection`] when the caller needs the typed
    /// cache state.
    #[deprecated(
        since = "1.0.0",
        note = "use TransportClient::reconcile_cached_connection; is_address_reachable is not a network probe"
    )]
    pub fn is_address_reachable(&self, addr: &CheetahString) {
        self.is_address_reachable_inner(addr);
    }

    pub fn close_clients(&self, addrs: Vec<String>) {
        self.close_clients_inner(addrs);
    }
}

impl<PR: RequestProcessor + Sync + Clone + 'static> TransportClient<PR> {
    /// Retained V1 compatibility facade for request processors fixed at construction.
    ///
    /// Use [`TransportClient::builder`] or [`RemotingClient::builder`] and
    /// propagate the fallible `build()?` result instead.
    #[deprecated(
        since = "1.0.0",
        note = "request processors are fixed at construction; use TransportClient::builder(...).build()? or RemotingClient::builder(...).build()?"
    )]
    pub fn register_processor(&self, processor: impl RequestProcessor + Sync) {
        self.register_processor_inner(processor);
    }
}
