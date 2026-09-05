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
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crate::clients::nameserver_endpoint::ConnectTarget;
#[cfg(test)]
use crate::config::TlsConfig;
use crate::runtime::config::client_config::TransportClientConfig;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::OperationContext;
use rocketmq_runtime::ResourceBudget;
use rocketmq_runtime::ResourcePermit;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskKind;
use rocketmq_security_api::PeerInfo;
use tokio::sync::broadcast;

use crate::base::connection_net_event::ConnectionNetEvent;
use crate::base::pending_request_table::materialize_and_estimate_remoting_command_retained_bytes;
use crate::base::pending_request_table::PendingRequestOwner;
use crate::base::pending_request_table::PendingRequestTable;
use crate::codec::remoting_command_codec::FrameLimits;
use crate::connection::Connection;
use crate::connection::ConnectionStateHandle;
// Import error helpers for convenient error creation
use crate::admission::AdmissionController;
use crate::admission::AdmissionLimits;
use crate::deadline::RequestDeadline;
use crate::error_helpers::remote_error;
use crate::net::channel::Channel;
use crate::net::channel::ChannelInner;
use crate::runtime::processor::RequestProcessor;
use crate::runtime::RPCHook;
use crate::security::TransportSecurity;
use crate::server::AuthorizedFrameRoute;
use crate::server::SessionHandle;
use crate::telemetry::TransportTelemetry;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

#[derive(Clone)]
pub(crate) struct TransportSession<PR> {
    /// The TCP connection decorated with the rocketmq remoting protocol encoder / decoder
    /// implemented using a buffered `TcpStream`.
    ///
    /// When `Listener` receives an inbound connection, the `TcpStream` is
    /// passed to `Connection::new`, which initializes the associated buffers.
    /// `Connection` allows the handler to operate at the "frame" level and keep
    /// the byte level protocol parsing details encapsulated in `Connection`.
    //connection: Connection,
    channel: Channel,
    notify_shutdown: broadcast::Sender<()>,
    session: SessionHandle,
    pending_requests: PendingRequestTable,
    pending_request_owner: PendingRequestOwner,
    task_lifecycle: Arc<ClientTaskLifecycle>,
    transport_security: Arc<TransportSecurity>,
    peer: PeerInfo,
    last_used_millis: Arc<AtomicU64>,
    accepting_requests: Arc<AtomicBool>,
    registry_token: Arc<()>,
    _processor: PhantomData<fn() -> PR>,
}

type ConnectedClientSession = (Channel, PendingRequestOwner, SessionHandle, SocketAddr, bool);
type ClientConnectFuture = Pin<Box<dyn Future<Output = RocketMQResult<ConnectedClientSession>> + Send>>;

pub(crate) trait ClientInboundOwner: Send + Sync + 'static {
    fn pending_requests(&self) -> PendingRequestTable;

    fn hook_snapshot(&self) -> Option<Arc<crate::hook_registry::HookSnapshot>>;

    fn register_rpc_hook(&self, hook: Arc<dyn RPCHook>);

    fn clear_rpc_hook(&self);

    fn run_connected(
        &self,
        connection: Connection,
        local_addr: SocketAddr,
        remote_addr: SocketAddr,
        task_group: TaskGroup,
        process_budget: ResourceBudget,
        ready: tokio::sync::oneshot::Sender<(Channel, PendingRequestOwner, SessionHandle)>,
    ) -> RocketMQResult<Pin<Box<dyn Future<Output = ()> + Send>>>;

    fn do_before_rpc_hooks_with_snapshot(
        &self,
        snapshot: Option<&crate::hook_registry::HookSnapshot>,
        remote_address: SocketAddr,
        request: Option<&mut RemotingCommand>,
    ) -> RocketMQResult<()> {
        if let Some(request) = request {
            crate::remoting::inner::run_before_rpc_hooks(snapshot, remote_address, request)?;
        }
        Ok(())
    }

    fn do_after_rpc_hooks_with_snapshot(
        &self,
        snapshot: Option<&crate::hook_registry::HookSnapshot>,
        remote_address: SocketAddr,
        request: &RemotingCommand,
        response: Option<&mut RemotingCommand>,
    ) -> RocketMQResult<()> {
        if let Some(response) = response {
            crate::remoting::inner::run_after_rpc_hooks(snapshot, remote_address, request, response)?;
        }
        Ok(())
    }
}

pub(crate) struct ProcessorClientInboundOwner<P> {
    dispatcher: Arc<crate::dispatch::AuthorizedCommandDispatcher<P>>,
    pending_requests: PendingRequestTable,
}

impl<P> ProcessorClientInboundOwner<P>
where
    P: RequestProcessor + Clone + Sync + 'static,
{
    pub(crate) fn new(
        processor: P,
        pending_requests: PendingRequestTable,
        process_budget: &ResourceBudget,
    ) -> RocketMQResult<Self> {
        let admission = Arc::new(
            AdmissionController::try_new_with_budget(AdmissionLimits::default(), process_budget)
                .map_err(|error| remote_error(format!("invalid  client inbound admission budget: {error}")))?,
        );
        Ok(Self {
            dispatcher: Arc::new(crate::dispatch::AuthorizedCommandDispatcher::new(
                processor,
                Vec::new(),
                Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
                admission,
            )),
            pending_requests,
        })
    }
}

impl<P> ClientInboundOwner for ProcessorClientInboundOwner<P>
where
    P: RequestProcessor + Clone + Sync + 'static,
{
    fn pending_requests(&self) -> PendingRequestTable {
        self.pending_requests.clone()
    }

    fn hook_snapshot(&self) -> Option<Arc<crate::hook_registry::HookSnapshot>> {
        self.dispatcher.hook_snapshot()
    }

    fn register_rpc_hook(&self, hook: Arc<dyn RPCHook>) {
        self.dispatcher.register_rpc_hook(hook);
    }

    fn clear_rpc_hook(&self) {
        self.dispatcher.clear_rpc_hook();
    }

    fn run_connected(
        &self,
        connection: Connection,
        local_addr: SocketAddr,
        remote_addr: SocketAddr,
        task_group: TaskGroup,
        _process_budget: ResourceBudget,
        ready: tokio::sync::oneshot::Sender<(Channel, PendingRequestOwner, SessionHandle)>,
    ) -> RocketMQResult<Pin<Box<dyn Future<Output = ()> + Send>>> {
        let route = Arc::new(ClientRoute {
            dispatcher: Arc::clone(&self.dispatcher),
            pending_requests: self.pending_requests.clone(),
            ready: parking_lot::Mutex::new(Some(ready)),
        });
        Ok(Box::pin(crate::server::run_connected_session_authorized(
            connection,
            local_addr,
            remote_addr,
            task_group,
            self.dispatcher.boundary(),
            None,
            Duration::from_secs(120),
            route,
        )))
    }
}

struct ClientRoute<P> {
    dispatcher: Arc<crate::dispatch::AuthorizedCommandDispatcher<P>>,
    pending_requests: PendingRequestTable,
    ready: parking_lot::Mutex<Option<tokio::sync::oneshot::Sender<(Channel, PendingRequestOwner, SessionHandle)>>>,
}

struct ClientRouteState {
    pending_owner: PendingRequestOwner,
    network_endpoint: crate::dispatch::NetworkSession,
    deferred_cleanup: crate::dispatch::DeferredSessionCleanupOwner,
}

impl<P> AuthorizedFrameRoute for ClientRoute<P>
where
    P: RequestProcessor + Clone + Sync + 'static,
{
    type SessionState = ClientRouteState;

    async fn connected(&self, session: SessionHandle) -> Option<Self::SessionState> {
        let channel_inner = ChannelInner::new_transport_session(
            session.connection(),
            self.pending_requests.clone(),
            session.task_group().clone(),
        )
        .ok()?;
        let pending_owner = channel_inner.pending_request_owner()?.clone();
        let channel = Channel::new(Arc::new(channel_inner), session.local_addr(), session.remote_addr());
        if let Some(ready) = self.ready.lock().take() {
            let _ = ready.send((channel, pending_owner.clone(), session.clone()));
        }
        Some(ClientRouteState {
            pending_owner,
            network_endpoint: self.dispatcher.open_network_session(),
            deferred_cleanup: crate::dispatch::DeferredSessionCleanupOwner::new(session.session_view().id()),
        })
    }

    async fn response(&self, state: &Self::SessionState, session: SessionHandle, command: RemotingCommand) {
        let opaque = command.opaque();
        let code = command.code();
        if !self
            .pending_requests
            .complete_response_for_owner(&state.pending_owner, opaque, command)
        {
            tracing::warn!(
                code,
                session_id = session.session_id(),
                "received client response without a matching pending request",
            );
        }
    }

    async fn request(
        &self,
        state: &Self::SessionState,
        authorized_session: &crate::dispatch::AuthorizedDispatchSession,
        session: SessionHandle,
        context: crate::dispatch::RequestContext,
        command: RemotingCommand,
        received_at: std::time::Instant,
        retained_bytes: usize,
        partial_frame_permit: Option<crate::admission::PartialFramePermit>,
    ) -> bool {
        match self
            .dispatcher
            .dispatch_network(
                authorized_session,
                state.network_endpoint.clone(),
                session,
                context,
                command,
                received_at,
                retained_bytes,
                partial_frame_permit,
                state.deferred_cleanup.registration(),
            )
            .await
        {
            Ok(outcome) => outcome.keeps_session_open(),
            Err(_) => false,
        }
    }

    fn close_pending(
        &self,
        state: &Self::SessionState,
        _session: SessionHandle,
    ) -> crate::dispatch::DeferredSessionCleanupReport {
        let report = state.deferred_cleanup.close();
        self.dispatcher.close_network_session(&state.network_endpoint);
        report
    }

    async fn disconnected(&self, state: Self::SessionState, _session: SessionHandle) -> usize {
        let cleanup = state.deferred_cleanup.clone();
        drop(state);
        cleanup.remaining_wait_permits()
    }
}

#[derive(Clone)]
pub(crate) enum SessionConnectTarget {
    Legacy(String),
    Resolved(ConnectTarget),
}

impl SessionConnectTarget {
    fn error_identity(&self) -> String {
        match self {
            Self::Legacy(address) => address.clone(),
            Self::Resolved(target) => target.identity().to_string(),
        }
    }
}

struct ClientTaskLifecycle {
    task_group: TaskGroup,
    operation: OperationContext,
}

fn new_client_connection_task_group_with_service_context(
    context: &ChildServiceContext,
) -> (TaskGroup, OperationContext) {
    (
        context.task_group().clone(),
        OperationContext::without_deadline(TaskKind::Service),
    )
}

impl<PR> Drop for TransportSession<PR> {
    fn drop(&mut self) {
        if Arc::strong_count(&self.task_lifecycle) != 1 {
            return;
        }

        let _ = self.notify_shutdown.send(());
        self.pending_requests.close_owner(&self.pending_request_owner, || {
            RocketMQError::network_connection_failed("client", "connection dropped")
        });
        self.session.abort();
        self.task_lifecycle.operation.cancel();
    }
}

// The explicit parameters are independently owned connection capabilities moved into one task.
#[allow(clippy::too_many_arguments)]
fn connect(
    target: SessionConnectTarget,
    cmd_handler: Arc<dyn ClientInboundOwner>,
    tx: Option<tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
    _notify: broadcast::Receiver<()>,
    _send_notify: broadcast::Receiver<()>,
    transport_config: TransportClientConfig,
    frame_limits: FrameLimits,
    task_group: TaskGroup,
    operation: OperationContext,
    process_budget: ResourceBudget,
    deadline: RequestDeadline,
    telemetry: TransportTelemetry,
) -> ClientConnectFuture {
    Box::pin(async move {
        let error_identity = target.error_identity();
        let connected = match target {
            SessionConnectTarget::Legacy(address) => {
                #[cfg(feature = "socks")]
                let connected = crate::client::connect_with_transport_config_and_telemetry(
                    address.as_str(),
                    &transport_config,
                    frame_limits,
                    deadline,
                    telemetry,
                )
                .await?;
                #[cfg(not(feature = "socks"))]
                let connected = crate::client::connect_with_config_and_telemetry(
                    address.as_str(),
                    &transport_config.tls,
                    frame_limits,
                    deadline,
                    telemetry,
                )
                .await?;
                connected
            }
            SessionConnectTarget::Resolved(target) => {
                #[cfg(feature = "socks")]
                let connected = crate::client::connect_target_with_transport_config_and_telemetry(
                    &target,
                    &transport_config,
                    frame_limits,
                    deadline,
                    telemetry,
                )
                .await?;
                #[cfg(not(feature = "socks"))]
                let connected = crate::client::connect_target_with_config_options_and_telemetry(
                    &target,
                    &transport_config.tls,
                    frame_limits,
                    crate::config::SocketOptions::default(),
                    deadline,
                    telemetry,
                )
                .await?;
                connected
            }
        };
        let (connection, local_addr, remote_address, negotiated_tls) = connected.into_parts_with_tls();
        let (ready, connected_session) = tokio::sync::oneshot::channel();
        let session_task_group = task_group.clone();
        let session_runner = cmd_handler.run_connected(
            connection,
            local_addr,
            remote_address,
            session_task_group,
            process_budget,
            ready,
        )?;
        task_group
            .spawn_operation(&operation, "rocketmq.transport.client-session", session_runner)
            .map_err(|error| remote_error(format!("failed to spawn transport client session: {error}")))?;
        let (channel, pending_request_owner, session) = deadline
            .timeout(connected_session)
            .await
            .map_err(|_| RocketMQError::network_connection_timeout(error_identity, deadline.budget_millis()))?
            .map_err(|_| remote_error("transport client session ended before connection setup"))?;
        if let Some(tx) = tx {
            let _ = tx.send(ConnectionNetEvent::CONNECTED(channel.remote_address()));
        }
        Ok((channel, pending_request_owner, session, remote_address, negotiated_tls))
    })
}

impl<PR> TransportSession<PR> {
    #[cfg(test)]
    pub(crate) async fn connect_with_service_context_until(
        context: &ChildServiceContext,
        addr: String,
        cmd_handler: Arc<dyn ClientInboundOwner>,
        tx: Option<&tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
        tls_config: TlsConfig,
        deadline: RequestDeadline,
    ) -> RocketMQResult<TransportSession<PR>> {
        Self::connect_with_service_context_until_and_telemetry(
            context,
            addr,
            cmd_handler,
            tx,
            tls_config,
            deadline,
            TransportTelemetry::noop(),
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn connect_with_service_context_until_and_telemetry(
        context: &ChildServiceContext,
        addr: String,
        cmd_handler: Arc<dyn ClientInboundOwner>,
        tx: Option<&tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
        tls_config: TlsConfig,
        deadline: RequestDeadline,
        telemetry: TransportTelemetry,
    ) -> RocketMQResult<TransportSession<PR>> {
        Self::connect_target_with_service_context_until_and_telemetry(
            context,
            SessionConnectTarget::Legacy(addr),
            cmd_handler,
            tx,
            TransportClientConfig {
                tls: tls_config,
                ..TransportClientConfig::default()
            },
            FrameLimits::java_compatibility(),
            deadline,
            telemetry,
        )
        .await
    }

    pub(crate) async fn connect_target_with_service_context_until_and_telemetry(
        context: &ChildServiceContext,
        target: SessionConnectTarget,
        cmd_handler: Arc<dyn ClientInboundOwner>,
        tx: Option<&tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
        transport_config: TransportClientConfig,
        frame_limits: FrameLimits,
        deadline: RequestDeadline,
        telemetry: TransportTelemetry,
    ) -> RocketMQResult<TransportSession<PR>> {
        let (task_group, operation) = new_client_connection_task_group_with_service_context(context);
        Self::connect_with_task_group(
            target,
            cmd_handler,
            tx,
            transport_config,
            frame_limits,
            task_group,
            operation,
            context.process_budget(),
            deadline,
            telemetry,
        )
        .await
    }

    // The explicit parameters preserve the task-group, TLS, event, and telemetry ownership boundary.
    #[allow(clippy::too_many_arguments)]
    async fn connect_with_task_group(
        target: SessionConnectTarget,
        cmd_handler: Arc<dyn ClientInboundOwner>,
        tx: Option<&tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
        transport_config: TransportClientConfig,
        frame_limits: FrameLimits,
        task_group: TaskGroup,
        operation: OperationContext,
        process_budget: ResourceBudget,
        deadline: RequestDeadline,
        telemetry: TransportTelemetry,
    ) -> RocketMQResult<TransportSession<PR>> {
        let (notify_shutdown, _) = broadcast::channel(1);
        let receiver = notify_shutdown.subscribe();
        let send_receiver = notify_shutdown.subscribe();
        let task_lifecycle = Arc::new(ClientTaskLifecycle {
            task_group: task_group.clone(),
            operation: operation.clone(),
        });
        let pending_requests = cmd_handler.pending_requests();
        let (channel, pending_request_owner, session, remote_address, negotiated_tls) = connect(
            target,
            cmd_handler,
            tx.cloned(),
            receiver,
            send_receiver,
            transport_config,
            frame_limits,
            task_group,
            operation,
            process_budget,
            deadline,
            telemetry,
        )
        .await?;
        Ok(TransportSession {
            channel,
            notify_shutdown,
            session,
            pending_requests,
            pending_request_owner,
            task_lifecycle,
            transport_security: Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
            peer: PeerInfo::new(remote_address, negotiated_tls),
            last_used_millis: Arc::new(AtomicU64::new(current_millis())),
            accepting_requests: Arc::new(AtomicBool::new(true)),
            registry_token: Arc::new(()),
            _processor: PhantomData,
        })
    }

    pub(crate) fn with_transport_security(mut self, transport_security: Arc<TransportSecurity>) -> Self {
        self.transport_security = transport_security;
        self
    }

    fn prepare_transport_request(
        &self,
        request: &mut RemotingCommand,
        deadline: RequestDeadline,
    ) -> RocketMQResult<()> {
        if !self.accepting_requests.load(Ordering::Acquire) {
            return Err(RocketMQError::network_connection_failed(
                self.peer.address().to_string(),
                "connection is draining and no longer accepts new requests",
            ));
        }
        self.last_used_millis.store(current_millis(), Ordering::Release);
        let transport_security = &self.transport_security;
        let target = self.peer.address().to_string();
        deadline.ensure_before_send(target.clone())?;
        transport_security
            .sign(request, Some(&self.peer))
            .map_err(|error| remote_error(format!("request signing failed: {error}")))?;
        deadline.ensure_before_send(target)
    }

    async fn send_prepared_transport(&self, request: RemotingCommand, deadline: RequestDeadline) -> RocketMQResult<()> {
        let target = self.peer.address().to_string();
        deadline.ensure_before_send(target.clone())?;
        let mut connection = self.session.connection();
        connection.send_command_with_deadline(request, deadline, target).await
    }

    async fn send_transport(&self, mut request: RemotingCommand, deadline: RequestDeadline) -> RocketMQResult<()> {
        self.prepare_transport_request(&mut request, deadline)?;
        self.send_prepared_transport(request, deadline).await
    }

    async fn send_transport_with_permit(
        &self,
        mut request: RemotingCommand,
        deadline: RequestDeadline,
        permit: ResourcePermit,
    ) -> RocketMQResult<()> {
        self.prepare_transport_request(&mut request, deadline)?;
        let target = self.peer.address().to_string();
        deadline.ensure_before_send(target.clone())?;
        let mut connection = self.session.connection();
        connection
            .send_command_with_deadline_and_permit(request, deadline, target, permit)
            .await
    }

    /// Invokes a remote operation with the given `RemotingCommand`.
    ///
    /// # Arguments
    ///
    /// * `request` - The `RemotingCommand` representing the request.
    ///
    /// # Returns
    ///
    /// The `RemotingCommand` representing the response, wrapped in a `Result`. Returns an error if
    /// the invocation fails.
    ///
    /// # Errors
    ///
    /// Returns an error when request signing or deadline validation fails, the
    /// pending-response owner rejects the correlation, the writer cannot send
    /// the command, the response owner closes, or the deadline expires before a
    /// response arrives.
    pub async fn send_read(
        &mut self,
        mut request: RemotingCommand,
        deadline: RequestDeadline,
    ) -> RocketMQResult<RemotingCommand> {
        self.prepare_transport_request(&mut request, deadline)?;
        let (tx, rx) = tokio::sync::oneshot::channel::<RocketMQResult<RemotingCommand>>();
        let opaque = request.opaque();
        let retained_bytes = materialize_and_estimate_remoting_command_retained_bytes(&mut request);
        let guard = self.pending_requests.register_for_owner_with_bytes(
            &self.pending_request_owner,
            opaque,
            deadline,
            retained_bytes,
            tx,
        )?;

        self.send_prepared_transport(request, deadline).await?;
        match deadline.timeout(rx).await {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => Err(remote_error(error.to_string())),
            Err(_) => Err(guard.expire(self.peer.address().to_string())),
        }
    }

    #[cfg(test)]
    async fn invoke_with_callback_timeout<F>(&self, mut request: RemotingCommand, timeout: Duration, mut func: F)
    where
        F: FnMut(),
    {
        let (tx, rx) = tokio::sync::oneshot::channel::<RocketMQResult<RemotingCommand>>();
        let deadline = RequestDeadline::after(timeout);
        if self.prepare_transport_request(&mut request, deadline).is_err() {
            return;
        }
        let retained_bytes = materialize_and_estimate_remoting_command_retained_bytes(&mut request);
        let guard = match self.pending_requests.register_for_owner_with_bytes(
            &self.pending_request_owner,
            request.opaque(),
            deadline,
            retained_bytes,
            tx,
        ) {
            Ok(guard) => guard,
            Err(_) => return,
        };
        if self.send_prepared_transport(request, deadline).await.is_err() {
            return;
        }

        if deadline.timeout(rx).await.is_err() {
            guard.expire(self.peer.address().to_string());
            self.retire_after_timeout().await;
        }
        func();
    }

    /// Sends a request using the caller's existing immutable deadline.
    ///
    /// # Errors
    ///
    /// Returns an error when request signing or deadline validation fails, or
    /// when the session writer rejects or fails the command.
    pub async fn send_until(&mut self, request: RemotingCommand, deadline: RequestDeadline) -> RocketMQResult<()> {
        self.send_transport(request, deadline).await
    }

    /// Sends a request under an existing deadline and process reservation.
    ///
    /// # Errors
    ///
    /// Returns an error when request signing or deadline validation fails, or
    /// when the session writer rejects or fails the command. The supplied
    /// permit is consumed by the attempted send.
    pub async fn send_until_with_permit(
        &mut self,
        request: RemotingCommand,
        deadline: RequestDeadline,
        permit: ResourcePermit,
    ) -> RocketMQResult<()> {
        self.send_transport_with_permit(request, deadline, permit).await
    }

    /// Sends multiple requests and collects responses (request-response batch).
    ///
    /// # Performance vs send_read()
    ///
    /// ```text
    /// 100x send_read():    ~5000ms  (sequential network RTT)
    /// send_batch_read():   ~100ms   (parallel + single RTT)
    /// Improvement: 50x faster
    /// ```
    ///
    /// # Arguments
    ///
    /// * `requests` - Vector of commands expecting responses
    /// * `timeout_millis` - Timeout for each individual request
    ///
    /// # Returns
    ///
    /// Vector of results in the same order as input requests
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let requests = vec![cmd1, cmd2, cmd3];
    /// let responses = client.send_batch_read(requests, 3000).await?;
    /// for response in responses {
    ///     match response {
    ///         Ok(cmd) => println!("Success: {:?}", cmd),
    ///         Err(e) => eprintln!("Failed: {}", e),
    ///     }
    /// }
    /// ```
    #[cfg(test)]
    async fn send_batch_read(
        &mut self,
        requests: Vec<RemotingCommand>,
        timeout_millis: u64,
    ) -> RocketMQResult<Vec<RocketMQResult<RemotingCommand>>> {
        let deadline = RequestDeadline::from_timeout_millis(timeout_millis);
        let mut receivers = Vec::with_capacity(requests.len());

        // Send all requests and collect oneshot receivers
        for mut request in requests {
            self.prepare_transport_request(&mut request, deadline)?;
            let (tx, rx) = tokio::sync::oneshot::channel::<RocketMQResult<RemotingCommand>>();
            let retained_bytes = materialize_and_estimate_remoting_command_retained_bytes(&mut request);
            let guard = self.pending_requests.register_for_owner_with_bytes(
                &self.pending_request_owner,
                request.opaque(),
                deadline,
                retained_bytes,
                tx,
            )?;

            self.send_prepared_transport(request, deadline).await?;
            receivers.push((guard, rx));
        }

        // Collect all responses
        let mut results = Vec::with_capacity(receivers.len());
        let mut timed_out = false;
        for (guard, rx) in receivers {
            let result = match deadline.timeout(rx).await {
                Ok(Ok(value)) => value,
                Ok(Err(error)) => Err(remote_error(error.to_string())),
                Err(_) => {
                    timed_out = true;
                    Err(guard.expire(self.peer.address().to_string()))
                }
            };
            results.push(result);
        }
        if timed_out {
            self.retire_after_timeout().await;
        }

        Ok(results)
    }

    /// Gracefully stop this client connection and return the task shutdown report.
    pub async fn close_with_report(&self, timeout: Duration) -> rocketmq_runtime::ShutdownReport {
        let deadline_expired = timeout.is_zero();
        let deadline = tokio::time::Instant::now() + timeout;
        self.accepting_requests.store(false, Ordering::Release);
        let _ = self.notify_shutdown.send(());
        let active_before = self.task_lifecycle.operation.active_task_count();
        self.session
            .request_close(crate::server::SessionCloseCause::ClientShutdown);
        let (session_close_finished, session_close_failed) =
            match tokio::time::timeout_at(deadline, self.session.wait_for_close_completion()).await {
                Ok(Ok(_)) => (true, false),
                Ok(Err(_)) => (true, true),
                Err(_) => (false, false),
            };
        let joined = self
            .task_lifecycle
            .operation
            .cancel_and_wait(
                &self.task_lifecycle.task_group,
                deadline.saturating_duration_since(tokio::time::Instant::now()),
            )
            .await
            .unwrap_or(false);
        let mut report = rocketmq_runtime::ShutdownReport::new("rocketmq.transport.client.connection", Duration::ZERO);
        if joined {
            report.completed = active_before;
        } else {
            report.aborted = active_before;
            report.timed_out = usize::from(active_before > 0);
        }
        if deadline_expired || !session_close_finished {
            report.timed_out = report.timed_out.max(1);
        }
        if session_close_failed {
            report.failed = 1;
            report.annotations.push(rocketmq_runtime::ShutdownAnnotation::new(
                "transport session close completed with an unhealthy lifecycle report",
            ));
        }
        self.pending_requests.close_owner(&self.pending_request_owner, || {
            RocketMQError::network_connection_failed("client", "connection closed")
        });
        report.log_if_unhealthy();
        report
    }

    pub fn connection(&self) -> &ConnectionStateHandle {
        self.channel.connection_ref()
    }

    pub fn remote_address(&self) -> SocketAddr {
        self.channel.remote_address()
    }

    pub(crate) fn is_same_registry_session(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.registry_token, &other.registry_token)
    }

    pub(crate) fn max_pending_request_age(&self) -> Duration {
        self.pending_requests.max_request_age()
    }

    pub(crate) fn idle_for_at(&self, now_millis: u64) -> Duration {
        Duration::from_millis(now_millis.saturating_sub(self.last_used_millis.load(Ordering::Acquire)))
    }

    #[cfg(test)]
    pub(crate) fn set_last_used_millis_for_test(&self, millis: u64) {
        self.last_used_millis.store(millis, Ordering::Release);
    }

    pub(crate) fn retire_after_timeout(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            self.accepting_requests.store(false, Ordering::Release);
            self.session.abort();
            let _ = self.notify_shutdown.send(());
            self.task_lifecycle.operation.cancel();
            self.pending_requests.close_owner(&self.pending_request_owner, || {
                RocketMQError::network_connection_failed("client", "connection retired after request timeout")
            });
        })
    }

    pub(crate) fn begin_drain(&self) {
        self.accepting_requests.store(false, Ordering::Release);
        self.pending_requests.retire_owner(&self.pending_request_owner);
    }

    pub(crate) async fn drain_and_close(&self, timeout: Duration) -> rocketmq_runtime::ShutdownReport {
        self.begin_drain();
        let deadline = tokio::time::Instant::now() + timeout;
        let _ = self
            .pending_requests
            .wait_owner_empty(&self.pending_request_owner, timeout)
            .await;
        self.close_with_report(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
    }
}

#[cfg(test)]
mod inbound_tests {
    use bytes::Bytes;
    use rocketmq_runtime::RuntimeContext;

    use super::*;
    use crate::dispatch::HandlerOutcome;
    use crate::dispatch::RemotingRequest;
    use crate::dispatch::RemotingResponse;

    #[derive(Clone)]
    struct EchoProcessor;

    impl RequestProcessor for EchoProcessor {
        async fn process(&mut self, request: &mut RemotingRequest) -> RocketMQResult<HandlerOutcome> {
            let response = RemotingCommand::create_response_command_with_code(request.command().code() + 1);
            let response =
                RemotingResponse::bytes(response, Bytes::from_static(b"client-inbound")).map_err(|error| {
                    RocketMQError::response_process_failed("client_inbound_test.remoting_response", error.to_string())
                })?;
            Ok(HandlerOutcome::Reply(response))
        }
    }

    #[tokio::test]
    async fn client_route_round_trips_a_real_transport_request_and_response() {
        let runtime = RuntimeContext::from_current("client-inbound-round-trip");
        let service = runtime.service_context("client");
        let pending = PendingRequestTable::try_with_limits_and_budget(Default::default(), &service.process_budget())
            .expect("pending request table");
        let owner = ProcessorClientInboundOwner::new(EchoProcessor, pending, &service.process_budget())
            .expect(" inbound owner");
        let (transport_io, peer_io) = tokio::io::duplex(4096);
        let local_addr = "127.0.0.1:21001".parse().expect("local address");
        let remote_addr = "127.0.0.1:21002".parse().expect("remote address");
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let runner = owner
            .run_connected(
                Connection::new_with_plaintext_stream(transport_io),
                local_addr,
                remote_addr,
                service.task_group().clone(),
                service.process_budget(),
                ready_tx,
            )
            .expect("session runner");
        let runner = tokio::spawn(runner);
        let _connected = ready_rx.await.expect(" client route should connect");
        let mut peer = Connection::new_with_plaintext_stream(peer_io);

        peer.send_command(RemotingCommand::create_remoting_command(701).set_opaque(91))
            .await
            .expect("send inbound request");
        let response = tokio::time::timeout(Duration::from_secs(1), peer.receive_command())
            .await
            .expect("response deadline")
            .expect("response decode")
            .expect("response frame");

        assert_eq!(response.code(), 702);
        assert_eq!(response.opaque(), 91);
        assert_eq!(response.body().map(Bytes::as_ref), Some(b"client-inbound".as_slice()));

        service.task_group().cancel();
        tokio::time::timeout(Duration::from_secs(1), runner)
            .await
            .expect("runner shutdown")
            .expect("runner task");
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use std::time::Duration;

    use rocketmq_runtime::RuntimeContext;
    use rocketmq_runtime::TaskGroupLifecycleState;
    use tokio::net::TcpListener;
    use tokio::time;

    use super::*;
    use crate::base::pending_request_table::PendingRequestTable;
    use crate::request_processor::default_request_processor::DefaultRequestProcessor;

    #[tokio::test]
    async fn request_timeout_cancels_only_the_connection_operation() {
        let runtime_context = RuntimeContext::from_current("remoting-client-drop-test");
        let service = runtime_context.service_context("remoting-client-service");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.expect("accept client");
            time::sleep(Duration::from_secs(5)).await;
        });
        let cmd_handler = Arc::new(
            ProcessorClientInboundOwner::new(
                DefaultRequestProcessor,
                PendingRequestTable::new(),
                &service.process_budget(),
            )
            .expect(" client inbound owner"),
        );

        let mut client = TransportSession::<DefaultRequestProcessor>::connect_with_service_context_until(
            &service,
            addr.to_string(),
            cmd_handler,
            None,
            TlsConfig::default(),
            RequestDeadline::after(Duration::from_secs(1)),
        )
        .await
        .expect("connect client");
        let task_group = client.task_lifecycle.task_group.clone();
        let operation = client.task_lifecycle.operation.clone();

        assert_eq!(task_group.lifecycle_state(), TaskGroupLifecycleState::Open);
        assert_eq!(operation.active_task_count(), 1);

        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let callback_called = called.clone();
        client
            .invoke_with_callback_timeout(
                RemotingCommand::create_remoting_command(5),
                Duration::from_millis(50),
                move || callback_called.store(true, std::sync::atomic::Ordering::SeqCst),
            )
            .await;
        assert!(called.load(std::sync::atomic::Ordering::SeqCst));
        assert!(operation.is_cancelled());
        assert!(!task_group.cancellation_token().is_cancelled());
        assert_eq!(task_group.lifecycle_state(), TaskGroupLifecycleState::Open);
        assert_eq!(client.connection().state(), crate::connection::ConnectionState::Closed);
        assert!(client
            .send_until(
                RemotingCommand::create_remoting_command(6),
                RequestDeadline::after(Duration::from_secs(1)),
            )
            .await
            .is_err());

        drop(client);

        assert!(!task_group.cancellation_token().is_cancelled());
        server.abort();
    }

    #[tokio::test]
    async fn batch_requests_share_one_absolute_response_deadline() {
        let runtime_context = RuntimeContext::from_current("remoting-client-batch-test");
        let service = runtime_context.service_context("remoting-client-service");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.expect("accept client");
            time::sleep(Duration::from_secs(5)).await;
        });
        let cmd_handler = Arc::new(
            ProcessorClientInboundOwner::new(
                DefaultRequestProcessor,
                PendingRequestTable::new(),
                &service.process_budget(),
            )
            .expect(" client inbound owner"),
        );

        let mut client = TransportSession::<DefaultRequestProcessor>::connect_with_service_context_until(
            &service,
            addr.to_string(),
            cmd_handler,
            None,
            TlsConfig::default(),
            RequestDeadline::after(Duration::from_secs(1)),
        )
        .await
        .expect("connect client");
        let task_group = client.task_lifecycle.task_group.clone();
        let operation = client.task_lifecycle.operation.clone();

        assert_eq!(operation.active_task_count(), 1);
        let started_at = time::Instant::now();
        let results = client
            .send_batch_read(
                vec![
                    RemotingCommand::create_remoting_command(1),
                    RemotingCommand::create_remoting_command(2),
                    RemotingCommand::create_remoting_command(3),
                ],
                100,
            )
            .await
            .expect("batch registration should succeed");
        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|result| matches!(
            result,
            Err(RocketMQError::Network(
                rocketmq_error::NetworkError::ResponseTimeout { .. }
            ))
        )));
        assert!(
            time::Instant::now().duration_since(started_at) < Duration::from_millis(200),
            "batch timeout must be one absolute deadline, not one timeout per sequential await"
        );
        time::timeout(Duration::from_secs(1), async {
            while !operation.is_cancelled() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("batch timeout must cancel the connection operation");
        assert!(!task_group.cancellation_token().is_cancelled());
        assert_eq!(client.connection().state(), crate::connection::ConnectionState::Closed);
        assert!(client
            .send_until(
                RemotingCommand::create_remoting_command(4),
                RequestDeadline::after(Duration::from_secs(1)),
            )
            .await
            .is_err());
        let active_before_close = operation.active_task_count();
        let report = client.close_with_report(Duration::from_secs(1)).await;

        assert!(!report.is_healthy(), "{}", report.to_json());
        assert_eq!(report.failed, 1, "{}", report.to_json());
        assert_eq!(report.completed, active_before_close, "{}", report.to_json());
        assert_eq!(report.aborted, 0, "{}", report.to_json());
        assert_eq!(report.timed_out, 0, "{}", report.to_json());
        assert!(report
            .annotations
            .iter()
            .any(|annotation| annotation.message.contains("unhealthy lifecycle report")));
        assert_eq!(operation.active_task_count(), 0);
        assert_eq!(task_group.lifecycle_state(), TaskGroupLifecycleState::Open);
        server.abort();
    }

    #[tokio::test]
    async fn connect_with_service_context_uses_fixed_component_owner() {
        let runtime_context = RuntimeContext::from_current("remoting-client-context-test");
        let service = runtime_context.service_context("remoting-client-service");
        let baseline_components = service.task_group().component_count();
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.expect("accept client");
            time::sleep(Duration::from_secs(5)).await;
        });
        let response_table = PendingRequestTable::new();
        let cmd_handler = Arc::new(
            ProcessorClientInboundOwner::new(
                DefaultRequestProcessor,
                response_table.clone(),
                &service.process_budget(),
            )
            .expect(" client inbound owner"),
        );

        let client = TransportSession::<DefaultRequestProcessor>::connect_with_service_context_until(
            &service,
            addr.to_string(),
            cmd_handler,
            None,
            TlsConfig::default(),
            RequestDeadline::after(Duration::from_secs(1)),
        )
        .await
        .expect("connect client with context");
        let task_group = client.task_lifecycle.task_group.clone();
        let operation = client.task_lifecycle.operation.clone();

        assert_eq!(task_group.id(), service.task_group().id());
        assert_eq!(task_group.parent_id(), service.task_group().parent_id());
        assert_eq!(task_group.lifecycle_state(), TaskGroupLifecycleState::Open);
        assert_eq!(operation.active_task_count(), 1);
        assert_eq!(
            service.task_group().component_count(),
            baseline_components + 1,
            "an active connection must own one independently cancellable session child"
        );

        let mut retained_client = client.clone();
        let mut retained_request = RemotingCommand::create_remoting_command(105).set_body(vec![7_u8; 4096]);
        let expected_retained_bytes = materialize_and_estimate_remoting_command_retained_bytes(&mut retained_request);
        let retained_invocation = tokio::spawn(async move {
            retained_client
                .send_read(retained_request, RequestDeadline::from_timeout_millis(100))
                .await
        });
        time::timeout(Duration::from_secs(1), async {
            while response_table.usage().count == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("request should register");
        assert_eq!(response_table.usage().bytes, expected_retained_bytes);
        assert!(retained_invocation.await.unwrap().is_err());
        assert_eq!(response_table.usage().bytes, 0);

        let report = client.close_with_report(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
        drop(client);
        assert_eq!(operation.active_task_count(), 0);
        assert!(!task_group.cancellation_token().is_cancelled());
        assert_eq!(task_group.lifecycle_state(), TaskGroupLifecycleState::Open);
        drop(task_group);
        tokio::task::yield_now().await;

        assert_eq!(service.task_group().component_count(), baseline_components);
        server.abort();
    }
}
