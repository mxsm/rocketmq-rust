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
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use futures_util::StreamExt;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::OperationContext;
use rocketmq_runtime::RuntimeError;
use rocketmq_runtime::RuntimeResult;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskId;
use rocketmq_runtime::TaskKind;
use rocketmq_security_api::PeerInfo;
use rocketmq_security_api::Principal;
use tokio_util::sync::CancellationToken;

use crate::admission::AdmissionClass;
use crate::admission::AdmissionController;
use crate::admission::AdmissionResource;
use crate::admission::AdmissionScope;
use crate::admission::AdmissionScopeHandle;
use crate::codec::remoting_command_codec::FrameLimits;
use crate::config::SocketOptions;
use crate::config::TlsConfig;
use crate::config::TlsMode;
use crate::connection::Connection;
use crate::connection::ConnectionId;
use crate::connection::ConnectionState;
use crate::connection::SessionLifecycle;
use crate::connection::SessionWriterDiagnostics;
use crate::connection::SessionWriterSnapshot;
use crate::dispatch::AuthorizedDispatchBoundary;
use crate::dispatch::RequestContext;
use crate::dispatch::ResponseSink;
use crate::file_region::FileTransferMode;
use crate::proxy_protocol::read_proxy_protocol;
use crate::proxy_protocol::ProxyProtocolConfig;
use crate::proxy_protocol::ProxyProtocolMetadata;
use crate::request_ordering::RequestOrdering;
use crate::security::TransportSecurity;
use crate::telemetry::TransportTelemetry;
use crate::tls::NegotiatedConnection;
use crate::tls::TlsServerRuntime;
use crate::writer_runtime::run_session_writer;
use crate::writer_runtime::writer_lanes;
use crate::writer_runtime::WriterLanes;
use crate::writer_runtime::WriterQueueConfig;

const SESSION_RETIREMENT_TIMEOUT: Duration = Duration::from_secs(5);

/// Bounded I/O budgets applied to every transport session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionIoPolicy {
    /// Maximum time a session may remain idle while waiting for the next frame.
    pub idle_timeout: Duration,
    /// Queue, fairness, batching, and socket-stall bounds for the sole writer owner.
    pub writer_queue: WriterQueueConfig,
}

impl Default for SessionIoPolicy {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(120),
            writer_queue: WriterQueueConfig::default(),
        }
    }
}

impl SessionIoPolicy {
    fn validate(self) -> RocketMQResult<Self> {
        if self.idle_timeout.is_zero() {
            return Err(RocketMQError::network_connection_failed(
                "transport-session-policy",
                "idle timeout must be greater than zero",
            ));
        }
        self.writer_queue.validate()?;
        Ok(self)
    }
}

#[allow(
    dead_code,
    reason = "the low-level session harness is exposed only by test_support and benchmark_support"
)]
pub trait SessionProcessor: Send + Sync + 'static {
    fn process(
        &self,
        request: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = RocketMQResult<RemotingCommand>> + Send + '_>>;

    fn request_ordering(&self, _request: &RemotingCommand) -> RequestOrdering {
        RequestOrdering::Concurrent
    }
}

struct SessionSendHandle {
    session_id: u64,
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
    transport_peer_addr: SocketAddr,
    proxy_protocol: Option<Arc<ProxyProtocolMetadata>>,
    connection_id: ConnectionId,
    frame_limits: FrameLimits,
    writer: WriterLanes,
    writer_diagnostics: Arc<SessionWriterDiagnostics>,
    admission: AdmissionScopeHandle,
    state_tx: tokio::sync::watch::Sender<ConnectionState>,
    state_rx: tokio::sync::watch::Receiver<ConnectionState>,
    task_group: TaskGroup,
    reader_cancellation: CancellationToken,
    writer_operation: OperationContext,
    lifecycle: Arc<SessionLifecycle>,
    writer_task_id: TaskId,
    telemetry: TransportTelemetry,
}

#[derive(Clone)]
pub struct SessionHandle {
    send: Arc<SessionSendHandle>,
    request_operation: OperationContext,
    response_class: Option<AdmissionClass>,
}

impl SessionHandle {
    pub fn session_id(&self) -> u64 {
        self.send.session_id
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.send.local_addr
    }

    pub fn remote_addr(&self) -> SocketAddr {
        self.send.remote_addr
    }

    /// Returns the socket peer before trusted PROXY source rewriting.
    pub fn transport_peer_addr(&self) -> SocketAddr {
        self.send.transport_peer_addr
    }

    /// Returns trusted PROXY source metadata for this session, when present.
    pub fn proxy_protocol(&self) -> Option<&Arc<ProxyProtocolMetadata>> {
        self.send.proxy_protocol.as_ref()
    }

    pub fn connection(&self) -> Connection {
        Connection::new_queued(
            self.send.writer.clone(),
            self.send.writer_diagnostics.clone(),
            self.send.admission.clone(),
            self.send.state_tx.clone(),
            self.send.state_rx.clone(),
            self.send.connection_id.clone(),
            self.send.frame_limits,
            self.response_class,
            self.send.lifecycle.clone(),
            self.send.telemetry.clone(),
        )
    }

    pub fn task_group(&self) -> &TaskGroup {
        &self.send.task_group
    }

    #[must_use]
    pub fn writer_snapshot(&self) -> SessionWriterSnapshot {
        let mut snapshot = self.send.writer_diagnostics.snapshot();
        self.send.writer.enrich_snapshot(&mut snapshot);
        snapshot
    }

    pub fn operation_context(&self) -> &OperationContext {
        &self.request_operation
    }

    pub(crate) fn abort(&self) {
        let _ = self.send.state_tx.send(ConnectionState::Closed);
        self.send.reader_cancellation.cancel();
        self.request_operation.cancel();
        self.send.writer_operation.cancel();
        self.send.task_group.abort_task(self.send.writer_task_id);
    }

    pub(crate) fn with_response_class(mut self, class: AdmissionClass) -> Self {
        self.response_class = Some(class);
        self
    }

    pub(crate) fn with_operation_context(mut self, operation: OperationContext) -> Self {
        self.request_operation = operation;
        self
    }

    /// Closes the session writer after all sends that already entered the lifecycle gate finish.
    /// Retirement has a five-second absolute deadline and aborts a writer blocked past it.
    ///
    /// # Errors
    ///
    /// Returns an error if the writer queue or close completion channel has already terminated, if
    /// closing the framed socket fails, or if the absolute retirement deadline expires.
    pub async fn retire(&self) -> rocketmq_error::RocketMQResult<()> {
        self.retire_with_timeout_inner(SESSION_RETIREMENT_TIMEOUT, None).await
    }

    async fn retire_with_timeout_inner(
        &self,
        timeout: Duration,
        started: Option<tokio::sync::oneshot::Sender<()>>,
    ) -> rocketmq_error::RocketMQResult<()> {
        match tokio::time::timeout(timeout, self.retire_inner(started)).await {
            Ok(result) => result,
            Err(_) => {
                let _ = self.send.state_tx.send(ConnectionState::Closed);
                self.send.reader_cancellation.cancel();
                self.request_operation.cancel();
                self.send.writer_operation.cancel();
                self.send.task_group.abort_task(self.send.writer_task_id);
                Err(rocketmq_error::RocketMQError::network_connection_failed(
                    "transport-session-writer",
                    "writer retirement exceeded its absolute deadline",
                ))
            }
        }
    }

    async fn retire_inner(
        &self,
        started: Option<tokio::sync::oneshot::Sender<()>>,
    ) -> rocketmq_error::RocketMQResult<()> {
        if let Some(started) = started {
            let _ = started.send(());
        }
        self.send.lifecycle.begin_retirement().await;
        let (completion, result) = tokio::sync::oneshot::channel();
        let send_result = self.send.writer.close(completion).await;
        if send_result.is_err() {
            let _ = self.send.state_tx.send(ConnectionState::Closed);
            self.send.reader_cancellation.cancel();
            self.request_operation.cancel();
            self.send.writer_operation.cancel();
            self.send.task_group.abort_task(self.send.writer_task_id);
            return Err(rocketmq_error::RocketMQError::network_connection_failed(
                "transport-session-writer",
                "writer queue closed before retirement",
            ));
        }
        let close_result = result.await.unwrap_or_else(|_| {
            Err(rocketmq_error::RocketMQError::network_connection_failed(
                "transport-session-writer",
                "writer retirement completion dropped",
            ))
        });
        let _ = self.send.state_tx.send(ConnectionState::Closed);
        self.send.reader_cancellation.cancel();
        self.request_operation.cancel();
        let _ = self
            .send
            .task_group
            .wait_task(self.send.writer_task_id, SESSION_RETIREMENT_TIMEOUT)
            .await;
        close_result
    }

    #[cfg(test)]
    fn connection_with_enqueue_gate(
        &self,
        checked: Arc<tokio::sync::Notify>,
        resume: Arc<tokio::sync::Notify>,
    ) -> Connection {
        let mut connection = self.connection();
        connection.set_enqueue_gate(checked, resume);
        connection
    }

    #[cfg(test)]
    async fn retire_with_signal(
        &self,
        started: tokio::sync::oneshot::Sender<()>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.retire_with_timeout_inner(SESSION_RETIREMENT_TIMEOUT, Some(started))
            .await
    }

    #[cfg(test)]
    async fn retire_with_timeout(&self, timeout: Duration) -> rocketmq_error::RocketMQResult<()> {
        self.retire_with_timeout_inner(timeout, None).await
    }
}

pub trait ConnectionHandler: Send + Sync + 'static {
    fn connected(&self, session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;

    fn request_ordering(&self, _command: &RemotingCommand) -> RequestOrdering {
        RequestOrdering::Concurrent
    }

    fn command(
        &self,
        session: SessionHandle,
        command: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;

    fn disconnected(&self, _session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }
}

#[derive(Clone, Copy)]
struct NegotiationTimeouts {
    protocol_detection: Duration,
    tls_handshake: Duration,
}

async fn negotiate_transport_connection(
    tls: &TlsServerRuntime,
    admission: &AdmissionController,
    scope: AdmissionScope,
    stream: tokio::net::TcpStream,
    remote_addr: SocketAddr,
    frame_limits: FrameLimits,
    timeouts: NegotiationTimeouts,
) -> Option<NegotiatedConnection> {
    // Protocol detection waits under the connection idle budget. Once TLS is identified, the
    // narrower handshake budget bounds only the cryptographic negotiation.
    let is_tls_handshake = tokio::time::timeout(
        timeouts.protocol_detection,
        tls.detect_tls_handshake(&stream, remote_addr),
    )
    .await
    .ok()
    .flatten()?;
    let _handshake_permit = admission
        .try_acquire(
            AdmissionResource::Handshake,
            scope,
            crate::admission::estimated_handshake_retained_bytes(),
            AdmissionClass::Data,
        )
        .ok()?;
    let negotiation =
        tls.negotiate_detected_connection_with_limits(stream, remote_addr, is_tls_handshake, frame_limits);

    if is_tls_handshake {
        tokio::time::timeout(timeouts.tls_handshake, negotiation)
            .await
            .ok()
            .flatten()
    } else {
        negotiation.await
    }
}

/// Canonical socket accept, admission, TLS handshake, and session ownership runtime.
pub struct TransportListener {
    listener: tokio::net::TcpListener,
    task_group: TaskGroup,
    tls: TlsServerRuntime,
    dispatch: Arc<AuthorizedDispatchBoundary>,
    handshake_timeout: Duration,
    io_policy: SessionIoPolicy,
    principal: Option<Principal>,
    next_session: AtomicU64,
    telemetry: TransportTelemetry,
    socket_options: SocketOptions,
    file_region_blocking: Option<BlockingExecutor>,
    file_transfer_mode: FileTransferMode,
    frame_limits: FrameLimits,
    proxy_protocol: ProxyProtocolConfig,
}

impl TransportListener {
    pub fn new(
        listener: tokio::net::TcpListener,
        task_group: TaskGroup,
        tls: TlsServerRuntime,
        admission: Arc<AdmissionController>,
        handshake_timeout: Duration,
    ) -> Self {
        let dispatch = Arc::new(AuthorizedDispatchBoundary::new(
            Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
            admission,
        ));
        Self {
            listener,
            task_group,
            tls,
            dispatch,
            handshake_timeout,
            io_policy: SessionIoPolicy::default(),
            principal: None,
            next_session: AtomicU64::new(1),
            telemetry: TransportTelemetry::noop(),
            socket_options: SocketOptions::default(),
            file_region_blocking: None,
            file_transfer_mode: FileTransferMode::Auto,
            frame_limits: FrameLimits::default(),
            proxy_protocol: ProxyProtocolConfig::default(),
        }
    }

    #[allow(
        dead_code,
        reason = "custom idle policy is used by the feature-gated session harness"
    )]
    pub fn with_idle_timeout(mut self, idle_timeout: Duration) -> Self {
        self.io_policy.idle_timeout = idle_timeout;
        self
    }

    /// Applies explicit idle and write-stall budgets to accepted sessions.
    #[allow(dead_code, reason = "custom I/O policy is used by the feature-gated session harness")]
    #[must_use]
    pub fn with_io_policy(mut self, io_policy: SessionIoPolicy) -> Self {
        self.io_policy = io_policy;
        self
    }

    /// Applies one symmetric frame profile to plaintext/TLS readers and writers.
    pub fn try_with_frame_limits(mut self, frame_limits: FrameLimits) -> RocketMQResult<Self> {
        frame_limits.validate()?;
        self.frame_limits = frame_limits;
        Ok(self)
    }

    /// Applies the trusted PROXY protocol policy before TLS/application negotiation.
    pub fn try_with_proxy_protocol(mut self, config: ProxyProtocolConfig) -> RocketMQResult<Self> {
        config.validate()?;
        self.proxy_protocol = config;
        Ok(self)
    }

    #[allow(dead_code, reason = "custom security is used by the feature-gated session harness")]
    pub fn with_security(mut self, security: Arc<TransportSecurity>, principal: Option<Principal>) -> Self {
        self.dispatch = Arc::new(AuthorizedDispatchBoundary::new(
            security,
            self.dispatch.admission_controller(),
        ));
        self.principal = principal;
        self
    }

    pub(crate) fn with_authorized_dispatch(
        mut self,
        dispatch: Arc<AuthorizedDispatchBoundary>,
        principal: Option<Principal>,
    ) -> Self {
        self.dispatch = dispatch;
        self.principal = principal;
        self
    }

    /// Binds accepted connections and their derived channels to one telemetry instance.
    #[must_use]
    pub fn with_telemetry(mut self, telemetry: TransportTelemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    #[must_use]
    #[allow(
        dead_code,
        reason = "custom socket policy is used by the feature-gated session harness"
    )]
    pub fn with_socket_options(mut self, socket_options: SocketOptions) -> Self {
        self.socket_options = socket_options;
        self
    }

    /// Injects the runtime-owned storage I/O lane and file-transfer mode for accepted sessions.
    #[must_use]
    pub fn with_file_region_io(mut self, blocking: BlockingExecutor, mode: FileTransferMode) -> Self {
        self.file_region_blocking = Some(blocking);
        self.file_transfer_mode = mode;
        self
    }

    pub async fn run<H>(self, handler: Arc<H>) -> RocketMQResult<()>
    where
        H: ConnectionHandler,
    {
        let io_policy = self.io_policy.validate()?;
        let cancellation = self.task_group.cancellation_token();
        let admission = self.dispatch.admission_controller();
        loop {
            let accepted = tokio::select! {
                () = cancellation.cancelled() => return Ok(()),
                accepted = accept_transport_connection(&self.listener) => accepted?,
            };
            let (stream, remote_addr) = accepted;
            if let Err(error) = self.socket_options.apply(&stream) {
                tracing::warn!(%remote_addr, %error, "rejected transport socket with invalid required options");
                continue;
            }
            let local_addr = stream.local_addr()?;
            let session_id = self.next_session.fetch_add(1, Ordering::Relaxed);
            let scope = AdmissionScope::new(remote_addr.ip()).with_session(session_id);
            let Ok(connection_permit) = admission.try_acquire(
                AdmissionResource::Connection,
                scope,
                crate::admission::estimated_connection_retained_bytes(),
                AdmissionClass::Data,
            ) else {
                continue;
            };
            let session_group = match self
                .task_group
                .try_child(format!("rocketmq.transport.session.{session_id}"))
            {
                Ok(session_group) => session_group,
                Err(_) => {
                    drop(stream);
                    return Ok(());
                }
            };
            let tls = self.tls.clone();
            let admission = admission.clone();
            let dispatch = self.dispatch.clone();
            let handshake_timeout = self.handshake_timeout;
            let session_io_policy = io_policy;
            let principal = self.principal.clone();
            let handler = handler.clone();
            let telemetry = self.telemetry.clone();
            let file_region_blocking = self.file_region_blocking.clone();
            let file_transfer_mode = self.file_transfer_mode;
            let frame_limits = self.frame_limits;
            let proxy_protocol = self.proxy_protocol.clone();
            let spawn_group = session_group.clone();
            if spawn_group
                .spawn_service("rocketmq.transport.session", async move {
                    let _connection_permit = connection_permit;
                    let handshake_cancellation = session_group.cancellation_token();
                    let mut stream = stream;
                    let proxy_metadata = tokio::select! {
                        () = handshake_cancellation.cancelled() => return,
                        metadata = read_proxy_protocol(&mut stream, remote_addr, &proxy_protocol) => match metadata {
                            Ok(metadata) => metadata.map(Arc::new),
                            Err(error) => {
                                tracing::warn!(%remote_addr, %error, "rejected invalid PROXY protocol header");
                                return;
                            }
                        },
                    };
                    let effective_remote_addr = proxy_metadata.as_ref().map_or(remote_addr, |metadata| metadata.source);
                    let effective_local_addr = proxy_metadata
                        .as_ref()
                        .map_or(local_addr, |metadata| metadata.destination);
                    let effective_scope = AdmissionScope::new(effective_remote_addr.ip()).with_session(session_id);
                    let negotiated = tokio::select! {
                        () = handshake_cancellation.cancelled() => return,
                        negotiated = negotiate_transport_connection(
                            &tls,
                            &admission,
                            effective_scope,
                            stream,
                            remote_addr,
                            frame_limits,
                            NegotiationTimeouts {
                                protocol_detection: io_policy.idle_timeout,
                                tls_handshake: handshake_timeout,
                            },
                        ) => negotiated,
                    };
                    let Some(negotiated) = negotiated else {
                        return;
                    };
                    let (connection, peer_is_tls) = negotiated.into_parts();
                    let mut connection = connection.with_telemetry(telemetry);
                    if let Some(blocking) = file_region_blocking {
                        connection = connection.with_file_region_io(blocking, file_transfer_mode);
                    }
                    run_framed_session(
                        connection,
                        effective_local_addr,
                        effective_remote_addr,
                        remote_addr,
                        proxy_metadata,
                        session_id,
                        effective_scope,
                        session_group,
                        dispatch,
                        principal,
                        peer_is_tls,
                        session_io_policy,
                        handler,
                    )
                    .await;
                })
                .is_err()
            {
                return Ok(());
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_framed_session<H>(
    connection: Connection,
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
    transport_peer_addr: SocketAddr,
    proxy_protocol: Option<Arc<ProxyProtocolMetadata>>,
    session_id: u64,
    scope: AdmissionScope,
    task_group: TaskGroup,
    dispatch: Arc<AuthorizedDispatchBoundary>,
    principal: Option<Principal>,
    peer_is_tls: bool,
    io_policy: SessionIoPolicy,
    handler: Arc<H>,
) where
    H: ConnectionHandler,
{
    let connection_id = connection.connection_id().clone();
    let frame_limits = connection.frame_limits();
    let telemetry = connection.telemetry();
    let admission = dispatch.admission_controller();
    let admission_scope = match admission.prepare_scope(scope) {
        Ok(handle) => handle,
        Err(_) => return,
    };
    let (frame_writer, mut stream) = connection.into_session_io(admission_scope.clone());
    let executor = match dispatch.session(&task_group, admission_scope.clone()) {
        Ok(executor) => executor,
        Err(_) => return,
    };
    let (state_tx, state_rx) = tokio::sync::watch::channel(ConnectionState::Healthy);
    let lifecycle = Arc::new(SessionLifecycle::new());
    let (writer, writes) = writer_lanes(io_policy.writer_queue);
    let writer_diagnostics = Arc::new(SessionWriterDiagnostics::new(io_policy.writer_queue.total_capacity()));
    let writer_task_diagnostics = Arc::clone(&writer_diagnostics);
    let writer_state = state_tx.clone();
    let writer_operation = OperationContext::without_deadline(TaskKind::Worker);
    let request_operation = executor.operation_context().clone();
    let reader_cancellation = CancellationToken::new();
    let reader_shutdown = reader_cancellation.clone();
    let writer_group = task_group.clone();
    let writer_cancellation = writer_operation.cancellation_token();
    let writer_loop = run_session_writer(
        frame_writer,
        writes,
        writer_task_diagnostics,
        writer_state,
        reader_shutdown,
        telemetry.clone(),
    );
    let writer_task_id = match writer_group.spawn("rocketmq.transport.session.writer", TaskKind::Worker, async move {
        tokio::select! {
            biased;
            () = writer_cancellation.cancelled() => {}
            () = writer_loop => {}
        }
    }) {
        Ok(writer_task_id) => writer_task_id,
        Err(_) => return,
    };
    let session = SessionHandle {
        send: Arc::new(SessionSendHandle {
            session_id,
            local_addr,
            remote_addr,
            transport_peer_addr,
            proxy_protocol,
            connection_id,
            frame_limits,
            writer: writer.clone(),
            writer_diagnostics,
            admission: admission_scope,
            state_tx: state_tx.clone(),
            state_rx,
            task_group: task_group.clone(),
            reader_cancellation: reader_cancellation.clone(),
            writer_operation,
            lifecycle,
            writer_task_id,
            telemetry,
        }),
        request_operation: request_operation.clone(),
        response_class: None,
    };

    handler.connected(session.clone()).await;
    let cancellation = task_group.cancellation_token();
    loop {
        let next = tokio::select! {
            () = cancellation.cancelled() => break,
            () = reader_cancellation.cancelled() => break,
            next = tokio::time::timeout(io_policy.idle_timeout, stream.next()) => next,
        };
        let decoded = match next {
            Ok(Some(Ok(decoded))) => decoded,
            Ok(Some(Err(_))) | Ok(None) | Err(_) => break,
        };
        session
            .send
            .telemetry
            .record_inbound_decoded_plaintext_bytes(decoded.retained_frame_bytes);
        let command = decoded.command;
        let partial_frame_permit = decoded.partial_frame_permit;
        let class = AdmissionClass::for_request_code(command.code());
        let bytes = decoded.retained_frame_bytes;
        let context = RequestContext::network(PeerInfo::new(remote_addr, peer_is_tls), principal.clone(), None);
        let ordering = handler.request_ordering(&command);
        let request_handler = handler.clone();
        let request_session = session.clone().with_response_class(class);
        let response = ResponseSink::Network(Arc::new(session.clone().with_response_class(class)));
        if executor
            .dispatch(
                context,
                command,
                bytes,
                partial_frame_permit,
                ordering,
                response,
                move |request_operation, command| async move {
                    request_handler
                        .command(request_session.with_operation_context(request_operation), command)
                        .await;
                },
            )
            .await
            .is_err()
        {
            break;
        }
    }
    let request_deadline = task_group
        .shutdown_deadline()
        .unwrap_or_else(|| ShutdownDeadline::after(SESSION_RETIREMENT_TIMEOUT));
    executor.drain_until(request_deadline).await.log_if_unhealthy();
    handler.disconnected(session.clone()).await;
    let _ = session.retire().await;
}

/// Runs an already-connected client or compatibility socket through the canonical framed session
/// reader and bounded writer runtime.
#[allow(clippy::too_many_arguments)]
pub async fn run_connected_session<H>(
    connection: Connection,
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
    task_group: TaskGroup,
    admission: Arc<AdmissionController>,
    security: Arc<TransportSecurity>,
    principal: Option<Principal>,
    idle_timeout: Duration,
    handler: Arc<H>,
) where
    H: ConnectionHandler,
{
    run_connected_session_with_io_policy(
        connection,
        local_addr,
        remote_addr,
        task_group,
        admission,
        security,
        principal,
        SessionIoPolicy {
            idle_timeout,
            ..SessionIoPolicy::default()
        },
        handler,
    )
    .await;
}

/// Runs an already-connected stream with explicit bounded I/O policy.
pub async fn run_connected_session_with_io_policy<H>(
    connection: Connection,
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
    task_group: TaskGroup,
    admission: Arc<AdmissionController>,
    security: Arc<TransportSecurity>,
    principal: Option<Principal>,
    io_policy: SessionIoPolicy,
    handler: Arc<H>,
) where
    H: ConnectionHandler,
{
    let Ok(io_policy) = io_policy.validate() else {
        return;
    };
    let session_id = u64::from(remote_addr.port());
    let scope = AdmissionScope::new(remote_addr.ip()).with_session(session_id);
    let Ok(session_group) = task_group.try_child(format!("rocketmq.transport.session.{session_id}")) else {
        return;
    };
    run_framed_session(
        connection,
        local_addr,
        remote_addr,
        remote_addr,
        None,
        session_id,
        scope,
        session_group,
        Arc::new(AuthorizedDispatchBoundary::new(security, admission)),
        principal,
        false,
        io_policy,
        handler,
    )
    .await;
}

async fn accept_transport_connection(
    listener: &tokio::net::TcpListener,
) -> RocketMQResult<(tokio::net::TcpStream, SocketAddr)> {
    listener.accept().await.map_err(Into::into)
}

#[derive(Debug, Clone)]
#[allow(
    dead_code,
    reason = "the low-level session server is exposed only by test_support and benchmark_support"
)]
pub struct SessionTransportServerConfig {
    pub bind_address: SocketAddr,
    pub tls: TlsConfig,
    pub handshake_timeout: Duration,
    pub io_policy: SessionIoPolicy,
    pub request_timeout: Duration,
    pub socket_options: SocketOptions,
    pub file_transfer_mode: FileTransferMode,
}

#[allow(
    dead_code,
    reason = "the low-level session server is exposed only by test_support and benchmark_support"
)]
impl SessionTransportServerConfig {
    pub fn loopback() -> Self {
        let mut tls = TlsConfig::default();
        tls.server.mode = TlsMode::Disabled;
        Self {
            bind_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            tls,
            handshake_timeout: Duration::from_secs(10),
            io_policy: SessionIoPolicy::default(),
            request_timeout: Duration::from_secs(30),
            socket_options: SocketOptions::default(),
            file_transfer_mode: FileTransferMode::Auto,
        }
    }
}

#[allow(
    dead_code,
    reason = "the low-level session server is exposed only by test_support and benchmark_support"
)]
pub struct SessionTransportServer {
    local_addr: SocketAddr,
    listener: Mutex<Option<tokio::net::TcpListener>>,
    service_context: ChildServiceContext,
    config: SessionTransportServerConfig,
    processor: Arc<dyn SessionProcessor>,
    dispatch: Arc<AuthorizedDispatchBoundary>,
    tls: TlsServerRuntime,
    started: AtomicBool,
    next_session: AtomicU64,
    active_sessions: AtomicUsize,
    principal: Option<Principal>,
    telemetry: TransportTelemetry,
    frame_limits: FrameLimits,
}

#[allow(dead_code, reason = "owned by the feature-gated low-level session server")]
struct ActiveSessionGuard {
    server: Arc<SessionTransportServer>,
}

#[allow(dead_code, reason = "owned by the feature-gated low-level session server")]
impl ActiveSessionGuard {
    fn new(server: Arc<SessionTransportServer>) -> Self {
        server.active_sessions.fetch_add(1, Ordering::AcqRel);
        Self { server }
    }
}

impl Drop for ActiveSessionGuard {
    fn drop(&mut self) {
        self.server.active_sessions.fetch_sub(1, Ordering::AcqRel);
    }
}

#[allow(dead_code, reason = "owned by the feature-gated low-level session server")]
struct ProcessorSessionHandler {
    processor: Arc<dyn SessionProcessor>,
    request_timeout: Duration,
}

impl ConnectionHandler for ProcessorSessionHandler {
    fn connected(&self, _session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }

    fn request_ordering(&self, request: &RemotingCommand) -> RequestOrdering {
        self.processor.request_ordering(request)
    }

    fn command(
        &self,
        session: SessionHandle,
        request: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let processor = self.processor.clone();
        let request_timeout = self.request_timeout;
        Box::pin(async move {
            let response = match tokio::time::timeout(request_timeout, processor.process(request)).await {
                Ok(Ok(response)) => response,
                Ok(Err(error)) => {
                    tracing::warn!(
                        session_id = session.session_id(),
                        remote_addr = %session.remote_addr(),
                        error_kind = ?error.kind(),
                        "transport request processor failed; aborting session"
                    );
                    session.abort();
                    return;
                }
                Err(_) => {
                    tracing::warn!(
                        session_id = session.session_id(),
                        remote_addr = %session.remote_addr(),
                        "transport request processor timed out; aborting session"
                    );
                    session.abort();
                    return;
                }
            };
            let mut connection = session.connection();
            let _ = connection.send_command(response).await;
        })
    }
}

#[allow(
    dead_code,
    reason = "the low-level session server is exposed only by test_support and benchmark_support"
)]
impl SessionTransportServer {
    pub async fn bind(
        service_context: ChildServiceContext,
        config: SessionTransportServerConfig,
        processor: Arc<dyn SessionProcessor>,
        admission: Arc<AdmissionController>,
    ) -> RocketMQResult<Arc<Self>> {
        Self::bind_with_frame_limits(service_context, config, FrameLimits::default(), processor, admission).await
    }

    pub async fn bind_with_frame_limits(
        service_context: ChildServiceContext,
        config: SessionTransportServerConfig,
        frame_limits: FrameLimits,
        processor: Arc<dyn SessionProcessor>,
        admission: Arc<AdmissionController>,
    ) -> RocketMQResult<Arc<Self>> {
        frame_limits.validate()?;
        Self::bind_with_capabilities(
            service_context,
            config,
            processor,
            admission,
            Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
            None,
            TransportTelemetry::noop(),
            frame_limits,
        )
        .await
    }

    pub async fn bind_with_security(
        service_context: ChildServiceContext,
        config: SessionTransportServerConfig,
        processor: Arc<dyn SessionProcessor>,
        admission: Arc<AdmissionController>,
        security: Arc<TransportSecurity>,
        principal: Option<Principal>,
    ) -> RocketMQResult<Arc<Self>> {
        Self::bind_with_security_and_telemetry(
            service_context,
            config,
            processor,
            admission,
            security,
            principal,
            TransportTelemetry::noop(),
        )
        .await
    }

    /// Binds a server whose accepted connections use one explicit telemetry instance.
    pub async fn bind_with_telemetry(
        service_context: ChildServiceContext,
        config: SessionTransportServerConfig,
        processor: Arc<dyn SessionProcessor>,
        admission: Arc<AdmissionController>,
        telemetry: TransportTelemetry,
    ) -> RocketMQResult<Arc<Self>> {
        Self::bind_with_security_and_telemetry(
            service_context,
            config,
            processor,
            admission,
            Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
            None,
            telemetry,
        )
        .await
    }

    /// Binds a server with explicit security and telemetry capabilities.
    pub async fn bind_with_security_and_telemetry(
        service_context: ChildServiceContext,
        config: SessionTransportServerConfig,
        processor: Arc<dyn SessionProcessor>,
        admission: Arc<AdmissionController>,
        security: Arc<TransportSecurity>,
        principal: Option<Principal>,
        telemetry: TransportTelemetry,
    ) -> RocketMQResult<Arc<Self>> {
        Self::bind_with_capabilities(
            service_context,
            config,
            processor,
            admission,
            security,
            principal,
            telemetry,
            FrameLimits::default(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn bind_with_capabilities(
        service_context: ChildServiceContext,
        config: SessionTransportServerConfig,
        processor: Arc<dyn SessionProcessor>,
        admission: Arc<AdmissionController>,
        security: Arc<TransportSecurity>,
        principal: Option<Principal>,
        telemetry: TransportTelemetry,
        frame_limits: FrameLimits,
    ) -> RocketMQResult<Arc<Self>> {
        config.io_policy.validate()?;
        frame_limits.validate()?;
        let listener = tokio::net::TcpListener::bind(config.bind_address).await?;
        let local_addr = listener.local_addr()?;
        let tls = TlsServerRuntime::initialize_with_service_context(config.tls.clone(), &service_context).await?;
        Ok(Arc::new(Self {
            local_addr,
            listener: Mutex::new(Some(listener)),
            service_context,
            config,
            processor,
            dispatch: Arc::new(AuthorizedDispatchBoundary::new(security, admission)),
            tls,
            started: AtomicBool::new(false),
            next_session: AtomicU64::new(1),
            active_sessions: AtomicUsize::new(0),
            principal,
            telemetry,
            frame_limits,
        }))
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn start(self: &Arc<Self>) -> RuntimeResult<()> {
        if self.started.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let listener = self
            .listener
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or(RuntimeError::TaskGroupClosing {
                group_id: self.service_context.task_group().id(),
                group_name: self.service_context.task_group().name().into(),
            })?;
        let server = self.clone();
        let cancellation = self.service_context.task_group().cancellation_token();
        self.service_context.spawn_service("transport.accept", async move {
            loop {
                let accepted = tokio::select! {
                    () = cancellation.cancelled() => break,
                    accepted = listener.accept() => accepted,
                };
                let Ok((stream, remote_addr)) = accepted else {
                    break;
                };
                if let Err(error) = server.config.socket_options.apply(&stream) {
                    tracing::warn!(%remote_addr, %error, "rejected transport socket with invalid required options");
                    continue;
                }
                let session_id = server.next_session.fetch_add(1, Ordering::Relaxed);
                let scope = AdmissionScope::new(remote_addr.ip()).with_session(session_id);
                let Ok(connection_permit) = server.dispatch.admission_controller().try_acquire(
                    AdmissionResource::Connection,
                    scope,
                    crate::admission::estimated_connection_retained_bytes(),
                    AdmissionClass::Data,
                ) else {
                    drop(stream);
                    continue;
                };
                let session_group = match server
                    .service_context
                    .task_group()
                    .try_child(format!("rocketmq.transport.session.{session_id}"))
                {
                    Ok(session_group) => session_group,
                    Err(_) => {
                        drop(stream);
                        break;
                    }
                };
                let session = server.clone();
                let active_session = ActiveSessionGuard::new(server.clone());
                let spawn_group = session_group.clone();
                if spawn_group
                    .spawn_service("rocketmq.transport.session", async move {
                        let _active_session = active_session;
                        let _connection_permit = connection_permit;
                        session
                            .run_session(stream, remote_addr, session_id, session_group)
                            .await;
                    })
                    .is_err()
                {
                    break;
                }
            }
        })?;
        Ok(())
    }

    async fn run_session(
        self: Arc<Self>,
        stream: tokio::net::TcpStream,
        remote_addr: SocketAddr,
        session_id: u64,
        session_group: TaskGroup,
    ) {
        let scope = AdmissionScope::new(remote_addr.ip()).with_session(session_id);
        let admission = self.dispatch.admission_controller();
        let handshake_cancellation = session_group.cancellation_token();
        let negotiated = tokio::select! {
            () = handshake_cancellation.cancelled() => return,
            negotiated = negotiate_transport_connection(
                &self.tls,
                &admission,
                scope,
                stream,
                remote_addr,
                self.frame_limits,
                NegotiationTimeouts {
                    protocol_detection: self.config.io_policy.idle_timeout,
                    tls_handshake: self.config.handshake_timeout,
                },
            ) => negotiated,
        };
        let Some(negotiated) = negotiated else {
            return;
        };
        let (connection, peer_is_tls) = negotiated.into_parts();
        let connection = connection.with_telemetry(self.telemetry.clone()).with_file_region_io(
            self.service_context.storage_io().clone(),
            self.config.file_transfer_mode,
        );
        run_framed_session(
            connection,
            self.local_addr,
            remote_addr,
            remote_addr,
            None,
            session_id,
            scope,
            session_group.clone(),
            self.dispatch.clone(),
            self.principal.clone(),
            peer_is_tls,
            self.config.io_policy,
            Arc::new(ProcessorSessionHandler {
                processor: self.processor.clone(),
                request_timeout: self.config.request_timeout,
            }),
        )
        .await;
    }

    /// Returns the number of top-level tasks and active session owners.
    #[must_use]
    pub fn live_task_count(&self) -> usize {
        self.service_context
            .task_group()
            .task_count()
            .saturating_add(self.active_sessions.load(Ordering::Acquire))
    }

    /// Returns the number of component ownership groups retained by the server.
    #[must_use]
    pub fn owned_component_group_count(&self) -> usize {
        self.service_context.task_group().component_count()
    }

    pub async fn shutdown_until(&self, deadline: ShutdownDeadline) -> ShutdownReport {
        self.tls.shutdown();
        self.service_context.task_group().shutdown_until(deadline).await
    }
}

#[cfg(test)]
mod retirement_tests {
    use std::future::Future;
    use std::net::SocketAddr;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::time::Duration;

    use rocketmq_error::NetworkError;
    use rocketmq_error::RocketMQError;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_runtime::RuntimeContext;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;
    use tokio::net::TcpStream;
    use tokio::sync::oneshot;
    use tokio::sync::Notify;

    use super::ConnectionHandler;
    use super::SessionHandle;
    use super::TransportListener;
    use crate::admission::AdmissionController;
    use crate::admission::AdmissionLimits;
    use crate::config::TlsConfig;
    #[cfg(feature = "tls")]
    use crate::config::TlsMode;
    use crate::connection::Connection;
    use crate::deadline::RequestDeadline;
    use crate::security::TransportSecurity;
    use crate::tls::TlsServerRuntime;

    struct CaptureSession {
        sender: std::sync::Mutex<Option<oneshot::Sender<SessionHandle>>>,
    }

    struct ObserveSessionRetirement {
        connected: std::sync::Mutex<Option<oneshot::Sender<SessionHandle>>>,
        disconnected: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    }

    impl ConnectionHandler for CaptureSession {
        fn connected(&self, session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(async move {
                if let Some(sender) = self.sender.lock().expect("capture lock").take() {
                    let _ = sender.send(session);
                }
            })
        }

        fn command(
            &self,
            _session: SessionHandle,
            _command: RemotingCommand,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(async {})
        }
    }

    impl ConnectionHandler for ObserveSessionRetirement {
        fn connected(&self, session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(async move {
                if let Some(sender) = self.connected.lock().expect("connected signal lock").take() {
                    let _ = sender.send(session);
                }
            })
        }

        fn command(
            &self,
            _session: SessionHandle,
            _command: RemotingCommand,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(async {})
        }

        fn disconnected(&self, _session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(async move {
                if let Some(sender) = self.disconnected.lock().expect("disconnected signal lock").take() {
                    let _ = sender.send(());
                }
            })
        }
    }

    #[tokio::test]
    async fn owner_cancellation_runs_disconnected_and_retires_the_session() {
        let runtime = RuntimeContext::from_current("transport-owner-cancellation-retirement-test");
        let service = runtime.service_context("transport-owner-cancellation-retirement");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let addr = listener.local_addr().expect("listener address");
        let tls = TlsServerRuntime::initialize_with_service_context(TlsConfig::default(), &service)
            .await
            .expect("initialize TLS runtime");
        let (connected_tx, connected_rx) = oneshot::channel();
        let (disconnected_tx, disconnected_rx) = oneshot::channel();
        let handler = Arc::new(ObserveSessionRetirement {
            connected: std::sync::Mutex::new(Some(connected_tx)),
            disconnected: std::sync::Mutex::new(Some(disconnected_tx)),
        });
        let transport = TransportListener::new(
            listener,
            service.task_group().clone(),
            tls,
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
            Duration::from_secs(1),
        )
        .with_idle_timeout(Duration::from_secs(30));
        let server = tokio::spawn(transport.run(handler));

        let mut client = TcpStream::connect(addr).await.expect("connect client");
        client.write_all(&[0]).await.expect("start plaintext session");
        let session = tokio::time::timeout(Duration::from_secs(1), connected_rx)
            .await
            .expect("session should connect")
            .expect("connected signal");

        service.task_group().cancel();

        tokio::time::timeout(Duration::from_secs(1), disconnected_rx)
            .await
            .expect("owner cancellation must run disconnected")
            .expect("disconnected signal");
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("listener should stop accepting")
            .expect("listener task")
            .expect("listener result");
        let report = service.task_group().shutdown(Duration::from_secs(1)).await;

        assert!(report.is_healthy(), "{}", report.to_json());
        assert_eq!(report.aborted, 0, "{}", report.to_json());
        assert_eq!(session.connection().state(), crate::connection::ConnectionState::Closed);
        let mut byte = [0_u8; 1];
        assert_eq!(client.read(&mut byte).await.expect("read retired socket"), 0);
    }

    #[tokio::test]
    async fn delayed_plaintext_first_byte_uses_idle_timeout_not_tls_handshake_timeout() {
        let runtime = RuntimeContext::from_current("transport-delayed-plaintext-test");
        let service = runtime.service_context("transport-delayed-plaintext");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let addr = listener.local_addr().expect("listener address");
        let tls = TlsServerRuntime::initialize_with_service_context(TlsConfig::default(), &service)
            .await
            .expect("initialize TLS runtime");
        let (session_tx, session_rx) = oneshot::channel();
        let handler = Arc::new(CaptureSession {
            sender: std::sync::Mutex::new(Some(session_tx)),
        });
        let transport = TransportListener::new(
            listener,
            service.task_group().clone(),
            tls,
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
            Duration::from_millis(20),
        )
        .with_idle_timeout(Duration::from_millis(500));
        let server = tokio::spawn(transport.run(handler));

        let mut client = TcpStream::connect(addr).await.expect("connect client");
        tokio::time::sleep(Duration::from_millis(80)).await;
        client.write_all(&[0]).await.expect("write delayed plaintext byte");

        tokio::time::timeout(Duration::from_millis(500), session_rx)
            .await
            .expect("plaintext session should outlive TLS handshake timeout")
            .expect("capture session");

        service.task_group().cancel();
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("server should stop")
            .expect("server task")
            .expect("server result");
    }

    #[tokio::test]
    async fn silent_connection_uses_idle_timeout_before_closing() {
        let runtime = RuntimeContext::from_current("transport-silent-connection-test");
        let service = runtime.service_context("transport-silent-connection");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let addr = listener.local_addr().expect("listener address");
        let tls = TlsServerRuntime::initialize_with_service_context(TlsConfig::default(), &service)
            .await
            .expect("initialize TLS runtime");
        let (session_tx, _session_rx) = oneshot::channel();
        let handler = Arc::new(CaptureSession {
            sender: std::sync::Mutex::new(Some(session_tx)),
        });
        let transport = TransportListener::new(
            listener,
            service.task_group().clone(),
            tls,
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
            Duration::from_millis(20),
        )
        .with_idle_timeout(Duration::from_millis(150));
        let server = tokio::spawn(transport.run(handler));

        let mut client = TcpStream::connect(addr).await.expect("connect client");
        let mut byte = [0u8; 1];
        assert!(
            tokio::time::timeout(Duration::from_millis(60), client.read(&mut byte))
                .await
                .is_err(),
            "silent connection must remain open past the TLS handshake timeout"
        );
        let read = tokio::time::timeout(Duration::from_millis(300), client.read(&mut byte))
            .await
            .expect("silent connection should reach idle timeout")
            .expect("read idle close");
        assert_eq!(read, 0, "idle timeout should close the silent connection");

        service.task_group().cancel();
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("server should stop")
            .expect("server task")
            .expect("server result");
    }

    #[cfg(feature = "tls")]
    #[tokio::test]
    async fn detected_tls_handshake_remains_bounded_by_handshake_timeout() {
        let runtime = RuntimeContext::from_current("transport-stalled-tls-test");
        let service = runtime.service_context("transport-stalled-tls");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let addr = listener.local_addr().expect("listener address");
        let tls = TlsServerRuntime::initialize_with_service_context(
            TlsConfig {
                test_mode_enable: true,
                server: crate::config::TlsServerConfig {
                    mode: TlsMode::Permissive,
                    ..Default::default()
                },
                ..Default::default()
            },
            &service,
        )
        .await
        .expect("initialize TLS runtime");
        assert_eq!(tls.active_generation(), 1, "test TLS acceptor must be active");
        let (session_tx, _session_rx) = oneshot::channel();
        let handler = Arc::new(CaptureSession {
            sender: std::sync::Mutex::new(Some(session_tx)),
        });
        let transport = TransportListener::new(
            listener,
            service.task_group().clone(),
            tls,
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
            Duration::from_millis(30),
        )
        .with_idle_timeout(Duration::from_millis(500));
        let server = tokio::spawn(transport.run(handler));

        let mut client = TcpStream::connect(addr).await.expect("connect client");
        client.write_all(&[0x16]).await.expect("write TLS handshake magic");
        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_millis(250), client.read(&mut byte))
            .await
            .expect("stalled TLS handshake should not reach idle timeout")
            .expect("read handshake close");
        assert_eq!(read, 0, "TLS handshake timeout should close the connection");

        service.task_group().cancel();
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("server should stop")
            .expect("server task")
            .expect("server result");
    }

    #[tokio::test]
    async fn retirement_waits_for_a_checked_send_before_closing_the_writer() {
        let runtime = RuntimeContext::from_current("transport-retirement-interleaving-test");
        let service = runtime.service_context("transport-retirement-interleaving");
        let (transport, peer_stream) = tokio::io::duplex(4096);
        let (session_tx, session_rx) = oneshot::channel();
        let handler = Arc::new(CaptureSession {
            sender: std::sync::Mutex::new(Some(session_tx)),
        });
        let local_addr: SocketAddr = "127.0.0.1:19001".parse().unwrap();
        let remote_addr: SocketAddr = "127.0.0.1:19002".parse().unwrap();
        let runner = tokio::spawn(super::run_connected_session(
            Connection::new_with_plaintext_stream(transport),
            local_addr,
            remote_addr,
            service.task_group().clone(),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
            Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
            None,
            Duration::from_secs(30),
            handler,
        ));
        let session = session_rx.await.expect("session capture");
        let checked = Arc::new(Notify::new());
        let resume_enqueue = Arc::new(Notify::new());
        let mut checked_connection = session.connection_with_enqueue_gate(checked.clone(), resume_enqueue.clone());
        let checked_send = tokio::spawn(async move {
            checked_connection
                .send_command(RemotingCommand::create_remoting_command(1))
                .await
        });
        checked.notified().await;

        let (retirement_started_tx, retirement_started_rx) = oneshot::channel();
        let retiring_session = session.clone();
        let mut retirement =
            tokio::spawn(async move { retiring_session.retire_with_signal(retirement_started_tx).await });
        retirement_started_rx.await.expect("retirement started");
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut retirement)
                .await
                .is_err(),
            "retirement must wait for a send that passed the lifecycle check"
        );

        resume_enqueue.notify_one();
        checked_send.await.unwrap().expect("checked send completes");
        retirement.await.unwrap().expect("retirement completes");

        let mut post_retirement = session.connection();
        assert!(post_retirement
            .send_command(RemotingCommand::create_remoting_command(2))
            .await
            .is_err());
        let mut peer = Connection::new_with_plaintext_stream(peer_stream);
        let first = peer.receive_command().await.unwrap().unwrap();
        assert_eq!(first.code(), 1);
        assert!(peer.receive_command().await.is_none());
        runner.await.unwrap();
    }

    #[tokio::test]
    async fn retirement_deadline_aborts_a_writer_blocked_on_socket_io() {
        let runtime = RuntimeContext::from_current("transport-retirement-deadline-test");
        let service = runtime.service_context("transport-retirement-deadline");
        let (transport, _peer_stream) = tokio::io::duplex(64);
        let (session_tx, session_rx) = oneshot::channel();
        let handler = Arc::new(CaptureSession {
            sender: std::sync::Mutex::new(Some(session_tx)),
        });
        let local_addr: SocketAddr = "127.0.0.1:19003".parse().unwrap();
        let remote_addr: SocketAddr = "127.0.0.1:19004".parse().unwrap();
        let runner = tokio::spawn(super::run_connected_session(
            Connection::new_with_plaintext_stream(transport),
            local_addr,
            remote_addr,
            service.task_group().clone(),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
            Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
            None,
            Duration::from_secs(30),
            handler,
        ));
        let session = session_rx.await.expect("session capture");
        let mut connection = session.connection();
        let mut blocked_send = tokio::spawn(async move {
            connection
                .send_command(RemotingCommand::create_remoting_command(3).set_body(vec![0_u8; 1024 * 1024]))
                .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut blocked_send)
                .await
                .is_err(),
            "the socket writer must be blocked before retirement starts"
        );

        let retirement = session.retire_with_timeout(Duration::from_millis(30)).await;
        assert!(retirement.is_err(), "the absolute retirement deadline must fire");
        assert_eq!(session.connection().state(), crate::connection::ConnectionState::Closed);
        assert!(tokio::time::timeout(Duration::from_secs(1), blocked_send)
            .await
            .expect("aborted writer releases the blocked send")
            .unwrap()
            .is_err());
        runner.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn request_deadline_wins_the_enqueue_race_without_a_socket_write() {
        let runtime = RuntimeContext::from_current("transport-request-deadline-race-test");
        let service = runtime.service_context("transport-request-deadline-race");
        let (transport, mut peer_stream) = tokio::io::duplex(4096);
        let (session_tx, session_rx) = oneshot::channel();
        let handler = Arc::new(CaptureSession {
            sender: std::sync::Mutex::new(Some(session_tx)),
        });
        let local_addr: SocketAddr = "127.0.0.1:19005".parse().unwrap();
        let remote_addr: SocketAddr = "127.0.0.1:19006".parse().unwrap();
        let runner = tokio::spawn(super::run_connected_session(
            Connection::new_with_plaintext_stream(transport),
            local_addr,
            remote_addr,
            service.task_group().clone(),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
            Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
            None,
            Duration::from_secs(30),
            handler,
        ));
        let session = session_rx.await.expect("session capture");
        let checked = Arc::new(Notify::new());
        let resume_enqueue = Arc::new(Notify::new());
        let mut connection = session.connection_with_enqueue_gate(checked.clone(), resume_enqueue);
        let deadline = RequestDeadline::after(Duration::from_millis(50));
        let send = tokio::spawn(async move {
            connection
                .send_command_with_deadline(
                    RemotingCommand::create_remoting_command(4),
                    deadline,
                    "127.0.0.1:19006".to_string(),
                )
                .await
        });
        checked.notified().await;

        tokio::time::advance(Duration::from_millis(50)).await;
        let error = send
            .await
            .expect("send task")
            .expect_err("deadline must win the enqueue race");

        assert!(matches!(
            error,
            RocketMQError::Network(NetworkError::DeadlineExceededBeforeSend { .. })
        ));
        let mut byte = [0_u8; 1];
        tokio::select! {
            biased;
            read = peer_stream.read(&mut byte) => panic!("unexpected socket read after deadline: {read:?}"),
            () = tokio::task::yield_now() => {}
        }

        session.retire().await.expect("retire session");
        runner.await.expect("session runner");
    }
}
