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
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::RuntimeError;
use rocketmq_runtime::RuntimeResult;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskId;
use rocketmq_runtime::TaskKind;
use rocketmq_security_api::Action;
use rocketmq_security_api::Decision;
use rocketmq_security_api::PeerInfo;
use rocketmq_security_api::Principal;
use rocketmq_security_api::Resource;
use rocketmq_security_api::ResourceKind;

use crate::admission::AdmissionClass;
use crate::admission::AdmissionController;
use crate::admission::AdmissionResource;
use crate::admission::AdmissionScope;
use crate::admission::FullPolicy;
use crate::config::TlsConfig;
use crate::config::TlsMode;
use crate::connection::record_transport_write;
use crate::connection::Connection;
use crate::connection::ConnectionId;
use crate::connection::ConnectionState;
use crate::connection::SessionLifecycle;
use crate::request_ordering::RequestOrdering;
use crate::security::TransportSecurity;
use crate::session_executor::SessionDispatchError;
use crate::session_executor::SessionExecutor;
use crate::telemetry::TransportTelemetry;
use crate::tls::NegotiatedConnection;
use crate::tls::TlsServerRuntime;
use crate::write_strategy::QueuedWrite;
use crate::write_strategy::WriterOperation;

const SESSION_WRITER_QUEUE_CAPACITY: usize = 1024;
const SESSION_RETIREMENT_TIMEOUT: Duration = Duration::from_secs(5);

pub trait RequestProcessor: Send + Sync + 'static {
    fn process(
        &self,
        request: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = RocketMQResult<RemotingCommand>> + Send + '_>>;

    fn request_ordering(&self, _request: &RemotingCommand) -> RequestOrdering {
        RequestOrdering::Concurrent
    }
}

#[derive(Clone)]
pub struct SessionHandle {
    session_id: u64,
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
    connection_id: ConnectionId,
    writer: tokio::sync::mpsc::Sender<QueuedWrite>,
    admission: Arc<AdmissionController>,
    scope: AdmissionScope,
    state_tx: tokio::sync::watch::Sender<ConnectionState>,
    state_rx: tokio::sync::watch::Receiver<ConnectionState>,
    task_group: TaskGroup,
    request_executor_group: TaskGroup,
    response_class: Option<AdmissionClass>,
    lifecycle: Arc<SessionLifecycle>,
    writer_task_id: TaskId,
    telemetry: TransportTelemetry,
}

impl SessionHandle {
    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    pub fn connection(&self) -> Connection {
        Connection::new_queued(
            self.writer.clone(),
            self.admission.clone(),
            self.scope,
            self.state_tx.clone(),
            self.state_rx.clone(),
            self.connection_id.clone(),
            self.response_class,
            self.lifecycle.clone(),
            self.telemetry.clone(),
        )
    }

    pub fn task_group(&self) -> &TaskGroup {
        &self.task_group
    }

    pub(crate) fn request_executor_group(&self) -> &TaskGroup {
        &self.request_executor_group
    }

    fn with_response_class(mut self, class: AdmissionClass) -> Self {
        self.response_class = Some(class);
        self
    }

    pub(crate) fn with_task_group(mut self, task_group: TaskGroup) -> Self {
        self.task_group = task_group;
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
                let _ = self.state_tx.send(ConnectionState::Closed);
                self.task_group.cancel();
                self.task_group.abort_task(self.writer_task_id);
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
        let _retirement_guard = self.lifecycle.begin_retirement().await;
        let (completion, result) = tokio::sync::oneshot::channel();
        let send_result = self.writer.send(QueuedWrite::close(completion)).await;
        if send_result.is_err() {
            let _ = self.state_tx.send(ConnectionState::Closed);
            self.task_group.cancel();
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
        let _ = self.state_tx.send(ConnectionState::Closed);
        self.task_group.cancel();
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
    let negotiation = tls.negotiate_detected_connection(stream, remote_addr, is_tls_handshake);

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
    admission: Arc<AdmissionController>,
    handshake_timeout: Duration,
    idle_timeout: Duration,
    security: Arc<TransportSecurity>,
    principal: Option<Principal>,
    next_session: AtomicU64,
    telemetry: TransportTelemetry,
}

impl TransportListener {
    pub fn new(
        listener: tokio::net::TcpListener,
        task_group: TaskGroup,
        tls: TlsServerRuntime,
        admission: Arc<AdmissionController>,
        handshake_timeout: Duration,
    ) -> Self {
        Self {
            listener,
            task_group,
            tls,
            admission,
            handshake_timeout,
            idle_timeout: Duration::from_secs(120),
            security: Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
            principal: None,
            next_session: AtomicU64::new(1),
            telemetry: TransportTelemetry::noop(),
        }
    }

    pub fn with_idle_timeout(mut self, idle_timeout: Duration) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    pub fn with_security(mut self, security: Arc<TransportSecurity>, principal: Option<Principal>) -> Self {
        self.security = security;
        self.principal = principal;
        self
    }

    /// Binds accepted connections and their derived channels to one telemetry instance.
    #[must_use]
    pub fn with_telemetry(mut self, telemetry: TransportTelemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub async fn run<H>(self, handler: Arc<H>) -> RocketMQResult<()>
    where
        H: ConnectionHandler,
    {
        let cancellation = self.task_group.cancellation_token();
        loop {
            let accepted = tokio::select! {
                () = cancellation.cancelled() => return Ok(()),
                accepted = accept_transport_connection(&self.listener) => accepted?,
            };
            let (stream, remote_addr) = accepted;
            if let Err(error) = stream.set_nodelay(true) {
                tracing::warn!(%remote_addr, %error, "failed to configure accepted transport socket");
            }
            let local_addr = stream.local_addr()?;
            let session_id = self.next_session.fetch_add(1, Ordering::Relaxed);
            let scope = AdmissionScope::new(remote_addr.ip()).with_session(session_id);
            let Ok(connection_permit) = self.admission.try_acquire(
                AdmissionResource::Connection,
                scope,
                crate::admission::estimated_connection_retained_bytes(),
                AdmissionClass::Data,
            ) else {
                continue;
            };
            let session_lease = match self.task_group.try_child_lease("rocketmq.transport.session") {
                Ok(lease) => lease,
                Err(_) => return Ok(()),
            };
            let session_group = session_lease.group().clone();
            let tls = self.tls.clone();
            let admission = self.admission.clone();
            let handshake_timeout = self.handshake_timeout;
            let idle_timeout = self.idle_timeout;
            let security = self.security.clone();
            let principal = self.principal.clone();
            let handler = handler.clone();
            let telemetry = self.telemetry.clone();
            let spawn_group = session_group.clone();
            if spawn_group
                .spawn("rocketmq.transport.session", TaskKind::Service, async move {
                    let _session_lease = session_lease;
                    let _connection_permit = connection_permit;
                    let handshake_cancellation = session_group.cancellation_token();
                    let negotiated = tokio::select! {
                        () = handshake_cancellation.cancelled() => return,
                        negotiated = negotiate_transport_connection(
                            &tls,
                            &admission,
                            scope,
                            stream,
                            remote_addr,
                            NegotiationTimeouts {
                                protocol_detection: idle_timeout,
                                tls_handshake: handshake_timeout,
                            },
                        ) => negotiated,
                    };
                    let Some(negotiated) = negotiated else {
                        return;
                    };
                    let (connection, peer_is_tls) = negotiated.into_parts();
                    let connection = connection.with_telemetry(telemetry);
                    run_framed_session(
                        connection,
                        local_addr,
                        remote_addr,
                        session_id,
                        scope,
                        session_group,
                        admission,
                        security,
                        principal,
                        peer_is_tls,
                        idle_timeout,
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
    session_id: u64,
    scope: AdmissionScope,
    task_group: TaskGroup,
    admission: Arc<AdmissionController>,
    security: Arc<TransportSecurity>,
    principal: Option<Principal>,
    peer_is_tls: bool,
    idle_timeout: Duration,
    handler: Arc<H>,
) where
    H: ConnectionHandler,
{
    let connection_id = connection.connection_id().clone();
    let telemetry = connection.telemetry();
    let (mut frame_writer, mut stream) = connection.into_session_io();
    let executor = match SessionExecutor::try_new(&task_group, admission.clone(), scope) {
        Ok(executor) => executor,
        Err(_) => return,
    };
    let (state_tx, state_rx) = tokio::sync::watch::channel(ConnectionState::Healthy);
    let lifecycle = Arc::new(SessionLifecycle::new());
    let (writer, mut writes) = tokio::sync::mpsc::channel::<QueuedWrite>(SESSION_WRITER_QUEUE_CAPACITY);
    let writer_state = state_tx.clone();
    let writer_group = task_group.clone();
    let writer_shutdown_group = writer_group.clone();
    let writer_task_id = match writer_group.spawn("rocketmq.transport.session.writer", TaskKind::Worker, async move {
        while let Some(next) = writes.recv().await {
            match next.operation {
                WriterOperation::Send(payload) => {
                    let completion = next.completion;
                    let deadline = next.deadline;
                    let target = next.target;
                    let progress = next.progress;
                    let result = if deadline.is_some_and(|deadline| deadline.is_expired()) {
                        Err(RocketMQError::network_deadline_exceeded_before_send(target))
                    } else {
                        if let Some(progress) = progress.as_ref() {
                            progress.start_write();
                        }
                        payload
                            .write_to(&mut frame_writer)
                            .await
                            .map_err(|error| RocketMQError::network_connection_failed(target, error.to_string()))
                    };
                    if result.is_ok() {
                        record_transport_write(payload.encoded_len());
                    }
                    let poisoned = frame_writer.is_poisoned();
                    if poisoned {
                        let _ = writer_state.send(ConnectionState::Degraded);
                    }
                    drop(next.permit);
                    let _ = completion.send(result);
                    if poisoned {
                        writes.close();
                        let _ = frame_writer.shutdown().await;
                        while let Some(pending) = writes.recv().await {
                            let target = match pending.operation {
                                WriterOperation::Send(_) => pending.target,
                                WriterOperation::Close => "transport-session-writer".to_string(),
                            };
                            drop(pending.permit);
                            let _ = pending.completion.send(Err(RocketMQError::network_connection_failed(
                                target,
                                "connection writer is poisoned by a previous frame failure",
                            )));
                        }
                        let _ = writer_state.send(ConnectionState::Closed);
                        writer_shutdown_group.cancel();
                        break;
                    }
                }
                WriterOperation::Close => {
                    let result = frame_writer.shutdown().await.map_err(Into::into);
                    let _ = next.completion.send(result);
                    break;
                }
            }
        }
    }) {
        Ok(writer_task_id) => writer_task_id,
        Err(_) => return,
    };
    let session = SessionHandle {
        session_id,
        local_addr,
        remote_addr,
        connection_id,
        writer: writer.clone(),
        admission: admission.clone(),
        scope,
        state_tx: state_tx.clone(),
        state_rx,
        task_group: task_group.clone(),
        request_executor_group: executor.task_group().clone(),
        response_class: None,
        lifecycle,
        writer_task_id,
        telemetry,
    };

    handler.connected(session.clone()).await;
    let cancellation = task_group.cancellation_token();
    loop {
        let next = tokio::select! {
            () = cancellation.cancelled() => break,
            next = tokio::time::timeout(idle_timeout, stream.next()) => next,
        };
        let decoded = match next {
            Ok(Some(Ok(decoded))) => decoded,
            Ok(Some(Err(_))) | Ok(None) | Err(_) => break,
        };
        let command = decoded.command;
        let class = AdmissionClass::for_request_code(command.code());
        let bytes = decoded.retained_frame_bytes;
        let peer = PeerInfo::new(remote_addr, peer_is_tls);
        if let Decision::Deny { reason } = security.authorize(
            &command,
            Some(&peer),
            principal.as_ref(),
            Resource::new(ResourceKind::Other, command.code().to_string()),
            Action::Manage,
        ) {
            let mut connection = session.clone().with_response_class(class).connection();
            let _ = connection
                .send_command(
                    RemotingCommand::create_response_command_with_code_remark(
                        ResponseCode::NoPermission,
                        reason.to_string(),
                    )
                    .set_opaque(command.opaque()),
                )
                .await;
            continue;
        }
        let ordering = handler.request_ordering(&command);
        let opaque = command.opaque();
        let request_handler = handler.clone();
        let request_session = session.clone().with_response_class(class);
        let rejection_session = session.clone().with_response_class(class);
        match executor.try_execute(
            bytes,
            class,
            ordering,
            move |request_group| async move {
                request_handler
                    .command(request_session.with_task_group(request_group), command)
                    .await;
            },
            move |_request_group, error| async move {
                let mut connection = rejection_session.connection();
                let _ = connection
                    .send_command(
                        RemotingCommand::create_response_command_with_code_remark(
                            ResponseCode::SystemBusy,
                            error.to_string(),
                        )
                        .set_opaque(opaque),
                    )
                    .await;
            },
        ) {
            Ok(_) => {}
            Err(SessionDispatchError::Admission(error)) if error.policy() == FullPolicy::Reject => {
                let mut connection = session.clone().with_response_class(class).connection();
                let _ = connection
                    .send_command(
                        RemotingCommand::create_response_command_with_code_remark(
                            ResponseCode::SystemBusy,
                            error.to_string(),
                        )
                        .set_opaque(opaque),
                    )
                    .await;
                continue;
            }
            Err(SessionDispatchError::Admission(_)) | Err(SessionDispatchError::Closing(_)) => break,
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
    let session_id = u64::from(remote_addr.port());
    let scope = AdmissionScope::new(remote_addr.ip()).with_session(session_id);
    run_framed_session(
        connection,
        local_addr,
        remote_addr,
        session_id,
        scope,
        task_group,
        admission,
        security,
        principal,
        false,
        idle_timeout,
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
pub struct TransportServerConfig {
    pub bind_address: SocketAddr,
    pub tls: TlsConfig,
    pub handshake_timeout: Duration,
    pub request_timeout: Duration,
}

impl TransportServerConfig {
    pub fn loopback() -> Self {
        let mut tls = TlsConfig::default();
        tls.server.mode = TlsMode::Disabled;
        Self {
            bind_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            tls,
            handshake_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
        }
    }
}

pub struct TransportServer {
    local_addr: SocketAddr,
    listener: Mutex<Option<tokio::net::TcpListener>>,
    service_context: ChildServiceContext,
    config: TransportServerConfig,
    processor: Arc<dyn RequestProcessor>,
    admission: Arc<AdmissionController>,
    tls: TlsServerRuntime,
    started: AtomicBool,
    next_session: AtomicU64,
    active_sessions: AtomicUsize,
    security: Arc<TransportSecurity>,
    principal: Option<Principal>,
    telemetry: TransportTelemetry,
}

struct ActiveSessionGuard {
    server: Arc<TransportServer>,
}

impl ActiveSessionGuard {
    fn new(server: Arc<TransportServer>) -> Self {
        server.active_sessions.fetch_add(1, Ordering::AcqRel);
        Self { server }
    }
}

impl Drop for ActiveSessionGuard {
    fn drop(&mut self) {
        self.server.active_sessions.fetch_sub(1, Ordering::AcqRel);
    }
}

struct ProcessorSessionHandler {
    processor: Arc<dyn RequestProcessor>,
    request_timeout: Duration,
    session_group: TaskGroup,
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
        let session_group = self.session_group.clone();
        Box::pin(async move {
            let response = match tokio::time::timeout(request_timeout, processor.process(request)).await {
                Ok(Ok(response)) => response,
                Ok(Err(_)) | Err(_) => {
                    session_group.cancel();
                    return;
                }
            };
            let mut connection = session.connection();
            let _ = connection.send_command(response).await;
        })
    }
}

impl TransportServer {
    pub async fn bind(
        service_context: ChildServiceContext,
        config: TransportServerConfig,
        processor: Arc<dyn RequestProcessor>,
        admission: Arc<AdmissionController>,
    ) -> RocketMQResult<Arc<Self>> {
        Self::bind_with_security(
            service_context,
            config,
            processor,
            admission,
            Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
            None,
        )
        .await
    }

    pub async fn bind_with_security(
        service_context: ChildServiceContext,
        config: TransportServerConfig,
        processor: Arc<dyn RequestProcessor>,
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
        config: TransportServerConfig,
        processor: Arc<dyn RequestProcessor>,
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
        config: TransportServerConfig,
        processor: Arc<dyn RequestProcessor>,
        admission: Arc<AdmissionController>,
        security: Arc<TransportSecurity>,
        principal: Option<Principal>,
        telemetry: TransportTelemetry,
    ) -> RocketMQResult<Arc<Self>> {
        let listener = tokio::net::TcpListener::bind(config.bind_address).await?;
        let local_addr = listener.local_addr()?;
        let tls = TlsServerRuntime::initialize_with_service_context(config.tls.clone(), &service_context).await?;
        Ok(Arc::new(Self {
            local_addr,
            listener: Mutex::new(Some(listener)),
            service_context,
            config,
            processor,
            admission,
            tls,
            started: AtomicBool::new(false),
            next_session: AtomicU64::new(1),
            active_sessions: AtomicUsize::new(0),
            security,
            principal,
            telemetry,
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
                let session_id = server.next_session.fetch_add(1, Ordering::Relaxed);
                let scope = AdmissionScope::new(remote_addr.ip()).with_session(session_id);
                let Ok(connection_permit) = server.admission.try_acquire(
                    AdmissionResource::Connection,
                    scope,
                    crate::admission::estimated_connection_retained_bytes(),
                    AdmissionClass::Data,
                ) else {
                    drop(stream);
                    continue;
                };
                let session_lease = match server
                    .service_context
                    .task_group()
                    .try_child_lease("rocketmq.transport.session")
                {
                    Ok(lease) => lease,
                    Err(_) => break,
                };
                let session_group = session_lease.group().clone();
                let session = server.clone();
                let active_session = ActiveSessionGuard::new(server.clone());
                let spawn_group = session_group.clone();
                if spawn_group
                    .spawn("rocketmq.transport.session", TaskKind::Service, async move {
                        let _active_session = active_session;
                        let _session_lease = session_lease;
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
        let handshake_cancellation = session_group.cancellation_token();
        let negotiated = tokio::select! {
            () = handshake_cancellation.cancelled() => return,
            negotiated = negotiate_transport_connection(
                &self.tls,
                &self.admission,
                scope,
                stream,
                remote_addr,
                NegotiationTimeouts {
                    protocol_detection: self.config.request_timeout,
                    tls_handshake: self.config.handshake_timeout,
                },
            ) => negotiated,
        };
        let Some(negotiated) = negotiated else {
            return;
        };
        let (connection, peer_is_tls) = negotiated.into_parts();
        let connection = connection.with_telemetry(self.telemetry.clone());
        run_framed_session(
            connection,
            self.local_addr,
            remote_addr,
            session_id,
            scope,
            session_group.clone(),
            self.admission.clone(),
            self.security.clone(),
            self.principal.clone(),
            peer_is_tls,
            self.config.request_timeout,
            Arc::new(ProcessorSessionHandler {
                processor: self.processor.clone(),
                request_timeout: self.config.request_timeout,
                session_group,
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

    /// Returns the number of child ownership groups retained by the server.
    #[must_use]
    pub fn owned_child_group_count(&self) -> usize {
        self.service_context.task_group().child_count()
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

    #[tokio::test(start_paused = true)]
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

    #[tokio::test(start_paused = true)]
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
    #[tokio::test(start_paused = true)]
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
