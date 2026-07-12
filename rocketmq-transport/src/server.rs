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
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use futures_util::SinkExt;
use futures_util::StreamExt;
use rocketmq_error::RocketMQResult;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeError;
use rocketmq_runtime::RuntimeResult;
use rocketmq_runtime::ServiceContext;
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
use crate::admission::AdmissionError;
use crate::admission::AdmissionPermit;
use crate::admission::AdmissionResource;
use crate::admission::AdmissionScope;
use crate::admission::FullPolicy;
use crate::config::TlsConfig;
use crate::config::TlsMode;
use crate::connection::Connection;
use crate::connection::ConnectionId;
use crate::connection::ConnectionState;
use crate::connection::QueuedWrite;
use crate::connection::SessionLifecycle;
use crate::security::TransportSecurity;
use crate::tls::TlsServerRuntime;

const SESSION_WRITER_QUEUE_CAPACITY: usize = 1024;
const SESSION_RETIREMENT_TIMEOUT: Duration = Duration::from_secs(5);

pub trait RequestProcessor: Send + Sync + 'static {
    fn process(
        &self,
        request: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = RocketMQResult<RemotingCommand>> + Send + '_>>;
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
    response_class: Option<AdmissionClass>,
    lifecycle: Arc<SessionLifecycle>,
    writer_task_id: TaskId,
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
        )
    }

    pub fn task_group(&self) -> &TaskGroup {
        &self.task_group
    }

    fn with_response_class(mut self, class: AdmissionClass) -> Self {
        self.response_class = Some(class);
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
        let send_result = self.writer.send(QueuedWrite::Close { completion }).await;
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

    fn command(
        &self,
        session: SessionHandle,
        command: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;

    fn disconnected(&self, _session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
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
            security: Arc::new(TransportSecurity::new(None, None)),
            principal: None,
            next_session: AtomicU64::new(1),
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
            let Ok(connection_permit) =
                self.admission
                    .try_acquire(AdmissionResource::Connection, scope, 0, AdmissionClass::Data)
            else {
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
            let spawn_group = session_group.clone();
            if spawn_group
                .spawn("rocketmq.transport.session", TaskKind::Service, async move {
                    let _session_lease = session_lease;
                    let _connection_permit = connection_permit;
                    let Ok(_handshake_permit) =
                        admission.try_acquire(AdmissionResource::Handshake, scope, 0, AdmissionClass::Data)
                    else {
                        return;
                    };
                    let handshake_cancellation = session_group.cancellation_token();
                    let negotiated = tokio::select! {
                        () = handshake_cancellation.cancelled() => return,
                        negotiated = tokio::time::timeout(
                            handshake_timeout,
                            tls.negotiate_connection(stream, remote_addr),
                        ) => negotiated,
                    };
                    let Ok(Some(negotiated)) = negotiated else {
                        return;
                    };
                    let (connection, peer_is_tls) = negotiated.into_parts();
                    drop(_handshake_permit);
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
    let (mut sink, mut stream) = connection.into_framed_parts();
    let (state_tx, state_rx) = tokio::sync::watch::channel(ConnectionState::Healthy);
    let lifecycle = Arc::new(SessionLifecycle::new());
    let (writer, mut writes) = tokio::sync::mpsc::channel(SESSION_WRITER_QUEUE_CAPACITY);
    let writer_cancellation = task_group.cancellation_token();
    let writer_state = state_tx.clone();
    let writer_group = task_group.clone();
    let writer_task_id = match writer_group.spawn("rocketmq.transport.session.writer", TaskKind::Worker, async move {
        loop {
            let next = tokio::select! {
                () = writer_cancellation.cancelled() => break,
                next = writes.recv() => next,
            };
            match next {
                Some(QueuedWrite::Data {
                    bytes,
                    completion,
                    _permit,
                }) => {
                    let result = sink.send(bytes).await;
                    if result.is_err() {
                        let _ = writer_state.send(ConnectionState::Degraded);
                    }
                    let failed = result.is_err();
                    let _ = completion.send(result);
                    drop(_permit);
                    if failed {
                        break;
                    }
                }
                Some(QueuedWrite::Close { completion }) => {
                    let result = sink.close().await;
                    let _ = completion.send(result);
                    break;
                }
                None => break,
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
        response_class: None,
        lifecycle,
        writer_task_id,
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
        let admission_permits = acquire_framed_request(&admission, scope, bytes, class);
        let _admission_permits = match admission_permits {
            Ok(permits) => permits,
            Err(error) if error.policy() == FullPolicy::Reject => {
                let mut connection = session.clone().with_response_class(class).connection();
                let _ = connection
                    .send_command(
                        RemotingCommand::create_response_command_with_code_remark(
                            ResponseCode::SystemBusy,
                            error.to_string(),
                        )
                        .set_opaque(command.opaque()),
                    )
                    .await;
                continue;
            }
            Err(_) => break,
        };
        handler
            .command(session.clone().with_response_class(class), command)
            .await;
    }
    handler.disconnected(session.clone()).await;
    let (completion, closed) = tokio::sync::oneshot::channel();
    let _ = writer.send(QueuedWrite::Close { completion }).await;
    let _ = closed.await;
    let _ = state_tx.send(ConnectionState::Closed);
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

struct FramedRequestAdmission {
    _inflight: AdmissionPermit,
    _queued: AdmissionPermit,
    _processor: AdmissionPermit,
}

fn acquire_framed_request(
    admission: &AdmissionController,
    scope: AdmissionScope,
    bytes: usize,
    class: AdmissionClass,
) -> Result<FramedRequestAdmission, AdmissionError> {
    let inflight = admission.try_acquire(AdmissionResource::Inflight, scope, bytes, class)?;
    let queued = admission.try_acquire(AdmissionResource::Queued, scope, bytes, class)?;
    let processor = admission.try_acquire(AdmissionResource::Processor, scope, bytes, class)?;
    Ok(FramedRequestAdmission {
        _inflight: inflight,
        _queued: queued,
        _processor: processor,
    })
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
    service_context: ServiceContext,
    config: TransportServerConfig,
    processor: Arc<dyn RequestProcessor>,
    admission: Arc<AdmissionController>,
    tls: TlsServerRuntime,
    started: AtomicBool,
    next_session: AtomicU64,
    security: Arc<TransportSecurity>,
    principal: Option<Principal>,
}

struct RequestAdmission {
    _inflight: AdmissionPermit,
    _queued: AdmissionPermit,
    _processor: AdmissionPermit,
}

impl TransportServer {
    pub async fn bind(
        service_context: ServiceContext,
        config: TransportServerConfig,
        processor: Arc<dyn RequestProcessor>,
        admission: Arc<AdmissionController>,
    ) -> RocketMQResult<Arc<Self>> {
        Self::bind_with_security(
            service_context,
            config,
            processor,
            admission,
            Arc::new(TransportSecurity::new(None, None)),
            None,
        )
        .await
    }

    pub async fn bind_with_security(
        service_context: ServiceContext,
        config: TransportServerConfig,
        processor: Arc<dyn RequestProcessor>,
        admission: Arc<AdmissionController>,
        security: Arc<TransportSecurity>,
        principal: Option<Principal>,
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
            security,
            principal,
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
                let Ok(connection_permit) =
                    server
                        .admission
                        .try_acquire(AdmissionResource::Connection, scope, 0, AdmissionClass::Data)
                else {
                    drop(stream);
                    continue;
                };
                let session = server.clone();
                let session_context = server.service_context.child(format!("transport.session.{session_id}"));
                let session_task_context = session_context.clone();
                if session_context
                    .spawn_service("transport.session", async move {
                        let _connection_permit = connection_permit;
                        session
                            .run_session(stream, remote_addr, session_id, session_task_context)
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
        session_context: ServiceContext,
    ) {
        let scope = AdmissionScope::new(remote_addr.ip()).with_session(session_id);
        let Ok(_handshake_permit) =
            self.admission
                .try_acquire(AdmissionResource::Handshake, scope, 0, AdmissionClass::Data)
        else {
            return;
        };
        let handshake_deadline = tokio::time::Instant::now() + self.config.handshake_timeout;
        let Ok(Some(negotiated)) =
            tokio::time::timeout_at(handshake_deadline, self.tls.negotiate_connection(stream, remote_addr)).await
        else {
            return;
        };
        let (connection, peer_is_tls) = negotiated.into_parts();
        let mut connection = connection.into_session_connection();
        drop(_handshake_permit);

        loop {
            let request_deadline = tokio::time::Instant::now() + self.config.request_timeout;
            let (request, bytes) =
                match tokio::time::timeout_at(request_deadline, connection.receive_command_with_retained_bytes()).await
                {
                    Ok(Some(Ok(request))) => request,
                    Ok(Some(Err(_))) | Ok(None) | Err(_) => break,
                };
            let peer = PeerInfo::new(remote_addr, peer_is_tls);
            let decision = self.security.authorize(
                &request,
                Some(&peer),
                self.principal.as_ref(),
                Resource::new(ResourceKind::Other, request.code().to_string()),
                Action::Manage,
            );
            if let Decision::Deny { reason } = decision {
                let denied = RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::NoPermission,
                    reason.to_string(),
                )
                .set_opaque(request.opaque());
                if tokio::time::timeout_at(request_deadline, connection.send_command(denied))
                    .await
                    .ok()
                    .and_then(Result::ok)
                    .is_none()
                {
                    break;
                }
                continue;
            }
            let class = AdmissionClass::for_request_code(request.code());
            let _admission = match self.acquire_request(scope, bytes, class) {
                Ok(admission) => admission,
                Err(error) => {
                    if error.policy() == FullPolicy::CloseConnection {
                        break;
                    }
                    let rejection = RemotingCommand::create_response_command_with_code_remark(
                        ResponseCode::SystemBusy,
                        error.to_string(),
                    )
                    .set_opaque(request.opaque());
                    if tokio::time::timeout_at(request_deadline, connection.send_command(rejection))
                        .await
                        .ok()
                        .and_then(Result::ok)
                        .is_none()
                    {
                        break;
                    }
                    continue;
                }
            };
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let processor = self.processor.clone();
            let processor_context = session_context.child("transport.processor");
            let processor_task = match processor_context.spawn_service("transport.processor", async move {
                let _ = sender.send(processor.process(request).await);
            }) {
                Ok(task_id) => task_id,
                Err(_) => break,
            };
            let response = match tokio::time::timeout_at(request_deadline, receiver).await {
                Ok(Ok(Ok(response))) => response,
                Ok(Ok(Err(_))) | Ok(Err(_)) | Err(_) => {
                    processor_context.task_group().abort_task(processor_task);
                    break;
                }
            };
            if tokio::time::timeout_at(request_deadline, connection.send_command(response))
                .await
                .ok()
                .and_then(Result::ok)
                .is_none()
            {
                break;
            }
        }
        let _ = connection.shutdown().await;
    }

    fn acquire_request(
        &self,
        scope: AdmissionScope,
        bytes: usize,
        class: AdmissionClass,
    ) -> Result<RequestAdmission, AdmissionError> {
        let inflight = self
            .admission
            .try_acquire(AdmissionResource::Inflight, scope, bytes, class)?;
        let queued = self
            .admission
            .try_acquire(AdmissionResource::Queued, scope, bytes, class)?;
        let processor = self
            .admission
            .try_acquire(AdmissionResource::Processor, scope, bytes, class)?;
        Ok(RequestAdmission {
            _inflight: inflight,
            _queued: queued,
            _processor: processor,
        })
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

    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_runtime::RuntimeContext;
    use tokio::sync::oneshot;
    use tokio::sync::Notify;

    use super::ConnectionHandler;
    use super::SessionHandle;
    use crate::admission::AdmissionController;
    use crate::admission::AdmissionLimits;
    use crate::connection::Connection;
    use crate::security::TransportSecurity;

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
            Connection::new_with_stream(transport),
            local_addr,
            remote_addr,
            service.task_group().clone(),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
            Arc::new(TransportSecurity::new(None, None)),
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
        let mut peer = Connection::new_with_stream(peer_stream);
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
            Connection::new_with_stream(transport),
            local_addr,
            remote_addr,
            service.task_group().clone(),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
            Arc::new(TransportSecurity::new(None, None)),
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
}
