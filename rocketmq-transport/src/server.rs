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

use futures_util::FutureExt;
use futures_util::StreamExt;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_error::SharedRocketMQError;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::RemotingCommandType;
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
use crate::admission::PartialFramePermit;
use crate::base::pending_request_table::PendingRequestOwner;
use crate::base::pending_request_table::PendingRequestTable;
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
use crate::dispatch::reserve_session_owner;
use crate::dispatch::AuthorizedDispatchBoundary;
use crate::dispatch::AuthorizedDispatchSession;
use crate::dispatch::DeferredSessionCleanupReport;
use crate::dispatch::NetworkResponsePlanContext;
use crate::dispatch::OriginalRequestIdentity;
use crate::dispatch::RequestContext;
use crate::dispatch::ResponseSink;
use crate::file_region::FileTransferMode;
use crate::net::channel::Channel;
use crate::net::channel::ChannelInner;
use crate::proxy_protocol::read_proxy_protocol;
use crate::proxy_protocol::ProxyProtocolConfig;
use crate::proxy_protocol::ProxyProtocolMetadata;
use crate::request_ordering::RequestOrdering;
use crate::security::TransportSecurity;
use crate::session_executor::SessionExecutorDrainReport;
use crate::session_view::SessionView;
use crate::telemetry::TransportTelemetry;
use crate::tls::NegotiatedConnection;
use crate::tls::TlsServerRuntime;
use crate::writer_runtime::run_session_writer;
use crate::writer_runtime::writer_lanes;
use crate::writer_runtime::WriterLanes;
use crate::writer_runtime::WriterQueueConfig;

const SESSION_RETIREMENT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SessionWriterCompletionHealth {
    Completed,
    Failed,
}

#[derive(Debug)]
struct SessionWriterCompletionReport {
    health: SessionWriterCompletionHealth,
    snapshot: SessionWriterSnapshot,
    failure: Option<SharedRocketMQError>,
}

impl SessionWriterCompletionReport {
    fn new(result: RocketMQResult<()>, snapshot: SessionWriterSnapshot) -> Self {
        let failure = result.err().map(SharedRocketMQError::new);
        Self {
            health: if failure.is_some() {
                SessionWriterCompletionHealth::Failed
            } else {
                SessionWriterCompletionHealth::Completed
            },
            snapshot,
            failure,
        }
    }

    fn is_healthy(&self) -> bool {
        self.health == SessionWriterCompletionHealth::Completed
            && self.snapshot.queued_items == 0
            && self.snapshot.queued_bytes == 0
            && self.snapshot.control_queued_items == 0
            && self.snapshot.control_queued_bytes == 0
            && self.snapshot.data_queued_items == 0
            && self.snapshot.data_queued_bytes == 0
    }
}

#[derive(Debug)]
struct SessionCloseReport {
    executor: SessionExecutorDrainReport,
    deferred_cleanup: DeferredSessionCleanupReport,
    remaining_wait_permits: usize,
    remaining_server_outbound_leases: usize,
    disconnected_panicked: bool,
    writer: SessionWriterCompletionReport,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SessionCloseCompletionSnapshot {
    pub(crate) healthy: bool,
    pub(crate) active_inline_tasks: usize,
    pub(crate) active_resume_tasks: usize,
    pub(crate) remaining_inline_tasks: usize,
    pub(crate) remaining_resume_tasks: usize,
    pub(crate) removed_waiters: usize,
    pub(crate) cleanup_panicked_targets: usize,
    pub(crate) remaining_wait_permits: usize,
    pub(crate) remaining_server_outbound_leases: usize,
    pub(crate) disconnected_panicked: bool,
    pub(crate) writer_healthy: bool,
    pub(crate) writer_queued_items: usize,
    pub(crate) writer_queued_bytes: usize,
}

#[derive(Clone)]
struct SessionCloseCompletion {
    snapshot: SessionCloseCompletionSnapshot,
    error: Option<SharedRocketMQError>,
}

struct SessionCloseCoordinator {
    completion: tokio::sync::watch::Sender<Option<Arc<SessionCloseCompletion>>>,
}

struct SessionCloseCompletionGuard {
    session: SessionHandle,
    armed: bool,
}

struct ServerOutboundAdmission {
    accepting: AtomicBool,
    active: AtomicUsize,
}

pub(crate) struct ServerOutboundLease {
    admission: Arc<ServerOutboundAdmission>,
}

impl ServerOutboundAdmission {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            accepting: AtomicBool::new(true),
            active: AtomicUsize::new(0),
        })
    }

    fn acquire(self: &Arc<Self>) -> Option<ServerOutboundLease> {
        if !self.accepting.load(Ordering::Acquire) {
            return None;
        }
        self.active.fetch_add(1, Ordering::AcqRel);
        if self.accepting.load(Ordering::Acquire) {
            Some(ServerOutboundLease {
                admission: Arc::clone(self),
            })
        } else {
            self.active.fetch_sub(1, Ordering::AcqRel);
            None
        }
    }

    fn close(&self) {
        self.accepting.store(false, Ordering::Release);
    }

    fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }

    fn is_closed(&self) -> bool {
        !self.accepting.load(Ordering::Acquire)
    }
}

impl Drop for ServerOutboundLease {
    fn drop(&mut self) {
        self.admission.active.fetch_sub(1, Ordering::AcqRel);
    }
}

impl SessionCloseCompletionGuard {
    fn new(session: SessionHandle) -> Self {
        Self { session, armed: true }
    }

    fn complete(&mut self, report: &SessionCloseReport) {
        self.session.complete_close(report);
        self.armed = false;
    }
}

impl Drop for SessionCloseCompletionGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.session.abort();
        let writer = self.session.writer_snapshot();
        let error = SharedRocketMQError::new(RocketMQError::network_connection_failed(
            "transport-session-close",
            "session finalizer exited before ordered close completion",
        ));
        self.session.send.close_coordinator.complete(SessionCloseCompletion {
            snapshot: SessionCloseCompletionSnapshot {
                healthy: false,
                active_inline_tasks: 0,
                active_resume_tasks: 0,
                remaining_inline_tasks: self.session.request_operation.active_task_count(),
                remaining_resume_tasks: 0,
                removed_waiters: 0,
                cleanup_panicked_targets: 0,
                remaining_wait_permits: 0,
                remaining_server_outbound_leases: self.session.send.server_outbound.active(),
                disconnected_panicked: false,
                writer_healthy: false,
                writer_queued_items: writer.queued_items,
                writer_queued_bytes: writer.queued_bytes,
            },
            error: Some(error),
        });
    }
}

impl SessionCloseCoordinator {
    fn new() -> Self {
        let (completion, _) = tokio::sync::watch::channel(None);
        Self { completion }
    }

    fn complete(&self, completion: SessionCloseCompletion) {
        self.completion.send_replace(Some(Arc::new(completion)));
    }

    async fn completed(&self) -> RocketMQResult<Arc<SessionCloseCompletion>> {
        let mut completion = self.completion.subscribe();
        loop {
            if let Some(completed) = completion.borrow().clone() {
                tracing::debug!(
                    healthy = completed.snapshot.healthy,
                    active_inline_tasks = completed.snapshot.active_inline_tasks,
                    active_resume_tasks = completed.snapshot.active_resume_tasks,
                    remaining_inline_tasks = completed.snapshot.remaining_inline_tasks,
                    remaining_resume_tasks = completed.snapshot.remaining_resume_tasks,
                    removed_waiters = completed.snapshot.removed_waiters,
                    cleanup_panicked_targets = completed.snapshot.cleanup_panicked_targets,
                    remaining_wait_permits = completed.snapshot.remaining_wait_permits,
                    remaining_server_outbound_leases = completed.snapshot.remaining_server_outbound_leases,
                    disconnected_panicked = completed.snapshot.disconnected_panicked,
                    writer_healthy = completed.snapshot.writer_healthy,
                    writer_queued_items = completed.snapshot.writer_queued_items,
                    writer_queued_bytes = completed.snapshot.writer_queued_bytes,
                    "transport session close waiter observed completion"
                );
                return Ok(completed);
            }
            completion.changed().await.map_err(|_| {
                RocketMQError::network_connection_failed(
                    "transport-session-close",
                    "session close coordinator retired before completion",
                )
            })?;
        }
    }

    async fn wait(&self) -> RocketMQResult<SessionCloseCompletionSnapshot> {
        let completed = self.completed().await?;
        match &completed.error {
            Some(error) => Err(error.clone().into_error()),
            None => Ok(completed.snapshot),
        }
    }
}

impl SessionCloseReport {
    fn is_healthy(&self) -> bool {
        self.executor.is_healthy()
            && self.deferred_cleanup.is_healthy()
            && self.remaining_wait_permits == 0
            && self.remaining_server_outbound_leases == 0
            && !self.disconnected_panicked
            && self.writer.is_healthy()
    }

    fn log(&self, session_id: u64) {
        if self.is_healthy() {
            tracing::debug!(
                session_id,
                active_inline_tasks = self.executor.active_inline_tasks,
                active_resume_tasks = self.executor.active_resume_tasks,
                remaining_inline_tasks = self.executor.remaining_inline_tasks,
                remaining_resume_tasks = self.executor.remaining_resume_tasks,
                cleanup_outcome = ?self.deferred_cleanup.outcome,
                registered_waiters = self.deferred_cleanup.registered_waiters,
                removed_waiters = self.deferred_cleanup.removed_waiters,
                cleanup_panicked_targets = self.deferred_cleanup.panicked_targets,
                cleanup_remaining_wait_permits = self.deferred_cleanup.remaining_wait_permits,
                remaining_wait_permits = self.remaining_wait_permits,
                remaining_server_outbound_leases = self.remaining_server_outbound_leases,
                disconnected_panicked = self.disconnected_panicked,
                writer_completion_healthy = true,
                writer_queued_items = self.writer.snapshot.queued_items,
                writer_queued_bytes = self.writer.snapshot.queued_bytes,
                writer_failed_writes = self.writer.snapshot.failed,
                "transport session shutdown report completed"
            );
        } else {
            self.executor.shutdown.log_if_unhealthy();
            tracing::warn!(
                session_id,
                active_inline_tasks = self.executor.active_inline_tasks,
                active_resume_tasks = self.executor.active_resume_tasks,
                remaining_inline_tasks = self.executor.remaining_inline_tasks,
                remaining_resume_tasks = self.executor.remaining_resume_tasks,
                cleanup_outcome = ?self.deferred_cleanup.outcome,
                registered_waiters = self.deferred_cleanup.registered_waiters,
                removed_waiters = self.deferred_cleanup.removed_waiters,
                cleanup_panicked_targets = self.deferred_cleanup.panicked_targets,
                cleanup_remaining_wait_permits = self.deferred_cleanup.remaining_wait_permits,
                remaining_wait_permits = self.remaining_wait_permits,
                remaining_server_outbound_leases = self.remaining_server_outbound_leases,
                disconnected_panicked = self.disconnected_panicked,
                writer_completion_healthy = self.writer.is_healthy(),
                writer_queued_items = self.writer.snapshot.queued_items,
                writer_queued_bytes = self.writer.snapshot.queued_bytes,
                writer_failed_writes = self.writer.snapshot.failed,
                "transport session shutdown report is unhealthy"
            );
        }
    }

    fn completion(&self) -> SessionCloseCompletion {
        let snapshot = SessionCloseCompletionSnapshot {
            healthy: self.is_healthy(),
            active_inline_tasks: self.executor.active_inline_tasks,
            active_resume_tasks: self.executor.active_resume_tasks,
            remaining_inline_tasks: self.executor.remaining_inline_tasks,
            remaining_resume_tasks: self.executor.remaining_resume_tasks,
            removed_waiters: self.deferred_cleanup.removed_waiters,
            cleanup_panicked_targets: self.deferred_cleanup.panicked_targets,
            remaining_wait_permits: self.remaining_wait_permits,
            remaining_server_outbound_leases: self.remaining_server_outbound_leases,
            disconnected_panicked: self.disconnected_panicked,
            writer_healthy: self.writer.is_healthy(),
            writer_queued_items: self.writer.snapshot.queued_items,
            writer_queued_bytes: self.writer.snapshot.queued_bytes,
        };
        let error = self.writer.failure.clone().or_else(|| {
            (!snapshot.healthy).then(|| {
                SharedRocketMQError::new(RocketMQError::network_connection_failed(
                    "transport-session-close",
                    "session close report is unhealthy",
                ))
            })
        });
        SessionCloseCompletion { snapshot, error }
    }
}

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
    request_sequence: AtomicU64,
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
    session_closed_tx: tokio::sync::watch::Sender<bool>,
    session_view: SessionView,
    task_group: TaskGroup,
    reader_cancellation: CancellationToken,
    server_outbound: Arc<ServerOutboundAdmission>,
    close_coordinator: SessionCloseCoordinator,
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
    original_request_identity: Option<OriginalRequestIdentity>,
    response_plan_context: Option<NetworkResponsePlanContext>,
}

struct SessionRetirementGuard<'a> {
    session: &'a SessionHandle,
    armed: bool,
}

impl<'a> SessionRetirementGuard<'a> {
    fn new(session: &'a SessionHandle) -> Self {
        Self { session, armed: true }
    }

    fn complete(&mut self) {
        self.armed = false;
    }
}

impl Drop for SessionRetirementGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.session.abort();
        }
    }
}

impl SessionHandle {
    pub fn session_id(&self) -> u64 {
        self.send.session_id
    }

    #[allow(
        dead_code,
        reason = "DSP-05 bridge ownership remains dormant until DSP-06 coexistence routing"
    )]
    pub(crate) fn same_canonical_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.send, &other.send)
    }

    #[allow(
        dead_code,
        reason = "DSP-05 bridge construction remains dormant until DSP-06 coexistence routing"
    )]
    pub(crate) fn legacy_processor_channel(
        &self,
        response: ResponseSink,
        response_table: PendingRequestTable,
        pending_request_owner: PendingRequestOwner,
    ) -> RocketMQResult<Channel> {
        let inner = ChannelInner::new_legacy_network_bridge(
            self.connection(),
            response,
            response_table,
            pending_request_owner,
            self.task_group().clone(),
        )?;
        Ok(Channel::new_canonical_network(Arc::new(inner), self.clone()))
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
        .with_response_plan_drop(
            self.response_plan_context
                .as_ref()
                .map(NetworkResponsePlanContext::transport_drop_handle),
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

    #[allow(
        dead_code,
        reason = "REQ-04 retains the canonical session view for the REQ-06 request builder"
    )]
    pub(crate) fn session_view(&self) -> SessionView {
        self.send.session_view.clone()
    }

    pub(crate) fn original_request_identity(&self) -> Option<OriginalRequestIdentity> {
        self.original_request_identity
    }

    pub(crate) fn is_same_session(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.send, &other.send)
    }

    pub(crate) fn abort(&self) {
        self.send.server_outbound.close();
        let _ = self.send.session_closed_tx.send(true);
        let _ = self.send.state_tx.send(ConnectionState::Closed);
        self.send.reader_cancellation.cancel();
        self.request_operation.cancel();
        self.send.writer_operation.cancel();
        self.send.task_group.abort_task(self.send.writer_task_id);
    }

    /// Cancels both socket halves and waits for the owned writer task to exit.
    /// A stuck writer is force-aborted after the normal retirement deadline.
    pub(crate) async fn terminate(&self) {
        self.send.server_outbound.close();
        let _ = self.send.session_closed_tx.send(true);
        let _ = self.send.state_tx.send(ConnectionState::Closed);
        self.send.reader_cancellation.cancel();
        self.request_operation.cancel();
        self.send.writer_operation.cancel();
        if !self
            .send
            .task_group
            .wait_task(self.send.writer_task_id, SESSION_RETIREMENT_TIMEOUT)
            .await
        {
            self.send.task_group.abort_task(self.send.writer_task_id);
        }
    }

    pub(crate) fn with_response_class(mut self, class: AdmissionClass) -> Self {
        self.response_class = Some(class);
        self
    }

    pub(crate) fn with_operation_context(mut self, operation: OperationContext) -> Self {
        self.request_operation = operation;
        self
    }

    pub(crate) fn with_original_request_identity(mut self, identity: Option<OriginalRequestIdentity>) -> Self {
        self.original_request_identity = identity;
        self
    }

    #[allow(
        dead_code,
        reason = "RSP-05 private response plan context is installed by later dispatcher wiring"
    )]
    pub(crate) fn with_response_plan_context(mut self, context: NetworkResponsePlanContext) -> Self {
        self.response_plan_context = Some(context);
        self
    }

    #[allow(
        dead_code,
        reason = "RSP-05 private response plan context is consumed by later dispatcher wiring"
    )]
    pub(crate) fn response_plan_context(&self) -> Option<&NetworkResponsePlanContext> {
        self.response_plan_context.as_ref()
    }

    /// Requests the server-owned ordered session close and waits for its full completion.
    ///
    /// The reader owner performs deferred cleanup, executor drain, disconnect notification, and
    /// the sole writer retirement before this method completes. Concurrent or repeated calls wait
    /// for the same completion.
    ///
    /// # Errors
    ///
    /// Returns an error if any stage of the server-owned close report is unhealthy or if the close
    /// coordinator terminates without publishing completion.
    pub async fn retire(&self) -> rocketmq_error::RocketMQResult<()> {
        self.request_close();
        self.wait_for_close_completion().await.map(|_| ())
    }

    pub(crate) fn request_close(&self) {
        self.send.server_outbound.close();
        let _ = self.send.session_closed_tx.send(true);
        self.send.reader_cancellation.cancel();
    }

    pub(crate) fn acquire_server_outbound(&self, target: &'static str) -> RocketMQResult<ServerOutboundLease> {
        self.send.server_outbound.acquire().ok_or_else(|| {
            RocketMQError::network_connection_failed(target, "server-originated outbound admission is closed")
        })
    }

    pub(crate) fn close_requested(&self) -> bool {
        self.send.server_outbound.is_closed()
    }

    pub(crate) async fn wait_for_close_completion(&self) -> RocketMQResult<SessionCloseCompletionSnapshot> {
        self.send.close_coordinator.wait().await
    }

    #[cfg(test)]
    pub(crate) async fn close_completion_snapshot(&self) -> RocketMQResult<SessionCloseCompletionSnapshot> {
        Ok(self.send.close_coordinator.completed().await?.snapshot)
    }

    fn complete_close(&self, report: &SessionCloseReport) {
        self.send.close_coordinator.complete(report.completion());
    }

    async fn retire_writer_owned(&self) -> rocketmq_error::RocketMQResult<()> {
        self.retire_with_timeout_inner(SESSION_RETIREMENT_TIMEOUT, None).await
    }

    async fn retire_with_timeout_inner(
        &self,
        timeout: Duration,
        started: Option<tokio::sync::oneshot::Sender<()>>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let mut retirement_guard = SessionRetirementGuard::new(self);
        let result = match tokio::time::timeout(timeout, self.retire_inner(started)).await {
            Ok(result) => result,
            Err(_) => {
                self.abort();
                Err(rocketmq_error::RocketMQError::network_connection_failed(
                    "transport-session-writer",
                    "writer retirement exceeded its absolute deadline",
                ))
            }
        };
        retirement_guard.complete();
        result
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
            let _ = self.send.session_closed_tx.send(true);
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
        let _ = self.send.session_closed_tx.send(true);
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

/// Statically selected frame route used by production authorized servers.
///
/// The associated state makes per-session capabilities generation-specific
/// without a runtime mode tag or a processor trait object.
pub(crate) trait AuthorizedFrameRoute: Send + Sync + 'static {
    type SessionState: Send + Sync + 'static;

    fn connected(&self, session: SessionHandle) -> impl Future<Output = Option<Self::SessionState>> + Send;

    fn response(
        &self,
        state: &Self::SessionState,
        session: SessionHandle,
        command: RemotingCommand,
    ) -> impl Future<Output = ()> + Send;

    #[allow(clippy::too_many_arguments)]
    fn request(
        &self,
        state: &Self::SessionState,
        authorized_session: &AuthorizedDispatchSession,
        session: SessionHandle,
        context: RequestContext,
        command: RemotingCommand,
        received_at: std::time::Instant,
        retained_bytes: usize,
        partial_frame_permit: Option<PartialFramePermit>,
    ) -> impl Future<Output = bool> + Send;

    fn close_pending(&self, state: &Self::SessionState, session: SessionHandle) -> DeferredSessionCleanupReport;

    fn disconnected(&self, state: Self::SessionState, session: SessionHandle) -> impl Future<Output = usize> + Send;
}

struct CompatibilityFrameRoute<H> {
    handler: Arc<H>,
}

impl<H> AuthorizedFrameRoute for CompatibilityFrameRoute<H>
where
    H: ConnectionHandler,
{
    type SessionState = ();

    async fn connected(&self, session: SessionHandle) -> Option<Self::SessionState> {
        self.handler.connected(session).await;
        Some(())
    }

    async fn response(&self, _state: &Self::SessionState, session: SessionHandle, command: RemotingCommand) {
        self.handler.command(session, command).await;
    }

    async fn request(
        &self,
        _state: &Self::SessionState,
        authorized_session: &AuthorizedDispatchSession,
        session: SessionHandle,
        context: RequestContext,
        command: RemotingCommand,
        _received_at: std::time::Instant,
        retained_bytes: usize,
        partial_frame_permit: Option<PartialFramePermit>,
    ) -> bool {
        let original = session.original_request_identity();
        let class = AdmissionClass::for_request_code(
            original.map_or_else(|| command.code(), OriginalRequestIdentity::original_code),
        );
        let ordering = self.handler.request_ordering(&command);
        let request_handler = Arc::clone(&self.handler);
        let request_session = session.clone().with_response_class(class);
        let response = ResponseSink::Network(Arc::new(session.clone().with_response_class(class)));
        authorized_session
            .dispatch(
                context,
                original,
                command,
                retained_bytes,
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
            .is_ok()
    }

    fn close_pending(&self, _state: &Self::SessionState, _session: SessionHandle) -> DeferredSessionCleanupReport {
        DeferredSessionCleanupReport::empty_completed()
    }

    async fn disconnected(&self, _state: Self::SessionState, session: SessionHandle) -> usize {
        self.handler.disconnected(session).await;
        0
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
    telemetry: TransportTelemetry,
    socket_options: SocketOptions,
    file_region_blocking: Option<BlockingExecutor>,
    file_transfer_mode: FileTransferMode,
    frame_limits: FrameLimits,
    proxy_protocol: ProxyProtocolConfig,
    #[cfg(test)]
    write_preflight_barrier: Option<crate::write_strategy::WritePreflightBarrier>,
    #[cfg(test)]
    test_request_deadline: Option<Duration>,
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
            telemetry: TransportTelemetry::noop(),
            socket_options: SocketOptions::default(),
            file_region_blocking: None,
            file_transfer_mode: FileTransferMode::Auto,
            frame_limits: FrameLimits::default(),
            proxy_protocol: ProxyProtocolConfig::default(),
            #[cfg(test)]
            write_preflight_barrier: None,
            #[cfg(test)]
            test_request_deadline: None,
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

    pub(crate) fn with_validated_frame_limits(mut self, frame_limits: FrameLimits) -> Self {
        debug_assert!(frame_limits.validate().is_ok());
        self.frame_limits = frame_limits;
        self
    }

    /// Applies the trusted PROXY protocol policy before TLS/application negotiation.
    pub fn try_with_proxy_protocol(mut self, config: ProxyProtocolConfig) -> RocketMQResult<Self> {
        config.validate()?;
        self.proxy_protocol = config;
        Ok(self)
    }

    pub(crate) fn with_validated_proxy_protocol(mut self, config: ProxyProtocolConfig) -> Self {
        debug_assert!(config.validate().is_ok());
        self.proxy_protocol = config;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_write_preflight_barrier(
        mut self,
        barrier: crate::write_strategy::WritePreflightBarrier,
    ) -> Self {
        self.write_preflight_barrier = Some(barrier);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_test_request_deadline(mut self, deadline: Duration) -> Self {
        self.test_request_deadline = Some(deadline);
        self
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

    #[allow(
        dead_code,
        reason = "preserved low-level ConnectionHandler compatibility entry point"
    )]
    pub async fn run<H>(self, handler: Arc<H>) -> RocketMQResult<()>
    where
        H: ConnectionHandler,
    {
        self.run_route(Arc::new(CompatibilityFrameRoute { handler })).await
    }

    pub(crate) async fn run_authorized<R>(self, route: Arc<R>) -> RocketMQResult<()>
    where
        R: AuthorizedFrameRoute,
    {
        self.run_route(route).await
    }

    async fn run_route<R>(self, route: Arc<R>) -> RocketMQResult<()>
    where
        R: AuthorizedFrameRoute,
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
            let Some(session_id) = reserve_session_owner() else {
                drop(stream);
                return Err(RocketMQError::network_connection_failed(
                    "transport-session-owner",
                    "process-local session owner namespace exhausted",
                ));
            };
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
            let route = route.clone();
            let telemetry = self.telemetry.clone();
            let file_region_blocking = self.file_region_blocking.clone();
            let file_transfer_mode = self.file_transfer_mode;
            let frame_limits = self.frame_limits;
            let proxy_protocol = self.proxy_protocol.clone();
            #[cfg(test)]
            let write_preflight_barrier = self.write_preflight_barrier.clone();
            #[cfg(test)]
            let test_request_deadline = self.test_request_deadline;
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
                    #[cfg(test)]
                    let mut connection = connection;
                    #[cfg(test)]
                    if let Some(barrier) = write_preflight_barrier {
                        connection.set_write_preflight_barrier(barrier);
                    }
                    let mut connection = connection.with_telemetry(telemetry);
                    if let Some(blocking) = file_region_blocking {
                        connection = connection.with_file_region_io(blocking, file_transfer_mode);
                    }
                    run_authorized_framed_session(
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
                        #[cfg(test)]
                        test_request_deadline,
                        route,
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
    run_framed_session_with_request_sequence(
        connection,
        local_addr,
        remote_addr,
        transport_peer_addr,
        proxy_protocol,
        session_id,
        scope,
        task_group,
        dispatch,
        principal,
        peer_is_tls,
        io_policy,
        AtomicU64::new(1),
        #[cfg(test)]
        None,
        handler,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn run_framed_session_with_request_sequence<H>(
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
    request_sequence: AtomicU64,
    #[cfg(test)] request_identity_exhausted: Option<tokio::sync::oneshot::Sender<()>>,
    handler: Arc<H>,
) where
    H: ConnectionHandler,
{
    run_authorized_framed_session_with_request_sequence(
        connection,
        local_addr,
        remote_addr,
        transport_peer_addr,
        proxy_protocol,
        session_id,
        scope,
        task_group,
        dispatch,
        principal,
        peer_is_tls,
        io_policy,
        request_sequence,
        #[cfg(test)]
        request_identity_exhausted,
        #[cfg(test)]
        None,
        Arc::new(CompatibilityFrameRoute { handler }),
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn run_authorized_framed_session<R>(
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
    #[cfg(test)] test_request_deadline: Option<Duration>,
    route: Arc<R>,
) where
    R: AuthorizedFrameRoute,
{
    run_authorized_framed_session_with_request_sequence(
        connection,
        local_addr,
        remote_addr,
        transport_peer_addr,
        proxy_protocol,
        session_id,
        scope,
        task_group,
        dispatch,
        principal,
        peer_is_tls,
        io_policy,
        AtomicU64::new(1),
        #[cfg(test)]
        None,
        #[cfg(test)]
        test_request_deadline,
        route,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn run_authorized_framed_session_with_request_sequence<R>(
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
    request_sequence: AtomicU64,
    #[cfg(test)] mut request_identity_exhausted: Option<tokio::sync::oneshot::Sender<()>>,
    #[cfg(test)] test_request_deadline: Option<Duration>,
    route: Arc<R>,
) where
    R: AuthorizedFrameRoute,
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
    let (session_closed_tx, session_closed_rx) = tokio::sync::watch::channel(false);
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
    let session_view = SessionView::network(
        session_id,
        local_addr,
        remote_addr,
        transport_peer_addr,
        proxy_protocol.as_deref(),
        state_rx.clone(),
        session_closed_rx,
    );
    let session = SessionHandle {
        send: Arc::new(SessionSendHandle {
            session_id,
            request_sequence,
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
            session_closed_tx,
            session_view,
            task_group: task_group.clone(),
            reader_cancellation: reader_cancellation.clone(),
            server_outbound: ServerOutboundAdmission::new(),
            close_coordinator: SessionCloseCoordinator::new(),
            writer_operation,
            lifecycle,
            writer_task_id,
            telemetry,
        }),
        request_operation: request_operation.clone(),
        response_class: None,
        original_request_identity: None,
        response_plan_context: None,
    };
    let mut close_completion = SessionCloseCompletionGuard::new(session.clone());

    let Some(route_state) = route.connected(session.clone()).await else {
        session.request_close();
        executor.begin_close();
        let request_deadline = task_group
            .shutdown_deadline()
            .unwrap_or_else(|| ShutdownDeadline::after(SESSION_RETIREMENT_TIMEOUT));
        let report = SessionCloseReport {
            executor: executor.drain_report_until(request_deadline).await,
            deferred_cleanup: DeferredSessionCleanupReport::empty_completed(),
            remaining_wait_permits: 0,
            remaining_server_outbound_leases: session.send.server_outbound.active(),
            disconnected_panicked: false,
            writer: SessionWriterCompletionReport::new(session.retire_writer_owned().await, session.writer_snapshot()),
        };
        report.log(session.session_id());
        close_completion.complete(&report);
        return;
    };
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
        let received_at = std::time::Instant::now();
        let command = decoded.command;
        session
            .send
            .telemetry
            .record_inbound_decoded_plaintext_bytes(decoded.retained_frame_bytes);

        if command.get_type() == RemotingCommandType::RESPONSE {
            route.response(&route_state, session.clone(), command).await;
            continue;
        }

        let Some(original_request_identity) =
            OriginalRequestIdentity::capture(session_id, &session.send.request_sequence, &command)
        else {
            #[cfg(test)]
            if let Some(signal) = request_identity_exhausted.take() {
                let _ = signal.send(());
            }
            tracing::error!(
                reason = "sequence_exhausted",
                "transport session stopped accepting because request identity allocation is exhausted"
            );
            break;
        };
        let partial_frame_permit = decoded.partial_frame_permit;
        let class = AdmissionClass::for_request_code(original_request_identity.original_code());
        let bytes = decoded.retained_frame_bytes;
        #[cfg(test)]
        let request_deadline = test_request_deadline.map(crate::deadline::RequestDeadline::after);
        #[cfg(not(test))]
        let request_deadline = None;
        let context = RequestContext::network_with_security_profile(
            PeerInfo::new(remote_addr, peer_is_tls),
            principal.clone(),
            request_deadline,
            dispatch.security_profile(),
        );
        let request_session = session
            .clone()
            .with_response_class(class)
            .with_original_request_identity(Some(original_request_identity));
        if !route
            .request(
                &route_state,
                &executor,
                request_session,
                context,
                command,
                received_at,
                bytes,
                partial_frame_permit,
            )
            .await
        {
            break;
        }
    }
    // Request dispatchers may still be draining accepted work, but the reader
    // no longer accepts frames. Publish the session-close transition now so
    // read-only views observe shutdown without closing the response writer.
    session.request_close();
    executor.begin_close();
    let deferred_cleanup = route.close_pending(&route_state, session.clone());
    let request_deadline = task_group
        .shutdown_deadline()
        .unwrap_or_else(|| ShutdownDeadline::after(SESSION_RETIREMENT_TIMEOUT));
    let executor_drain = executor.drain_report_until(request_deadline).await;
    let disconnected = std::panic::AssertUnwindSafe(route.disconnected(route_state, session.clone()))
        .catch_unwind()
        .await;
    let (remaining_wait_permits, disconnected_panicked) = match disconnected {
        Ok(remaining_wait_permits) => (remaining_wait_permits, false),
        Err(_) => (0, true),
    };
    let writer_completion =
        SessionWriterCompletionReport::new(session.retire_writer_owned().await, session.writer_snapshot());
    let report = SessionCloseReport {
        executor: executor_drain,
        deferred_cleanup,
        remaining_wait_permits,
        remaining_server_outbound_leases: session.send.server_outbound.active(),
        disconnected_panicked,
        writer: writer_completion,
    };
    report.log(session.session_id());
    close_completion.complete(&report);
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

/// Runs an already-connected client socket through a statically selected V2
/// route and its existing authorization boundary.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_connected_session_authorized<R>(
    connection: Connection,
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
    task_group: TaskGroup,
    dispatch: Arc<AuthorizedDispatchBoundary>,
    principal: Option<Principal>,
    idle_timeout: Duration,
    route: Arc<R>,
) where
    R: AuthorizedFrameRoute,
{
    let Some(session_id) = reserve_session_owner() else {
        tracing::error!(
            reason = "owner_exhausted",
            "connected V2 transport session rejected because request owner allocation is exhausted"
        );
        return;
    };
    let scope = AdmissionScope::new(remote_addr.ip()).with_session(session_id);
    let Ok(session_group) = task_group.try_child(format!("rocketmq.transport.session.{session_id}")) else {
        return;
    };
    run_authorized_framed_session(
        connection,
        local_addr,
        remote_addr,
        remote_addr,
        None,
        session_id,
        scope,
        session_group,
        dispatch,
        principal,
        false,
        SessionIoPolicy {
            idle_timeout,
            ..SessionIoPolicy::default()
        },
        #[cfg(test)]
        None,
        route,
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
    let Some(session_id) = reserve_session_owner() else {
        tracing::error!(
            reason = "owner_exhausted",
            "connected transport session rejected because request owner allocation is exhausted"
        );
        return;
    };
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
            let original_opaque = session
                .original_request_identity()
                .map(OriginalRequestIdentity::original_opaque);
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
            let response = if let Some(original_opaque) = original_opaque {
                response.set_opaque(original_opaque)
            } else {
                response
            };
            let mut connection = session.connection();
            let _ = connection.send_response(response).await;
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
                let Some(session_id) = reserve_session_owner() else {
                    drop(stream);
                    tracing::error!(
                        reason = "owner_exhausted",
                        "transport server stopped accepting because request owner allocation is exhausted"
                    );
                    break;
                };
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
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::task::Poll;
    use std::time::Duration;

    use rocketmq_error::NetworkError;
    use rocketmq_error::RocketMQError;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_protocol::protocol::RemotingCommandType;
    use rocketmq_runtime::RuntimeContext;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;
    use tokio::net::TcpStream;
    use tokio::sync::oneshot;
    use tokio::sync::Notify;

    use super::ConnectionHandler;
    use super::SessionCloseReport;
    use super::SessionHandle;
    use super::SessionWriterCompletionReport;
    use super::TransportListener;
    use crate::admission::AdmissionController;
    use crate::admission::AdmissionLimits;
    use crate::config::TlsConfig;
    #[cfg(feature = "tls")]
    use crate::config::TlsMode;
    use crate::connection::Connection;
    use crate::connection::SessionWriterSnapshot;
    use crate::deadline::RequestDeadline;
    use crate::proxy_protocol::ProxyProtocolConfig;
    use crate::security::TransportSecurity;
    use crate::session_executor::SessionExecutorDrainReport;
    use crate::session_view::SessionId;
    use crate::session_view::SessionView;
    use crate::tls::TlsServerRuntime;

    #[test]
    fn composite_close_report_requires_drained_tasks_waiters_and_writer() {
        let healthy = SessionCloseReport {
            executor: SessionExecutorDrainReport {
                shutdown: rocketmq_runtime::ShutdownReport::new("test.session", Duration::ZERO),
                active_inline_tasks: 1,
                active_resume_tasks: 1,
                remaining_inline_tasks: 0,
                remaining_resume_tasks: 0,
            },
            deferred_cleanup: crate::dispatch::DeferredSessionCleanupReport::empty_completed(),
            remaining_wait_permits: 0,
            remaining_server_outbound_leases: 0,
            disconnected_panicked: false,
            writer: SessionWriterCompletionReport::new(Ok(()), SessionWriterSnapshot::default()),
        };
        assert!(healthy.is_healthy());

        let failed_writer = SessionWriterCompletionReport::new(
            Err(RocketMQError::network_connection_failed(
                "test-session-writer",
                "writer completion failed",
            )),
            SessionWriterSnapshot::default(),
        );
        assert!(!failed_writer.is_healthy());
        let queued_writer = SessionWriterCompletionReport::new(
            Ok(()),
            SessionWriterSnapshot {
                queued_items: 1,
                ..SessionWriterSnapshot::default()
            },
        );
        assert!(!queued_writer.is_healthy());
    }

    struct CaptureSession {
        sender: std::sync::Mutex<Option<oneshot::Sender<SessionHandle>>>,
    }

    struct ObserveSessionRetirement {
        connected: std::sync::Mutex<Option<oneshot::Sender<SessionHandle>>>,
        disconnected: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    }

    struct CaptureRequestIdentities {
        connected: std::sync::Mutex<Option<oneshot::Sender<u64>>>,
        commands:
            tokio::sync::mpsc::UnboundedSender<(RemotingCommandType, Option<crate::dispatch::OriginalRequestIdentity>)>,
    }

    struct SequenceExhaustionLifecycle {
        calls: AtomicUsize,
        entered: std::sync::Mutex<Option<oneshot::Sender<crate::dispatch::OriginalRequestIdentity>>>,
        release: Arc<Notify>,
        disconnected: std::sync::Mutex<Option<oneshot::Sender<SessionHandle>>>,
    }

    struct ReaderExitObserver {
        connected: std::sync::Mutex<Option<oneshot::Sender<SessionHandle>>>,
        entered: std::sync::Mutex<Option<oneshot::Sender<()>>>,
        release: Arc<Notify>,
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

    impl ConnectionHandler for CaptureRequestIdentities {
        fn connected(&self, session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(async move {
                if let Some(sender) = self.connected.lock().expect("connected identity lock").take() {
                    let _ = sender.send(session.session_id());
                }
            })
        }

        fn command(
            &self,
            session: SessionHandle,
            command: RemotingCommand,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(async move {
                let _ = self
                    .commands
                    .send((command.get_type(), session.original_request_identity()));
            })
        }
    }

    impl ConnectionHandler for SequenceExhaustionLifecycle {
        fn connected(&self, _session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(async {})
        }

        fn command(
            &self,
            session: SessionHandle,
            command: RemotingCommand,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let identity = session
                    .original_request_identity()
                    .expect("accepted request must carry its identity");
                if let Some(sender) = self.entered.lock().expect("sequence entry lock").take() {
                    let _ = sender.send(identity);
                }
                self.release.notified().await;
                let mut connection = session.connection();
                let _ = connection
                    .send_response(RemotingCommand::create_response_command_with_code(0).set_opaque(command.opaque()))
                    .await;
            })
        }

        fn disconnected(&self, session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(async move {
                if let Some(sender) = self.disconnected.lock().expect("sequence disconnect lock").take() {
                    let _ = sender.send(session);
                }
            })
        }
    }

    impl ConnectionHandler for ReaderExitObserver {
        fn connected(&self, session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(async move {
                if let Some(sender) = self.connected.lock().expect("reader exit connected lock").take() {
                    let _ = sender.send(session);
                }
            })
        }

        fn command(
            &self,
            session: SessionHandle,
            command: RemotingCommand,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(async move {
                if let Some(sender) = self.entered.lock().expect("reader exit entered lock").take() {
                    let _ = sender.send(());
                }
                self.release.notified().await;
                let mut connection = session.connection();
                connection
                    .send_response(RemotingCommand::create_response_command_with_code(0).set_opaque(command.opaque()))
                    .await
                    .expect("accepted response should still reach the canonical writer");
            })
        }
    }

    #[tokio::test]
    async fn request_frames_share_session_owner_and_response_frames_do_not_consume_sequence() {
        let runtime = RuntimeContext::from_current("transport-request-identity-sequence-test");
        let service = runtime.service_context("transport-request-identity-sequence");
        let (transport, peer_stream) = tokio::io::duplex(4096);
        let (connected_tx, connected_rx) = oneshot::channel();
        let (commands_tx, mut commands_rx) = tokio::sync::mpsc::unbounded_channel();
        let handler = Arc::new(CaptureRequestIdentities {
            connected: std::sync::Mutex::new(Some(connected_tx)),
            commands: commands_tx,
        });
        let runner = tokio::spawn(super::run_connected_session(
            Connection::new_with_plaintext_stream(transport),
            "127.0.0.1:19101".parse().expect("local address"),
            "127.0.0.1:19102".parse().expect("remote address"),
            service.task_group().clone(),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
            Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
            None,
            Duration::from_secs(30),
            handler,
        ));
        let owner_id = connected_rx.await.expect("connected session owner");
        let mut peer = Connection::new_with_plaintext_stream(peer_stream);
        peer.send_command(RemotingCommand::create_response_command_with_code(0).set_opaque(33))
            .await
            .expect("response frame should be sent");
        peer.send_command(RemotingCommand::create_remoting_command(991).set_opaque(44))
            .await
            .expect("first request frame should be sent");
        peer.send_command(RemotingCommand::create_remoting_command(992).set_opaque(44))
            .await
            .expect("second request frame should be sent");

        let mut response_identity = None;
        let mut request_identities = Vec::new();
        for _ in 0..3 {
            let (command_type, identity) = tokio::time::timeout(Duration::from_secs(1), commands_rx.recv())
                .await
                .expect("command should be dispatched")
                .expect("identity observer should remain open");
            match command_type {
                RemotingCommandType::REQUEST => {
                    request_identities.push(identity.expect("request frame must carry identity"));
                }
                RemotingCommandType::RESPONSE => response_identity = Some(identity),
            }
        }
        request_identities.sort_unstable_by_key(|identity| identity.request_id().sequence());

        assert_eq!(response_identity, Some(None));
        assert_eq!(request_identities.len(), 2);
        assert!(request_identities
            .iter()
            .all(|identity| identity.request_id().owner_id() == owner_id));
        assert_eq!(request_identities[0].request_id().sequence(), 1);
        assert_eq!(request_identities[1].request_id().sequence(), 2);
        assert_eq!(request_identities[0].original_opaque(), 44);
        assert_eq!(request_identities[1].original_opaque(), 44);

        drop(peer);
        runner.await.expect("session runner should finish");
    }

    #[tokio::test]
    async fn sequence_exhaustion_drains_accepted_work_and_retires_without_dispatching_the_exhausted_request() {
        let runtime = RuntimeContext::from_current("transport-request-sequence-exhaustion-test");
        let service = runtime.service_context("transport-request-sequence-exhaustion");
        let (transport, peer_stream) = tokio::io::duplex(4096);
        let (entered_tx, entered_rx) = oneshot::channel();
        let (exhausted_tx, exhausted_rx) = oneshot::channel();
        let (disconnected_tx, disconnected_rx) = oneshot::channel();
        let release = Arc::new(Notify::new());
        let handler = Arc::new(SequenceExhaustionLifecycle {
            calls: AtomicUsize::new(0),
            entered: std::sync::Mutex::new(Some(entered_tx)),
            release: Arc::clone(&release),
            disconnected: std::sync::Mutex::new(Some(disconnected_tx)),
        });
        let session_id = crate::dispatch::reserve_session_owner().expect("test process owner should be available");
        let local_addr: SocketAddr = "127.0.0.1:19401".parse().expect("local address");
        let remote_addr: SocketAddr = "127.0.0.1:19402".parse().expect("remote address");
        let admission = Arc::new(AdmissionController::new(AdmissionLimits::default()));
        let security = Arc::new(TransportSecurity::development_insecure_loopback(None, None));
        let dispatch = Arc::new(crate::dispatch::AuthorizedDispatchBoundary::new(
            Arc::clone(&security),
            Arc::clone(&admission),
        ));
        let scope = crate::admission::AdmissionScope::new(remote_addr.ip()).with_session(session_id);
        let session_group = service
            .task_group()
            .try_child("transport-request-sequence-exhaustion-session")
            .expect("test session group should be created");
        let runner_handler = Arc::clone(&handler);
        let mut runner = tokio::spawn(super::run_framed_session_with_request_sequence(
            Connection::new_with_plaintext_stream(transport),
            local_addr,
            remote_addr,
            remote_addr,
            None,
            session_id,
            scope,
            session_group,
            dispatch,
            None,
            false,
            super::SessionIoPolicy {
                idle_timeout: Duration::from_secs(30),
                ..super::SessionIoPolicy::default()
            },
            AtomicU64::new(u64::MAX - 1),
            Some(exhausted_tx),
            runner_handler,
        ));
        let mut peer = Connection::new_with_plaintext_stream(peer_stream);
        peer.send_command(RemotingCommand::create_remoting_command(701).set_opaque(11))
            .await
            .expect("last allocatable request should be sent");
        let identity = tokio::time::timeout(Duration::from_secs(1), entered_rx)
            .await
            .expect("last allocatable request should enter the handler")
            .expect("handler should report the accepted identity");
        assert_eq!(identity.request_id().sequence(), u64::MAX - 1);

        peer.send_command(RemotingCommand::create_remoting_command(702).set_opaque(22))
            .await
            .expect("exhausted request frame should reach the session");
        tokio::time::timeout(Duration::from_secs(1), exhausted_rx)
            .await
            .expect("session should detect request sequence exhaustion")
            .expect("exhaustion signal should remain open");
        assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
        assert!(!runner.is_finished(), "accepted work must drain before retirement");

        release.notify_one();
        let response = tokio::time::timeout(Duration::from_secs(1), peer.receive_command())
            .await
            .expect("accepted request should complete while the session drains")
            .expect("accepted response read should succeed")
            .expect("accepted request should emit one response");
        assert_eq!(response.opaque(), 11);
        let disconnected_session = tokio::time::timeout(Duration::from_secs(1), disconnected_rx)
            .await
            .expect("sequence exhaustion should run disconnected")
            .expect("disconnected signal should remain open");
        tokio::time::timeout(Duration::from_secs(1), &mut runner)
            .await
            .expect("sequence-exhausted session should retire")
            .expect("session runner should not panic");
        assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            disconnected_session.connection().state(),
            crate::connection::ConnectionState::Closed
        );
        assert!(
            peer.receive_command().await.is_none(),
            "exhausted request must not receive a response"
        );

        let report = service.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn independently_connected_sessions_receive_distinct_process_owners() {
        let runtime = RuntimeContext::from_current("transport-distinct-session-owner-test");
        let service = runtime.service_context("transport-distinct-session-owner");
        let (first_transport, first_peer) = tokio::io::duplex(1024);
        let (second_transport, second_peer) = tokio::io::duplex(1024);
        let (first_tx, first_rx) = oneshot::channel();
        let (second_tx, second_rx) = oneshot::channel();
        let admission = Arc::new(AdmissionController::new(AdmissionLimits::default()));
        let security = Arc::new(TransportSecurity::development_insecure_loopback(None, None));
        let first_runner = tokio::spawn(super::run_connected_session(
            Connection::new_with_plaintext_stream(first_transport),
            "127.0.0.1:19201".parse().expect("first local address"),
            "127.0.0.1:19202".parse().expect("first remote address"),
            service.task_group().clone(),
            Arc::clone(&admission),
            Arc::clone(&security),
            None,
            Duration::from_secs(30),
            Arc::new(CaptureSession {
                sender: std::sync::Mutex::new(Some(first_tx)),
            }),
        ));
        let second_runner = tokio::spawn(super::run_connected_session(
            Connection::new_with_plaintext_stream(second_transport),
            "127.0.0.1:19301".parse().expect("second local address"),
            "127.0.0.1:19302".parse().expect("second remote address"),
            service.task_group().clone(),
            admission,
            security,
            None,
            Duration::from_secs(30),
            Arc::new(CaptureSession {
                sender: std::sync::Mutex::new(Some(second_tx)),
            }),
        ));
        let first_session = first_rx.await.expect("first session should connect");
        let second_session = second_rx.await.expect("second session should connect");

        assert_ne!(first_session.session_id(), second_session.session_id());
        assert!(!matches!(first_session.session_id(), 0 | u64::MAX));
        assert!(!matches!(second_session.session_id(), 0 | u64::MAX));

        drop(first_peer);
        drop(second_peer);
        first_runner.await.expect("first session runner should finish");
        second_runner.await.expect("second session runner should finish");
    }

    #[tokio::test]
    async fn canonical_network_session_view_keeps_addresses_and_shared_close_state() {
        let runtime = RuntimeContext::from_current("transport-canonical-session-view-test");
        let service = runtime.service_context("transport-canonical-session-view");
        let (transport, peer) = tokio::io::duplex(1024);
        let (connected_tx, connected_rx) = oneshot::channel();
        let local_addr: SocketAddr = "127.0.0.1:19501".parse().expect("local address");
        let remote_addr: SocketAddr = "127.0.0.1:19502".parse().expect("remote address");
        let runner = tokio::spawn(super::run_connected_session(
            Connection::new_with_plaintext_stream(transport),
            local_addr,
            remote_addr,
            service.task_group().clone(),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
            Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
            None,
            Duration::from_secs(30),
            Arc::new(CaptureSession {
                sender: std::sync::Mutex::new(Some(connected_tx)),
            }),
        ));
        let session = connected_rx.await.expect("network session should connect");
        let mut first = session.session_view();
        let mut second = first.clone();

        let SessionView::Network {
            id,
            local_addr: actual_local,
            remote_addr: actual_remote,
            transport_peer_addr,
            proxy,
            state,
        } = &first
        else {
            panic!("network session must retain a network view");
        };
        assert_eq!(*id, SessionId::from_session_owner(session.session_id()));
        assert_eq!(*actual_local, local_addr);
        assert_eq!(*actual_remote, remote_addr);
        assert_eq!(*transport_peer_addr, remote_addr);
        assert!(proxy.is_none());
        assert!(state.is_healthy());

        drop(peer);
        runner.await.expect("session runner should finish");

        let SessionView::Network { state, .. } = &mut first else {
            panic!("network session view must retain its variant");
        };
        state.closed().await;
        assert!(state.is_closed());
        let SessionView::Network { state, .. } = &mut second else {
            panic!("network session view must retain its variant");
        };
        state.closed().await;
        assert!(state.is_closed());
    }

    #[tokio::test]
    async fn listener_network_session_view_retains_trusted_proxy_and_transport_peer_addresses() {
        let runtime = RuntimeContext::from_current("transport-session-view-proxy-listener-test");
        let service = runtime.service_context("transport-session-view-proxy-listener");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let listener_addr = listener.local_addr().expect("listener address");
        let tls = TlsServerRuntime::initialize_with_service_context(TlsConfig::default(), &service)
            .await
            .expect("initialize TLS runtime");
        let (connected_tx, connected_rx) = oneshot::channel();
        let proxy_config = ProxyProtocolConfig {
            enabled: true,
            trusted_proxies: vec!["127.0.0.0/8".parse().expect("trusted loopback CIDR")],
            ..ProxyProtocolConfig::default()
        };
        let transport = TransportListener::new(
            listener,
            service.task_group().clone(),
            tls,
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
            Duration::from_secs(1),
        )
        .try_with_proxy_protocol(proxy_config)
        .expect("trusted proxy configuration should be valid");
        let server = tokio::spawn(transport.run(Arc::new(CaptureSession {
            sender: std::sync::Mutex::new(Some(connected_tx)),
        })));
        let mut client = TcpStream::connect(listener_addr).await.expect("connect proxy peer");
        let transport_peer = client.local_addr().expect("client local address");
        client
            .write_all(b"PROXY TCP4 198.51.100.44 192.0.2.10 43123 10911\r\n\0")
            .await
            .expect("write trusted PROXY header and plaintext discriminator");
        let session = tokio::time::timeout(Duration::from_secs(1), connected_rx)
            .await
            .expect("trusted proxy session should connect")
            .expect("connected session capture");
        let view = session.session_view();

        let SessionView::Network {
            local_addr,
            remote_addr,
            transport_peer_addr,
            proxy,
            ..
        } = view
        else {
            panic!("listener session must retain a network view");
        };
        let proxy = proxy.expect("trusted PROXY header must create a snapshot");
        let source: SocketAddr = "198.51.100.44:43123".parse().expect("PROXY source address");
        let destination: SocketAddr = "192.0.2.10:10911".parse().expect("PROXY destination address");

        assert_eq!(local_addr, destination);
        assert_eq!(remote_addr, source);
        assert_eq!(transport_peer_addr, transport_peer);
        assert_eq!(proxy.source(), source);
        assert_eq!(proxy.destination(), destination);
        assert_ne!(transport_peer_addr, source);
        assert_ne!(transport_peer_addr, destination);

        service.task_group().cancel();
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("listener should stop after owner cancellation")
            .expect("listener task")
            .expect("listener result");
        let report = service.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn canonical_session_view_closes_when_the_reader_stops_before_dispatch_drains() {
        let runtime = RuntimeContext::from_current("transport-session-view-reader-exit-test");
        let service = runtime.service_context("transport-session-view-reader-exit");
        let (transport, peer_stream) = tokio::io::duplex(1024);
        let (connected_tx, connected_rx) = oneshot::channel();
        let (entered_tx, entered_rx) = oneshot::channel();
        let release = Arc::new(Notify::new());
        let runner = tokio::spawn(super::run_connected_session(
            Connection::new_with_plaintext_stream(transport),
            "127.0.0.1:19601".parse().expect("local address"),
            "127.0.0.1:19602".parse().expect("remote address"),
            service.task_group().clone(),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
            Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
            None,
            Duration::from_secs(30),
            Arc::new(ReaderExitObserver {
                connected: std::sync::Mutex::new(Some(connected_tx)),
                entered: std::sync::Mutex::new(Some(entered_tx)),
                release: Arc::clone(&release),
            }),
        ));
        let session = connected_rx.await.expect("network session should connect");
        let view = session.session_view();
        let mut peer = Connection::new_with_plaintext_stream(peer_stream);
        peer.send_command(RemotingCommand::create_remoting_command(710).set_opaque(12))
            .await
            .expect("request should enter the session");
        entered_rx.await.expect("request handler should begin draining work");

        peer.shutdown().await.expect("client write half should close");
        tokio::time::timeout(Duration::from_secs(1), view.state().closed())
            .await
            .expect("reader exit must close the view before accepted work drains");
        assert!(view.state().is_closed());

        release.notify_one();
        let response = tokio::time::timeout(Duration::from_secs(1), peer.receive_command())
            .await
            .expect("accepted response should arrive after the reader exits")
            .expect("accepted response read should succeed")
            .expect("accepted response should be present");
        assert_eq!(response.opaque(), 12);
        runner.await.expect("session runner should finish");
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
    async fn aborting_retirement_while_an_active_send_is_draining_aborts_the_exact_session() {
        let runtime = RuntimeContext::from_current("transport-retirement-cancellation-test");
        let service = runtime.service_context("transport-retirement-cancellation");
        let (transport, _peer_stream) = tokio::io::duplex(4096);
        let (session_tx, session_rx) = oneshot::channel();
        let handler = Arc::new(CaptureSession {
            sender: std::sync::Mutex::new(Some(session_tx)),
        });
        let local_addr: SocketAddr = "127.0.0.1:19007".parse().unwrap();
        let remote_addr: SocketAddr = "127.0.0.1:19008".parse().unwrap();
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
                .send_command(RemotingCommand::create_remoting_command(5))
                .await
        });
        checked.notified().await;

        let (retirement_started_tx, retirement_started_rx) = oneshot::channel();
        let retiring_session = session.clone();
        let retirement = tokio::spawn(async move { retiring_session.retire_with_signal(retirement_started_tx).await });
        retirement_started_rx.await.expect("retirement started");
        std::future::poll_fn(|context| match session.send.lifecycle.begin_send() {
            None => Poll::Ready(()),
            Some(lease) => {
                drop(lease);
                context.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await;

        retirement.abort();
        assert!(retirement
            .await
            .expect_err("retirement caller must be aborted")
            .is_cancelled());
        assert_eq!(session.connection().state(), crate::connection::ConnectionState::Closed);
        assert!(session.send.reader_cancellation.is_cancelled());
        assert!(session.send.writer_operation.is_cancelled());

        resume_enqueue.notify_one();
        let send_error = checked_send
            .await
            .expect("checked send task")
            .expect_err("session abort must reject the draining send");
        assert!(matches!(
            send_error,
            RocketMQError::Network(NetworkError::ConnectionFailed { .. })
        ));
        runner.await.expect("session runner");
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
