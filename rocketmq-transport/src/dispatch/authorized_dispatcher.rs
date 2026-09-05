// Copyright 2026 The RocketMQ Rust Authors
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
use std::sync::Arc;
use std::time::Instant;

use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::OperationContext;
use rocketmq_runtime::ResourceBudget;
use rocketmq_runtime::RuntimeError;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskId;
use rocketmq_security_api::Action;
use rocketmq_security_api::Decision;
use rocketmq_security_api::Resource;
use rocketmq_security_api::ResourceKind;
use rocketmq_security_api::SecurityBootstrapProfile;

use super::OriginalRequestIdentity;
use super::RequestContext;
use crate::admission::AdmissionClass;
use crate::admission::AdmissionController;
use crate::admission::AdmissionRejection;
use crate::admission::AdmissionScopeHandle;
use crate::admission::FullPolicy;
use crate::admission::PartialFramePermit;
use crate::base::pending_request_table::PendingRequestLimits;
use crate::base::pending_request_table::PendingRequestTable;
use crate::error::TransportError;
use crate::request_ordering::RequestOrdering;
use crate::runtime::processor::RequestProcessor;
use crate::runtime::RPCHook;
use crate::security::TransportSecurity;
use crate::session_executor::SessionDispatchAttempt;
use crate::session_executor::SessionExecutor;
use crate::telemetry::TransportTelemetry;

#[path = "authorized_dispatcher/core.rs"]
mod core;
#[path = "authorized_dispatcher/embedded.rs"]
mod embedded;

pub(crate) use core::AuthorizedDispatchError;
pub(crate) use core::AuthorizedDispatcherCore;

/// Result of submitting a command to the shared dispatch boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DispatchOutcome {
    /// The lifecycle-owned processor task was accepted.
    Accepted(TaskId),
    /// Authorization, deadline, or reject-policy admission produced a response.
    Rejected,
    /// Admission policy requires the owning connection to close the session.
    CloseSession,
    /// The session lifecycle closed before the request could be accepted.
    SessionClosed,
}

impl DispatchOutcome {
    pub(crate) const fn keeps_session_open(self) -> bool {
        matches!(self, Self::Accepted(_) | Self::Rejected)
    }
}

/// Security and admission capabilities shared by all command entry adapters.
pub(crate) struct AuthorizedDispatchBoundary {
    security: Arc<TransportSecurity>,
    admission: Arc<AdmissionController>,
}

impl AuthorizedDispatchBoundary {
    /// Creates one authoritative security and admission boundary.
    #[must_use]
    pub(crate) fn new(security: Arc<TransportSecurity>, admission: Arc<AdmissionController>) -> Self {
        Self { security, admission }
    }

    /// Returns the shared admission controller used for connection, request,
    /// and processor capacity.
    #[must_use]
    pub(crate) fn admission_controller(&self) -> Arc<AdmissionController> {
        Arc::clone(&self.admission)
    }

    pub(crate) fn deferred_admission(&self) -> Option<crate::dispatch::DeferredAdmission> {
        self.admission.deferred_admission()
    }

    /// Returns the security profile enforced by this shared boundary.
    #[must_use]
    pub(crate) fn security_profile(&self) -> SecurityBootstrapProfile {
        self.security.profile()
    }

    pub(crate) fn has_security_owner(&self, security: &Arc<TransportSecurity>) -> bool {
        Arc::ptr_eq(&self.security, security)
    }

    pub(crate) fn security_owner(&self) -> Arc<TransportSecurity> {
        Arc::clone(&self.security)
    }

    pub(crate) fn has_admission_owner(&self, admission: &Arc<AdmissionController>) -> bool {
        Arc::ptr_eq(&self.admission, admission)
    }

    pub(crate) fn session(
        self: &Arc<Self>,
        task_group: &TaskGroup,
        scope: AdmissionScopeHandle,
    ) -> Result<AuthorizedDispatchSession, RuntimeError> {
        Ok(AuthorizedDispatchSession {
            boundary: Arc::clone(self),
            session_id: scope.session_id(),
            executor: SessionExecutor::try_new(task_group, scope)?,
        })
    }
}

/// Public network dispatcher for the request-processor contract.
///
/// The facade owns one security/admission boundary and one statically
/// monomorphized  processor core. It does not construct a processor channel;
/// response correlation remains private to each canonical network session.
pub struct AuthorizedCommandDispatcher<P> {
    boundary: Arc<AuthorizedDispatchBoundary>,
    core: Arc<AuthorizedDispatcherCore<crate::dispatch::ExplicitProcessor<P>>>,
    telemetry: TransportTelemetry,
}

impl<P> AuthorizedCommandDispatcher<P>
where
    P: RequestProcessor + Clone + Sync + 'static,
{
    /// Creates a network-only  dispatcher.
    ///
    /// # Panics
    ///
    /// Panics when the pending-request budget cannot be
    /// initialized. Production composition should use
    /// [`Self::try_new_with_telemetry_and_budget`].
    #[must_use]
    pub fn new(
        request_processor: P,
        rpc_hooks: Vec<Arc<dyn RPCHook>>,
        security: Arc<TransportSecurity>,
        admission: Arc<AdmissionController>,
    ) -> Self {
        Self::new_with_telemetry(
            request_processor,
            rpc_hooks,
            security,
            admission,
            TransportTelemetry::noop(),
        )
    }

    /// Creates a dispatcher with the composition-owned transport telemetry.
    ///
    /// # Panics
    ///
    /// Panics when the pending-request budget cannot be
    /// initialized. Production composition should use
    /// [`Self::try_new_with_telemetry_and_budget`].
    #[must_use]
    pub fn new_with_telemetry(
        request_processor: P,
        rpc_hooks: Vec<Arc<dyn RPCHook>>,
        security: Arc<TransportSecurity>,
        admission: Arc<AdmissionController>,
        telemetry: TransportTelemetry,
    ) -> Self {
        Self {
            boundary: Arc::new(AuthorizedDispatchBoundary::new(security, admission)),
            core: Arc::new(AuthorizedDispatcherCore::new_with_telemetry(
                request_processor,
                rpc_hooks,
                telemetry.clone(),
            )),
            telemetry,
        }
    }

    /// Creates a dispatcher whose server-request correlations are charged
    /// to the composition-owned process budget.
    ///
    /// # Errors
    ///
    /// Returns a typed configuration error when the bounded pending-request
    /// table cannot be derived from `process_budget`.
    pub fn try_new_with_telemetry_and_budget(
        request_processor: P,
        rpc_hooks: Vec<Arc<dyn RPCHook>>,
        security: Arc<TransportSecurity>,
        admission: Arc<AdmissionController>,
        telemetry: TransportTelemetry,
        process_budget: &ResourceBudget,
    ) -> rocketmq_error::RocketMQResult<Self> {
        let response_table = PendingRequestTable::try_with_limits_and_budget(
            PendingRequestLimits {
                max_count: 512,
                ..PendingRequestLimits::default()
            },
            process_budget,
        )?;
        Ok(Self {
            boundary: Arc::new(AuthorizedDispatchBoundary::new(security, admission)),
            core: Arc::new(AuthorizedDispatcherCore::new_with_pending_requests_and_telemetry(
                request_processor,
                rpc_hooks,
                response_table,
                telemetry.clone(),
            )),
            telemetry,
        })
    }

    /// Returns the exact security and admission boundary used by this dispatcher.
    #[must_use]
    pub(crate) fn boundary(&self) -> Arc<AuthorizedDispatchBoundary> {
        Arc::clone(&self.boundary)
    }

    /// Returns this  dispatcher's configured deferred-wait owner.
    ///
    /// `None` is a fail-closed signal: deferred registration must not proceed
    /// until the admission controller was explicitly configured.
    #[must_use]
    pub fn deferred_admission(&self) -> Option<crate::dispatch::DeferredAdmission> {
        self.boundary.deferred_admission()
    }

    pub(crate) fn register_rpc_hook(&self, hook: Arc<dyn RPCHook>) {
        self.core.register_rpc_hook(hook);
    }

    pub(crate) fn clear_rpc_hook(&self) {
        self.core.clear_rpc_hook();
    }

    pub(crate) fn hook_snapshot(&self) -> Option<Arc<crate::hook_registry::HookSnapshot>> {
        self.core.hook_snapshot()
    }

    pub(crate) async fn dispatch_network(
        &self,
        authorized_session: &AuthorizedDispatchSession,
        network_session: crate::dispatch::NetworkSession,
        session: crate::server::SessionHandle,
        context: RequestContext,
        command: RemotingCommand,
        received_at: Instant,
        retained_bytes: usize,
        partial_frame_permit: Option<PartialFramePermit>,
        session_cleanup: crate::dispatch::DeferredSessionCleanupRegistration,
    ) -> Result<DispatchOutcome, TransportError> {
        self.core
            .dispatch_network(
                authorized_session,
                network_session,
                session,
                context,
                command,
                received_at,
                retained_bytes,
                partial_frame_permit,
                Some(session_cleanup),
            )
            .await
            .map_err(|source| TransportError::dispatch(source))
    }

    pub(crate) fn open_network_session(&self) -> crate::dispatch::NetworkSession {
        self.core.open_network_session()
    }

    pub(crate) fn complete_network_response(
        &self,
        session: &crate::dispatch::NetworkSession,
        response: RemotingCommand,
    ) {
        self.core.complete_network_response(session, response);
    }

    pub(crate) fn close_network_session(&self, session: &crate::dispatch::NetworkSession) {
        self.core.close_network_session(session);
    }
}

pub(crate) struct AuthorizedDispatchSession {
    boundary: Arc<AuthorizedDispatchBoundary>,
    session_id: Option<u64>,
    executor: SessionExecutor,
}

impl AuthorizedDispatchSession {
    pub(crate) fn begin_close(&self) {
        self.executor.begin_close();
    }

    pub(crate) fn operation_context(&self) -> &OperationContext {
        self.executor.operation_context()
    }

    /// Dispatches a request handled by the low-level connection callback.
    ///
    /// This adapter shares the same authorization, admission, deadline, and
    /// lifecycle owner as processor-based routes, while leaving the callback
    /// responsible for writing its application response.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn dispatch_handler<F, Fut>(
        &self,
        context: RequestContext,
        original_request_identity: OriginalRequestIdentity,
        command: RemotingCommand,
        retained_bytes: usize,
        partial_frame_permit: Option<PartialFramePermit>,
        ordering: RequestOrdering,
        response_session: crate::server::SessionHandle,
        execute: F,
    ) -> Result<DispatchOutcome, TransportError>
    where
        F: FnOnce(OperationContext, RemotingCommand) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        if context.transport() != super::RequestTransport::Network
            || self.session_id != Some(response_session.session_id())
            || original_request_identity.request_id().owner_id() != response_session.session_id()
            || !original_request_identity.matches_command(&command)
        {
            return Err(TransportError::dispatch(AuthorizedDispatchError::SessionMismatch));
        }
        let opaque = original_request_identity.original_opaque();
        let request_code = original_request_identity.original_code();
        let is_one_way = original_request_identity.is_one_way();
        if context.deadline().is_some_and(|deadline| deadline.is_expired()) {
            send_handler_boundary_response(&response_session, is_one_way, deadline_response(opaque))
                .await
                .map_err(|source| TransportError::dispatch(source))?;
            return Ok(DispatchOutcome::Rejected);
        }
        if let Decision::Deny { reason } = self.boundary.security.authorize_for_dispatch(
            &command,
            context.peer(),
            context.principal(),
            Resource::new(ResourceKind::Other, request_code.to_string()),
            Action::Manage,
        ) {
            send_handler_boundary_response(
                &response_session,
                is_one_way,
                RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::NoPermission,
                    reason.to_string(),
                )
                .set_opaque(opaque),
            )
            .await
            .map_err(|source| TransportError::dispatch(source))?;
            return Ok(DispatchOutcome::Rejected);
        }

        let deadline = context.deadline();
        let timeout_session = response_session.clone();
        let rejection_session = response_session.clone();
        match self.executor.try_execute(
            retained_bytes,
            AdmissionClass::for_request_code(request_code),
            partial_frame_permit,
            ordering,
            move |operation| async move {
                let execution = execute(operation, command);
                if let Some(deadline) = deadline {
                    if deadline.timeout(execution).await.is_err() {
                        let _ = send_handler_boundary_response(&timeout_session, is_one_way, deadline_response(opaque))
                            .await;
                    }
                } else {
                    execution.await;
                }
            },
            move |_operation, error| async move {
                let _ =
                    send_handler_boundary_response(&rejection_session, is_one_way, admission_response(opaque, &error))
                        .await;
            },
        ) {
            Ok(SessionDispatchAttempt::Accepted(task_id)) => Ok(DispatchOutcome::Accepted(task_id)),
            Ok(SessionDispatchAttempt::AdmissionRejected {
                rejection,
                retained_partial,
            }) if rejection.policy() == FullPolicy::Reject => {
                drop(retained_partial);
                send_handler_boundary_response(&response_session, is_one_way, admission_response(opaque, &rejection))
                    .await
                    .map_err(|source| TransportError::dispatch(source))?;
                Ok(DispatchOutcome::Rejected)
            }
            Ok(SessionDispatchAttempt::AdmissionRejected {
                rejection: _,
                retained_partial,
            }) => {
                drop(retained_partial);
                Ok(DispatchOutcome::CloseSession)
            }
            Ok(SessionDispatchAttempt::SessionClosed { retained_partial }) => {
                drop(retained_partial);
                Ok(DispatchOutcome::SessionClosed)
            }
            Err(error) => Err(TransportError::dispatch(AuthorizedDispatchError::Closing(error))),
        }
    }

    pub(crate) async fn drain_until(&self, deadline: ShutdownDeadline) -> ShutdownReport {
        self.executor.drain_until(deadline).await
    }

    pub(crate) async fn drain_report_until(
        &self,
        deadline: ShutdownDeadline,
    ) -> crate::session_executor::SessionExecutorDrainReport {
        self.executor.drain_report_until(deadline).await
    }
}

async fn send_handler_response(
    session: &crate::server::SessionHandle,
    command: RemotingCommand,
) -> Result<(), AuthorizedDispatchError> {
    session
        .connection()
        .send_command(command)
        .await
        .map_err(AuthorizedDispatchError::BoundaryResponse)
}

async fn send_handler_boundary_response(
    session: &crate::server::SessionHandle,
    is_one_way: bool,
    command: RemotingCommand,
) -> Result<(), AuthorizedDispatchError> {
    if is_one_way {
        return Ok(());
    }
    send_handler_response(session, command).await
}

pub(super) fn admission_response(opaque: i32, error: &AdmissionRejection) -> RemotingCommand {
    RemotingCommand::create_response_command_with_code_remark(ResponseCode::SystemBusy, error.to_string())
        .set_opaque(opaque)
}

pub(super) fn deadline_response(opaque: i32) -> RemotingCommand {
    RemotingCommand::create_response_command_with_code_remark(
        ResponseCode::SystemError,
        "request deadline exceeded".to_owned(),
    )
    .set_opaque(opaque)
}
