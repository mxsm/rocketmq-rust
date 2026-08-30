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
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
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

use super::reserve_session_owner;
use super::LocalResponseReceiver;
use super::OriginalRequestIdentity;
use super::RequestContext;
use super::RequestTransport;
use super::ResponseSink;
use super::ResponseSinkError;
use crate::admission::AdmissionClass;
use crate::admission::AdmissionController;
use crate::admission::AdmissionError;
use crate::admission::AdmissionScope;
use crate::admission::AdmissionScopeHandle;
use crate::admission::FullPolicy;
use crate::admission::PartialFramePermit;
use crate::base::pending_request_table::materialize_and_estimate_remoting_command_retained_bytes;
use crate::base::pending_request_table::PendingRequestLimits;
use crate::base::pending_request_table::PendingRequestTable;
use crate::net::channel::Channel;
use crate::net::channel::ChannelInner;
use crate::remoting::inner::RemotingGeneralHandler;
use crate::request_ordering::RequestOrdering;
use crate::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
use crate::runtime::processor::RequestProcessor;
use crate::runtime::processor_v2::RequestProcessorV2;
use crate::runtime::RPCHook;
use crate::security::TransportSecurity;
use crate::session_executor::SessionDispatchError;
use crate::session_executor::SessionExecutor;
use crate::session_view::EmbeddedSessionRecord;
use crate::telemetry::TransportTelemetry;

mod embedded_v2;
mod v2;

pub(crate) use v2::AuthorizedDispatchV2Error;
pub(crate) use v2::AuthorizedDispatcherCore;

/// Result of submitting a command to the shared dispatch boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchOutcome {
    /// The lifecycle-owned processor task was accepted.
    Accepted(TaskId),
    /// Authorization, deadline, or reject-policy admission produced a response.
    Rejected,
}

/// Typed failure returned by the shared dispatch boundary.
#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    /// The request session could not be created under its lifecycle owner.
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    /// The request session stopped accepting work.
    #[error("authorized dispatch session is closing: {0}")]
    Closing(String),
    /// A non-reject admission policy could not admit the request.
    #[error("authorized dispatch admission failed: {0}")]
    Admission(#[source] AdmissionError),
    /// The selected response output failed.
    #[error(transparent)]
    Response(#[from] ResponseSinkError),
    /// A local-only API received a network request context.
    #[error("embedded dispatch requires an embedded request context")]
    InvalidEmbeddedContext,
}

/// Security and admission capabilities shared by all command entry adapters.
pub struct AuthorizedDispatchBoundary {
    security: Arc<TransportSecurity>,
    admission: Arc<AdmissionController>,
}

impl AuthorizedDispatchBoundary {
    /// Creates one authoritative security and admission boundary.
    #[must_use]
    pub fn new(security: Arc<TransportSecurity>, admission: Arc<AdmissionController>) -> Self {
        Self { security, admission }
    }

    /// Returns the shared admission controller used for connection, request,
    /// and processor capacity.
    #[must_use]
    pub fn admission_controller(&self) -> Arc<AdmissionController> {
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

/// Public network dispatcher for the V2 request-processor contract.
///
/// The facade owns one security/admission boundary and one statically
/// monomorphized V2 processor core. It does not construct a legacy channel;
/// response correlation remains private to each canonical V2 network session.
pub struct AuthorizedCommandDispatcherV2<P> {
    boundary: Arc<AuthorizedDispatchBoundary>,
    core: Arc<AuthorizedDispatcherCore<crate::dispatch::ExplicitV2Processor<P>>>,
    telemetry: TransportTelemetry,
}

impl<P> AuthorizedCommandDispatcherV2<P>
where
    P: RequestProcessorV2 + Clone + Sync + 'static,
{
    /// Creates a network-only V2 dispatcher.
    ///
    /// # Panics
    ///
    /// Panics when the compatibility pending-request budget cannot be
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

    /// Creates a V2 dispatcher with the composition-owned transport telemetry.
    ///
    /// # Panics
    ///
    /// Panics when the compatibility pending-request budget cannot be
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

    /// Creates a V2 dispatcher whose server-request correlations are charged
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
    pub fn boundary(&self) -> Arc<AuthorizedDispatchBoundary> {
        Arc::clone(&self.boundary)
    }

    /// Returns this V2 dispatcher's configured deferred-wait owner.
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
        network_session: crate::dispatch::V2NetworkSession,
        session: crate::server::SessionHandle,
        context: RequestContext,
        command: RemotingCommand,
        received_at: Instant,
        retained_bytes: usize,
        partial_frame_permit: Option<PartialFramePermit>,
        session_cleanup: crate::dispatch::DeferredSessionCleanupRegistration,
    ) -> Result<DispatchOutcome, AuthorizedDispatchV2Error> {
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
    }

    pub(crate) fn open_network_session(&self) -> crate::dispatch::V2NetworkSession {
        self.core.open_network_session()
    }

    pub(crate) fn complete_network_response(
        &self,
        session: &crate::dispatch::V2NetworkSession,
        response: RemotingCommand,
    ) {
        self.core.complete_network_response(session, response);
    }

    pub(crate) fn close_network_session(&self, session: &crate::dispatch::V2NetworkSession) {
        self.core.close_network_session(session);
    }
}

pub(crate) struct AuthorizedDispatchSession {
    boundary: Arc<AuthorizedDispatchBoundary>,
    #[allow(
        dead_code,
        reason = "DSP-03 validates the canonical session owner before later coexistence routing wires V2"
    )]
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

    pub(crate) async fn dispatch<F, Fut>(
        &self,
        context: RequestContext,
        original_request_identity: Option<OriginalRequestIdentity>,
        command: RemotingCommand,
        retained_bytes: usize,
        partial_frame_permit: Option<PartialFramePermit>,
        ordering: RequestOrdering,
        response: ResponseSink,
        execute: F,
    ) -> Result<DispatchOutcome, DispatchError>
    where
        F: FnOnce(OperationContext, RemotingCommand) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let opaque =
            original_request_identity.map_or_else(|| command.opaque(), OriginalRequestIdentity::original_opaque);
        let request_code =
            original_request_identity.map_or_else(|| command.code(), OriginalRequestIdentity::original_code);
        if context.deadline().is_some_and(|deadline| deadline.is_expired()) {
            response.send(deadline_response(opaque)).await?;
            return Ok(DispatchOutcome::Rejected);
        }
        if let Decision::Deny { reason } = self.boundary.security.authorize_for_dispatch(
            &command,
            context.peer(),
            context.principal(),
            Resource::new(ResourceKind::Other, request_code.to_string()),
            Action::Manage,
        ) {
            response
                .send(
                    RemotingCommand::create_response_command_with_code_remark(
                        ResponseCode::NoPermission,
                        reason.to_string(),
                    )
                    .set_opaque(opaque),
                )
                .await?;
            return Ok(DispatchOutcome::Rejected);
        }

        let deadline = context.deadline();
        let timeout_response = response.clone();
        let processor_rejection = response.clone();
        match self.executor.try_execute(
            retained_bytes,
            AdmissionClass::for_request_code(request_code),
            partial_frame_permit,
            ordering,
            move |operation| async move {
                let execution = execute(operation, command);
                if let Some(deadline) = deadline {
                    if deadline.timeout(execution).await.is_err() {
                        let _ = timeout_response.send(deadline_response(opaque)).await;
                    }
                } else {
                    execution.await;
                }
            },
            move |_operation, error| async move {
                let _ = processor_rejection.send(admission_response(opaque, &error)).await;
            },
        ) {
            Ok(task_id) => Ok(DispatchOutcome::Accepted(task_id)),
            Err(SessionDispatchError::Admission {
                error,
                retained_partial,
            }) if error.policy() == FullPolicy::Reject => {
                let _retained_partial = retained_partial;
                response.send(admission_response(opaque, &error)).await?;
                Ok(DispatchOutcome::Rejected)
            }
            Err(SessionDispatchError::Admission {
                error,
                retained_partial,
            }) => {
                drop(retained_partial);
                Err(DispatchError::Admission(error))
            }
            Err(SessionDispatchError::Closing(error)) => Err(DispatchError::Closing(error.to_string())),
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

/// Shared RPC-hook, typed processor, security, admission, and error-mapping
/// dispatcher used by network listeners and the embedded Proxy adapter.
pub struct AuthorizedCommandDispatcher<RP> {
    boundary: Arc<AuthorizedDispatchBoundary>,
    handler: Arc<RemotingGeneralHandler<RP>>,
    network: Arc<AuthorizedDispatcherCore<crate::dispatch::LegacyProcessorAdapter<RP>>>,
}

impl<RP> AuthorizedCommandDispatcher<RP>
where
    RP: RequestProcessor + Sync + Clone + 'static,
{
    /// Creates a dispatcher over an explicitly injected process budget.
    ///
    /// # Errors
    ///
    /// Returns an error when the pending-request budget cannot be derived.
    pub fn try_new(
        request_processor: RP,
        rpc_hooks: Vec<Arc<dyn RPCHook>>,
        process_budget: &ResourceBudget,
        telemetry: TransportTelemetry,
        security: Arc<TransportSecurity>,
        admission: Arc<AdmissionController>,
    ) -> rocketmq_error::RocketMQResult<Self> {
        let response_table = PendingRequestTable::try_with_limits_and_budget(
            PendingRequestLimits {
                max_count: 512,
                ..Default::default()
            },
            process_budget,
        )
        .map_err(|error| {
            rocketmq_error::RocketMQError::response_process_failed(
                "authorized_dispatcher.pending_requests",
                error.to_string(),
            )
        })?;
        let boundary = Arc::new(AuthorizedDispatchBoundary::new(security, admission));
        let network_processor = request_processor.clone();
        let handler = Arc::new(RemotingGeneralHandler::new_with_telemetry(
            request_processor,
            rpc_hooks.clone(),
            response_table.clone(),
            telemetry.clone(),
        ));
        let adapter = crate::dispatch::LegacyProcessorAdapter::new(
            network_processor,
            std::any::type_name::<RP>(),
            telemetry,
            response_table,
        );
        Ok(Self {
            boundary,
            handler,
            network: Arc::new(AuthorizedDispatcherCore::new_legacy(adapter, rpc_hooks)),
        })
    }

    /// Returns the exact security and admission boundary used by this handler.
    #[must_use]
    pub fn boundary(&self) -> Arc<AuthorizedDispatchBoundary> {
        Arc::clone(&self.boundary)
    }

    pub(crate) fn request_ordering(&self, command: &RemotingCommand) -> RequestOrdering {
        self.handler.request_processor.request_ordering(command)
    }

    pub(crate) fn open_network_session(&self) -> crate::dispatch::LegacyNetworkSession {
        self.network.open_network_session()
    }

    pub(crate) fn complete_network_response(
        &self,
        session: &crate::dispatch::LegacyNetworkSession,
        response: RemotingCommand,
    ) {
        self.network.complete_network_response(session, response);
    }

    pub(crate) fn close_network_session(&self, session: &crate::dispatch::LegacyNetworkSession) {
        self.network.close_network_session(session);
    }

    pub(crate) async fn dispatch_network(
        &self,
        authorized_session: &AuthorizedDispatchSession,
        network_session: crate::dispatch::LegacyNetworkSession,
        session: crate::server::SessionHandle,
        context: RequestContext,
        command: RemotingCommand,
        received_at: Instant,
        retained_bytes: usize,
        partial_frame_permit: Option<PartialFramePermit>,
        session_cleanup: crate::dispatch::DeferredSessionCleanupRegistration,
    ) -> Result<DispatchOutcome, AuthorizedDispatchV2Error> {
        self.network
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
    }

    /// Dispatches one embedded command without creating a listener, socket
    /// pair, transport stream, writer task, or detached task.
    ///
    /// # Errors
    ///
    /// Returns a typed context, runtime, admission, response, cancellation, or
    /// deadline error.
    pub async fn dispatch_embedded(
        self: &Arc<Self>,
        task_group: &TaskGroup,
        context: RequestContext,
        mut command: RemotingCommand,
    ) -> Result<RemotingCommand, DispatchError> {
        if context.transport() != RequestTransport::EmbeddedProxy {
            return Err(DispatchError::InvalidEmbeddedContext);
        }
        let (session_id, original_request_identity) = capture_embedded_request_identity(&command)?;
        let _session_record = EmbeddedSessionRecord::new(session_id);
        let retained_bytes = materialize_and_estimate_remoting_command_retained_bytes(&mut command);
        let scope = AdmissionScope::new(IpAddr::V4(Ipv4Addr::LOCALHOST)).with_session(session_id);
        let scope = self
            .boundary
            .admission
            .prepare_scope(scope)
            .map_err(DispatchError::Admission)?;
        let session = self.boundary.session(task_group, scope)?;
        let (response, receiver): (ResponseSink, LocalResponseReceiver) = ResponseSink::local();
        let local_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let remote_address = local_address;
        let mut channel = Channel::new(
            Arc::new(ChannelInner::new_local(response.clone(), task_group.clone())),
            local_address,
            remote_address,
        );
        channel.set_channel_id(format!("embedded-proxy-{session_id}"));
        let handler_context = Arc::new(ConnectionHandlerContextWrapper::new(channel));
        let ordering = self.request_ordering(&command);
        let handler = Arc::clone(&self.handler);
        let deadline = context.deadline();
        let cancellation = task_group.cancellation_token();

        session
            .dispatch(
                context,
                Some(original_request_identity),
                command,
                retained_bytes,
                None,
                ordering,
                response,
                move |_operation, command| async move {
                    handler
                        .process_message_received(&handler_context, Some(original_request_identity), command)
                        .await;
                },
            )
            .await?;
        let result = receiver.receive(&cancellation, deadline).await;
        let drain_budget = deadline.map_or(Duration::from_secs(3), |deadline| deadline.remaining());
        session
            .drain_until(ShutdownDeadline::after(drain_budget))
            .await
            .log_if_unhealthy();
        result.map_err(DispatchError::Response)
    }
}

fn capture_embedded_request_identity(
    command: &RemotingCommand,
) -> Result<(u64, OriginalRequestIdentity), DispatchError> {
    capture_embedded_request_identity_with_owner(command, reserve_session_owner())
}

fn capture_embedded_request_identity_with_owner(
    command: &RemotingCommand,
    session_id: Option<u64>,
) -> Result<(u64, OriginalRequestIdentity), DispatchError> {
    let session_id = session_id.ok_or_else(|| {
        DispatchError::Runtime(RuntimeError::LifecycleOperation {
            operation: "authorized_dispatcher.reserve_embedded_session_owner",
            message: "process-local session owner namespace exhausted".to_owned(),
        })
    })?;
    let request_sequence = AtomicU64::new(1);
    let identity = OriginalRequestIdentity::capture(session_id, &request_sequence, command).ok_or_else(|| {
        DispatchError::Runtime(RuntimeError::LifecycleOperation {
            operation: "authorized_dispatcher.capture_embedded_request_identity",
            message: "embedded request identity namespace exhausted".to_owned(),
        })
    })?;
    Ok((session_id, identity))
}

pub(super) fn admission_response(opaque: i32, error: &AdmissionError) -> RemotingCommand {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_sessions_use_distinct_process_owners_even_with_the_same_opaque() {
        let network_owner = reserve_session_owner().expect("test process owner should be available");
        let command = RemotingCommand::create_remoting_command(-91_762).set_opaque(77);

        let (_, first) = capture_embedded_request_identity(&command).expect("first embedded identity");
        let (_, second) = capture_embedded_request_identity(&command).expect("second embedded identity");

        assert_ne!(first.request_id().owner_id(), network_owner);
        assert_ne!(second.request_id().owner_id(), network_owner);
        assert_ne!(first.request_id().owner_id(), second.request_id().owner_id());
        assert_eq!(first.request_id().sequence(), 1);
        assert_eq!(second.request_id().sequence(), 1);
        assert_eq!(first.original_opaque(), second.original_opaque());
        assert_eq!(first.original_code(), -91_762);
        assert_eq!(second.original_code(), -91_762);
    }

    #[test]
    fn embedded_owner_exhaustion_is_a_runtime_lifecycle_failure() {
        let command = RemotingCommand::create_remoting_command(10).set_opaque(77);

        let error = capture_embedded_request_identity_with_owner(&command, None)
            .expect_err("an exhausted process owner namespace must fail closed");

        match error {
            DispatchError::Runtime(RuntimeError::LifecycleOperation { operation, message }) => {
                assert_eq!(operation, "authorized_dispatcher.reserve_embedded_session_owner");
                assert_eq!(message, "process-local session owner namespace exhausted");
            }
            other => panic!("unexpected embedded exhaustion error: {other:?}"),
        }
    }
}
