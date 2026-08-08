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
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

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

use super::LocalResponseReceiver;
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
use crate::runtime::RPCHook;
use crate::security::TransportSecurity;
use crate::session_executor::SessionDispatchError;
use crate::session_executor::SessionExecutor;
use crate::telemetry::TransportTelemetry;

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

    pub(crate) fn session(
        self: &Arc<Self>,
        task_group: &TaskGroup,
        scope: AdmissionScopeHandle,
    ) -> Result<AuthorizedDispatchSession, RuntimeError> {
        Ok(AuthorizedDispatchSession {
            boundary: Arc::clone(self),
            executor: SessionExecutor::try_new(task_group, scope)?,
        })
    }
}

pub(crate) struct AuthorizedDispatchSession {
    boundary: Arc<AuthorizedDispatchBoundary>,
    executor: SessionExecutor,
}

impl AuthorizedDispatchSession {
    pub(crate) fn operation_context(&self) -> &OperationContext {
        self.executor.operation_context()
    }

    pub(crate) async fn dispatch<F, Fut>(
        &self,
        context: RequestContext,
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
        let opaque = command.opaque();
        if context.deadline().is_some_and(|deadline| deadline.is_expired()) {
            response.send(deadline_response(opaque)).await?;
            return Ok(DispatchOutcome::Rejected);
        }
        if let Decision::Deny { reason } = self.boundary.security.authorize(
            &command,
            context.peer(),
            context.principal(),
            Resource::new(ResourceKind::Other, command.code().to_string()),
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
            AdmissionClass::for_request_code(command.code()),
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
}

/// Shared RPC-hook, typed processor, security, admission, and error-mapping
/// dispatcher used by network listeners and the embedded Proxy adapter.
pub struct AuthorizedCommandDispatcher<RP> {
    boundary: Arc<AuthorizedDispatchBoundary>,
    handler: Arc<RemotingGeneralHandler<RP>>,
    next_embedded_session: AtomicU64,
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
        Ok(Self {
            boundary: Arc::new(AuthorizedDispatchBoundary::new(security, admission)),
            handler: Arc::new(RemotingGeneralHandler::new_with_telemetry(
                request_processor,
                rpc_hooks,
                response_table,
                telemetry,
            )),
            next_embedded_session: AtomicU64::new(1),
        })
    }

    /// Returns the exact security and admission boundary used by this handler.
    #[must_use]
    pub fn boundary(&self) -> Arc<AuthorizedDispatchBoundary> {
        Arc::clone(&self.boundary)
    }

    pub(crate) fn response_table(&self) -> PendingRequestTable {
        self.handler.response_table.clone()
    }

    pub(crate) fn request_ordering(&self, command: &RemotingCommand) -> RequestOrdering {
        self.handler.request_processor.request_ordering(command)
    }

    pub(crate) async fn process_network(
        &self,
        context: &crate::runtime::connection_handler_context::ConnectionHandlerContext,
        command: RemotingCommand,
    ) {
        self.handler.process_message_received(context, command).await;
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
        let retained_bytes = materialize_and_estimate_remoting_command_retained_bytes(&mut command);
        let session_id = self.next_embedded_session.fetch_add(1, Ordering::Relaxed).max(1);
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
                command,
                retained_bytes,
                None,
                ordering,
                response,
                move |_operation, command| async move {
                    handler.process_message_received(&handler_context, command).await;
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

fn admission_response(opaque: i32, error: &AdmissionError) -> RemotingCommand {
    RemotingCommand::create_response_command_with_code_remark(ResponseCode::SystemBusy, error.to_string())
        .set_opaque(opaque)
}

fn deadline_response(opaque: i32) -> RemotingCommand {
    RemotingCommand::create_response_command_with_code_remark(
        ResponseCode::SystemError,
        "request deadline exceeded".to_owned(),
    )
    .set_opaque(opaque)
}
