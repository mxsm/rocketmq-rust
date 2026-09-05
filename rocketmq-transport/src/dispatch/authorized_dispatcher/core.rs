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

//! Private  dispatch from trusted network ingress to terminal response delivery.

use std::sync::Arc;
use std::time::Instant;

use rocketmq_error::RocketMQError;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeError;
use rocketmq_security_api::Action;
use rocketmq_security_api::Decision;
use rocketmq_security_api::Resource;
use rocketmq_security_api::ResourceKind;
use tracing::Instrument;

use super::admission_response;
use super::deadline_response;
use super::AuthorizedDispatchSession;
use super::DispatchOutcome;
use crate::admission::AdmissionClass;
use crate::admission::FullPolicy;
use crate::admission::PartialFramePermit;
use crate::base::pending_request_table::PendingRequestTable;
use crate::contract::TransportContractViolation;
use crate::dispatch::processor_adapter::AdmittedProcessorObserver;
use crate::dispatch::remoting_request::RemotingRequestBuilder;
use crate::dispatch::remoting_request::RequestLifecycleProvenance;
use crate::dispatch::DeferredCommitError;
use crate::dispatch::DeferredCommitErrorKind;
use crate::dispatch::DispatchProcessor;
use crate::dispatch::DispatchProcessorError;
use crate::dispatch::ExplicitProcessor;
use crate::dispatch::InternalProcessorCandidate;
use crate::dispatch::InternalProcessorOutcome;
use crate::dispatch::OriginalRequestIdentity;
use crate::dispatch::RemotingResponse;
use crate::dispatch::RequestContext;
use crate::dispatch::RequestTransport;
use crate::dispatch::ResponseCompletionOutcome;
use crate::dispatch::ResponseOperationalFailure;
use crate::dispatch::ResponseSink;
use crate::dispatch::WriteProgress;
use crate::hook_registry::HookRegistry;
use crate::runtime::processor::RequestProcessor;
use crate::runtime::RPCHook;
use crate::server::SessionHandle;
use crate::session_executor::SessionDispatchAttempt;
use crate::telemetry::BoundaryRejectionReason;

/// Stable private failure from scheduling or executing one dispatch.
#[derive(Debug, thiserror::Error)]
pub(crate) enum AuthorizedDispatchError {
    #[error(" dispatch requires a network request context")]
    InvalidNetworkContext,
    #[error(" dispatch requires the canonical ingress request identity")]
    MissingOriginalIdentity,
    #[error("the canonical ingress identity no longer matches the request command")]
    OriginalIdentityMismatch,
    #[error("the authorized dispatch session does not own the canonical network session")]
    SessionMismatch,
    #[error("authorized dispatch session is closing")]
    Closing(#[source] RuntimeError),
    #[error(" boundary response failed")]
    BoundaryResponse(#[source] RocketMQError),
    #[error(transparent)]
    Contract(#[from] TransportContractViolation),
    #[error(transparent)]
    DeferredCommit(#[from] DeferredCommitError),
    #[error("one-way requests cannot complete with {outcome}")]
    OneWayOutcome { outcome: &'static str },
    #[error(transparent)]
    Response(#[from] ResponseOperationalFailure),
}

impl AuthorizedDispatchError {
    const fn category(&self) -> &'static str {
        match self {
            Self::InvalidNetworkContext => "invalid_network_context",
            Self::MissingOriginalIdentity => "missing_original_identity",
            Self::OriginalIdentityMismatch => "original_identity_mismatch",
            Self::SessionMismatch => "session_mismatch",
            Self::Closing(_) => "closing",
            Self::BoundaryResponse(_) => "boundary_response",
            Self::Contract(_) => "contract",
            Self::DeferredCommit(error) => error.category(),
            Self::OneWayOutcome { .. } => "one_way_outcome",
            Self::Response(_) => "response",
        }
    }
}

/// Network-only processor boundary.
pub(crate) struct AuthorizedDispatcherCore<D> {
    processor: D,
    rpc_hooks: HookRegistry,
    #[cfg(test)]
    reported_failures: std::sync::Mutex<Vec<&'static str>>,
    #[cfg(test)]
    reported_failure_notify: tokio::sync::Notify,
}

#[cfg(test)]
type TestAuthorizedDispatcherCore<D> = AuthorizedDispatcherCore<D>;

impl<P> AuthorizedDispatcherCore<ExplicitProcessor<P>>
where
    P: RequestProcessor + Clone + Sync + 'static,
{
    #[cfg(test)]
    pub(crate) fn new(processor: P, rpc_hooks: Vec<Arc<dyn RPCHook>>) -> Self {
        Self::from_dispatch_processor(ExplicitProcessor::new(processor), rpc_hooks)
    }

    pub(crate) fn new_with_telemetry(
        processor: P,
        rpc_hooks: Vec<Arc<dyn RPCHook>>,
        telemetry: crate::telemetry::TransportTelemetry,
    ) -> Self {
        Self::from_dispatch_processor(
            ExplicitProcessor::with_response_table_and_telemetry(processor, PendingRequestTable::new(), telemetry),
            rpc_hooks,
        )
    }

    pub(crate) fn new_with_pending_requests_and_telemetry(
        processor: P,
        rpc_hooks: Vec<Arc<dyn RPCHook>>,
        response_table: PendingRequestTable,
        telemetry: crate::telemetry::TransportTelemetry,
    ) -> Self {
        Self::from_dispatch_processor(
            ExplicitProcessor::with_response_table_and_telemetry(processor, response_table, telemetry),
            rpc_hooks,
        )
    }

    pub(super) fn clone_explicit_processor(&self) -> ExplicitProcessor<P> {
        self.processor.clone()
    }

    pub(super) const fn explicit_processor(&self) -> &ExplicitProcessor<P> {
        &self.processor
    }
}

impl From<DispatchProcessorError> for AuthorizedDispatchError {
    fn from(error: DispatchProcessorError) -> Self {
        match error {
            DispatchProcessorError::Contract(error) => Self::Contract(error),
        }
    }
}

impl<D> AuthorizedDispatcherCore<D>
where
    D: DispatchProcessor,
{
    fn from_dispatch_processor(processor: D, rpc_hooks: Vec<Arc<dyn RPCHook>>) -> Self {
        Self {
            processor,
            rpc_hooks: HookRegistry::new(rpc_hooks),
            #[cfg(test)]
            reported_failures: std::sync::Mutex::new(Vec::new()),
            #[cfg(test)]
            reported_failure_notify: tokio::sync::Notify::new(),
        }
    }

    pub(crate) fn open_network_session(&self) -> D::NetworkSession {
        self.processor.open_network_session()
    }

    pub(crate) fn complete_network_response(&self, session: &D::NetworkSession, response: RemotingCommand) {
        self.processor.complete_network_response(session, response);
    }

    pub(crate) fn close_network_session(&self, session: &D::NetworkSession) {
        self.processor.close_network_session(session);
    }

    /// Admits one canonical network request into its existing session executor.
    pub(crate) async fn dispatch_network(
        self: &Arc<Self>,
        authorized_session: &AuthorizedDispatchSession,
        network_session: D::NetworkSession,
        session: SessionHandle,
        context: RequestContext,
        command: RemotingCommand,
        received_at: Instant,
        retained_bytes: usize,
        partial_frame_permit: Option<PartialFramePermit>,
        session_cleanup: Option<crate::dispatch::DeferredSessionCleanupRegistration>,
    ) -> Result<DispatchOutcome, AuthorizedDispatchError> {
        if context.transport() != RequestTransport::Network {
            return Err(AuthorizedDispatchError::InvalidNetworkContext);
        }
        if authorized_session.session_id != Some(session.session_id()) {
            return Err(AuthorizedDispatchError::SessionMismatch);
        }
        let original = session
            .original_request_identity()
            .ok_or(AuthorizedDispatchError::MissingOriginalIdentity)?;
        if original.request_id().owner_id() != session.session_id() {
            return Err(AuthorizedDispatchError::SessionMismatch);
        }
        if !original.matches_command(&command) {
            return Err(AuthorizedDispatchError::OriginalIdentityMismatch);
        }

        let request_started = received_at;
        let class = AdmissionClass::for_request_code(original.original_code());
        let lifecycle = RequestLifecycleProvenance::from_network_session(&session);
        let builder = RemotingRequestBuilder::new(original, request_started, context, lifecycle, command);
        let request_bytes = builder.command().body().map_or(0, |body| body.len() as u64);
        let observation = self.processor.begin_boundary_observation(&builder, request_bytes);
        let span = observation
            .as_ref()
            .map_or_else(tracing::Span::none, crate::telemetry::RequestObservation::span);

        async {
            let ordering = self.processor.request_ordering(&builder);
            let resume_executor = authorized_session.executor.deferred_resume_executor();

            if builder.deadline().is_some_and(|deadline| deadline.is_expired()) {
                return send_boundary_response(
                    &session,
                    original,
                    request_started,
                    builder.control().clone(),
                    BoundaryRejectionReason::DeadlineExpired,
                    deadline_response(original.original_opaque()),
                    observation.clone(),
                )
                .await
                .map(BoundaryResponseAttempt::dispatch_outcome);
            }
            if let Decision::Deny { reason } = authorized_session.boundary.security.authorize_for_dispatch(
                builder.command(),
                builder.peer(),
                builder.principal(),
                Resource::new(ResourceKind::Other, original.original_code().to_string()),
                Action::Manage,
            ) {
                let response = RemotingCommand::create_response_command_with_code_remark(
                    rocketmq_protocol::code::response_code::ResponseCode::NoPermission,
                    reason.to_string(),
                );
                return send_boundary_response(
                    &session,
                    original,
                    request_started,
                    builder.control().clone(),
                    BoundaryRejectionReason::SecurityDenied,
                    response,
                    observation.clone(),
                )
                .await
                .map(BoundaryResponseAttempt::dispatch_outcome);
            }

            let admitted_dispatcher = Arc::clone(self);
            let admitted_session = session.clone();
            let admitted_observation = observation.clone();
            let remote_address = session.remote_addr();
            let rejected_session = session.clone();
            let rejected_original = original;
            let rejected_control = builder.control().clone();
            let rejected_observation = observation.clone();
            let boundary_control = builder.control().clone();
            match authorized_session.executor.try_execute(
                retained_bytes,
                class,
                partial_frame_permit,
                ordering,
                move |_operation| async move {
                    let processor = admitted_dispatcher.processor.clone();
                    if let Err(error) = admitted_dispatcher
                        .execute_admitted_network(
                            processor,
                            admitted_session,
                            network_session,
                            class,
                            original,
                            remote_address,
                            request_started,
                            builder,
                            ordering,
                            resume_executor,
                            retained_bytes,
                            session_cleanup,
                            admitted_observation,
                        )
                        .await
                    {
                        admitted_dispatcher.report_admitted_failure(&error);
                    }
                },
                move |_operation, error| async move {
                    let response = admission_response(rejected_original.original_opaque(), &error);
                    if !matches!(
                        send_boundary_response(
                            &rejected_session,
                            rejected_original,
                            request_started,
                            rejected_control,
                            BoundaryRejectionReason::AdmissionRejected,
                            response,
                            rejected_observation,
                        )
                        .await,
                        Ok(BoundaryResponseAttempt::Delivered)
                    ) {
                        tracing::warn!(
                            failure = "admission_boundary_response",
                            " admission rejection response could not be written"
                        );
                    }
                },
            ) {
                Ok(SessionDispatchAttempt::Accepted(task_id)) => Ok(DispatchOutcome::Accepted(task_id)),
                Ok(SessionDispatchAttempt::AdmissionRejected {
                    rejection,
                    retained_partial,
                }) if rejection.policy() == FullPolicy::Reject => {
                    drop(retained_partial);
                    let response = admission_response(original.original_opaque(), &rejection);
                    send_boundary_response(
                        &session,
                        original,
                        request_started,
                        boundary_control,
                        BoundaryRejectionReason::AdmissionRejected,
                        response,
                        observation.clone(),
                    )
                    .await
                    .map(BoundaryResponseAttempt::dispatch_outcome)
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
                Err(error) => Err(AuthorizedDispatchError::Closing(error)),
            }
        }
        .instrument(span)
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the private admitted boundary carries the original session execution metadata without exposing a mutable aggregate"
    )]
    async fn execute_admitted_network(
        &self,
        processor: D,
        session: SessionHandle,
        network_session: D::NetworkSession,
        class: AdmissionClass,
        original: OriginalRequestIdentity,
        remote_address: std::net::SocketAddr,
        request_started: Instant,
        builder: RemotingRequestBuilder,
        ordering: crate::request_ordering::RequestOrdering,
        resume_executor: crate::session_executor::DeferredResumeExecutor,
        retained_bytes: usize,
        session_cleanup: Option<crate::dispatch::DeferredSessionCleanupRegistration>,
        observation: Option<crate::telemetry::RequestObservation>,
    ) -> Result<(), AuthorizedDispatchError> {
        let request_bytes = builder.command().body().map_or(0, |body| body.len() as u64);
        let mut metrics = processor.begin_admitted(&builder, request_bytes, observation);
        let span = metrics.span();
        let observation = Some(metrics.observation().clone());
        let mut observer_owner = AdmittedProcessorObserver::new(processor, observation);
        let Some(processor) = observer_owner.processor_mut() else {
            return Err(AuthorizedDispatchError::Closing(RuntimeError::context_unavailable(
                rocketmq_runtime::RuntimeOperation::SessionExecutor,
            )));
        };
        let result = async {
            let sink = ResponseSink::network(session.clone(), class, builder.control().clone());

            if builder.deadline().is_some_and(|deadline| deadline.is_expired()) {
                let response = RemotingResponse::command(deadline_response(original.original_opaque()))?;
                let candidate = processor.deadline_candidate(response);
                return self
                    .finish_candidate(
                        processor,
                        sink,
                        original,
                        request_started,
                        candidate,
                        retained_bytes,
                        &mut metrics,
                    )
                    .await;
            }

            if let Some(candidate) = processor.reject_request(original.original_code())? {
                return self
                    .finish_candidate(
                        processor,
                        sink,
                        original,
                        request_started,
                        candidate,
                        retained_bytes,
                        &mut metrics,
                    )
                    .await;
            }

            let builder = if original.is_one_way() {
                builder
            } else {
                processor.install_deferred_response(
                    builder,
                    &sink,
                    &session,
                    ordering,
                    class,
                    resume_executor,
                    retained_bytes,
                    session_cleanup,
                    Some(metrics.observation().clone()),
                )?
            };
            let mut request = builder.build()?;
            let hook_snapshot = self.rpc_hooks.snapshot();
            let candidate = processor
                .process(
                    &mut request,
                    hook_snapshot.as_deref(),
                    remote_address,
                    &session,
                    &network_session,
                    &sink,
                )
                .await?;
            let InternalProcessorCandidate { outcome, failure } = candidate;
            let outcome = processor.resolve_outcome(&mut request, outcome)?;
            self.finish_candidate(
                processor,
                sink,
                original,
                request_started,
                InternalProcessorCandidate { outcome, failure },
                retained_bytes,
                &mut metrics,
            )
            .await
        }
        .instrument(span)
        .await;
        observer_owner.bind();
        result
    }

    async fn finish_candidate(
        &self,
        _processor: &D,
        sink: ResponseSink,
        original: OriginalRequestIdentity,
        _request_started: Instant,
        candidate: InternalProcessorCandidate,
        retained_bytes: usize,
        metrics: &mut crate::dispatch::DispatchMetricsGuard,
    ) -> Result<(), AuthorizedDispatchError> {
        let InternalProcessorCandidate { outcome, failure } = candidate;
        match outcome {
            InternalProcessorOutcome::Handled(crate::dispatch::HandlerOutcome::Reply(response)) => {
                let response_code = response.response_code();
                if failure.is_some() {
                    metrics.complete_process_request_failed(response_code);
                }
                if original.is_one_way() {
                    drop(response);
                    if failure.is_none() {
                        metrics.complete_oneway();
                    }
                    if let Some(failure) = failure {
                        self.report_failure_category(failure.category());
                    }
                    return Ok(());
                }
                deliver_and_observe(sink, original, response, failure.is_some(), metrics).await
            }
            InternalProcessorOutcome::Handled(crate::dispatch::HandlerOutcome::Deferred(registration)) => {
                if original.is_one_way() {
                    drop(registration);
                    return Err(AuthorizedDispatchError::OneWayOutcome { outcome: "deferred" });
                }
                metrics.arm_deferred_metrics(retained_bytes);
                match registration.commit() {
                    Ok(()) => {
                        metrics.record_deferred_registered();
                        Ok(())
                    }
                    Err(error) => match error.kind() {
                        DeferredCommitErrorKind::ParentCancelled
                        | DeferredCommitErrorKind::SessionClosed
                        | DeferredCommitErrorKind::DeadlineExpired => Ok(()),
                        DeferredCommitErrorKind::ResponseState | DeferredCommitErrorKind::RegistryInvariant => {
                            Err(AuthorizedDispatchError::DeferredCommit(error))
                        }
                    },
                }
            }
            InternalProcessorOutcome::Handled(crate::dispatch::HandlerOutcome::NoReply(marker)) => {
                if original.is_one_way() {
                    drop(marker);
                    return Err(AuthorizedDispatchError::OneWayOutcome { outcome: "no_reply" });
                }
                drop(marker);
                metrics.complete_protocol_no_response();
                Ok(())
            }
        }
    }

    pub(crate) fn register_rpc_hook(&self, hook: Arc<dyn RPCHook>) {
        self.rpc_hooks.register(hook);
    }

    pub(crate) fn clear_rpc_hook(&self) {
        self.rpc_hooks.clear();
    }

    pub(crate) fn hook_snapshot(&self) -> Option<Arc<crate::hook_registry::HookSnapshot>> {
        self.rpc_hooks.snapshot()
    }

    #[cfg(test)]
    async fn execute_admitted(
        &self,
        processor: D,
        session: SessionHandle,
        class: AdmissionClass,
        original: OriginalRequestIdentity,
        remote_address: std::net::SocketAddr,
        request_started: Instant,
        builder: RemotingRequestBuilder,
    ) -> Result<(), AuthorizedDispatchError> {
        self.execute_admitted_network(
            processor,
            session,
            self.open_network_session(),
            class,
            original,
            remote_address,
            request_started,
            builder,
            crate::request_ordering::RequestOrdering::Concurrent,
            crate::session_executor::DeferredResumeExecutor::retired(),
            0,
            None,
            None,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn dispatch(
        self: &Arc<Self>,
        authorized_session: &AuthorizedDispatchSession,
        session: SessionHandle,
        context: RequestContext,
        command: RemotingCommand,
        retained_bytes: usize,
        partial_frame_permit: Option<PartialFramePermit>,
    ) -> Result<DispatchOutcome, AuthorizedDispatchError> {
        self.dispatch_network(
            authorized_session,
            self.open_network_session(),
            session,
            context,
            command,
            Instant::now(),
            retained_bytes,
            partial_frame_permit,
            None,
        )
        .await
    }

    fn report_admitted_failure(&self, error: &AuthorizedDispatchError) {
        if matches!(error, AuthorizedDispatchError::Response { .. }) {
            return;
        }
        self.report_failure_category(error.category());
    }

    pub(super) fn report_failure_category(&self, category: &'static str) {
        #[cfg(test)]
        self.reported_failures
            .lock()
            .expect(" failure report lock")
            .push(category);
        #[cfg(test)]
        self.reported_failure_notify.notify_waiters();
        tracing::warn!(
            failure = category,
            "admitted dispatch terminated without a response attempt"
        );
    }

    #[cfg(test)]
    pub(super) fn reported_failure_categories(&self) -> Vec<&'static str> {
        self.reported_failures.lock().expect(" failure report lock").clone()
    }

    #[cfg(test)]
    pub(super) async fn wait_for_failure_report(&self) {
        if self.reported_failures.lock().expect(" failure report lock").is_empty() {
            self.reported_failure_notify.notified().await;
        }
    }
}

async fn deliver_and_observe(
    sink: ResponseSink,
    original: OriginalRequestIdentity,
    response: RemotingResponse,
    failure_recorded: bool,
    metrics: &mut crate::dispatch::DispatchMetricsGuard,
) -> Result<(), AuthorizedDispatchError> {
    let response_code = response.response_code();
    let body_kind = response.body_kind();
    let bound = response.bind(original)?;
    let write_started = Instant::now();
    let result = sink.send_response(bound).await;
    let write_elapsed = write_started.elapsed();
    let failed = !matches!(result, Ok(ResponseCompletionOutcome::Completed(_)));
    if !failure_recorded {
        if failed {
            metrics.complete_write_channel_failed(response_code);
        } else {
            metrics.complete_response(response_code);
        }
    }
    metrics.observation().complete_reply(
        crate::runtime::processor::ResponseObservationMode::Inline,
        response_code,
        body_kind,
        write_elapsed,
        match result.as_ref() {
            Ok(ResponseCompletionOutcome::Completed(receipt)) => Ok(*receipt),
            Ok(outcome) => Err((Some(*outcome), response_outcome_progress(*outcome))),
            Err(error) => Err((None, Some(error.write_progress()))),
        },
    );
    result.map(|_| ()).map_err(AuthorizedDispatchError::Response)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BoundaryResponseAttempt {
    Delivered,
    CloseSession,
    SessionClosed,
}

impl BoundaryResponseAttempt {
    const fn dispatch_outcome(self) -> DispatchOutcome {
        match self {
            Self::Delivered => DispatchOutcome::Rejected,
            Self::CloseSession => DispatchOutcome::CloseSession,
            Self::SessionClosed => DispatchOutcome::SessionClosed,
        }
    }
}

async fn send_boundary_response(
    session: &SessionHandle,
    original: OriginalRequestIdentity,
    _request_started: Instant,
    control: crate::dispatch::RequestControlView,
    reason: BoundaryRejectionReason,
    response: RemotingCommand,
    observation: Option<crate::telemetry::RequestObservation>,
) -> Result<BoundaryResponseAttempt, AuthorizedDispatchError> {
    if original.is_one_way() {
        if let Some(observation) = observation {
            observation.complete_boundary_rejection(
                reason,
                None,
                None,
                None,
                crate::runtime::processor::ResponseObservationOutcome::Failed {
                    completion: None,
                    progress: Some(WriteProgress::NotStarted),
                },
            );
        }
        return Ok(BoundaryResponseAttempt::Delivered);
    }
    if let Some(observation) = observation {
        let response = RemotingResponse::command(response)?;
        let response_code = response.response_code();
        let body_kind = response.body_kind();
        let bound = response.bind(original)?;
        let sink = ResponseSink::network(
            session.clone(),
            AdmissionClass::Control,
            control.boundary_response_control(),
        );
        let write_started = Instant::now();
        let result = sink.send_response(bound).await;
        let write_elapsed = write_started.elapsed();
        observation.complete_boundary_rejection(
            reason,
            Some(response_code),
            Some(body_kind),
            Some(write_elapsed),
            match result.as_ref() {
                Ok(ResponseCompletionOutcome::Completed(receipt)) => {
                    crate::runtime::processor::ResponseObservationOutcome::Written(*receipt)
                }
                Ok(outcome) => crate::runtime::processor::ResponseObservationOutcome::Failed {
                    completion: Some(*outcome),
                    progress: response_outcome_progress(*outcome),
                },
                Err(error) => crate::runtime::processor::ResponseObservationOutcome::Failed {
                    completion: None,
                    progress: Some(error.write_progress()),
                },
            },
        );
        return result
            .map(|outcome| match outcome {
                ResponseCompletionOutcome::Completed(_) => BoundaryResponseAttempt::Delivered,
                ResponseCompletionOutcome::SessionClosed => BoundaryResponseAttempt::SessionClosed,
                ResponseCompletionOutcome::AlreadyCompleted(_)
                | ResponseCompletionOutcome::DeadlineExpired
                | ResponseCompletionOutcome::Cancelled
                | ResponseCompletionOutcome::QueueSaturated => BoundaryResponseAttempt::CloseSession,
            })
            .map_err(AuthorizedDispatchError::Response);
    }
    session
        .clone()
        .with_response_class(AdmissionClass::Control)
        .connection()
        .send_command(response.set_opaque(original.original_opaque()))
        .await
        .map(|_| BoundaryResponseAttempt::Delivered)
        .map_err(AuthorizedDispatchError::BoundaryResponse)
}

const fn response_outcome_progress(outcome: ResponseCompletionOutcome) -> Option<WriteProgress> {
    match outcome {
        ResponseCompletionOutcome::Completed(_) | ResponseCompletionOutcome::AlreadyCompleted(_) => None,
        ResponseCompletionOutcome::DeadlineExpired
        | ResponseCompletionOutcome::Cancelled
        | ResponseCompletionOutcome::SessionClosed
        | ResponseCompletionOutcome::QueueSaturated => Some(WriteProgress::NotStarted),
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/dispatch/authorized_dispatcher.rs"]
mod tests;
