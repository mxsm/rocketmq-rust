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
use crate::admission::AdmissionError;
use crate::admission::FullPolicy;
use crate::admission::PartialFramePermit;
use crate::base::pending_request_table::PendingRequestTable;
use crate::dispatch::processor_adapter::AdmittedProcessorObserver;
use crate::dispatch::remoting_request::RemotingRequestBuildError;
use crate::dispatch::remoting_request::RemotingRequestBuilder;
use crate::dispatch::remoting_request::RequestLifecycleProvenance;
use crate::dispatch::DeferredCommitError;
use crate::dispatch::DispatchProcessor;
use crate::dispatch::DispatchProcessorError;
use crate::dispatch::ExplicitProcessor;
use crate::dispatch::HandlerOutcomeContractError;
use crate::dispatch::InternalProcessorCandidate;
use crate::dispatch::InternalProcessorOutcome;
use crate::dispatch::OriginalRequestIdentity;
use crate::dispatch::RequestContext;
use crate::dispatch::RequestTransport;
use crate::dispatch::ResponseBindingError;
use crate::dispatch::ResponseErrorKind;
use crate::dispatch::ResponsePlan;
use crate::dispatch::ResponsePlanError;
use crate::dispatch::ResponseSink;
use crate::dispatch::WriteProgress;
use crate::hook_registry::HookRegistry;
use crate::runtime::processor::RequestProcessor;
use crate::runtime::RPCHook;
use crate::server::SessionHandle;
use crate::session_executor::SessionDispatchError;
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
    #[error("authorized dispatch session is closing: {0}")]
    Closing(String),
    #[error("authorized dispatch admission failed: {0}")]
    Admission(#[source] AdmissionError),
    #[error(" boundary response failed")]
    BoundaryResponse(#[source] RocketMQError),
    #[error(transparent)]
    RequestBuild(#[from] RemotingRequestBuildError),
    #[error(transparent)]
    ResponsePlan(#[from] ResponsePlanError),
    #[error(transparent)]
    ResponseBinding(#[from] ResponseBindingError),
    #[error(transparent)]
    HandlerContract(#[from] HandlerOutcomeContractError),
    #[error(transparent)]
    DeferredCommit(#[from] DeferredCommitError),
    #[error("one-way requests cannot complete with {outcome}")]
    OneWayOutcome { outcome: &'static str },
    #[error(" response delivery failed: {kind:?}, progress={progress:?}")]
    Response {
        kind: ResponseErrorKind,
        progress: Option<WriteProgress>,
    },
}

impl AuthorizedDispatchError {
    const fn category(&self) -> &'static str {
        match self {
            Self::InvalidNetworkContext => "invalid_network_context",
            Self::MissingOriginalIdentity => "missing_original_identity",
            Self::OriginalIdentityMismatch => "original_identity_mismatch",
            Self::SessionMismatch => "session_mismatch",
            Self::Closing(_) => "closing",
            Self::Admission(_) => "admission",
            Self::BoundaryResponse(_) => "boundary_response",
            Self::RequestBuild(_) => "request_build",
            Self::ResponsePlan(_) => "response_plan",
            Self::ResponseBinding(_) => "response_binding",
            Self::HandlerContract(_) => "handler_contract",
            Self::DeferredCommit(error) => error.category(),
            Self::OneWayOutcome { .. } => "one_way_outcome",
            Self::Response { .. } => "response",
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
            DispatchProcessorError::ResponsePlan(error) => Self::ResponsePlan(error),
            DispatchProcessorError::HandlerContract(error) => Self::HandlerContract(error),
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
                send_boundary_response(
                    &session,
                    original,
                    request_started,
                    builder.control().clone(),
                    BoundaryRejectionReason::DeadlineExpired,
                    deadline_response(original.original_opaque()),
                    observation.clone(),
                )
                .await?;
                return Ok(DispatchOutcome::Rejected);
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
                send_boundary_response(
                    &session,
                    original,
                    request_started,
                    builder.control().clone(),
                    BoundaryRejectionReason::SecurityDenied,
                    response,
                    observation.clone(),
                )
                .await?;
                return Ok(DispatchOutcome::Rejected);
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
                    if send_boundary_response(
                        &rejected_session,
                        rejected_original,
                        request_started,
                        rejected_control,
                        BoundaryRejectionReason::AdmissionRejected,
                        response,
                        rejected_observation,
                    )
                    .await
                    .is_err()
                    {
                        tracing::warn!(
                            failure = "admission_boundary_response",
                            " admission rejection response could not be written"
                        );
                    }
                },
            ) {
                Ok(task_id) => Ok(DispatchOutcome::Accepted(task_id)),
                Err(SessionDispatchError::Admission {
                    error,
                    retained_partial,
                }) if error.policy() == FullPolicy::Reject => {
                    drop(retained_partial);
                    let response = admission_response(original.original_opaque(), &error);
                    send_boundary_response(
                        &session,
                        original,
                        request_started,
                        boundary_control,
                        BoundaryRejectionReason::AdmissionRejected,
                        response,
                        observation.clone(),
                    )
                    .await?;
                    Ok(DispatchOutcome::Rejected)
                }
                Err(SessionDispatchError::Admission {
                    error,
                    retained_partial,
                }) => {
                    drop(retained_partial);
                    Err(AuthorizedDispatchError::Admission(error))
                }
                Err(SessionDispatchError::Closing(error)) => Err(AuthorizedDispatchError::Closing(error.to_string())),
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
            return Err(AuthorizedDispatchError::Closing(
                "admitted response observer owner is unavailable".to_owned(),
            ));
        };
        let result = async {
            let response = ResponseSink::network_plan(session.clone(), class, builder.control().clone());

            if builder.deadline().is_some_and(|deadline| deadline.is_expired()) {
                let plan = ResponsePlan::command(deadline_response(original.original_opaque()))?;
                let candidate = processor.deadline_candidate(plan);
                return self
                    .finish_candidate(
                        processor,
                        response,
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
                        response,
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
                    &response,
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
                    &response,
                )
                .await?;
            let InternalProcessorCandidate { outcome, failure } = candidate;
            let outcome = processor.resolve_outcome(&mut request, outcome)?;
            self.finish_candidate(
                processor,
                response,
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
        response: ResponseSink,
        original: OriginalRequestIdentity,
        _request_started: Instant,
        candidate: InternalProcessorCandidate,
        retained_bytes: usize,
        metrics: &mut crate::dispatch::DispatchMetricsGuard,
    ) -> Result<(), AuthorizedDispatchError> {
        let InternalProcessorCandidate { outcome, failure } = candidate;
        match outcome {
            InternalProcessorOutcome::Handled(crate::dispatch::HandlerOutcome::Reply(plan)) => {
                let response_code = plan.response_code();
                if failure.is_some() {
                    metrics.complete_process_request_failed(response_code);
                }
                if original.is_one_way() {
                    drop(plan);
                    if failure.is_none() {
                        metrics.complete_oneway();
                    }
                    if let Some(failure) = failure {
                        self.report_failure_category(failure.category());
                    }
                    return Ok(());
                }
                deliver_and_observe(response, original, plan, failure.is_some(), metrics).await
            }
            InternalProcessorOutcome::Handled(crate::dispatch::HandlerOutcome::Deferred(registration)) => {
                if original.is_one_way() {
                    drop(registration);
                    return Err(AuthorizedDispatchError::OneWayOutcome { outcome: "deferred" });
                }
                metrics.arm_deferred_metrics(retained_bytes);
                registration.commit()?;
                metrics.record_deferred_registered();
                Ok(())
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
    response: ResponseSink,
    original: OriginalRequestIdentity,
    plan: ResponsePlan,
    failure_recorded: bool,
    metrics: &mut crate::dispatch::DispatchMetricsGuard,
) -> Result<(), AuthorizedDispatchError> {
    let response_code = plan.response_code();
    let body_kind = plan.body_kind();
    let bound = plan.bind(original)?;
    let write_started = Instant::now();
    let result = response.send_plan(bound).await;
    let write_elapsed = write_started.elapsed();
    let failure = result
        .as_ref()
        .err()
        .map(|error| (error.kind(), error.write_progress()));
    if !failure_recorded {
        if failure.is_some() {
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
            Ok(receipt) => Ok(*receipt),
            Err(error) => Err((error.kind(), error.write_progress())),
        },
    );
    match failure {
        Some((kind, progress)) => Err(AuthorizedDispatchError::Response { kind, progress }),
        None => Ok(()),
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
) -> Result<(), AuthorizedDispatchError> {
    if original.is_one_way() {
        if let Some(observation) = observation {
            observation.complete_boundary_rejection(
                reason,
                None,
                None,
                None,
                crate::runtime::processor::ResponseObservationOutcome::Failed {
                    kind: None,
                    progress: Some(WriteProgress::NotStarted),
                },
            );
        }
        return Ok(());
    }
    if let Some(observation) = observation {
        let plan = ResponsePlan::command(response)?;
        let response_code = plan.response_code();
        let body_kind = plan.body_kind();
        let bound = plan.bind(original)?;
        let response = ResponseSink::network_plan(
            session.clone(),
            AdmissionClass::Control,
            control.boundary_response_control(),
        );
        let write_started = Instant::now();
        let result = response.send_plan(bound).await;
        let write_elapsed = write_started.elapsed();
        observation.complete_boundary_rejection(
            reason,
            Some(response_code),
            Some(body_kind),
            Some(write_elapsed),
            match result.as_ref() {
                Ok(receipt) => crate::runtime::processor::ResponseObservationOutcome::Written(*receipt),
                Err(error) => crate::runtime::processor::ResponseObservationOutcome::Failed {
                    kind: Some(error.kind()),
                    progress: error.write_progress(),
                },
            },
        );
        return result.map(|_| ()).map_err(|error| AuthorizedDispatchError::Response {
            kind: error.kind(),
            progress: error.write_progress(),
        });
    }
    session
        .clone()
        .with_response_class(AdmissionClass::Control)
        .connection()
        .send_command(response.set_opaque(original.original_opaque()))
        .await
        .map_err(AuthorizedDispatchError::BoundaryResponse)
}

#[cfg(test)]
#[path = "../../../tests/unit/dispatch/authorized_dispatcher.rs"]
mod tests;
