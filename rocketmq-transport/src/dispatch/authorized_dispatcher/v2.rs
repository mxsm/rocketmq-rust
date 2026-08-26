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

//! Private V2 dispatch from trusted network ingress to terminal response delivery.

use std::sync::Arc;
use std::time::Instant;

use rocketmq_error::RocketMQError;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_security_api::Action;
use rocketmq_security_api::Decision;
use rocketmq_security_api::Resource;
use rocketmq_security_api::ResourceKind;

use super::admission_response;
use super::deadline_response;
use super::AuthorizedDispatchSession;
use super::DispatchOutcome;
use crate::admission::AdmissionClass;
use crate::admission::AdmissionError;
use crate::admission::FullPolicy;
use crate::admission::PartialFramePermit;
use crate::dispatch::remoting_request::RemotingRequestBuildError;
use crate::dispatch::remoting_request::RemotingRequestBuilder;
use crate::dispatch::remoting_request::RequestLifecycleProvenance;
use crate::dispatch::HandlerOutcome;
use crate::dispatch::HandlerOutcomeContractError;
use crate::dispatch::OriginalRequestIdentity;
use crate::dispatch::RemotingRequest;
use crate::dispatch::RequestContext;
use crate::dispatch::RequestTransport;
use crate::dispatch::ResponseBindingError;
use crate::dispatch::ResponseErrorKind;
use crate::dispatch::ResponsePlan;
use crate::dispatch::ResponsePlanError;
use crate::dispatch::ResponseSink;
use crate::dispatch::WriteProgress;
use crate::hook_registry::HookRegistry;
use crate::hook_registry::HookSnapshot;
use crate::remoting::inner::run_after_rpc_hooks;
use crate::remoting::inner::run_before_rpc_hooks;
use crate::runtime::processor_v2::RejectRequestDecision;
use crate::runtime::processor_v2::RequestProcessorV2;
use crate::runtime::processor_v2::ResponseWriteObservationV2;
use crate::runtime::processor_v2::ResponseWritePath;
use crate::runtime::RPCHook;
use crate::server::SessionHandle;
use crate::session_executor::SessionDispatchError;

/// Stable private failure from scheduling or executing one V2 dispatch.
#[derive(Debug, thiserror::Error)]
#[allow(
    dead_code,
    reason = "DSP-03 defines the private dispatcher failure consumed by later coexistence routing"
)]
pub(crate) enum AuthorizedDispatchV2Error {
    #[error("V2 dispatch requires a network request context")]
    InvalidNetworkContext,
    #[error("V2 dispatch requires the canonical ingress request identity")]
    MissingOriginalIdentity,
    #[error("the canonical ingress identity no longer matches the request command")]
    OriginalIdentityMismatch,
    #[error("the authorized dispatch session does not own the canonical network session")]
    SessionMismatch,
    #[error("authorized V2 dispatch session is closing: {0}")]
    Closing(String),
    #[error("authorized V2 dispatch admission failed: {0}")]
    Admission(#[source] AdmissionError),
    #[error("V2 boundary response failed")]
    BoundaryResponse(#[source] RocketMQError),
    #[error(transparent)]
    RequestBuild(#[from] RemotingRequestBuildError),
    #[error(transparent)]
    ResponsePlan(#[from] ResponsePlanError),
    #[error(transparent)]
    ResponseBinding(#[from] ResponseBindingError),
    #[error(transparent)]
    HandlerContract(#[from] HandlerOutcomeContractError),
    #[error("one-way requests cannot complete with {outcome}")]
    OneWayOutcome { outcome: &'static str },
    #[error("V2 response delivery failed: {kind:?}, progress={progress:?}")]
    Response {
        kind: ResponseErrorKind,
        progress: Option<WriteProgress>,
    },
}

impl AuthorizedDispatchV2Error {
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
            Self::OneWayOutcome { .. } => "one_way_outcome",
            Self::Response { .. } => "response",
        }
    }
}

/// Network-only V2 processor boundary.
#[allow(
    dead_code,
    reason = "DSP-03 defines the private dispatcher core wired by the later coexistence stage"
)]
pub(crate) struct AuthorizedCommandDispatcherV2<P> {
    processor: P,
    rpc_hooks: HookRegistry,
    #[cfg(test)]
    reported_failures: std::sync::Mutex<Vec<&'static str>>,
}

struct HandlerCandidate {
    outcome: HandlerOutcome,
    failure: Option<HandlerFailureOrigin>,
}

impl HandlerCandidate {
    const fn success(outcome: HandlerOutcome) -> Self {
        Self { outcome, failure: None }
    }

    const fn failure(outcome: HandlerOutcome, failure: HandlerFailureOrigin) -> Self {
        Self {
            outcome,
            failure: Some(failure),
        }
    }
}

#[derive(Clone, Copy)]
enum HandlerFailureOrigin {
    BeforeHook,
    ProcessorError,
    Deadline,
    AfterHook,
    ProcessorErrorAfterHookError,
}

impl HandlerFailureOrigin {
    const fn category(self) -> &'static str {
        match self {
            Self::BeforeHook => "before_hook",
            Self::ProcessorError => "processor_error",
            Self::Deadline => "deadline",
            Self::AfterHook => "after_hook",
            Self::ProcessorErrorAfterHookError => "processor_error_after_hook_error",
        }
    }

    const fn after_hook_error(self) -> Self {
        match self {
            Self::ProcessorError => Self::ProcessorErrorAfterHookError,
            Self::BeforeHook | Self::Deadline | Self::AfterHook | Self::ProcessorErrorAfterHookError => Self::AfterHook,
        }
    }
}

impl<P> AuthorizedCommandDispatcherV2<P>
where
    P: RequestProcessorV2 + Clone + Sync + 'static,
{
    #[allow(
        dead_code,
        reason = "DSP-03 construction remains private until later coexistence routing"
    )]
    pub(crate) fn new(processor: P, rpc_hooks: Vec<Arc<dyn RPCHook>>) -> Self {
        Self {
            processor,
            rpc_hooks: HookRegistry::new(rpc_hooks),
            #[cfg(test)]
            reported_failures: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Admits one canonical network request into its existing session executor.
    #[allow(
        dead_code,
        reason = "DSP-03 dispatch remains private until later coexistence routing"
    )]
    pub(crate) async fn dispatch(
        self: &Arc<Self>,
        authorized_session: &AuthorizedDispatchSession,
        session: SessionHandle,
        context: RequestContext,
        command: RemotingCommand,
        retained_bytes: usize,
        partial_frame_permit: Option<PartialFramePermit>,
    ) -> Result<DispatchOutcome, AuthorizedDispatchV2Error> {
        if context.transport() != RequestTransport::Network {
            return Err(AuthorizedDispatchV2Error::InvalidNetworkContext);
        }
        if authorized_session.session_id != Some(session.session_id()) {
            return Err(AuthorizedDispatchV2Error::SessionMismatch);
        }
        let original = session
            .original_request_identity()
            .ok_or(AuthorizedDispatchV2Error::MissingOriginalIdentity)?;
        if original.request_id().owner_id() != session.session_id() {
            return Err(AuthorizedDispatchV2Error::SessionMismatch);
        }
        if !original.matches_command(&command) {
            return Err(AuthorizedDispatchV2Error::OriginalIdentityMismatch);
        }

        let request_started = Instant::now();
        let class = AdmissionClass::for_request_code(original.original_code());
        let lifecycle = RequestLifecycleProvenance::from_network_session(&session);
        let builder = RemotingRequestBuilder::new(original, request_started, context, lifecycle, command);
        let ordering = self.processor.request_ordering(builder.ingress_view());

        if builder.deadline().is_some_and(|deadline| deadline.is_expired()) {
            send_boundary_response(&session, original, deadline_response(original.original_opaque())).await?;
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
            send_boundary_response(&session, original, response).await?;
            return Ok(DispatchOutcome::Rejected);
        }

        let admitted_dispatcher = Arc::clone(self);
        let admitted_session = session.clone();
        let remote_address = session.remote_addr();
        let rejected_session = session.clone();
        let rejected_original = original;
        match authorized_session.executor.try_execute(
            retained_bytes,
            class,
            partial_frame_permit,
            ordering,
            move |_operation| async move {
                let processor = admitted_dispatcher.processor.clone();
                if let Err(error) = admitted_dispatcher
                    .execute_admitted(
                        processor,
                        admitted_session,
                        class,
                        original,
                        remote_address,
                        request_started,
                        builder,
                    )
                    .await
                {
                    admitted_dispatcher.report_admitted_failure(&error);
                }
            },
            move |_operation, error| async move {
                let response = admission_response(rejected_original.original_opaque(), &error);
                if send_boundary_response(&rejected_session, rejected_original, response)
                    .await
                    .is_err()
                {
                    tracing::warn!(
                        failure = "admission_boundary_response",
                        "V2 admission rejection response could not be written"
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
                send_boundary_response(&session, original, response).await?;
                Ok(DispatchOutcome::Rejected)
            }
            Err(SessionDispatchError::Admission {
                error,
                retained_partial,
            }) => {
                drop(retained_partial);
                Err(AuthorizedDispatchV2Error::Admission(error))
            }
            Err(SessionDispatchError::Closing(error)) => Err(AuthorizedDispatchV2Error::Closing(error.to_string())),
        }
    }

    #[allow(
        dead_code,
        reason = "DSP-03 admitted execution is reached through the not-yet-wired private dispatcher"
    )]
    async fn execute_admitted(
        &self,
        mut processor: P,
        session: SessionHandle,
        class: AdmissionClass,
        original: OriginalRequestIdentity,
        remote_address: std::net::SocketAddr,
        request_started: Instant,
        builder: RemotingRequestBuilder,
    ) -> Result<(), AuthorizedDispatchV2Error> {
        let response = ResponseSink::network_plan(session, class, builder.control().clone());

        if builder.deadline().is_some_and(|deadline| deadline.is_expired()) {
            let plan = ResponsePlan::command(deadline_response(original.original_opaque()))?;
            if original.is_one_way() {
                drop(plan);
                self.report_failure_category(HandlerFailureOrigin::Deadline.category());
                return Ok(());
            }
            return deliver_and_observe(&processor, response, original, request_started, plan).await;
        }

        match processor.reject_request(original.original_code()) {
            RejectRequestDecision::Proceed => {}
            RejectRequestDecision::Reject(plan) => {
                if original.is_one_way() {
                    drop(plan);
                    return Ok(());
                }
                return deliver_and_observe(&processor, response, original, request_started, plan).await;
            }
        }

        let mut request = builder.build()?;
        let hook_snapshot = self.rpc_hooks.snapshot();
        let before_result = request.with_body_free_hook_command(|request_head| {
            run_before_rpc_hooks(hook_snapshot.as_deref(), remote_address, request_head)
        });
        let candidate = match before_result {
            Ok(()) => {
                let processed = match request.meta().deadline() {
                    Some(deadline) => deadline.timeout(processor.process(&mut request)).await,
                    None => Ok(processor.process(&mut request).await),
                };
                match processed {
                    Ok(Ok(outcome)) => apply_after_hook(
                        &mut request,
                        HandlerCandidate::success(outcome),
                        hook_snapshot.as_deref(),
                        remote_address,
                    )?,
                    Ok(Err(error)) => {
                        let plan = crate::error_response::response_plan_from_error(&error)?;
                        apply_after_hook(
                            &mut request,
                            HandlerCandidate::failure(
                                HandlerOutcome::Reply(plan),
                                HandlerFailureOrigin::ProcessorError,
                            ),
                            hook_snapshot.as_deref(),
                            remote_address,
                        )?
                    }
                    Err(_) => HandlerCandidate::failure(
                        HandlerOutcome::Reply(ResponsePlan::command(deadline_response(original.original_opaque()))?),
                        HandlerFailureOrigin::Deadline,
                    ),
                }
            }
            Err(error) => HandlerCandidate::failure(
                HandlerOutcome::Reply(crate::error_response::response_plan_from_error(&error)?),
                HandlerFailureOrigin::BeforeHook,
            ),
        };

        let outcome = request.resolve_handler_outcome(candidate.outcome)?;
        if original.is_one_way() {
            return match outcome {
                HandlerOutcome::Reply(plan) => {
                    drop(plan);
                    if let Some(failure) = candidate.failure {
                        self.report_failure_category(failure.category());
                    }
                    Ok(())
                }
                HandlerOutcome::Deferred(registration) => {
                    drop(registration);
                    Err(AuthorizedDispatchV2Error::OneWayOutcome { outcome: "deferred" })
                }
                HandlerOutcome::NoReply(marker) => {
                    drop(marker);
                    Err(AuthorizedDispatchV2Error::OneWayOutcome { outcome: "no_reply" })
                }
            };
        }

        match outcome {
            HandlerOutcome::Reply(plan) => {
                deliver_and_observe(&processor, response, original, request_started, plan).await
            }
            HandlerOutcome::Deferred(registration) => {
                drop(registration);
                Ok(())
            }
            HandlerOutcome::NoReply(marker) => {
                drop(marker);
                Ok(())
            }
        }
    }

    #[cfg(test)]
    fn register_rpc_hook(&self, hook: Arc<dyn RPCHook>) {
        self.rpc_hooks.register(hook);
    }

    fn report_admitted_failure(&self, error: &AuthorizedDispatchV2Error) {
        if matches!(error, AuthorizedDispatchV2Error::Response { .. }) {
            return;
        }
        self.report_failure_category(error.category());
    }

    fn report_failure_category(&self, category: &'static str) {
        #[cfg(test)]
        self.reported_failures
            .lock()
            .expect("V2 failure report lock")
            .push(category);
        tracing::warn!(
            failure = category,
            "admitted V2 dispatch terminated without a response attempt"
        );
    }

    #[cfg(test)]
    fn reported_failure_categories(&self) -> Vec<&'static str> {
        self.reported_failures.lock().expect("V2 failure report lock").clone()
    }
}

#[allow(
    dead_code,
    reason = "DSP-03 hook application is reached through the not-yet-wired private dispatcher"
)]
fn apply_after_hook(
    request: &mut RemotingRequest,
    candidate: HandlerCandidate,
    hook_snapshot: Option<&HookSnapshot>,
    remote_address: std::net::SocketAddr,
) -> Result<HandlerCandidate, AuthorizedDispatchV2Error> {
    let HandlerCandidate { outcome, failure } = candidate;
    let HandlerOutcome::Reply(mut plan) = outcome else {
        return Ok(HandlerCandidate { outcome, failure });
    };

    let result = request.with_body_free_hook_request(|request_head| {
        plan.with_body_free_hook_head(|response_head| {
            run_after_rpc_hooks(hook_snapshot, remote_address, request_head, response_head)
        })
    });
    match result {
        Ok(()) => Ok(HandlerCandidate {
            outcome: HandlerOutcome::Reply(plan),
            failure,
        }),
        Err(error) => {
            drop(plan);
            let failure = failure
                .map(HandlerFailureOrigin::after_hook_error)
                .unwrap_or(HandlerFailureOrigin::AfterHook);
            Ok(HandlerCandidate::failure(
                HandlerOutcome::Reply(crate::error_response::response_plan_from_error(&error)?),
                failure,
            ))
        }
    }
}

#[allow(
    dead_code,
    reason = "DSP-03 delivery is reached through the not-yet-wired private dispatcher"
)]
async fn deliver_and_observe<P>(
    processor: &P,
    response: ResponseSink,
    original: OriginalRequestIdentity,
    request_started: Instant,
    plan: ResponsePlan,
) -> Result<(), AuthorizedDispatchV2Error>
where
    P: RequestProcessorV2,
{
    let response_code = plan.response_code();
    let body_kind = plan.body_kind();
    let bound = plan.bind(original)?;
    let write_started = Instant::now();
    let result = response.send_plan(bound).await;
    let write_elapsed = write_started.elapsed();
    let end_to_end_elapsed = request_started.elapsed();

    match result {
        Ok(receipt) => {
            processor.observe_response_write(ResponseWriteObservationV2::from_result(
                original.request_id(),
                original.original_code(),
                response_code,
                body_kind,
                ResponseWritePath::Inline,
                write_elapsed,
                end_to_end_elapsed,
                Ok(receipt),
            ));
            Ok(())
        }
        Err(error) => {
            let kind = error.kind();
            let progress = error.write_progress();
            processor.observe_response_write(ResponseWriteObservationV2::from_result(
                original.request_id(),
                original.original_code(),
                response_code,
                body_kind,
                ResponseWritePath::Inline,
                write_elapsed,
                end_to_end_elapsed,
                Err(error),
            ));
            Err(AuthorizedDispatchV2Error::Response { kind, progress })
        }
    }
}

#[allow(
    dead_code,
    reason = "DSP-03 boundary rejection is reached through the not-yet-wired private dispatcher"
)]
async fn send_boundary_response(
    session: &SessionHandle,
    original: OriginalRequestIdentity,
    response: RemotingCommand,
) -> Result<(), AuthorizedDispatchV2Error> {
    if original.is_one_way() {
        return Ok(());
    }
    session
        .clone()
        .with_response_class(AdmissionClass::Control)
        .connection()
        .send_command(response.set_opaque(original.original_opaque()))
        .await
        .map_err(AuthorizedDispatchV2Error::BoundaryResponse)
}

#[cfg(test)]
#[path = "v2/tests.rs"]
mod tests;
