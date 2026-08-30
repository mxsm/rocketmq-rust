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

//! Sealed V2 processor boundary behind the private dispatch core.

use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

use rocketmq_error::RocketMQError;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

use super::remoting_request::RemotingRequestBuilder;
use super::HandlerOutcome;
use super::HandlerOutcomeContractError;
use super::OriginalRequestIdentity;
use super::RemotingRequest;
use super::ResponseBodyKind;
use super::ResponseError;
use super::ResponsePlan;
use super::ResponsePlanError;
use super::ResponseReceipt;
use super::ResponseSink;
use crate::base::pending_request_table::PendingRequestOwner;
use crate::base::pending_request_table::PendingRequestTable;
use crate::hook_registry::HookSnapshot;
use crate::remoting::inner::run_after_rpc_hooks;
use crate::remoting::inner::run_before_rpc_hooks;
use crate::request_ordering::RequestOrdering;
use crate::runtime::processor_v2::RejectRequestDecision;
use crate::runtime::processor_v2::RequestProcessorV2;
use crate::runtime::processor_v2::ResponseObservationOutcomeV2;
use crate::runtime::processor_v2::ResponseWriteObservationV2;
use crate::runtime::processor_v2::ResponseWritePath;
use crate::server::SessionHandle;
use crate::telemetry::TransportTelemetry;

mod sealed {
    pub trait Sealed {}
}

/// Stable response-correlation owner allocated once for one canonical V2
/// network session.
#[derive(Clone)]
pub(crate) struct V2NetworkSession {
    response_table: PendingRequestTable,
    owner: PendingRequestOwner,
}

impl V2NetworkSession {
    pub(crate) fn response_table(&self) -> &PendingRequestTable {
        &self.response_table
    }

    pub(crate) fn owner(&self) -> &PendingRequestOwner {
        &self.owner
    }
}

/// Explicit wrapper preventing the V2 implementation from overlapping the
/// legacy compatibility implementation.
pub(crate) struct ExplicitV2Processor<P> {
    processor: P,
    response_table: PendingRequestTable,
    telemetry: TransportTelemetry,
}

impl<P> ExplicitV2Processor<P> {
    pub(crate) fn new(processor: P) -> Self {
        Self::with_response_table(processor, PendingRequestTable::new())
    }

    pub(crate) fn with_response_table(processor: P, response_table: PendingRequestTable) -> Self {
        Self::with_response_table_and_telemetry(processor, response_table, TransportTelemetry::noop())
    }

    pub(crate) const fn with_response_table_and_telemetry(
        processor: P,
        response_table: PendingRequestTable,
        telemetry: TransportTelemetry,
    ) -> Self {
        Self {
            processor,
            response_table,
            telemetry,
        }
    }
}

mod embedded_v2_processor;
pub(crate) use embedded_v2_processor::EmbeddedProcessorResolveError;
pub(crate) use embedded_v2_processor::EmbeddedResolvedOutcome;

impl<P> Clone for ExplicitV2Processor<P>
where
    P: Clone,
{
    fn clone(&self) -> Self {
        Self {
            processor: self.processor.clone(),
            response_table: self.response_table.clone(),
            telemetry: self.telemetry.clone(),
        }
    }
}

/// Internal terminal result before immutable binding and delivery.
pub(crate) enum InternalProcessorOutcome {
    V2(HandlerOutcome),
}

pub(crate) struct InternalProcessorCandidate {
    pub(crate) outcome: InternalProcessorOutcome,
    pub(crate) failure: Option<InternalFailureOrigin>,
    pub(crate) observe_write: bool,
}

impl InternalProcessorCandidate {
    pub(crate) fn success(outcome: InternalProcessorOutcome) -> Self {
        Self {
            outcome,
            failure: None,
            observe_write: true,
        }
    }

    pub(crate) fn failure(outcome: InternalProcessorOutcome, failure: InternalFailureOrigin) -> Self {
        Self {
            outcome,
            failure: Some(failure),
            observe_write: true,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum InternalFailureOrigin {
    BeforeHook,
    ProcessorError,
    Deadline,
    AfterHook,
    ProcessorErrorAfterHookError,
}

impl InternalFailureOrigin {
    pub(crate) const fn category(self) -> &'static str {
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

#[derive(Debug, thiserror::Error)]
pub(crate) enum DispatchProcessorError {
    #[error(transparent)]
    ResponsePlan(#[from] ResponsePlanError),
    #[error(transparent)]
    HandlerContract(#[from] HandlerOutcomeContractError),
}

pub(crate) enum DispatchMetricsGuard {
    ExplicitV2 {
        observation: crate::telemetry::V2RequestObservation,
    },
}

impl DispatchMetricsGuard {
    pub(crate) fn complete_response(&mut self, response_code: i32) {
        let _ = response_code;
    }

    pub(crate) fn complete_oneway(&mut self) {
        let Self::ExplicitV2 { observation } = self;
        observation.complete_no_response(ResponseObservationOutcomeV2::Oneway);
    }

    pub(crate) fn complete_protocol_no_response(&mut self) {
        let Self::ExplicitV2 { observation } = self;
        observation.complete_no_response(ResponseObservationOutcomeV2::ProtocolNoResponse);
    }

    pub(crate) fn arm_deferred_metrics(&mut self, retained_bytes: usize) {
        let Self::ExplicitV2 { observation } = self;
        observation.arm_deferred_metrics(retained_bytes);
    }

    pub(crate) fn record_deferred_registered(&mut self) {
        let Self::ExplicitV2 { observation } = self;
        observation.record_deferred_registered();
    }

    pub(crate) fn v2_observation(&self) -> Option<&crate::telemetry::V2RequestObservation> {
        let Self::ExplicitV2 { observation } = self;
        Some(observation)
    }

    pub(crate) fn span(&self) -> tracing::Span {
        let Self::ExplicitV2 { observation } = self;
        observation.span()
    }

    pub(crate) fn complete_process_request_failed(&mut self, response_code: i32) {
        let Self::ExplicitV2 { observation } = self;
        observation.complete_request_failed(response_code);
    }

    pub(crate) fn complete_write_channel_failed(&mut self, response_code: i32) {
        let Self::ExplicitV2 { observation } = self;
        observation.complete_write_failed(response_code);
    }
}

/// Sealed statically dispatched processor boundary used by the V2 core.
pub(crate) trait DispatchProcessor: sealed::Sealed + Clone + Send + Sync + 'static {
    type NetworkSession: Clone + Send + Sync + 'static;

    fn open_network_session(&self) -> Self::NetworkSession;

    fn complete_network_response(&self, session: &Self::NetworkSession, response: RemotingCommand);

    fn close_network_session(&self, session: &Self::NetworkSession);

    fn request_ordering(&self, builder: &RemotingRequestBuilder) -> RequestOrdering;

    fn begin_boundary_observation(
        &self,
        builder: &RemotingRequestBuilder,
        request_bytes: u64,
    ) -> Option<crate::telemetry::V2RequestObservation>;

    fn begin_admitted(
        &self,
        builder: &RemotingRequestBuilder,
        request_bytes: u64,
        observation: Option<crate::telemetry::V2RequestObservation>,
    ) -> DispatchMetricsGuard;

    fn bind_response_observer(self, observation: Option<crate::telemetry::V2RequestObservation>);

    fn reject_request(&self, request_code: i32) -> Result<Option<InternalProcessorCandidate>, DispatchProcessorError>;

    fn deadline_candidate(&self, plan: ResponsePlan) -> InternalProcessorCandidate;

    fn install_deferred_response(
        &self,
        builder: RemotingRequestBuilder,
        response: &ResponseSink,
        session: &SessionHandle,
        ordering: RequestOrdering,
        class: crate::admission::AdmissionClass,
        resume_executor: crate::session_executor::DeferredResumeExecutor,
        retained_bytes: usize,
        session_cleanup: Option<crate::dispatch::DeferredSessionCleanupRegistration>,
        observation: Option<crate::telemetry::V2RequestObservation>,
    ) -> Result<RemotingRequestBuilder, super::remoting_request::RemotingRequestBuildError>;

    fn process(
        &mut self,
        request: &mut RemotingRequest,
        hook_snapshot: Option<&HookSnapshot>,
        remote_address: SocketAddr,
        session: &SessionHandle,
        network_session: &Self::NetworkSession,
        response: &ResponseSink,
    ) -> impl Future<Output = Result<InternalProcessorCandidate, DispatchProcessorError>> + Send;

    fn resolve_outcome(
        &self,
        request: &mut RemotingRequest,
        outcome: InternalProcessorOutcome,
    ) -> Result<InternalProcessorOutcome, DispatchProcessorError>;

    fn observe_response_write(
        &self,
        original: OriginalRequestIdentity,
        response_code: i32,
        body_kind: ResponseBodyKind,
        write_elapsed: Duration,
        end_to_end_elapsed: Duration,
        result: Result<ResponseReceipt, ResponseError>,
    );
}

pub(crate) struct AdmittedProcessorObserver<D>
where
    D: DispatchProcessor,
{
    processor: Option<D>,
    observation: Option<crate::telemetry::V2RequestObservation>,
}

impl<D> AdmittedProcessorObserver<D>
where
    D: DispatchProcessor,
{
    pub(crate) fn new(processor: D, observation: Option<crate::telemetry::V2RequestObservation>) -> Self {
        Self {
            processor: Some(processor),
            observation,
        }
    }

    pub(crate) fn processor_mut(&mut self) -> Option<&mut D> {
        self.processor.as_mut()
    }

    pub(crate) fn bind(&mut self) {
        if let Some(processor) = self.processor.take() {
            processor.bind_response_observer(self.observation.take());
        }
    }
}

impl<D> Drop for AdmittedProcessorObserver<D>
where
    D: DispatchProcessor,
{
    fn drop(&mut self) {
        self.bind();
    }
}

impl<P> sealed::Sealed for ExplicitV2Processor<P> {}

impl<P> DispatchProcessor for ExplicitV2Processor<P>
where
    P: RequestProcessorV2 + Clone + Sync + 'static,
{
    type NetworkSession = V2NetworkSession;

    fn open_network_session(&self) -> Self::NetworkSession {
        V2NetworkSession {
            response_table: self.response_table.clone(),
            owner: self.response_table.new_owner(),
        }
    }

    fn complete_network_response(&self, session: &Self::NetworkSession, response: RemotingCommand) {
        if !session
            .response_table
            .complete_response_for_owner(&session.owner, response.opaque(), response)
        {
            tracing::warn!(
                frame = "unexpected_response",
                generation = "v2",
                "unmatched response frame dropped on V2 transport session"
            );
        }
    }

    fn close_network_session(&self, session: &Self::NetworkSession) {
        session.response_table.close_owner(&session.owner, || {
            RocketMQError::network_connection_failed(
                "v2-server-request",
                "canonical V2 session closed while awaiting response",
            )
        });
    }

    fn request_ordering(&self, builder: &RemotingRequestBuilder) -> RequestOrdering {
        self.processor.request_ordering(builder.ingress_view())
    }

    fn begin_boundary_observation(
        &self,
        builder: &RemotingRequestBuilder,
        request_bytes: u64,
    ) -> Option<crate::telemetry::V2RequestObservation> {
        let original = builder.ingress_view().original_identity();
        Some(self.telemetry.begin_v2_observation(
            original,
            builder.received_at(),
            builder.origin(),
            builder.authentication(),
            builder.deadline(),
            request_bytes,
        ))
    }

    fn begin_admitted(
        &self,
        builder: &RemotingRequestBuilder,
        request_bytes: u64,
        observation: Option<crate::telemetry::V2RequestObservation>,
    ) -> DispatchMetricsGuard {
        let observation = observation.unwrap_or_else(|| {
            self.telemetry.begin_v2_observation(
                builder.ingress_view().original_identity(),
                builder.received_at(),
                builder.origin(),
                builder.authentication(),
                builder.deadline(),
                request_bytes,
            )
        });
        DispatchMetricsGuard::ExplicitV2 { observation }
    }

    fn bind_response_observer(self, observation: Option<crate::telemetry::V2RequestObservation>) {
        if let Some(observation) = observation {
            let processor = self.processor;
            observation.bind_response_observer(move |observation| processor.observe_response(observation));
        }
    }

    fn reject_request(&self, request_code: i32) -> Result<Option<InternalProcessorCandidate>, DispatchProcessorError> {
        Ok(match self.processor.reject_request(request_code) {
            RejectRequestDecision::Proceed => None,
            RejectRequestDecision::Reject(plan) => Some(InternalProcessorCandidate::success(
                InternalProcessorOutcome::V2(HandlerOutcome::Reply(plan)),
            )),
        })
    }

    fn deadline_candidate(&self, plan: ResponsePlan) -> InternalProcessorCandidate {
        InternalProcessorCandidate::failure(
            InternalProcessorOutcome::V2(HandlerOutcome::Reply(plan)),
            InternalFailureOrigin::Deadline,
        )
    }

    fn install_deferred_response(
        &self,
        builder: RemotingRequestBuilder,
        response: &ResponseSink,
        session: &SessionHandle,
        ordering: RequestOrdering,
        class: crate::admission::AdmissionClass,
        resume_executor: crate::session_executor::DeferredResumeExecutor,
        _retained_bytes: usize,
        session_cleanup: Option<crate::dispatch::DeferredSessionCleanupRegistration>,
        observation: Option<crate::telemetry::V2RequestObservation>,
    ) -> Result<RemotingRequestBuilder, super::remoting_request::RemotingRequestBuildError> {
        let mut seed = response
            .network_deferred_seed_with_resume(session, ordering, class, resume_executor)
            .ok_or(super::remoting_request::RemotingRequestBuildError::DeferredResponseOwnerMismatch)?;
        if let Some(session_cleanup) = session_cleanup {
            seed = seed.with_session_cleanup(session_cleanup);
        }
        if let Some(observation) = observation {
            seed = seed.with_observation(observation);
        }
        Ok(builder.with_deferred_response_seed(seed))
    }

    #[allow(
        clippy::manual_async_fn,
        reason = "the sealed dispatcher boundary must state the Send guarantee required by SessionExecutor"
    )]
    fn process(
        &mut self,
        request: &mut RemotingRequest,
        hook_snapshot: Option<&HookSnapshot>,
        remote_address: SocketAddr,
        _session: &SessionHandle,
        _network_session: &Self::NetworkSession,
        _response: &ResponseSink,
    ) -> impl Future<Output = Result<InternalProcessorCandidate, DispatchProcessorError>> + Send {
        async move {
            let before_result = request.with_body_free_hook_command(|request_head| {
                run_before_rpc_hooks(hook_snapshot, remote_address, request_head)
            });
            let candidate = match before_result {
                Ok(()) => {
                    let candidate = self.process_embedded(request).await?;
                    apply_v2_after_hook(request, candidate, hook_snapshot, remote_address)?
                }
                Err(error) => InternalProcessorCandidate::failure(
                    InternalProcessorOutcome::V2(HandlerOutcome::Reply(
                        crate::error_response::response_plan_from_error(&error)?,
                    )),
                    InternalFailureOrigin::BeforeHook,
                ),
            };
            Ok(candidate)
        }
    }

    fn resolve_outcome(
        &self,
        request: &mut RemotingRequest,
        outcome: InternalProcessorOutcome,
    ) -> Result<InternalProcessorOutcome, DispatchProcessorError> {
        let InternalProcessorOutcome::V2(outcome) = outcome;
        Ok(InternalProcessorOutcome::V2(request.resolve_handler_outcome(outcome)?))
    }

    fn observe_response_write(
        &self,
        original: OriginalRequestIdentity,
        response_code: i32,
        body_kind: ResponseBodyKind,
        write_elapsed: Duration,
        end_to_end_elapsed: Duration,
        result: Result<ResponseReceipt, ResponseError>,
    ) {
        self.processor
            .observe_response_write(ResponseWriteObservationV2::from_result(
                original.request_id(),
                original.original_code(),
                response_code,
                body_kind,
                ResponseWritePath::Inline,
                write_elapsed,
                end_to_end_elapsed,
                result,
            ));
    }
}

fn apply_v2_after_hook(
    request: &mut RemotingRequest,
    candidate: InternalProcessorCandidate,
    hook_snapshot: Option<&HookSnapshot>,
    remote_address: SocketAddr,
) -> Result<InternalProcessorCandidate, DispatchProcessorError> {
    let InternalProcessorCandidate {
        outcome,
        failure,
        observe_write,
    } = candidate;
    let InternalProcessorOutcome::V2(HandlerOutcome::Reply(mut plan)) = outcome else {
        return Ok(InternalProcessorCandidate {
            outcome,
            failure,
            observe_write,
        });
    };
    let result = request.with_body_free_hook_request(|request_head| {
        plan.with_body_free_hook_head(|response_head| {
            run_after_rpc_hooks(hook_snapshot, remote_address, request_head, response_head)
        })
    });
    match result {
        Ok(()) => Ok(InternalProcessorCandidate {
            outcome: InternalProcessorOutcome::V2(HandlerOutcome::Reply(plan)),
            failure,
            observe_write,
        }),
        Err(error) => {
            drop(plan);
            let failure = failure
                .map(InternalFailureOrigin::after_hook_error)
                .unwrap_or(InternalFailureOrigin::AfterHook);
            Ok(InternalProcessorCandidate::failure(
                InternalProcessorOutcome::V2(HandlerOutcome::Reply(crate::error_response::response_plan_from_error(
                    &error,
                )?)),
                failure,
            ))
        }
    }
}
