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

//! Sealed processor boundary behind the private dispatch core.

use std::future::Future;
use std::net::SocketAddr;

use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

use super::remoting_request::RemotingRequestBuilder;
use super::HandlerOutcome;
use super::RemotingRequest;
use super::RemotingResponse;
use super::ResponseSink;
use crate::base::pending_request_table::PendingRequestOwner;
use crate::base::pending_request_table::PendingRequestTable;
use crate::contract::TransportContractViolation;
use crate::hook_registry::HookSnapshot;
use crate::remoting::inner::run_after_rpc_hooks;
use crate::remoting::inner::run_before_rpc_hooks;
use crate::request_ordering::RequestOrdering;
use crate::runtime::processor::RejectRequestDecision;
use crate::runtime::processor::RequestProcessor;
use crate::runtime::processor::ResponseObservationOutcome;
use crate::server::SessionHandle;
use crate::telemetry::TransportTelemetry;

mod sealed {
    pub trait Sealed {}
}

/// Stable response-correlation owner allocated once for one canonical
/// network session.
#[derive(Clone)]
pub(crate) struct NetworkSession {
    response_table: PendingRequestTable,
    owner: PendingRequestOwner,
}

impl NetworkSession {
    pub(crate) fn response_table(&self) -> &PendingRequestTable {
        &self.response_table
    }

    pub(crate) fn owner(&self) -> &PendingRequestOwner {
        &self.owner
    }
}

/// Explicit wrapper around the canonical processor implementation.
pub(crate) struct ExplicitProcessor<P> {
    processor: P,
    response_table: PendingRequestTable,
    telemetry: TransportTelemetry,
}

impl<P> ExplicitProcessor<P> {
    #[cfg(test)]
    pub(crate) fn new(processor: P) -> Self {
        Self::with_response_table(processor, PendingRequestTable::new())
    }

    #[cfg(test)]
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

#[path = "processor_adapter/embedded_processor.rs"]
mod embedded_processor;
pub(crate) use embedded_processor::EmbeddedResolvedOutcome;

impl<P> Clone for ExplicitProcessor<P>
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
    Handled(HandlerOutcome),
}

pub(crate) struct InternalProcessorCandidate {
    pub(crate) outcome: InternalProcessorOutcome,
    pub(crate) failure: Option<InternalFailureOrigin>,
}

impl InternalProcessorCandidate {
    pub(crate) fn success(outcome: InternalProcessorOutcome) -> Self {
        Self { outcome, failure: None }
    }

    pub(crate) fn failure(outcome: InternalProcessorOutcome, failure: InternalFailureOrigin) -> Self {
        Self {
            outcome,
            failure: Some(failure),
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
    Contract(#[from] TransportContractViolation),
}

pub(crate) enum DispatchMetricsGuard {
    Explicit {
        observation: crate::telemetry::RequestObservation,
    },
}

impl DispatchMetricsGuard {
    pub(crate) fn complete_response(&mut self, response_code: i32) {
        let _ = response_code;
    }

    pub(crate) fn complete_oneway(&mut self) {
        let Self::Explicit { observation } = self;
        observation.complete_no_response(ResponseObservationOutcome::Oneway);
    }

    pub(crate) fn complete_protocol_no_response(&mut self) {
        let Self::Explicit { observation } = self;
        observation.complete_no_response(ResponseObservationOutcome::ProtocolNoResponse);
    }

    pub(crate) fn arm_deferred_metrics(&mut self, retained_bytes: usize) {
        let Self::Explicit { observation } = self;
        observation.arm_deferred_metrics(retained_bytes);
    }

    pub(crate) fn record_deferred_registered(&mut self) {
        let Self::Explicit { observation } = self;
        observation.record_deferred_registered();
    }

    pub(crate) fn observation(&self) -> &crate::telemetry::RequestObservation {
        let Self::Explicit { observation } = self;
        observation
    }

    pub(crate) fn span(&self) -> tracing::Span {
        let Self::Explicit { observation } = self;
        observation.span()
    }

    pub(crate) fn complete_process_request_failed(&mut self, response_code: i32) {
        let Self::Explicit { observation } = self;
        observation.complete_request_failed(response_code);
    }

    pub(crate) fn complete_write_channel_failed(&mut self, response_code: i32) {
        let Self::Explicit { observation } = self;
        observation.complete_write_failed(response_code);
    }
}

/// Sealed statically dispatched processor boundary used by the core.
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
    ) -> Option<crate::telemetry::RequestObservation>;

    fn begin_admitted(
        &self,
        builder: &RemotingRequestBuilder,
        request_bytes: u64,
        observation: Option<crate::telemetry::RequestObservation>,
    ) -> DispatchMetricsGuard;

    fn bind_response_observer(self, observation: Option<crate::telemetry::RequestObservation>);

    fn reject_request(&self, request_code: i32) -> Result<Option<InternalProcessorCandidate>, DispatchProcessorError>;

    fn deadline_candidate(&self, response: RemotingResponse) -> InternalProcessorCandidate;

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
        observation: Option<crate::telemetry::RequestObservation>,
    ) -> Result<RemotingRequestBuilder, TransportContractViolation>;

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
}

pub(crate) struct AdmittedProcessorObserver<D>
where
    D: DispatchProcessor,
{
    processor: Option<D>,
    observation: Option<crate::telemetry::RequestObservation>,
}

impl<D> AdmittedProcessorObserver<D>
where
    D: DispatchProcessor,
{
    pub(crate) fn new(processor: D, observation: Option<crate::telemetry::RequestObservation>) -> Self {
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

impl<P> sealed::Sealed for ExplicitProcessor<P> {}

impl<P> DispatchProcessor for ExplicitProcessor<P>
where
    P: RequestProcessor + Clone + Sync + 'static,
{
    type NetworkSession = NetworkSession;

    fn open_network_session(&self) -> Self::NetworkSession {
        NetworkSession {
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
                "unmatched response frame dropped on transport session"
            );
        }
    }

    fn close_network_session(&self, session: &Self::NetworkSession) {
        session.response_table.close_owner(&session.owner, || {
            crate::error_helpers::network(crate::error_helpers::connection_failed_without_source(
                crate::error_helpers::TransportStage::Closed,
            ))
        });
    }

    fn request_ordering(&self, builder: &RemotingRequestBuilder) -> RequestOrdering {
        self.processor.request_ordering(builder.ingress_view())
    }

    fn begin_boundary_observation(
        &self,
        builder: &RemotingRequestBuilder,
        request_bytes: u64,
    ) -> Option<crate::telemetry::RequestObservation> {
        let original = builder.ingress_view().original_identity();
        Some(self.telemetry.begin_observation(
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
        observation: Option<crate::telemetry::RequestObservation>,
    ) -> DispatchMetricsGuard {
        let observation = observation.unwrap_or_else(|| {
            self.telemetry.begin_observation(
                builder.ingress_view().original_identity(),
                builder.received_at(),
                builder.origin(),
                builder.authentication(),
                builder.deadline(),
                request_bytes,
            )
        });
        DispatchMetricsGuard::Explicit { observation }
    }

    fn bind_response_observer(self, observation: Option<crate::telemetry::RequestObservation>) {
        if let Some(observation) = observation {
            let processor = self.processor;
            observation.bind_response_observer(move |observation| processor.observe_response(observation));
        }
    }

    fn reject_request(&self, request_code: i32) -> Result<Option<InternalProcessorCandidate>, DispatchProcessorError> {
        Ok(match self.processor.reject_request(request_code) {
            RejectRequestDecision::Proceed => None,
            RejectRequestDecision::Reject(response) => Some(InternalProcessorCandidate::success(
                InternalProcessorOutcome::Handled(HandlerOutcome::Reply(response)),
            )),
        })
    }

    fn deadline_candidate(&self, response: RemotingResponse) -> InternalProcessorCandidate {
        InternalProcessorCandidate::failure(
            InternalProcessorOutcome::Handled(HandlerOutcome::Reply(response)),
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
        observation: Option<crate::telemetry::RequestObservation>,
    ) -> Result<RemotingRequestBuilder, TransportContractViolation> {
        let mut seed = response
            .network_deferred_seed_with_resume(session, ordering, class, resume_executor)
            .ok_or(TransportContractViolation::DeferredResponseOwnerMismatch)?;
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
                    apply_after_hook(request, candidate, hook_snapshot, remote_address)?
                }
                Err(error) => InternalProcessorCandidate::failure(
                    InternalProcessorOutcome::Handled(HandlerOutcome::Reply(
                        crate::error_response::remoting_response_from_error(&error)?,
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
        let InternalProcessorOutcome::Handled(outcome) = outcome;
        Ok(InternalProcessorOutcome::Handled(
            request.resolve_handler_outcome(outcome)?,
        ))
    }
}

fn apply_after_hook(
    request: &mut RemotingRequest,
    candidate: InternalProcessorCandidate,
    hook_snapshot: Option<&HookSnapshot>,
    remote_address: SocketAddr,
) -> Result<InternalProcessorCandidate, DispatchProcessorError> {
    let InternalProcessorCandidate { outcome, failure } = candidate;
    let InternalProcessorOutcome::Handled(HandlerOutcome::Reply(mut response)) = outcome else {
        return Ok(InternalProcessorCandidate { outcome, failure });
    };
    let result = request.with_body_free_hook_request(|request_head| {
        response.with_body_free_hook_head(|response_head| {
            run_after_rpc_hooks(hook_snapshot, remote_address, request_head, response_head)
        })
    });
    match result {
        Ok(()) => Ok(InternalProcessorCandidate {
            outcome: InternalProcessorOutcome::Handled(HandlerOutcome::Reply(response)),
            failure,
        }),
        Err(error) => {
            drop(response);
            let failure = failure
                .map(InternalFailureOrigin::after_hook_error)
                .unwrap_or(InternalFailureOrigin::AfterHook);
            Ok(InternalProcessorCandidate::failure(
                InternalProcessorOutcome::Handled(HandlerOutcome::Reply(
                    crate::error_response::remoting_response_from_error(&error)?,
                )),
                failure,
            ))
        }
    }
}
