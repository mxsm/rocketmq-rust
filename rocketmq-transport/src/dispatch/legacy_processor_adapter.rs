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

//! Sealed compatibility processing behind the private V2 dispatch core.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
use std::time::Duration;

use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::TaskGroup;

use super::remoting_request::RemotingRequestBuilder;
use super::HandlerOutcome;
use super::HandlerOutcomeContractError;
use super::OriginalRequestIdentity;
use super::RemotingRequest;
use super::RequestOrigin;
use super::ResponseBodyKind;
use super::ResponseError;
use super::ResponsePlan;
use super::ResponsePlanError;
use super::ResponseReceipt;
use super::ResponseSink;
use crate::base::pending_request_table::PendingRequestOwner;
use crate::base::pending_request_table::PendingRequestTable;
use crate::hook_registry::HookSnapshot;
use crate::net::channel::Channel;
use crate::net::channel::ChannelInner;
use crate::remoting::inner::is_long_polling_request;
use crate::remoting::inner::legacy_processor_error_response;
use crate::remoting::inner::legacy_rejection_response;
use crate::remoting::inner::run_after_rpc_hooks;
use crate::remoting::inner::run_before_rpc_hooks;
use crate::request_ordering::RequestOrdering;
use crate::runtime::connection_handler_context::ConnectionHandlerContext;
use crate::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
use crate::runtime::processor::RequestProcessor;
use crate::runtime::processor::ResponseWriteObservation;
use crate::runtime::processor::ResponseWriteOutcome;
use crate::runtime::processor_v2::RejectRequestDecision;
use crate::runtime::processor_v2::RequestProcessorV2;
use crate::runtime::processor_v2::ResponseWriteObservationV2;
use crate::runtime::processor_v2::ResponseWritePath;
use crate::server::SessionHandle;
use crate::session_view::EmbeddedSessionRecord;
use crate::session_view::SessionId;
use crate::session_view::SessionView;
use crate::telemetry::TransportRequestMetricsGuard;
use crate::telemetry::TransportTelemetry;

mod sealed {
    pub trait Sealed {}
}

#[cfg(test)]
static BRIDGE_CONSTRUCTIONS: Mutex<Vec<SessionId>> = Mutex::new(Vec::new());

#[cfg(test)]
static BRIDGE_CHANNEL_INNER_CONSTRUCTIONS: Mutex<Vec<SessionId>> = Mutex::new(Vec::new());

#[cfg(test)]
pub(crate) fn bridge_construction_counts(session_id: SessionId) -> (usize, usize) {
    let bridges = BRIDGE_CONSTRUCTIONS
        .lock()
        .expect("legacy bridge construction counter lock")
        .iter()
        .filter(|candidate| **candidate == session_id)
        .count();
    let channel_inners = BRIDGE_CHANNEL_INNER_CONSTRUCTIONS
        .lock()
        .expect("legacy bridge ChannelInner construction counter lock")
        .iter()
        .filter(|candidate| **candidate == session_id)
        .count();
    (bridges, channel_inners)
}

/// Separate legacy transport capability aggregate. It is never stored in a
/// `RemotingRequest` and cannot be constructed from an arbitrary channel and
/// context pair.
#[allow(
    dead_code,
    reason = "DSP-05 defines the private bridge consumed by DSP-06 coexistence routing"
)]
pub(crate) struct LegacyRequestBridge {
    channel: Channel,
    context: ConnectionHandlerContext,
    canonical_session_id: SessionId,
}

#[allow(
    dead_code,
    reason = "DSP-05 defines bridge construction and validation before DSP-06 routing"
)]
impl LegacyRequestBridge {
    fn from_network_session(
        session: &SessionHandle,
        response: &ResponseSink,
        endpoint: &LegacyNetworkSession,
    ) -> Result<Self, LegacyProcessorAdapterError> {
        let canonical_session_id = SessionId::from_session_owner(session.session_id());
        #[cfg(test)]
        BRIDGE_CONSTRUCTIONS
            .lock()
            .expect("legacy bridge construction counter lock")
            .push(canonical_session_id);
        if !response.is_network_transport() {
            return Err(LegacyProcessorAdapterError::TransportKindMismatch);
        }
        if !response.is_network_owner(session) {
            return Err(LegacyProcessorAdapterError::CompletionOwnerMismatch);
        }
        if !response.is_canonical_network_plan_owner(session) {
            return Err(LegacyProcessorAdapterError::CompletionOwnerMismatch);
        }
        #[cfg(test)]
        BRIDGE_CHANNEL_INNER_CONSTRUCTIONS
            .lock()
            .expect("legacy bridge ChannelInner construction counter lock")
            .push(canonical_session_id);
        let channel = session
            .legacy_processor_channel(
                response.clone(),
                endpoint.response_table.clone(),
                endpoint.owner.clone(),
            )
            .map_err(LegacyProcessorAdapterError::BridgeConstruction)?;
        let context = Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let bridge = Self {
            channel,
            context,
            canonical_session_id,
        };
        bridge.validate_network(session, response)?;
        if !bridge.channel.has_pending_request_owner(&endpoint.owner) {
            return Err(LegacyProcessorAdapterError::CompletionOwnerMismatch);
        }
        Ok(bridge)
    }

    fn from_embedded_session(
        session: &EmbeddedSessionRecord,
        response: &ResponseSink,
        task_group: &TaskGroup,
        response_table: PendingRequestTable,
    ) -> Result<Self, LegacyProcessorAdapterError> {
        let session_view = session.view();
        let canonical_session_id = session_view.id();
        #[cfg(test)]
        BRIDGE_CONSTRUCTIONS
            .lock()
            .expect("legacy bridge construction counter lock")
            .push(canonical_session_id);
        if !response.is_local() {
            return Err(LegacyProcessorAdapterError::TransportKindMismatch);
        }
        if !response.is_local_plan_owner(session_view.state(), task_group) {
            return Err(LegacyProcessorAdapterError::CompletionOwnerMismatch);
        }
        #[cfg(test)]
        BRIDGE_CHANNEL_INNER_CONSTRUCTIONS
            .lock()
            .expect("legacy bridge ChannelInner construction counter lock")
            .push(canonical_session_id);
        let inner = Arc::new(ChannelInner::new_local_legacy_bridge(
            response.clone(),
            task_group.clone(),
            response_table,
        ));
        let channel =
            Channel::new_canonical_embedded(inner, canonical_session_id, session_view.state().clone(), task_group);
        let context = Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let bridge = Self {
            channel,
            context,
            canonical_session_id,
        };
        bridge.validate_embedded(session, response, task_group)?;
        Ok(bridge)
    }

    fn validate_network(
        &self,
        session: &SessionHandle,
        response: &ResponseSink,
    ) -> Result<(), LegacyProcessorAdapterError> {
        if self.canonical_session_id != SessionId::from_session_owner(session.session_id())
            || self.channel.canonical_session_id() != Some(self.canonical_session_id)
            || self.context.channel().canonical_session_id() != Some(self.canonical_session_id)
        {
            return Err(LegacyProcessorAdapterError::SessionMismatch);
        }
        if !self.channel.is_network_transport() || !response.is_network_transport() {
            return Err(LegacyProcessorAdapterError::TransportKindMismatch);
        }
        if !self.channel.is_canonical_network_owner(session) {
            return Err(LegacyProcessorAdapterError::WriterOwnerMismatch);
        }
        if !response.is_network_owner(session) {
            return Err(LegacyProcessorAdapterError::CompletionOwnerMismatch);
        }
        if !self.channel.shares_inner(self.context.channel()) {
            return Err(LegacyProcessorAdapterError::ChannelContextMismatch);
        }
        debug_assert!(self.channel.shares_inner(self.context.channel()));
        debug_assert_eq!(self.channel.canonical_session_id(), Some(self.canonical_session_id));
        debug_assert_eq!(
            self.context.channel().canonical_session_id(),
            Some(self.canonical_session_id)
        );
        debug_assert!(self.channel.is_network_transport());
        debug_assert!(response.is_network_owner(session));
        Ok(())
    }

    fn validate_embedded(
        &self,
        session: &EmbeddedSessionRecord,
        response: &ResponseSink,
        task_group: &TaskGroup,
    ) -> Result<(), LegacyProcessorAdapterError> {
        let session_view = session.view();
        if self.canonical_session_id != session_view.id()
            || self.channel.canonical_session_id() != Some(self.canonical_session_id)
            || self.context.channel().canonical_session_id() != Some(self.canonical_session_id)
        {
            return Err(LegacyProcessorAdapterError::SessionMismatch);
        }
        if self.channel.is_network_transport() || !response.is_local() {
            return Err(LegacyProcessorAdapterError::TransportKindMismatch);
        }
        if !response.is_local_plan_owner(session_view.state(), task_group) {
            return Err(LegacyProcessorAdapterError::CompletionOwnerMismatch);
        }
        if !self.channel.is_canonical_embedded_owner(
            self.canonical_session_id,
            session_view.state(),
            response,
            task_group,
        ) {
            return Err(LegacyProcessorAdapterError::CompletionOwnerMismatch);
        }
        if !self.channel.shares_inner(self.context.channel()) {
            return Err(LegacyProcessorAdapterError::ChannelContextMismatch);
        }
        debug_assert!(self.channel.shares_inner(self.context.channel()));
        debug_assert_eq!(self.channel.canonical_session_id(), Some(self.canonical_session_id));
        debug_assert_eq!(
            self.context.channel().canonical_session_id(),
            Some(self.canonical_session_id)
        );
        debug_assert!(!self.channel.is_network_transport());
        debug_assert!(self.channel.is_canonical_embedded_owner(
            self.canonical_session_id,
            session_view.state(),
            response,
            task_group
        ));
        Ok(())
    }

    fn validate_request(&self, request: &RemotingRequest) -> Result<(), LegacyProcessorAdapterError> {
        if request.session().id() != self.canonical_session_id {
            return Err(LegacyProcessorAdapterError::SessionMismatch);
        }
        match (request.origin(), request.session(), self.channel.is_network_transport()) {
            (RequestOrigin::Network { .. }, SessionView::Network { .. }, true)
            | (RequestOrigin::Embedded { .. }, SessionView::Embedded { .. }, false) => Ok(()),
            _ => Err(LegacyProcessorAdapterError::TransportKindMismatch),
        }
    }
}

/// Stable response-correlation owner allocated once for one canonical V1
/// network session and reused by every admitted legacy bridge in that session.
#[derive(Clone)]
pub(crate) struct LegacyNetworkSession {
    response_table: PendingRequestTable,
    owner: PendingRequestOwner,
}

impl LegacyNetworkSession {
    #[cfg(test)]
    pub(crate) fn for_test(response_table: PendingRequestTable) -> Self {
        let owner = response_table.new_owner();
        Self { response_table, owner }
    }

    pub(crate) fn response_table(&self) -> &PendingRequestTable {
        &self.response_table
    }

    pub(crate) fn owner(&self) -> &PendingRequestOwner {
        &self.owner
    }
}

/// Existing V1 processor plus the stable endpoint capabilities needed by each
/// admitted bridge invocation.
#[allow(
    dead_code,
    reason = "DSP-05 defines the private adapter consumed by DSP-06 coexistence routing"
)]
pub(crate) struct LegacyProcessorAdapter<P> {
    processor: P,
    processor_name: &'static str,
    telemetry: TransportTelemetry,
    response_table: PendingRequestTable,
}

#[allow(
    dead_code,
    reason = "DSP-05 defines private adapter construction before DSP-06 routing"
)]
impl<P> LegacyProcessorAdapter<P> {
    pub(crate) fn new(
        processor: P,
        processor_name: &'static str,
        telemetry: TransportTelemetry,
        response_table: PendingRequestTable,
    ) -> Self {
        Self {
            processor,
            processor_name,
            telemetry,
            response_table,
        }
    }

    pub(crate) fn open_network_session(&self) -> LegacyNetworkSession {
        LegacyNetworkSession {
            response_table: self.response_table.clone(),
            owner: self.response_table.new_owner(),
        }
    }
}

impl<P> Clone for LegacyProcessorAdapter<P>
where
    P: Clone,
{
    fn clone(&self) -> Self {
        Self {
            processor: self.processor.clone(),
            processor_name: self.processor_name,
            telemetry: self.telemetry.clone(),
            response_table: self.response_table.clone(),
        }
    }
}

/// Explicit wrapper preventing the V2 implementation from overlapping the
/// legacy compatibility implementation.
pub(crate) struct ExplicitV2Processor<P> {
    processor: P,
}

impl<P> ExplicitV2Processor<P> {
    pub(crate) const fn new(processor: P) -> Self {
        Self { processor }
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
        }
    }
}

/// Internal terminal result before immutable binding and delivery.
#[allow(
    dead_code,
    reason = "legacy variants remain dormant until DSP-06 routes V1 processors through this seam"
)]
pub(crate) enum InternalProcessorOutcome {
    V2(HandlerOutcome),
    LegacyReply(LegacyReplyCandidate),
    LegacyAmbiguousNone,
}

pub(crate) enum LegacyReplyCandidate {
    OwnedCommand(RemotingCommand),
    ValidatedPlan(ResponsePlan),
}

impl LegacyReplyCandidate {
    pub(crate) fn response_code(&self) -> i32 {
        match self {
            Self::OwnedCommand(command) => command.code(),
            Self::ValidatedPlan(plan) => plan.response_code(),
        }
    }

    pub(crate) fn into_plan(self) -> Result<ResponsePlan, DispatchProcessorError> {
        match self {
            Self::OwnedCommand(command) => legacy_plan(command),
            Self::ValidatedPlan(plan) => Ok(plan),
        }
    }
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

    #[allow(
        dead_code,
        reason = "legacy observation policy remains dormant until DSP-06 coexistence routing"
    )]
    fn suppress_observation(mut self) -> Self {
        self.observe_write = false;
        self
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
#[allow(
    dead_code,
    reason = "legacy bridge failures remain dormant until DSP-06 coexistence routing"
)]
pub(crate) enum LegacyProcessorAdapterError {
    #[error("legacy bridge session provenance mismatch")]
    SessionMismatch,
    #[error("legacy bridge transport kind mismatch")]
    TransportKindMismatch,
    #[error("legacy bridge writer owner mismatch")]
    WriterOwnerMismatch,
    #[error("legacy bridge completion owner mismatch")]
    CompletionOwnerMismatch,
    #[error("legacy bridge channel and context do not share one inner channel")]
    ChannelContextMismatch,
    #[error("legacy bridge construction failed")]
    BridgeConstruction(#[source] rocketmq_error::RocketMQError),
    #[error("legacy processor returned a malformed response command")]
    MalformedLegacyResponse(#[source] ResponsePlanError),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DispatchProcessorError {
    #[error(transparent)]
    Legacy(#[from] LegacyProcessorAdapterError),
    #[error(transparent)]
    ResponsePlan(#[from] ResponsePlanError),
    #[error(transparent)]
    HandlerContract(#[from] HandlerOutcomeContractError),
}

#[allow(dead_code, reason = "legacy metrics remain dormant until DSP-06 coexistence routing")]
pub(crate) enum DispatchMetricsGuard {
    ExplicitV2,
    Legacy(TransportRequestMetricsGuard),
}

impl DispatchMetricsGuard {
    pub(crate) fn complete_response(&mut self, response_code: i32) {
        if let Self::Legacy(guard) = self {
            guard.complete_response(response_code);
        }
    }

    pub(crate) fn complete_oneway(&mut self) {
        if let Self::Legacy(guard) = self {
            guard.complete_oneway();
        }
    }

    pub(crate) fn complete_legacy_ambiguous_none(&mut self) {
        if let Self::Legacy(guard) = self {
            guard.complete_legacy_ambiguous_none();
        }
    }

    pub(crate) fn complete_process_request_failed(&mut self, response_code: i32) {
        if let Self::Legacy(guard) = self {
            let _ = response_code;
            guard.complete_process_request_failed(
                rocketmq_protocol::code::response_code::ResponseCode::SystemError.to_i32(),
            );
        }
    }

    pub(crate) fn complete_write_channel_failed(&mut self, response_code: i32) {
        if let Self::Legacy(guard) = self {
            guard.complete_write_channel_failed(response_code);
        }
    }
}

/// Sealed statically dispatched processor boundary used by the V2 core.
pub(crate) trait DispatchProcessor: sealed::Sealed + Clone + Send + Sync + 'static {
    type NetworkSession: Clone + Send + Sync + 'static;

    fn open_network_session(&self) -> Self::NetworkSession;

    fn complete_network_response(&self, session: &Self::NetworkSession, response: RemotingCommand);

    fn close_network_session(&self, session: &Self::NetworkSession);

    fn request_ordering(&self, builder: &RemotingRequestBuilder) -> RequestOrdering;

    fn begin_admitted(&self, original: OriginalRequestIdentity, request_bytes: u64) -> DispatchMetricsGuard;

    fn reject_request(&self, request_code: i32) -> Result<Option<InternalProcessorCandidate>, DispatchProcessorError>;

    fn deadline_candidate(&self, plan: ResponsePlan) -> InternalProcessorCandidate;

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

impl<P> sealed::Sealed for ExplicitV2Processor<P> {}

impl<P> DispatchProcessor for ExplicitV2Processor<P>
where
    P: RequestProcessorV2 + Clone + Sync + 'static,
{
    type NetworkSession = ();

    fn open_network_session(&self) -> Self::NetworkSession {}

    fn complete_network_response(&self, _session: &Self::NetworkSession, _response: RemotingCommand) {
        tracing::warn!(
            frame = "unexpected_response",
            generation = "v2",
            "unexpected response frame dropped on V2-only transport session"
        );
    }

    fn close_network_session(&self, _session: &Self::NetworkSession) {}

    fn request_ordering(&self, builder: &RemotingRequestBuilder) -> RequestOrdering {
        self.processor.request_ordering(builder.ingress_view())
    }

    fn begin_admitted(&self, _original: OriginalRequestIdentity, _request_bytes: u64) -> DispatchMetricsGuard {
        DispatchMetricsGuard::ExplicitV2
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
        let InternalProcessorOutcome::V2(outcome) = outcome else {
            return Err(LegacyProcessorAdapterError::TransportKindMismatch.into());
        };
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

impl<P> sealed::Sealed for LegacyProcessorAdapter<P> {}

impl<P> DispatchProcessor for LegacyProcessorAdapter<P>
where
    P: RequestProcessor + Clone + Sync + 'static,
{
    type NetworkSession = LegacyNetworkSession;

    fn open_network_session(&self) -> Self::NetworkSession {
        self.open_network_session()
    }

    fn complete_network_response(&self, session: &Self::NetworkSession, response: RemotingCommand) {
        let opaque = response.opaque();
        if !session
            .response_table
            .complete_response_for_owner(&session.owner, opaque, response)
        {
            tracing::warn!(
                frame = "unmatched_response",
                generation = "v1",
                "response frame did not match pending work for its canonical session owner"
            );
        }
    }

    fn close_network_session(&self, session: &Self::NetworkSession) {
        session.response_table.close_owner(&session.owner, || {
            rocketmq_error::RocketMQError::network_connection_failed(
                "legacy_session_pending_requests",
                "canonical V1 network session closed",
            )
        });
    }

    fn request_ordering(&self, builder: &RemotingRequestBuilder) -> RequestOrdering {
        self.processor.request_ordering(builder.command())
    }

    fn begin_admitted(&self, original: OriginalRequestIdentity, request_bytes: u64) -> DispatchMetricsGuard {
        self.telemetry
            .record_legacy_processor_request(self.processor_name, original.original_code());
        DispatchMetricsGuard::Legacy(self.telemetry.request_guard(
            original.original_code(),
            request_bytes,
            is_long_polling_request(original.original_code()),
        ))
    }

    fn reject_request(&self, request_code: i32) -> Result<Option<InternalProcessorCandidate>, DispatchProcessorError> {
        Ok(
            legacy_rejection_response(self.processor.reject_request(request_code)).map(|command| {
                InternalProcessorCandidate::success(InternalProcessorOutcome::LegacyReply(
                    LegacyReplyCandidate::OwnedCommand(command),
                ))
            }),
        )
    }

    fn deadline_candidate(&self, plan: ResponsePlan) -> InternalProcessorCandidate {
        InternalProcessorCandidate::failure(
            InternalProcessorOutcome::LegacyReply(LegacyReplyCandidate::ValidatedPlan(plan)),
            InternalFailureOrigin::Deadline,
        )
        .suppress_observation()
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
        session: &SessionHandle,
        network_session: &Self::NetworkSession,
        response: &ResponseSink,
    ) -> impl Future<Output = Result<InternalProcessorCandidate, DispatchProcessorError>> + Send {
        async move {
            let bridge = LegacyRequestBridge::from_network_session(session, response, network_session)?;
            bridge.validate_request(request)?;

            if let Err(error) = run_before_rpc_hooks(hook_snapshot, remote_address, request.legacy_command_mut()) {
                return Ok(InternalProcessorCandidate::failure(
                    InternalProcessorOutcome::LegacyReply(LegacyReplyCandidate::OwnedCommand(
                        crate::error_response::command_from_error(&error),
                    )),
                    InternalFailureOrigin::BeforeHook,
                )
                .suppress_observation());
            }

            let deadline = request.meta().deadline();
            let processed = {
                let channel = bridge.channel.clone();
                let context = bridge.context.clone();
                let process = self
                    .processor
                    .process_request(channel, context, request.legacy_command_mut());
                match deadline {
                    Some(deadline) => deadline.timeout(process).await,
                    None => Ok(process.await),
                }
            };
            let (mut response, failure) = match processed {
                Ok(Ok(response)) => (response, None),
                Ok(Err(_)) => (
                    Some(legacy_processor_error_response()),
                    Some(InternalFailureOrigin::ProcessorError),
                ),
                Err(_) => {
                    return Ok(InternalProcessorCandidate::failure(
                        InternalProcessorOutcome::LegacyReply(LegacyReplyCandidate::OwnedCommand(
                            super::authorized_dispatcher::deadline_response(
                                request.original_identity().original_opaque(),
                            ),
                        )),
                        InternalFailureOrigin::Deadline,
                    )
                    .suppress_observation());
                }
            };

            let Some(mut response) = response.take() else {
                return Ok(InternalProcessorCandidate::success(
                    InternalProcessorOutcome::LegacyAmbiguousNone,
                ));
            };
            if let Err(error) = run_after_rpc_hooks(hook_snapshot, remote_address, request.command(), &mut response) {
                let failure = failure
                    .map(InternalFailureOrigin::after_hook_error)
                    .unwrap_or(InternalFailureOrigin::AfterHook);
                return Ok(InternalProcessorCandidate::failure(
                    InternalProcessorOutcome::LegacyReply(LegacyReplyCandidate::OwnedCommand(
                        crate::error_response::command_from_error(&error),
                    )),
                    failure,
                )
                .suppress_observation());
            }
            let outcome = InternalProcessorOutcome::LegacyReply(LegacyReplyCandidate::OwnedCommand(response));
            Ok(match failure {
                Some(failure) => InternalProcessorCandidate::failure(outcome, failure),
                None => InternalProcessorCandidate::success(outcome),
            })
        }
    }

    fn resolve_outcome(
        &self,
        request: &mut RemotingRequest,
        outcome: InternalProcessorOutcome,
    ) -> Result<InternalProcessorOutcome, DispatchProcessorError> {
        match outcome {
            InternalProcessorOutcome::LegacyReply(reply) => {
                if request.original_identity().is_one_way() {
                    request.resolve_legacy_oneway_reply()?;
                    return Ok(InternalProcessorOutcome::LegacyReply(reply));
                }
                let plan = reply.into_plan()?;
                let resolved = request.resolve_handler_outcome(HandlerOutcome::Reply(plan))?;
                let HandlerOutcome::Reply(plan) = resolved else {
                    return Err(LegacyProcessorAdapterError::TransportKindMismatch.into());
                };
                Ok(InternalProcessorOutcome::LegacyReply(
                    LegacyReplyCandidate::ValidatedPlan(plan),
                ))
            }
            InternalProcessorOutcome::LegacyAmbiguousNone => Ok(InternalProcessorOutcome::LegacyAmbiguousNone),
            InternalProcessorOutcome::V2(_) => Err(LegacyProcessorAdapterError::TransportKindMismatch.into()),
        }
    }

    fn observe_response_write(
        &self,
        original: OriginalRequestIdentity,
        response_code: i32,
        _body_kind: ResponseBodyKind,
        write_elapsed: Duration,
        end_to_end_elapsed: Duration,
        result: Result<ResponseReceipt, ResponseError>,
    ) {
        self.processor.observe_response_write(ResponseWriteObservation {
            request_code: original.original_code(),
            response_code,
            write_elapsed,
            end_to_end_elapsed,
            outcome: if result.is_ok() {
                ResponseWriteOutcome::Sent
            } else {
                ResponseWriteOutcome::Failed
            },
        });
    }
}

#[allow(
    dead_code,
    reason = "legacy response conversion remains dormant until DSP-06 coexistence routing"
)]
fn legacy_plan(command: RemotingCommand) -> Result<ResponsePlan, DispatchProcessorError> {
    ResponsePlan::from_legacy_command(command)
        .map_err(|error| LegacyProcessorAdapterError::MalformedLegacyResponse(error).into())
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

#[cfg(test)]
#[path = "legacy_processor_adapter/tests.rs"]
mod tests;
