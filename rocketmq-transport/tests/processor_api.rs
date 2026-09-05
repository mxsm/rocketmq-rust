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
use std::rc::Rc;
use std::time::Duration;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_security_api::Principal;
use rocketmq_transport::api::AuthenticationState;
use rocketmq_transport::api::AuthorizedCommandDispatcher;
use rocketmq_transport::api::ClaimedDeferred;
use rocketmq_transport::api::DefaultRequestProcessor;
use rocketmq_transport::api::DeferredCancellationReason;
use rocketmq_transport::api::DeferredClaimError;
use rocketmq_transport::api::DeferredClaimErrorKind;
use rocketmq_transport::api::DeferredExpiry;
use rocketmq_transport::api::DeferredExpiryBatch;
use rocketmq_transport::api::DeferredExpiryBatchStats;
use rocketmq_transport::api::DeferredExpiryError;
use rocketmq_transport::api::DeferredExpiryErrorKind;
use rocketmq_transport::api::DeferredExpiryKind;
use rocketmq_transport::api::DeferredExpiryMargins;
use rocketmq_transport::api::DeferredId;
use rocketmq_transport::api::DeferredParts;
use rocketmq_transport::api::DeferredRegistration;
use rocketmq_transport::api::DeferredRegistry;
use rocketmq_transport::api::DeferredRegistryError;
use rocketmq_transport::api::DeferredRegistryErrorKind;
use rocketmq_transport::api::DeferredRegistryShutdownOutcome;
use rocketmq_transport::api::DeferredRegistryShutdownStats;
use rocketmq_transport::api::DeferredRequest;
use rocketmq_transport::api::DeferredResponder;
use rocketmq_transport::api::DeferredResumeError;
use rocketmq_transport::api::DeferredResumeErrorKind;
use rocketmq_transport::api::DeferredResumeRetainedSize;
use rocketmq_transport::api::DeferredRetainedSize;
use rocketmq_transport::api::DeferredRetainedSizeParts;
use rocketmq_transport::api::DeferredTerminalReason;
use rocketmq_transport::api::DeferredWaitPermit;
use rocketmq_transport::api::DeferredWakeReason;
use rocketmq_transport::api::EmbeddedCaller;
use rocketmq_transport::api::EmbeddedDispatchError;
use rocketmq_transport::api::EmbeddedDispatchErrorKind;
use rocketmq_transport::api::EmbeddedDispatchOutcome;
use rocketmq_transport::api::EmbeddedResponse;
use rocketmq_transport::api::EmbeddedResponseBody;
use rocketmq_transport::api::FileRegion;
use rocketmq_transport::api::FileRegionSequence;
use rocketmq_transport::api::HandlerOutcome;
use rocketmq_transport::api::IngressRequestView;
use rocketmq_transport::api::LocalRequestProcessor;
use rocketmq_transport::api::OriginalRequestIdentity;
use rocketmq_transport::api::ProtocolNoResponse;
use rocketmq_transport::api::ProtocolNoResponseError;
use rocketmq_transport::api::ProtocolNoResponseReason;
use rocketmq_transport::api::ProxyInfoSnapshot;
use rocketmq_transport::api::RejectRequestDecision;
use rocketmq_transport::api::RemotingClient;
use rocketmq_transport::api::RemotingClientBuilder;
use rocketmq_transport::api::RemotingRequest;
use rocketmq_transport::api::RemotingResponse;
use rocketmq_transport::api::RequestControlView;
use rocketmq_transport::api::RequestDeadline;
use rocketmq_transport::api::RequestId;
use rocketmq_transport::api::RequestMeta;
use rocketmq_transport::api::RequestOrdering;
use rocketmq_transport::api::RequestOrderingKey;
use rocketmq_transport::api::RequestOrigin;
use rocketmq_transport::api::RequestProcessor;
use rocketmq_transport::api::ResponseBodyKind;
use rocketmq_transport::api::ResponseBuildError;
use rocketmq_transport::api::ResponseDisposition;
use rocketmq_transport::api::ResponseErrorKind;
use rocketmq_transport::api::ResponseReceipt;
use rocketmq_transport::api::ResponseWriteObservation;
use rocketmq_transport::api::ResponseWriteOutcome;
use rocketmq_transport::api::ResponseWritePath;
use rocketmq_transport::api::SessionEvent;
use rocketmq_transport::api::SessionId;
use rocketmq_transport::api::SessionRegistry;
use rocketmq_transport::api::SessionStateView;
use rocketmq_transport::api::SessionView;
use rocketmq_transport::api::TransportClient;
use rocketmq_transport::api::TransportClientBuilder;
use rocketmq_transport::api::TransportServer;
use rocketmq_transport::api::WriteProgress;

fn assert_same_deadline_type(_: &RequestDeadline, _: &RequestDeadline) {}

fn assert_same_request_id_type(value: Option<RequestId>) -> Option<RequestId> {
    value
}

fn assert_original_identity_contract(identity: Option<OriginalRequestIdentity>) {
    if let Some(identity) = identity {
        let _: RequestId = identity.request_id();
        let _: i32 = identity.original_code();
        let _: i32 = identity.original_opaque();
        let _: bool = identity.is_one_way();
    }
}

fn closed<'a>(state: &'a SessionStateView) -> impl Future<Output = ()> + 'a {
    state.closed()
}

fn cancelled<'a>(control: &'a RequestControlView) -> impl Future<Output = ()> + 'a {
    control.cancelled()
}

fn assert_request_meta_contract(meta: Option<RequestMeta>) {
    if let Some(meta) = meta {
        let _: std::time::Instant = meta.received_at();
        let _: Option<RequestDeadline> = meta.deadline();
    }
}

fn assert_request_control_contract(control: Option<RequestControlView>) {
    if let Some(control) = control {
        let _: Option<RequestDeadline> = control.deadline();
        let _: bool = control.is_cancelled();
        std::mem::drop(cancelled(&control));
    }
}

fn assert_ingress_view_contract(view: IngressRequestView<'_>) {
    let _: RequestId = view.original_identity().request_id();
    let _: Option<&std::collections::HashMap<CheetahString, CheetahString>> = view.ext_fields();
}

fn assert_remoting_request_contract(request: Option<RemotingRequest>) {
    if let Some(mut request) = request {
        let _: OriginalRequestIdentity = request.original_identity();
        let _: &RequestMeta = request.meta();
        let _: &RequestOrigin = request.origin();
        let _: &AuthenticationState = request.authentication();
        let _: &SessionView = request.session();
        let _: &RequestControlView = request.control();
        let _: &RemotingCommand = request.command();
        let _: &mut RemotingCommand = request.command_mut();
        let _: Option<&String> = request.extension::<String>();
        let _: Result<Option<String>, String> = request.try_insert_extension("extension".to_owned());
    }
}

fn assert_file_region_dto_contract(_: Option<FileRegion>, _: Option<FileRegionSequence>) {}

fn assert_error_contract<T: std::error::Error>() {}

fn assert_debug_contract<T: std::fmt::Debug>() {}

fn assert_embedded_dispatch_contract(outcome: Option<EmbeddedDispatchOutcome>, error: Option<EmbeddedDispatchError>) {
    if let Some(outcome) = outcome {
        match outcome {
            EmbeddedDispatchOutcome::Reply(plan) => assert_remoting_response_contract(Some(plan)),
            EmbeddedDispatchOutcome::OneWay { request_id } | EmbeddedDispatchOutcome::Deferred { request_id } => {
                let _: RequestId = request_id;
            }
            EmbeddedDispatchOutcome::NoReply { request_id, reason } => {
                let _: RequestId = request_id;
                let _: ProtocolNoResponseReason = reason;
            }
            _ => {}
        }
    }
    if let Some(error) = error {
        let _: EmbeddedDispatchErrorKind = error.kind();
        let _: &(dyn std::error::Error + 'static) = &error;
    }
}

fn assert_remoting_response_contract(plan: Option<RemotingResponse>) {
    if let Some(plan) = plan {
        let _: i32 = plan.response_code();
        let _: ResponseBodyKind = plan.body_kind();
        let _: usize = plan.body_len();
        let _: usize = plan.body_part_count();
        let response: EmbeddedResponse = plan.into_embedded_response();
        let _: &RemotingCommand = response.head();
        let _: i32 = response.response_code();
        let _: &EmbeddedResponseBody = response.body();
        let _: (RemotingCommand, EmbeddedResponseBody) = response.into_parts();
    }

    let _: fn(RemotingCommand) -> Result<RemotingResponse, ResponseBuildError> = RemotingResponse::command;
    let _: fn(i32) -> RemotingResponse = RemotingResponse::empty_response;
    let _: fn(RemotingCommand, Bytes) -> Result<RemotingResponse, ResponseBuildError> = RemotingResponse::bytes;
    let _: fn(RemotingCommand, Vec<Bytes>) -> Result<RemotingResponse, ResponseBuildError> = RemotingResponse::segments;
    let _: fn(RemotingCommand, FileRegionSequence) -> Result<RemotingResponse, ResponseBuildError> =
        RemotingResponse::file_regions;
    let _ = ResponseBodyKind::Empty;
    let _ = ResponseBodyKind::Bytes;
    let _ = ResponseBodyKind::Segments;
    let _ = ResponseBodyKind::FileRegions;
    assert_error_contract::<ResponseBuildError>();
}

fn consume_handler_outcome_exhaustively(outcome: HandlerOutcome) -> Option<RequestId> {
    match outcome {
        HandlerOutcome::Reply(plan) => {
            let _: i32 = plan.response_code();
            None
        }
        HandlerOutcome::Deferred(registration) => Some(registration.request_id()),
        HandlerOutcome::NoReply(marker) => {
            let _: i32 = marker.original_code();
            let _: ProtocolNoResponseReason = marker.reason();
            Some(marker.request_id())
        }
    }
}

fn assert_handler_outcome_contract(registration: Option<DeferredRegistration>, marker: Option<ProtocolNoResponse>) {
    if let Some(registration) = registration {
        let _: DeferredId = registration.deferred_id();
        let _: RequestId = registration.request_id();
        let _: String = format!("{registration:?}");
    }
    if let Some(marker) = marker {
        let _: RequestId = marker.request_id();
        let _: i32 = marker.original_code();
        let _: ProtocolNoResponseReason = marker.reason();
    }

    let _: fn(&RemotingRequest, ProtocolNoResponseReason) -> Result<ProtocolNoResponse, ProtocolNoResponseError> =
        RemotingRequest::protocol_no_response;
    assert_error_contract::<ProtocolNoResponseError>();
    let _: RocketMQError = ProtocolNoResponseError::OneWayRequest.into();
}

fn assert_deferred_registry_contract<R, E>(
    registry: DeferredRegistry<R>,
    id: Option<DeferredId>,
    parts: Option<DeferredParts>,
    request: Option<DeferredRequest<R>>,
    error: Option<DeferredRegistryError<R, E>>,
) where
    R: Send + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    let _: DeferredRegistry<R> = registry.clone();
    let _: fn() -> DeferredRegistry<R> = DeferredRegistry::<R>::new;
    let _: fn(&DeferredRegistry<R>) -> DeferredRegistryShutdownOutcome = DeferredRegistry::<R>::shutdown;
    let _ = id;
    if let Some(parts) = parts {
        let _: RequestId = parts.request_id();
        let _: SessionId = parts.session_id();
        let _: usize = parts.retained_bytes();
        let _: DeferredResponder = parts.into_responder();
    }
    if let Some(mut request) = request {
        let _: RequestId = request.request_id();
        let _: SessionId = request.session_id();
        let _: usize = request.retained_bytes();
        let _: &R = request.resume();
        let _: &mut R = request.resume_mut();
        let _: (R, DeferredParts) = request.into_resume_and_parts();
    }
    if let Some(error) = error {
        let _: DeferredRegistryErrorKind = error.kind();
        let _: RequestId = error.request_id();
    }

    let _: fn(DeferredResponder, DeferredWaitPermit) -> DeferredParts = DeferredParts::new;
    let _: fn(
        DeferredParts,
        tokio::time::Instant,
        DeferredExpiryMargins,
    ) -> Result<DeferredParts, DeferredExpiryError> = DeferredParts::try_with_expiry;
    let _: fn(R, DeferredParts) -> DeferredRequest<R> = DeferredRequest::new;
    let _: fn(DeferredRetainedSizeParts) -> Result<DeferredRetainedSize, _> = DeferredRegistry::<R>::try_retained_size;
}

fn assert_deferred_registry_recovery_contract() {
    type Error = DeferredRegistryError<String, std::io::Error>;
    let _: fn(Error) -> Option<DeferredRequest<String>> = Error::into_request;
    let _: fn(Error) -> Option<DeferredParts> = Error::into_parts;
    let _: fn(Error) -> Option<(std::io::Error, DeferredParts)> = Error::into_builder_failure;
}

fn assert_claim_resume_contract<R>(
    registry: &DeferredRegistry<R>,
    id: DeferredId,
    mut claimed: ClaimedDeferred<R>,
    claim_error: Option<&DeferredClaimError>,
    resume_error: Option<&DeferredResumeError>,
) where
    R: Send + 'static,
{
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    fn assert_send_future<F: Future + Send>(_: F) {}

    let _: DeferredId = claimed.deferred_id();
    let _: RequestId = claimed.request_id();
    let _: DeferredWakeReason = claimed.reason();
    let _: &R = claimed.resume_data();
    let _: &mut R = claimed.resume_data_mut();
    assert_send_future(registry.claim(id, DeferredWakeReason::MessageArrived));
    let resume = claimed.resume(DeferredResumeRetainedSize::new(7), |_, _| async move {
        RemotingResponse::command(RemotingCommand::create_response_command_with_code(0))
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
    });
    assert_send_future(resume);
    if let Some(error) = claim_error {
        let _: DeferredClaimErrorKind = error.kind();
        let _: DeferredId = error.deferred_id();
        let _: Option<RequestId> = error.request_id();
        let _ = error.prior_terminal_state();
    }
    if let Some(error) = resume_error {
        let _: DeferredResumeErrorKind = error.kind();
        let _: DeferredId = error.deferred_id();
        let _: RequestId = error.request_id();
        let _ = error.prior_terminal_state();
        let _: Option<DeferredTerminalReason> = error.prior_terminal_reason();
        let _: Option<WriteProgress> = error.write_progress();
    }
    let _: usize = DeferredResumeRetainedSize::default().dynamic_bytes();
    assert_send::<DeferredRegistry<std::cell::Cell<u8>>>();
    assert_sync::<DeferredRegistry<std::cell::Cell<u8>>>();
    assert_send::<ClaimedDeferred<std::cell::Cell<u8>>>();
}

struct LocalOnlyProcessor;

impl LocalRequestProcessor for LocalOnlyProcessor {
    async fn process(&mut self, _request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let local = Rc::new(());
        std::future::ready(()).await;
        drop(local);
        Err(RocketMQError::illegal_argument("local processor contract"))
    }
}

#[derive(Clone)]
struct SendProcessor;

impl RequestProcessor for SendProcessor {
    async fn process(&mut self, _request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        Err(RocketMQError::illegal_argument("send processor contract"))
    }
}

fn assert_local_processor<T: LocalRequestProcessor>() {}

fn assert_send_processor<T: RequestProcessor + Send>() {}

fn consume_rejection_exhaustively(decision: RejectRequestDecision) -> Option<RemotingResponse> {
    match decision {
        RejectRequestDecision::Proceed => None,
        RejectRequestDecision::Reject(plan) => Some(plan),
    }
}

fn inspect_write_outcome(outcome: ResponseWriteOutcome) -> Option<ResponseReceipt> {
    match outcome {
        ResponseWriteOutcome::Written(receipt) => Some(receipt),
        ResponseWriteOutcome::Failed { kind, progress } => {
            let _: ResponseErrorKind = kind;
            let _: Option<WriteProgress> = progress;
            None
        }
    }
}

fn assert_response_write_observation_contract(observation: &ResponseWriteObservation) {
    let _: RequestId = observation.request_id();
    let _: i32 = observation.original_code();
    let _: i32 = observation.response_code();
    let _: ResponseBodyKind = observation.body_kind();
    let _: ResponseWritePath = observation.path();
    let _: Duration = observation.write_elapsed();
    let _: Duration = observation.end_to_end_elapsed();
    let _: Option<ResponseReceipt> = inspect_write_outcome(observation.outcome());
}

fn order_from_ingress<P: LocalRequestProcessor>(processor: &P, ingress: IngressRequestView<'_>) -> RequestOrdering {
    processor.request_ordering(ingress)
}

fn assert_session_view_contract(view: SessionView) {
    let _: SessionId = view.id();
    let _: &SessionStateView = view.state();
    match view {
        SessionView::Network {
            id,
            local_addr,
            remote_addr,
            transport_peer_addr,
            proxy,
            state,
            ..
        } => {
            let _: SessionId = id;
            let _: std::net::SocketAddr = local_addr;
            let _: std::net::SocketAddr = remote_addr;
            let _: std::net::SocketAddr = transport_peer_addr;
            let _: bool = state.is_healthy();
            let _: bool = state.is_closed();
            let _: Option<ProxyInfoSnapshot> = proxy;
            std::mem::drop(closed(&state));
        }
        SessionView::Embedded { id, state, .. } => {
            let _: SessionId = id;
            let _: bool = state.is_healthy();
            let _: bool = state.is_closed();
            std::mem::drop(closed(&state));
        }
        _ => {}
    }
}

#[test]
fn api_exposes_the_request_deadline_type() {
    let deadline = RequestDeadline::after(Duration::from_secs(1));

    assert_same_deadline_type(&deadline, &deadline);
}

#[test]
fn api_exposes_request_id_and_read_only_original_identity() {
    let source_value: Option<RequestId> = None;
    let returned_value: Option<RequestId> = assert_same_request_id_type(source_value);
    assert!(returned_value.is_none());
    assert_original_identity_contract(None);
}

#[test]
fn api_exposes_read_only_origin_and_authenticated_principal() {
    fn authenticated_principal(state: &AuthenticationState) -> Option<&Principal> {
        state.principal()
    }

    fn has_authenticated_principal(state: &AuthenticationState) -> bool {
        match state {
            AuthenticationState::Anonymous | AuthenticationState::SecurityDisabled => false,
            AuthenticationState::Authenticated(principal, ..) => principal.id() == "user",
            _ => state.principal().is_some(),
        }
    }

    fn inspect_origin(origin: &RequestOrigin) -> Option<std::net::SocketAddr> {
        match origin {
            RequestOrigin::Network { peer, .. } => Some(peer.address()),
            RequestOrigin::Embedded { caller } => {
                let _known_caller = matches!(caller, EmbeddedCaller::BrokerProxy);
                None
            }
            _ => None,
        }
    }

    let _: fn(&AuthenticationState) -> Option<&Principal> = authenticated_principal;
    let _: fn(&AuthenticationState) -> bool = has_authenticated_principal;
    let _: fn(&RequestOrigin) -> Option<std::net::SocketAddr> = inspect_origin;
}

#[test]
fn api_exposes_read_only_network_and_embedded_session_views() {
    let _: fn(SessionView) = assert_session_view_contract;
}

#[test]
fn api_exposes_read_only_request_metadata_and_control() {
    assert_request_meta_contract(None);
    assert_request_control_contract(None);
}

#[test]
fn api_exposes_the_request_aggregate_and_ingress_view_without_legacy_reexports() {
    let _: fn(IngressRequestView<'_>) = assert_ingress_view_contract;
    assert_remoting_request_contract(None);
}

#[test]
fn api_exposes_only_remoting_response_metadata_and_file_region_construction_dtos() {
    assert_remoting_response_contract(None);
    assert_file_region_dto_contract(None, None);
}

#[test]
fn api_exposes_the_affine_embedded_outcome_and_redacted_error_facades() {
    assert_debug_contract::<EmbeddedDispatchOutcome>();
    assert_debug_contract::<EmbeddedDispatchErrorKind>();
    assert_error_contract::<EmbeddedDispatchError>();
    assert_embedded_dispatch_contract(None, None);
}

#[test]
fn api_exposes_exactly_three_exhaustive_affine_handler_outcomes() {
    assert_debug_contract::<HandlerOutcome>();
    assert_debug_contract::<DeferredRegistration>();
    assert_debug_contract::<ProtocolNoResponse>();
    let plan = RemotingResponse::command(RemotingCommand::create_response_command_with_code(0))
        .expect("public reply plan should construct");
    assert_eq!(consume_handler_outcome_exhaustively(HandlerOutcome::Reply(plan)), None);
    assert_handler_outcome_contract(None, None);

    let _ = ProtocolNoResponseReason::CallbackHandled;
    let _ = ProtocolNoResponseReason::NotificationHandled;
    let unsupported = ProtocolNoResponseError::Unsupported {
        request_code: -91,
        reason: ProtocolNoResponseReason::CallbackHandled,
    };
    assert!(unsupported.to_string().contains("-91"));
}

#[test]
fn api_exposes_the_affine_transactional_deferred_registry_contract() {
    let contract = assert_deferred_registry_contract::<String, std::io::Error>;
    let _ = contract;
    assert_deferred_registry_recovery_contract();
    assert_debug_contract::<DeferredId>();
    assert_debug_contract::<DeferredParts>();
    assert_debug_contract::<DeferredRequest<String>>();
    assert_debug_contract::<DeferredRegistry<String>>();
    assert_debug_contract::<DeferredRegistryShutdownOutcome>();
    assert_debug_contract::<DeferredRegistryShutdownStats>();
    assert_debug_contract::<DeferredExpiry>();
    assert_debug_contract::<DeferredExpiryBatchStats>();
    assert_error_contract::<DeferredExpiryError>();
    let _: fn(DeferredRegistryShutdownStats) -> usize = DeferredRegistryShutdownStats::detached_entries;
    let _: fn(DeferredRegistryShutdownStats) -> usize = DeferredRegistryShutdownStats::notified_tickets;
    let _: fn(DeferredRegistryShutdownStats) -> usize = DeferredRegistryShutdownStats::terminalized_responses;
    let _: fn(DeferredRegistryShutdownStats) -> usize = DeferredRegistryShutdownStats::in_progress_responses;
    let _: fn(DeferredRegistryShutdownStats) -> usize = DeferredRegistryShutdownStats::invariant_failures;
    assert_error_contract::<DeferredRegistryError<String, std::io::Error>>();
    assert_eq!(DeferredRegistryErrorKind::Builder.as_str(), "builder");
    let _ = assert_claim_resume_contract::<String>;
    assert_error_contract::<DeferredClaimError>();
    assert_error_contract::<DeferredResumeError>();
    assert_eq!(DeferredClaimErrorKind::AlreadyClaimed.as_str(), "already_claimed");
    assert_eq!(DeferredResumeErrorKind::TaskTerminated.as_str(), "task_terminated");
    let margins = DeferredExpiryMargins::new(Duration::from_millis(2), Duration::from_millis(3));
    assert_eq!(margins.recovery(), Duration::from_millis(2));
    assert_eq!(margins.write(), Duration::from_millis(3));
    assert_eq!(DeferredExpiryKind::LongPollTimeout.as_str(), "long_poll_timeout");
    assert_eq!(DeferredExpiryErrorKind::AlreadyAttached.as_str(), "already_attached");
    assert_eq!(DeferredTerminalReason::OwnerDeadline.as_str(), "owner_deadline");
    assert_eq!(
        DeferredTerminalReason::SessionClosed.terminal_state(),
        rocketmq_transport::api::ResponseTerminalState::Closed
    );
    let _ = DeferredCancellationReason::ReceiverDropped;
    let stats = DeferredExpiryBatchStats::default();
    assert_eq!(stats.examined(), 0);
    assert_eq!(stats.long_poll_claims(), 0);
    assert_eq!(stats.pending_long_poll(), 0);
    assert_eq!(stats.owner_expired(), 0);
    assert_eq!(stats.invariant_failures(), 0);
    fn assert_send<T: Send>() {}
    assert_send::<DeferredExpiryBatch<String>>();
    let _: fn(DeferredExpiryError) -> DeferredParts = DeferredExpiryError::into_parts;
}

#[test]
fn api_processor_contract_preserves_local_and_send_future_boundaries() {
    assert_local_processor::<LocalOnlyProcessor>();
    assert_send_processor::<SendProcessor>();

    let processor = LocalOnlyProcessor;
    assert!(matches!(processor.reject_request(39), RejectRequestDecision::Proceed));
    assert!(consume_rejection_exhaustively(RejectRequestDecision::default()).is_none());
    let _: fn(&LocalOnlyProcessor, IngressRequestView<'_>) -> RequestOrdering =
        order_from_ingress::<LocalOnlyProcessor>;
}

#[test]
fn api_processor_side_contracts_are_typed_and_body_free() {
    let empty = RemotingResponse::empty_response(6);
    assert_eq!(empty.response_code(), 6);
    assert_eq!(empty.body_kind(), ResponseBodyKind::Empty);
    assert_eq!(empty.body_len(), 0);

    let plan = RemotingResponse::bytes(
        RemotingCommand::create_response_command_with_code(7),
        Bytes::from_static(b"owned rejection"),
    )
    .expect("public rejection plan");
    let plan = consume_rejection_exhaustively(RejectRequestDecision::Reject(plan))
        .expect("rejection should own its remoting response");
    assert_eq!(plan.response_code(), 7);
    assert_eq!(plan.body_kind(), ResponseBodyKind::Bytes);
    assert_eq!(plan.body_len(), 15);

    let _ = ResponseWritePath::Inline;
    let _ = ResponseWritePath::Deferred;
    let failure = ResponseWriteOutcome::Failed {
        kind: ResponseErrorKind::Transport,
        progress: Some(WriteProgress::PossiblyPartial),
    };
    assert!(inspect_write_outcome(failure).is_none());
    let _ = ResponseDisposition::TransportWritten;
    let _ = ResponseDisposition::InProcessAccepted;
    let _ = RequestOrdering::Concurrent;
    let _ = RequestOrdering::Ordered(RequestOrderingKey::new(9));
    let _: fn(&ResponseWriteObservation) = assert_response_write_observation_contract;
}

#[test]
fn api_network_dispatcher_and_server_are_public_nameable_facades() {
    let runtime = rocketmq_runtime::RuntimeOwner::new().expect("public facade runtime");
    let service_context = runtime.root_context().component("public-facades");
    let security =
        std::sync::Arc::new(rocketmq_transport::api::TransportSecurity::development_insecure_loopback(None, None));
    let admission = std::sync::Arc::new(rocketmq_transport::api::AdmissionController::new(
        rocketmq_transport::api::AdmissionLimits::default(),
    ));

    let dispatcher: std::sync::Arc<AuthorizedCommandDispatcher<SendProcessor>> = std::sync::Arc::new(
        AuthorizedCommandDispatcher::new(SendProcessor, Vec::new(), security, admission),
    );
    let _: TransportServer<SendProcessor> = TransportServer::new_with_authorized_dispatcher(
        std::sync::Arc::new(rocketmq_transport::api::ServerConfig::default()),
        service_context,
        dispatcher,
    );
}

#[test]
fn api_client_facade_defaults_and_builders_use_the_canonical_processor() {
    fn assert_transport<PR: RequestProcessor>(_: Option<TransportClient<PR>>) {}
    fn assert_remoting<PR: RequestProcessor>(_: Option<RemotingClient<PR>>) {}

    let transport: Option<TransportClient> = None;
    let remoting: Option<RemotingClient> = None;
    assert_transport(transport);
    assert_remoting(remoting);

    let _: Option<TransportClientBuilder<DefaultRequestProcessor>> = None;
    let _: Option<RemotingClientBuilder<DefaultRequestProcessor>> = None;
    let registry = SessionRegistry::new();
    assert!(registry.is_empty());
    let _: tokio::sync::broadcast::Receiver<SessionEvent> = registry.subscribe();
}
