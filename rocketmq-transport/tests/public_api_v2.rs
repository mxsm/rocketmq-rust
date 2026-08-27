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
use rocketmq_transport::api::v1::AuthorizedCommandDispatcher;
use rocketmq_transport::api::v1::DefaultRequestProcessor;
use rocketmq_transport::api::v1::RequestDeadline as V1RequestDeadline;
use rocketmq_transport::api::v1::RequestId as V1RequestId;
use rocketmq_transport::api::v1::TransportServer;
use rocketmq_transport::api::v1::TransportTelemetry;
use rocketmq_transport::api::v2::AuthenticationState;
use rocketmq_transport::api::v2::AuthorizedCommandDispatcherV2;
use rocketmq_transport::api::v2::DeferredRegistration;
use rocketmq_transport::api::v2::EmbeddedCaller;
use rocketmq_transport::api::v2::FileRegion;
use rocketmq_transport::api::v2::FileRegionSequence;
use rocketmq_transport::api::v2::HandlerOutcome;
use rocketmq_transport::api::v2::IngressRequestView;
use rocketmq_transport::api::v2::LocalRequestProcessorV2;
use rocketmq_transport::api::v2::OriginalRequestIdentity;
use rocketmq_transport::api::v2::ProtocolNoResponse;
use rocketmq_transport::api::v2::ProtocolNoResponseError;
use rocketmq_transport::api::v2::ProtocolNoResponseReason;
use rocketmq_transport::api::v2::ProxyInfoSnapshot;
use rocketmq_transport::api::v2::RejectRequestDecision;
use rocketmq_transport::api::v2::RemotingRequest;
use rocketmq_transport::api::v2::RequestControlView;
use rocketmq_transport::api::v2::RequestDeadline as V2RequestDeadline;
use rocketmq_transport::api::v2::RequestId as V2RequestId;
use rocketmq_transport::api::v2::RequestMeta;
use rocketmq_transport::api::v2::RequestOrdering;
use rocketmq_transport::api::v2::RequestOrderingKey;
use rocketmq_transport::api::v2::RequestOrigin;
use rocketmq_transport::api::v2::RequestProcessorV2;
use rocketmq_transport::api::v2::ResponseBodyKind;
use rocketmq_transport::api::v2::ResponseDisposition;
use rocketmq_transport::api::v2::ResponseErrorKind;
use rocketmq_transport::api::v2::ResponsePlan;
use rocketmq_transport::api::v2::ResponsePlanError;
use rocketmq_transport::api::v2::ResponseReceipt;
use rocketmq_transport::api::v2::ResponseWriteObservationV2;
use rocketmq_transport::api::v2::ResponseWriteOutcomeV2;
use rocketmq_transport::api::v2::ResponseWritePath;
use rocketmq_transport::api::v2::SessionId;
use rocketmq_transport::api::v2::SessionStateView;
use rocketmq_transport::api::v2::SessionView;
use rocketmq_transport::api::v2::TransportServerV2;
use rocketmq_transport::api::v2::WriteProgress;

fn assert_same_deadline_type(_: &V1RequestDeadline, _: &V2RequestDeadline) {}

fn assert_same_request_id_type(value: Option<V1RequestId>) -> Option<V2RequestId> {
    value
}

fn assert_original_identity_contract(identity: Option<OriginalRequestIdentity>) {
    if let Some(identity) = identity {
        let _: V2RequestId = identity.request_id();
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
        let _: Option<V2RequestDeadline> = meta.deadline();
    }
}

fn assert_request_control_contract(control: Option<RequestControlView>) {
    if let Some(control) = control {
        let _: Option<V2RequestDeadline> = control.deadline();
        let _: bool = control.is_cancelled();
        std::mem::drop(cancelled(&control));
    }
}

fn assert_ingress_view_contract(view: IngressRequestView<'_>) {
    let _: V2RequestId = view.original_identity().request_id();
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
        let _: Result<Option<String>, String> = request.try_insert_extension("v2-extension".to_owned());
    }
}

fn assert_file_region_dto_contract(_: Option<FileRegion>, _: Option<FileRegionSequence>) {}

fn assert_error_contract<T: std::error::Error>() {}

fn assert_debug_contract<T: std::fmt::Debug>() {}

fn assert_response_plan_contract(plan: Option<ResponsePlan>) {
    if let Some(plan) = plan {
        let _: i32 = plan.response_code();
        let _: ResponseBodyKind = plan.body_kind();
        let _: usize = plan.body_len();
        let _: usize = plan.body_part_count();
    }

    let _: fn(RemotingCommand) -> Result<ResponsePlan, ResponsePlanError> = ResponsePlan::command;
    let _: fn(RemotingCommand, Bytes) -> Result<ResponsePlan, ResponsePlanError> = ResponsePlan::bytes;
    let _: fn(RemotingCommand, Vec<Bytes>) -> Result<ResponsePlan, ResponsePlanError> = ResponsePlan::segments;
    let _: fn(RemotingCommand, FileRegionSequence) -> Result<ResponsePlan, ResponsePlanError> =
        ResponsePlan::file_regions;
    let _ = ResponseBodyKind::Empty;
    let _ = ResponseBodyKind::Bytes;
    let _ = ResponseBodyKind::Segments;
    let _ = ResponseBodyKind::FileRegions;
    assert_error_contract::<ResponsePlanError>();
}

fn consume_handler_outcome_exhaustively(outcome: HandlerOutcome) -> Option<V2RequestId> {
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
        let _: V2RequestId = registration.request_id();
        let _: String = format!("{registration:?}");
    }
    if let Some(marker) = marker {
        let _: V2RequestId = marker.request_id();
        let _: i32 = marker.original_code();
        let _: ProtocolNoResponseReason = marker.reason();
    }

    let _: fn(&RemotingRequest, ProtocolNoResponseReason) -> Result<ProtocolNoResponse, ProtocolNoResponseError> =
        RemotingRequest::protocol_no_response;
    assert_error_contract::<ProtocolNoResponseError>();
    let _: RocketMQError = ProtocolNoResponseError::OneWayRequest.into();
}

struct LocalOnlyProcessor;

impl LocalRequestProcessorV2 for LocalOnlyProcessor {
    async fn process(&mut self, _request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let local = Rc::new(());
        std::future::ready(()).await;
        drop(local);
        Err(RocketMQError::illegal_argument("local processor contract"))
    }
}

#[derive(Clone)]
struct SendProcessor;

impl RequestProcessorV2 for SendProcessor {
    async fn process(&mut self, _request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        Err(RocketMQError::illegal_argument("send processor contract"))
    }
}

fn assert_local_processor<T: LocalRequestProcessorV2>() {}

fn assert_send_processor<T: RequestProcessorV2 + Send>() {}

fn consume_rejection_exhaustively(decision: RejectRequestDecision) -> Option<ResponsePlan> {
    match decision {
        RejectRequestDecision::Proceed => None,
        RejectRequestDecision::Reject(plan) => Some(plan),
    }
}

fn inspect_write_outcome(outcome: ResponseWriteOutcomeV2) -> Option<ResponseReceipt> {
    match outcome {
        ResponseWriteOutcomeV2::Written(receipt) => Some(receipt),
        ResponseWriteOutcomeV2::Failed { kind, progress } => {
            let _: ResponseErrorKind = kind;
            let _: Option<WriteProgress> = progress;
            None
        }
    }
}

fn assert_response_write_observation_contract(observation: &ResponseWriteObservationV2) {
    let _: V2RequestId = observation.request_id();
    let _: i32 = observation.original_code();
    let _: i32 = observation.response_code();
    let _: ResponseBodyKind = observation.body_kind();
    let _: ResponseWritePath = observation.path();
    let _: Duration = observation.write_elapsed();
    let _: Duration = observation.end_to_end_elapsed();
    let _: Option<ResponseReceipt> = inspect_write_outcome(observation.outcome());
}

fn order_from_ingress<P: LocalRequestProcessorV2>(processor: &P, ingress: IngressRequestView<'_>) -> RequestOrdering {
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
fn v2_exposes_the_v1_request_deadline_type() {
    let deadline = V2RequestDeadline::after(Duration::from_secs(1));

    assert_same_deadline_type(&deadline, &deadline);
}

#[test]
fn v2_reuses_v1_request_id_and_exposes_read_only_original_identity() {
    let v1_value: Option<V1RequestId> = None;
    let v2_value: Option<V2RequestId> = assert_same_request_id_type(v1_value);
    assert!(v2_value.is_none());
    assert_original_identity_contract(None);
}

#[test]
fn v2_exposes_read_only_origin_and_authenticated_principal() {
    fn authenticated_principal(state: &AuthenticationState) -> Option<&Principal> {
        state.principal()
    }

    fn has_authenticated_principal(state: &AuthenticationState) -> bool {
        match state {
            AuthenticationState::Anonymous | AuthenticationState::SecurityDisabled => false,
            AuthenticationState::Authenticated(principal, ..) => principal.id() == "v2-user",
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
fn v2_exposes_read_only_network_and_embedded_session_views() {
    let _: fn(SessionView) = assert_session_view_contract;
}

#[test]
fn v2_exposes_read_only_request_metadata_and_control() {
    assert_request_meta_contract(None);
    assert_request_control_contract(None);
}

#[test]
fn v2_exposes_the_request_aggregate_and_ingress_view_without_legacy_reexports() {
    let _: fn(IngressRequestView<'_>) = assert_ingress_view_contract;
    assert_remoting_request_contract(None);
}

#[test]
fn v2_exposes_only_response_plan_metadata_and_file_region_construction_dtos() {
    assert_response_plan_contract(None);
    assert_file_region_dto_contract(None, None);
}

#[test]
fn v2_exposes_exactly_three_exhaustive_affine_handler_outcomes() {
    assert_debug_contract::<HandlerOutcome>();
    assert_debug_contract::<DeferredRegistration>();
    assert_debug_contract::<ProtocolNoResponse>();
    let plan = ResponsePlan::command(RemotingCommand::create_response_command_with_code(0))
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
fn v2_processor_contract_preserves_local_and_send_future_boundaries() {
    assert_local_processor::<LocalOnlyProcessor>();
    assert_send_processor::<SendProcessor>();

    let processor = LocalOnlyProcessor;
    assert!(matches!(processor.reject_request(39), RejectRequestDecision::Proceed));
    assert!(consume_rejection_exhaustively(RejectRequestDecision::default()).is_none());
    let _: fn(&LocalOnlyProcessor, IngressRequestView<'_>) -> RequestOrdering =
        order_from_ingress::<LocalOnlyProcessor>;
}

#[test]
fn v2_processor_side_contracts_are_typed_and_body_free() {
    let plan = ResponsePlan::bytes(
        RemotingCommand::create_response_command_with_code(7),
        Bytes::from_static(b"owned rejection"),
    )
    .expect("public rejection plan");
    let plan = consume_rejection_exhaustively(RejectRequestDecision::Reject(plan))
        .expect("rejection should own its response plan");
    assert_eq!(plan.response_code(), 7);
    assert_eq!(plan.body_kind(), ResponseBodyKind::Bytes);
    assert_eq!(plan.body_len(), 15);

    let _ = ResponseWritePath::Inline;
    let _ = ResponseWritePath::Deferred;
    let failure = ResponseWriteOutcomeV2::Failed {
        kind: ResponseErrorKind::Transport,
        progress: Some(WriteProgress::PossiblyPartial),
    };
    assert!(inspect_write_outcome(failure).is_none());
    let _ = ResponseDisposition::TransportWritten;
    let _ = ResponseDisposition::InProcessAccepted;
    let _ = RequestOrdering::Concurrent;
    let _ = RequestOrdering::Ordered(RequestOrderingKey::new(9));
    let _: fn(&ResponseWriteObservationV2) = assert_response_write_observation_contract;
}

#[test]
fn v2_network_dispatcher_and_server_are_public_nameable_facades() {
    let runtime = rocketmq_runtime::RuntimeOwner::new(rocketmq_runtime::RuntimeConfig::default())
        .expect("public V2 facade runtime");
    let service_context = runtime.root_context().component("public-v2-facades");
    let security =
        std::sync::Arc::new(rocketmq_transport::api::v1::TransportSecurity::development_insecure_loopback(None, None));
    let admission = std::sync::Arc::new(rocketmq_transport::api::v1::AdmissionController::new(
        rocketmq_transport::api::v1::AdmissionLimits::default(),
    ));

    let v1_dispatcher: std::sync::Arc<AuthorizedCommandDispatcher<DefaultRequestProcessor>> = std::sync::Arc::new(
        AuthorizedCommandDispatcher::try_new(
            DefaultRequestProcessor,
            Vec::new(),
            &service_context.process_budget(),
            TransportTelemetry::noop(),
            std::sync::Arc::clone(&security),
            std::sync::Arc::clone(&admission),
        )
        .expect("public V1 facade construction"),
    );
    let _: std::sync::Arc<rocketmq_transport::api::v1::AuthorizedDispatchBoundary> = v1_dispatcher.boundary();
    let _: TransportServer<DefaultRequestProcessor> = TransportServer::new(
        std::sync::Arc::new(rocketmq_transport::api::v1::ServerConfig::default()),
        service_context.clone(),
    );

    let dispatcher: std::sync::Arc<AuthorizedCommandDispatcherV2<SendProcessor>> = std::sync::Arc::new(
        AuthorizedCommandDispatcherV2::new(SendProcessor, Vec::new(), security, admission),
    );
    let _: std::sync::Arc<rocketmq_transport::api::v1::AuthorizedDispatchBoundary> = dispatcher.boundary();
    let _: TransportServerV2<SendProcessor> = TransportServerV2::new_with_authorized_dispatcher(
        std::sync::Arc::new(rocketmq_transport::api::v1::ServerConfig::default()),
        service_context,
        dispatcher,
    );
}
