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

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

use super::harness::loopback_server_config;
use super::harness::start_server;
use super::harness::TestRuntime;
use super::loopback_security;
use super::AdmissionController;
use super::AdmissionLimits;
use super::DeferredAdmission;
use super::DeferredParts;
use super::DeferredRegistry;
use super::DeferredRequest;
use super::DeferredRetainedSizeParts;
use super::DeferredWaitLimits;
use super::DeferredWakeReason;
use super::HandlerOutcome;
use super::ProtocolNoResponseReason;
use super::RejectRequestDecision;
use super::RemotingRequest;
use super::RequestProcessor;
use super::ResponsePlan;
use super::TransportServer;
use crate::telemetry::TransportTelemetry;

const REPLY_CODE: i32 = 8_201;
const ERROR_CODE: i32 = 8_202;
const REJECT_CODE: i32 = 8_203;
const DEFERRED_CODE: i32 = 8_204;
const NO_REPLY_CODE: i32 = 39;

#[derive(Clone)]
struct ConstructionProbeProcessor {
    registry: DeferredRegistry<i32>,
    admission: DeferredAdmission,
    registrations: tokio::sync::mpsc::UnboundedSender<crate::dispatch::DeferredId>,
}

impl RequestProcessor for ConstructionProbeProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        match request.command().code() {
            ERROR_CODE => Err(RocketMQError::illegal_argument("mapped processor failure")),
            NO_REPLY_CODE => Ok(HandlerOutcome::NoReply(
                request.protocol_no_response(ProtocolNoResponseReason::CallbackHandled)?,
            )),
            DEFERRED_CODE => {
                let responder = request
                    .take_deferred_responder()
                    .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
                let retained = DeferredRegistry::<i32>::try_retained_size(DeferredRetainedSizeParts::new(0))
                    .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
                let permit = self
                    .admission
                    .try_reserve(retained)
                    .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
                let registration = self
                    .registry
                    .register(DeferredRequest::new(
                        request.original_identity().original_opaque(),
                        DeferredParts::new(responder, permit),
                    ))
                    .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
                self.registrations
                    .send(registration.deferred_id())
                    .map_err(|_| RocketMQError::illegal_argument("construction registration observer closed"))?;
                Ok(HandlerOutcome::Deferred(registration))
            }
            REPLY_CODE => ResponsePlan::bytes(
                RemotingCommand::create_response_command_with_code(ResponseCode::Success),
                Bytes::from_static(b"inline-without-deferred-state"),
            )
            .map(HandlerOutcome::Reply)
            .map_err(|error| RocketMQError::illegal_argument(error.to_string())),
            code => Err(RocketMQError::illegal_argument(format!(
                "unexpected construction-probe request code {code}"
            ))),
        }
    }

    fn request_ordering(
        &self,
        _ingress: crate::dispatch::IngressRequestView<'_>,
    ) -> crate::request_ordering::RequestOrdering {
        crate::request_ordering::RequestOrdering::Ordered(crate::request_ordering::RequestOrderingKey::new(8_222))
    }

    fn reject_request(&self, code: i32) -> RejectRequestDecision {
        if code == REJECT_CODE {
            return RejectRequestDecision::Reject(
                ResponsePlan::command(RemotingCommand::create_response_command_with_code(
                    ResponseCode::NoPermission,
                ))
                .expect("construction-probe rejection plan"),
            );
        }
        RejectRequestDecision::Proceed
    }
}

#[tokio::test]
async fn real_tcp_inline_paths_construct_zero_deferred_states_and_a_real_defer_constructs_one() {
    let runtime = TestRuntime::new("transport-inline-deferred-state");
    let admission_controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let admission = DeferredAdmission::try_configure(
        admission_controller.as_ref(),
        DeferredWaitLimits::new(4, 4 * 1024 * 1024),
    )
    .expect("construction-probe deferred admission");
    let registry = DeferredRegistry::<i32>::new();
    let (registration_tx, mut registrations) = tokio::sync::mpsc::unbounded_channel();
    let processor = ConstructionProbeProcessor {
        registry: registry.clone(),
        admission: admission.clone(),
        registrations: registration_tx,
    };
    let (telemetry, constructions) = TransportTelemetry::with_deferred_state_construction_capture();
    let server = TransportServer::new(loopback_server_config(), runtime.service_context(), processor)
        .with_transport_security(loopback_security(), None)
        .with_admission_controller(admission_controller)
        .with_telemetry(telemetry);
    let (mut client, _address, mut running) = start_server(runtime, server).await;

    client
        .send_command(RemotingCommand::create_remoting_command(REPLY_CODE).set_opaque(8_301))
        .await
        .expect("send inline reply request");
    let reply = receive(&mut client, "inline reply").await;
    assert_eq!((reply.code(), reply.opaque()), (ResponseCode::Success.to_i32(), 8_301));
    assert_eq!(constructions.load(Ordering::SeqCst), 0);

    client
        .send_command(RemotingCommand::create_remoting_command(ERROR_CODE).set_opaque(8_302))
        .await
        .expect("send mapped processor error request");
    let mapped = receive(&mut client, "mapped processor error").await;
    assert_eq!(ResponseCode::from(mapped.code()), ResponseCode::InvalidParameter);
    assert_eq!(mapped.opaque(), 8_302);
    assert_eq!(constructions.load(Ordering::SeqCst), 0);

    client
        .send_command(RemotingCommand::create_remoting_command(REJECT_CODE).set_opaque(8_303))
        .await
        .expect("send rejected request");
    let rejected = receive(&mut client, "rejected request").await;
    assert_eq!(ResponseCode::from(rejected.code()), ResponseCode::NoPermission);
    assert_eq!(rejected.opaque(), 8_303);
    assert_eq!(constructions.load(Ordering::SeqCst), 0);

    client
        .send_command(RemotingCommand::create_remoting_command(NO_REPLY_CODE).set_opaque(8_304))
        .await
        .expect("send allowlisted no-reply request");
    client
        .send_command(RemotingCommand::create_remoting_command(REPLY_CODE).set_opaque(8_305))
        .await
        .expect("send no-reply ordering sentinel");
    let sentinel = receive(&mut client, "no-reply ordering sentinel").await;
    assert_eq!(sentinel.opaque(), 8_305, "NoReply must not synthesize a frame");
    assert_eq!(constructions.load(Ordering::SeqCst), 0);

    client
        .send_command(RemotingCommand::create_remoting_command(DEFERRED_CODE).set_opaque(8_306))
        .await
        .expect("send genuine deferred request");
    let id = tokio::time::timeout(Duration::from_secs(1), registrations.recv())
        .await
        .expect("genuine deferred registration deadline")
        .expect("genuine deferred registration observer");
    let claim = registry
        .claim(id, DeferredWakeReason::MessageArrived)
        .await
        .expect("genuine deferred registration commits");
    assert_eq!(constructions.load(Ordering::SeqCst), 1);
    drop(claim);
    assert_eq!(admission.snapshot().waiting_count(), 0);
    assert_eq!(admission.snapshot().retained_bytes(), 0);

    running.begin_shutdown();
    running.finish().await;
    assert_eq!(registry.test_index_counts(), (0, 0, 0));
    assert_eq!(constructions.load(Ordering::SeqCst), 1);
}

async fn receive(client: &mut crate::connection::Connection, path: &'static str) -> RemotingCommand {
    tokio::time::timeout(Duration::from_secs(1), client.receive_command())
        .await
        .unwrap_or_else(|_| panic!("{path} response deadline"))
        .unwrap_or_else(|| panic!("{path} connection remains open"))
        .unwrap_or_else(|error| panic!("{path} response frame: {error}"))
}
