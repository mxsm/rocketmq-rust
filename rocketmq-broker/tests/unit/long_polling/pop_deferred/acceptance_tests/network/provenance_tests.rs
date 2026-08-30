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

use parking_lot::Mutex;
use rocketmq_security_api::AuthenticatedRequestContext;
use rocketmq_security_api::Decision;
use rocketmq_security_api::Principal;
use rocketmq_security_api::RequestPolicy;
use rocketmq_transport::api::AuthorizedCommandDispatcher;
use rocketmq_transport::api::EmbeddedDispatchOutcome;
use rocketmq_transport::api::TransportSecurity;
use rocketmq_transport::test_support::EmbeddedRequestHarness;

use super::*;
use crate::long_polling::pop_deferred::service::PopDeferredRegisterErrorKind;
use crate::long_polling::pop_deferred::service::PreparedPopRegistration;

fn success_reply() -> rocketmq_error::RocketMQResult<HandlerOutcome> {
    ResponsePlan::command(RemotingCommand::create_response_command_with_code(
        ResponseCode::Success,
    ))
    .map(HandlerOutcome::Reply)
    .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
}

#[derive(Default)]
struct ProvenanceProbeState {
    prepared: Option<PreparedPopRegistration>,
}

#[derive(Clone)]
struct ProvenanceProbeProcessor {
    service: Arc<PopDeferredService>,
    state: Arc<Mutex<ProvenanceProbeState>>,
}

impl RequestProcessor for ProvenanceProbeProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let prepared = {
            let mut state = self.state.lock();
            state.prepared.take()
        };
        if let Some(prepared) = prepared {
            let error = self
                .service
                .register(prepared, request)
                .expect_err("a prepared proof cannot be paired with another request");
            assert_eq!(error.kind(), PopDeferredRegisterErrorKind::ProvenanceMismatch);
            return success_reply();
        }

        let prepared = self
            .service
            .prepare(request, None, None, PopRetainedEstimate::default())
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        self.state.lock().prepared = Some(prepared);
        success_reply()
    }
}

#[derive(Clone)]
struct EmbeddedOriginProbeProcessor {
    service: Arc<PopDeferredService>,
    observed: Arc<Mutex<Option<PopDeferredPrepareErrorKind>>>,
}

impl RequestProcessor for EmbeddedOriginProbeProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let Err(error) = self
            .service
            .prepare(request, None, None, PopRetainedEstimate::default())
        else {
            panic!("embedded POP must not create deferred state");
        };
        *self.observed.lock() = Some(error.kind());
        success_reply()
    }
}

struct AllowEmbeddedPolicy;

impl RequestPolicy for AllowEmbeddedPolicy {
    fn evaluate_authenticated(&self, _context: AuthenticatedRequestContext<'_>) -> Decision {
        Decision::Allow
    }
}

#[tokio::test]
async fn cross_request_prepared_proof_fails_before_responder_take() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref(), 2, 2, 2);
    let state = Arc::new(Mutex::new(ProvenanceProbeState::default()));
    let processor = ProvenanceProbeProcessor {
        service: Arc::clone(&service),
        state: Arc::clone(&state),
    };
    let (mut client, _address, running) = start_server(processor, Arc::clone(&controller)).await;

    client
        .send_command(request_command("TopicA", "GroupA", 0, None, 41))
        .await
        .expect("send proof source request");
    let first = client
        .receive_command()
        .await
        .expect("proof source connection")
        .expect("proof source inline reply");
    assert_eq!(first.code(), ResponseCode::Success as i32);
    assert_eq!(service.index_snapshot().reserved(), 1);
    assert_eq!(service.admission_snapshot().waiting_count(), 1);

    client
        .send_command(request_command("TopicA", "GroupA", 0, None, 42))
        .await
        .expect("send mismatched proof target");
    let second = client
        .receive_command()
        .await
        .expect("proof target connection")
        .expect("mismatched request still owns its inline responder");
    assert_eq!(second.code(), ResponseCode::Success as i32);
    assert!(state.lock().prepared.is_none());
    assert_released(&service);

    running.finish().await;
}

#[tokio::test]
async fn embedded_origin_is_rejected_before_any_pop_reservation() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref(), 2, 2, 2);
    let observed = Arc::new(Mutex::new(None));
    let processor = EmbeddedOriginProbeProcessor {
        service: Arc::clone(&service),
        observed: Arc::clone(&observed),
    };
    let runtime = RuntimeOwner::new(RuntimeConfig::server_default("broker-pop-deferred-embedded-origin"))
        .expect("embedded POP runtime owner");
    let context = runtime.root_context().component("pop-deferred.embedded-origin");
    let dispatcher = Arc::new(AuthorizedCommandDispatcher::new(
        processor,
        Vec::new(),
        Arc::new(TransportSecurity::secure_enforced(
            Some(Arc::new(AllowEmbeddedPolicy)),
            None,
        )),
        Arc::clone(&controller),
    ));
    let harness = EmbeddedRequestHarness::new(
        dispatcher,
        context.task_group().clone(),
        Principal::new("broker-pop-deferred-test"),
    );

    let outcome = harness
        .dispatch(None, request_command("TopicA", "GroupA", 0, None, 43))
        .await
        .expect("embedded POP rejection is an inline response");
    assert!(matches!(outcome, EmbeddedDispatchOutcome::Reply(_)));
    assert_eq!(*observed.lock(), Some(PopDeferredPrepareErrorKind::EmbeddedOrigin));
    assert_released(&service);

    drop(harness);
    drop(context);
    let task_report = runtime.shutdown_tasks().await;
    assert_clean_shutdown("embedded POP runtime", &task_report);
    let final_report = runtime.shutdown_background();
    assert_clean_shutdown("embedded POP runtime finalization", &final_report);
}
