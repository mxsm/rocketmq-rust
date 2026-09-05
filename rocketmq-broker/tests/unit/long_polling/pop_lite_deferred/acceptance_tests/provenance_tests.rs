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
use rocketmq_transport::api::DeferredAdmission;
use rocketmq_transport::api::DeferredExpiryMargins;
use rocketmq_transport::api::DeferredWaitLimits;
use rocketmq_transport::api::EmbeddedDispatchOutcome;
use rocketmq_transport::api::TransportSecurity;
use rocketmq_transport::test_support::EmbeddedRequestHarness;

use super::*;
use crate::long_polling::pop_lite_deferred::prepare::PopLiteDeferredPrepareErrorKind;
use crate::long_polling::pop_lite_deferred::prepare::PopLiteDeferredRegisterErrorKind;
use crate::long_polling::pop_lite_deferred::prepare::PreparedPopLiteRegistration;

fn success_reply() -> rocketmq_error::RocketMQResult<HandlerOutcome> {
    RemotingResponse::command(RemotingCommand::create_response_command_with_code(
        ResponseCode::Success,
    ))
    .map(HandlerOutcome::Reply)
    .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
}

#[derive(Default)]
struct ProvenanceState {
    prepared: Option<PreparedPopLiteRegistration>,
}

#[derive(Clone)]
struct ProvenanceProcessor {
    service: Arc<PopLiteDeferredService>,
    state: Arc<Mutex<ProvenanceState>>,
}

impl RequestProcessor for ProvenanceProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        if let Some(prepared) = self.state.lock().prepared.take() {
            let error = self
                .service
                .register(prepared, request)
                .expect_err("a prepared PopLite proof cannot move to another request");
            assert_eq!(error.kind(), PopLiteDeferredRegisterErrorKind::ProvenanceMismatch);
            return success_reply();
        }
        let prepared = self
            .service
            .prepare(request, PopLiteRetainedEstimate::default())
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        self.state.lock().prepared = Some(prepared);
        success_reply()
    }
}

#[derive(Clone)]
struct EmbeddedOriginProcessor {
    service: Arc<PopLiteDeferredService>,
    observed: Arc<Mutex<Option<PopLiteDeferredPrepareErrorKind>>>,
}

impl RequestProcessor for EmbeddedOriginProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let error = match self.service.prepare(request, PopLiteRetainedEstimate::default()) {
            Err(error) => error,
            Ok(_) => panic!("embedded PopLite must fail before allocating deferred resources"),
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

fn assert_empty(service: &PopLiteDeferredService) {
    let snapshot = service.resource_snapshot();
    assert_eq!(snapshot.admission.waiting_count(), 0);
    assert_eq!(snapshot.admission.retained_bytes(), 0);
    assert_eq!(snapshot.index.live, 0);
    assert_eq!(snapshot.index.reserved, 0);
    assert_eq!(snapshot.prepared_registrations, 0);
    assert_eq!(snapshot.resume_execution_count, 0);
    assert_eq!(snapshot.resume_execution_bytes, 0);
}

#[tokio::test]
async fn pop_lite_deferred_cross_request_provenance_fails_before_responder_take() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref(), LiteEventDispatcher::default());
    let state = Arc::new(Mutex::new(ProvenanceState::default()));
    let processor = ProvenanceProcessor {
        service: Arc::clone(&service),
        state: Arc::clone(&state),
    };
    let (mut client, running) = start_server(processor, controller).await;

    for opaque in [101, 102] {
        client
            .send_command(request_command_for("provenance-client", opaque, 60_000))
            .await
            .expect("send PopLite provenance request");
        let reply = client
            .receive_command()
            .await
            .expect("provenance connection")
            .expect("inline provenance response");
        assert_eq!(reply.opaque(), opaque);
    }
    assert!(state.lock().prepared.is_none());
    assert_empty(&service);
    running.finish().await;
}

#[tokio::test]
async fn pop_lite_deferred_embedded_origin_is_rejected_before_capacity_take() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref(), LiteEventDispatcher::default());
    let observed = Arc::new(Mutex::new(None));
    let processor = EmbeddedOriginProcessor {
        service: Arc::clone(&service),
        observed: Arc::clone(&observed),
    };
    let runtime = RuntimeOwner::plan(RuntimeConfig::server_default("pop-lite-embedded-origin"))
        .expect("test runtime configuration is valid")
        .build()
        .expect("embedded PopLite runtime");
    let context = runtime.root_context().component("pop-lite.embedded-origin");
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
        Principal::new("pop-lite-embedded-test"),
    );

    let outcome = harness
        .dispatch(None, request_command_for("embedded-client", 103, 60_000))
        .await
        .expect("embedded PopLite inline response");
    assert!(matches!(outcome, EmbeddedDispatchOutcome::Reply(_)));
    assert_eq!(*observed.lock(), Some(PopLiteDeferredPrepareErrorKind::EmbeddedOrigin));
    assert_empty(&service);

    drop(harness);
    drop(context);
    assert!(runtime.shutdown_tasks().await.is_healthy());
    assert!(runtime.shutdown_background().is_healthy());
}

#[derive(Clone)]
struct CapacityProbeProcessor {
    service: Arc<PopLiteDeferredService>,
    held: Arc<Mutex<Vec<PreparedPopLiteRegistration>>>,
    observed: Arc<Mutex<Vec<PopLiteDeferredPrepareErrorKind>>>,
}

impl RequestProcessor for CapacityProbeProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        match self.service.prepare(request, PopLiteRetainedEstimate::default()) {
            Ok(prepared) => self.held.lock().push(prepared),
            Err(error) => self.observed.lock().push(error.kind()),
        }
        success_reply()
    }
}

fn limited_service(controller: &AdmissionController, limits: DeferredWaitLimits) -> Arc<PopLiteDeferredService> {
    let admission = DeferredAdmission::try_configure(controller, limits).expect("limited PopLite admission");
    Arc::new(PopLiteDeferredService::new(
        admission,
        PopLiteIndexLimits::new(nonzero(4), nonzero(4), nonzero(2)),
        LiteEventDispatcher::default(),
        DeferredExpiryMargins::new(Duration::from_millis(2), Duration::from_millis(2)),
        Duration::from_secs(30),
        nonzero(4),
    ))
}

#[tokio::test]
async fn pop_lite_deferred_wait_admission_enforces_count_and_bytes_before_take() {
    for (limits, requests) in [
        (DeferredWaitLimits::new(1, 1024 * 1024), 2_usize),
        (DeferredWaitLimits::new(4, 1), 1_usize),
    ] {
        let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
        let service = limited_service(controller.as_ref(), limits);
        let held = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::new(Mutex::new(Vec::new()));
        let processor = CapacityProbeProcessor {
            service: Arc::clone(&service),
            held: Arc::clone(&held),
            observed: Arc::clone(&observed),
        };
        let (mut client, running) = start_server(processor, controller).await;
        for index in 0..requests {
            client
                .send_command(request_command_for("capacity-client", 110 + index as i32, 60_000))
                .await
                .expect("send PopLite capacity probe");
            client
                .receive_command()
                .await
                .expect("capacity connection")
                .expect("capacity inline response");
        }
        assert_eq!(
            observed.lock().as_slice(),
            &[PopLiteDeferredPrepareErrorKind::Admission]
        );
        held.lock().clear();
        assert_empty(&service);
        running.finish().await;
    }
}
