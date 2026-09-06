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
use crate::long_polling::notification_deferred::service::NotificationDeferredPrepareOutcome;
use crate::long_polling::notification_deferred::service::NotificationDeferredPrepareRejectionKind;
use crate::long_polling::notification_deferred::service::NotificationDeferredRegisterOutcome;
use crate::long_polling::notification_deferred::service::NotificationDeferredRegisterRejectionKind;
use crate::long_polling::notification_deferred::service::NotificationRegisterFault;
use crate::long_polling::notification_deferred::service::PreparedNotificationRegistration;

fn success_reply(polling_full: bool) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
    let header = NotificationResponseHeader {
        has_msg: false,
        polling_full,
    };
    RemotingResponse::command(
        application_remoting_command_factory().create_success_response_command_with_header(header),
    )
    .map(HandlerOutcome::Reply)
    .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
}

fn limited_service(
    controller: &AdmissionController,
    wait_limits: DeferredWaitLimits,
    index_limits: NotificationCriteriaLimits,
) -> Arc<NotificationDeferredService> {
    let admission = DeferredAdmission::try_configure(controller, wait_limits).expect("limited Notification admission");
    Arc::new(NotificationDeferredService::new(
        admission,
        index_limits,
        DeferredExpiryMargins::new(Duration::from_millis(2), Duration::from_millis(2)),
        nonzero(4),
        nonzero(4),
        nonzero(2),
        nonzero(1024 * 1024),
    ))
}

fn assert_empty(service: &NotificationDeferredService) {
    let snapshot = service.snapshot();
    assert_eq!(snapshot.admission().waiting_count(), 0);
    assert_eq!(snapshot.admission().retained_bytes(), 0);
    assert_eq!(snapshot.index().live(), 0);
    assert_eq!(snapshot.index().reserved(), 0);
    assert_eq!(snapshot.prepared(), 0);
    assert_eq!(snapshot.pending_claims(), 0);
    assert_eq!(snapshot.resume_executions(), 0);
    assert_eq!(snapshot.resume_execution_bytes(), 0);
}

#[derive(Default)]
struct ProvenanceState {
    prepared: Option<PreparedNotificationRegistration>,
}

#[derive(Clone)]
struct ProvenanceProcessor {
    service: Arc<NotificationDeferredService>,
    state: Arc<Mutex<ProvenanceState>>,
}

impl RequestProcessor for ProvenanceProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        if let Some(prepared) = self.state.lock().prepared.take() {
            let rejection = match self.service.register(prepared, request) {
                Ok(NotificationDeferredRegisterOutcome::Rejected(rejection)) => rejection,
                Ok(NotificationDeferredRegisterOutcome::Registered(_)) | Err(_) => {
                    panic!("a prepared Notification proof cannot move to another request")
                }
            };
            assert_eq!(
                rejection.kind(),
                NotificationDeferredRegisterRejectionKind::ProvenanceMismatch
            );
            return success_reply(false);
        }
        let prepared = match self
            .service
            .prepare(request, None, None, NotificationRetainedEstimate::default())
        {
            Ok(NotificationDeferredPrepareOutcome::Prepared(prepared)) => *prepared,
            Ok(NotificationDeferredPrepareOutcome::Rejected(rejection)) => {
                return Err(RocketMQError::illegal_argument(format!("{:?}", rejection.kind())));
            }
            Err(error) => return Err(RocketMQError::illegal_argument(error.to_string())),
        };
        self.state.lock().prepared = Some(prepared);
        success_reply(false)
    }
}

#[derive(Clone)]
struct EmbeddedOriginProcessor {
    service: Arc<NotificationDeferredService>,
    observed: Arc<Mutex<Option<NotificationDeferredPrepareRejectionKind>>>,
}

impl RequestProcessor for EmbeddedOriginProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let error = match self
            .service
            .prepare(request, None, None, NotificationRetainedEstimate::default())
        {
            Ok(NotificationDeferredPrepareOutcome::Rejected(rejection)) => rejection,
            Ok(NotificationDeferredPrepareOutcome::Prepared(_)) | Err(_) => {
                panic!("embedded Notification must fail before allocating deferred resources")
            }
        };
        *self.observed.lock() = Some(error.kind());
        success_reply(false)
    }
}

struct AllowEmbeddedPolicy;

impl RequestPolicy for AllowEmbeddedPolicy {
    fn evaluate_authenticated(&self, _context: AuthenticatedRequestContext<'_>) -> Decision {
        Decision::Allow
    }
}

#[derive(Clone)]
struct CapacityProcessor {
    service: Arc<NotificationDeferredService>,
    held: Arc<Mutex<Vec<PreparedNotificationRegistration>>>,
    observed: Arc<Mutex<Vec<NotificationDeferredPrepareRejectionKind>>>,
}

impl RequestProcessor for CapacityProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let polling_full = match self
            .service
            .prepare(request, None, None, NotificationRetainedEstimate::default())
        {
            Ok(NotificationDeferredPrepareOutcome::Prepared(prepared)) => {
                self.held.lock().push(*prepared);
                false
            }
            Ok(NotificationDeferredPrepareOutcome::Rejected(rejection)) => {
                self.observed.lock().push(rejection.kind());
                true
            }
            Err(error) => return Err(RocketMQError::illegal_argument(error.to_string())),
        };
        success_reply(polling_full)
    }
}

#[derive(Clone)]
struct OneWayProcessor {
    service: Arc<NotificationDeferredService>,
    observed: mpsc::UnboundedSender<NotificationDeferredPrepareRejectionKind>,
}

#[derive(Clone)]
struct PostTakeFaultProcessor {
    service: Arc<NotificationDeferredService>,
    fault: NotificationRegisterFault,
    observed: mpsc::UnboundedSender<NotificationDeferredRegisterRejectionKind>,
}

impl RequestProcessor for PostTakeFaultProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let prepared = match self
            .service
            .prepare(request, None, None, NotificationRetainedEstimate::default())
        {
            Ok(NotificationDeferredPrepareOutcome::Prepared(prepared)) => *prepared,
            Ok(NotificationDeferredPrepareOutcome::Rejected(rejection)) => {
                return Err(RocketMQError::illegal_argument(format!("{:?}", rejection.kind())));
            }
            Err(error) => return Err(RocketMQError::illegal_argument(error.to_string())),
        };
        self.service.force_register_fault(self.fault);
        let rejection = match self.service.register(prepared, request) {
            Ok(NotificationDeferredRegisterOutcome::Rejected(rejection)) => rejection,
            Ok(NotificationDeferredRegisterOutcome::Registered(_)) | Err(_) => {
                panic!("injected post-take Notification fault must fail registration")
            }
        };
        let kind = rejection.kind();
        drop(rejection);
        self.observed
            .send(kind)
            .map_err(|_| RocketMQError::illegal_argument("post-take observer closed"))?;
        Err(RocketMQError::illegal_argument(
            "injected post-take Notification rejection",
        ))
    }
}

impl RequestProcessor for OneWayProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let rejection = match self
            .service
            .prepare(request, None, None, NotificationRetainedEstimate::default())
        {
            Ok(NotificationDeferredPrepareOutcome::Rejected(rejection)) => rejection,
            Ok(NotificationDeferredPrepareOutcome::Prepared(_)) | Err(_) => {
                panic!("one-way Notification must fail before deferred allocation")
            }
        };
        self.observed
            .send(rejection.kind())
            .map_err(|_| RocketMQError::illegal_argument("one-way observer closed"))?;
        success_reply(false)
    }
}

#[tokio::test]
async fn notification_deferred_one_way_is_rejected_before_capacity_take_and_writes_nothing() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref());
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let processor = OneWayProcessor {
        service: Arc::clone(&service),
        observed: observed_tx,
    };
    let (mut client, running) = start_server(processor, controller).await;
    client
        .send_command(request_command().mark_oneway_rpc())
        .await
        .expect("send one-way Notification");
    assert_eq!(
        observed_rx.recv().await,
        Some(NotificationDeferredPrepareRejectionKind::OneWay)
    );
    assert_empty(&service);
    running.finish().await;
    assert!(
        client.receive_command().await.is_none(),
        "one-way request emits no frame"
    );
}

#[tokio::test]
async fn notification_deferred_cross_request_provenance_fails_before_responder_take() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref());
    let state = Arc::new(Mutex::new(ProvenanceState::default()));
    let processor = ProvenanceProcessor {
        service: Arc::clone(&service),
        state: Arc::clone(&state),
    };
    let (mut client, running) = start_server(processor, controller).await;

    for opaque in [10_101, 10_102] {
        client
            .send_command(request_command_for("provenance-group", opaque, 60_000))
            .await
            .expect("send Notification provenance request");
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
async fn notification_deferred_embedded_origin_is_rejected_before_capacity_take() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref());
    let observed = Arc::new(Mutex::new(None));
    let processor = EmbeddedOriginProcessor {
        service: Arc::clone(&service),
        observed: Arc::clone(&observed),
    };
    let runtime = RuntimeOwner::plan(RuntimeConfig::server_default("notification-embedded-origin"))
        .expect("test runtime configuration is valid")
        .build()
        .expect("embedded Notification runtime");
    let context = runtime.root_context().component("notification.embedded-origin");
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
        Principal::new("notification-embedded-test"),
    );

    let outcome = harness
        .dispatch(None, request_command_for("embedded-group", 10_103, 60_000))
        .await
        .expect("embedded Notification inline response");
    assert!(matches!(outcome, EmbeddedDispatchOutcome::Reply(_)));
    assert_eq!(
        *observed.lock(),
        Some(NotificationDeferredPrepareRejectionKind::EmbeddedOrigin)
    );
    assert_empty(&service);

    drop(harness);
    drop(context);
    assert!(runtime.shutdown_tasks().await.is_healthy());
    assert!(runtime.shutdown_background().is_healthy());
}

#[tokio::test]
async fn notification_deferred_global_per_key_wait_and_bytes_full_map_to_polling_full() {
    let cases = [
        (
            "global",
            DeferredWaitLimits::new(4, 1024 * 1024),
            NotificationCriteriaLimits::new(nonzero(1), 3, 1),
            vec![("GroupA", 10_201), ("GroupB", 10_202)],
            NotificationDeferredPrepareRejectionKind::Index,
        ),
        (
            "per-key",
            DeferredWaitLimits::new(4, 1024 * 1024),
            NotificationCriteriaLimits::new(nonzero(4), 0, 1),
            vec![("GroupA", 10_203), ("GroupA", 10_204)],
            NotificationDeferredPrepareRejectionKind::Index,
        ),
        (
            "wait-count",
            DeferredWaitLimits::new(1, 1024 * 1024),
            NotificationCriteriaLimits::new(nonzero(4), 3, 1),
            vec![("GroupA", 10_205), ("GroupB", 10_206)],
            NotificationDeferredPrepareRejectionKind::Admission,
        ),
        (
            "wait-bytes",
            DeferredWaitLimits::new(4, 1),
            NotificationCriteriaLimits::new(nonzero(4), 3, 1),
            vec![("GroupA", 10_207)],
            NotificationDeferredPrepareRejectionKind::Admission,
        ),
    ];

    for (label, wait_limits, index_limits, requests, expected_kind) in cases {
        let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
        let service = limited_service(controller.as_ref(), wait_limits, index_limits);
        let held = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::new(Mutex::new(Vec::new()));
        let processor = CapacityProcessor {
            service: Arc::clone(&service),
            held: Arc::clone(&held),
            observed: Arc::clone(&observed),
        };
        let (mut client, running) = start_server(processor, controller).await;
        let mut polling_full = Vec::new();
        for (group, opaque) in requests {
            client
                .send_command(request_command_for(group, opaque, 60_000))
                .await
                .expect("send Notification capacity probe");
            let reply = client
                .receive_command()
                .await
                .expect("capacity connection")
                .expect("capacity inline response");
            let header = reply
                .decode_command_custom_header::<NotificationResponseHeader>()
                .expect("Notification capacity response header");
            polling_full.push(header.polling_full);
        }
        assert_eq!(observed.lock().as_slice(), &[expected_kind], "{label}");
        assert_eq!(polling_full.last(), Some(&true), "{label}");
        held.lock().clear();
        assert_empty(&service);
        running.finish().await;
    }
}

#[tokio::test]
async fn notification_deferred_post_take_close_expiry_and_registry_fail_closed_without_a_frame() {
    let cases = [
        (
            "service-close",
            NotificationRegisterFault::Close,
            NotificationDeferredRegisterRejectionKind::ServiceClosedAfterTake,
        ),
        (
            "expiry",
            NotificationRegisterFault::Expiry,
            NotificationDeferredRegisterRejectionKind::Expiry,
        ),
        (
            "registry",
            NotificationRegisterFault::Builder,
            NotificationDeferredRegisterRejectionKind::Registry,
        ),
    ];

    for (label, fault, expected) in cases {
        let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
        let service = service(controller.as_ref());
        let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
        let processor = PostTakeFaultProcessor {
            service: Arc::clone(&service),
            fault,
            observed: observed_tx,
        };
        let (mut client, running) = start_server(processor, controller).await;
        client
            .send_command(request_command_for("post-take-group", 10_300, 60_000))
            .await
            .expect("send post-take Notification probe");
        assert_eq!(observed_rx.recv().await, Some(expected), "{label}");
        assert_empty(&service);
        let _ = service.shutdown();
        running.finish().await;
        assert!(
            client.receive_command().await.is_none(),
            "{label} emitted a response frame"
        );
    }
}
