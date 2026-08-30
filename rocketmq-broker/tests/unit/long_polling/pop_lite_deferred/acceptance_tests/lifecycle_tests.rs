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

use super::*;
use crate::long_polling::pop_lite_deferred::prepare::PopLiteDeferredPrepareErrorKind;
use crate::long_polling::pop_lite_deferred::prepare::PopLiteDeferredRegisterErrorKind;

#[derive(Clone)]
struct AfterTakeCloseProcessor {
    service: Arc<PopLiteDeferredService>,
    observed: mpsc::UnboundedSender<PopLiteDeferredRegisterErrorKind>,
}

impl RequestProcessor for AfterTakeCloseProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let prepared = self
            .service
            .prepare(request, PopLiteRetainedEstimate::default())
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        let error = self
            .service
            .register(prepared, request)
            .expect_err("the service closes after responder transfer");
        let kind = error.kind();
        let message = error.to_string();
        drop(error);
        self.observed
            .send(kind)
            .map_err(|_| RocketMQError::illegal_argument("after-take observer closed"))?;
        Err(RocketMQError::illegal_argument(message))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pop_lite_deferred_close_after_responder_take_rolls_back_builder_resources() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref(), LiteEventDispatcher::default());
    let taken = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    service.set_register_after_take_hook(Arc::clone(&taken), Arc::clone(&resume));
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let processor = AfterTakeCloseProcessor {
        service: Arc::clone(&service),
        observed: observed_tx,
    };
    let (mut client, running) = start_server(processor, controller).await;
    client
        .send_command(request_command_for("after-take-close", 401, 60_000))
        .await
        .expect("send after-take close request");

    tokio::task::spawn_blocking(move || taken.wait())
        .await
        .expect("observe responder transfer");
    service.shutdown();
    tokio::task::spawn_blocking(move || resume.wait())
        .await
        .expect("release after-take registration");
    assert_eq!(
        observed_rx.recv().await.expect("after-take close result"),
        PopLiteDeferredRegisterErrorKind::ServiceClosedAfterTake
    );
    let snapshot = service.resource_snapshot();
    assert_eq!(snapshot.admission.waiting_count(), 0);
    assert_eq!(snapshot.admission.retained_bytes(), 0);
    assert_eq!(snapshot.index.live, 0);
    assert_eq!(snapshot.index.reserved, 0);
    assert_eq!(snapshot.prepared_registrations, 0);
    assert_eq!(snapshot.resume_execution_count, 0);
    assert_eq!(snapshot.resume_execution_bytes, 0);
    running.finish().await;
    assert!(
        client.receive_command().await.is_none(),
        "sealed service after responder take emits no fallback frame"
    );
}

#[derive(Clone)]
struct OneWayProbeProcessor {
    service: Arc<PopLiteDeferredService>,
    observed: mpsc::UnboundedSender<PopLiteDeferredPrepareErrorKind>,
}

impl RequestProcessor for OneWayProbeProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let error = match self.service.prepare(request, PopLiteRetainedEstimate::default()) {
            Err(error) => error,
            Ok(_) => panic!("one-way PopLite must fail before allocating deferred resources"),
        };
        self.observed
            .send(error.kind())
            .map_err(|_| RocketMQError::illegal_argument("one-way observer closed"))?;
        RemotingResponse::command(RemotingCommand::create_response_command_with_code(
            ResponseCode::Success,
        ))
        .map(HandlerOutcome::Reply)
        .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
    }
}

#[tokio::test]
async fn pop_lite_deferred_one_way_is_rejected_before_capacity_and_emits_no_frame() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref(), LiteEventDispatcher::default());
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let processor = OneWayProbeProcessor {
        service: Arc::clone(&service),
        observed: observed_tx,
    };
    let (mut client, running) = start_server(processor, controller).await;
    let command = request_command_for("one-way-client", 402, 60_000).mark_oneway_rpc();
    client.send_command(command).await.expect("send one-way PopLite probe");
    assert_eq!(
        observed_rx.recv().await.expect("one-way PopLite prepare result"),
        PopLiteDeferredPrepareErrorKind::OneWay
    );
    let snapshot = service.resource_snapshot();
    assert_eq!(snapshot.admission.waiting_count(), 0);
    assert_eq!(snapshot.admission.retained_bytes(), 0);
    assert_eq!(snapshot.index.live, 0);
    assert_eq!(snapshot.index.reserved, 0);
    assert_eq!(snapshot.prepared_registrations, 0);
    running.finish().await;
    assert!(
        client.receive_command().await.is_none(),
        "one-way PopLite emits no frame"
    );
}

#[tokio::test]
async fn pop_lite_deferred_shutdown_drains_registered_waiter_without_processor_or_frame() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref(), LiteEventDispatcher::default());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredTestProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
    };
    let (mut client, running) = start_server(processor, controller).await;
    client
        .send_command(request_command_for("shutdown-waiter", 403, 60_000))
        .await
        .expect("send shutdown PopLite waiter");
    registrations.recv().await.expect("observe shutdown PopLite waiter");
    let registered = service.resource_snapshot();
    assert_eq!(registered.admission.waiting_count(), 1);
    assert_eq!(registered.index.live, 1);

    assert!(matches!(
        service.shutdown(),
        rocketmq_transport::api::DeferredRegistryShutdownOutcome::Completed(_)
    ));
    // The registration observer runs before the dispatcher commits
    // `HandlerOutcome::Deferred`. Registry shutdown drains registry-owned
    // entries synchronously, while a concurrently finishing handler may still
    // own the provisional builder. Await the test-owned server task group so
    // that either commit or rollback has reached its canonical terminal before
    // asserting the complete admission snapshot.
    running.finish().await;
    let drained = service.resource_snapshot();
    assert_eq!(drained.admission.waiting_count(), 0);
    assert_eq!(drained.admission.retained_bytes(), 0);
    assert_eq!(drained.index.live, 0);
    assert_eq!(drained.index.reserved, 0);
    assert_eq!(drained.index.candidates, 0);
    assert_eq!(drained.event_reservations.events, 0);
    assert_eq!(drained.active_client_gates, 0);
    assert_eq!(drained.pending_claims, 0);
    assert_eq!(drained.resume_execution_count, 0);
    assert_eq!(drained.resume_execution_bytes, 0);

    assert!(
        client.receive_command().await.is_none(),
        "service shutdown emits no synthetic PopLite response"
    );
}

#[tokio::test]
async fn pop_lite_deferred_service_close_drops_claim_and_restores_event_batch_exactly_once() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let dispatcher = LiteEventDispatcher::default();
    let service = service(controller.as_ref(), dispatcher.clone());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredTestProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
    };
    let (mut client, running) = start_server(processor, controller).await;
    client
        .send_command(request_command_for("service-close-claim", 404, 60_000))
        .await
        .expect("send service-close PopLite waiter");
    registrations.recv().await.expect("observe service-close waiter");

    let client_id = CheetahString::from_static_str("service-close-claim");
    let first = CheetahString::from_static_str("%LMQ%$parent-topic$close-a");
    let second = CheetahString::from_static_str("%LMQ%$parent-topic$close-b");
    dispatcher.do_full_dispatch(
        &client_id,
        &CheetahString::from_static_str("group-a"),
        &HashSet::from([first.clone(), second.clone()]),
    );
    let claim = service
        .claim_event(&client_id)
        .await
        .expect("claim event before service close")
        .expect("service-close waiter is claimable");
    let claimed = service.resource_snapshot();
    assert_eq!(claimed.event_reservations.events, 2);
    assert_eq!(claimed.event_reservations.permits, 2);
    assert_eq!(claimed.active_client_gates, 1);

    let _ = service.shutdown();
    drop(claim);
    assert_eq!(dispatcher.pending_events(&client_id), vec![first, second]);
    assert_eq!(dispatcher.budget_snapshot().current_count, 2);
    assert_eq!(dispatcher.reservation_snapshot().events, 0);
    let drained = service.resource_snapshot();
    assert_eq!(drained.admission.waiting_count(), 0);
    assert_eq!(drained.index.live, 0);
    assert_eq!(drained.event_reservations.events, 0);
    assert_eq!(drained.active_client_gates, 0);
    assert_eq!(drained.resume_execution_count, 0);
    assert_eq!(drained.resume_execution_bytes, 0);

    assert_eq!(dispatcher.take_pending_events(&client_id).len(), 2);
    running.finish().await;
    assert!(client.receive_command().await.is_none());
}

#[tokio::test]
async fn pop_lite_deferred_inactive_session_cannot_claim_execute_or_write() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let dispatcher = LiteEventDispatcher::default();
    let service = service(controller.as_ref(), dispatcher.clone());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredTestProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
    };
    let (mut client, running) = start_server(processor, controller).await;
    client
        .send_command(request_command_for("inactive-session", 405, 60_000))
        .await
        .expect("send inactive-session PopLite waiter");
    registrations.recv().await.expect("observe inactive-session waiter");
    client.shutdown().await.expect("close inactive PopLite session");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = service.resource_snapshot();
            if snapshot.admission.waiting_count() == 0 && snapshot.index.live == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("session cleanup drains inactive PopLite waiter");

    let client_id = CheetahString::from_static_str("inactive-session");
    let event = CheetahString::from_static_str("%LMQ%$parent-topic$inactive");
    dispatcher.do_full_dispatch(
        &client_id,
        &CheetahString::from_static_str("group-a"),
        &HashSet::from([event.clone()]),
    );
    assert!(service
        .claim_event(&client_id)
        .await
        .expect("inactive-session miss is not a claim error")
        .is_none());
    let terminal = service.resource_snapshot();
    assert_eq!(terminal.pending_claims, 0);
    assert_eq!(terminal.active_client_gates, 0);
    assert_eq!(terminal.event_reservations.events, 0);
    assert_eq!(terminal.resume_execution_count, 0);
    assert_eq!(terminal.resume_execution_bytes, 0);
    assert_eq!(dispatcher.pending_events(&client_id), vec![event]);
    assert_eq!(dispatcher.take_pending_events(&client_id).len(), 1);

    running.finish().await;
    assert!(client.receive_command().await.is_none());
}
