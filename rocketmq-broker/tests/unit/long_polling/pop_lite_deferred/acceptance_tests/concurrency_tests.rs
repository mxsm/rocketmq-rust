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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use tokio::sync::Notify;
use tokio::sync::Semaphore;

use super::*;

fn assert_terminal_snapshot(service: &PopLiteDeferredService) {
    let snapshot = service.resource_snapshot();
    assert_eq!(snapshot.admission.waiting_count(), 0);
    assert_eq!(snapshot.admission.retained_bytes(), 0);
    assert_eq!(snapshot.index.live, 0);
    assert_eq!(snapshot.index.reserved, 0);
    assert_eq!(snapshot.index.candidates, 0);
    assert_eq!(snapshot.event_reservations.batches, 0);
    assert_eq!(snapshot.event_reservations.events, 0);
    assert_eq!(snapshot.event_reservations.permits, 0);
    assert_eq!(snapshot.event_reservations.retained_bytes, 0);
    assert_eq!(snapshot.active_client_gates, 0);
    assert_eq!(snapshot.prepared_registrations, 0);
    assert_eq!(snapshot.pending_claims, 0);
    assert_eq!(snapshot.resume_execution_count, 0);
    assert_eq!(snapshot.resume_execution_bytes, 0);
    assert_eq!(snapshot.pending_replays, 0);
}

#[derive(Clone)]
struct OpaqueRegistrationProcessor {
    service: Arc<PopLiteDeferredService>,
    registrations: mpsc::UnboundedSender<(i32, DeferredId)>,
}

impl RequestProcessor for OpaqueRegistrationProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let opaque = request.command().opaque();
        let prepared = self
            .service
            .prepare(request, PopLiteRetainedEstimate::default())
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        let registration = self
            .service
            .register(prepared, request)
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        self.registrations
            .send((opaque, registration.deferred_id()))
            .map_err(|_| RocketMQError::illegal_argument("opaque registration observer closed"))?;
        Ok(HandlerOutcome::Deferred(registration))
    }

    fn request_ordering(&self, _ingress: rocketmq_transport::api::IngressRequestView<'_>) -> RequestOrdering {
        RequestOrdering::Concurrent
    }
}

#[tokio::test]
async fn pop_lite_deferred_different_clients_resume_and_write_in_parallel() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let dispatcher = LiteEventDispatcher::default();
    let service = service(controller.as_ref(), dispatcher.clone());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredTestProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
    };
    let (mut client, running) = start_server(processor, controller).await;

    let clients = [("parallel-a", 201), ("parallel-b", 202)];
    let mut claims = Vec::new();
    for (client_name, opaque) in clients {
        client
            .send_command(request_command_for(client_name, opaque, 60_000))
            .await
            .expect("send parallel PopLite waiter");
        registrations.recv().await.expect("observe parallel registration");
        let client_id = CheetahString::from_string(client_name.to_owned());
        let event = CheetahString::from_string(format!("%LMQ%$parent-topic${client_name}"));
        assert_eq!(
            dispatcher.do_full_dispatch(
                &client_id,
                &CheetahString::from_static_str("group-a"),
                &HashSet::from([event]),
            ),
            1
        );
        claims.push(
            service
                .claim_event(&client_id)
                .await
                .expect("claim parallel PopLite event")
                .expect("parallel client has a waiter"),
        );
    }

    let started = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Semaphore::new(0));
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (receipt_tx, mut receipt_rx) = mpsc::unbounded_channel();
    for claim in claims {
        let service_for_resume = Arc::clone(&service);
        let started_count = Arc::clone(&started);
        let release_handler = Arc::clone(&release);
        let started_sender = started_tx.clone();
        let receipt_sender = receipt_tx.clone();
        running
            .action_context
            .spawn_service("pop-lite-parallel-resume", async move {
                let result = service_for_resume
                    .resume_event_claim(
                        claim,
                        DeferredResumeRetainedSize::new(97),
                        move |_resume, reason, reservation| async move {
                            assert_eq!(reason, DeferredWakeReason::MessageArrived);
                            started_count.fetch_add(1, Ordering::SeqCst);
                            let _ = started_sender.send(());
                            release_handler
                                .acquire()
                                .await
                                .map_err(|_| RocketMQError::illegal_argument("parallel release closed"))?
                                .forget();
                            let batch = reservation.commit();
                            batch.complete(&HashSet::new());
                            RemotingResponse::command(RemotingCommand::create_response_command_with_code(
                                ResponseCode::Success,
                            ))
                            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
                        },
                    )
                    .await;
                let _ = receipt_sender.send(result);
            })
            .expect("spawn parallel PopLite resume");
    }
    drop(started_tx);
    drop(receipt_tx);
    started_rx.recv().await.expect("first parallel handler starts");
    started_rx.recv().await.expect("second parallel handler starts");
    assert_eq!(started.load(Ordering::SeqCst), 2);
    let active = service.resource_snapshot();
    assert_eq!(active.active_client_gates, 2);
    assert_eq!(active.resume_execution_count, 2);
    assert!(active.resume_execution_bytes >= 194);

    release.add_permits(2);
    for _ in 0..2 {
        receipt_rx
            .recv()
            .await
            .expect("parallel receipt")
            .expect("parallel canonical write");
    }
    let mut opaqueness = HashSet::new();
    for _ in 0..2 {
        let response = client
            .receive_command()
            .await
            .expect("parallel connection")
            .expect("parallel response frame");
        opaqueness.insert(response.opaque());
    }
    assert_eq!(opaqueness, HashSet::from([201, 202]));
    assert_terminal_snapshot(&service);
    running.finish().await;
}

#[tokio::test]
async fn pop_lite_deferred_same_client_timeout_is_not_serialized_by_event_gate() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let dispatcher = LiteEventDispatcher::default();
    let service = service(controller.as_ref(), dispatcher.clone());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = OpaqueRegistrationProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
    };
    let (mut client, running) = start_server(processor, controller).await;

    for (opaque, poll_time) in [(204, 10_000), (205, 60_000)] {
        client
            .send_command(request_command_for("same-client-timeout", opaque, poll_time))
            .await
            .expect("send same-client PopLite waiter");
    }
    let mut timeout_id = None;
    for _ in 0..2 {
        let (opaque, id) = registrations.recv().await.expect("observe same-client waiter");
        if opaque == 205 {
            timeout_id = Some(id);
        }
    }
    let timeout_id = timeout_id.expect("map timeout waiter by original opaque");

    let client_id = CheetahString::from_static_str("same-client-timeout");
    dispatcher.do_full_dispatch(
        &client_id,
        &CheetahString::from_static_str("group-a"),
        &HashSet::from([CheetahString::from_static_str("%LMQ%$parent-topic$same-client-timeout")]),
    );
    let event_claim = service
        .claim_event(&client_id)
        .await
        .expect("claim same-client event")
        .expect("event waiter is claimable");

    let event_started = Arc::new(Notify::new());
    let event_release = Arc::new(Notify::new());
    let event_started_for_handler = Arc::clone(&event_started);
    let event_release_for_handler = Arc::clone(&event_release);
    let (event_receipt_tx, event_receipt_rx) = oneshot::channel();
    let service_for_event = Arc::clone(&service);
    running
        .action_context
        .spawn_service("pop-lite-same-client-event", async move {
            let result = service_for_event
                .resume_event_claim(
                    event_claim,
                    DeferredResumeRetainedSize::new(101),
                    move |_resume, reason, reservation| async move {
                        assert_eq!(reason, DeferredWakeReason::MessageArrived);
                        event_started_for_handler.notify_one();
                        event_release_for_handler.notified().await;
                        reservation.commit().complete(&HashSet::new());
                        RemotingResponse::command(RemotingCommand::create_response_command_with_code(
                            ResponseCode::Success,
                        ))
                        .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
                    },
                )
                .await;
            let _ = event_receipt_tx.send(result);
        })
        .expect("spawn same-client event resume");
    event_started.notified().await;
    assert_eq!(service.resource_snapshot().active_client_gates, 1);

    let mut timeout_claim = service
        .registry
        .claim(timeout_id, DeferredWakeReason::Timeout)
        .await
        .expect("claim same-client timeout independently of event gate");
    drop(timeout_claim.resume_data_mut().take_index_lease());
    service
        .resume_claimed(
            timeout_claim,
            DeferredResumeRetainedSize::new(41),
            move |_resume, reason| async move {
                assert_eq!(reason, DeferredWakeReason::Timeout);
                RemotingResponse::command(RemotingCommand::create_response_command_with_code(
                    ResponseCode::PollingTimeout,
                ))
                .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
            },
        )
        .await
        .expect("same-client timeout writes while event remains active");

    let timeout_response = client
        .receive_command()
        .await
        .expect("same-client timeout connection")
        .expect("timeout response precedes blocked event response");
    assert_eq!(timeout_response.opaque(), 205);
    assert_eq!(timeout_response.code(), ResponseCode::PollingTimeout as i32);
    let while_event_is_blocked = service.resource_snapshot();
    assert_eq!(while_event_is_blocked.active_client_gates, 1);
    assert_eq!(while_event_is_blocked.resume_execution_count, 1);

    event_release.notify_one();
    event_receipt_rx
        .await
        .expect("event receipt observer")
        .expect("event response writes after release");
    let event_response = client
        .receive_command()
        .await
        .expect("same-client event connection")
        .expect("event response frame");
    assert_eq!(event_response.opaque(), 204);
    assert_eq!(event_response.code(), ResponseCode::Success as i32);
    assert_terminal_snapshot(&service);
    running.finish().await;
}

#[derive(Clone)]
struct CommitWindowProcessor {
    service: Arc<PopLiteDeferredService>,
    registrations: mpsc::UnboundedSender<DeferredId>,
    release: Arc<Semaphore>,
}

impl RequestProcessor for CommitWindowProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let prepared = self
            .service
            .prepare(request, PopLiteRetainedEstimate::default())
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        let registration = self
            .service
            .register(prepared, request)
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        self.registrations
            .send(registration.deferred_id())
            .map_err(|_| RocketMQError::illegal_argument("commit-window observer closed"))?;
        self.release
            .acquire()
            .await
            .map_err(|_| RocketMQError::illegal_argument("commit-window release closed"))?
            .forget();
        Ok(HandlerOutcome::Deferred(registration))
    }
}

#[tokio::test]
async fn pop_lite_deferred_registration_commit_window_blocks_claim_until_activation() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let dispatcher = LiteEventDispatcher::default();
    let service = service(controller.as_ref(), dispatcher.clone());
    let release = Arc::new(Semaphore::new(0));
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = CommitWindowProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
        release: Arc::clone(&release),
    };
    let (mut client, running) = start_server(processor, controller).await;
    client
        .send_command(request_command_for("commit-window", 203, 60_000))
        .await
        .expect("send commit-window PopLite waiter");
    registrations
        .recv()
        .await
        .expect("registration exists before outcome commit");

    let client_id = CheetahString::from_static_str("commit-window");
    dispatcher.do_full_dispatch(
        &client_id,
        &CheetahString::from_static_str("group-a"),
        &HashSet::from([CheetahString::from_static_str("%LMQ%$parent-topic$commit-window")]),
    );
    let service_for_claim = Arc::clone(&service);
    let claim_client_id = client_id.clone();
    let claim_task = tokio::spawn(async move { service_for_claim.claim_event(&claim_client_id).await });
    tokio::task::yield_now().await;
    assert!(
        !claim_task.is_finished(),
        "claim waits for HandlerOutcome::Deferred commit"
    );

    release.add_permits(1);
    let claim = claim_task
        .await
        .expect("commit-window claim task")
        .expect("commit-window registry claim")
        .expect("activation makes the candidate claimable");
    drop(claim);
    assert_eq!(dispatcher.take_pending_events(&client_id).len(), 1);
    client.shutdown().await.expect("close commit-window session");
    running.finish().await;
}
