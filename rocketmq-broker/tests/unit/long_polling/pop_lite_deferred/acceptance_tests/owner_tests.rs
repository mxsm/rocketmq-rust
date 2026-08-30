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

use std::collections::HashSet;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_transport::api::AdmissionController;
use rocketmq_transport::api::AdmissionLimits;
use rocketmq_transport::api::DeferredResumeRetainedSize;
use rocketmq_transport::api::DeferredWakeReason;
use rocketmq_transport::api::RemotingResponse;
use tokio::sync::mpsc;

use super::request_command;
use super::service;
use super::start_server;
use super::CountingBodyOwner;
use super::DeferredTestProcessor;
use super::ORIGINAL_OPAQUE;

#[tokio::test]
async fn pop_lite_deferred_execution_admission_rejects_before_processor_and_writes_one_error() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let dispatcher = crate::lite::lite_event_dispatcher::LiteEventDispatcher::default();
    let service = service(controller.as_ref(), dispatcher.clone());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredTestProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
    };
    let (mut client, running) = start_server(processor, Arc::clone(&controller)).await;
    client
        .send_command(request_command())
        .await
        .expect("send execution-reject PopLite request");
    registrations
        .recv()
        .await
        .expect("observe execution-reject registration");

    let client_id = CheetahString::from_static_str("client-a");
    let event = CheetahString::from_static_str("%LMQ%$parent-topic$child-a");
    dispatcher.do_full_dispatch(
        &client_id,
        &CheetahString::from_static_str("group-a"),
        &HashSet::from([event.clone()]),
    );
    let claim = service
        .claim_event(&client_id)
        .await
        .expect("execution-reject event claim")
        .expect("execution-reject waiter is claimable");
    let executions = Arc::new(AtomicUsize::new(0));
    let executions_for_handler = Arc::clone(&executions);
    service
        .resume_event_claim(
            claim,
            DeferredResumeRetainedSize::new(AdmissionLimits::default().queued.bytes),
            move |_resume, _reason, reservation| async move {
                executions_for_handler.fetch_add(1, Ordering::SeqCst);
                let batch = reservation.commit();
                batch.complete(&HashSet::new());
                RemotingResponse::command(RemotingCommand::create_response_command_with_code(
                    ResponseCode::Success,
                ))
                .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
            },
        )
        .await
        .expect("bounded execution rejection writes its canonical error response");

    let response = client
        .receive_command()
        .await
        .expect("execution-reject connection remains open")
        .expect("one execution-reject response frame");
    assert_eq!(response.opaque(), ORIGINAL_OPAQUE);
    assert_eq!(response.code(), ResponseCode::SystemBusy as i32);
    assert_eq!(executions.load(Ordering::SeqCst), 0);
    assert_eq!(dispatcher.pending_events(&client_id), vec![event.clone()]);
    assert_eq!(dispatcher.budget_snapshot().current_count, 1);
    let snapshot = service.resource_snapshot();
    assert_eq!(snapshot.active_client_gates, 0);
    assert_eq!(snapshot.accepted_resumes, 0);
    assert_eq!(snapshot.resume_execution_count, 0);
    assert_eq!(snapshot.resume_execution_bytes, 0);
    let admission = controller.snapshot();
    assert_eq!(admission.queued.current_count, 0);
    assert_eq!(admission.inflight.current_count, 0);
    assert_eq!(admission.processors.current_count, 0);
    assert_eq!(admission.queued.rejected_count, 1);

    assert_eq!(dispatcher.take_pending_events(&client_id), vec![event]);
    running.finish().await;
    assert!(
        client.receive_command().await.is_none(),
        "admission rejection emits exactly one frame"
    );
}

#[tokio::test]
async fn pop_lite_deferred_handler_failure_drops_body_owner_once_and_rolls_back_event() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let dispatcher = crate::lite::lite_event_dispatcher::LiteEventDispatcher::default();
    let service = service(controller.as_ref(), dispatcher.clone());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredTestProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
    };
    let (mut client, running) = start_server(processor, controller).await;
    client
        .send_command(request_command())
        .await
        .expect("send owner-failure PopLite request");
    registrations.recv().await.expect("observe owner-failure registration");

    let client_id = CheetahString::from_static_str("client-a");
    let event = CheetahString::from_static_str("%LMQ%$parent-topic$child-a");
    dispatcher.do_full_dispatch(
        &client_id,
        &CheetahString::from_static_str("group-a"),
        &HashSet::from([event.clone()]),
    );
    let claim = service
        .claim_event(&client_id)
        .await
        .expect("owner-failure event claim")
        .expect("owner-failure waiter is claimable");
    let owner_drops = Arc::new(AtomicUsize::new(0));
    let owner_drops_for_handler = Arc::clone(&owner_drops);
    service
        .resume_event_claim(
            claim,
            DeferredResumeRetainedSize::default(),
            move |_resume, reason, _reservation| async move {
                assert_eq!(reason, DeferredWakeReason::MessageArrived);
                let _body = Bytes::from_owner(CountingBodyOwner {
                    body: b"owner-backed-pop-lite-failure".to_vec(),
                    drops: owner_drops_for_handler,
                });
                Err::<RemotingResponse, _>(RocketMQError::illegal_argument("PopLite owner failure"))
            },
        )
        .await
        .expect("handler failure writes its canonical typed error response");

    let response = client
        .receive_command()
        .await
        .expect("owner-failure connection remains open")
        .expect("one owner-failure response frame");
    assert_eq!(response.opaque(), ORIGINAL_OPAQUE);
    assert_eq!(response.code(), ResponseCode::InvalidParameter as i32);
    assert_eq!(owner_drops.load(Ordering::SeqCst), 1);
    assert_eq!(dispatcher.pending_events(&client_id), vec![event.clone()]);
    assert_eq!(dispatcher.budget_snapshot().current_count, 1);
    let snapshot = service.resource_snapshot();
    assert_eq!(snapshot.active_client_gates, 0);
    assert_eq!(snapshot.accepted_resumes, 0);
    assert_eq!(snapshot.resume_execution_count, 0);
    assert_eq!(snapshot.resume_execution_bytes, 0);

    assert_eq!(dispatcher.take_pending_events(&client_id), vec![event]);
    running.finish().await;
    assert!(
        client.receive_command().await.is_none(),
        "handler failure emits exactly one frame"
    );
}
