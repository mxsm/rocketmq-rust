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

use rocketmq_transport::api::ClaimedDeferred;
use rocketmq_transport::api::DeferredResumeOutcome;
use tokio::sync::Notify;

use super::*;
use crate::long_polling::notification_deferred::service::ResumeNotification;

struct DropSignal(Arc<Notify>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

fn assert_terminal(service: &NotificationDeferredService) {
    let snapshot = service.snapshot();
    assert_eq!(snapshot.admission().waiting_count(), 0);
    assert_eq!(snapshot.admission().retained_bytes(), 0);
    assert_eq!(snapshot.index().live(), 0);
    assert_eq!(snapshot.index().reserved(), 0);
    assert_eq!(snapshot.index().candidates(), 0);
    assert_eq!(snapshot.index().keys(), 0);
    assert_eq!(snapshot.index().oldest_waiter_age_millis(), None);
    assert_eq!(snapshot.prepared(), 0);
    assert_eq!(snapshot.pending_claims(), 0);
    assert_eq!(snapshot.resume_executions(), 0);
    assert_eq!(snapshot.resume_execution_bytes(), 0);
    assert_eq!(snapshot.active_continuations(), 0);
    assert_eq!(snapshot.continuation_bytes(), 0);
}

async fn registered_waiter(
    service: &Arc<NotificationDeferredService>,
    controller: Arc<AdmissionController>,
    opaque: i32,
) -> (Connection, RunningServer) {
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredProcessor {
        service: Arc::clone(service),
        registrations: registration_tx,
        filter: None,
    };
    let (mut client, running) = start_server(processor, controller).await;
    client
        .send_command(request_command_for("GroupA", opaque, 60_000))
        .await
        .expect("send terminal-audit Notification waiter");
    registrations
        .recv()
        .await
        .expect("observe terminal-audit Notification registration");
    (client, running)
}

async fn claim_one(service: &NotificationDeferredService) -> ClaimedDeferred<ResumeNotification> {
    let topic = CheetahString::from_static_str("TopicA");
    let prepared = service.prepare_arrival_batch(NotificationArrivalView::new(&topic, 0), None);
    let (mut claims, cursor) = service.claim_prepared_arrival(prepared).await.into_parts();
    assert!(cursor.is_complete());
    assert_eq!(claims.len(), 1);
    claims.pop().expect("one Notification waiter is claimed")
}

#[tokio::test]
async fn notification_deferred_execution_admission_rejects_before_handler_and_writes_one_error() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref());
    let (mut client, running) = registered_waiter(&service, Arc::clone(&controller), 8_101).await;
    let claim = claim_one(&service).await;
    assert_eq!(service.snapshot().admission().waiting_count(), 1);

    let handler_calls = Arc::new(AtomicUsize::new(0));
    let handler_calls_for_resume = Arc::clone(&handler_calls);
    assert!(matches!(
        service
            .resume_claimed(
                claim,
                DeferredResumeRetainedSize::new(AdmissionLimits::default().queued.bytes),
                move |_resume, _reason| async move {
                    handler_calls_for_resume.fetch_add(1, Ordering::SeqCst);
                    RemotingResponse::command(RemotingCommand::create_response_command_with_code(
                        ResponseCode::Success,
                    ))
                    .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
                },
            )
            .await
            .expect("execution rejection writes one canonical overload response"),
        DeferredResumeOutcome::Completed(_)
    ));

    let response = client
        .receive_command()
        .await
        .expect("execution-reject session remains open")
        .expect("execution rejection emits one response frame");
    assert_eq!(response.opaque(), 8_101);
    assert_eq!(response.code(), ResponseCode::SystemBusy as i32);
    assert_eq!(handler_calls.load(Ordering::SeqCst), 0);
    assert_terminal(&service);
    let admission = controller.snapshot();
    assert_eq!(admission.queued.current_count, 0);
    assert_eq!(admission.inflight.current_count, 0);
    assert_eq!(admission.processors.current_count, 0);
    assert_eq!(admission.queued.rejected_count, 1);

    running.finish().await;
    assert!(
        client.receive_command().await.is_none(),
        "execution admission rejection has one terminal frame"
    );
}

#[tokio::test]
async fn notification_deferred_service_shutdown_drains_registered_candidate_without_handler_or_frame() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref());
    let (mut client, running) = registered_waiter(&service, controller, 8_102).await;
    let topic = CheetahString::from_static_str("TopicA");
    let prepared = service.prepare_arrival_batch(NotificationArrivalView::new(&topic, 0), None);
    assert_eq!(prepared.candidate_count(), 1);
    assert_eq!(service.snapshot().index().candidates(), 1);

    let DeferredRegistryShutdownOutcome::Completed(stats) = service.shutdown() else {
        panic!("terminal audit owns Notification service shutdown");
    };
    assert_eq!(stats.detached_entries(), 1);
    assert_eq!(stats.invariant_failures(), 0);
    let claimed_after_shutdown = service.claim_prepared_arrival(prepared).await;
    assert!(claimed_after_shutdown.into_parts().0.is_empty());
    assert_terminal(&service);

    running.finish().await;
    assert!(
        client.receive_command().await.is_none(),
        "service shutdown emits no Notification frame"
    );
}

#[tokio::test]
async fn notification_deferred_inactive_session_cannot_claim_execute_or_write() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref());
    let (mut client, running) = registered_waiter(&service, controller, 8_103).await;
    client.shutdown().await.expect("close inactive Notification session");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = service.snapshot();
            if snapshot.admission().waiting_count() == 0 && snapshot.index().live() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("inactive session cleanup drains Notification waiter");

    let topic = CheetahString::from_static_str("TopicA");
    let prepared = service.prepare_arrival_batch(NotificationArrivalView::new(&topic, 0), None);
    assert_eq!(prepared.candidate_count(), 0);
    assert!(service.claim_prepared_arrival(prepared).await.into_parts().0.is_empty());
    assert_terminal(&service);

    running.finish().await;
    assert!(client.receive_command().await.is_none());
}

#[tokio::test]
async fn notification_deferred_service_shutdown_stops_accepted_handler_without_a_frame() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref());
    let (mut client, running) = registered_waiter(&service, controller, 8_104).await;
    let claim = claim_one(&service).await;
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let dropped = Arc::new(Notify::new());
    let calls_for_handler = Arc::clone(&handler_calls);
    let started_for_handler = Arc::clone(&started);
    let release_for_handler = Arc::clone(&release);
    let dropped_for_handler = Arc::clone(&dropped);
    let service_for_resume = Arc::clone(&service);
    let (receipt_tx, receipt_rx) = oneshot::channel();
    running
        .action_context
        .spawn_service("notification-terminal-service-shutdown", async move {
            let result = service_for_resume
                .resume_claimed(
                    claim,
                    DeferredResumeRetainedSize::new(257),
                    move |_resume, _reason| async move {
                        calls_for_handler.fetch_add(1, Ordering::SeqCst);
                        let _drop_signal = DropSignal(dropped_for_handler);
                        started_for_handler.notify_one();
                        release_for_handler.notified().await;
                        RemotingResponse::command(RemotingCommand::create_response_command_with_code(
                            ResponseCode::Success,
                        ))
                        .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
                    },
                )
                .await;
            let _ = receipt_tx.send(result);
        })
        .expect("spawn accepted Notification resume");
    started.notified().await;
    let active = service.snapshot();
    assert_eq!(active.admission().waiting_count(), 0);
    assert_eq!(active.resume_executions(), 1);
    assert_eq!(active.resume_execution_bytes(), 257);

    assert!(matches!(
        service.shutdown(),
        DeferredRegistryShutdownOutcome::Completed(_)
    ));
    release.notify_one();
    dropped.notified().await;
    let outcome = receipt_rx
        .await
        .expect("service-shutdown receipt channel")
        .expect("service shutdown is a normal deferred resume outcome");
    assert!(matches!(outcome, DeferredResumeOutcome::Cancelled));
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);
    assert_terminal(&service);

    running.finish().await;
    assert!(
        client.receive_command().await.is_none(),
        "service shutdown cancels the accepted handler before write"
    );
}

#[tokio::test]
async fn notification_deferred_parent_cancel_stops_accepted_handler_without_a_frame() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref());
    let (mut client, mut running) = registered_waiter(&service, controller, 8_105).await;
    let claim = claim_one(&service).await;
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let dropped = Arc::new(Notify::new());
    let calls_for_handler = Arc::clone(&handler_calls);
    let started_for_handler = Arc::clone(&started);
    let release_for_handler = Arc::clone(&release);
    let dropped_for_handler = Arc::clone(&dropped);
    let service_for_resume = Arc::clone(&service);
    let (receipt_tx, receipt_rx) = oneshot::channel();
    running
        .action_context
        .spawn_service("notification-terminal-parent-cancel", async move {
            let result = service_for_resume
                .resume_claimed(
                    claim,
                    DeferredResumeRetainedSize::new(193),
                    move |_resume, _reason| async move {
                        calls_for_handler.fetch_add(1, Ordering::SeqCst);
                        let _drop_signal = DropSignal(dropped_for_handler);
                        started_for_handler.notify_one();
                        release_for_handler.notified().await;
                        RemotingResponse::command(RemotingCommand::create_response_command_with_code(
                            ResponseCode::Success,
                        ))
                        .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
                    },
                )
                .await;
            let _ = receipt_tx.send(result);
        })
        .expect("spawn parent-cancelled Notification resume");
    started.notified().await;
    assert_eq!(service.snapshot().admission().waiting_count(), 0);
    assert_eq!(service.snapshot().resume_executions(), 1);

    running.begin_shutdown();
    dropped.notified().await;
    let outcome = receipt_rx
        .await
        .expect("parent-cancel receipt channel")
        .expect("parent cancellation is a normal deferred resume outcome");
    assert!(matches!(outcome, DeferredResumeOutcome::Cancelled));
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);
    assert_terminal(&service);
    drop(release);

    running.finish().await;
    assert!(
        client.receive_command().await.is_none(),
        "parent cancellation drains without a Notification frame"
    );
}
