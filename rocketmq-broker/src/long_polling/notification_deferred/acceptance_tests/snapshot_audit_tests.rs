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

#[tokio::test]
async fn notification_deferred_snapshot_tracks_accepted_resume_count_and_bytes_until_terminal() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
        filter: None,
    };
    let (mut client, running) = start_server(processor, controller).await;
    client
        .send_command(request_command_for("GroupA", 7_001, 60_000))
        .await
        .expect("send observed Notification waiter");
    registrations.recv().await.expect("observe Notification registration");

    let registered = service.snapshot();
    assert_eq!(registered.admission().waiting_count(), 1);
    assert!(registered.admission().retained_bytes() > 0);
    assert_eq!(registered.index().live(), 1);
    assert_eq!(registered.index().reserved(), 0);
    assert_eq!(registered.index().candidates(), 0);
    assert_eq!(registered.index().keys(), 1);
    assert!(registered.index().oldest_waiter_age_millis().is_some());
    let manual_now = tokio::time::Instant::now();
    let initial_age = service
        .index
        .snapshot_at_for_test(manual_now)
        .oldest_waiter_age_millis()
        .expect("registered waiter has an age");
    let normalized_age = |now| {
        service
            .index
            .snapshot_at_for_test(now)
            .oldest_waiter_age_millis()
            .expect("registered waiter remains indexed")
            .checked_sub(initial_age)
            .expect("manual observation cannot move backwards")
    };
    assert_eq!(normalized_age(manual_now), 0);
    assert_eq!(
        normalized_age(manual_now + Duration::from_millis(7)),
        7,
        "the manual monotonic clock reports the exact positive registered-age delta"
    );

    let topic = CheetahString::from_static_str("TopicA");
    let prepared = service.prepare_arrival_batch(NotificationArrivalView::new(&topic, 0), None);
    let (mut claims, cursor) = service.claim_prepared_arrival(prepared).await.into_parts();
    assert!(cursor.is_complete());
    let claim = claims.pop().expect("claim observed Notification waiter");
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let started_in_handler = Arc::clone(&started);
    let release_in_handler = Arc::clone(&release);
    let service_for_resume = Arc::clone(&service);
    let (receipt_tx, receipt_rx) = oneshot::channel();
    running
        .action_context
        .spawn_service("notification-deferred-observed-resume", async move {
            let result = service_for_resume
                .resume_claimed(
                    claim,
                    DeferredResumeRetainedSize::new(321),
                    move |_resume, reason| async move {
                        assert_eq!(reason, DeferredWakeReason::MessageArrived);
                        started_in_handler.notify_one();
                        release_in_handler.notified().await;
                        let head = application_remoting_command_factory().create_success_response_command_with_header(
                            NotificationResponseHeader {
                                has_msg: false,
                                polling_full: false,
                            },
                        );
                        ResponsePlan::command(head).map_err(|error| RocketMQError::illegal_argument(error.to_string()))
                    },
                )
                .await;
            let _ = receipt_tx.send(result);
        })
        .expect("spawn observed Notification resume");

    started.notified().await;
    let active = service.snapshot();
    assert_eq!(active.resume_executions(), 1);
    assert_eq!(active.resume_execution_bytes(), 321);
    assert_eq!(
        active.admission().waiting_count(),
        0,
        "wait permit precedes execution admission release"
    );
    assert_eq!(active.index().live(), 0);

    release.notify_one();
    receipt_rx
        .await
        .expect("observed resume receipt channel")
        .expect("observed resume writes canonically");
    let response = client
        .receive_command()
        .await
        .expect("observed resume connection")
        .expect("observed resume frame");
    assert_eq!(response.opaque(), 7_001);
    let terminal = service.snapshot();
    assert_eq!(terminal.admission().waiting_count(), 0);
    assert_eq!(terminal.admission().retained_bytes(), 0);
    assert_eq!(terminal.index().live(), 0);
    assert_eq!(terminal.index().reserved(), 0);
    assert_eq!(terminal.index().candidates(), 0);
    assert_eq!(terminal.index().keys(), 0);
    assert_eq!(terminal.index().oldest_waiter_age_millis(), None);
    assert_eq!(terminal.resume_executions(), 0);
    assert_eq!(terminal.resume_execution_bytes(), 0);
    assert_eq!(terminal.pending_claims(), 0);
    assert_eq!(terminal.prepared(), 0);
    assert_eq!(terminal.active_continuations(), 0);
    assert_eq!(terminal.continuation_bytes(), 0);

    running.finish().await;
}
