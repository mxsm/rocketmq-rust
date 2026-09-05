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

use rocketmq_transport::api::DeferredClaimOutcome;

use super::*;

#[tokio::test]
async fn lifecycle_owned_sweeper_claims_protocol_timeout_and_releases_index() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref(), 2, 2, 2);
    let barrier = Arc::new(ProcessorBarrier::default());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredTestProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
        barrier: Arc::clone(&barrier),
        hold_before_outcome: false,
        rollback_registration: false,
    };
    let (mut client, _address, running) = start_server(processor, Arc::clone(&controller)).await;
    client
        .send_command(expiring_request_command("TopicA", "GroupA", 0, 61))
        .await
        .expect("send expiring POP waiter");
    let registered = registrations.recv().await.expect("expiring registration");
    commit_barrier(&mut client, &barrier, 62).await;
    assert!(service.index_contains(registered.id));

    let (claims_tx, mut claims_rx) = mpsc::unbounded_channel();
    service
        .start_sweeper(
            running.action_context.task_group(),
            NonZeroU64::new(5).expect("non-zero sweep interval"),
            move |claims| {
                let claims_tx = claims_tx.clone();
                async move {
                    let _ = claims_tx.send(claims);
                }
            },
        )
        .expect("start lifecycle-owned POP sweeper");
    let mut claims = tokio::time::timeout(Duration::from_secs(2), claims_rx.recv())
        .await
        .expect("owned sweeper callback")
        .expect("owned sweeper claim batch");
    assert_eq!(claims.len(), 1);
    let claim = claims.pop().expect("one timeout claim");
    assert_eq!(claim.deferred_id(), registered.id);
    assert_eq!(claim.reason(), DeferredWakeReason::Timeout);
    drop(claim);
    assert_released(&service);

    running.finish().await;
}

#[test]
fn index_and_wait_admission_fail_before_responder_transfer_and_release_every_reservation() {
    let index_controller = AdmissionController::new(AdmissionLimits::default());
    let index_limited = service(&index_controller, 2, 1, 1);
    let first = index_limited
        .prepare_at(
            preflight_test_data("TopicA", "127.0.0.1:1"),
            None,
            None,
            PopRetainedEstimate::default(),
            10_000,
            tokio::time::Instant::now(),
        )
        .expect("first index reservation");
    let Err(index_error) = index_limited.prepare_at(
        preflight_test_data("TopicB", "127.0.0.1:2"),
        None,
        None,
        PopRetainedEstimate::default(),
        10_000,
        tokio::time::Instant::now(),
    ) else {
        panic!("global POP index capacity must reject before responder transfer");
    };
    assert_eq!(index_error.kind(), PopDeferredPrepareErrorKind::Index);
    drop(first);
    assert_released(&index_limited);

    let admission_controller = AdmissionController::new(AdmissionLimits::default());
    let admission_limited = service(&admission_controller, 1, 2, 2);
    let first = admission_limited
        .prepare_at(
            preflight_test_data("TopicA", "127.0.0.1:3"),
            None,
            None,
            PopRetainedEstimate::default(),
            10_000,
            tokio::time::Instant::now(),
        )
        .expect("first wait permit");
    let Err(admission_error) = admission_limited.prepare_at(
        preflight_test_data("TopicB", "127.0.0.1:4"),
        None,
        None,
        PopRetainedEstimate::default(),
        10_000,
        tokio::time::Instant::now(),
    ) else {
        panic!("independent wait admission must reject before responder transfer");
    };
    assert_eq!(admission_error.kind(), PopDeferredPrepareErrorKind::Admission);
    assert_eq!(admission_limited.index_snapshot().reserved(), 1);
    drop(first);
    assert_released(&admission_limited);
}

#[tokio::test]
async fn dropped_registration_rolls_back_registry_index_lease_and_wait_permit() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref(), 2, 2, 2);
    let barrier = Arc::new(ProcessorBarrier::default());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredTestProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
        barrier,
        hold_before_outcome: false,
        rollback_registration: true,
    };
    let (mut client, _address, running) = start_server(processor, Arc::clone(&controller)).await;
    client
        .send_command(request_command("TopicA", "GroupA", 0, None, 21))
        .await
        .expect("send rollback registration");
    let rolled_back = registrations.recv().await.expect("observe provisional registration");
    tokio::time::timeout(Duration::from_secs(2), async {
        while service.admission_snapshot().waiting_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("registration rollback releases every owner");
    assert_released(&service);
    let claim = service
        .claim(rolled_back.id, DeferredWakeReason::MessageArrived)
        .await
        .expect("rolled-back identity is a normal lifecycle outcome");
    assert!(matches!(claim, DeferredClaimOutcome::NotFound));
    running.finish().await;
}

#[tokio::test]
async fn shutdown_between_registration_and_dispatch_commit_cleans_every_owner() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref(), 2, 2, 2);
    let barrier = Arc::new(ProcessorBarrier::default());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredTestProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
        barrier: Arc::clone(&barrier),
        hold_before_outcome: true,
        rollback_registration: false,
    };
    let (mut client, _address, running) = start_server(processor, Arc::clone(&controller)).await;
    client
        .send_command(request_command("TopicA", "GroupA", 0, None, 71))
        .await
        .expect("send register-shutdown race waiter");
    let registered = registrations.recv().await.expect("race registration");
    barrier.before_outcome.notified().await;
    assert!(service.index_contains(registered.id));

    assert!(matches!(
        service.shutdown(),
        rocketmq_transport::api::DeferredRegistryShutdownOutcome::Completed(_)
    ));
    assert_released(&service);
    barrier.release_outcome.notify_one();

    running.finish().await;
    assert!(
        client.receive_command().await.is_none(),
        "shutdown before commit emits no deferred response frame"
    );
}

#[tokio::test]
async fn shutdown_terminalizes_active_waiter_emits_no_frame_and_cannot_reopen() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref(), 2, 2, 2);
    let barrier = Arc::new(ProcessorBarrier::default());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredTestProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
        barrier: Arc::clone(&barrier),
        hold_before_outcome: false,
        rollback_registration: false,
    };
    let (mut client, _address, running) = start_server(processor, Arc::clone(&controller)).await;
    client
        .send_command(request_command("TopicA", "GroupA", 0, None, 31))
        .await
        .expect("send shutdown waiter");
    let registered = registrations.recv().await.expect("shutdown registration");
    commit_barrier(&mut client, &barrier, 32).await;
    assert!(service.index_contains(registered.id));

    let outcome = service.shutdown();
    assert!(matches!(
        outcome,
        rocketmq_transport::api::DeferredRegistryShutdownOutcome::Completed(_)
    ));
    assert_released(&service);
    let Err(reopen) = service.prepare_at(
        preflight_test_data("TopicB", "127.0.0.1:5"),
        None,
        None,
        PopRetainedEstimate::default(),
        10_000,
        tokio::time::Instant::now(),
    ) else {
        panic!("sealed POP service must reject new preparation before responder transfer");
    };
    assert_eq!(reopen.kind(), PopDeferredPrepareErrorKind::ServiceClosed);
    assert_released(&service);

    running.finish().await;
    assert!(
        client.receive_command().await.is_none(),
        "service-stop cleanup emits no meaningless POP response frame"
    );
}
