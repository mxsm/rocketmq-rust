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

fn processor(
    service: &Arc<PullDeferredService>,
    registrations: mpsc::UnboundedSender<RegistrationObservation>,
    barrier: &Arc<ProcessorBarrier>,
    hold_before_outcome: bool,
    rollback_registration: bool,
) -> PullDeferredTestProcessor {
    PullDeferredTestProcessor {
        service: Arc::clone(service),
        registrations,
        barrier: Arc::clone(barrier),
        hold_before_outcome,
        rollback_registration,
    }
}

#[tokio::test]
async fn message_arrival_and_timeout_have_exactly_one_claim_winner() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref());
    let barrier = Arc::new(ProcessorBarrier::default());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let (mut client, _address, running) = start_server(
        processor(&service, registration_tx, &barrier, false, false),
        Arc::clone(&controller),
    )
    .await;
    client
        .send_command(request_command(71))
        .await
        .expect("send claim race waiter");
    let registration = registrations.recv().await.expect("claim race registration");
    commit_barrier(&mut client, &barrier, 72).await;

    let (arrival, timeout) = tokio::join!(
        service.claim(registration.id, DeferredWakeReason::MessageArrived),
        service.claim(registration.id, DeferredWakeReason::Timeout),
    );
    match (arrival, timeout) {
        (Ok(DeferredClaimOutcome::Claimed(winner)), Ok(DeferredClaimOutcome::AlreadyClaimed)) => {
            assert_eq!(winner.reason(), DeferredWakeReason::MessageArrived);
            drop(winner);
        }
        (Ok(DeferredClaimOutcome::AlreadyClaimed), Ok(DeferredClaimOutcome::Claimed(winner))) => {
            assert_eq!(winner.reason(), DeferredWakeReason::Timeout);
            drop(winner);
        }
        _ => panic!("exactly one concurrent reason must retain the Pull claim"),
    }
    assert_released(&service);
    running.finish().await;
}

#[tokio::test]
async fn stale_arrival_bounded_continuation_claims_every_matching_waiter() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref());
    let barrier = Arc::new(ProcessorBarrier::default());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let (mut client, _address, running) = start_server(
        processor(&service, registration_tx, &barrier, false, false),
        Arc::clone(&controller),
    )
    .await;
    for opaque in [81, 82, 83] {
        client
            .send_command(request_command(opaque))
            .await
            .expect("send all-match Pull waiter");
        registrations.recv().await.expect("all-match Pull registration");
    }
    commit_barrier(&mut client, &barrier, 84).await;

    let topic = CheetahString::from_static_str("TopicA");
    let refreshes = AtomicUsize::new(0);
    let mut submitted = Vec::new();
    let stats = service
        .produce_arrival(
            PullArrivalView::new(&topic, 0, 7),
            || {
                refreshes.fetch_add(1, Ordering::SeqCst);
                Ok::<_, ()>(8)
            },
            |batch| {
                submitted.push(batch);
                Ok::<_, ()>(())
            },
        )
        .expect("bounded Pull arrival producer");
    assert_eq!(refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(stats.inspected(), 3);
    assert_eq!(stats.candidates(), 3);
    assert_eq!(stats.batches(), 3);
    assert_eq!(service.index_snapshot().candidates(), 3);

    let mut claims = Vec::new();
    for candidate in submitted.into_iter().flatten() {
        let DeferredClaimOutcome::Claimed(claim) = service
            .claim_candidate(candidate, DeferredWakeReason::MessageArrived)
            .await
            .expect("each affine candidate has a normal claim outcome")
        else {
            panic!("each affine candidate must retain its claimed Pull request");
        };
        claims.push(claim);
    }
    assert_eq!(claims.len(), 3);
    assert_eq!(service.index_snapshot(), PullIndexSnapshot::default());
    drop(claims);
    assert_released(&service);
    running.finish().await;
}

#[tokio::test]
async fn producer_rejection_restores_candidate_without_spinning() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref());
    let barrier = Arc::new(ProcessorBarrier::default());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let (mut client, _address, running) = start_server(
        processor(&service, registration_tx, &barrier, false, false),
        Arc::clone(&controller),
    )
    .await;
    client
        .send_command(request_command(91))
        .await
        .expect("send producer rejection waiter");
    let registration = registrations.recv().await.expect("producer rejection registration");
    commit_barrier(&mut client, &barrier, 92).await;

    let topic = CheetahString::from_static_str("TopicA");
    let mut calls = 0;
    let result = service.produce_arrival(
        PullArrivalView::new(&topic, 0, 8),
        || Ok::<_, &'static str>(8),
        |_batch| {
            calls += 1;
            Err("executor sealed")
        },
    );
    assert_eq!(result, Err("executor sealed"));
    assert_eq!(calls, 1);
    assert_eq!(service.index_snapshot().live(), 1);
    assert_eq!(service.index_snapshot().candidates(), 0);
    let DeferredClaimOutcome::Claimed(claim) = service
        .claim(registration.id, DeferredWakeReason::ForcedRefresh)
        .await
        .expect("restored waiter has a normal claim outcome")
    else {
        panic!("restored waiter must retain its claimed Pull request");
    };
    drop(claim);
    assert_released(&service);
    running.finish().await;
}

#[tokio::test]
async fn shutdown_before_dispatch_commit_releases_every_affine_owner() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref());
    let barrier = Arc::new(ProcessorBarrier::default());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let (mut client, _address, running) = start_server(
        processor(&service, registration_tx, &barrier, true, false),
        Arc::clone(&controller),
    )
    .await;
    client
        .send_command(request_command(101))
        .await
        .expect("send shutdown-before-commit waiter");
    registrations.recv().await.expect("shutdown-before-commit registration");
    barrier.before_outcome.notified().await;
    assert_eq!(service.index_snapshot().live(), 1);
    assert!(matches!(
        service.shutdown(),
        rocketmq_transport::api::DeferredRegistryShutdownOutcome::Completed(_)
    ));
    assert_released(&service);
    barrier.release_outcome.notify_one();
    running.finish().await;
    assert!(client.receive_command().await.is_none(), "shutdown emits no Pull frame");
}

#[tokio::test]
async fn session_close_and_registration_rollback_release_index_wait_and_bytes() {
    for (opaque, rollback) in [(111, false), (112, true)] {
        let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
        let service = service(controller.as_ref());
        let barrier = Arc::new(ProcessorBarrier::default());
        let (registration_tx, mut registrations) = mpsc::unbounded_channel();
        let (mut client, _address, running) = start_server(
            processor(&service, registration_tx, &barrier, false, rollback),
            Arc::clone(&controller),
        )
        .await;
        client
            .send_command(request_command(opaque))
            .await
            .expect("send session-close/rollback waiter");
        registrations.recv().await.expect("session-close/rollback registration");
        if !rollback {
            commit_barrier(&mut client, &barrier, opaque + 10).await;
            client.shutdown().await.expect("close Pull client session");
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while service.admission_snapshot().waiting_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Pull cleanup releases wait admission");
        assert_released(&service);
        running.finish().await;
        assert!(client.receive_command().await.is_none(), "cleanup emits no Pull frame");
    }
}
