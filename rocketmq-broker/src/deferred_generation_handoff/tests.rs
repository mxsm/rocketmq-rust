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

use std::cell::RefCell;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Barrier;
use std::time::Duration;

use super::*;

fn string(value: &str) -> CheetahString {
    CheetahString::from_slice(value)
}

fn pop_target() -> DeferredGenerationTarget {
    DeferredGenerationTarget::pop(string("topic"), string("group"), 3)
}

fn enroll(handoff: &DeferredGenerationHandoff, target: DeferredGenerationTarget) -> LegacyWaitLease {
    handoff
        .arrival_adapter()
        .enroll_legacy_wait(target, || Ok::<_, ()>(()))
        .expect("legacy enrollment should succeed")
        .1
}

fn publish_new(handoff: &DeferredGenerationHandoff) {
    let mut transaction = handoff.cutover_transaction().expect("cutover transaction");
    transaction.seal_legacy_acceptance().expect("seal legacy acceptance");
    transaction
        .publish_v2_aggregate(DeferredGenerationV2Publisher::nonblocking_atomic(|| Ok::<_, ()>(())))
        .expect("publish V2 aggregate");
    transaction.publish_default_new().expect("publish New default");
}

#[test]
fn registered_waiter_and_route_permit_have_distinct_lifetimes() {
    let handoff = DeferredGenerationHandoff::new();
    let target = pop_target();
    let wait = enroll(&handoff, target.clone());
    let permit = handoff.acquire_route(target.clone()).expect("route permit");
    let snapshot = handoff.snapshot();
    assert_eq!(snapshot.occupancy, 1);
    assert_eq!(snapshot.candidates, 1);
    assert_eq!(permit.generation(), DeferredGeneration::Legacy);

    let wake = wait.begin_wake(permit).expect("wake should begin");
    let snapshot = handoff.snapshot();
    assert_eq!(snapshot.occupancy, 0);
    assert_eq!(snapshot.candidates, 0);
    assert_eq!(snapshot.active_wakes, 1);
    assert_eq!(snapshot.wake_gates, 1);
    let continuation = wake.into_continuation();
    assert_eq!(handoff.snapshot().continuations, 1);
    drop(continuation);
    assert!(handoff.zero_report().is_zero());
}

#[test]
fn node_publication_and_shutdown_seal_are_serialized_by_one_gate() {
    let handoff = Arc::new(DeferredGenerationHandoff::new());
    let target = pop_target();
    let slot = Arc::new(LegacyWaitHandoff::default());
    let (installed_tx, installed_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    let enrollment_handoff = Arc::clone(&handoff);
    let enrollment_target = target.clone();
    let enrollment_slot = Arc::clone(&slot);
    let rollback_slot = Arc::clone(&slot);
    let enrollment = std::thread::spawn(move || {
        enrollment_handoff
            .arrival_adapter()
            .install_legacy_wait(
                enrollment_target.clone(),
                |lease| {
                    enrollment_slot
                        .install(&enrollment_target, lease)
                        .map_err(|lease| ((), lease))?;
                    installed_tx.send(()).expect("signal installed node");
                    release_rx.recv().expect("release atomic publication");
                    Ok(())
                },
                move || rollback_slot.release(),
            )
            .expect("enrollment wins the gate")
    });

    installed_rx.recv().expect("node installed while gate is held");
    let sealing_handoff = Arc::clone(&handoff);
    let sealing = std::thread::spawn(move || sealing_handoff.seal());
    assert!(!sealing.is_finished(), "seal must wait for node publication");
    release_tx.send(()).expect("finish publication");
    enrollment.join().expect("enrollment thread");
    assert_eq!(sealing.join().expect("sealing thread"), DeferredGenerationSeal::Sealed);

    let snapshot = handoff.snapshot();
    assert!(snapshot.sealed);
    assert_eq!(snapshot.occupancy, 1);
    slot.release();
    assert!(handoff.zero_report().is_zero());
}

#[test]
fn panicking_legacy_install_rolls_back_before_resuming_unwind() {
    let handoff = Arc::new(DeferredGenerationHandoff::new());
    let target = pop_target();
    let slot = Arc::new(LegacyWaitHandoff::default());
    let barrier = Arc::new(Barrier::new(2));
    let (done_tx, done_rx) = mpsc::channel();
    let worker_handoff = Arc::clone(&handoff);
    let worker_barrier = Arc::clone(&barrier);
    let worker_slot = Arc::clone(&slot);
    let rollback_slot = Arc::clone(&slot);
    let worker = std::thread::spawn(move || {
        let panic = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _ = worker_handoff.arrival_adapter().install_legacy_wait(
                target.clone(),
                |provisional_lease| -> Result<(), ((), LegacyWaitLease)> {
                    worker_slot
                        .install(&target, provisional_lease)
                        .map_err(|lease| ((), lease))?;
                    worker_barrier.wait();
                    panic!("installer panic after receiving provisional lease");
                },
                move || rollback_slot.release(),
            );
        }));
        done_tx.send(panic.is_err()).expect("report panic completion");
    });

    barrier.wait();
    assert!(
        done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("panic must not self-deadlock"),
        "installer panic must resume after rollback"
    );
    worker.join().expect("panic is contained by worker");
    assert!(
        slot.target().is_none(),
        "rollback must remove the half-published table token"
    );
    assert!(handoff.zero_report().is_zero());
}

#[test]
fn panicking_legacy_claim_releases_gate_before_resuming_unwind() {
    let handoff = Arc::new(DeferredGenerationHandoff::new());
    let target = pop_target();
    let table = Arc::new(parking_lot::Mutex::new(vec![enroll(&handoff, target.clone())]));
    let barrier = Arc::new(Barrier::new(2));
    let (done_tx, done_rx) = mpsc::channel();
    let worker_handoff = Arc::clone(&handoff);
    let worker_barrier = Arc::clone(&barrier);
    let worker_table = Arc::clone(&table);
    let rollback_table = Arc::clone(&table);
    let worker = std::thread::spawn(move || {
        let panic = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _ = worker_handoff.arrival_adapter().claim_legacy_table::<LegacyWaitLease>(
                target,
                |entries| {
                    let lease = worker_table.lock().pop().expect("remove table entry before panic");
                    entries.push(lease);
                    worker_barrier.wait();
                    panic!("claim panic before publication");
                },
                move |entries| {
                    rollback_table.lock().extend(entries);
                },
            );
        }));
        done_tx.send(panic.is_err()).expect("report panic completion");
    });

    barrier.wait();
    assert!(
        done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("panic must not retain coordinator gate"),
        "claim panic must resume outside the gate"
    );
    worker.join().expect("panic is contained by worker");
    assert_eq!(
        table.lock().len(),
        1,
        "panic rollback must restore removed affine entries"
    );
    drop(table.lock().pop());
    assert!(handoff.zero_report().is_zero());
}

#[test]
fn releasing_one_session_target_does_not_clear_another() {
    let handoff = DeferredGenerationHandoff::new();
    let first_target = DeferredGenerationTarget::pull(string("first-topic"), 1);
    let second_target = DeferredGenerationTarget::pull(string("second-topic"), 2);
    let first = enroll(&handoff, first_target.clone());
    let second = enroll(&handoff, second_target.clone());

    drop(first);
    let snapshot = handoff.snapshot();
    assert_eq!(snapshot.occupancy, 1);
    assert_eq!(snapshot.tracked_targets, 1);
    assert_eq!(snapshot.targets[0].target, second_target);
    assert_ne!(snapshot.targets[0].target, first_target);

    drop(second);
    assert!(handoff.zero_report().is_zero());
}

#[test]
fn quiescent_route_targets_do_not_accumulate_history() {
    let handoff = DeferredGenerationHandoff::new();
    for client in 0..32 {
        drop(
            handoff
                .acquire_route(DeferredGenerationTarget::pop_lite(string(&format!("client-{client}"))))
                .expect("route permit"),
        );
    }
    assert!(handoff.zero_report().is_zero());
}

#[test]
fn only_pop_lite_has_per_target_single_flight() {
    let handoff = DeferredGenerationHandoff::new();
    let lite = DeferredGenerationTarget::pop_lite(string("client"));
    let first = enroll(&handoff, lite.clone());
    let second = enroll(&handoff, lite.clone());
    let first_wake = first
        .begin_wake(handoff.acquire_route(lite.clone()).expect("first route"))
        .expect("first wake");
    let error = second
        .begin_wake(handoff.acquire_route(lite).expect("second route"))
        .expect_err("PopLite wake must be single-flight");
    let (second, route) = match error {
        LegacyWakeBeginError::PopLiteSingleFlight { wait, route } => (wait, route),
        _ => panic!("expected PopLite single-flight error"),
    };
    drop(first_wake);
    drop(second.begin_wake(route).expect("second wake"));
    assert!(handoff.zero_report().is_zero());

    let pop = pop_target();
    let first = enroll(&handoff, pop.clone());
    let second = enroll(&handoff, pop.clone());
    let first_wake = first
        .begin_wake(handoff.acquire_route(pop.clone()).expect("first route"))
        .expect("first POP wake");
    let second_wake = second
        .begin_wake(handoff.acquire_route(pop).expect("second route"))
        .expect("second POP wake");
    drop((first_wake, second_wake));
    assert!(handoff.zero_report().is_zero());
}

#[test]
fn cutover_seals_then_explicitly_transitions_drained_legacy_target() {
    let handoff = DeferredGenerationHandoff::new();
    let target = pop_target();
    let wait = enroll(&handoff, target.clone());
    publish_new(&handoff);
    assert_eq!(handoff.generation_for(&target), DeferredGeneration::Legacy);
    assert!(matches!(
        handoff.try_transition_target_to_new(target.clone(), |_| false),
        Err(DeferredGenerationTargetTransitionError::Draining(_))
    ));
    drop(wait);
    assert_eq!(handoff.generation_for(&target), DeferredGeneration::Legacy);
    assert!(matches!(
        handoff.try_transition_target_to_new(target.clone(), |_| true),
        Err(DeferredGenerationTargetTransitionError::LegacyTableOccupied)
    ));
    let replay = handoff
        .try_transition_target_to_new(target.clone(), |_| false)
        .expect("drained target transition");
    assert_eq!(handoff.generation_for(&target), DeferredGeneration::New);
    assert_eq!(replay.target(), &target);
    assert_eq!(handoff.snapshot().replay_tokens, 1);
    replay.complete_after_replay_accepted();
    assert!(handoff.zero_report().is_zero());
}

#[test]
fn transition_scan_is_o1_closed_before_publish_and_bounded_after_publish() {
    let handoff = DeferredGenerationHandoff::new();
    let target = pop_target();
    let wait = enroll(&handoff, target.clone());
    let probes = AtomicUsize::new(0);

    assert!(!handoff.transition_scan_ready_for_test());
    assert!(handoff.take_transition_candidates(1).is_empty());
    assert_eq!(handoff.transition_candidate_scans_for_test(), 0);
    assert_eq!(probes.load(Ordering::Relaxed), 0);

    publish_new(&handoff);
    drop(wait);
    assert!(handoff.transition_scan_ready_for_test());
    let candidates = handoff.take_transition_candidates(1);
    assert_eq!(handoff.transition_candidate_scans_for_test(), 1);
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].target, target);
    assert_eq!(candidates[0].kind, DeferredGenerationTransitionKind::LegacyTarget);
    let replay = handoff
        .try_transition_target_to_new(target, |_| {
            probes.fetch_add(1, Ordering::Relaxed);
            false
        })
        .expect("published candidate probes then transitions");
    assert_eq!(probes.load(Ordering::Relaxed), 1);
    replay.complete_after_replay_accepted();
    assert!(handoff.zero_report().is_zero());
}

#[test]
fn transition_candidates_requeue_at_the_tail_without_fixed_prefix_starvation() {
    let handoff = DeferredGenerationHandoff::new();
    let targets = [
        DeferredGenerationTarget::pull(string("pull-a"), 0),
        DeferredGenerationTarget::pull(string("pull-b"), 0),
        DeferredGenerationTarget::pull(string("pull-c"), 0),
    ];
    let waits = targets
        .iter()
        .cloned()
        .map(|target| enroll(&handoff, target))
        .collect::<Vec<_>>();
    publish_new(&handoff);
    drop(waits);

    let first = handoff
        .take_transition_candidates(1)
        .pop()
        .expect("first bounded candidate");
    handoff.requeue_transition_candidate(&first.target);
    let second = handoff
        .take_transition_candidates(1)
        .pop()
        .expect("next bounded candidate");
    assert_ne!(second.target, first.target, "requeued work moves behind its peers");
}

#[test]
fn transition_rejects_absent_and_explicitly_new_targets() {
    let handoff = DeferredGenerationHandoff::new();
    publish_new(&handoff);
    let target = DeferredGenerationTarget::pop_lite(string("pending-client"));

    assert!(matches!(
        handoff.try_transition_target_to_new(target.clone(), |_| false),
        Err(DeferredGenerationTargetTransitionError::TargetAbsent)
    ));
    let route = handoff.acquire_route(target.clone()).expect("new route permit");
    assert_eq!(handoff.generation_for(&target), DeferredGeneration::New);
    assert!(matches!(
        handoff.try_transition_target_to_new(target.clone(), |_| false),
        Err(DeferredGenerationTargetTransitionError::TargetAlreadyNew)
    ));
    drop(route);
    assert!(handoff.zero_report().is_zero());
}

#[test]
fn transition_rejects_shutdown_before_inspecting_target_state() {
    let handoff = DeferredGenerationHandoff::new();
    let target = pop_target();
    let wait = enroll(&handoff, target.clone());
    publish_new(&handoff);
    drop(wait);
    assert_eq!(handoff.seal(), DeferredGenerationSeal::Sealed);

    assert!(matches!(
        handoff.try_transition_target_to_new(target, |_| false),
        Err(DeferredGenerationTargetTransitionError::ShutdownSealed)
    ));
}

#[test]
fn dropped_replay_token_stays_abandoned_until_explicit_retry_completion() {
    let handoff = DeferredGenerationHandoff::new();
    let target = pop_target();
    let wait = enroll(&handoff, target.clone());
    publish_new(&handoff);
    drop(wait);
    drop(
        handoff
            .try_transition_target_to_new(target.clone(), |_| false)
            .expect("drained target transition"),
    );

    let snapshot = handoff.snapshot();
    assert_eq!(snapshot.replay_tokens, 0);
    assert_eq!(snapshot.abandoned_replays, 1);
    assert_eq!(snapshot.tracked_targets, 1);
    assert!(!handoff.zero_report().is_zero());

    let abandoned = handoff
        .take_transition_candidates(1)
        .pop()
        .expect("abandoned replay is returned to the transition queue");
    assert_eq!(abandoned.target, target);
    assert_eq!(abandoned.kind, DeferredGenerationTransitionKind::AbandonedReplay);
    let retry = handoff
        .retry_abandoned_replay(abandoned.target)
        .expect("abandoned replay remains explicitly retryable");
    assert_eq!(handoff.snapshot().replay_tokens, 1);
    assert_eq!(handoff.snapshot().abandoned_replays, 0);

    drop(retry);
    let retried = handoff
        .take_transition_candidates(1)
        .pop()
        .expect("a failed replay is requeued at the tail");
    assert_eq!(retried.target, target);
    assert_eq!(retried.kind, DeferredGenerationTransitionKind::AbandonedReplay);
    let retry = handoff
        .retry_abandoned_replay(retried.target)
        .expect("requeued replay remains retryable");
    retry.complete_after_replay_accepted();
    assert!(handoff.zero_report().is_zero());
}

#[test]
fn shutdown_prunes_quiescent_legacy_marker_and_abandoned_replay() {
    let handoff = DeferredGenerationHandoff::new();
    let legacy = pop_target();
    let wait = enroll(&handoff, legacy.clone());
    publish_new(&handoff);
    drop(wait);
    assert_eq!(handoff.snapshot().tracked_targets, 1);

    let new_target = DeferredGenerationTarget::pop_lite(string("new-client"));
    let new_route = handoff.acquire_route(new_target).expect("new route permit");
    assert_eq!(handoff.seal(), DeferredGenerationSeal::Sealed);
    assert_eq!(handoff.snapshot().tracked_targets, 1);
    drop(new_route);
    assert!(handoff.zero_report().is_zero());

    let handoff = DeferredGenerationHandoff::new();
    let target = pop_target();
    let wait = enroll(&handoff, target.clone());
    publish_new(&handoff);
    drop(wait);
    drop(
        handoff
            .try_transition_target_to_new(target, |_| false)
            .expect("drained target transition"),
    );
    assert_eq!(handoff.seal(), DeferredGenerationSeal::Sealed);
    let snapshot = handoff.snapshot();
    assert_eq!(snapshot.abandoned_replays, 0);
    assert_eq!(snapshot.tracked_targets, 0);
    assert!(handoff.zero_report().is_zero());
}

#[test]
fn replay_token_dropped_after_shutdown_does_not_create_abandoned_work() {
    let handoff = DeferredGenerationHandoff::new();
    let target = pop_target();
    let wait = enroll(&handoff, target.clone());
    publish_new(&handoff);
    drop(wait);
    let replay = handoff
        .try_transition_target_to_new(target, |_| false)
        .expect("drained target transition");

    assert_eq!(handoff.seal(), DeferredGenerationSeal::Sealed);
    assert_eq!(handoff.snapshot().replay_tokens, 1);
    drop(replay);

    assert!(handoff.take_transition_candidates(1).is_empty());
    assert!(handoff.zero_report().is_zero());
}

#[test]
fn cutover_cannot_publish_new_out_of_order() {
    let handoff = DeferredGenerationHandoff::new();
    let mut transaction = handoff.cutover_transaction().expect("cutover transaction");
    assert_eq!(
        transaction.publish_default_new(),
        Err(DeferredGenerationCutoverError::InvalidStage)
    );
    transaction.seal_legacy_acceptance().expect("seal legacy acceptance");
    assert_eq!(
        transaction.publish_default_new(),
        Err(DeferredGenerationCutoverError::InvalidStage)
    );
    transaction
        .publish_v2_aggregate(DeferredGenerationV2Publisher::nonblocking_atomic(|| Ok::<_, ()>(())))
        .expect("publish V2 aggregate");
    transaction.publish_default_new().expect("publish New default");
    drop(transaction);
    assert_eq!(handoff.default_generation(), DeferredGeneration::New);
}

#[test]
fn interrupted_cutover_resumes_from_the_last_published_stage() {
    let handoff = DeferredGenerationHandoff::new();
    {
        let mut transaction = handoff.cutover_transaction().expect("cutover transaction");
        transaction.seal_legacy_acceptance().expect("seal legacy acceptance");
    }
    assert!(!handoff.transition_scan_ready_for_test());
    assert!(handoff.take_transition_candidates(1).is_empty());
    assert_eq!(handoff.transition_candidate_scans_for_test(), 0);
    {
        let mut transaction = handoff.cutover_transaction().expect("resumed transaction");
        transaction
            .seal_legacy_acceptance()
            .expect("sealing is idempotent after an interruption");
        transaction
            .publish_v2_aggregate(DeferredGenerationV2Publisher::nonblocking_atomic(|| Ok::<_, ()>(())))
            .expect("publish V2 aggregate");
    }
    assert!(!handoff.transition_scan_ready_for_test());
    assert!(handoff.take_transition_candidates(1).is_empty());
    assert_eq!(handoff.transition_candidate_scans_for_test(), 0);
    let mut transaction = handoff.cutover_transaction().expect("second resumed transaction");
    transaction
        .publish_v2_aggregate(DeferredGenerationV2Publisher::nonblocking_atomic(|| Ok::<_, ()>(())))
        .expect("V2 publication is idempotent after an interruption");
    transaction.publish_default_new().expect("publish New default");
    drop(transaction);
    assert_eq!(handoff.default_generation(), DeferredGeneration::New);
}

#[test]
fn aggregate_publish_error_panic_retry_and_idempotence_are_exactly_once() {
    let handoff = DeferredGenerationHandoff::new();
    let attempts = AtomicUsize::new(0);
    {
        let mut transaction = handoff.cutover_transaction().expect("cutover transaction");
        transaction.seal_legacy_acceptance().expect("seal legacy acceptance");
        assert!(matches!(
            transaction.publish_v2_aggregate(DeferredGenerationV2Publisher::nonblocking_atomic(|| {
                attempts.fetch_add(1, Ordering::AcqRel);
                Err::<(), _>("publish failed")
            })),
            Err(DeferredGenerationV2PublishError::Publish("publish failed"))
        ));
    }
    assert_eq!(attempts.load(Ordering::Acquire), 1);
    assert!(!handoff.snapshot().v2_aggregate_published);

    let mut transaction = handoff.cutover_transaction().expect("retry transaction");
    let panic = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = transaction.publish_v2_aggregate(DeferredGenerationV2Publisher::nonblocking_atomic(
            || -> Result<(), ()> {
                attempts.fetch_add(1, Ordering::AcqRel);
                panic!("real aggregate publisher panicked");
            },
        ));
    }));
    assert!(panic.is_err());
    drop(transaction);
    assert_eq!(attempts.load(Ordering::Acquire), 2);
    assert!(!handoff.snapshot().v2_aggregate_published);

    let mut transaction = handoff.cutover_transaction().expect("recoverable transaction");
    transaction
        .publish_v2_aggregate(DeferredGenerationV2Publisher::nonblocking_atomic(|| {
            attempts.fetch_add(1, Ordering::AcqRel);
            Ok::<_, ()>(())
        }))
        .expect("successful real publication advances stage");
    transaction
        .publish_v2_aggregate(DeferredGenerationV2Publisher::nonblocking_atomic(|| {
            attempts.fetch_add(1, Ordering::AcqRel);
            Ok::<_, ()>(())
        }))
        .expect("committed publication is idempotent without republishing");
    assert_eq!(attempts.load(Ordering::Acquire), 3);
    transaction.publish_default_new().expect("publish New default");
    drop(transaction);
    assert_eq!(handoff.default_generation(), DeferredGeneration::New);
}

#[test]
fn shutdown_and_producer_routing_serialize_after_controlled_publish_commit() {
    let handoff = Arc::new(DeferredGenerationHandoff::new());
    let publish_calls = Arc::new(AtomicUsize::new(0));
    let (publish_entered_tx, publish_entered_rx) = mpsc::channel();
    let (publish_release_tx, publish_release_rx) = mpsc::channel();
    let worker_handoff = Arc::clone(&handoff);
    let worker_publish_calls = Arc::clone(&publish_calls);
    let worker = std::thread::spawn(move || {
        let mut transaction = worker_handoff.cutover_transaction().expect("cutover transaction");
        transaction.seal_legacy_acceptance().expect("seal legacy acceptance");
        transaction
            .publish_v2_aggregate(DeferredGenerationV2Publisher::blocking_for_serialization_test(|| {
                worker_publish_calls.fetch_add(1, Ordering::AcqRel);
                publish_entered_tx.send(()).expect("signal aggregate publish entry");
                publish_release_rx.recv().expect("release aggregate publisher");
                Ok::<_, ()>(())
            }))
            .expect("controlled aggregate publish commits under the coordinator gate");
        transaction.publish_default_new().expect("publish New default");
    });

    publish_entered_rx.recv().expect("aggregate publisher entered");

    let shutdown_handoff = Arc::clone(&handoff);
    let (shutdown_started_tx, shutdown_started_rx) = mpsc::channel();
    let (shutdown_done_tx, shutdown_done_rx) = mpsc::channel();
    let shutdown = std::thread::spawn(move || {
        shutdown_started_tx.send(()).expect("signal shutdown start");
        shutdown_done_tx
            .send(shutdown_handoff.seal())
            .expect("publish shutdown result");
    });

    let route_handoff = Arc::clone(&handoff);
    let (route_started_tx, route_started_rx) = mpsc::channel();
    let (route_done_tx, route_done_rx) = mpsc::channel();
    let route = std::thread::spawn(move || {
        route_started_tx.send(()).expect("signal producer route start");
        let observed = match route_handoff.acquire_route(pop_target()) {
            Ok(route) => Some(route.generation()),
            Err(DeferredGenerationRouteError::ShutdownSealed) => None,
        };
        route_done_tx.send(observed).expect("publish producer route result");
    });

    shutdown_started_rx.recv().expect("shutdown started");
    route_started_rx.recv().expect("producer route started");
    while handoff.seal_gate_attempts_for_test() == 0 {
        std::thread::yield_now();
    }
    assert!(
        shutdown_done_rx.try_recv().is_err(),
        "shutdown must wait for publish commit"
    );
    assert!(
        route_done_rx.try_recv().is_err(),
        "producer routing must wait for publish commit"
    );

    publish_release_tx.send(()).expect("release aggregate publish");
    worker.join().expect("cutover worker");
    shutdown.join().expect("shutdown worker");
    route.join().expect("producer route worker");
    assert_eq!(
        shutdown_done_rx.recv().expect("shutdown result"),
        DeferredGenerationSeal::Sealed
    );
    if let Some(generation) = route_done_rx.recv().expect("producer route result") {
        assert_eq!(generation, DeferredGeneration::New);
    }
    assert_eq!(publish_calls.load(Ordering::Acquire), 1);
    let snapshot = handoff.snapshot();
    assert!(snapshot.sealed);
    assert!(snapshot.v2_aggregate_published);
    assert_eq!(snapshot.default_generation, DeferredGeneration::New);
    assert!(!handoff.transition_scan_ready_for_test());
    let scans = handoff.transition_candidate_scans_for_test();
    assert!(handoff.take_transition_candidates(1).is_empty());
    assert_eq!(handoff.transition_candidate_scans_for_test(), scans);
}

#[test]
fn shutdown_seals_producers_and_arrival_order_is_fixed() {
    let handoff = DeferredGenerationHandoff::new();
    let calls = RefCell::new(Vec::new());
    handoff.arrival_adapter().route_arrival(
        |_| calls.borrow_mut().push("pull"),
        |_| calls.borrow_mut().push("pop"),
        |_| calls.borrow_mut().push("notification"),
        |_| calls.borrow_mut().push("pop-lite"),
    );
    assert_eq!(calls.into_inner(), vec!["pull", "pop", "notification", "pop-lite"]);
    assert_eq!(handoff.seal(), DeferredGenerationSeal::Sealed);
    assert!(matches!(
        handoff.acquire_route(pop_target()),
        Err(DeferredGenerationRouteError::ShutdownSealed)
    ));
    assert!(matches!(
        handoff
            .arrival_adapter()
            .enroll_legacy_wait(pop_target(), || Ok::<_, ()>(())),
        Err(DeferredGenerationLegacyEnrollmentError::ShutdownSealed)
    ));
    assert!(handoff.zero_report().is_zero());
}
