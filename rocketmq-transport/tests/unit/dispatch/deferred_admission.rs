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

use std::alloc::Layout;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::sync::atomic::AtomicUsize;
use std::sync::Barrier;
use std::time::Duration;

use rocketmq_runtime::shutdown_deadline::ShutdownDeadline;
use rocketmq_runtime::BudgetDimension;
use rocketmq_runtime::BudgetRejection;
use rocketmq_runtime::ResourceBudgetTree;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;

use super::*;
use crate::admission::AdmissionClass;
use crate::admission::AdmissionLimits;
use crate::admission::AdmissionResource;
use crate::admission::AdmissionScope;
use crate::admission::PartialFramePermit;
use crate::admission::ResourceLimit;
use crate::contract::TransportContractViolation;
use crate::dispatch::DeferredAdmissionAcquireOutcome;
use crate::dispatch::ResponseState;
use crate::request_ordering::RequestOrdering;
use crate::session_executor::SessionDispatchAttempt;
use crate::session_executor::SessionExecutor;

fn process_budget(name: &str, count: usize, bytes: usize) -> ResourceBudget {
    ResourceBudgetTree::new(name, BudgetLimit::new(count, bytes, FullPolicy::Reject))
        .expect("test process budget")
        .root()
}

fn controller(process_budget: &ResourceBudget) -> AdmissionController {
    AdmissionController::try_new_with_budget(AdmissionLimits::default(), process_budget)
        .expect("test admission controller")
}

fn retained(resume_bytes: usize) -> DeferredRetainedSize {
    DeferredRetainedSize::try_from_parts(DeferredRetainedSizeParts::new(resume_bytes)).expect("small retained size")
}

fn expect_acquired(admission: &DeferredAdmission, retained: DeferredRetainedSize, context: &str) -> DeferredWaitPermit {
    match admission.try_reserve(retained) {
        DeferredAdmissionAcquireOutcome::Acquired(permit) => permit,
        DeferredAdmissionAcquireOutcome::WaiterCapacityExhausted(_) => {
            panic!("{context}: waiter capacity was unexpectedly exhausted")
        }
        DeferredAdmissionAcquireOutcome::RetainedByteCapacityExhausted(_) => {
            panic!("{context}: retained-byte capacity was unexpectedly exhausted")
        }
        DeferredAdmissionAcquireOutcome::ParentCapacityExhausted(_) => {
            panic!("{context}: parent capacity was unexpectedly exhausted")
        }
    }
}

fn expect_waiter_capacity_exhausted(outcome: DeferredAdmissionAcquireOutcome) -> BudgetRejection {
    match outcome {
        DeferredAdmissionAcquireOutcome::Acquired(_) => {
            panic!("reservation unexpectedly acquired waiter capacity")
        }
        DeferredAdmissionAcquireOutcome::WaiterCapacityExhausted(rejection) => rejection,
        DeferredAdmissionAcquireOutcome::RetainedByteCapacityExhausted(_) => {
            panic!("reservation exhausted retained-byte capacity instead of waiter capacity")
        }
        DeferredAdmissionAcquireOutcome::ParentCapacityExhausted(_) => {
            panic!("reservation exhausted parent capacity instead of waiter capacity")
        }
    }
}

fn expect_retained_byte_capacity_exhausted(outcome: DeferredAdmissionAcquireOutcome) -> BudgetRejection {
    match outcome {
        DeferredAdmissionAcquireOutcome::Acquired(_) => {
            panic!("reservation unexpectedly acquired retained-byte capacity")
        }
        DeferredAdmissionAcquireOutcome::WaiterCapacityExhausted(_) => {
            panic!("reservation exhausted waiter capacity instead of retained-byte capacity")
        }
        DeferredAdmissionAcquireOutcome::RetainedByteCapacityExhausted(rejection) => rejection,
        DeferredAdmissionAcquireOutcome::ParentCapacityExhausted(_) => {
            panic!("reservation exhausted parent capacity instead of retained-byte capacity")
        }
    }
}

fn expect_parent_capacity_exhausted(outcome: DeferredAdmissionAcquireOutcome) -> BudgetRejection {
    match outcome {
        DeferredAdmissionAcquireOutcome::Acquired(_) => {
            panic!("reservation unexpectedly acquired parent capacity")
        }
        DeferredAdmissionAcquireOutcome::WaiterCapacityExhausted(_) => {
            panic!("reservation exhausted waiter capacity instead of parent capacity")
        }
        DeferredAdmissionAcquireOutcome::RetainedByteCapacityExhausted(_) => {
            panic!("reservation exhausted retained-byte capacity instead of parent capacity")
        }
        DeferredAdmissionAcquireOutcome::ParentCapacityExhausted(rejection) => rejection,
    }
}

#[test]
fn retained_size_charges_exact_layout_and_each_declared_part() {
    let header = Layout::array::<AtomicUsize>(2).expect("Arc header layout");
    let (allocation, _) = header
        .extend(Layout::new::<ResponseState>())
        .expect("Arc response-state layout");
    assert_eq!(response_state_allocation_bytes(), allocation.pad_to_align().size());
    let fixed = size_of::<DeferredResponder>()
        + size_of::<crate::dispatch::RequestControlView>()
        + response_state_allocation_bytes()
        + size_of::<DeferredWaitPermit>();
    let empty = DeferredRetainedSize::try_from_parts(DeferredRetainedSizeParts::new(0)).expect("fixed retained size");
    assert_eq!(empty.bytes(), fixed);

    let parts = DeferredRetainedSizeParts::new(11)
        .with_filter_bytes(13)
        .with_secondary_index_bytes(17)
        .with_metadata_bytes(19);
    assert_eq!(parts.resume_bytes(), 11);
    assert_eq!(parts.filter_bytes(), 13);
    assert_eq!(parts.secondary_index_bytes(), 17);
    assert_eq!(parts.metadata_bytes(), 19);
    assert_eq!(
        DeferredRetainedSize::try_from_parts(parts)
            .expect("checked retained parts")
            .bytes(),
        fixed + 11 + 13 + 17 + 19
    );
}

#[test]
fn retained_size_overflow_fails_before_touching_the_budget() {
    let process = process_budget("deferred-overflow-process", 32, usize::MAX);
    let controller = controller(&process);
    let admission = DeferredAdmission::try_configure(&controller, DeferredWaitLimits::new(4, usize::MAX))
        .expect("configure deferred admission");
    let before = admission.snapshot();
    let error = DeferredRetainedSize::try_from_parts(DeferredRetainedSizeParts::new(usize::MAX).with_metadata_bytes(1))
        .expect_err("retained size must overflow");
    assert_eq!(error, TransportContractViolation::DeferredRetainedSizeOverflow);
    assert_eq!(admission.snapshot(), before);
}

#[test]
fn zero_and_parent_exceeding_configuration_preserve_budget_sources_and_leave_slot_empty() {
    let process = process_budget("secret-process-owner", 8, 8 * 1024);
    let controller = controller(&process);
    enum ExpectedConfiguration {
        ZeroWaiters,
        ZeroRetainedBytes,
        ExceedsProcess(BudgetDimension),
    }

    for (limits, expected) in [
        (DeferredWaitLimits::new(0, 1), ExpectedConfiguration::ZeroWaiters),
        (DeferredWaitLimits::new(1, 0), ExpectedConfiguration::ZeroRetainedBytes),
        (
            DeferredWaitLimits::new(9, 1),
            ExpectedConfiguration::ExceedsProcess(BudgetDimension::Count),
        ),
        (
            DeferredWaitLimits::new(1, 8 * 1024 + 1),
            ExpectedConfiguration::ExceedsProcess(BudgetDimension::Bytes),
        ),
    ] {
        let error = DeferredAdmission::try_configure(&controller, limits).expect_err("configuration must fail closed");
        for rendered in [format!("{error}"), format!("{error:?}")] {
            assert!(!rendered.contains("secret-process-owner"));
            assert!(!rendered.contains("transport-deferred-wait"));
        }
        let source = std::error::Error::source(&error).expect("runtime contract remains the typed source");
        assert!(source
            .downcast_ref::<rocketmq_runtime::RuntimeContractViolation>()
            .is_some());
        match expected {
            ExpectedConfiguration::ZeroWaiters => {
                let TransportContractViolation::DeferredAdmissionZeroWaiterCapacity(source) = error else {
                    panic!("zero waiter configuration returned the wrong contract violation")
                };
                assert_eq!(
                    source,
                    rocketmq_runtime::RuntimeContractViolation::ZeroBudgetCapacity {
                        dimension: BudgetDimension::Count
                    }
                );
            }
            ExpectedConfiguration::ZeroRetainedBytes => {
                let TransportContractViolation::DeferredAdmissionZeroRetainedByteCapacity(source) = error else {
                    panic!("zero retained-byte configuration returned the wrong contract violation")
                };
                assert_eq!(
                    source,
                    rocketmq_runtime::RuntimeContractViolation::ZeroBudgetCapacity {
                        dimension: BudgetDimension::Bytes
                    }
                );
            }
            ExpectedConfiguration::ExceedsProcess(dimension) => {
                let TransportContractViolation::DeferredAdmissionExceedsProcessCapacity(source) = error else {
                    panic!("parent-exceeding configuration returned the wrong contract violation")
                };
                assert_eq!(
                    source,
                    rocketmq_runtime::RuntimeContractViolation::ChildBudgetExceedsParent { dimension }
                );
            }
        }
        assert!(controller.deferred_admission().is_none());
    }

    DeferredAdmission::try_configure(&controller, DeferredWaitLimits::new(2, 1024))
        .expect("failed configuration must not poison the slot");
}

#[test]
fn concurrent_equal_configuration_shares_one_owner_and_different_limits_conflict() {
    let process = process_budget("deferred-config-process", 64, 1024 * 1024);
    let controller = Arc::new(controller(&process));
    let size = retained(1);
    let limits = DeferredWaitLimits::new(4, size.bytes() * 8);
    let start = Arc::new(Barrier::new(9));
    let configured = std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for _ in 0..8 {
            let controller = Arc::clone(&controller);
            let start = Arc::clone(&start);
            workers.push(scope.spawn(move || {
                start.wait();
                DeferredAdmission::try_configure(&controller, limits).expect("equal configuration")
            }));
        }
        start.wait();
        workers
            .into_iter()
            .map(|worker| worker.join().expect("configuration worker"))
            .collect::<Vec<_>>()
    });
    assert!(configured.iter().all(|admission| configured[0].same_owner(admission)));

    let permits = configured
        .iter()
        .take(4)
        .map(|admission| expect_acquired(admission, size, "one shared capacity"))
        .collect::<Vec<_>>();
    assert_eq!(configured[0].snapshot().waiting_count(), 4);
    let rejection = expect_waiter_capacity_exhausted(configured[4].try_reserve(size));
    assert_eq!(rejection.dimension(), BudgetDimension::Count);
    drop(permits);

    let conflict = DeferredAdmission::try_configure(&controller, DeferredWaitLimits::new(5, size.bytes() * 8))
        .expect_err("different limits must not replace the owner");
    assert_eq!(conflict, TransportContractViolation::DeferredAdmissionConflict);
    assert!(controller
        .deferred_admission()
        .expect("configured owner")
        .same_owner(&configured[0]));
}

#[test]
fn acquire_outcomes_distinguish_local_count_local_bytes_and_parent_capacity() {
    let count_process = process_budget("deferred-count-process", 16, 1024 * 1024);
    let count_controller = controller(&count_process);
    let size = retained(1);
    let count_admission =
        DeferredAdmission::try_configure(&count_controller, DeferredWaitLimits::new(1, size.bytes() * 2))
            .expect("count admission");
    let count_permit = expect_acquired(&count_admission, size, "first count permit");
    let count_rejection = expect_waiter_capacity_exhausted(count_admission.try_reserve(size));
    assert_eq!(count_rejection.dimension(), BudgetDimension::Count);
    drop(count_permit);

    let bytes_process = process_budget("deferred-bytes-process", 16, 1024 * 1024);
    let bytes_controller = controller(&bytes_process);
    let bytes_admission =
        DeferredAdmission::try_configure(&bytes_controller, DeferredWaitLimits::new(2, size.bytes() * 2 - 1))
            .expect("bytes admission");
    let bytes_permit = expect_acquired(&bytes_admission, size, "first bytes permit");
    let bytes_rejection = expect_retained_byte_capacity_exhausted(bytes_admission.try_reserve(size));
    assert_eq!(bytes_rejection.dimension(), BudgetDimension::Bytes);
    drop(bytes_permit);

    let parent_process = process_budget("secret-parent-process", 2, 1024 * 1024);
    let parent_controller = controller(&parent_process);
    let parent_admission =
        DeferredAdmission::try_configure(&parent_controller, DeferredWaitLimits::new(2, size.bytes() * 2))
            .expect("parent admission");
    let unrelated = parent_process.try_acquire_data(1).expect("occupy shared parent");
    let parent_permit = expect_acquired(&parent_admission, size, "remaining parent capacity");
    let parent_rejection = expect_parent_capacity_exhausted(parent_admission.try_reserve(size));
    assert_eq!(parent_rejection.dimension(), BudgetDimension::Count);
    drop(parent_permit);
    drop(unrelated);
}

#[test]
fn permit_move_release_drop_and_terminal_simulations_release_exactly_once() {
    let process = process_budget("deferred-release-process", 32, 1024 * 1024);
    let controller = controller(&process);
    let size = retained(7);
    let admission = DeferredAdmission::try_configure(&controller, DeferredWaitLimits::new(8, size.bytes() * 8))
        .expect("release admission");
    let released_before = admission.inner.budget.snapshot().released_count;

    let permit = expect_acquired(&admission, size, "moved permit");
    assert_eq!(permit.retained_bytes(), size.bytes());
    let moved = permit;
    moved.release();
    assert_eq!(admission.snapshot().waiting_count(), 0);
    assert_eq!(admission.inner.budget.snapshot().released_count, released_before + 1);

    let dropped = expect_acquired(&admission, size, "dropped permit");
    drop(dropped);
    assert_eq!(admission.inner.budget.snapshot().released_count, released_before + 2);

    for terminal in ["claim", "cancel", "timeout", "session_close"] {
        let permit = expect_acquired(&admission, size, "terminal simulation permit");
        if terminal == "claim" {
            permit.release();
        } else {
            drop(permit);
        }
        assert_eq!(admission.snapshot().waiting_count(), 0, "{terminal}");
    }
    assert_eq!(admission.inner.budget.snapshot().released_count, released_before + 6);
}

#[test]
fn concurrent_reservations_stop_at_the_shared_cap_and_finish_at_zero() {
    let process = process_budget("deferred-concurrent-process", 64, 1024 * 1024);
    let controller = controller(&process);
    let size = retained(3);
    let admission = DeferredAdmission::try_configure(&controller, DeferredWaitLimits::new(4, size.bytes() * 12))
        .expect("concurrent admission");
    let start = Arc::new(Barrier::new(13));
    let acquired = Arc::new(Barrier::new(13));
    let release = Arc::new(Barrier::new(13));
    let successes = std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for _ in 0..12 {
            let admission = admission.clone();
            let start = Arc::clone(&start);
            let acquired = Arc::clone(&acquired);
            let release = Arc::clone(&release);
            workers.push(scope.spawn(move || {
                start.wait();
                let permit = match admission.try_reserve(size) {
                    DeferredAdmissionAcquireOutcome::Acquired(permit) => Some(permit),
                    DeferredAdmissionAcquireOutcome::WaiterCapacityExhausted(_) => None,
                    DeferredAdmissionAcquireOutcome::RetainedByteCapacityExhausted(_) => None,
                    DeferredAdmissionAcquireOutcome::ParentCapacityExhausted(_) => None,
                };
                acquired.wait();
                release.wait();
                let success = permit.is_some();
                drop(permit);
                success
            }));
        }
        start.wait();
        acquired.wait();
        assert_eq!(admission.snapshot().waiting_count(), 4);
        release.wait();
        workers
            .into_iter()
            .map(|worker| worker.join().expect("reservation worker"))
            .filter(|success| *success)
            .count()
    });
    assert_eq!(successes, 4);
    assert_eq!(admission.snapshot().waiting_count(), 0);
    assert_eq!(admission.snapshot().retained_bytes(), 0);
}

#[tokio::test]
async fn deferred_permit_is_independent_from_real_session_execution_and_baseline_snapshots() {
    let runtime = RuntimeOwner::plan(RuntimeConfig::server_default("deferred-independence-runtime"))
        .expect("test runtime configuration is valid")
        .build()
        .expect("deferred independence runtime owner");
    let service = runtime.root_context().component("deferred-independence-session");
    let limits = AdmissionLimits {
        processors: ResourceLimit { count: 1, bytes: 1024 },
        ..AdmissionLimits::default()
    };
    let controller = AdmissionController::try_new_with_budget(limits, &service.process_budget())
        .expect("lifecycle-owned admission controller");
    let baseline_before = controller.snapshot();
    let size = retained(5);
    let admission = DeferredAdmission::try_configure(&controller, DeferredWaitLimits::new(4, size.bytes() * 4))
        .expect("independent deferred admission");
    assert_eq!(controller.snapshot(), baseline_before);

    let scope = controller
        .prepare_scope(AdmissionScope::new(IpAddr::V4(Ipv4Addr::LOCALHOST)).with_session(9810))
        .expect("prepared session admission scope");
    let executor = SessionExecutor::try_new(service.task_group(), scope).expect("session executor");
    let task_admission = admission.clone();
    let (permit_tx, permit_rx) = tokio::sync::oneshot::channel();
    let first_attempt = executor
        .try_execute(
            1,
            AdmissionClass::Data,
            None,
            RequestOrdering::Concurrent,
            move |_operation| async move {
                let permit = expect_acquired(&task_admission, size, "deferred permit in real processor task");
                assert!(
                    permit_tx.send(permit).is_ok(),
                    "test owner must receive deferred permit"
                );
            },
            |_operation, _error| async {},
        )
        .expect("first real session task");
    assert!(matches!(first_attempt, SessionDispatchAttempt::Accepted(_)));
    let deferred = permit_rx
        .await
        .expect("first task must execute and transfer its permit");
    assert!(
        executor
            .operation_context()
            .wait(service.task_group(), Duration::from_secs(1))
            .await
            .expect("wait for first session task"),
        "first session task must finish"
    );

    let after_first = controller.snapshot();
    assert_eq!(after_first.queued.current_count, 0);
    assert_eq!(after_first.inflight.current_count, 0);
    assert_eq!(after_first.processors.current_count, 0);
    assert_eq!(after_first, baseline_before);
    assert_eq!(admission.snapshot().waiting_count(), 1);

    let (second_tx, second_rx) = tokio::sync::oneshot::channel();
    let second_attempt = executor
        .try_execute(
            1,
            AdmissionClass::Data,
            None,
            RequestOrdering::Concurrent,
            move |_operation| async move {
                assert!(second_tx.send(()).is_ok(), "test owner must observe the second task");
            },
            |_operation, _error| async {},
        )
        .expect("second real session task must be admitted with processor capacity one");
    assert!(matches!(second_attempt, SessionDispatchAttempt::Accepted(_)));
    second_rx.await.expect("second real session task must execute");
    assert!(
        executor
            .operation_context()
            .wait(service.task_group(), Duration::from_secs(1))
            .await
            .expect("wait for second session task"),
        "second session task must finish"
    );
    assert_eq!(controller.snapshot(), baseline_before);
    assert_eq!(admission.snapshot().waiting_count(), 1);

    deferred.release();
    assert_eq!(admission.snapshot().waiting_count(), 0);
    assert_eq!(admission.snapshot().retained_bytes(), 0);
    assert_eq!(controller.snapshot(), baseline_before);

    let drain = executor
        .drain_until(ShutdownDeadline::after(Duration::from_secs(1)))
        .await;
    assert!(drain.is_healthy(), "session executor must drain: {}", drain.to_json());
    runtime
        .shutdown_tasks()
        .await
        .assert_no_task_leak()
        .expect("runtime owner tasks must drain");
}

#[tokio::test]
async fn closed_session_dispatch_recovers_the_unconsumed_partial_frame_without_an_error() {
    let runtime = RuntimeOwner::plan(RuntimeConfig::server_default("closed-session-dispatch-recovery"))
        .expect("test runtime configuration is valid")
        .build()
        .expect("runtime owner");
    let service = runtime.root_context().component("closed-session-dispatch-recovery");
    let controller = AdmissionController::try_new_with_budget(AdmissionLimits::default(), &service.process_budget())
        .expect("lifecycle-owned admission controller");
    let scope = controller
        .prepare_scope(AdmissionScope::new(IpAddr::V4(Ipv4Addr::LOCALHOST)).with_session(9811))
        .expect("prepared session admission scope");
    let partial = PartialFramePermit::new(
        scope
            .try_acquire(AdmissionResource::PartialFrame, 64, AdmissionClass::Data)
            .expect("partial-frame permit"),
    );
    assert_eq!(controller.snapshot().partial_frames.current_count, 1);

    let executor = SessionExecutor::try_new(service.task_group(), scope).expect("session executor");
    executor.begin_close();
    let attempt = executor
        .try_execute(
            64,
            AdmissionClass::Data,
            Some(partial),
            RequestOrdering::Concurrent,
            |_operation| async { panic!("closed session must not execute the request") },
            |_operation, _rejection| async { panic!("closed session must not run rejection handling") },
        )
        .expect("closed session is a source-free dispatch outcome");
    let retained_partial = match attempt {
        SessionDispatchAttempt::SessionClosed {
            retained_partial: Some(partial),
        } => partial,
        _ => panic!("closed session must retain the caller's partial-frame permit"),
    };
    assert_eq!(controller.snapshot().partial_frames.current_count, 1);
    drop(retained_partial);
    assert_eq!(controller.snapshot().partial_frames.current_count, 0);
    assert_eq!(controller.snapshot().queued.current_count, 0);
    assert_eq!(controller.snapshot().inflight.current_count, 0);

    runtime
        .shutdown_tasks()
        .await
        .assert_no_task_leak()
        .expect("runtime owner tasks must drain");
}

#[tokio::test]
async fn closed_task_group_does_not_mask_an_operation_owner_invariant() {
    let runtime = RuntimeOwner::plan(RuntimeConfig::server_default("session-dispatch-owner-invariant"))
        .expect("test runtime configuration is valid")
        .build()
        .expect("runtime owner");
    let root = runtime.root_context();
    let service = root.component("session-dispatch-owner");
    let conflicting_owner = root.component("session-dispatch-conflicting-owner");
    let controller = AdmissionController::try_new_with_budget(AdmissionLimits::default(), &service.process_budget())
        .expect("lifecycle-owned admission controller");
    let scope = controller
        .prepare_scope(AdmissionScope::new(IpAddr::V4(Ipv4Addr::LOCALHOST)).with_session(9812))
        .expect("prepared session admission scope");
    let executor = SessionExecutor::try_new(service.task_group(), scope).expect("session executor");

    let binding_task = conflicting_owner
        .task_group()
        .spawn_draining_operation(
            executor.operation_context(),
            "session-dispatch-conflicting-owner",
            async {},
        )
        .expect("bind operation to conflicting owner");
    assert!(
        conflicting_owner
            .task_group()
            .wait_task(binding_task, Duration::from_secs(1))
            .await,
        "conflicting owner task must finish"
    );
    let _ = service.task_group().shutdown_now();

    let error = executor
        .try_execute(
            64,
            AdmissionClass::Data,
            None,
            RequestOrdering::Concurrent,
            |_operation| async { panic!("owner mismatch must not execute the request") },
            |_operation, _rejection| async { panic!("owner mismatch must not run rejection handling") },
        )
        .expect_err("operation-owner invariant must remain an operational error");
    assert_eq!(error.operation(), rocketmq_runtime::RuntimeOperation::OperationOwner);
    assert_eq!(controller.snapshot().queued.current_count, 0);
    assert_eq!(controller.snapshot().inflight.current_count, 0);

    runtime
        .shutdown_tasks()
        .await
        .assert_no_task_leak()
        .expect("runtime owner tasks must drain");
}

#[test]
fn dispatcher_is_fail_closed_until_explicit_configuration_and_then_shares_owner() {
    #[derive(Clone)]
    struct Processor;

    impl crate::runtime::processor::RequestProcessor for Processor {
        async fn process(
            &mut self,
            _request: &mut crate::dispatch::RemotingRequest,
        ) -> rocketmq_error::RocketMQResult<crate::dispatch::HandlerOutcome> {
            Err(rocketmq_error::RocketMQError::illegal_argument("unused test processor"))
        }
    }

    let process = process_budget("deferred-dispatcher-process", 262_144, 1024 * 1024 * 1024);
    let controller = Arc::new(controller(&process));
    let dispatcher = crate::dispatch::AuthorizedCommandDispatcher::new(
        Processor,
        Vec::new(),
        Arc::new(crate::security::TransportSecurity::development_insecure_loopback(
            None, None,
        )),
        Arc::clone(&controller),
    );
    assert!(dispatcher.deferred_admission().is_none());

    let configured = DeferredAdmission::try_configure(&controller, DeferredWaitLimits::new(4, 64 * 1024))
        .expect("explicit deferred configuration");
    assert!(dispatcher
        .deferred_admission()
        .expect("dispatcher receives configured owner")
        .same_owner(&configured));
}
