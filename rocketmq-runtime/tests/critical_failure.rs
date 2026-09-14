// Copyright 2023 The RocketMQ Rust Authors
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

use std::time::Duration;

use rocketmq_runtime::CriticalFailureKind;
use rocketmq_runtime::CriticalFailureRecovery;
use rocketmq_runtime::CriticalFailureState;
use rocketmq_runtime::DependencyReadiness;
use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::ServiceLifecycle;
use rocketmq_runtime::ServiceLifecycleConfig;
use rocketmq_runtime::ServiceLifecycleState;
use rocketmq_runtime::ShutdownReason;
use rocketmq_runtime::TaskKind;

fn new_lifecycle(name: &str) -> ServiceLifecycle {
    ServiceLifecycle::new(ServiceLifecycleConfig {
        service_name: name.into(),
        probe_bind_addr: None,
        shutdown_timeout: Duration::from_secs(5),
        liveness_stale_after: Duration::from_secs(60),
    })
}

/// Waits until `condition` holds, yielding between checks instead of sleeping.
async fn wait_until(mut condition: impl FnMut() -> bool, description: &str) {
    for _ in 0..10_000 {
        if condition() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("timed out waiting for {description}");
}

#[tokio::test]
async fn a_critical_service_panic_reaches_the_owner_and_revokes_readiness() {
    let context = RuntimeContext::from_current("critical-panic");
    let monitored = context.service_context("monitored");
    // The monitor runs under an owner outside the monitored group, so a poisoned
    // monitored group cannot prevent it from running.
    let supervisor = context.service_context("supervisor");
    let failures = CriticalFailureState::new();
    let lifecycle = new_lifecycle("critical-panic-service");
    let mut notifications = failures.subscribe(4).expect("a positive capacity is valid");

    lifecycle
        .spawn_critical_failure_monitor(
            &supervisor.task_spawner(),
            &failures,
            CriticalFailureRecovery::RevokeReadiness,
        )
        .expect("the monitor owner is open");

    lifecycle.mark_ready().expect("the lifecycle is startable");
    lifecycle.record_progress();
    assert!(lifecycle.is_ready());

    monitored
        .spawn_critical_service("panicking-service", failures.clone(), async move {
            panic!("injected critical service panic");
        })
        .expect("the task should spawn");

    wait_until(|| !lifecycle.is_ready(), "the readiness revocation").await;

    let failure = notifications
        .try_recv()
        .expect("the failure was notified to the subscriber");
    assert_eq!(failure.kind(), CriticalFailureKind::Panicked);
    assert_eq!(failure.task_kind(), TaskKind::Service);
    assert_eq!(lifecycle.dependency_readiness(), DependencyReadiness::Degraded);
    assert!(
        lifecycle.is_live(),
        "liveness stays independent of the failed critical work"
    );
    assert_eq!(failures.occurrences(), 1);
    assert_eq!(failures.pending(), None, "the monitor handled the record");
}

#[tokio::test]
async fn an_early_service_return_is_an_unexpected_exit_but_cancellation_is_not() {
    let context = RuntimeContext::from_current("critical-exit");
    let failures = CriticalFailureState::new();
    let monitored = context.service_context("monitored");

    monitored
        .spawn_critical_service("returns-early", failures.clone(), async {})
        .expect("the task should spawn");
    wait_until(|| failures.pending().is_some(), "the early return").await;
    let failure = failures.handle().expect("a pending record exists");
    assert_eq!(failure.kind(), CriticalFailureKind::ExitedUnexpectedly);
    assert_eq!(failure.sequence(), 1);

    // A critical service cancelled by its owner is an expected exit.
    let cancelled = CriticalFailureState::new();
    let cancellable_context = context.service_context("cancelled");
    let cancellation = cancellable_context.task_group().cancellation_token();
    let cancellable = cancellable_context
        .spawn_critical_service("runs-until-cancelled", cancelled.clone(), async move {
            cancellation.cancelled().await;
        })
        .expect("the task should spawn");
    assert!(cancellable_context.task_group().contains_task(cancellable));

    cancellable_context.task_group().cancel();
    wait_until(
        || !cancellable_context.task_group().contains_task(cancellable),
        "the cancelled service to stop",
    )
    .await;
    assert_eq!(
        cancelled.occurrences(),
        0,
        "owner cancellation is an expected exit, not a critical failure"
    );
}

#[tokio::test]
async fn a_full_notification_channel_does_not_lose_the_failure() {
    let failures = CriticalFailureState::new();
    let mut subscription = failures.subscribe(1).expect("a positive capacity is valid");

    let first = failures.record(CriticalFailureKind::Panicked, TaskKind::Worker);
    let second = failures.record(CriticalFailureKind::Unrecoverable, TaskKind::Worker);

    assert_eq!(subscription.try_recv(), Some(first));
    assert_eq!(
        subscription.try_recv(),
        None,
        "the second notification is dropped by the full channel"
    );
    assert_eq!(
        failures.pending(),
        Some(first),
        "the first unhandled failure stays pending as the authority"
    );
    assert_eq!(failures.occurrences(), 2, "the dropped notification is still counted");
    assert_eq!(second.sequence(), 2);

    assert_eq!(failures.handle(), Some(first));
    assert_eq!(failures.pending(), None);

    assert!(failures.subscribe(0).is_err(), "a zero capacity is rejected");
}

#[tokio::test]
async fn a_non_service_critical_task_records_panics_only() {
    let context = RuntimeContext::from_current("critical-worker");
    let failures = CriticalFailureState::new();
    let monitored = context.service_context("monitored");

    monitored
        .spawn_critical("completes-normally", TaskKind::Worker, failures.clone(), async {})
        .expect("the task should spawn");
    tokio::task::yield_now().await;
    assert_eq!(
        failures.occurrences(),
        0,
        "a worker that returns normally is not a critical failure"
    );

    context
        .service_context("panicking")
        .spawn_critical("panics", TaskKind::Worker, failures.clone(), async {
            panic!("injected worker panic");
        })
        .expect("the task should spawn");
    wait_until(|| failures.pending().is_some(), "the worker panic").await;
    assert_eq!(
        failures.pending().expect("a pending record exists").kind(),
        CriticalFailureKind::Panicked
    );
    assert_eq!(
        failures.pending().expect("a pending record exists").task_kind(),
        TaskKind::Worker
    );
}

#[tokio::test]
async fn the_handling_policy_decides_the_lifecycle_outcome() {
    let context = RuntimeContext::from_current("critical-policy");
    let supervisor = context.service_context("supervisor");
    let lifecycle = new_lifecycle("critical-policy-service");
    lifecycle.mark_ready().expect("the lifecycle is startable");

    let failing = CriticalFailureState::new();
    lifecycle
        .spawn_critical_failure_monitor(
            &supervisor.task_spawner(),
            &failing,
            CriticalFailureRecovery::FailService,
        )
        .expect("the monitor owner is open");
    context
        .service_context("monitored")
        .spawn_critical_service("fails", failing.clone(), async {})
        .expect("the task should spawn");
    wait_until(
        || lifecycle.state() == ServiceLifecycleState::Failed,
        "the failed state",
    )
    .await;
    assert!(
        lifecycle.shutdown_request().is_none(),
        "failing alone does not request shutdown"
    );

    let shutdown = CriticalFailureState::new();
    let ordered = new_lifecycle("critical-policy-ordered");
    ordered.mark_ready().expect("the lifecycle is startable");
    ordered
        .spawn_critical_failure_monitor(
            &supervisor.task_spawner(),
            &shutdown,
            CriticalFailureRecovery::FailAndRequestShutdown,
        )
        .expect("the monitor owner is open");
    context
        .service_context("ordered")
        .spawn_critical_service("orders-shutdown", shutdown.clone(), async {})
        .expect("the task should spawn");
    wait_until(|| ordered.shutdown_request().is_some(), "the ordered shutdown").await;
    assert_eq!(
        ordered.shutdown_request().expect("a request exists").reason,
        ShutdownReason::Internal
    );
    assert_eq!(ordered.state(), ServiceLifecycleState::Failed);
}
