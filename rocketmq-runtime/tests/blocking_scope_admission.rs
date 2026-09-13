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

use std::num::NonZeroUsize;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_error::CanonicalCondition;
use rocketmq_runtime::BlockingPoolPolicy;
use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::ShutdownDeadline;
use tokio::sync::oneshot;
use tokio::sync::Notify;

fn scoped_context_with_queue_depth(max_queue_depth: usize) -> RuntimeContext {
    RuntimeContext::try_from_current_with_blocking_policy(
        "blocking-scope",
        BlockingPoolPolicy {
            max_concurrency: 1,
            max_queue_depth,
            queue_timeout: Duration::from_secs(5),
            task_timeout: Duration::from_secs(5),
            ..BlockingPoolPolicy::default()
        },
    )
    .unwrap()
}

fn scoped_context() -> RuntimeContext {
    scoped_context_with_queue_depth(2)
}

#[tokio::test]
async fn closing_one_scope_rejects_normal_work_without_stopping_a_sibling() {
    let context = scoped_context();
    let draining = context.service_context("draining");
    let sibling = context.service_context("sibling");

    draining.task_group().cancel();
    let error = draining.storage_io().spawn_io("closed-scope", || 1).await.unwrap_err();
    assert_eq!(error.condition(), CanonicalCondition::Unavailable);

    assert_eq!(sibling.storage_io().spawn_io("sibling", || 2).await.unwrap(), 2);
    assert!(context.shutdown_tasks(Duration::from_secs(1)).await.is_healthy());
}

#[tokio::test]
async fn cancellation_wakes_an_admission_waiter_before_the_lane_deadline() {
    let context = scoped_context();
    let scope = context.service_context("waiting");
    let executor = scope.storage_io().clone();
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let first = tokio::spawn(async move {
        executor
            .spawn_io("holding-capacity", move || {
                let _ = started_tx.send(());
                let _ = release_rx.recv();
            })
            .await
    });
    started_rx.await.unwrap();

    let waiter_started = Arc::new(Notify::new());
    let waiter_started_signal = Arc::clone(&waiter_started);
    let waiting_executor = scope.storage_io().clone();
    let waiter = tokio::spawn(async move {
        waiter_started_signal.notify_one();
        waiting_executor.spawn_io("waiting", || 7).await
    });
    waiter_started.notified().await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while scope.storage_io().snapshot().queued != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    scope.task_group().cancel();
    let error = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.condition(), CanonicalCondition::Unavailable);
    assert_eq!(scope.storage_io().snapshot().queued, 0);

    release_tx.send(()).unwrap();
    first.await.unwrap().unwrap();
    assert!(context.shutdown_tasks(Duration::from_secs(1)).await.is_healthy());
}

#[tokio::test]
async fn preaccepted_drain_lease_allows_only_its_bounded_finalization_io() {
    let context = scoped_context();
    let scope = context.service_context("finalization");
    let sibling = context.service_context("sibling");
    let lease = scope
        .metadata_io()
        .try_drain_lease(
            ShutdownDeadline::after(Duration::from_secs(1)),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();

    scope.task_group().cancel();
    let regular = scope
        .metadata_io()
        .spawn_io("ordinary-after-close", || 1)
        .await
        .unwrap_err();
    assert_eq!(regular.condition(), CanonicalCondition::Unavailable);
    assert_eq!(lease.spawn_io("final-metadata-flush", || 2).await.unwrap(), 2);
    assert_eq!(lease.remaining_operations(), 0);
    let exhausted = lease.spawn_io("another-flush", || 3).await.unwrap_err();
    assert_eq!(exhausted.condition(), CanonicalCondition::ResourceExhausted);

    assert_eq!(sibling.metadata_io().spawn_io("sibling", || 4).await.unwrap(), 4);
    assert!(scope
        .metadata_io()
        .try_drain_lease(
            ShutdownDeadline::after(Duration::from_secs(1)),
            NonZeroUsize::new(1).unwrap()
        )
        .is_err());
    assert!(context.shutdown_tasks(Duration::from_secs(1)).await.is_healthy());
}

#[tokio::test]
async fn root_cancellation_closes_normal_scopes_but_preserves_preaccepted_finalization() {
    let context = scoped_context();
    let scope = context.service_context("root-finalization");
    let sibling = context.service_context("root-sibling");
    let lease = scope
        .storage_io()
        .try_drain_lease(
            ShutdownDeadline::after(Duration::from_secs(1)),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();

    context.root_group().cancel();
    let regular = scope
        .storage_io()
        .spawn_io("ordinary-after-root-close", || 1)
        .await
        .unwrap_err();
    assert_eq!(regular.condition(), CanonicalCondition::Unavailable);
    let sibling_error = sibling
        .storage_io()
        .spawn_io("ordinary-sibling-after-root-close", || 3)
        .await
        .unwrap_err();
    assert_eq!(sibling_error.condition(), CanonicalCondition::Unavailable);
    assert_eq!(lease.spawn_io("root-final-flush", || 2).await.unwrap(), 2);
    assert!(context.shutdown_tasks(Duration::from_secs(1)).await.is_healthy());
}

#[tokio::test]
async fn drain_lease_never_extends_its_original_deadline() {
    let context = scoped_context();
    let scope = context.service_context("expired-finalization");
    let error = scope
        .storage_io()
        .try_drain_lease(ShutdownDeadline::after(Duration::ZERO), NonZeroUsize::new(1).unwrap())
        .unwrap_err();
    assert_eq!(error.condition(), CanonicalCondition::DeadlineExceeded);
    assert!(context.shutdown_tasks(Duration::from_secs(1)).await.is_healthy());
}

#[tokio::test]
async fn root_shutdown_deadline_bounds_a_preaccepted_finalization_slot() {
    let context = scoped_context();
    let scope = context.service_context("bounded-finalization");
    let lease = scope
        .storage_io()
        .try_drain_lease(
            ShutdownDeadline::after(Duration::from_secs(5)),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
    let called = Arc::new(AtomicBool::new(false));
    let operation_called = Arc::clone(&called);

    let _ = context
        .shutdown_tasks_until(ShutdownDeadline::after(Duration::ZERO))
        .await;
    let error = lease
        .spawn_io("root-deadline-expired", move || {
            operation_called.store(true, Ordering::Release);
        })
        .await
        .unwrap_err();

    assert_eq!(error.condition(), CanonicalCondition::DeadlineExceeded);
    assert!(!called.load(Ordering::Acquire));
    assert_eq!(lease.remaining_operations(), 1);
}

#[tokio::test]
async fn finalization_slot_waits_for_queue_capacity_released_by_closing_scope() {
    let context = scoped_context_with_queue_depth(1);
    let scope = context.service_context("queued-finalization");
    let executor = scope.storage_io().clone();
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let first = tokio::spawn(async move {
        executor
            .spawn_io("holding-capacity", move || {
                let _ = started_tx.send(());
                let _ = release_rx.recv();
                1
            })
            .await
    });
    started_rx.await.unwrap();

    let queued_executor = scope.storage_io().clone();
    let queued = tokio::spawn(async move { queued_executor.spawn_io("queued-normal", || 2).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while scope.storage_io().snapshot().queued != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let lease = scope
        .storage_io()
        .try_drain_lease(
            ShutdownDeadline::after(Duration::from_secs(1)),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
    scope.task_group().cancel();
    let finalization = lease.spawn_io("final-after-queue-drain", || 3);
    tokio::pin!(finalization);

    // The queued ordinary submission still owns the only queue permit until
    // cancellation is polled. A drain slot must wait for that bounded release
    // instead of failing this finalization with an immediate capacity error.
    assert!(futures::poll!(&mut finalization).is_pending());
    let queued_error = queued.await.unwrap().unwrap_err();
    assert_eq!(queued_error.condition(), CanonicalCondition::Unavailable);

    release_tx.send(()).unwrap();
    assert_eq!(first.await.unwrap().unwrap(), 1);
    assert_eq!(finalization.await.unwrap(), 3);
    assert_eq!(lease.remaining_operations(), 0);
}

#[tokio::test]
async fn drain_lease_allowance_is_shared_across_concurrent_submissions() {
    let context = scoped_context();
    let scope = context.service_context("shared-allowance");
    let lease = scope
        .metadata_io()
        .try_drain_lease(
            ShutdownDeadline::after(Duration::from_secs(1)),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();

    let (first, second) = tokio::join!(
        lease.spawn_io("first-final-flush", || 1),
        lease.spawn_io("second-final-flush", || 2)
    );
    let mut successes = 0;
    let mut exhausted = 0;
    for result in [first, second] {
        match result {
            Ok(_) => successes += 1,
            Err(error) => {
                assert_eq!(error.condition(), CanonicalCondition::ResourceExhausted);
                exhausted += 1;
            }
        }
    }

    assert_eq!(successes, 1);
    assert_eq!(exhausted, 1);
    assert_eq!(lease.remaining_operations(), 0);
    assert!(context.shutdown_tasks(Duration::from_secs(1)).await.is_healthy());
}

#[tokio::test]
async fn finalization_submission_cannot_extend_slot_deadline() {
    let context = scoped_context();
    let scope = context.service_context("submission-deadline");
    let lease = scope
        .storage_io()
        .try_drain_lease(
            ShutdownDeadline::after(Duration::from_secs(5)),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
    let called = Arc::new(AtomicBool::new(false));
    let operation_called = Arc::clone(&called);

    let error = lease
        .spawn_io_until(
            "expired-final-flush",
            ShutdownDeadline::after(Duration::ZERO),
            move || {
                operation_called.store(true, Ordering::Release);
            },
        )
        .await
        .unwrap_err();

    assert_eq!(error.condition(), CanonicalCondition::DeadlineExceeded);
    assert!(!called.load(Ordering::Acquire));
    assert_eq!(lease.remaining_operations(), 1);
    assert!(context.shutdown_tasks(Duration::from_secs(1)).await.is_healthy());
}
