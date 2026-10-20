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

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::CriticalFailureKind;
use rocketmq_runtime::CriticalFailureState;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::ResourceBudgetTree;
use rocketmq_runtime::ResourcePermit;
use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::TaskKind;
use tokio::sync::oneshot;

struct PendingResource {
    permit: Option<ResourcePermit>,
    dropped: Arc<AtomicBool>,
    started: Option<oneshot::Sender<()>>,
    drop_gate: Option<(oneshot::Sender<()>, mpsc::Receiver<()>)>,
    panic_on_drop: bool,
}

impl Future for PendingResource {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        if let Some(started) = self.started.take() {
            let _ = started.send(());
        }
        Poll::Pending
    }
}

impl Drop for PendingResource {
    fn drop(&mut self) {
        if let Some((started, release)) = self.drop_gate.take() {
            let _ = started.send(());
            let _ = release.recv();
        }
        drop(self.permit.take());
        self.dropped.store(true, Ordering::Release);
        assert!(!self.panic_on_drop, "injected destructor panic");
    }
}

fn pending_resource() -> (ResourceBudgetTree, Arc<AtomicBool>, PendingResource) {
    let budget = ResourceBudgetTree::new("task-resource", BudgetLimit::new(1, 8, FullPolicy::Reject)).unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let future = PendingResource {
        permit: Some(budget.root().try_acquire_data(8).unwrap()),
        dropped: dropped.clone(),
        started: None,
        drop_gate: None,
        panic_on_drop: false,
    };
    (budget, dropped, future)
}

#[tokio::test]
async fn group_abort_before_first_poll_confirms_resource_destruction() {
    let context = RuntimeContext::from_current("before-poll");
    let group = context.service_context("service").task_group().clone();
    let (budget, dropped, future) = pending_resource();
    let (id, handle) = group.spawn_service_with_handle("pending", future).unwrap();
    assert!(group.abort_task_and_wait(id, Duration::from_secs(1)).await);
    assert!(dropped.load(Ordering::Acquire));
    assert_eq!(budget.root().snapshot().current_bytes, 0);
    assert!(handle.await.unwrap_err().is_cancelled());
    assert!(!group.contains_task(id));
    let report = group.shutdown(Duration::from_secs(1)).await;
    assert_eq!(report.aborted, 1);
    assert_eq!(report.completed + report.cancelled + report.panicked + report.leaked, 0);
}

#[tokio::test]
async fn external_abort_before_first_poll_settles_the_registry() {
    let context = RuntimeContext::from_current("external-before-poll");
    let group = context.service_context("service").task_group().clone();
    let (budget, dropped, future) = pending_resource();
    let (id, handle) = group.spawn_service_with_handle("pending", future).unwrap();
    handle.abort();
    assert!(handle.await.unwrap_err().is_cancelled());
    assert!(dropped.load(Ordering::Acquire));
    assert_eq!(budget.root().snapshot().current_bytes, 0);
    assert!(!group.contains_task(id));
    let report = group.shutdown(Duration::from_secs(1)).await;
    assert_eq!(report.aborted, 1);
    assert!(report.is_healthy(), "{}", report.to_json());
}

#[tokio::test]
async fn external_abort_after_first_poll_settles_the_registry() {
    let context = RuntimeContext::from_current("external-running");
    let group = context.service_context("service").task_group().clone();
    let (budget, dropped, mut future) = pending_resource();
    let (started_tx, started_rx) = oneshot::channel();
    future.started = Some(started_tx);
    let (id, handle) = group.spawn_service_with_handle("pending", future).unwrap();
    started_rx.await.unwrap();
    handle.abort();
    assert!(handle.await.unwrap_err().is_cancelled());
    assert!(group.wait_task(id, Duration::ZERO).await);
    assert!(dropped.load(Ordering::Acquire));
    assert_eq!(budget.root().snapshot().current_bytes, 0);
    let report = group.shutdown(Duration::from_secs(1)).await;
    assert_eq!(report.aborted, 1);
    assert_eq!(report.completed + report.cancelled + report.panicked + report.leaked, 0);
}

#[tokio::test]
async fn destructor_panic_settles_without_losing_completion() {
    let context = RuntimeContext::from_current("destructor-panic");
    let group = context.service_context("service").task_group().clone();
    let (budget, dropped, mut future) = pending_resource();
    let (started_tx, started_rx) = oneshot::channel();
    future.started = Some(started_tx);
    future.panic_on_drop = true;
    let (id, handle) = group.spawn_service_with_handle("pending", future).unwrap();
    started_rx.await.unwrap();
    handle.abort();
    assert!(handle.await.unwrap_err().is_panic());
    assert!(group.wait_task(id, Duration::ZERO).await);
    assert!(dropped.load(Ordering::Acquire));
    assert_eq!(budget.root().snapshot().current_bytes, 0);
    let report = group.shutdown(Duration::from_secs(1)).await;
    assert_eq!(report.panicked, 1);
    assert_eq!(report.aborted + report.completed + report.cancelled + report.leaked, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_keeps_the_record_while_the_user_destructor_is_running() {
    let context = RuntimeContext::from_current("destruction-gate");
    let group = context.service_context("service").task_group().clone();
    let (budget, dropped, mut future) = pending_resource();
    let (started_tx, started_rx) = oneshot::channel();
    let (drop_started_tx, drop_started_rx) = oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    future.started = Some(started_tx);
    future.drop_gate = Some((drop_started_tx, release_rx));
    let (id, handle) = group.spawn_service_with_handle("pending", future).unwrap();
    started_rx.await.unwrap();
    assert!(group.abort_task(id));
    drop_started_rx.await.unwrap();
    assert!(group.contains_task(id));
    assert!(!group.wait_task(id, Duration::ZERO).await);
    assert!(!dropped.load(Ordering::Acquire));
    assert_eq!(budget.root().snapshot().current_bytes, 8);
    let report = group.shutdown_now();
    assert_eq!(report.aborted, 0);
    assert_eq!(report.leaked, 1);
    assert!(!report.is_healthy());

    release_tx.send(()).unwrap();
    assert!(handle.await.unwrap_err().is_cancelled());
    assert!(group.wait_task(id, Duration::from_secs(1)).await);
    assert!(dropped.load(Ordering::Acquire));
    assert_eq!(budget.root().snapshot().current_bytes, 0);
    assert!(!group.contains_task(id));
    // The already-returned report is a snapshot, not a promise of later state.
    assert_eq!(report.aborted, 0);
    assert_eq!(report.leaked, 1);
}

#[tokio::test]
async fn a_task_id_from_another_group_is_never_treated_as_local() {
    let context = RuntimeContext::from_current("foreign-task-id");
    let first = context.service_context("first").task_group().clone();
    let second = context.service_context("second").task_group().clone();
    let first_id = first.spawn_service("pending", std::future::pending::<()>()).unwrap();
    let second_id = second.spawn_service("pending", std::future::pending::<()>()).unwrap();

    // Both groups number their tasks from the same start, but ids stay distinct.
    assert_eq!(first_id.as_u64(), second_id.as_u64());
    assert_ne!(first_id, second_id);
    assert_eq!(first_id.group_id(), first.id());
    assert!(first.owns_task(first_id));
    assert!(!second.owns_task(first_id));

    // The foreign id must neither reach the same-numbered local task nor be
    // reported as finished while its owner still runs it.
    assert!(!second.contains_task(first_id));
    assert!(!second.abort_task(first_id));
    assert!(second.contains_task(second_id));
    let foreign_wait = tokio::time::timeout(
        Duration::from_secs(5),
        second.wait_task(first_id, Duration::from_secs(60)),
    )
    .await;
    assert_eq!(foreign_wait, Ok(false));
    assert!(!second.abort_task_and_wait(first_id, Duration::from_secs(1)).await);
    assert!(first.contains_task(first_id));

    assert!(first.abort_task_and_wait(first_id, Duration::from_secs(1)).await);
    assert!(first.wait_task(first_id, Duration::ZERO).await);
    assert!(!second.wait_task(first_id, Duration::ZERO).await);
    assert!(second.abort_task_and_wait(second_id, Duration::from_secs(1)).await);
}

#[tokio::test]
async fn repeated_abort_and_normal_completion_each_settle_once() {
    let context = RuntimeContext::from_current("single-settlement");
    let group = context.service_context("service").task_group().clone();
    let (id, pending) = group
        .spawn_service_with_handle("pending", std::future::pending())
        .unwrap();
    assert!(group.abort_task(id));
    assert!(group.abort_task(id));
    assert!(pending.await.unwrap_err().is_cancelled());
    let (_, completed) = group.spawn_service_with_handle("completed", async {}).unwrap();
    completed.await.unwrap();
    let report = group.shutdown(Duration::from_secs(1)).await;
    assert_eq!(report.aborted, 1);
    assert_eq!(report.completed, 1);
    assert_eq!(report.cancelled + report.panicked + report.leaked, 0);
}

#[tokio::test]
async fn panic_propagation_also_releases_user_resources_once() {
    let context = RuntimeContext::from_current("panic-completion");
    let group = context.service_context("service").task_group().clone();
    let (budget, dropped, resource) = pending_resource();
    let (_, handle) = group
        .spawn_service_with_handle("panicking", async move {
            let _resource = resource;
            panic!("injected task panic");
        })
        .unwrap();
    assert!(handle.await.unwrap_err().is_panic());
    assert!(dropped.load(Ordering::Acquire));
    assert_eq!(budget.root().snapshot().current_bytes, 0);
    let report = group.shutdown(Duration::from_secs(1)).await;
    assert_eq!(report.panicked, 1);
    assert_eq!(report.aborted + report.completed + report.cancelled + report.leaked, 0);
}

#[tokio::test]
async fn critical_destructor_panic_is_recorded_before_completion_before_and_after_poll() {
    for poll_first in [false, true] {
        let context = RuntimeContext::from_current("critical-destructor-panic");
        let group = context.service_context("service").task_group().clone();
        let failures = CriticalFailureState::new();
        let (budget, dropped, mut future) = pending_resource();
        let (started_tx, started_rx) = oneshot::channel();
        future.started = Some(started_tx);
        future.panic_on_drop = true;
        let id = group
            .spawn_critical("pending", TaskKind::Worker, failures.clone(), future)
            .unwrap();
        if poll_first {
            started_rx.await.unwrap();
        }
        assert!(group.abort_task_and_wait(id, Duration::from_secs(1)).await);
        assert!(dropped.load(Ordering::Acquire));
        assert_eq!(budget.root().snapshot().current_bytes, 0);
        assert_eq!(failures.occurrences(), 1);
        assert_eq!(failures.handle().unwrap().kind(), CriticalFailureKind::Panicked);
        let report = group.shutdown(Duration::from_secs(1)).await;
        assert_eq!(report.panicked, 1);
        assert_eq!(report.aborted + report.completed + report.cancelled + report.leaked, 0);
    }
}

#[tokio::test]
async fn critical_abort_without_panic_is_not_a_failure() {
    let context = RuntimeContext::from_current("critical-abort");
    let group = context.service_context("service").task_group().clone();
    let failures = CriticalFailureState::new();
    let id = group
        .spawn_critical_service("pending", failures.clone(), std::future::pending())
        .unwrap();
    assert!(group.abort_task_and_wait(id, Duration::from_secs(1)).await);
    assert_eq!(failures.occurrences(), 0);
    assert_eq!(group.shutdown(Duration::from_secs(1)).await.aborted, 1);
}
