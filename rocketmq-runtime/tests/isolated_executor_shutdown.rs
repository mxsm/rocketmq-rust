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

use rocketmq_runtime::{
    BlockingExecutor, BlockingPoolPolicy, RuntimeContext, RuntimeErrorKind, ShutdownDeadline, TaskKind,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc, Arc,
};
use std::time::Duration;

#[tokio::test]
async fn shutdown_wakes_queued_work_and_waits_for_the_actual_closure_without_closing_siblings() {
    let context = RuntimeContext::try_from_current("isolated-shutdown").unwrap();
    let service = context.service_context("service");
    let executor = BlockingExecutor::new_isolated(
        BlockingPoolPolicy {
            max_concurrency: 1,
            max_queue_depth: 4,
            task_timeout: Duration::from_secs(10),
            ..Default::default()
        },
        service.task_group().clone(),
    )
    .unwrap();
    let sibling = BlockingExecutor::new(BlockingPoolPolicy::default(), service.task_group().clone()).unwrap();
    let (release, wait) = mpsc::channel();
    let (started, start) = tokio::sync::oneshot::channel();
    let running_executor = executor.clone();
    let (_, running) = service
        .task_group()
        .spawn_with_handle("held", TaskKind::Worker, async move {
            running_executor
                .spawn_io("held", move || {
                    started.send(()).unwrap();
                    wait.recv_timeout(Duration::from_secs(10)).unwrap();
                })
                .await
                .unwrap();
        })
        .unwrap();
    start.await.unwrap();
    let executed = Arc::new(AtomicBool::new(false));
    let marker = executed.clone();
    let mut queued = Box::pin(executor.spawn_io("queued", move || marker.store(true, Ordering::Release)));
    assert!(futures::poll!(queued.as_mut()).is_pending());
    assert_eq!(executor.snapshot().queued, 1);
    executor.clone().stop_admission().unwrap();
    assert_eq!(queued.await.unwrap_err().kind(), RuntimeErrorKind::Closed);
    assert!(!executed.load(Ordering::Acquire));
    let report = executor
        .shutdown_until(ShutdownDeadline::after(Duration::ZERO))
        .await
        .unwrap();
    assert!(!report.completed);
    assert_eq!(report.pending_operations, 1);
    assert_eq!(report.snapshot.blocking_still_running, 1);
    assert_eq!(
        executor.spawn_io("late", || ()).await.unwrap_err().kind(),
        RuntimeErrorKind::Closed
    );
    assert_eq!(sibling.spawn_io("sibling", || 42).await.unwrap(), 42);
    release.send(()).unwrap();
    running.await.unwrap();
    let report = executor
        .shutdown_until(ShutdownDeadline::after(Duration::from_secs(5)))
        .await
        .unwrap();
    assert!(report.completed);
    assert_eq!(report.pending_operations, 0);
    assert!(report.snapshot.tasks.is_empty());
    assert!(
        sibling
            .shutdown_until(ShutdownDeadline::after(Duration::from_secs(5)))
            .await
            .unwrap()
            .completed
    );
    assert!(context.shutdown_tasks(Duration::from_secs(5)).await.is_healthy());
}

#[tokio::test]
async fn managed_lanes_cannot_be_closed_through_an_executor_handle() {
    let context = RuntimeContext::try_from_current("managed-lifecycle").unwrap();
    let service = context.service_context("service");
    assert!(service.metadata_io().stop_admission().is_err());
    assert!(service
        .metadata_io()
        .shutdown_until(ShutdownDeadline::after(Duration::ZERO))
        .await
        .is_err());
    assert_eq!(service.metadata_io().spawn_io("still-open", || 42).await.unwrap(), 42);
    assert!(context.shutdown_tasks(Duration::from_secs(5)).await.is_healthy());
}

#[tokio::test]
async fn panicking_closures_release_independent_tracking() {
    let context = RuntimeContext::try_from_current("isolated-panic").unwrap();
    let service = context.service_context("service");
    let executor = BlockingExecutor::new_isolated(BlockingPoolPolicy::default(), service.task_group().clone()).unwrap();
    assert!(executor
        .spawn_io("panic", || panic!("injected blocking panic"))
        .await
        .is_err());
    let report = executor
        .shutdown_until(ShutdownDeadline::after(Duration::from_secs(5)))
        .await
        .unwrap();
    assert!(report.completed);
    assert_eq!(report.snapshot.global_running, 0);
    assert!(context.shutdown_tasks(Duration::from_secs(5)).await.is_healthy());
}
