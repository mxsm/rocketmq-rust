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

use std::error::Error;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc;

use rocketmq_error::CanonicalCondition;
use tokio::sync::oneshot;

use super::*;

fn executor() -> BlockingExecutor {
    BlockingExecutor::new_with_budget(
        BlockingPoolPolicy {
            max_concurrency: 1,
            ..BlockingPoolPolicy::default()
        },
        BlockingLane::StorageIo,
        GlobalBlockingBudget::isolated(1),
        RuntimeHandle::new(tokio::runtime::Handle::current()),
    )
}

async fn gated_task(executor: &BlockingExecutor) -> (BlockingTask<usize>, mpsc::Sender<()>) {
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let task = executor
        .submit_io("gated", move || {
            let _ = started_tx.send(());
            let _ = release_rx.recv();
            42
        })
        .await
        .unwrap();
    started_rx.await.unwrap();
    (task, release_tx)
}

fn assert_released(executor: &BlockingExecutor) {
    let snapshot = executor.snapshot();
    assert!(snapshot.tasks.is_empty());
    assert_eq!(snapshot.global_running, 0);
    assert_eq!(snapshot.global_available, 1);
}

#[tokio::test]
async fn diagnostics_aggregate_tracks_real_blocking_timeout_and_completion() {
    let executor = executor();
    let (mut task, release) = gated_task(&executor).await;
    let aggregate = executor.aggregate();
    let detailed = executor.snapshot();
    assert_eq!(aggregate.lane, detailed.lane);
    assert_eq!(aggregate.max_concurrency, detailed.max_concurrency);
    assert_eq!(aggregate.max_queue_depth, detailed.max_queue_depth);
    assert_eq!(aggregate.queued, detailed.queued);
    assert_eq!(aggregate.running, detailed.running);
    assert_eq!(aggregate.blocking_still_running, detailed.blocking_still_running);
    assert_eq!(aggregate.task_kinds[0].1, 1);
    assert_eq!(aggregate.task_kinds[1].1, 0);
    assert_eq!(aggregate.task_kinds[2].1, 0);
    assert!(aggregate.task_kinds[0].2 <= detailed.tasks[0].elapsed);

    assert!(task.wait_until(Instant::now()).await.is_err());
    let aggregate = executor.aggregate();
    assert_eq!(aggregate.running, 0);
    assert_eq!(aggregate.timed_out_still_running, 1);
    assert_eq!(aggregate.blocking_still_running, 1);
    assert_eq!(aggregate.task_kinds[0].1, 1);

    release.send(()).unwrap();
    assert_eq!(task.wait().await.unwrap(), 42);
    let aggregate = executor.aggregate();
    assert_eq!(aggregate.queued, 0);
    assert_eq!(aggregate.blocking_still_running, 0);
    assert!(aggregate.task_kinds.iter().all(|(_, count, _)| *count == 0));
}

#[tokio::test]
async fn expired_observation_retains_capacity_and_the_late_result() {
    let executor = executor();
    let (mut task, release) = gated_task(&executor).await;

    let error = task.wait_until(Instant::now()).await.unwrap_err();
    assert_eq!(error.condition(), CanonicalCondition::DeadlineExceeded);
    let snapshot = executor.snapshot();
    assert_eq!(snapshot.timed_out_still_running, 1);
    assert_eq!(snapshot.global_running, 1);
    assert_eq!(snapshot.global_available, 0);

    release.send(()).unwrap();
    assert_eq!(task.wait().await.unwrap(), 42);
    assert_released(&executor);
    assert_eq!(
        task.wait().await.unwrap_err().condition(),
        CanonicalCondition::Unavailable
    );
}

#[tokio::test]
async fn dropping_one_wait_does_not_consume_execution_completion() {
    let executor = executor();
    let (mut task, release) = gated_task(&executor).await;
    {
        let wait = task.wait();
        tokio::pin!(wait);
        assert!(futures::poll!(&mut wait).is_pending());
    }
    assert_eq!(executor.snapshot().global_running, 1);

    release.send(()).unwrap();
    assert_eq!(task.wait().await.unwrap(), 42);
    assert_released(&executor);
}

#[tokio::test]
async fn abandoning_the_ticket_keeps_real_work_tracked_until_exit() {
    let executor = executor();
    let (task, release) = gated_task(&executor).await;
    let completion = task.join_handle.as_ref().unwrap().abort_handle();
    drop(task);
    assert_eq!(executor.snapshot().timed_out_still_running, 1);
    assert_eq!(executor.snapshot().global_available, 0);

    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !completion.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_released(&executor);
}

struct CaptureProbe {
    executor: BlockingExecutor,
    dropped: Arc<AtomicBool>,
}

impl Drop for CaptureProbe {
    fn drop(&mut self) {
        // User captures must be destroyed before releasing the operation's
        // budget or removing the active record, even before its first run.
        let snapshot = self.executor.snapshot();
        self.dropped.store(
            snapshot.global_running == 1 && snapshot.tasks.len() == 1,
            Ordering::Release,
        );
    }
}

#[test]
fn cancellation_before_execution_releases_captures_capacity_and_registry() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        // Occupy Tokio's only blocking thread, independently of lane admission,
        // so the tested closure is submitted but cannot start.
        let occupied = tokio::task::spawn_blocking(move || {
            let _ = started_tx.send(());
            let _ = release_rx.recv();
        });
        started_rx.await.unwrap();

        let executor = executor();
        let dropped = Arc::new(AtomicBool::new(false));
        let capture = CaptureProbe {
            executor: executor.clone(),
            dropped: dropped.clone(),
        };
        let mut task = executor
            .submit_io("never-started", move || {
                drop(capture);
                panic!("cancelled operation must not execute");
            })
            .await
            .unwrap();
        task.join_handle.as_ref().unwrap().abort();
        // Let Tokio dequeue and destroy the aborted closure, then await both
        // operations before the test runtime is dropped.
        release_tx.send(()).unwrap();
        occupied.await.unwrap();
        let error = task.wait().await.unwrap_err();
        assert!(error
            .source()
            .unwrap()
            .downcast_ref::<tokio::task::JoinError>()
            .unwrap()
            .is_cancelled());
        assert!(dropped.load(Ordering::Acquire));
        assert_released(&executor);
    });
}

#[tokio::test]
async fn panic_settles_execution_and_destroys_user_resources() {
    let executor = executor();
    let dropped = Arc::new(AtomicBool::new(false));
    let capture = CaptureProbe {
        executor: executor.clone(),
        dropped: dropped.clone(),
    };
    let mut task = executor
        .submit_io("panicking", move || {
            let _capture = capture;
            panic!("injected blocking failure");
        })
        .await
        .unwrap();
    let error = task.wait().await.unwrap_err();
    assert_eq!(error.operation(), crate::RuntimeOperation::RunBlockingTask);
    assert!(error
        .source()
        .unwrap()
        .downcast_ref::<tokio::task::JoinError>()
        .unwrap()
        .is_panic());
    assert!(!error.to_string().contains("injected blocking failure"));
    assert!(dropped.load(Ordering::Acquire));
    assert_released(&executor);
    assert_eq!(executor.spawn_io("after-panic", || 7).await.unwrap(), 7);
    assert_released(&executor);
}

#[tokio::test]
async fn expired_drain_deadline_prevents_user_closure_from_starting() {
    let executor = executor();
    let task_id = BlockingTaskId(1);
    executor.tasks.insert(
        task_id,
        BlockingTaskMeta {
            id: task_id,
            name: Arc::from("expired-drain-work"),
            kind: BlockingKind::ShortIo,
            state: BlockingTaskState::Running,
            queued_at: Instant::now(),
            started_at: Some(Instant::now()),
        },
    );
    let permit = executor
        .budget
        .acquire(BlockingLane::StorageIo, Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    let called = Arc::new(AtomicBool::new(false));
    let operation_called = Arc::clone(&called);
    let work = BlockingWork {
        operation: move || operation_called.store(true, Ordering::Release),
        permit,
        completion: BlockingCompletionGuard {
            tasks: Arc::clone(&executor.tasks),
            task_id,
        },
        execution_deadline: Some(ShutdownDeadline::after(Duration::ZERO)),
    };

    let error = work.run().unwrap_err();
    assert_eq!(error.condition(), CanonicalCondition::DeadlineExceeded);
    assert!(!called.load(Ordering::Acquire));
    assert_released(&executor);
}
