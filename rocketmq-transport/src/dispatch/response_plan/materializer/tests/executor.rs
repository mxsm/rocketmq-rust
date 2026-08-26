// Copyright 2026 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Condvar;
use std::sync::Mutex;

use rocketmq_runtime::RuntimeError;

use super::*;

struct BlockingLease {
    file: File,
    accesses: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
    dropped: Arc<tokio::sync::Notify>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

impl FileRegionLease for BlockingLease {
    fn file(&self) -> &File {
        let access = self.accesses.fetch_add(1, Ordering::SeqCst);
        if access > 0 {
            self.entered.notify_one();
            let (released, condition) = &*self.release;
            let mut released = released.lock().expect("release lock");
            while !*released {
                released = condition.wait(released).expect("release wait");
            }
        }
        &self.file
    }
}

impl Drop for BlockingLease {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        self.dropped.notify_one();
    }
}

struct PanicAfterValidationLease {
    file: File,
    accesses: AtomicUsize,
    drops: Arc<AtomicUsize>,
}

impl FileRegionLease for PanicAfterValidationLease {
    fn file(&self) -> &File {
        if self.accesses.fetch_add(1, Ordering::SeqCst) > 0 {
            panic!("test-only file lease panic after validation");
        }
        &self.file
    }
}

impl Drop for PanicAfterValidationLease {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

struct BlockingRegionHarness {
    regions: FileRegionSequence,
    accesses: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
    dropped: Arc<tokio::sync::Notify>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

impl BlockingRegionHarness {
    fn new() -> Self {
        let mut file = tempfile::tempfile().expect("temporary file");
        file.write_all(b"blocking-file-body").expect("write file body");
        let accesses = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let dropped = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let lease = Arc::new(BlockingLease {
            file,
            accesses: Arc::clone(&accesses),
            drops: Arc::clone(&drops),
            entered: Arc::clone(&entered),
            dropped: Arc::clone(&dropped),
            release: Arc::clone(&release),
        });
        let region = FileRegion::try_new(lease, 0, 18).expect("blocking file region");
        Self {
            regions: FileRegionSequence::single(region),
            accesses,
            drops,
            entered,
            dropped,
            release,
        }
    }
}

fn blocking_policy(queue_timeout: Duration, task_timeout: Duration, max_queue_depth: usize) -> BlockingPoolPolicy {
    BlockingPoolPolicy {
        name: "legacy-materializer-blocking-test".to_owned(),
        max_concurrency: 1,
        max_queue_depth,
        queue_timeout,
        task_timeout,
        warn_after: Duration::from_secs(60),
    }
}

struct Occupant {
    release: Arc<(Mutex<bool>, Condvar)>,
    task: tokio::task::JoinHandle<Result<(), RuntimeError>>,
}

impl Occupant {
    async fn start(blocking: BlockingExecutor) -> Self {
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let started_wait = started.notified();
        let operation_started = Arc::clone(&started);
        let operation_release = Arc::clone(&release);
        let task = tokio::spawn(async move {
            blocking
                .spawn_io("legacy-materializer-test-occupant", move || {
                    operation_started.notify_one();
                    let (released, condition) = &*operation_release;
                    let mut released = released.lock().expect("occupant release lock");
                    while !*released {
                        released = condition.wait(released).expect("occupant release wait");
                    }
                })
                .await
        });
        started_wait.await;
        Self { release, task }
    }

    async fn release(self) {
        let (released, condition) = &*self.release;
        *released.lock().expect("occupant release lock") = true;
        condition.notify_all();
        self.task
            .await
            .expect("occupant task should join")
            .expect("occupant should complete");
    }
}

async fn wait_for_queued(blocking: &BlockingExecutor, expected: usize) {
    for _ in 0..10_000 {
        if blocking.snapshot().queued == expected {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("blocking executor did not reach queued={expected}");
}

async fn wait_for_running(blocking: &BlockingExecutor, expected: usize) {
    for _ in 0..10_000 {
        if blocking.blocking_still_running() == expected {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("blocking executor did not reach blocking_still_running={expected}");
}

async fn assert_duplicate_completed(duplicate: ResponseSink, code: i32) {
    assert!(matches!(
        duplicate
            .send_plan(bind(
                ResponsePlan::command(response_head(code, code)).expect("duplicate plan"),
                code as u64,
                code,
            ))
            .await,
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Completed
        })
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn running_parent_cancellation_returns_early_but_keeps_lease_tracked_until_closure_exit() {
    let policy = blocking_policy(Duration::from_secs(1), Duration::from_secs(5), 4);
    let (harness, control) = ControlHarness::with_policy("legacy-materializer-running-cancel", None, policy);
    let file = BlockingRegionHarness::new();
    let entered = file.entered.notified();
    let dropped = file.dropped.notified();
    let accesses = Arc::clone(&file.accesses);
    let drops = Arc::clone(&file.drops);
    let release = Arc::clone(&file.release);
    let (receiver, duplicate) = handoff(
        ResponsePlan::file_regions(response_head(101, 31), file.regions).expect("file plan"),
        control,
    )
    .await;
    let blocking = harness.blocking().clone();
    let materialization = tokio::spawn(async move { receiver.receive_command(limits(18, 1), &blocking).await });
    entered.await;
    assert_eq!(accesses.load(Ordering::SeqCst), 2);

    harness.parent.cancel();
    assert!(matches!(
        materialization.await.expect("materialization task"),
        Err(LegacyLocalMaterializationError::Cancelled)
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(harness.blocking().blocking_still_running(), 1);
    assert_duplicate_completed(duplicate, 102).await;

    let (released, condition) = &*release;
    *released.lock().expect("release lock") = true;
    condition.notify_all();
    dropped.await;
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    wait_for_running(harness.blocking(), 0).await;
    harness.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn running_request_deadline_wins_over_executor_policy_and_keeps_the_closure_tracked() {
    let request_deadline = RequestDeadline::after(Duration::from_secs(1));
    let policy = blocking_policy(Duration::from_secs(30), Duration::from_secs(30), 4);
    let (harness, control) = ControlHarness::with_policy(
        "legacy-materializer-running-request-deadline",
        Some(request_deadline),
        policy,
    );
    let file = BlockingRegionHarness::new();
    let entered = file.entered.notified();
    let dropped = file.dropped.notified();
    let accesses = Arc::clone(&file.accesses);
    let drops = Arc::clone(&file.drops);
    let release = Arc::clone(&file.release);
    let (receiver, duplicate) = handoff(
        ResponsePlan::file_regions(response_head(111, 38), file.regions).expect("file plan"),
        control,
    )
    .await;
    let blocking = harness.blocking().clone();
    let materialization = tokio::spawn(async move { receiver.receive_command(limits(18, 1), &blocking).await });
    entered.await;
    assert_eq!(accesses.load(Ordering::SeqCst), 2);

    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(request_deadline.is_expired());
    assert!(matches!(
        materialization.await.expect("materialization task"),
        Err(LegacyLocalMaterializationError::DeadlineExceeded)
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(harness.blocking().blocking_still_running(), 1);
    assert_duplicate_completed(duplicate, 112).await;

    let (released, condition) = &*release;
    *released.lock().expect("release lock") = true;
    condition.notify_all();
    dropped.await;
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    wait_for_running(harness.blocking(), 0).await;
    harness.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn running_session_close_wins_when_the_request_deadline_is_also_expired() {
    let request_deadline = RequestDeadline::after(Duration::from_secs(1));
    let policy = blocking_policy(Duration::from_secs(30), Duration::from_secs(30), 4);
    let (harness, control) = ControlHarness::with_policy(
        "legacy-materializer-running-session-deadline",
        Some(request_deadline),
        policy,
    );
    let file = BlockingRegionHarness::new();
    let entered = file.entered.notified();
    let dropped = file.dropped.notified();
    let drops = Arc::clone(&file.drops);
    let release = Arc::clone(&file.release);
    let (receiver, duplicate) = handoff(
        ResponsePlan::file_regions(response_head(113, 39), file.regions).expect("file plan"),
        control,
    )
    .await;
    let blocking = harness.blocking().clone();
    let materialization = tokio::spawn(async move { receiver.receive_command(limits(18, 1), &blocking).await });
    entered.await;

    harness
        ._closed_tx
        .send(true)
        .expect("session close observer should remain open");
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(request_deadline.is_expired());
    assert!(matches!(
        materialization.await.expect("materialization task"),
        Err(LegacyLocalMaterializationError::SessionClosed)
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(harness.blocking().blocking_still_running(), 1);
    assert_duplicate_completed(duplicate, 114).await;

    let (released, condition) = &*release;
    *released.lock().expect("release lock") = true;
    condition.notify_all();
    dropped.await;
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    wait_for_running(harness.blocking(), 0).await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn running_caller_future_drop_keeps_executor_tracking_and_prevents_a_second_receive() {
    let policy = blocking_policy(Duration::from_secs(1), Duration::from_secs(5), 4);
    let (harness, control) = ControlHarness::with_policy("legacy-materializer-running-drop", None, policy);
    let file = BlockingRegionHarness::new();
    let entered = file.entered.notified();
    let dropped = file.dropped.notified();
    let drops = Arc::clone(&file.drops);
    let release = Arc::clone(&file.release);
    let (receiver, duplicate) = handoff(
        ResponsePlan::file_regions(response_head(103, 32), file.regions).expect("file plan"),
        control,
    )
    .await;
    let blocking = harness.blocking().clone();
    let materialization = tokio::spawn(async move { receiver.receive_command(limits(18, 1), &blocking).await });
    entered.await;

    materialization.abort();
    match materialization.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("caller future should have been aborted"),
    }
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(harness.blocking().blocking_still_running(), 1);
    assert_duplicate_completed(duplicate, 104).await;

    let (released, condition) = &*release;
    *released.lock().expect("release lock") = true;
    condition.notify_all();
    dropped.await;
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    wait_for_running(harness.blocking(), 0).await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_caller_future_drop_never_reads_the_file_and_releases_the_unstarted_lease_once() {
    let policy = blocking_policy(Duration::from_secs(5), Duration::from_secs(5), 4);
    let (harness, control) = ControlHarness::with_policy("legacy-materializer-queued-drop", None, policy);
    let occupant = Occupant::start(harness.blocking().clone()).await;
    let (regions, lease, accesses, drops) = counting_region(b"queued-drop-body");
    let (receiver, duplicate) = handoff(
        ResponsePlan::file_regions(response_head(105, 33), regions).expect("file plan"),
        control,
    )
    .await;
    drop(lease);
    let blocking = harness.blocking().clone();
    let observer = blocking.clone();
    let materialization = tokio::spawn(async move { receiver.receive_command(limits(16, 1), &blocking).await });
    wait_for_queued(&observer, 1).await;

    materialization.abort();
    match materialization.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("caller future should have been aborted"),
    }
    assert_eq!(accesses.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_duplicate_completed(duplicate, 106).await;

    occupant.release().await;
    harness.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn queued_request_deadline_drops_the_unstarted_file_operation_without_access() {
    let request_deadline = RequestDeadline::after(Duration::from_secs(1));
    let policy = blocking_policy(Duration::from_secs(30), Duration::from_secs(30), 4);
    let (harness, control) = ControlHarness::with_policy(
        "legacy-materializer-queued-request-deadline",
        Some(request_deadline),
        policy,
    );
    let occupant = Occupant::start(harness.blocking().clone()).await;
    let body = b"queued-deadline";
    let (regions, lease, accesses, drops) = counting_region(body);
    let (receiver, duplicate) = handoff(
        ResponsePlan::file_regions(response_head(115, 40), regions).expect("file plan"),
        control,
    )
    .await;
    drop(lease);
    let blocking = harness.blocking().clone();
    let observer = blocking.clone();
    let materialization = tokio::spawn(async move { receiver.receive_command(limits(body.len(), 1), &blocking).await });
    wait_for_queued(&observer, 1).await;

    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(request_deadline.is_expired());
    assert!(matches!(
        materialization.await.expect("materialization task"),
        Err(LegacyLocalMaterializationError::DeadlineExceeded)
    ));
    wait_for_queued(&observer, 0).await;
    assert_eq!(accesses.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_duplicate_completed(duplicate, 116).await;

    occupant.release().await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn executor_queue_full_and_queue_timeout_preserve_runtime_sources_without_file_reads() {
    let full_policy = blocking_policy(Duration::from_secs(5), Duration::from_secs(5), 1);
    let (full_harness, full_control) = ControlHarness::with_policy("legacy-materializer-queue-full", None, full_policy);
    let full_occupant = Occupant::start(full_harness.blocking().clone()).await;
    let queued_executor = full_harness.blocking().clone();
    let queued = tokio::spawn(async move {
        queued_executor
            .spawn_io("legacy-materializer-queue-filler", || ())
            .await
    });
    wait_for_queued(full_harness.blocking(), 1).await;
    let (regions, lease, accesses, drops) = counting_region(b"queue-full-body");
    let (receiver, _) = handoff(
        ResponsePlan::file_regions(response_head(107, 34), regions).expect("file plan"),
        full_control,
    )
    .await;
    drop(lease);
    let error = expect_materialization_error(
        receiver.receive_command(limits(15, 1), full_harness.blocking()).await,
        "full executor queue should reject",
    );
    assert!(matches!(
        error,
        LegacyLocalMaterializationError::Runtime {
            source: RuntimeError::BlockingQueueFull { .. }
        }
    ));
    assert_eq!(accesses.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    full_occupant.release().await;
    queued
        .await
        .expect("queued filler task")
        .expect("queued filler should complete");
    full_harness.shutdown().await;

    let timeout_policy = blocking_policy(Duration::from_millis(30), Duration::from_secs(5), 2);
    let (timeout_harness, timeout_control) =
        ControlHarness::with_policy("legacy-materializer-queue-timeout", None, timeout_policy);
    let timeout_occupant = Occupant::start(timeout_harness.blocking().clone()).await;
    let (regions, lease, accesses, drops) = counting_region(b"queue-timeout-body");
    let (receiver, _) = handoff(
        ResponsePlan::file_regions(response_head(108, 35), regions).expect("file plan"),
        timeout_control,
    )
    .await;
    drop(lease);
    let error = expect_materialization_error(
        receiver
            .receive_command(limits(18, 1), timeout_harness.blocking())
            .await,
        "executor queue timeout should fail",
    );
    assert!(matches!(
        error,
        LegacyLocalMaterializationError::Runtime {
            source: RuntimeError::BlockingQueueTimeout { .. }
        }
    ));
    assert_eq!(accesses.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    timeout_occupant.release().await;
    timeout_harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn executor_running_timeout_and_join_panic_remain_typed_and_release_leases_once() {
    let timeout_policy = blocking_policy(Duration::from_secs(1), Duration::from_millis(30), 2);
    let (timeout_harness, timeout_control) =
        ControlHarness::with_policy("legacy-materializer-running-timeout", None, timeout_policy);
    let file = BlockingRegionHarness::new();
    let entered = file.entered.notified();
    let dropped = file.dropped.notified();
    let drops = Arc::clone(&file.drops);
    let release = Arc::clone(&file.release);
    let (receiver, _) = handoff(
        ResponsePlan::file_regions(response_head(109, 36), file.regions).expect("file plan"),
        timeout_control,
    )
    .await;
    let blocking = timeout_harness.blocking().clone();
    let materialization = tokio::spawn(async move { receiver.receive_command(limits(18, 1), &blocking).await });
    entered.await;
    let error = expect_materialization_error(
        materialization.await.expect("materialization task"),
        "running task timeout should fail",
    );
    assert!(matches!(
        error,
        LegacyLocalMaterializationError::Runtime {
            source: RuntimeError::BlockingTaskTimeoutStillRunning { .. }
        }
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(timeout_harness.blocking().blocking_still_running(), 1);
    let (released, condition) = &*release;
    *released.lock().expect("release lock") = true;
    condition.notify_all();
    dropped.await;
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    wait_for_running(timeout_harness.blocking(), 0).await;
    timeout_harness.shutdown().await;

    let (join_harness, join_control) = ControlHarness::new("legacy-materializer-join-panic", None);
    let mut file = tempfile::tempfile().expect("temporary file");
    file.write_all(b"panic-file-body").expect("write file body");
    let drops = Arc::new(AtomicUsize::new(0));
    let lease = Arc::new(PanicAfterValidationLease {
        file,
        accesses: AtomicUsize::new(0),
        drops: Arc::clone(&drops),
    });
    let region = FileRegion::try_new(lease, 0, 15).expect("panic file region");
    let (receiver, _) = handoff(
        ResponsePlan::file_regions(response_head(110, 37), FileRegionSequence::single(region)).expect("file plan"),
        join_control,
    )
    .await;
    let error = expect_materialization_error(
        receiver.receive_command(limits(15, 1), join_harness.blocking()).await,
        "blocking join panic should fail",
    );
    assert!(matches!(
        error,
        LegacyLocalMaterializationError::Runtime {
            source: RuntimeError::BlockingJoin { .. }
        }
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    join_harness.shutdown().await;
}
