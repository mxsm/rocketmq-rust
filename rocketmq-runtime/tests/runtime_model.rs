use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::BlockingKind;
use rocketmq_runtime::BlockingPoolPolicy;
use rocketmq_runtime::DetachedTaskPolicy;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::RuntimeError;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroupLifecycleState;
use rocketmq_runtime::TaskKind;
use tokio::sync::oneshot;
use tokio::sync::Barrier;
use tokio::sync::Notify;

#[tokio::test]
async fn task_group_shutdown_waits_for_completed_tasks() {
    let context = RuntimeContext::from_current("task-group-complete-test");
    let group = context.root_group().child("service");
    let notify = Arc::new(Notify::new());
    let notify_task = notify.clone();

    group
        .spawn_service("complete-task", async move {
            notify_task.notify_one();
        })
        .unwrap();

    notify.notified().await;
    tokio::time::sleep(Duration::from_millis(10)).await;

    let report = group.shutdown(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
    assert_eq!(report.completed, 1);
}

#[tokio::test]
async fn task_group_shutdown_reports_panics() {
    let context = RuntimeContext::from_current("task-group-panic-test");
    let group = context.root_group().child("service");

    group
        .spawn("panic-task", TaskKind::Worker, async move {
            panic!("expected panic for runtime task accounting");
        })
        .unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;

    let report = group.shutdown(Duration::from_secs(1)).await;
    assert_eq!(report.panicked, 1);
    assert!(!report.is_healthy());
}

#[tokio::test]
async fn task_group_enters_poisoned_state_after_task_panic_and_rejects_new_work() {
    let context = RuntimeContext::from_current("task-group-poisoned-test");
    let group = context.root_group().child("service");

    group
        .spawn("panic-task", TaskKind::Worker, async move {
            panic!("expected panic for poisoned task group accounting");
        })
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if group.lifecycle_state() == TaskGroupLifecycleState::Poisoned {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("task group should enter poisoned state after task panic");

    assert!(matches!(
        group
            .spawn_service("late-task-after-panic", async {})
            .expect_err("poisoned task group should reject new tasks"),
        RuntimeError::TaskGroupClosing { .. }
    ));
    assert!(matches!(
        group
            .try_child("late-child-after-panic")
            .expect_err("poisoned task group should reject new children"),
        RuntimeError::TaskGroupClosing { .. }
    ));

    let report = group.shutdown(Duration::from_secs(1)).await;
    assert_eq!(report.panicked, 1, "{}", report.to_json());
    assert_eq!(group.lifecycle_state(), TaskGroupLifecycleState::ShutdownCompleted);
}

#[tokio::test]
async fn task_group_spawn_service_with_handle_remains_tracked() {
    let context = RuntimeContext::from_current("task-group-handle-test");
    let group = context.root_group().child("service");

    let (_task_id, handle) = group.spawn_service_with_handle("handled-task", async move {}).unwrap();

    handle.await.expect("tracked task should join");

    let report = group.shutdown(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
    assert_eq!(report.completed, 1);
}

#[tokio::test]
async fn task_group_shutdown_aborts_after_timeout_without_leak() {
    let context = RuntimeContext::from_current("task-group-abort-test");
    let group = context.root_group().child("service");

    group
        .spawn_service("pending-task", async move {
            std::future::pending::<()>().await;
        })
        .unwrap();

    let report = group.shutdown(Duration::from_millis(20)).await;
    assert_eq!(report.aborted, 1, "{}", report.to_json());
    assert_eq!(report.leaked, 0, "{}", report.to_json());
    assert_eq!(report.timed_out, 1, "{}", report.to_json());
}

#[tokio::test]
async fn task_group_detached_track_only_reports_policy() {
    let context = RuntimeContext::from_current("task-group-detached-track-test");
    let group = context.root_group().child("service");

    group
        .spawn_detached("detached-task", TaskKind::Worker, async move {
            std::future::pending::<()>().await;
        })
        .unwrap();

    let report = group.shutdown(Duration::from_millis(20)).await;

    assert_eq!(report.aborted, 0, "{}", report.to_json());
    assert_eq!(report.detached_still_running, 1, "{}", report.to_json());
    assert!(!report.is_healthy(), "{}", report.to_json());
    assert_eq!(report.remaining_tasks.len(), 1, "{}", report.to_json());
    assert_eq!(
        report.remaining_tasks[0].detached_policy,
        Some(DetachedTaskPolicy::TrackOnly),
        "{}",
        report.to_json()
    );
}

#[tokio::test]
async fn task_group_detached_abort_on_shutdown_is_aborted() {
    let context = RuntimeContext::from_current("task-group-detached-abort-test");
    let group = context.root_group().child("service");

    group
        .spawn_detached_with_policy(
            "detached-abort-task",
            TaskKind::Worker,
            DetachedTaskPolicy::AbortOnShutdown,
            async move {
                std::future::pending::<()>().await;
            },
        )
        .unwrap();

    let report = group.shutdown(Duration::from_secs(1)).await;

    assert_eq!(report.aborted, 1, "{}", report.to_json());
    assert_eq!(report.detached_still_running, 0, "{}", report.to_json());
    assert!(report.remaining_tasks.is_empty(), "{}", report.to_json());
}

#[tokio::test]
async fn task_group_shutdown_now_aborts_without_async_wait() {
    let context = RuntimeContext::from_current("task-group-shutdown-now-test");
    let group = context.root_group().child("service");

    group
        .spawn_service("pending-task", async move {
            std::future::pending::<()>().await;
        })
        .unwrap();

    let report = group.shutdown_now();
    assert_eq!(report.aborted, 1, "{}", report.to_json());
    assert_eq!(report.leaked, 0, "{}", report.to_json());
    assert_eq!(
        group.lifecycle_state(),
        rocketmq_runtime::TaskGroupLifecycleState::ShutdownCompleted
    );
    assert!(group.spawn_service("late-task", async {}).is_err());

    let second_report = group.shutdown(Duration::from_secs(1)).await;
    assert_eq!(second_report.aborted, 1, "{}", second_report.to_json());
}

#[tokio::test]
async fn task_group_shutdown_starts_children_concurrently() {
    let context = RuntimeContext::from_current("task-group-child-shutdown-test");
    let group = context.root_group().child("service");
    let first_child = group.child("first-child");
    let second_child = group.child("second-child");
    let second_child_observer = second_child.clone();

    first_child
        .spawn_service("wait-for-sibling-shutdown", async move {
            loop {
                match second_child_observer.lifecycle_state() {
                    rocketmq_runtime::TaskGroupLifecycleState::Closing
                    | rocketmq_runtime::TaskGroupLifecycleState::Closed
                    | rocketmq_runtime::TaskGroupLifecycleState::ShutdownCompleted
                    | rocketmq_runtime::TaskGroupLifecycleState::Poisoned => break,
                    rocketmq_runtime::TaskGroupLifecycleState::Open => tokio::task::yield_now().await,
                }
            }
        })
        .unwrap();

    let report = group.shutdown(Duration::from_millis(500)).await;

    assert!(report.is_healthy(), "{}", report.to_json());
    assert_eq!(report.children.len(), 2, "{}", report.to_json());
    assert_eq!(report.timed_out, 0, "{}", report.to_json());
    assert!(
        report.children.iter().all(|child| child.timed_out == 0),
        "{}",
        report.to_json()
    );
}

#[tokio::test]
async fn task_group_rejects_child_creation_after_shutdown() {
    let context = RuntimeContext::from_current("task-group-late-child-test");
    let group = context.root_group().child("service");

    let report = group.shutdown(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
    assert_eq!(
        group.lifecycle_state(),
        rocketmq_runtime::TaskGroupLifecycleState::ShutdownCompleted
    );

    let error = group
        .try_child("late-child")
        .expect_err("try_child should reject child creation after shutdown");
    assert!(matches!(error, RuntimeError::TaskGroupClosing { .. }));

    let late_child = group.child("late-child-compat");
    assert_eq!(
        late_child.lifecycle_state(),
        rocketmq_runtime::TaskGroupLifecycleState::ShutdownCompleted
    );
    assert!(late_child.spawn_service("late-task", async {}).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn task_group_concurrent_spawn_shutdown_leaves_no_metadata() {
    const PRODUCERS: usize = 8;
    const TASKS: usize = 10_000;

    let context = RuntimeContext::from_current("task-group-concurrent-spawn-shutdown-test");
    let group = context.root_group().child("service");
    let start = Arc::new(Barrier::new(PRODUCERS + 1));
    let first_spawned = Arc::new(Notify::new());
    let accepted = Arc::new(AtomicUsize::new(0));
    let rejected = Arc::new(AtomicUsize::new(0));
    let mut producers = Vec::with_capacity(PRODUCERS);

    for producer_id in 0..PRODUCERS {
        let group = group.clone();
        let start = start.clone();
        let first_spawned = first_spawned.clone();
        let accepted = accepted.clone();
        let rejected = rejected.clone();
        producers.push(tokio::spawn(async move {
            start.wait().await;
            let start_index = producer_id * (TASKS / PRODUCERS);
            let end_index = if producer_id + 1 == PRODUCERS {
                TASKS
            } else {
                start_index + (TASKS / PRODUCERS)
            };

            for task_index in start_index..end_index {
                match group.spawn_service(format!("short-task-{task_index}"), async {}) {
                    Ok(_task_id) => {
                        if accepted.fetch_add(1, Ordering::SeqCst) == 0 {
                            first_spawned.notify_one();
                        }
                    }
                    Err(RuntimeError::TaskGroupClosing { .. }) => {
                        rejected.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(error) => panic!("unexpected task group spawn error: {error}"),
                }

                if task_index % 64 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        }));
    }

    let shutdown_group = group.clone();
    let shutdown_start = start.clone();
    let shutdown_first_spawned = first_spawned.clone();
    let shutdown = tokio::spawn(async move {
        shutdown_start.wait().await;
        shutdown_first_spawned.notified().await;
        shutdown_group.shutdown(Duration::from_secs(5)).await
    });

    for producer in producers {
        producer.await.expect("producer task should not panic");
    }

    let report = shutdown.await.expect("shutdown task should not panic");
    let accepted = accepted.load(Ordering::SeqCst);
    let rejected = rejected.load(Ordering::SeqCst);

    assert!(accepted > 0, "at least one task should be registered before shutdown");
    assert_eq!(accepted + rejected, TASKS);
    assert_eq!(group.lifecycle_state(), TaskGroupLifecycleState::ShutdownCompleted);
    assert_eq!(group.task_count(), 0, "{}", report.to_json());
    assert_eq!(report.leaked, 0, "{}", report.to_json());
    assert!(report.remaining_tasks.is_empty(), "{}", report.to_json());

    let second_report = group.shutdown(Duration::from_secs(1)).await;
    assert!(second_report.remaining_tasks.is_empty(), "{}", second_report.to_json());
    assert_eq!(second_report.leaked, 0, "{}", second_report.to_json());
}

#[tokio::test]
async fn scheduled_no_overlap_skips_while_previous_run_is_active() {
    let context = RuntimeContext::from_current("scheduled-test");
    let service = context.service_context("service");
    let scheduled = ScheduledTaskGroup::new(service.task_group().child("scheduled"));
    let config = ScheduledTaskConfig::fixed_rate_no_overlap("slow-task", Duration::from_millis(10));

    scheduled
        .schedule_fixed_rate_no_overlap(config, || async {
            tokio::time::sleep(Duration::from_millis(50)).await;
        })
        .unwrap();

    tokio::time::sleep(Duration::from_millis(90)).await;
    let snapshot = scheduled
        .snapshot()
        .into_iter()
        .find(|task| task.name == "slow-task")
        .expect("scheduled task snapshot should exist");

    assert!(snapshot.runs >= 1, "{snapshot:?}");
    assert!(snapshot.skips >= 1, "{snapshot:?}");
    assert_eq!(snapshot.overlaps, 0, "{snapshot:?}");

    let report = scheduled.shutdown(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
    assert!(report.leaked == 0, "{}", report.to_json());
    assert!(report.completed + report.cancelled > 0, "{}", report.to_json());
}

#[tokio::test]
async fn scheduled_fixed_rate_allows_overlap_and_reports_overlap_metrics() {
    let context = RuntimeContext::from_current("scheduled-overlap-test");
    let service = context.service_context("service");
    let scheduled = ScheduledTaskGroup::new(service.task_group().child("scheduled"));
    let config = ScheduledTaskConfig::fixed_rate("overlap-task", Duration::from_millis(10));

    scheduled
        .schedule_fixed_rate(config, || async {
            tokio::time::sleep(Duration::from_millis(50)).await;
        })
        .unwrap();

    tokio::time::sleep(Duration::from_millis(90)).await;
    let snapshot = scheduled
        .snapshot()
        .into_iter()
        .find(|task| task.name == "overlap-task")
        .expect("scheduled task snapshot should exist");

    assert_eq!(snapshot.mode, rocketmq_runtime::ScheduleMode::FixedRateAllowOverlap);
    assert_eq!(snapshot.skips, 0, "{snapshot:?}");
    assert!(snapshot.overlaps >= 1, "{snapshot:?}");
    assert!(snapshot.active_runs >= 1, "{snapshot:?}");
    assert!(snapshot.max_elapsed_ms >= 40, "{snapshot:?}");

    let report = scheduled.shutdown(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
}

#[tokio::test]
async fn scheduled_spawn_failure_rolls_back_registered_metrics() {
    let context = RuntimeContext::from_current("scheduled-rollback-test");
    let scheduled = ScheduledTaskGroup::new(context.root_group().child("scheduled"));
    let report = scheduled.shutdown(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());

    let result = scheduled.schedule_fixed_delay(
        ScheduledTaskConfig::fixed_delay("late-task", Duration::from_secs(1)),
        || async {},
    );

    assert!(result.is_err());
    assert!(
        scheduled.snapshot().into_iter().all(|task| task.name != "late-task"),
        "failed schedule should not leave a metrics snapshot"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocking_executor_limits_concurrency() {
    let context = RuntimeContext::from_current("blocking-test");
    let policy = BlockingPoolPolicy {
        max_concurrency: 1,
        queue_timeout: Duration::from_secs(1),
        task_timeout: Duration::from_secs(1),
        ..BlockingPoolPolicy::default()
    };
    let executor = BlockingExecutor::new(policy, context.root_group().child("blocking")).unwrap();
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));

    let first = run_limited_blocking_task(executor.clone(), active.clone(), max_active.clone());
    let second = run_limited_blocking_task(executor, active, max_active.clone());
    let (first, second) = tokio::join!(first, second);

    first.unwrap();
    second.unwrap();
    assert_eq!(max_active.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn blocking_executor_rejects_long_running_spawn_blocking() {
    let context = RuntimeContext::from_current("blocking-long-running-test");

    let error = context
        .blocking()
        .spawn("legacy-loop", BlockingKind::LongRunning, || ())
        .await
        .expect_err("long-running blocking work must use a dedicated thread");

    assert!(matches!(
        error,
        RuntimeError::UnsupportedBlockingKind {
            kind: BlockingKind::LongRunning,
            ..
        }
    ));
    let snapshot = context.blocking().snapshot();
    assert_eq!(snapshot.blocking_still_running, 0);
    assert!(snapshot.tasks.is_empty(), "{snapshot:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocking_executor_reports_timeout_until_reaper_cleans_late_exit() {
    let context = RuntimeContext::from_current("blocking-reaper-test");
    let policy = BlockingPoolPolicy {
        name: "blocking-reaper-test".to_string(),
        max_concurrency: 1,
        queue_timeout: Duration::from_secs(1),
        task_timeout: Duration::from_millis(20),
        ..BlockingPoolPolicy::default()
    };
    let executor = BlockingExecutor::new(policy, context.root_group().child("blocking")).unwrap();
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();

    let run = {
        let executor = executor.clone();
        tokio::spawn(async move {
            executor
                .spawn_io("slow-io", move || {
                    let _ = started_tx.send(());
                    let _ = release_rx.recv();
                })
                .await
        })
    };

    tokio::time::timeout(Duration::from_secs(1), started_rx)
        .await
        .expect("blocking task should start before caller timeout")
        .expect("blocking task should signal start");

    let error = tokio::time::timeout(Duration::from_secs(1), run)
        .await
        .expect("caller should receive task timeout")
        .expect("spawn task should join")
        .expect_err("blocking timeout should be reported while closure is still running");
    assert!(matches!(error, RuntimeError::BlockingTaskTimeoutStillRunning { .. }));

    let snapshot = executor.snapshot();
    assert_eq!(snapshot.timed_out_still_running, 1, "{snapshot:?}");
    assert_eq!(executor.blocking_still_running(), 1);

    let mut report = ShutdownReport::new("blocking-reaper-test", Duration::ZERO);
    report.merge_blocking(snapshot);
    assert_eq!(report.blocking_still_running, 1, "{}", report.to_json());
    assert!(!report.is_healthy(), "{}", report.to_json());

    release_tx
        .send(())
        .expect("blocking task release signal should be accepted");
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if executor.blocking_still_running() == 0 && executor.snapshot().tasks.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("blocking reaper should remove the late-exiting task");
}

#[test]
fn runtime_owner_uses_configured_blocking_policy_in_shutdown_report() {
    let config = RuntimeConfig {
        worker_threads: 2,
        max_blocking_threads: 2,
        shutdown_timeout: Duration::from_millis(50),
        blocking_pool_policy: BlockingPoolPolicy {
            name: "configured-blocking-test".to_string(),
            max_concurrency: 1,
            queue_timeout: Duration::from_secs(1),
            task_timeout: Duration::from_millis(20),
            ..BlockingPoolPolicy::default()
        },
        ..RuntimeConfig::default()
    };
    let owner = RuntimeOwner::new(config).expect("runtime owner should start");
    let context = owner.context().clone();
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();

    let report = owner.block_on(async move {
        let error = context
            .blocking()
            .spawn_io("context-slow-io", move || {
                let _ = started_tx.send(());
                let _ = release_rx.recv();
            })
            .await
            .expect_err("configured task_timeout should return while blocking closure is still running");
        assert!(matches!(error, RuntimeError::BlockingTaskTimeoutStillRunning { .. }));

        started_rx.await.expect("blocking task should signal that it started");

        let report = context.shutdown_tasks(Duration::from_millis(50)).await;
        assert_eq!(report.blocking_still_running, 1, "{}", report.to_json());
        assert!(!report.blocking_tasks.is_empty(), "{}", report.to_json());
        assert!(!report.is_healthy(), "{}", report.to_json());

        release_tx
            .send(())
            .expect("blocking task release signal should be accepted");
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if context.blocking().blocking_still_running() == 0 && context.blocking().snapshot().tasks.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("blocking reaper should clean the configured runtime context task");

        report
    });

    assert_eq!(report.blocking_tasks[0].name, "context-slow-io");
}

#[test]
fn runtime_owner_drop_shutdowns_root_group_when_not_explicitly_closed() {
    let config = RuntimeConfig {
        worker_threads: 2,
        max_blocking_threads: 2,
        shutdown_timeout: Duration::from_millis(50),
        ..RuntimeConfig::default()
    };
    let owner = RuntimeOwner::new(config).expect("runtime owner should start");
    let context = owner.context().clone();

    owner.block_on(async {
        context
            .root_group()
            .spawn_service("owner-drop-pending-task", async {
                std::future::pending::<()>().await;
            })
            .expect("pending task should spawn");
        tokio::task::yield_now().await;
    });

    drop(owner);

    assert_eq!(
        context.root_group().lifecycle_state(),
        TaskGroupLifecycleState::ShutdownCompleted
    );
    assert!(context.root_group().spawn_service("late-task", async {}).is_err());
}

#[test]
fn runtime_owner_shutdown_background_closes_root_group_without_drop_fallback() {
    let config = RuntimeConfig {
        worker_threads: 2,
        max_blocking_threads: 2,
        shutdown_timeout: Duration::from_millis(50),
        ..RuntimeConfig::default()
    };
    let owner = RuntimeOwner::new(config).expect("runtime owner should start");
    let context = owner.context().clone();

    owner.block_on(async {
        context
            .root_group()
            .spawn_service("owner-background-pending-task", async {
                std::future::pending::<()>().await;
            })
            .expect("pending task should spawn");
        tokio::task::yield_now().await;
    });

    let report = owner.shutdown_background();

    assert_eq!(report.aborted, 1, "{}", report.to_json());
    assert_eq!(
        context.root_group().lifecycle_state(),
        TaskGroupLifecycleState::ShutdownCompleted
    );
    assert!(context.root_group().spawn_service("late-task", async {}).is_err());
}

async fn run_limited_blocking_task(
    executor: BlockingExecutor,
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
) -> rocketmq_runtime::RuntimeResult<()> {
    executor
        .spawn_io("limited-io", move || {
            let current = active.fetch_add(1, Ordering::SeqCst) + 1;
            max_active.fetch_max(current, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(50));
            active.fetch_sub(1, Ordering::SeqCst);
        })
        .await
}
