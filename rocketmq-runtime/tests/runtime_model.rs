use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::BlockingPoolPolicy;
use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::TaskKind;
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
        rocketmq_runtime::TaskGroupLifecycleState::Closed
    );
    assert!(group.spawn_service("late-task", async {}).is_err());

    let second_report = group.shutdown(Duration::from_secs(1)).await;
    assert_eq!(second_report.aborted, 1, "{}", second_report.to_json());
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

    let report = service.task_group().shutdown(Duration::from_secs(1)).await;
    assert!(report.leaked == 0, "{}", report.to_json());
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
