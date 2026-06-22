use std::collections::HashSet;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::RuntimeError;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskGroupLifecycleState;
use tokio::sync::Barrier;
use tokio::sync::Notify;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn task_group_concurrency_model_covers_spawn_shutdown_races() {
    let context = RuntimeContext::from_current("task-group-concurrency-model");
    let group = context.root_group().child("service");

    run_task_group_concurrency_model(group).await;
}

async fn run_task_group_concurrency_model(group: TaskGroup) {
    deep_child_creation_does_not_overflow(group.child("deep-child-id"));
    child_creation_racing_with_shutdown_rejects_late_children(group.child("child-race")).await;
    spawn_metadata_is_registered_before_task_can_finish(group.child("metadata-before-finish")).await;
    shutdown_after_spawn_leaves_no_running_metadata_for_completed_tasks(group.child("completed-cleanup")).await;
    duplicate_child_names_keep_distinct_identity(group.child("duplicate-children")).await;

    let report = group.shutdown(Duration::from_secs(5)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
}

fn deep_child_creation_does_not_overflow(group: TaskGroup) {
    let mut current = group;
    for depth in 0..16 {
        let parent = current.clone();
        let child = parent.child(format!("deep-child-{depth}"));
        assert_eq!(child.parent_id(), Some(parent.id()));
        assert_ne!(child.id(), parent.id());
        current = child;
    }
}

async fn child_creation_racing_with_shutdown_rejects_late_children(group: TaskGroup) {
    const PRODUCERS: usize = 8;
    const LATE_CHILDREN_PER_PRODUCER: usize = 64;

    let ready = Arc::new(AtomicUsize::new(0));
    let ready_notify = Arc::new(Notify::new());
    let late_start = Arc::new(Notify::new());
    let rejected = Arc::new(AtomicUsize::new(0));
    let accepted_late = Arc::new(AtomicUsize::new(0));
    let mut producers = Vec::with_capacity(PRODUCERS);

    for producer_id in 0..PRODUCERS {
        let group = group.clone();
        let ready = ready.clone();
        let ready_notify = ready_notify.clone();
        let late_start = late_start.clone();
        let rejected = rejected.clone();
        let accepted_late = accepted_late.clone();

        producers.push(tokio::spawn(async move {
            let warmup_child = group
                .try_child(format!("warmup-child-{producer_id}"))
                .expect("warmup child should be created before shutdown starts");
            assert_eq!(warmup_child.parent_id(), Some(group.id()));

            if ready.fetch_add(1, Ordering::AcqRel) + 1 == PRODUCERS {
                ready_notify.notify_one();
            }

            late_start.notified().await;

            for child_index in 0..LATE_CHILDREN_PER_PRODUCER {
                match group.try_child(format!("late-child-{producer_id}-{child_index}")) {
                    Ok(_child) => {
                        accepted_late.fetch_add(1, Ordering::AcqRel);
                    }
                    Err(RuntimeError::TaskGroupClosing { .. }) => {
                        rejected.fetch_add(1, Ordering::AcqRel);
                    }
                    Err(error) => panic!("unexpected child creation error: {error}"),
                }
                tokio::task::yield_now().await;
            }
        }));
    }

    wait_until(Duration::from_secs(1), || ready.load(Ordering::Acquire) == PRODUCERS)
        .await
        .expect("all child producers should reach the late-child gate");

    let shutdown_group = group.clone();
    let shutdown = tokio::spawn(async move { shutdown_group.shutdown(Duration::from_secs(5)).await });

    wait_until(Duration::from_secs(1), || {
        group.lifecycle_state() != TaskGroupLifecycleState::Open
    })
    .await
    .expect("shutdown should close child creation before late attempts");
    late_start.notify_waiters();

    for producer in producers {
        producer.await.expect("child producer should not panic");
    }

    let report = shutdown.await.expect("shutdown task should not panic");
    assert!(report.is_healthy(), "{}", report.to_json());
    assert_eq!(rejected.load(Ordering::Acquire), PRODUCERS * LATE_CHILDREN_PER_PRODUCER);
    assert_eq!(accepted_late.load(Ordering::Acquire), 0);
    assert!(matches!(
        group
            .try_child("post-shutdown-child")
            .expect_err("late child creation should be rejected after shutdown"),
        RuntimeError::TaskGroupClosing { .. }
    ));
}

async fn spawn_metadata_is_registered_before_task_can_finish(group: TaskGroup) {
    let release = Arc::new(Notify::new());
    let release_task = release.clone();

    let (task_id, handle) = group
        .spawn_service_with_handle("metadata-visible-task", async move {
            release_task.notified().await;
        })
        .expect("tracked task should spawn");

    assert!(group.contains_task(task_id));
    assert_eq!(group.task_count(), 1);

    release.notify_one();
    handle.await.expect("tracked task should join");
    assert!(group.wait_task(task_id, Duration::from_secs(1)).await);

    let report = group.shutdown(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
    assert_eq!(report.completed, 1, "{}", report.to_json());
    assert_eq!(group.task_count(), 0, "{}", report.to_json());
}

async fn shutdown_after_spawn_leaves_no_running_metadata_for_completed_tasks(group: TaskGroup) {
    const TASKS: usize = 512;

    let mut handles = Vec::with_capacity(TASKS);
    for task_index in 0..TASKS {
        let (_task_id, handle) = group
            .spawn_service_with_handle(format!("completed-task-{task_index}"), async move {})
            .expect("short tracked task should spawn");
        handles.push(handle);
    }

    for handle in handles {
        handle.await.expect("short tracked task should join");
    }

    wait_until(Duration::from_secs(1), || group.task_count() == 0)
        .await
        .expect("completed tasks should remove task metadata before shutdown");

    let report = group.shutdown(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
    assert_eq!(report.completed, TASKS, "{}", report.to_json());
    assert_eq!(report.leaked, 0, "{}", report.to_json());
    assert!(report.remaining_tasks.is_empty(), "{}", report.to_json());
}

async fn duplicate_child_names_keep_distinct_identity(group: TaskGroup) {
    const CHILDREN: usize = 64;

    let start = Arc::new(Barrier::new(CHILDREN));
    let mut creators = Vec::with_capacity(CHILDREN);
    for _ in 0..CHILDREN {
        let group = group.clone();
        let start = start.clone();
        creators.push(tokio::spawn(async move {
            start.wait().await;
            let child = group
                .try_child("duplicate-model-child")
                .expect("duplicate child name should still create a distinct child");
            assert_eq!(child.parent_id(), Some(group.id()));
            child.id()
        }));
    }

    let mut child_ids = HashSet::with_capacity(CHILDREN);
    for creator in creators {
        let child_id = creator.await.expect("child creator should not panic");
        assert!(child_ids.insert(child_id), "duplicate TaskGroupId: {child_id:?}");
    }
    assert_eq!(child_ids.len(), CHILDREN);

    let report = group.shutdown(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
    assert_eq!(report.children.len(), CHILDREN, "{}", report.to_json());
    assert!(report
        .children
        .iter()
        .all(|child| child.name == "duplicate-model-child"));
}

async fn wait_until(timeout: Duration, condition: impl Fn() -> bool) -> Result<(), tokio::time::error::Elapsed> {
    tokio::time::timeout(timeout, async {
        loop {
            if condition() {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
}
