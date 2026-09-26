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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::TaskGroupLifecycleState;
use tokio::sync::oneshot;

#[tokio::test]
async fn sibling_report_wait_and_overlapping_subtree_shutdown_complete() {
    let runtime = RuntimeContext::from_current("flat-shutdown");
    let root = runtime.service_context("root");
    let first = root.component("first");
    let second = root.component("second");
    let leaf = second.component("leaf");
    let token = leaf.task_group().cancellation_token();
    leaf.spawn_service("cooperative", async move { token.cancelled().await })
        .unwrap();

    let token = first.task_group().cancellation_token();
    let sibling = second.task_group().clone();
    first
        .spawn_service("wait-for-sibling-report", async move {
            token.cancelled().await;
            assert!(sibling.shutdown(Duration::from_secs(1)).await.is_healthy());
        })
        .unwrap();

    let (root_report, child_report) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(
            root.task_group().shutdown(Duration::from_secs(1)),
            second.task_group().shutdown(Duration::from_secs(1))
        )
    })
    .await
    .unwrap();
    assert!(root_report.is_healthy(), "{}", root_report.to_json());
    assert!(child_report.is_healthy());
    assert_eq!(root_report.children.len(), 2);
    assert_eq!(child_report.children[0].cancelled, 1);
}

#[tokio::test]
async fn timed_out_counts_only_tasks_the_shutdown_deadline_aborted() {
    let runtime = RuntimeContext::from_current("timed-out-accounting");
    let group = runtime.service_context("long-lived").task_group().clone();

    // An abort requested before shutdown stays in the group's counters but is
    // not a shutdown timeout.
    let aborted_early = group
        .spawn_service("aborted-early", std::future::pending::<()>())
        .unwrap();
    assert!(group.abort_task(aborted_early));
    assert!(group.wait_task(aborted_early, Duration::from_secs(1)).await);

    // These tasks ignore cancellation, so only the deadline stops them.
    for index in 0..2 {
        group
            .spawn_service(format!("stubborn-{index}"), std::future::pending::<()>())
            .unwrap();
    }

    let report = group.shutdown(Duration::from_millis(50)).await;
    assert_eq!(report.timed_out, 2, "{}", report.to_json());
    assert_eq!(report.aborted + report.leaked, 3, "{}", report.to_json());
}

#[tokio::test]
async fn a_task_woken_by_shutdown_sees_its_group_closed() {
    let runtime = RuntimeContext::from_current("closed-after-cancellation");
    let group = runtime.service_context("closing-order").task_group().clone();
    let token = group.cancellation_token();
    let observed = group.clone();
    let (state_tx, state_rx) = oneshot::channel();
    group
        .spawn_service("observe-state-on-cancel", async move {
            token.cancelled().await;
            let _ = state_tx.send(observed.lifecycle_state());
        })
        .unwrap();
    assert_eq!(group.lifecycle_state(), TaskGroupLifecycleState::Open);

    let report = group.shutdown(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
    assert_eq!(state_rx.await.unwrap(), TaskGroupLifecycleState::Closed);
    assert_eq!(group.lifecycle_state(), TaskGroupLifecycleState::ShutdownCompleted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_states_are_observed_in_shutdown_order() {
    let runtime = RuntimeContext::from_current("lifecycle-order");
    let group = runtime.service_context("watched").task_group().clone();
    let token = group.cancellation_token();
    group
        .spawn_service("cooperative", async move { token.cancelled().await })
        .unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let watcher = {
        let group = group.clone();
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut observed = vec![group.lifecycle_state()];
            while !stop.load(Ordering::Acquire) {
                let state = group.lifecycle_state();
                if matches!(
                    state,
                    TaskGroupLifecycleState::Closed | TaskGroupLifecycleState::ShutdownCompleted
                ) {
                    assert!(
                        group.cancellation_token().is_cancelled(),
                        "{state:?} before cancellation"
                    );
                }
                if observed.last() != Some(&state) {
                    observed.push(state);
                }
            }
            observed
        })
    };

    let report = group.shutdown(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
    stop.store(true, Ordering::Release);
    let observed = watcher.join().expect("state watcher");
    let order = [
        TaskGroupLifecycleState::Open,
        TaskGroupLifecycleState::Closing,
        TaskGroupLifecycleState::Closed,
        TaskGroupLifecycleState::ShutdownCompleted,
    ];
    let positions: Vec<_> = observed
        .iter()
        .map(|state| {
            order
                .iter()
                .position(|expected| expected == state)
                .expect("no other state")
        })
        .collect();
    assert!(positions.windows(2).all(|pair| pair[0] < pair[1]), "{observed:?}");
    assert_eq!(observed.last(), Some(&TaskGroupLifecycleState::ShutdownCompleted));
}

#[tokio::test]
async fn cancelled_tree_shutdown_can_resume_without_lost_report_notifications() {
    let runtime = RuntimeContext::from_current("resumed-shutdown");
    let root = runtime.service_context("root");
    let leaf = root.component("middle").component("leaf");
    let (release, released) = oneshot::channel();
    leaf.spawn_service("custom-drain", async {
        let _ = released.await;
    })
    .unwrap();

    let mut abandoned = root.task_group().shutdown(Duration::from_secs(5));
    assert!(futures::poll!(&mut abandoned).is_pending());
    drop(abandoned);
    release.send(()).unwrap();

    let report = tokio::time::timeout(
        Duration::from_secs(1),
        root.task_group().shutdown(Duration::from_secs(1)),
    )
    .await
    .unwrap();
    assert!(report.is_healthy(), "{}", report.to_json());
    assert_eq!(report.children[0].children[0].cancelled, 1);
    assert_eq!(
        report.to_json(),
        root.task_group().shutdown(Duration::ZERO).await.to_json()
    );
}

#[tokio::test]
async fn an_earlier_root_deadline_wakes_all_flat_scope_waiters() {
    let runtime = RuntimeContext::from_current("tightened-tree-deadline");
    let root = runtime.service_context("root");
    let leaf = root.component("middle").component("leaf");
    let (started, ready) = oneshot::channel();
    leaf.spawn_service("uncooperative", async {
        started.send(()).unwrap();
        std::future::pending::<()>().await;
    })
    .unwrap();
    ready.await.unwrap();

    let mut shutdown = root.task_group().shutdown(Duration::from_secs(3600));
    assert!(futures::poll!(&mut shutdown).is_pending());
    let earlier = ShutdownDeadline::after(Duration::ZERO);
    drop(root.task_group().shutdown_until(earlier));
    assert_eq!(leaf.task_group().shutdown_deadline(), Some(earlier));
    let report = tokio::time::timeout(Duration::from_secs(1), shutdown).await.unwrap();
    assert!(!report.is_healthy());
    assert!(report.children[0].children[0].timed_out > 0);
}
