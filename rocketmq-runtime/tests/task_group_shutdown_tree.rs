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

use std::time::Duration;

use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::ShutdownDeadline;
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
