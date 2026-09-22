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

use rocketmq_runtime::BudgetClass;
use rocketmq_runtime::ProcessMemoryLimit;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;

#[test]
fn child_contexts_share_the_runtime_owners_process_budget() {
    let memory_limit = ProcessMemoryLimit::configured(1024).expect("configured memory limit");
    let owner = RuntimeOwner::plan(RuntimeConfig::default())
        .expect("valid runtime configuration")
        .with_memory_limit(memory_limit)
        .build()
        .expect("runtime owner");
    let producer = owner.root_context().component("producer");
    let transport = owner.root_context().component("transport");

    assert_eq!(owner.resources().memory_limit(), memory_limit);
    let producer_budget = producer.process_budget();
    let transport_budget = transport.process_budget();
    let permit = producer_budget
        .try_acquire(768, BudgetClass::Data)
        .expect("producer reservation");

    assert_eq!(transport_budget.snapshot().current_bytes, 768);
    let exhausted = transport_budget
        .try_acquire(300, BudgetClass::Data)
        .expect_err("shared process ceiling must reject aggregate overcommit");
    assert_eq!(exhausted.exhausted_path(), "process");

    drop(permit);
    assert_eq!(owner.resources().process_budget().snapshot().current_bytes, 0);
}

#[tokio::test]
async fn active_leaf_keeps_dropped_ancestors_joinable_and_releases_them_after_shutdown() {
    use rocketmq_runtime::RuntimeContext;
    use std::time::Duration;
    let context = RuntimeContext::from_current("ancestry");
    for _ in 0..64 {
        let parent = context.service_context("parent");
        let middle = parent.component("middle");
        let leaf = middle.component("leaf");
        let cancellation = leaf.task_group().cancellation_token();
        leaf.spawn_service("pending", async move {
            cancellation.cancelled().await;
        })
        .unwrap();
        drop((parent, middle, leaf));
    }
    assert_eq!(context.root_group().component_count(), 64);
    let report = context.shutdown_tasks(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
    assert_eq!(report.children.len(), 64);
    for parent in &report.children {
        assert_eq!(parent.children[0].children[0].cancelled, 1);
    }
    assert_eq!(context.root_group().component_count(), 0);
}

#[tokio::test]
async fn dropped_intermediate_cannot_hide_an_uncooperative_or_panicking_leaf() {
    use rocketmq_runtime::RuntimeContext;
    use std::time::Duration;
    for panic in [false, true] {
        let context = RuntimeContext::from_current("ancestry-failure");
        let parent = context.service_context("parent");
        let leaf = parent.component("leaf");
        let cancellation = leaf.task_group().cancellation_token();
        let id = leaf
            .spawn_service("pending", async move {
                if panic {
                    cancellation.cancelled().await;
                    panic!("injected leaf panic");
                }
                std::future::pending::<()>().await;
            })
            .unwrap();
        drop(parent);
        assert_eq!(context.root_group().component_count(), 1);
        let report = context.shutdown_tasks(Duration::from_millis(10)).await;
        assert!(!report.is_healthy());
        let leaf_report = &report.children[0].children[0];
        if panic {
            assert_eq!(leaf_report.panicked, 1);
        } else {
            assert!(leaf_report.timed_out > 0);
        }
        assert!(leaf.task_group().wait_task(id, Duration::from_secs(1)).await);
        drop(leaf);
        assert_eq!(context.root_group().component_count(), 0);
    }
}

#[tokio::test]
async fn root_shutdown_waits_for_custom_leaf_drain_after_all_context_handles_are_dropped() {
    use rocketmq_runtime::RuntimeContext;
    use std::time::Duration;
    let context = RuntimeContext::from_current("custom-drain");
    let parent = context.service_context("parent");
    let middle = parent.component("middle");
    let leaf = middle.component("leaf");
    let cancellation = leaf.task_group().cancellation_token();
    let (draining_tx, draining_rx) = tokio::sync::oneshot::channel();
    let (released_tx, released_rx) = tokio::sync::oneshot::channel();
    leaf.spawn_service("drain", async move {
        cancellation.cancelled().await;
        let _ = draining_tx.send(());
        let _ = released_rx.await;
    })
    .unwrap();
    drop((parent, middle, leaf));
    let shutdown = context.shutdown_tasks(Duration::from_secs(1));
    tokio::pin!(shutdown);
    assert!(futures::poll!(&mut shutdown).is_pending());
    draining_rx.await.unwrap();
    assert!(futures::poll!(&mut shutdown).is_pending());
    released_tx.send(()).unwrap();
    let report = shutdown.await;
    assert!(report.is_healthy(), "{}", report.to_json());
    assert_eq!(report.children[0].children[0].children[0].cancelled, 1);
    assert_eq!(context.root_group().component_count(), 0);
}
