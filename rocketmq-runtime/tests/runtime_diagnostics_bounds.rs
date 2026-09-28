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

use rocketmq_runtime::{RuntimeContext, TaskKind};
use std::time::Duration;

#[tokio::test]
async fn task_details_remain_bounded_and_settle_after_task_table_churn() {
    let context = RuntimeContext::try_from_current("bounded-churn").unwrap();
    let service = context.service_context("service");
    let group = service.task_group();
    let release = tokio_util::sync::CancellationToken::new();
    let tasks = (0..4096)
        .map(|_| {
            let release = release.clone();
            group
                .spawn("private-churn-name", TaskKind::Worker, async move {
                    release.cancelled().await;
                })
                .unwrap()
        })
        .collect::<Vec<_>>();
    let bounded = group.diagnostics_task_details(7, 3);
    assert_eq!(bounded.tasks_scanned, 7);
    assert_eq!(bounded.details.len(), 3);
    assert!(bounded.truncated);
    release.cancel();
    for task in tasks {
        assert!(group.wait_task(task, Duration::from_secs(5)).await);
    }
    let cancellation = group.cancellation_token();
    group
        .spawn("private-survivor", TaskKind::Service, async move {
            cancellation.cancelled().await;
        })
        .unwrap();
    let sparse = group.diagnostics_task_details(7, 3);
    assert_eq!(sparse.tasks_scanned, 1);
    assert_eq!(sparse.details.len(), 1);
    assert!(!sparse.truncated);
    assert!(!serde_json::to_string(&sparse).unwrap().contains("private-"));
    assert!(context.shutdown_tasks(Duration::from_secs(5)).await.is_healthy());
    assert!(group.diagnostics_task_details(7, 3).details.is_empty());
}

#[tokio::test]
async fn empty_wide_trees_charge_enumeration_before_materializing_children() {
    let context = RuntimeContext::try_from_current("bounded-wide").unwrap();
    let root = context.service_context("service");
    let children = (0..4096)
        .map(|i| root.task_group().try_child(format!("child-{i}")).unwrap())
        .collect::<Vec<_>>();
    let sample = root.task_group().diagnostics_task_details(4, 4);
    assert_eq!(sample.group_entries_scanned, 4);
    assert_eq!(sample.tasks_scanned, 0);
    assert!(sample.truncated);
    assert!(root.task_group().diagnostics_task_details(0, 0).truncated);
    drop(children);
    let sample = root.task_group().diagnostics_task_details(4, 4);
    assert_eq!(sample.group_entries_scanned, 0);
    assert!(!sample.truncated);
    assert!(context.shutdown_tasks(Duration::from_secs(5)).await.is_healthy());
}

#[tokio::test]
async fn deep_tree_sampling_is_iterative_and_does_not_disclose_task_names() {
    let context = RuntimeContext::try_from_current("bounded-deep").unwrap();
    let root = context.service_context("service");
    let mut groups = vec![root.task_group().clone()];
    for i in 0..512 {
        groups.push(groups.last().unwrap().try_child(format!("child-{i}")).unwrap());
    }
    let cancellation = groups.last().unwrap().cancellation_token();
    groups
        .last()
        .unwrap()
        .spawn("private-task-name", TaskKind::Worker, async move {
            cancellation.cancelled().await
        })
        .unwrap();
    let short = root.task_group().diagnostics_task_details(8, 1);
    assert_eq!(short.group_entries_scanned, 8);
    assert!(short.truncated);
    assert!(short.details.is_empty());
    let full = root.task_group().diagnostics_task_details(1024, 1);
    assert_eq!(full.group_entries_scanned, 512);
    assert_eq!(full.tasks_scanned, 1);
    assert!(!full.truncated);
    assert!(!serde_json::to_string(&full).unwrap().contains("private-task-name"));
    assert!(context.shutdown_tasks(Duration::from_secs(5)).await.is_healthy());
    while groups.pop().is_some() {}
}
