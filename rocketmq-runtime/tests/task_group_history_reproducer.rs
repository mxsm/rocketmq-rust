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

use rocketmq_runtime::RuntimeContext;

const PR_OPERATIONS: usize = 1_024;

#[tokio::test]
async fn transient_static_children_do_not_retain_history_without_scrape() {
    let context = RuntimeContext::from_current("task-group-static-history-reproducer");
    let parent = context.root_group().child("component");

    exercise_static_child_churn(&parent, PR_OPERATIONS);
}

#[tokio::test]
async fn transient_child_leases_do_not_require_a_diagnostics_scrape_for_cleanup() {
    let context = RuntimeContext::from_current("task-group-lease-history-reproducer");
    let parent = context.root_group().child("component");

    exercise_child_lease_churn(&parent, PR_OPERATIONS);
}

#[tokio::test]
#[ignore = "100k churn profile is reserved for explicit lifecycle validation"]
async fn churn_100k() {
    let context = RuntimeContext::from_current("task-group-100k-history-reproducer");
    let parent = context.root_group().child("component");

    exercise_static_child_churn(&parent, 100_000);
    exercise_child_lease_churn(&parent, 100_000);
}

#[tokio::test]
#[ignore = "1m churn profile is reserved for explicit lifecycle validation"]
async fn churn_1m() {
    let context = RuntimeContext::from_current("task-group-1m-history-reproducer");
    let parent = context.root_group().child("component");

    exercise_static_child_churn(&parent, 1_000_000);
    exercise_child_lease_churn(&parent, 1_000_000);
}

fn exercise_static_child_churn(parent: &rocketmq_runtime::TaskGroup, operations: usize) {
    for index in 0..operations {
        drop(parent.child(format!("operation-{index}")));
    }

    assert_eq!(
        parent.child_count(),
        0,
        "dropping transient static children must unregister them from the parent"
    );
}

fn exercise_child_lease_churn(parent: &rocketmq_runtime::TaskGroup, operations: usize) {
    let before = parent.child_stats();
    for index in 0..operations {
        drop(
            parent
                .try_child_lease(format!("operation-{index}"))
                .expect("the parent remains open throughout the reproducer"),
        );
    }

    let stats = parent.child_stats();
    assert_eq!(stats.active, 0);
    assert_eq!(stats.registry_slots, 0);
    assert_eq!(stats.created - before.created, operations);
    assert_eq!(
        stats.pruned - before.pruned,
        operations,
        "lease drop must unregister immediately"
    );
}
