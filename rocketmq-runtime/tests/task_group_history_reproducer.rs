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

const OPERATIONS: usize = 100_000;

#[tokio::test]
#[ignore = "red reproducer: active-only registration is implemented by issue #8849 follow-up"]
async fn transient_static_children_do_not_retain_history_without_scrape() {
    let context = RuntimeContext::from_current("task-group-static-history-reproducer");
    let parent = context.root_group().child("component");

    for index in 0..OPERATIONS {
        drop(parent.child(format!("operation-{index}")));
    }

    assert_eq!(
        parent.child_count(),
        0,
        "dropping transient static children must unregister them from the parent"
    );
}

#[tokio::test]
#[ignore = "red reproducer: lease drop must unregister without diagnostics-driven pruning"]
async fn transient_child_leases_do_not_require_a_diagnostics_scrape_for_cleanup() {
    let context = RuntimeContext::from_current("task-group-lease-history-reproducer");
    let parent = context.root_group().child("component");

    for index in 0..OPERATIONS {
        drop(
            parent
                .try_child_lease(format!("operation-{index}"))
                .expect("the parent remains open throughout the reproducer"),
        );
    }

    let stats = parent.child_stats();
    assert_eq!(stats.active, 0);
    assert_eq!(
        stats.pruned, 0,
        "lease drop must unregister immediately instead of leaving cleanup to child_stats"
    );
}
