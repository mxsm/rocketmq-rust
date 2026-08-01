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

use rocketmq_client_rust::test_support::run_lite_pull_assignment_registry_probe;

#[tokio::test]
async fn assignment_registry_releases_every_resource_after_100k_churn() {
    const ITERATIONS: usize = 100_000;

    let result = run_lite_pull_assignment_registry_probe(ITERATIONS).await;

    assert_eq!(result.iterations, ITERATIONS);
    assert_eq!(result.peak_entries, 1);
    assert_eq!(result.final_entries, 0);
    assert_eq!(result.final_owned_tasks, 0);
    assert!(result.same_queue_serialized);
    assert!(result.different_queues_independent);
    assert!(result.stale_entry_rejected);
}

#[test]
fn lite_pull_queue_resources_have_one_registry_owner() {
    let source = include_str!("../src/consumer/consumer_impl/default_lite_pull_consumer_impl.rs");

    assert!(source.contains("assignment_registry: Arc<AssignmentRegistry>"));
    assert!(!source.contains("message_queue_locks:"));
    assert!(!source.contains("task_handles:"));
}
