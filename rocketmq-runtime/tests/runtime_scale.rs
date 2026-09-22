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

use std::sync::Arc;
use std::time::Duration;

use rocketmq_runtime::*;

#[tokio::test]
async fn completed_scopes_and_dynamic_keys_reclaim_registrations_under_churn() {
    struct Clock;
    impl MonotonicClock for Clock {
        fn now(&self) -> Duration {
            Duration::ZERO
        }
    }
    let context = RuntimeContext::from_current("registry-churn");
    let limit = BudgetLimit::new(4, 64, FullPolicy::Reject);
    let tree = ResourceBudgetTree::with_clock_and_key_capacity("churn", limit, Arc::new(Clock), 1).unwrap();
    let budget = tree.root();
    let mut escaped = Vec::new();
    for iteration in 0..5_000 {
        let middle = context.service_context("temporary");
        let leaf = middle.component("nested").component("leaf");
        let id = leaf.spawn("short-work", TaskKind::Worker, async {}).unwrap();
        drop(middle);
        assert!(leaf.task_group().wait_task(id, Duration::from_secs(2)).await);
        drop(leaf);
        let key = budget.register_dynamic_child("reused-key", limit).unwrap();
        let old = key.budget();
        let permit = old.try_acquire_data(8).unwrap();
        key.close();
        assert!(!key.retire_until(tokio::time::Instant::now()).await.released);
        drop(permit);
        assert!(key.retire_until(tokio::time::Instant::now()).await.released);
        escaped.push(old);
        if escaped.len() > 32 {
            escaped.remove(0);
        }
        if iteration % 100 == 0 {
            let state = context.diagnostics_snapshot();
            assert_eq!(state.task_count, 0);
            assert_eq!(state.child_count, 0);
            assert_eq!(budget.snapshot().current_count, 0);
            assert_eq!(budget.snapshot().current_bytes, 0);
            for stale in &escaped {
                assert!(stale.try_acquire_data(1).unwrap_err().is_closed());
            }
        }
    }
    let state = budget.snapshot();
    assert_eq!(state.admitted_count, state.released_count);
    assert!(context.shutdown_tasks(Duration::from_secs(2)).await.is_healthy());
    println!("churn: 5000 scopes/keys; retained stale handles <=32; live tasks/children/reservations=0");
}

#[test]
fn metadata_history_retirement_remains_bounded_over_many_actor_generations() {
    #[derive(Debug)]
    struct CompletedWrite;
    impl MetadataFileSystem for CompletedWrite {
        fn persist_atomic(&self, _: &std::path::Path, _: &[u8]) -> RuntimeResult<()> {
            Ok(())
        }
    }
    let owner = RuntimeOwner::plan(RuntimeConfig::for_parallelism("metadata-churn", 2))
        .unwrap()
        .with_metadata_target_capacity(std::num::NonZeroUsize::new(1).unwrap())
        .build()
        .unwrap();
    owner.block_on(async {
        let parent = owner.root_context().component("metadata");
        let mut receipts = Vec::new();
        for index in 1..=1_024 {
            let actor = MetadataIoConfig::default()
                .into_plan()
                .unwrap()
                .start_with_file_system(&parent, Arc::new(CompletedWrite))
                .unwrap();
            let deadline = MetadataDeadline::after(Duration::from_secs(2));
            let MetadataIoAdmissionOutcome::Accepted(receipt) = actor
                .submit(
                    MetadataWriteRequest::new("resource", index, "metadata-churn.json", vec![1]),
                    deadline,
                )
                .unwrap()
            else {
                panic!("free target");
            };
            assert!(!actor.shutdown_until(deadline).await.timed_out);
            assert_eq!(
                actor.retire_target_for_new_identity("resource").unwrap(),
                MetadataTargetRetirementOutcome::Retired
            );
            assert_eq!(owner.resources().metadata_target_stats().retained_targets, 0);
            receipts.push((index, receipt));
        }
        for (index, receipt) in receipts {
            assert_eq!(
                receipt
                    .wait_until(MetadataDeadline::after(Duration::from_secs(1)))
                    .await
                    .unwrap(),
                MetadataGeneration::new(index)
            );
        }
    });
    assert!(owner
        .shutdown_runtime_blocking_with_timeout(Duration::from_secs(2))
        .unwrap()
        .is_healthy());
    println!(
        "metadata churn: 1024 identities; capacity=1; retained targets=0; old receipts preserved (injected filesystem)"
    );
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires an actual cgroup hard limit and ROCKETMQ_TEST_CGROUP_BYTES"]
fn detects_actual_cgroup_limit_and_uses_it_in_owner_planning() {
    let expected: u64 = std::env::var("ROCKETMQ_TEST_CGROUP_BYTES").unwrap().parse().unwrap();
    let detected = ProcessMemoryLimit::detect().unwrap();
    assert_eq!(detected.source(), MemoryLimitSource::CgroupV2);
    assert_eq!(detected.bytes(), expected);
    let plan = RuntimeOwner::plan(RuntimeConfig::for_parallelism("cgroup-probe", 2)).unwrap();
    let owner = plan.build().unwrap();
    assert_eq!(owner.resources().memory_budget().detected(), detected);
    assert!(owner
        .shutdown_runtime_blocking_with_timeout(Duration::from_secs(2))
        .unwrap()
        .is_healthy());
    println!("actual cgroup v2 limit={expected}; owner memory planning matches detected kernel constraint");
}
