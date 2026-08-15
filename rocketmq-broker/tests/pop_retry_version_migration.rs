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

use rocketmq_broker::test_support::PopProfileStoreProbe;
use rocketmq_model::common::pop_retry_policy::PopRetryMigrationState;
use rocketmq_model::common::pop_retry_policy::PopRetryPolicy;
use tempfile::TempDir;

#[test]
fn group_policy_survives_upgrade_write_rollback_and_restart() {
    let root = TempDir::new().expect("temp dir");
    let store = PopProfileStoreProbe::open(root.path(), 16).expect("open profile store");

    let v1 = store
        .upsert_policy("group-a", &["orders"], PopRetryPolicy::v1_only(0), 10)
        .expect("persist v1-only policy");
    let dual_v1 = store
        .upsert_policy(
            "group-a",
            &["orders"],
            v1.retry_policy
                .transition_to(PopRetryMigrationState::DualReadV1Write, 2)
                .expect("enter dual-read before switching writes"),
            11,
        )
        .expect("persist dual-read/v1-write policy");
    let dual_v2 = store
        .upsert_policy(
            "group-a",
            &["orders"],
            dual_v1
                .retry_policy
                .transition_to(PopRetryMigrationState::DualReadV2Write, 3)
                .expect("switch writes to v2"),
            12,
        )
        .expect("persist dual-read/v2-write policy");
    let rolled_back = store
        .upsert_policy(
            "group-a",
            &["orders"],
            dual_v2
                .retry_policy
                .transition_to(PopRetryMigrationState::DualReadV1Write, 4)
                .expect("roll writes back while retaining v2 reads"),
            13,
        )
        .expect("persist rollback policy");
    assert_eq!(
        rolled_back.retry_policy.state(),
        Ok(PopRetryMigrationState::DualReadV1Write)
    );
    drop(store);

    let reopened = PopProfileStoreProbe::open(root.path(), 16).expect("reopen profile store");
    let snapshot = reopened.snapshot();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].generation, 4);
    assert_eq!(
        snapshot[0].retry_policy.state(),
        Ok(PopRetryMigrationState::DualReadV1Write)
    );
    assert_eq!(
        snapshot[0].retry_policy.read_topics("orders", "group-a"),
        vec!["%RETRY%group-a_orders", "%RETRY%group-a+orders"]
    );
}

#[test]
fn persisted_profile_rejects_a_skipped_v1_to_v2_only_transition() {
    let root = TempDir::new().expect("temp dir");
    let store = PopProfileStoreProbe::open(root.path(), 16).expect("open profile store");
    store
        .upsert_policy("group-a", &["orders"], PopRetryPolicy::v1_only(0), 10)
        .expect("persist v1-only policy");

    let error = store
        .upsert_policy("group-a", &["orders"], PopRetryPolicy::v2_only(2), 11)
        .expect_err("migration must not skip both dual-read states");
    assert!(error.contains("not safe"), "{error}");
    assert_eq!(store.generation(), 1);
}
