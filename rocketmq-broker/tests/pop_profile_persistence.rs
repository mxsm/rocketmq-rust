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

use std::error::Error as StdError;

use rocketmq_broker::test_support::PopProfileStoreProbe;
use rocketmq_error::ViewValueRef;
use rocketmq_error::CORE_CONFIGURATION_INVALID;
use rocketmq_error::CORE_INTERNAL_FAILURE;
use rocketmq_error::STORAGE_STATE_CORRUPTED;
use rocketmq_model::common::pop_retry_policy::PopRetryPolicy;
use rocketmq_store_api::StoreComponent;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;
use tempfile::TempDir;

#[test]
fn profile_survives_restart_and_isolated_from_inflight_state() {
    let root = TempDir::new().expect("temp dir");
    let store = PopProfileStoreProbe::open(root.path(), 16).expect("open profile store");
    let profile = store
        .upsert("group-a", &["topic-a", "%RETRY%group-a"], 2, 42)
        .expect("persist profile");
    assert_eq!(profile.generation, 1);
    assert_eq!(store.inflight_record_count().expect("scan inflight"), 0);
    let inventory: serde_json::Value = serde_json::from_slice(
        &std::fs::read(root.path().join("config/storage-format-inventory.json")).expect("storage format inventory"),
    )
    .expect("decode storage format inventory");
    assert_eq!(
        inventory
            .pointer("/popConsumerProfile/declared")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
    drop(store);

    let reopened = PopProfileStoreProbe::open(root.path(), 16).expect("reopen profile store");
    let snapshot = reopened.snapshot();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].group, "group-a");
    assert_eq!(snapshot[0].topics, vec!["%RETRY%group-a", "topic-a"]);
    assert_eq!(snapshot[0].retry_version, 2);
    assert_eq!(snapshot[0].last_seen, 42);
}

#[test]
fn tombstone_prevents_deleted_profile_from_resurrecting() {
    let root = TempDir::new().expect("temp dir");
    let store = PopProfileStoreProbe::open(root.path(), 16).expect("open profile store");
    store.upsert("group-a", &["topic-a"], 1, 10).expect("persist profile");
    assert!(store.remove("group-a", 11).expect("remove profile"));
    drop(store);

    let reopened = PopProfileStoreProbe::open(root.path(), 16).expect("reopen profile store");
    assert!(reopened.snapshot().is_empty());
    assert_eq!(reopened.generation(), 2);
}

#[test]
fn unknown_format_version_fails_closed() {
    let root = TempDir::new().expect("temp dir");
    PopProfileStoreProbe::write_unknown_marker(root.path(), 99).expect("write marker fixture");

    let error = PopProfileStoreProbe::open_with_error(root.path(), 16).expect_err("unknown version must fail");
    assert_eq!(error.descriptor(), &CORE_INTERNAL_FAILURE);
    let source = error
        .source()
        .and_then(|source| source.downcast_ref::<StoreError>())
        .expect("the broker error must preserve the typed storage failure");
    assert_eq!(source.descriptor(), &STORAGE_STATE_CORRUPTED);
    assert_eq!(source.operation(), StoreOperation::Admin);
    assert_eq!(source.component(), StoreComponent::RocksDb);
}

#[test]
fn capacity_failure_does_not_advance_generation() {
    let root = TempDir::new().expect("temp dir");
    let store = PopProfileStoreProbe::open(root.path(), 1).expect("open profile store");
    store
        .upsert("group-a", &["topic-a"], 1, 10)
        .expect("persist first profile");
    let original = store.snapshot();

    let error = store
        .upsert_policy_with_error("group-b", &["topic-b"], PopRetryPolicy::v1_only(0), 11)
        .expect_err("capacity must be enforced");
    assert_eq!(error.descriptor(), &CORE_CONFIGURATION_INVALID);
    assert_eq!(
        error
            .diagnostic_view()
            .expect("valid configuration error context")
            .fields()
            .find(|field| field.name() == "key")
            .map(|field| field.value()),
        Some(ViewValueRef::Text("capacity"))
    );
    assert_eq!(store.generation(), 1);
    assert_eq!(store.snapshot(), original);
    drop(store);

    let reopened = PopProfileStoreProbe::open(root.path(), 1).expect("reopen profile store");
    assert_eq!(reopened.generation(), 1);
    assert_eq!(reopened.snapshot(), original);
}
