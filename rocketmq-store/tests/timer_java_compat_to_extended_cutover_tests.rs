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

#![cfg(feature = "extended_timeline")]

use std::sync::Arc;
use std::sync::OnceLock;

use dashmap::DashMap;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_store::BrokerStorePort;
use rocketmq_store::LocalFileMessageStore;
use rocketmq_store::MessageStoreConfig;
use rocketmq_store::StoreRuntimeConfig;
use rocketmq_store_api::TimerStoreMode;

fn service_context() -> ChildServiceContext {
    static OWNER: OnceLock<RuntimeOwner> = OnceLock::new();
    OWNER
        .get_or_init(|| {
            RuntimeOwner::new(RuntimeConfig::server_default("timer-cutover-contract")).expect("timer cutover runtime")
        })
        .root_context()
        .component("store")
}

fn store(root: &std::path::Path, mode: TimerStoreMode, epoch: u64) -> LocalFileMessageStore {
    let config = MessageStoreConfig {
        store_path_root_dir: root.to_string_lossy().into_owned().into(),
        timer_wheel_enable: false,
        timer_store_mode: mode,
        timer_extended_shadow_enable: false,
        timer_extended_admission_enable: mode == TimerStoreMode::ExtendedTimeline,
        timer_extended_activation_epoch: epoch,
        timer_extended_admission_horizon_days: 3,
        read_uncommitted: true,
        duplication_enable: true,
        ..MessageStoreConfig::default()
    };
    LocalFileMessageStore::new(
        Arc::new(config),
        rocketmq_store_local::commit_log::append::micro_batch::MicroBatchPolicy::disabled(1)
            .expect("valid test policy"),
        Arc::new(StoreRuntimeConfig::default()),
        Arc::new(DashMap::new()),
        None,
        false,
        service_context(),
    )
    .expect("create formal Timer cutover Store")
    .expect("test Timer Store configuration is valid")
}

fn shadow_store(root: &std::path::Path) -> LocalFileMessageStore {
    let config = MessageStoreConfig {
        store_path_root_dir: root.to_string_lossy().into_owned().into(),
        timer_wheel_enable: false,
        timer_store_mode: TimerStoreMode::JavaCompat,
        timer_extended_shadow_enable: true,
        read_uncommitted: true,
        duplication_enable: true,
        ..MessageStoreConfig::default()
    };
    LocalFileMessageStore::new(
        Arc::new(config),
        rocketmq_store_local::commit_log::append::micro_batch::MicroBatchPolicy::disabled(1)
            .expect("valid test policy"),
        Arc::new(StoreRuntimeConfig::default()),
        Arc::new(DashMap::new()),
        None,
        false,
        service_context(),
    )
    .expect("create shadow Timer cutover Store")
    .expect("test Timer Store configuration is valid")
}

#[tokio::test]
async fn formal_activation_requires_a_nonzero_epoch() {
    let root = tempfile::tempdir().expect("store root");
    let mut store = store(root.path(), TimerStoreMode::ExtendedTimeline, 0);

    let error = store.init().await.expect_err("zero epoch must fail closed");

    assert!(error.to_string().contains("non-zero activation epoch"), "{error}");
}

#[tokio::test]
async fn shadow_observation_can_roll_back_to_java_compat_without_conversion() {
    let root = tempfile::tempdir().expect("store root");
    let mut shadow = shadow_store(root.path());
    shadow
        .wire_owned_root_dependencies()
        .expect("wire Extended Timeline shadow");
    shadow.init().await.expect("initialize shadow");
    drop(shadow);

    let mut java_compat = store(root.path(), TimerStoreMode::JavaCompat, 0);
    java_compat.init().await.expect("shadow never claims formal ownership");
}

#[tokio::test]
async fn formal_owner_marker_blocks_silent_java_compat_rollback() {
    let root = tempfile::tempdir().expect("store root");
    let mut formal = store(root.path(), TimerStoreMode::ExtendedTimeline, 7);
    formal
        .wire_owned_root_dependencies()
        .expect("wire formal Extended Timeline");
    formal.init().await.expect("activate formal Extended Timeline");
    drop(formal);

    let mut rollback = store(root.path(), TimerStoreMode::JavaCompat, 0);
    let error = rollback
        .init()
        .await
        .expect_err("formal ownership requires an explicit offline conversion");

    assert!(error.to_string().contains("rollback is unsafe"), "{error}");
}

#[tokio::test]
async fn formal_restart_requires_the_persisted_activation_epoch() {
    let root = tempfile::tempdir().expect("store root");
    let mut first = store(root.path(), TimerStoreMode::ExtendedTimeline, 11);
    first
        .wire_owned_root_dependencies()
        .expect("wire formal Extended Timeline");
    first.init().await.expect("activate formal Extended Timeline");
    drop(first);

    let mut incompatible = store(root.path(), TimerStoreMode::ExtendedTimeline, 12);
    let error = incompatible
        .init()
        .await
        .expect_err("activation epoch cannot change across restart");

    assert!(error.to_string().contains("does not match persisted epoch"), "{error}");
}
