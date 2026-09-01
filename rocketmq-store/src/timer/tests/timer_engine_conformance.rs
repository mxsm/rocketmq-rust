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

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
#[cfg(feature = "extended_timeline")]
use dashmap::DashMap;
use rocketmq_store_api::TimerEngineId;
use rocketmq_store_api::TimerStoreMode;

use crate::timer::engine::select_engine_owner;
use crate::timer::engine::TimerEngine;
use crate::timer::engine::WorkBudget;
use crate::timer::java_compat::JavaCompatEngine;
use crate::timer::timer_message_store::TimerMessageStore;
#[cfg(feature = "extended_timeline")]
use crate::LocalFileMessageStore;
use crate::MessageStoreConfig;
#[cfg(feature = "extended_timeline")]
use crate::StoreRuntimeConfig;

async fn assert_engine_conformance<E>(engine: E, expected: TimerEngineId)
where
    E: TimerEngine,
{
    assert_eq!(engine.engine_id(), expected);
    engine.load().await.expect("load contract");
    let progress = engine
        .enqueue_source(WorkBudget::try_new(1, 1_024, Instant::now() + Duration::from_secs(1)).expect("budget"))
        .await
        .expect("bounded source pump");
    assert!(progress.messages <= 1);
    assert!(progress.bytes <= 1_024);
    assert!(progress.durable);
    engine.checkpoint().await.expect("checkpoint contract");
    engine.shutdown().await.expect("shutdown contract");
}

#[test]
fn routing_defaults_to_java_compat_inside_its_horizon() {
    let owner = select_engine_owner(TimerStoreMode::JavaCompat, None, 60_000, 86_400_000, false)
        .expect("Java-compatible route");
    assert_eq!(owner, TimerEngineId::JavaCompat);
}

#[test]
fn routing_fails_closed_when_extended_capability_is_unavailable() {
    assert!(select_engine_owner(
        TimerStoreMode::ExtendedTimeline,
        None,
        365 * 86_400_000,
        3 * 86_400_000,
        false,
    )
    .is_none());
}

#[test]
fn routing_preserves_the_persisted_owner_after_configuration_changes() {
    let owner = select_engine_owner(
        TimerStoreMode::ExtendedTimeline,
        Some(TimerEngineId::JavaCompat),
        365 * 86_400_000,
        3 * 86_400_000,
        true,
    )
    .expect("persisted owner");
    assert_eq!(owner, TimerEngineId::JavaCompat);
}

#[tokio::test]
async fn java_compat_engine_conformance_obeys_bounded_pump_and_checkpoint_contracts() {
    let directory = tempfile::tempdir().expect("timer root");
    let config = Arc::new(MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(directory.path().to_string_lossy().into_owned()),
        read_uncommitted: true,
        ..MessageStoreConfig::default()
    });
    let store = Arc::new(TimerMessageStore::new_with_message_store_config(
        config,
        crate::runtime::test_service_context("timer-engine-conformance"),
    ));
    assert!(store.load());
    let engine = JavaCompatEngine::new(store);
    assert_engine_conformance(engine, TimerEngineId::JavaCompat).await;
}

#[cfg(feature = "extended_timeline")]
#[tokio::test]
async fn extended_timeline_engine_uses_the_same_bounded_conformance_contract() {
    let directory = tempfile::tempdir().expect("timer root");
    let config = Arc::new(MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(directory.path().to_string_lossy().into_owned()),
        timer_wheel_enable: false,
        timer_extended_shadow_enable: true,
        read_uncommitted: true,
        duplication_enable: true,
        ..MessageStoreConfig::default()
    });
    let mut store = LocalFileMessageStore::new(
        config,
        rocketmq_store_local::commit_log::append::micro_batch::MicroBatchPolicy::disabled(1)
            .expect("valid test policy"),
        Arc::new(StoreRuntimeConfig::default()),
        Arc::new(DashMap::new()),
        None,
        false,
        crate::runtime::test_service_context("extended-timer-engine-conformance"),
    )
    .expect("create Extended Timeline conformance Store")
    .expect("test Timer Store configuration is valid");
    store.wire_owned_root_dependencies().expect("wire Extended Timeline");
    let engine = store
        .extended_timeline_engine_for_test()
        .expect("Extended Timeline engine");

    assert_engine_conformance(engine, TimerEngineId::ExtendedTimeline).await;
}
