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

//! The native engine as the store runs it under `timerStoreMode = native`, beside the
//! Java-compatible engine, which still delivers the timers it admitted.

use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::MessageConst;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_store_api::TimerEngineId;
use rocketmq_store_api::TimerRecallRegistration;
use rocketmq_store_api::TimerRecallStatus;
use rocketmq_store_api::TimerStoreMode;
use tempfile::tempdir;

use super::support::delivered;
use super::support::delivered_keys;
use super::support::process_until_idle;
use super::support::put_all;
use super::support::timer_message;
use super::support::with_property;
use super::support::ManualClock;
use super::support::REAL_TOPIC;
use crate::base::backend_ops::MessageStoreEngine;
use crate::config::timer_native_config::TimerNativeConfig;
use crate::timer::timer_message_store::build_delete_key;
use crate::timer::timer_message_store::TIMER_TOPIC;
use crate::LocalFileMessageStore;
use crate::MessageStoreConfig;
use crate::StoreRuntimeConfig;

const HOUR_MS: i64 = 3_600_000;

/// Opens a store under `root` in `mode`, or `None` when it does not load.
async fn open(root: &Path, mode: TimerStoreMode) -> Option<LocalFileMessageStore> {
    let config = MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(root.to_string_lossy().into_owned()),
        read_uncommitted: true,
        timer_store_mode: mode,
        timer_native: TimerNativeConfig {
            lane_count: 4,
            ..TimerNativeConfig::default()
        },
        ..MessageStoreConfig::default()
    };
    let topic_config_table = Arc::new(DashMap::new());
    for topic in [REAL_TOPIC, TIMER_TOPIC] {
        topic_config_table.insert(CheetahString::from_static_str(topic), Arc::new(TopicConfig::default()));
    }
    let mut store = LocalFileMessageStore::new(
        Arc::new(config),
        rocketmq_store_local::commit_log::append::micro_batch::MicroBatchPolicy::disabled(1)
            .expect("valid test policy"),
        Arc::new(StoreRuntimeConfig::default()),
        topic_config_table,
        None,
        false,
        crate::runtime::test_service_context("timer-native-test"),
    )
    .expect("create a native timer test store")
    .expect("the test store configuration is valid");
    store
        .wire_owned_root_dependencies()
        .expect("wire the store's timer capabilities");
    if store.init().await.is_err() || !store.load().await {
        return None;
    }
    Some(store)
}

/// Shuts `store` down and releases its root.
async fn close(mut store: LocalFileMessageStore) {
    store.shutdown().await;
}

/// A timer the Broker admitted to the native engine.
fn native(message: MessageExtBrokerInner) -> MessageExtBrokerInner {
    with_property(message, MessageConst::TIMER_ENGINE_TYPE, TimerEngineId::Native.as_str())
}

/// A timer the Broker admitted to the Java-compatible engine, with the route envelope it
/// stamps.
fn java(message: MessageExtBrokerInner) -> MessageExtBrokerInner {
    [
        (MessageConst::TIMER_ENGINE_TYPE, TimerEngineId::JavaCompat.as_str()),
        (MessageConst::PROPERTY_TIMER_FORMAT_VERSION, "1"),
        (MessageConst::PROPERTY_TIMER_POLICY_FINGERPRINT, "1"),
        (MessageConst::PROPERTY_TIMER_GENERATION, "0"),
        (MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN, "F:1:j0:0:0"),
    ]
    .into_iter()
    .fold(message, |message, (key, value)| with_property(message, key, value))
}

/// The recall of the timer whose unique key is `target`, due at `deliver_ms`.
fn recall_of(target: &str, deliver_ms: i64) -> MessageExtBrokerInner {
    native(with_property(
        timer_message(deliver_ms, &format!("recall-{target}")),
        MessageConst::PROPERTY_TIMER_DEL_UNIQKEY,
        build_delete_key(REAL_TOPIC, target).to_string(),
    ))
}

fn keys(keys: &[&str]) -> HashSet<String> {
    keys.iter().map(|key| (*key).to_owned()).collect()
}

#[tokio::test]
async fn native_timers_go_to_the_native_engine_and_java_timers_stay_with_the_java_engine() {
    let root = tempdir().unwrap();
    let mut store = open(root.path(), TimerStoreMode::Native)
        .await
        .expect("open a native store");
    let timer_store = store.get_timer_message_store().cloned().unwrap();
    let native_timer = store
        .native_timer()
        .cloned()
        .expect("native mode runs the native engine");
    let now_ms = current_millis() as i64;
    put_all(
        &mut store,
        vec![
            native(timer_message(now_ms - 1_000, "n0")),
            java(timer_message(now_ms - 1_000, "j0")),
            native(timer_message(now_ms + 2 * HOUR_MS, "n1")),
        ],
    )
    .await;

    native_timer.set_delivering(true);
    assert_eq!(native_timer.process_once().await, 1);
    store.reput_once().await;
    assert_eq!(delivered_keys(&store), keys(&["n0"]));
    assert!(delivered(&store)[0].1.is_some(), "a delivered timer carries its token");

    // The Java-compatible engine delivers a slot once the clock has passed its end.
    timer_store.set_clock(Arc::new(ManualClock(AtomicI64::new(now_ms + 5_000))));
    timer_store.set_should_running_dequeue(true);
    process_until_idle(&timer_store).await;
    store.reput_once().await;
    // Each engine delivered its own timer once; the native timer due later waits.
    assert_eq!(delivered(&store).len(), 2);
    assert_eq!(delivered_keys(&store), keys(&["n0", "j0"]));
    close(store).await;
}

#[tokio::test]
async fn a_native_recall_cancels_its_timer() {
    let root = tempdir().unwrap();
    let mut store = open(root.path(), TimerStoreMode::Native)
        .await
        .expect("open a native store");
    let native_timer = store.native_timer().cloned().unwrap();
    let due_ms = current_millis() as i64 - 1_000;
    put_all(
        &mut store,
        vec![
            native(timer_message(due_ms, "n0")),
            recall_of("n0", due_ms),
            native(timer_message(due_ms, "n1")),
        ],
    )
    .await;

    native_timer.set_delivering(true);
    assert_eq!(native_timer.process_once().await, 2);
    store.reput_once().await;
    assert_eq!(delivered_keys(&store), keys(&["n1"]));
    close(store).await;
}

#[tokio::test]
async fn nothing_is_delivered_on_a_slave() {
    let root = tempdir().unwrap();
    let mut store = open(root.path(), TimerStoreMode::Native)
        .await
        .expect("open a native store");
    let native_timer = store.native_timer().cloned().unwrap();
    put_all(
        &mut store,
        vec![native(timer_message(current_millis() as i64 - 1_000, "n0"))],
    )
    .await;

    assert_eq!(native_timer.process_once().await, 0);
    native_timer.set_delivering(true);
    assert_eq!(native_timer.process_once().await, 1);
    close(store).await;
}

#[tokio::test]
async fn delivered_native_timers_stay_delivered_across_a_restart() {
    let root = tempdir().unwrap();
    let mut store = open(root.path(), TimerStoreMode::Native)
        .await
        .expect("open a native store");
    let native_timer = store.native_timer().cloned().unwrap();
    let now_ms = current_millis() as i64;
    put_all(
        &mut store,
        vec![
            native(timer_message(now_ms - 1_000, "n0")),
            native(timer_message(now_ms + 2 * HOUR_MS, "n1")),
        ],
    )
    .await;
    native_timer.set_delivering(true);
    assert_eq!(native_timer.process_once().await, 1);
    drop(native_timer);
    close(store).await;

    let mut store = open(root.path(), TimerStoreMode::Native)
        .await
        .expect("reopen the native store");
    let native_timer = store.native_timer().cloned().unwrap();
    native_timer.set_delivering(true);
    assert_eq!(native_timer.process_once().await, 0);
    store.reput_once().await;
    assert_eq!(delivered(&store).len(), 1);
    // The later timer waits in its hour bucket.
    assert_eq!(native_timer.engine().counts().hours, 1);
    close(store).await;
}

#[tokio::test]
async fn a_store_holding_native_timers_loads_only_in_native_mode() {
    let root = tempdir().unwrap();
    let store = open(root.path(), TimerStoreMode::Native)
        .await
        .expect("open a native store");
    close(store).await;

    assert!(open(root.path(), TimerStoreMode::JavaCompat).await.is_none());
    let store = open(root.path(), TimerStoreMode::Native)
        .await
        .expect("reopen the native store");
    close(store).await;
}

fn registration(target: &str, deliver_ms: i64) -> TimerRecallRegistration {
    TimerRecallRegistration {
        key: build_delete_key(REAL_TOPIC, target).to_string(),
        generation: 0,
        deliver_ms,
    }
}

#[tokio::test]
async fn a_recall_registered_before_its_tombstone_cancels_its_timer_unless_withdrawn() {
    let root = tempdir().unwrap();
    let mut store = open(root.path(), TimerStoreMode::Native)
        .await
        .expect("open a native store");
    let native_timer = store.native_timer().cloned().unwrap();
    let start_ms = current_millis() as i64;
    let clock = Arc::new(ManualClock(AtomicI64::new(start_ms)));
    native_timer.set_clock(clock.clone());
    let due_ms = start_ms + 1_000;
    put_all(
        &mut store,
        vec![native(timer_message(due_ms, "n0")), native(timer_message(due_ms, "n1"))],
    )
    .await;
    let register = |store: &LocalFileMessageStore, target| {
        MessageStoreEngine::register_native_timer_recall(store, &registration(target, due_ms)).unwrap()
    };

    assert_eq!(register(&store, "n0"), TimerRecallStatus::Cancelled);
    // The tombstone of the recall of n1 could not be written.
    assert_eq!(register(&store, "n1"), TimerRecallStatus::Cancelled);
    MessageStoreEngine::withdraw_native_timer_recall(&store, &registration("n1", due_ms));

    clock.0.store(due_ms, Ordering::Release);
    native_timer.set_delivering(true);
    assert_eq!(native_timer.process_once().await, 2);
    store.reput_once().await;
    assert_eq!(delivered_keys(&store), keys(&["n1"]));
    assert_eq!(register(&store, "n1"), TimerRecallStatus::TooLate);
    close(store).await;
}

#[tokio::test]
async fn native_recalls_are_unsupported_in_other_modes() {
    let root = tempdir().unwrap();
    let store = open(root.path(), TimerStoreMode::JavaCompat)
        .await
        .expect("open a Java-compatible store");
    let deliver_ms = current_millis() as i64 + 60_000;
    assert_eq!(
        MessageStoreEngine::register_native_timer_recall(&store, &registration("j0", deliver_ms)).unwrap(),
        TimerRecallStatus::Unsupported
    );
    close(store).await;
}
