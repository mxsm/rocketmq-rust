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

use bytes::Bytes;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_protocol::common::message::message_decoder::message_properties_to_string;
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

/// A timer the Broker admitted to the native engine, with the delivery token it gives it.
fn native(message: MessageExtBrokerInner) -> MessageExtBrokerInner {
    let unique_key = message
        .property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
        ))
        .map(|key| key.to_string())
        .unwrap_or_default();
    let message = with_property(message, MessageConst::TIMER_ENGINE_TYPE, TimerEngineId::Native.as_str());
    with_property(
        message,
        MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN,
        native_token(&unique_key),
    )
}

fn native_token(unique_key: &str) -> String {
    format!("N:1:{unique_key}:0:0")
}

/// The message a master wrote for native timer `unique_key`, as its slave receives it.
fn delivered_by_master(unique_key: &str) -> MessageExtBrokerInner {
    let mut message = MessageExtBrokerInner::default();
    message.set_topic(CheetahString::from_static_str(REAL_TOPIC));
    message.message_ext_inner.queue_id = 0;
    message.set_body(Bytes::from_static(b"timer-test-body"));
    for (key, value) in [
        (
            MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            unique_key.to_owned(),
        ),
        (MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN, native_token(unique_key)),
    ] {
        message.put_property(CheetahString::from_static_str(key), CheetahString::from_string(value));
    }
    message.properties_string = message_properties_to_string(message.get_properties());
    message
}

/// The unique keys of the messages in the real topic, in queue order, repeats included.
fn delivered_in_order(store: &LocalFileMessageStore) -> Vec<String> {
    delivered(store).into_iter().map(|(key, _)| key).collect()
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

/// The recall of `target` as the Broker writes it while the Java-compatible engine is active.
fn java_recall_of(target: &str, deliver_ms: i64) -> MessageExtBrokerInner {
    java(with_property(
        timer_message(deliver_ms, &format!("recall-{target}")),
        MessageConst::PROPERTY_TIMER_DEL_UNIQKEY,
        build_delete_key(REAL_TOPIC, target).to_string(),
    ))
}

/// A clock standing at `time_ms`.
fn clock_at(time_ms: i64) -> Arc<ManualClock> {
    Arc::new(ManualClock(AtomicI64::new(time_ms)))
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

/// Opens a store in native mode that indexed `timers`, then closes it.
async fn native_store_holding(root: &Path, timers: Vec<MessageExtBrokerInner>) {
    let mut store = open(root, TimerStoreMode::Native).await.expect("open a native store");
    let native_timer = store.native_timer().cloned().unwrap();
    put_all(&mut store, timers).await;
    assert_eq!(native_timer.process_once().await, 0);
    drop(native_timer);
    close(store).await;
}

#[tokio::test]
async fn switching_away_from_native_drains_its_timers_then_retires_it() {
    let root = tempdir().unwrap();
    let due_ms = current_millis() as i64 + 2 * HOUR_MS;
    native_store_holding(root.path(), vec![native(timer_message(due_ms, "n0"))]).await;

    let mut store = open(root.path(), TimerStoreMode::JavaCompat)
        .await
        .expect("another mode drains the native timers");
    let native_timer = store.native_timer().cloned().expect("the native engine drains");
    native_timer.set_clock(clock_at(due_ms + 1_000));
    native_timer.set_delivering(true);
    assert_eq!(native_timer.process_once().await, 1);
    store.reput_once().await;
    assert_eq!(delivered_keys(&store), keys(&["n0"]));
    drop(native_timer);
    close(store).await;

    // Drained and retired: another mode no longer runs it.
    let store = open(root.path(), TimerStoreMode::JavaCompat).await.unwrap();
    assert!(store.native_timer().is_none());
    close(store).await;

    // Native again, it starts after the records already there and is drained again later.
    let mut store = open(root.path(), TimerStoreMode::Native).await.unwrap();
    let native_timer = store.native_timer().cloned().unwrap();
    native_timer.set_delivering(true);
    assert_eq!(native_timer.process_once().await, 0);
    put_all(&mut store, vec![native(timer_message(due_ms, "n1"))]).await;
    assert_eq!(native_timer.process_once().await, 0);
    drop(native_timer);
    close(store).await;
    let store = open(root.path(), TimerStoreMode::JavaCompat).await.unwrap();
    assert!(store.native_timer().is_some(), "an engine holding timers drains again");
    close(store).await;
}

#[tokio::test]
async fn a_draining_native_engine_holding_timers_is_not_retired() {
    let root = tempdir().unwrap();
    let due_ms = current_millis() as i64 + 2 * HOUR_MS;
    native_store_holding(root.path(), vec![native(timer_message(due_ms, "n0"))]).await;

    for _ in 0..2 {
        let store = open(root.path(), TimerStoreMode::JavaCompat).await.unwrap();
        let native_timer = store.native_timer().cloned().expect("the native engine drains");
        native_timer.set_delivering(true);
        assert_eq!(native_timer.process_once().await, 0);
        drop(native_timer);
        close(store).await;
    }
}

#[tokio::test]
async fn a_java_recall_cancels_a_timer_the_native_engine_drains() {
    let root = tempdir().unwrap();
    let due_ms = current_millis() as i64 + 2 * HOUR_MS;
    native_store_holding(
        root.path(),
        vec![native(timer_message(due_ms, "n0")), native(timer_message(due_ms, "n1"))],
    )
    .await;

    let mut store = open(root.path(), TimerStoreMode::JavaCompat).await.unwrap();
    let native_timer = store.native_timer().cloned().unwrap();
    put_all(&mut store, vec![java_recall_of("n0", due_ms)]).await;
    native_timer.set_clock(clock_at(due_ms + 1_000));
    native_timer.set_delivering(true);
    assert_eq!(native_timer.process_once().await, 2);
    store.reput_once().await;
    assert_eq!(delivered_keys(&store), keys(&["n1"]));
    drop(native_timer);
    close(store).await;
}

#[tokio::test]
async fn a_native_recall_cancels_a_timer_the_java_engine_admitted() {
    let root = tempdir().unwrap();
    let mut store = open(root.path(), TimerStoreMode::Native)
        .await
        .expect("open a native store");
    let timer_store = store.get_timer_message_store().cloned().unwrap();
    let now_ms = current_millis() as i64;
    let due_ms = now_ms + 60_000;
    put_all(
        &mut store,
        vec![
            java(timer_message(due_ms, "j0")),
            java(timer_message(due_ms, "j1")),
            recall_of("j0", due_ms),
        ],
    )
    .await;

    timer_store.set_clock(clock_at(due_ms + 5_000));
    timer_store.set_should_running_dequeue(true);
    process_until_idle(&timer_store).await;
    store.reput_once().await;
    assert_eq!(delivered_keys(&store), keys(&["j1"]));
    close(store).await;
}

#[tokio::test]
async fn a_native_engine_that_never_ran_starts_after_the_records_already_there() {
    let root = tempdir().unwrap();
    let mut store = open(root.path(), TimerStoreMode::JavaCompat).await.unwrap();
    let due_ms = current_millis() as i64 + 60_000;
    put_all(
        &mut store,
        vec![java(timer_message(due_ms, "j0")), java_recall_of("j0", due_ms)],
    )
    .await;
    close(store).await;

    let store = open(root.path(), TimerStoreMode::Native).await.unwrap();
    let native_timer = store.native_timer().cloned().unwrap();
    assert_eq!(native_timer.process_once().await, 0);
    // The recall the Java-compatible engine took before the switch is not the native engine's.
    assert_eq!(native_timer.engine().counts().recalls, 0);
    drop(native_timer);
    close(store).await;
}

#[tokio::test]
async fn a_slave_follows_its_master_and_a_promotion_writes_only_what_the_master_did_not() {
    let root = tempdir().unwrap();
    let mut store = open(root.path(), TimerStoreMode::Native)
        .await
        .expect("open a native store");
    let native_timer = store.native_timer().cloned().unwrap();
    native_timer.set_delivering(false);
    let due_ms = current_millis() as i64 - 1_000;
    put_all(
        &mut store,
        vec![
            native(timer_message(due_ms, "n0")),
            native(timer_message(due_ms, "n1")),
            native(timer_message(due_ms, "n2")),
        ],
    )
    .await;
    // The master wrote n0 and n2; its write of n1 failed and stays.
    put_all(&mut store, vec![delivered_by_master("n0"), delivered_by_master("n2")]).await;
    assert_eq!(native_timer.process_once().await, 0);

    // Promoted, the slave writes n1, and not n2, which its old master wrote.
    native_timer.set_delivering(true);
    assert_eq!(native_timer.process_once().await, 2);
    store.reput_once().await;
    assert_eq!(delivered_in_order(&store), ["n0", "n2", "n1"]);
    drop(native_timer);
    close(store).await;
}

#[tokio::test]
async fn a_slave_keeps_the_delivery_it_followed_across_a_restart() {
    let root = tempdir().unwrap();
    let mut store = open(root.path(), TimerStoreMode::Native)
        .await
        .expect("open a native store");
    let native_timer = store.native_timer().cloned().unwrap();
    native_timer.set_delivering(false);
    let due_ms = current_millis() as i64 - 1_000;
    put_all(
        &mut store,
        vec![
            native(timer_message(due_ms, "n0")),
            native(timer_message(due_ms, "n1")),
            delivered_by_master("n0"),
        ],
    )
    .await;
    assert_eq!(native_timer.process_once().await, 0);
    drop(native_timer);
    close(store).await;

    let mut store = open(root.path(), TimerStoreMode::Native)
        .await
        .expect("reopen the native store");
    let native_timer = store.native_timer().cloned().unwrap();
    native_timer.set_delivering(true);
    assert_eq!(native_timer.process_once().await, 1);
    store.reput_once().await;
    assert_eq!(delivered_in_order(&store), ["n0", "n1"]);
    drop(native_timer);
    close(store).await;
}
