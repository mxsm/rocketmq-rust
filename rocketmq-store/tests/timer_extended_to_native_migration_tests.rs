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
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
use rocketmq_protocol::common::message::message_decoder::message_properties_to_string;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_store::migrate_extended_timers_to_native;
use rocketmq_store::BrokerAdminStore;
use rocketmq_store::BrokerReadStore;
use rocketmq_store::BrokerStorePort;
use rocketmq_store::BrokerWriteStore;
use rocketmq_store::ExtendedTimerMigrationBlocker;
use rocketmq_store::ExtendedTimerMigrationMode;
use rocketmq_store::LocalFileMessageStore;
use rocketmq_store::MessageStoreConfig;
use rocketmq_store::StoreRuntimeConfig;
use rocketmq_store::TIMER_OUT_MS;
use rocketmq_store::TIMER_TOPIC;
use rocketmq_store_api::TimerEngineId;
use rocketmq_store_api::TimerStoreMode;
use rocketmq_store_api::EXTENDED_TIMELINE_FORMAT_VERSION;
use tempfile::TempDir;

const REAL_TOPIC: &str = "extended-migration-real-topic";

fn service_context() -> ChildServiceContext {
    static OWNER: OnceLock<RuntimeOwner> = OnceLock::new();
    OWNER
        .get_or_init(|| {
            RuntimeOwner::plan(RuntimeConfig::server_default("timer-extended-migration"))
                .expect("test runtime configuration is valid")
                .build()
                .expect("timer migration test runtime")
        })
        .root_context()
        .component("timer-extended-migration-store")
}

/// A store of `mode` under `root`; the CommitLog and queue sizes match across modes.
fn store_config(root: &TempDir, mode: TimerStoreMode) -> MessageStoreConfig {
    let mut config = MessageStoreConfig {
        store_path_root_dir: root.path().to_string_lossy().into_owned().into(),
        read_uncommitted: true,
        timer_wheel_enable: mode == TimerStoreMode::Native,
        timer_store_mode: mode,
        // The Extended settings go with the Extended mode, as an operator removes them.
        timer_extended_shadow_enable: false,
        timer_extended_admission_enable: mode == TimerStoreMode::ExtendedTimeline,
        timer_extended_activation_epoch: if mode == TimerStoreMode::ExtendedTimeline { 7 } else { 0 },
        timer_extended_admission_horizon_days: 3,
        // The Extended delivery contract runs with duplication; Reput then stops at the confirm
        // offset, which nothing here moves for the native engine's writes.
        duplication_enable: mode == TimerStoreMode::ExtendedTimeline,
        mapped_file_size_commit_log: 4096,
        mapped_file_size_consume_queue: 200,
        ..MessageStoreConfig::default()
    };
    config.timer_store_config.scheduler_interval_ms = 10;
    config.timer_store_config.materialize_batch_messages = 8;
    config.timer_store_config.due_scan_messages = 8;
    config.timer_store_config.minimum_free_bytes = 1;
    config.timer_store_config.minimum_free_ratio_basis_points = 1;
    config
}

async fn open(config: MessageStoreConfig) -> LocalFileMessageStore {
    let topics = Arc::new(DashMap::new());
    for topic in [TIMER_TOPIC, REAL_TOPIC] {
        topics.insert(CheetahString::from_static_str(topic), Arc::new(TopicConfig::default()));
    }
    let mut store = LocalFileMessageStore::new(
        Arc::new(config),
        rocketmq_store_local::commit_log::append::micro_batch::MicroBatchPolicy::disabled(1)
            .expect("valid test policy"),
        Arc::new(StoreRuntimeConfig::default()),
        topics,
        None,
        false,
        service_context(),
    )
    .expect("create a migration test store")
    .expect("test store configuration is valid");
    store.wire_owned_root_dependencies().expect("wire the timer engines");
    store.init().await.expect("initialize the store");
    assert!(store.load().await, "load the store");
    store.start().await.expect("start the store");
    store
}

fn token(unique_key: &str) -> String {
    format!("R:1:{unique_key}:0")
}

/// A timer the Broker admitted to the Extended Timeline.
fn extended_timer(deliver_at_ms: i64, unique_key: &str) -> MessageExtBrokerInner {
    let mut message = MessageExtBrokerInner::default();
    message.set_topic(CheetahString::from_static_str(TIMER_TOPIC));
    message.message_ext_inner.queue_id = 0;
    message.set_body(Bytes::from_static(b"migrated-payload"));
    for (key, value) in [
        (MessageConst::PROPERTY_REAL_TOPIC, REAL_TOPIC.to_owned()),
        (MessageConst::PROPERTY_REAL_QUEUE_ID, "0".to_owned()),
        (TIMER_OUT_MS, deliver_at_ms.to_string()),
        (
            MessageConst::PROPERTY_TIMER_ORIGINAL_DELIVER_MS,
            deliver_at_ms.to_string(),
        ),
        (
            MessageConst::TIMER_ENGINE_TYPE,
            TimerEngineId::ExtendedTimeline.as_str().to_owned(),
        ),
        (
            MessageConst::PROPERTY_TIMER_FORMAT_VERSION,
            EXTENDED_TIMELINE_FORMAT_VERSION.to_string(),
        ),
        (MessageConst::PROPERTY_TIMER_POLICY_FINGERPRINT, "7".to_owned()),
        (MessageConst::PROPERTY_TIMER_GENERATION, "0".to_owned()),
        (MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN, token(unique_key)),
        (
            MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            unique_key.to_owned(),
        ),
    ] {
        message.put_property(CheetahString::from_static_str(key), CheetahString::from_string(value));
    }
    message.properties_string = message_properties_to_string(message.get_properties());
    message
}

fn delivered_count(store: &LocalFileMessageStore) -> i64 {
    store.get_max_offset_in_queue(&CheetahString::from_static_str(REAL_TOPIC), 0)
}

/// Runs Reput until `done` holds, or panics after `timeout`.
async fn wait_for(
    store: &mut LocalFileMessageStore,
    timeout: Duration,
    what: &str,
    done: impl Fn(&LocalFileMessageStore) -> bool,
) {
    let deadline = Instant::now() + timeout;
    loop {
        store.reput_once().await;
        if done(store) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{what}; native={:?}",
            store.get_runtime_info().get("timerNativeMetrics")
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn materialized(store: &LocalFileMessageStore) -> u64 {
    store
        .get_runtime_info()
        .get("timerExtendedMaterializedRecords")
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

/// The tokens and store times of the messages in the real topic, in queue order.
async fn delivered(store: &LocalFileMessageStore) -> Vec<(String, i64)> {
    let result = store
        .get_message(
            &CheetahString::from_static_str("migration-group"),
            &CheetahString::from_static_str(REAL_TOPIC),
            0,
            0,
            32,
            None,
        )
        .await
        .expect("real-topic messages");
    result
        .message_mapped_list()
        .iter()
        .map(|mapped| {
            let mut bytes = mapped.get_bytes().expect("message bytes");
            let message = MessageDecoder::decode(&mut bytes, true, false, false, false, false).expect("decode");
            let property = |key: &'static str| {
                message
                    .property(&CheetahString::from_static_str(key))
                    .map(|value| value.to_string())
                    .unwrap_or_default()
            };
            (
                property(MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN),
                message.store_timestamp(),
            )
        })
        .collect()
}

#[tokio::test]
async fn pending_extended_timers_move_to_the_native_engine_and_are_delivered_once() {
    let root = TempDir::new().expect("temporary store root");
    let mut store = open(store_config(&root, TimerStoreMode::ExtendedTimeline)).await;
    let now_ms = current_millis() as i64;
    let late_ms = now_ms + 5_000;
    for timer in [
        extended_timer(now_ms + 300, "early"),
        extended_timer(late_ms, "late-1"),
        extended_timer(late_ms, "late-2"),
    ] {
        assert!(store.put_message(timer).await.is_ok());
    }
    wait_for(
        &mut store,
        Duration::from_secs(10),
        "the early timer is delivered",
        |store| delivered_count(store) == 1 && materialized(store) >= 3,
    )
    .await;
    store.shutdown().await;
    // The RocksDB lock goes with the store, as it goes with a stopped Broker.
    drop(store);

    let config = store_config(&root, TimerStoreMode::Native);
    let survey =
        migrate_extended_timers_to_native(&config, ExtendedTimerMigrationMode::DryRun).expect("survey the timers");
    assert!(survey.dry_run && survey.blockers.is_empty(), "{survey:?}");
    assert_eq!(survey.migrated, 2);
    assert!(root.path().join("timer-extended").exists(), "a dry run changes nothing");

    let report = match migrate_extended_timers_to_native(&config, ExtendedTimerMigrationMode::Apply) {
        Ok(report) => report,
        Err(error) => panic!(
            "migrate the timers: {error:?} source={:?}",
            std::error::Error::source(&error).map(|source| source.to_string())
        ),
    };
    assert_eq!(
        (report.migrated, report.finished + report.committed),
        (2, 1),
        "{report:?}"
    );
    let backup = report.backup.expect("the Extended Timeline went aside");
    assert!(root.path().join(&backup).exists());
    assert!(!root.path().join("timer-extended").exists());
    assert!(!root.path().join("config/timer-store-owner.meta").exists());
    assert!(migrate_extended_timers_to_native(&config, ExtendedTimerMigrationMode::Apply).is_err());

    let mut store = open(config).await;
    let native: std::collections::HashMap<String, i64> = serde_json::from_str(
        store
            .get_runtime_info()
            .get("timerNativeMetrics")
            .expect("the native engine runs"),
    )
    .expect("native metrics");
    assert!(native["payloadDays"] >= 1, "the migrated timers are copies: {native:?}");
    wait_for(
        &mut store,
        Duration::from_secs(15),
        "the migrated timers are delivered",
        |store| delivered_count(store) == 3,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    store.reput_once().await;
    assert_eq!(delivered_count(&store), 3, "each timer is delivered once");
    let messages = delivered(&store).await;
    assert_eq!(messages[0].0, token("early"));
    let mut late: Vec<_> = messages[1..].to_vec();
    late.sort();
    assert_eq!(
        late.iter().map(|(token, _)| token.clone()).collect::<Vec<_>>(),
        [token("late-1"), token("late-2")]
    );
    assert!(late.iter().all(|(_, stored_ms)| *stored_ms >= late_ms), "{late:?}");
    store.shutdown().await;
}

#[tokio::test]
async fn a_timer_later_than_the_native_engine_admits_keeps_the_store_as_it_is() {
    let root = TempDir::new().expect("temporary store root");
    let mut store = open(store_config(&root, TimerStoreMode::ExtendedTimeline)).await;
    let due_ms = current_millis() as i64 + 2 * 86_400_000;
    assert!(store.put_message(extended_timer(due_ms, "far")).await.is_ok());
    wait_for(
        &mut store,
        Duration::from_secs(10),
        "the timer is materialized",
        |store| materialized(store) >= 1,
    )
    .await;
    store.shutdown().await;
    // The RocksDB lock goes with the store, as it goes with a stopped Broker.
    drop(store);

    let mut config = store_config(&root, TimerStoreMode::Native);
    config.timer_native.max_delay_days = 1;
    let report =
        migrate_extended_timers_to_native(&config, ExtendedTimerMigrationMode::Apply).expect("survey the timers");

    assert_eq!(
        report.blockers,
        [ExtendedTimerMigrationBlocker::BeyondNativeMaxDelay {
            timers: 1,
            required_days: 2,
        }]
    );
    // Operators read the printed report.
    assert_eq!(
        serde_json::to_value(&report).expect("serialize the report")["blockers"],
        serde_json::json!([{"kind": "beyondNativeMaxDelay", "timers": 1, "requiredDays": 2}])
    );
    assert!(report.backup.is_none());
    assert!(root.path().join("timer-extended").exists());
    assert!(root.path().join("config/timer-store-owner.meta").exists());
    assert!(!root.path().join("timer-native").exists());
}

/// A stopped store whose Extended Timeline holds one timer due in a minute.
async fn extended_store_with_one_timer(root: &TempDir) {
    let mut store = open(store_config(root, TimerStoreMode::ExtendedTimeline)).await;
    assert!(store
        .put_message(extended_timer(current_millis() as i64 + 60_000, "pending"))
        .await
        .is_ok());
    wait_for(
        &mut store,
        Duration::from_secs(10),
        "the timer is materialized",
        |store| materialized(store) >= 1,
    )
    .await;
    store.shutdown().await;
    drop(store);
}

#[tokio::test]
async fn a_migration_run_again_replaces_what_an_unfinished_one_wrote() {
    let root = TempDir::new().expect("temporary store root");
    extended_store_with_one_timer(&root).await;
    let config = store_config(&root, TimerStoreMode::Native);
    std::fs::create_dir_all(root.path().join("timer-native").join("left-behind")).unwrap();

    assert!(
        migrate_extended_timers_to_native(&config, ExtendedTimerMigrationMode::Apply).is_err(),
        "native files that no migration left are not overwritten"
    );

    std::fs::write(
        root.path().join("config/timer-extended-migration.meta"),
        "timer-extended.migrated-1",
    )
    .unwrap();
    let report = migrate_extended_timers_to_native(&config, ExtendedTimerMigrationMode::Apply).expect("migrate again");

    assert_eq!(report.migrated, 1);
    assert!(!root.path().join("timer-native").join("left-behind").exists());
    assert!(!root.path().join("config/timer-extended-migration.meta").exists());
}
