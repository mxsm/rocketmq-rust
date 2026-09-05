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
use rocketmq_protocol::common::message::message_decoder::message_properties_to_string;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_store::BrokerReadStore;
use rocketmq_store::BrokerStorePort;
use rocketmq_store::BrokerWriteStore;
use rocketmq_store::LocalFileMessageStore;
use rocketmq_store::MessageStoreConfig;
use rocketmq_store::StoreRuntimeConfig;
use rocketmq_store::TIMER_OUT_MS;
use rocketmq_store::TIMER_TOPIC;
use rocketmq_store_api::PersistedTimerRoute;
use rocketmq_store_api::StoreOperation;
use rocketmq_store_api::TimerEngineEpoch;
use rocketmq_store_api::TimerEngineId;
use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::TimerPayloadStoreLocator;
use rocketmq_store_api::TimerSourceCqOffset;
use rocketmq_store_api::EXTENDED_TIMELINE_FORMAT_VERSION;
use rocketmq_store_local::timer::payload_store::TimerPayloadStore;
use rocketmq_store_local::timer::payload_store::TimerPayloadStoreConfig;
use rocketmq_store_rocksdb::batch::RocksDbWriteBatch;
use rocketmq_store_rocksdb::store::KeyValueStore;
use rocketmq_store_rocksdb::timer::checkpoint::TimelineCheckpointKind;
use rocketmq_store_rocksdb::timer::checkpoint::TimelineCheckpointV1;
use rocketmq_store_rocksdb::timer::codec::encode_ready_key;
use rocketmq_store_rocksdb::timer::codec::TimelineKeyV1;
use rocketmq_store_rocksdb::timer::codec::TimelineRecordV1;
use rocketmq_store_rocksdb::timer::state_index::RocksDbTimelineStateIndex;
use rocketmq_store_rocksdb::timer::state_index::StateTransitionResult;
use rocketmq_store_rocksdb::timer::state_index::TimelineState;
use rocketmq_store_rocksdb::timer::state_index::TimelineStateRecordV1;
use rocketmq_store_rocksdb::timer::timeline_index::RocksDbTimelineIndex;
use rocketmq_store_rocksdb::timer::timeline_index::TimelineIndexEntry;
use rocketmq_store_rocksdb::timer::LATE_READY_CF;
use rocketmq_store_rocksdb::timer::READY_CF;
use tempfile::TempDir;

const REAL_TOPIC: &str = "extended-shadow-real-topic";

fn service_context() -> ChildServiceContext {
    static OWNER: OnceLock<RuntimeOwner> = OnceLock::new();
    OWNER
        .get_or_init(|| {
            RuntimeOwner::plan(RuntimeConfig::server_default("timer-extended-integration"))
                .expect("test runtime configuration is valid")
                .build()
                .expect("timer extended test runtime")
        })
        .root_context()
        .component("timer-extended-store")
}

fn new_store(root: &TempDir) -> LocalFileMessageStore {
    let mut config = MessageStoreConfig {
        store_path_root_dir: root.path().to_string_lossy().into_owned().into(),
        read_uncommitted: true,
        timer_wheel_enable: false,
        timer_extended_shadow_enable: true,
        duplication_enable: true,
        mapped_file_size_commit_log: 4096,
        mapped_file_size_consume_queue: 200,
        ..MessageStoreConfig::default()
    };
    config.timer_store_config.scheduler_interval_ms = 10;
    config.timer_store_config.materialize_batch_messages = 4;

    let topics = Arc::new(DashMap::new());
    topics.insert(
        CheetahString::from_static_str(TIMER_TOPIC),
        Arc::new(TopicConfig::default()),
    );
    topics.insert(
        CheetahString::from_static_str(REAL_TOPIC),
        Arc::new(TopicConfig::default()),
    );
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
    .expect("create Extended Timeline integration Store")
    .expect("test Timer Store configuration is valid");
    store.wire_owned_root_dependencies().expect("wire timeline shadow");
    store
}

fn timer_message(deliver_at_ms: i64) -> MessageExtBrokerInner {
    let mut message = MessageExtBrokerInner::default();
    message.set_topic(CheetahString::from_static_str(TIMER_TOPIC));
    message.message_ext_inner.queue_id = 0;
    message.set_body(Bytes::from_static(b"one-year-payload"));
    message.put_property(
        CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC),
        CheetahString::from_static_str(REAL_TOPIC),
    );
    message.put_property(
        CheetahString::from_static_str(MessageConst::PROPERTY_REAL_QUEUE_ID),
        CheetahString::from_static_str("3"),
    );
    message.put_property(
        CheetahString::from_static_str(TIMER_OUT_MS),
        CheetahString::from_string(deliver_at_ms.to_string()),
    );
    message.put_property(
        CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_GENERATION),
        CheetahString::from_static_str("7"),
    );
    message.properties_string = message_properties_to_string(message.get_properties());
    message
}

fn invalid_timer_message(deliver_at_ms: i64) -> MessageExtBrokerInner {
    let mut message = MessageExtBrokerInner::default();
    message.set_topic(CheetahString::from_static_str(TIMER_TOPIC));
    message.message_ext_inner.queue_id = 0;
    message.set_body(Bytes::from_static(b"invalid-timer-source"));
    message.put_property(
        CheetahString::from_static_str(TIMER_OUT_MS),
        CheetahString::from_string(deliver_at_ms.to_string()),
    );
    message.properties_string = message_properties_to_string(message.get_properties());
    message
}

async fn wait_for_materialized(store: &LocalFileMessageStore) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let runtime = store.get_runtime_info();
        if runtime
            .get("timerExtendedMaterializationLag")
            .is_some_and(|lag| lag == "0")
            && runtime
                .get("timerExtendedMaterializedRecords")
                .is_some_and(|records| records == "1")
        {
            assert_eq!(
                runtime.get("timerExtendedShadowDifferences").map(String::as_str),
                Some("0")
            );
            return;
        }
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("Extended Timeline did not materialize within the test deadline");
}

async fn wait_for_zero_lag(store: &LocalFileMessageStore) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if store
            .get_runtime_info()
            .get("timerExtendedMaterializationLag")
            .is_some_and(|lag| lag == "0")
        {
            return;
        }
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("Extended Timeline replay did not close its source lag");
}

#[tokio::test]
async fn materializer_payload_first_shadow_restart_keeps_one_year_payload_idempotent() {
    let root = TempDir::new().expect("tempdir");
    let deliver_at_ms = 1_800_000_000_000i64;
    let mut store = new_store(&root);
    store.init().await.expect("init");
    assert!(store.load().await, "load");
    store.start().await.expect("start");
    assert!(store.put_message(timer_message(deliver_at_ms)).await.is_ok());
    store.reput_once().await;
    wait_for_materialized(&store).await;
    store.shutdown().await;
    drop(store);

    {
        let timeline = RocksDbTimelineIndex::open(root.path())
            .expect("reopen Timeline")
            .expect("valid Timeline configuration");
        let checkpoint = timeline
            .checkpoint(TimelineCheckpointKind::MaterializedSource, 0)
            .expect("checkpoint read")
            .expect("materialized checkpoint");
        assert_eq!(checkpoint.materialized_source_offset.get(), 0);
        let page = timeline
            .range_scan_shadow(deliver_at_ms - 1, deliver_at_ms + 1, None, 10, 16 * 1024)
            .expect("shadow scan");
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].key.generation, TimerGeneration::new(7));
        assert!(timeline
            .store()
            .get_cf(StoreOperation::Read, READY_CF, &encode_ready_key(page.entries[0].key),)
            .expect("ready lookup")
            .is_none());

        let payloads = TimerPayloadStore::new(TimerPayloadStoreConfig::for_store_root(root.path()))
            .expect("payload store")
            .expect("valid payload configuration");
        payloads.load().expect("payload recovery");
        let payload = payloads.read(page.entries[0].record.payload).expect("durable payload");
        assert_eq!(payload.due_time_ms, deliver_at_ms);
        assert_eq!(payload.real_topic, REAL_TOPIC);
        assert!(!payload.frame.is_empty());
        assert_eq!(payloads.metrics().record_count, 1);

        timeline
            .put_batch(
                &[],
                Some((
                    TimelineCheckpointKind::MaterializedSource,
                    0,
                    TimelineCheckpointV1 {
                        materialized_source_offset: rocketmq_store_api::TimerSourceCqOffset::new(-1),
                        generation: checkpoint.generation.saturating_add(1),
                        ..checkpoint
                    },
                )),
            )
            .expect("simulate lagging source checkpoint");
        timeline.close();
    }

    let mut replay = new_store(&root);
    replay.init().await.expect("restart init");
    assert!(replay.load().await, "restart load");
    replay.start().await.expect("restart start");
    wait_for_zero_lag(&replay).await;
    replay.shutdown().await;
    drop(replay);

    let payloads = TimerPayloadStore::new(TimerPayloadStoreConfig::for_store_root(root.path()))
        .expect("payload store")
        .expect("valid payload configuration");
    payloads.load().expect("payload recovery");
    assert_eq!(
        payloads.metrics().record_count,
        1,
        "idempotent replay must not duplicate payload"
    );
}

#[tokio::test]
async fn materializer_gap_keeps_checkpoint_and_cleanup_fence_at_first_failed_source() {
    let root = TempDir::new().expect("tempdir");
    let mut store = new_store(&root);
    store.init().await.expect("init");
    assert!(store.load().await, "load");
    store.start().await.expect("start");
    assert!(store
        .put_message(invalid_timer_message(1_800_000_000_000))
        .await
        .is_ok());
    assert!(store.put_message(timer_message(1_800_000_100_000)).await.is_ok());
    store.reput_once().await;

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let runtime = store.get_runtime_info();
        if runtime
            .get("timerExtendedMaterializationFailures")
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|failures| failures > 0)
        {
            assert_eq!(
                runtime.get("timerExtendedMaterializationLag").map(String::as_str),
                Some("2")
            );
            assert_eq!(
                runtime.get("timerExtendedMaterializedRecords").map(String::as_str),
                Some("0")
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "materializer did not report the invalid source"
        );
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    store.shutdown().await;
    drop(store);

    let timeline = RocksDbTimelineIndex::open(root.path())
        .expect("Timeline")
        .expect("valid Timeline configuration");
    assert!(timeline
        .checkpoint(TimelineCheckpointKind::MaterializedSource, 0)
        .expect("checkpoint")
        .is_none());
    let payloads = TimerPayloadStore::new(TimerPayloadStoreConfig::for_store_root(root.path()))
        .expect("payload store")
        .expect("valid payload configuration");
    payloads.load().expect("payload recovery");
    assert_eq!(payloads.metrics().record_count, 0);
}

fn formal_state(generation: TimerGeneration, state: TimelineState) -> TimelineStateRecordV1 {
    TimelineStateRecordV1 {
        state,
        state_version: 0,
        route: PersistedTimerRoute::try_new(
            TimerEngineId::ExtendedTimeline,
            EXTENDED_TIMELINE_FORMAT_VERSION,
            1,
            generation,
            format!("delivery-token-{}", generation.get()),
        )
        .expect("route"),
        admission_epoch: TimerEngineEpoch::new(5),
        owner_epoch: TimerEngineEpoch::new(5),
        claim_seq: 0,
        due_time_ms: 1_800_000_000_000,
        lane: 0,
        terminal_at_ms: 0,
        shadow_only: false,
    }
}

#[test]
fn late_ready_due_scanner_ready_outbox_transition_is_crash_safe() {
    let root = TempDir::new().expect("tempdir");
    let timeline = RocksDbTimelineIndex::open(root.path())
        .expect("Timeline")
        .expect("valid Timeline configuration");
    let generation = TimerGeneration::new(3);
    let key = TimelineKeyV1 {
        due_time_ms: 8_000,
        lane: 4,
        timer_id: TimerId::new(99),
        generation,
    };
    let entry = TimelineIndexEntry {
        key,
        record: TimelineRecordV1 {
            payload: TimerPayloadStoreLocator::try_new(0, 4, 0, 0, 10, 1).expect("locator"),
            source_cq_offset: TimerSourceCqOffset::new(10),
            source_physical_offset: 100,
            source_size: 10,
            state_version: 0,
            owner_engine: TimerEngineId::ExtendedTimeline,
            shadow_only: false,
        },
    };
    let mut materialize = RocksDbWriteBatch::with_capacity(3);
    RocksDbTimelineIndex::append_entry(&mut materialize, &entry).expect("entry");
    RocksDbTimelineStateIndex::append_state(
        &mut materialize,
        key.timer_id,
        generation,
        &formal_state(generation, TimelineState::Pending),
    )
    .expect("state");
    materialize.put_cf(LATE_READY_CF, encode_ready_key(key), 0u64.to_be_bytes());
    timeline.write_batch(&materialize).expect("materialize");

    let states = RocksDbTimelineStateIndex::new(timeline.store());
    let mut promote = RocksDbWriteBatch::with_capacity(2);
    promote.put_cf(READY_CF, encode_ready_key(key), 1u64.to_be_bytes());
    promote.delete_cf(LATE_READY_CF, encode_ready_key(key));
    assert!(matches!(
        states
            .compare_and_set(
                key.timer_id,
                generation,
                TimelineState::Pending,
                0,
                TimelineState::Ready,
                promote,
            )
            .expect("promote"),
        StateTransitionResult::Applied(_)
    ));
    assert!(timeline
        .store()
        .get_cf(StoreOperation::Read, READY_CF, &encode_ready_key(key))
        .expect("ready")
        .is_some());
    assert!(timeline
        .store()
        .get_cf(StoreOperation::Read, LATE_READY_CF, &encode_ready_key(key))
        .expect("late ready")
        .is_none());
    assert_eq!(
        states
            .get(key.timer_id, generation)
            .expect("state")
            .expect("record")
            .state,
        TimelineState::Ready
    );
}

#[test]
fn recall_generation_state_keys_fence_old_work() {
    let root = TempDir::new().expect("tempdir");
    let timeline = RocksDbTimelineIndex::open(root.path())
        .expect("Timeline")
        .expect("valid Timeline configuration");
    let timer_id = TimerId::new(123);
    let old = TimerGeneration::new(1);
    let active = TimerGeneration::new(2);
    let mut batch = RocksDbWriteBatch::with_capacity(2);
    RocksDbTimelineStateIndex::append_state(&mut batch, timer_id, old, &formal_state(old, TimelineState::Ready))
        .expect("old state");
    RocksDbTimelineStateIndex::append_state(
        &mut batch,
        timer_id,
        active,
        &formal_state(active, TimelineState::Pending),
    )
    .expect("active state");
    timeline.write_batch(&batch).expect("states");
    let states = RocksDbTimelineStateIndex::new(timeline.store());
    assert!(matches!(
        states
            .compare_and_set(
                timer_id,
                active,
                TimelineState::Pending,
                0,
                TimelineState::Cancelled,
                RocksDbWriteBatch::default(),
            )
            .expect("cancel active"),
        StateTransitionResult::Applied(_)
    ));
    assert_eq!(
        states.get(timer_id, old).expect("old").expect("state").state,
        TimelineState::Ready
    );
    assert_eq!(
        states.get(timer_id, active).expect("active").expect("state").state,
        TimelineState::Cancelled
    );
}
