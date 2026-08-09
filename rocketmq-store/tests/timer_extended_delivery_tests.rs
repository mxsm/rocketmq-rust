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
use rocketmq_model::common::broker::broker_role::BrokerRole;
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
use rocketmq_store::BrokerReadStore;
use rocketmq_store::BrokerReplicationStore;
use rocketmq_store::BrokerStorePort;
use rocketmq_store::BrokerWriteStore;
use rocketmq_store::LocalFileMessageStore;
use rocketmq_store::MessageStoreConfig;
use rocketmq_store::StoreRuntimeConfig;
use rocketmq_store::TIMER_OUT_MS;
use rocketmq_store::TIMER_TOPIC;
use rocketmq_store_api::TimerEngineId;
use rocketmq_store_api::TimerStoreMode;
use rocketmq_store_api::EXTENDED_TIMELINE_FORMAT_VERSION;
use tempfile::TempDir;

const REAL_TOPIC: &str = "extended-delivery-real-topic";
const DELIVERY_TOKEN: &str = "R:1:delivery-contract:0";

fn service_context() -> ChildServiceContext {
    static OWNER: OnceLock<RuntimeOwner> = OnceLock::new();
    OWNER
        .get_or_init(|| {
            RuntimeOwner::new(RuntimeConfig::server_default("timer-extended-delivery"))
                .expect("timer delivery test runtime")
        })
        .root_context()
        .component("timer-extended-delivery-store")
}

fn new_formal_store(root: &TempDir) -> LocalFileMessageStore {
    let mut config = MessageStoreConfig {
        store_path_root_dir: root.path().to_string_lossy().into_owned().into(),
        read_uncommitted: true,
        timer_wheel_enable: false,
        timer_store_mode: TimerStoreMode::ExtendedTimeline,
        timer_extended_shadow_enable: false,
        timer_extended_admission_enable: true,
        timer_extended_activation_epoch: 7,
        timer_extended_admission_horizon_days: 3,
        duplication_enable: true,
        mapped_file_size_commit_log: 4096,
        mapped_file_size_consume_queue: 200,
        ..MessageStoreConfig::default()
    };
    config.timer_store_config.scheduler_interval_ms = 10;
    config.timer_store_config.materialize_batch_messages = 8;
    config.timer_store_config.due_scan_messages = 8;
    config.timer_store_config.minimum_free_bytes = 1;
    config.timer_store_config.minimum_free_ratio_basis_points = 1;

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
        Arc::new(StoreRuntimeConfig::default()),
        topics,
        None,
        false,
        service_context(),
    );
    store.wire_owned_root_dependencies().expect("wire formal timeline");
    store
}

fn canonical_timer(deliver_at_ms: i64) -> MessageExtBrokerInner {
    let mut message = MessageExtBrokerInner::default();
    message.set_topic(CheetahString::from_static_str(TIMER_TOPIC));
    message.message_ext_inner.queue_id = 0;
    message.set_body(Bytes::from_static(b"formal-long-horizon-payload"));
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
        (MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN, DELIVERY_TOKEN.to_owned()),
    ] {
        message.put_property(CheetahString::from_static_str(key), CheetahString::from_string(value));
    }
    message.properties_string = message_properties_to_string(message.get_properties());
    message
}

#[tokio::test]
async fn timer_delivery_is_not_early_and_preserves_the_admission_token() {
    let root = TempDir::new().expect("temporary store root");
    let mut store = new_formal_store(&root);
    store.init().await.expect("initialize formal store");
    assert!(store.load().await, "load formal store");
    store.start().await.expect("start formal store");

    let deliver_at_ms = current_millis() as i64 + 1_500;
    assert!(store.put_message(canonical_timer(deliver_at_ms)).await.is_ok());
    store.reput_once().await;

    tokio::time::sleep(Duration::from_millis(200)).await;
    store.reput_once().await;
    assert_eq!(
        store.get_max_offset_in_queue(&CheetahString::from_static_str(REAL_TOPIC), 0),
        0,
        "formal delivery must not publish before the absolute deadline"
    );

    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        store.reput_once().await;
        if store.get_max_offset_in_queue(&CheetahString::from_static_str(REAL_TOPIC), 0) == 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "formal timer was not delivered; runtime={:?}",
            store.get_runtime_info()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let result = store
        .get_message(
            &CheetahString::from_static_str("delivery-contract-group"),
            &CheetahString::from_static_str(REAL_TOPIC),
            0,
            0,
            1,
            None,
        )
        .await
        .expect("final real-topic message");
    assert_eq!(result.message_count(), 1);
    let mut bytes = result.message_mapped_list()[0].get_bytes().expect("final frame bytes");
    let delivered = MessageDecoder::decode(&mut bytes, true, false, false, false, false).expect("decode final message");
    assert_eq!(delivered.topic(), REAL_TOPIC);
    assert_eq!(
        delivered.body(),
        Some(Bytes::from_static(b"formal-long-horizon-payload"))
    );
    assert_eq!(
        delivered.property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN,
        )),
        Some(CheetahString::from_static_str(DELIVERY_TOKEN))
    );
    let dequeue_time = delivered
        .property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DEQUEUE_MS))
        .and_then(|value| value.parse::<i64>().ok())
        .expect("final dequeue timestamp");
    assert!(dequeue_time >= deliver_at_ms);

    let snapshot = store
        .create_extended_timer_snapshot()
        .expect("create consistent Extended snapshot");
    snapshot.validate().expect("validate Extended snapshot manifest");
    let installed_generation = store
        .get_runtime_info()
        .get("timerExtendedInstalledSnapshotGeneration")
        .and_then(|value| value.parse::<u64>().ok())
        .expect("installed snapshot generation metric");
    assert_eq!(installed_generation, snapshot.generation);
    store
        .release_extended_timer_snapshot(&snapshot)
        .expect("release replicated snapshot pin");

    store
        .sync_broker_role_with_term(BrokerRole::Slave, 10)
        .expect("persist demotion fence");
    assert_eq!(
        store
            .get_runtime_info()
            .get("timerExtendedAdmissionActive")
            .map(String::as_str),
        Some("false")
    );
    store
        .sync_broker_role_with_term(BrokerRole::SyncMaster, 11)
        .expect("promote a snapshot-backed caught-up member");
    let promoted = store.get_runtime_info();
    assert_eq!(
        promoted.get("timerExtendedAdmissionActive").map(String::as_str),
        Some("true")
    );
    assert!(promoted
        .get("timerExtendedRoleEpoch")
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|epoch| epoch >= 11));

    store.shutdown().await;
}

#[tokio::test]
async fn canary_horizon_rejects_a_message_supported_by_the_physical_format() {
    let root = TempDir::new().expect("temporary store root");
    let mut store = new_formal_store(&root);
    store.init().await.expect("initialize formal store");
    assert!(store.load().await, "load formal store");
    store.start().await.expect("start formal store");

    let four_days_ms = 4 * 86_400_000;
    let result = store
        .put_message(canonical_timer(current_millis() as i64 + four_days_ms))
        .await;
    assert!(!result.is_ok(), "the 3-day canary horizon must bound new admissions");

    store.shutdown().await;
}
