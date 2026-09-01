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
use std::sync::OnceLock;
use std::time::Duration;

use bytes::Bytes;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_model::common::attribute::cleanup_policy::CleanupPolicy;
use rocketmq_model::common::attribute::Attribute;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_model::TopicAttributes::TopicAttributes;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_store::BrokerReadStore;
use rocketmq_store::BrokerStorePort;
use rocketmq_store::BrokerWriteStore;
use rocketmq_store::FlushDiskType;
use rocketmq_store::GetMessageStatus;
use rocketmq_store::LocalFileMessageStore;
use rocketmq_store::MessageStoreConfig;
use rocketmq_store::PutMessageStatus;
use rocketmq_store::StoreRuntimeConfig;

fn service_context() -> rocketmq_runtime::ChildServiceContext {
    static OWNER: OnceLock<RuntimeOwner> = OnceLock::new();
    OWNER
        .get_or_init(|| {
            RuntimeOwner::new(RuntimeConfig::server_default("compaction-lifecycle-tests"))
                .expect("compaction lifecycle runtime")
        })
        .root_context()
        .component("compaction-store")
}

fn compaction_topic(name: &str) -> TopicConfig {
    let mut config = TopicConfig::new(name);
    config.attributes.insert(
        TopicAttributes::cleanup_policy_attribute().name().clone(),
        CleanupPolicy::COMPACTION.to_string().into(),
    );
    config
}

fn delete_topic(name: &str) -> TopicConfig {
    TopicConfig::new(name)
}

fn message(topic: &CheetahString, key: Option<&str>, body: &'static [u8]) -> MessageExtBrokerInner {
    let mut message = MessageExtBrokerInner::default();
    message.set_topic(topic.clone());
    message.message_ext_inner.set_queue_id(0);
    message.set_body(Bytes::from_static(body));
    if let Some(key) = key {
        message.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_KEYS),
            CheetahString::from_string(key.to_owned()),
        );
    }
    message
}

fn new_store(root: &std::path::Path, topics: Arc<DashMap<CheetahString, Arc<TopicConfig>>>) -> LocalFileMessageStore {
    let config = MessageStoreConfig {
        store_path_root_dir: root.to_string_lossy().to_string().into(),
        enable_compaction: true,
        compaction_schedule_internal: 1,
        flush_disk_type: FlushDiskType::AsyncFlush,
        mapped_file_size_commit_log: 1024 * 1024,
        mapped_file_size_consume_queue: 20 * 1024,
        ha_listen_port: 0,
        timer_wheel_enable: false,
        ..MessageStoreConfig::default()
    };
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
    .expect("create compaction lifecycle Store")
    .expect("test Timer Store configuration is valid");
    store
        .wire_owned_root_dependencies()
        .expect("wire compaction test store dependencies");
    store
}

async fn start(store: &mut LocalFileMessageStore) {
    store.init().await.expect("initialize compaction test store");
    assert!(store.load().await, "load compaction test store");
    store.start().await.expect("start compaction test store");
}

async fn wait_for_status(
    store: &LocalFileMessageStore,
    group: &CheetahString,
    topic: &CheetahString,
    offset: i64,
    expected: GetMessageStatus,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if store
            .get_message(group, topic, 0, offset, 32, None)
            .await
            .is_some_and(|result| result.status() == Some(expected))
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {expected:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn tombstone_is_durable_and_cleanup_policy_selects_exactly_one_read_path() {
    let temp = tempfile::tempdir().expect("compaction temp directory");
    let topic = CheetahString::from_static_str("CompactionLifecycleTopic");
    let group = CheetahString::from_static_str("CompactionLifecycleGroup");
    let topics = Arc::new(DashMap::new());
    topics.insert(topic.clone(), Arc::new(compaction_topic(topic.as_str())));

    let mut store = new_store(temp.path(), topics.clone());
    start(&mut store).await;
    for body in [b"old".as_slice(), b"new".as_slice()] {
        let result = store.put_message(message(&topic, Some("a b"), body)).await;
        assert_eq!(result.put_message_status(), PutMessageStatus::PutOk);
    }
    wait_for_status(&store, &group, &topic, 1, GetMessageStatus::Found).await;

    topics.insert(topic.clone(), Arc::new(delete_topic(topic.as_str())));
    wait_for_status(&store, &group, &topic, 0, GetMessageStatus::Found).await;
    topics.insert(topic.clone(), Arc::new(compaction_topic(topic.as_str())));

    let tombstone = store.put_message(message(&topic, Some("a b"), b"")).await;
    assert_eq!(tombstone.put_message_status(), PutMessageStatus::PutOk);
    wait_for_status(&store, &group, &topic, 0, GetMessageStatus::NoMatchedLogicQueue).await;
    let current = temp.path().join("compaction").join("CURRENT");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while !current.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "compaction generation was not published"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    store.shutdown().await;
    drop(store);

    let mut restarted = new_store(temp.path(), topics);
    start(&mut restarted).await;
    wait_for_status(&restarted, &group, &topic, 0, GetMessageStatus::NoMatchedLogicQueue).await;
    restarted.shutdown().await;
}

#[tokio::test]
async fn missing_key_keeps_main_commit_log_ack_but_has_no_compaction_projection() {
    let temp = tempfile::tempdir().expect("compaction temp directory");
    let topic = CheetahString::from_static_str("CompactionMissingKeyTopic");
    let group = CheetahString::from_static_str("CompactionMissingKeyGroup");
    let topics = Arc::new(DashMap::new());
    topics.insert(topic.clone(), Arc::new(compaction_topic(topic.as_str())));
    let mut store = new_store(temp.path(), topics);
    start(&mut store).await;

    let result = store.put_message(message(&topic, None, b"main-log-value")).await;
    assert_eq!(result.put_message_status(), PutMessageStatus::PutOk);
    wait_for_status(&store, &group, &topic, 0, GetMessageStatus::NoMatchedLogicQueue).await;
    assert!(
        store.get_max_phy_offset() > 0,
        "main CommitLog append must remain acknowledged"
    );
    store.shutdown().await;
}
