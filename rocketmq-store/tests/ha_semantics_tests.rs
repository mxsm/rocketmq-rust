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

//! Integration tests for store-facing HA semantics.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::config::flush_disk_type::FlushDiskType;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::message_store::local_file_message_store::LocalFileMessageStore;
use tempfile::TempDir;

fn new_test_store(message_store_config: MessageStoreConfig) -> ArcMut<LocalFileMessageStore> {
    let broker_config = Arc::new(BrokerConfig::default());
    let topic_table: Arc<DashMap<CheetahString, ArcMut<TopicConfig>>> = Arc::new(DashMap::new());

    let mut store = ArcMut::new(LocalFileMessageStore::new(
        Arc::new(message_store_config),
        broker_config,
        topic_table,
        None,
        false,
    ));
    let store_clone = store.clone();
    store.set_message_store_arc(store_clone);
    store
}

fn ha_test_config(temp_dir: &TempDir) -> MessageStoreConfig {
    MessageStoreConfig {
        store_path_root_dir: temp_dir.path().to_string_lossy().to_string().into(),
        broker_role: BrokerRole::SyncMaster,
        flush_disk_type: FlushDiskType::SyncFlush,
        mapped_file_size_commit_log: 4096,
        mapped_file_size_consume_queue: 200,
        in_sync_replicas: 2,
        min_in_sync_replicas: 1,
        slave_timeout: 50,
        ha_listen_port: 0,
        enable_controller_mode: false,
        ..MessageStoreConfig::default()
    }
}

fn build_message(topic: &CheetahString, body: &'static [u8]) -> MessageExtBrokerInner {
    let mut msg = MessageExtBrokerInner::default();
    msg.set_topic(topic.clone());
    msg.message_ext_inner.set_queue_id(0);
    msg.set_body(Bytes::from_static(body));
    msg
}

#[tokio::test]
async fn sync_master_without_slave_ack_returns_flush_slave_timeout() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let topic = CheetahString::from_static_str("phase6-ha-flush-slave-timeout-topic");
    let mut store = new_test_store(ha_test_config(&temp_dir));

    store.init().await.expect("init store");
    assert!(store.load().await, "load store");
    store.start().await.expect("start store");
    tokio::time::sleep(Duration::from_millis(20)).await;

    let result = store.put_message(build_message(&topic, b"phase6-ha-body")).await;

    assert_eq!(result.put_message_status(), PutMessageStatus::FlushSlaveTimeout);

    store.shutdown().await;
}

#[tokio::test]
async fn wait_store_msg_ok_false_skips_ha_wait_and_returns_put_ok() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let topic = CheetahString::from_static_str("phase6-ha-skip-topic");
    let mut store = new_test_store(ha_test_config(&temp_dir));

    store.init().await.expect("init store");
    assert!(store.load().await, "load store");
    store.start().await.expect("start store");
    tokio::time::sleep(Duration::from_millis(20)).await;

    let mut msg = build_message(&topic, b"phase6-ha-skip-body");
    msg.set_wait_store_msg_ok(false);

    let result = store.put_message(msg).await;

    assert_eq!(result.put_message_status(), PutMessageStatus::PutOk);

    store.shutdown().await;
}
