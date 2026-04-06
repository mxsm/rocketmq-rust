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

use std::fs::OpenOptions;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_status_enum::GetMessageStatus;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::base::store_enum::StoreType;
use rocketmq_store::config::flush_disk_type::FlushDiskType;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::message_store::rocksdb_message_store::RocksDBMessageStore;
use tempfile::TempDir;

fn rocksdb_store_config(temp_dir: &TempDir) -> MessageStoreConfig {
    MessageStoreConfig {
        store_path_root_dir: temp_dir.path().to_string_lossy().to_string().into(),
        store_type: StoreType::RocksDB,
        flush_disk_type: FlushDiskType::AsyncFlush,
        mapped_file_size_commit_log: 4096,
        mapped_file_size_consume_queue: 200,
        ha_listen_port: 0,
        ..MessageStoreConfig::default()
    }
}

fn new_test_store(temp_dir: &TempDir) -> ArcMut<RocksDBMessageStore> {
    let broker_config = Arc::new(BrokerConfig::default());
    let topic_table: Arc<DashMap<CheetahString, ArcMut<TopicConfig>>> = Arc::new(DashMap::new());

    let mut store = ArcMut::new(RocksDBMessageStore::new(
        Arc::new(rocksdb_store_config(temp_dir)),
        broker_config,
        topic_table,
        None,
        false,
    ));
    let store_clone = store.clone();
    store.set_message_store_arc(store_clone);
    store
}

fn build_test_message(topic: &CheetahString, queue_id: i32, body: &'static [u8]) -> MessageExtBrokerInner {
    let mut msg = MessageExtBrokerInner::default();
    msg.set_topic(topic.clone());
    msg.message_ext_inner.set_queue_id(queue_id);
    msg.set_body(Bytes::from_static(body));
    msg
}

fn first_commitlog_file(root: &Path) -> PathBuf {
    PathBuf::from(root).join("commitlog").join("00000000000000000000")
}

fn corrupt_commitlog_tail(commitlog_file: &Path, offset: i64, payload: &[u8]) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(commitlog_file)
        .expect("open commitlog file");
    file.seek(SeekFrom::Start(offset as u64)).expect("seek commitlog tail");
    file.write_all(payload).expect("write dirty tail");
    file.sync_data().expect("sync dirty tail");
}

#[tokio::test]
async fn rocksdb_store_load_start_recover_round_trip() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let topic = CheetahString::from_static_str("rocksdb-round-trip-topic");
    let group = CheetahString::from_static_str("rocksdb-round-trip-group");

    let mut writer = new_test_store(&temp_dir);
    writer.init().await.expect("init writer");
    assert!(writer.load().await, "load writer");
    writer.start().await.expect("start writer");

    let put_result = writer
        .put_message(build_test_message(&topic, 0, b"rocksdb-round-trip-body"))
        .await;
    assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);

    writer.reput_once().await;
    writer.shutdown().await;
    drop(writer);

    let mut reloaded = new_test_store(&temp_dir);
    reloaded.init().await.expect("init reloaded store");
    assert!(reloaded.load().await, "load reloaded store");
    reloaded.start().await.expect("start reloaded store");

    let get_result = reloaded
        .get_message(&group, &topic, 0, 0, 32, None)
        .await
        .expect("get message result");
    assert_eq!(get_result.status(), Some(GetMessageStatus::Found));
    assert_eq!(get_result.message_count(), 1);
    assert_eq!(reloaded.get_max_offset_in_queue(&topic, 0), 1);
}

#[tokio::test]
async fn rocksdb_query_message_after_dispatch() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let topic = CheetahString::from_static_str("rocksdb-query-topic");
    let key = CheetahString::from_static_str("rocksdb-query-key");

    let mut store = new_test_store(&temp_dir);
    store.init().await.expect("init store");
    assert!(store.load().await, "load store");
    store.start().await.expect("start store");

    let mut msg = build_test_message(&topic, 0, b"rocksdb-query-body");
    msg.set_keys(key.clone());

    let put_result = store.put_message(msg).await;
    assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);

    store.reput_once().await;

    let result = store
        .query_message(&topic, &key, 10, 0, i64::MAX)
        .await
        .expect("query message result");

    assert_eq!(result.message_maped_list.len(), 1);
    assert!(result.buffer_total_size > 0);
    assert!(result.index_last_update_phyoffset >= 0);
}

#[tokio::test]
async fn rocksdb_recovery_skips_dirty_tail() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let topic = CheetahString::from_static_str("rocksdb-recovery-topic");

    let mut writer = new_test_store(&temp_dir);
    writer.init().await.expect("init writer");
    assert!(writer.load().await, "load writer");
    writer.start().await.expect("start writer");

    let put_result = writer
        .put_message(build_test_message(&topic, 0, b"rocksdb-recovery-body"))
        .await;
    assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);

    let append_result = put_result.append_message_result().expect("append result");
    let valid_end = append_result.wrote_offset + append_result.wrote_bytes as i64;

    writer.reput_once().await;
    writer.shutdown().await;
    drop(writer);

    let commitlog_file = first_commitlog_file(temp_dir.path());
    corrupt_commitlog_tail(&commitlog_file, valid_end, &[0x13, 0x37, 0xC0, 0xDE]);

    let mut reloaded = new_test_store(&temp_dir);
    reloaded.init().await.expect("init reloaded store");
    assert!(reloaded.load().await, "load reloaded store");

    assert_eq!(reloaded.get_max_phy_offset(), valid_end);
    assert!(reloaded.get_commit_log_data(valid_end).is_none());
    assert_eq!(reloaded.get_max_offset_in_queue(&topic, 0), 1);
}
