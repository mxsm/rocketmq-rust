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

#![cfg(feature = "rocksdb_store")]

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
use rocketmq_common::common::boundary_type::BoundaryType;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_store::base::dispatch_request::DispatchRequest;
use rocketmq_store::base::message_status_enum::GetMessageStatus;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::base::store_enum::StoreType;
use rocketmq_store::config::flush_disk_type::FlushDiskType;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::message_store::rocksdb_message_store::RocksDBMessageStore;
use rocketmq_store::message_store::OwnedMessageStore;
use rocketmq_store::store_error::StoreErrorKind;
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

fn rocksdb_store_config_with_maintenance(temp_dir: &TempDir) -> MessageStoreConfig {
    MessageStoreConfig {
        mem_table_flush_interval_ms: 10,
        ..rocksdb_store_config(temp_dir)
    }
}

fn new_owned_test_store(temp_dir: &TempDir) -> RocksDBMessageStore {
    let broker_config = Arc::new(BrokerConfig::default());
    let topic_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>> = Arc::new(DashMap::new());

    RocksDBMessageStore::try_new(
        Arc::new(rocksdb_store_config(temp_dir)),
        broker_config,
        topic_table,
        None,
        false,
    )
    .expect("create RocksDB message store")
}

fn new_owned_test_store_with_config(config: MessageStoreConfig) -> RocksDBMessageStore {
    let broker_config = Arc::new(BrokerConfig::default());
    let topic_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>> = Arc::new(DashMap::new());

    RocksDBMessageStore::try_new(Arc::new(config), broker_config, topic_table, None, false)
        .expect("create RocksDB message store")
}

fn new_test_store(store: RocksDBMessageStore) -> OwnedMessageStore {
    OwnedMessageStore::rocksdb(store)
}

fn build_test_message(topic: &CheetahString, queue_id: i32, body: &'static [u8]) -> MessageExtBrokerInner {
    let mut msg = MessageExtBrokerInner::default();
    msg.set_topic(topic.clone());
    msg.message_ext_inner.set_queue_id(queue_id);
    msg.set_body(Bytes::from_static(body));
    msg
}

async fn assert_trait_reads_rocksdb_cq<MS: MessageStore>(
    store: &MS,
    group: &CheetahString,
    topic: &CheetahString,
    wrote_offset: i64,
) {
    let get_result = store
        .get_message(group, topic, 0, 0, 32, None)
        .await
        .expect("trait get message result");
    assert_eq!(get_result.status(), Some(GetMessageStatus::Found));
    assert_eq!(get_result.message_count(), 1);
    assert_eq!(store.get_max_offset_in_queue(topic, 0), 1);
    assert_eq!(store.get_min_offset_in_queue(topic, 0), 0);
    assert_eq!(store.get_commit_log_offset_in_queue(topic, 0, 0), wrote_offset);
    assert!(store.get_message_store_timestamp(topic, 0, 0) > 0);
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

    let mut writer = new_owned_test_store(&temp_dir);
    writer.init().await.expect("init writer");
    assert!(writer.load().await, "load writer");
    writer.start().await.expect("start writer");

    let put_result = writer
        .put_message(build_test_message(&topic, 0, b"rocksdb-round-trip-body"))
        .await;
    assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);
    let append_result = put_result.append_message_result().expect("append result");
    let wrote_offset = append_result.wrote_offset;

    writer.reput_once().await;
    writer.shutdown().await;
    drop(writer);

    let mut reloaded = new_owned_test_store(&temp_dir);
    reloaded.init().await.expect("init reloaded store");
    assert!(reloaded.load().await, "load reloaded store");
    reloaded.start().await.expect("start reloaded store");

    let metrics_before_pull = reloaded.rocksdb_store().metrics();
    let get_result = reloaded
        .get_message(&group, &topic, 0, 0, 32, None)
        .await
        .expect("get message result");
    let metrics_after_pull = reloaded.rocksdb_store().metrics();
    assert_eq!(metrics_after_pull.read_count - metrics_before_pull.read_count, 2);
    assert_eq!(metrics_after_pull.scan_count - metrics_before_pull.scan_count, 1);
    assert_eq!(get_result.status(), Some(GetMessageStatus::Found));
    assert_eq!(get_result.message_count(), 1);
    assert_eq!(reloaded.get_max_offset_in_queue(&topic, 0), 1);
    assert_eq!(reloaded.get_min_offset_in_queue(&topic, 0), 0);
    assert_eq!(reloaded.get_commit_log_offset_in_queue(&topic, 0, 0), wrote_offset);
    assert!(
        reloaded.get_message_store_timestamp(&topic, 0, 0) > 0,
        "RocksDB CQ timestamp should be recovered"
    );
    assert_trait_reads_rocksdb_cq(&reloaded, &group, &topic, wrote_offset).await;

    let overflow_result = reloaded
        .get_message(&group, &topic, 0, 1, 32, None)
        .await
        .expect("overflow get message result");
    assert_eq!(overflow_result.status(), Some(GetMessageStatus::OffsetOverflowOne));
    assert_eq!(overflow_result.next_begin_offset(), 1);

    let generic_store = new_test_store(reloaded);
    assert_trait_reads_rocksdb_cq(&generic_store, &group, &topic, wrote_offset).await;
}

#[tokio::test]
async fn rocksdb_message_store_start_and_shutdown_manage_rocksdb_maintenance_services() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let mut store = new_owned_test_store_with_config(rocksdb_store_config_with_maintenance(&temp_dir));
    store.init().await.expect("init store");
    assert!(store.load().await, "load store");

    store.start().await.expect("start store");
    assert!(store.is_rocksdb_maintenance_running());
    assert!(store.is_message_rocksdb_maintenance_running());

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
    while store.rocksdb_store().metrics().flush_count == 0
        || store.message_rocksdb_storage().store().metrics().flush_count == 0
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "RocksDB maintenance services did not flush both stores before deadline"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    store.shutdown().await;
    assert!(!store.is_rocksdb_maintenance_running());
    assert!(!store.is_message_rocksdb_maintenance_running());
    assert!(
        store
            .consume_queue_store()
            .put_message_position(&[DispatchRequest {
                topic: CheetahString::from_static_str("closed-maintenance-topic"),
                queue_id: 0,
                consume_queue_offset: 0,
                commit_log_offset: 0,
                msg_size: 1,
                tags_code: 0,
                store_timestamp: 1,
                success: true,
                ..DispatchRequest::default()
            }])
            .is_err(),
        "shutdown should close the RocksDB consume queue store"
    );
}

#[tokio::test]
async fn rocksdb_query_message_after_dispatch() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let topic = CheetahString::from_static_str("rocksdb-query-topic");
    let key = CheetahString::from_static_str("rocksdb-query-key");

    let mut store = new_owned_test_store(&temp_dir);
    assert_eq!(
        store.get_dispatcher_list().len(),
        2,
        "default Rocks mode owns CQ and Index dispatch only"
    );
    store.init().await.expect("init store");
    assert!(store.load().await, "load store");
    store.start().await.expect("start store");

    let mut msg = build_test_message(&topic, 0, b"rocksdb-query-body");
    msg.set_keys(key.clone());

    let put_result = store.put_message(msg).await;
    assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);
    let append_result = put_result.append_message_result().expect("append result");

    store.reput_once().await;
    assert_eq!(
        store.rocksdb_index_service().pending_len(),
        0,
        "reput must drain the RocksDB index batch"
    );

    let indexed_offsets = store
        .message_rocksdb_storage()
        .query_offsets_for_index(
            topic.as_str(),
            rocketmq_common::common::message::MessageConst::INDEX_KEY_TYPE,
            key.as_str(),
            append_result.store_timestamp,
            append_result.store_timestamp,
            10,
        )
        .expect("query RocksDB index directly after reput");
    assert_eq!(indexed_offsets.len(), 1, "reput must flush the RocksDB index batch");

    let result = store
        .query_message(&topic, &key, 10, 0, i64::MAX)
        .await
        .expect("query message result");

    assert_eq!(result.message_maped_list.len(), 1);
    assert!(result.buffer_total_size > 0);
    assert!(result.index_last_update_phyoffset >= 0);
}

#[tokio::test]
async fn rocksdb_query_message_uses_rocksdb_index_without_local_file_index_dispatch() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let topic = CheetahString::from_static_str("rocksdb-query-rocks-index-only-topic");
    let key = CheetahString::from_static_str("rocksdb-query-rocks-index-only-key");
    let uniq_key = CheetahString::from_static_str("rocksdb-query-rocks-index-only-uniq");

    let mut store = new_owned_test_store(&temp_dir);
    store.init().await.expect("init store");
    assert!(store.load().await, "load store");
    store.start().await.expect("start store");

    let mut msg = build_test_message(&topic, 0, b"rocksdb-query-rocks-index-only-body");
    msg.set_keys(key.clone());
    let put_result = store.put_message(msg).await;
    assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);
    let append_result = put_result.append_message_result().expect("append result");

    store
        .rocksdb_index_service()
        .build_index(&DispatchRequest {
            topic: topic.clone(),
            queue_id: 0,
            commit_log_offset: append_result.wrote_offset,
            msg_size: append_result.wrote_bytes,
            store_timestamp: append_result.store_timestamp,
            keys: key.clone(),
            uniq_key: Some(uniq_key),
            success: true,
            ..DispatchRequest::default()
        })
        .expect("manual rocksdb index build should enqueue");
    store
        .rocksdb_index_service()
        .flush_pending()
        .expect("manual rocksdb index build should flush");

    let result = store
        .query_message(
            &topic,
            &key,
            10,
            append_result.store_timestamp,
            append_result.store_timestamp,
        )
        .await
        .expect("rocksdb index query message result");

    assert_eq!(result.message_maped_list.len(), 1);
    assert!(result.buffer_total_size > 0);
    assert_eq!(result.index_last_update_phyoffset, append_result.wrote_offset);
    assert_eq!(result.index_last_update_timestamp, append_result.store_timestamp);
}

#[tokio::test]
async fn rocksdb_recovery_skips_dirty_tail() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let topic = CheetahString::from_static_str("rocksdb-recovery-topic");

    let mut writer = new_owned_test_store(&temp_dir);
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

    let mut reloaded = new_owned_test_store(&temp_dir);
    reloaded.init().await.expect("init reloaded store");
    assert!(reloaded.load().await, "load reloaded store");

    assert_eq!(reloaded.get_max_phy_offset(), valid_end);
    assert!(reloaded.get_commit_log_data(valid_end).is_none());
    assert_eq!(reloaded.get_max_offset_in_queue(&topic, 0), 1);
    assert_eq!(
        reloaded.get_commit_log_offset_in_queue(&topic, 0, 0),
        append_result.wrote_offset
    );
}

#[tokio::test]
async fn rocks_adapter_matches_the_frozen_local_pull_contract() {
    let rocks_dir = TempDir::new().expect("create RocksDB temp dir");
    let topic = CheetahString::from_static_str("adapter-parity-topic");
    let group = CheetahString::from_static_str("adapter-parity-group");
    let mut rocks = new_owned_test_store(&rocks_dir);

    rocks.init().await.expect("init Rocks parity store");
    assert!(rocks.load().await, "load Rocks parity store");
    rocks.start().await.expect("start Rocks parity store");
    let rocks_put = rocks
        .put_message(build_test_message(&topic, 0, b"adapter-parity-body"))
        .await;
    assert_eq!(rocks_put.put_message_status(), PutMessageStatus::PutOk);
    rocks.reput_once().await;

    let rocks_found = rocks
        .get_message(&group, &topic, 0, 0, 32, None)
        .await
        .expect("Rocks found result");
    assert_eq!(rocks_found.status(), Some(GetMessageStatus::Found));
    assert_eq!(rocks_found.message_count(), 1);
    assert_eq!(rocks.get_min_offset_in_queue(&topic, 0), 0);
    assert_eq!(rocks.get_max_offset_in_queue(&topic, 0), 1);

    let rocks_overflow = rocks
        .get_message(&group, &topic, 0, 1, 32, None)
        .await
        .expect("Rocks overflow result");
    assert_eq!(rocks_overflow.status(), Some(GetMessageStatus::OffsetOverflowOne));
    assert_eq!(rocks_overflow.next_begin_offset(), 1);
}

#[tokio::test]
async fn explicit_double_write_keeps_the_local_compatibility_mirror() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let topic = CheetahString::from_static_str("rocksdb-double-write-topic");
    let mut config = rocksdb_store_config(&temp_dir);
    config.rocksdb_cq_double_write_enable = true;
    let mut store = new_owned_test_store_with_config(config);
    store.init().await.expect("init store");
    assert!(store.load().await, "load store");
    store.start().await.expect("start store");

    let put_result = store
        .put_message(build_test_message(&topic, 0, b"explicit-double-write"))
        .await;
    assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);
    store.reput_once().await;

    assert_eq!(store.get_dispatcher_list().len(), 4);
    assert!(store.local_file_store().get_consume_queue(&topic, 0).is_some());
    assert_eq!(store.get_max_offset_in_queue(&topic, 0), 1);
}

#[tokio::test]
async fn restart_reput_advances_the_single_local_wal_queue_offset() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let topic = CheetahString::from_static_str("rocksdb-restart-catchup-topic");
    let mut writer = new_owned_test_store(&temp_dir);
    writer.init().await.expect("init writer");
    assert!(writer.load().await, "load writer");

    let first = writer
        .put_message(build_test_message(&topic, 0, b"first-before-reput"))
        .await;
    assert_eq!(first.put_message_status(), PutMessageStatus::PutOk);
    assert_eq!(
        first
            .append_message_result()
            .expect("first append result")
            .logics_offset,
        0
    );
    writer.flush();
    writer.close_rocksdb();
    drop(writer);

    let mut reloaded = new_owned_test_store(&temp_dir);
    reloaded.init().await.expect("init reloaded store");
    assert!(reloaded.load().await, "load reloaded store");
    reloaded.reput_once().await;
    assert_eq!(reloaded.get_max_offset_in_queue(&topic, 0), 1);

    let second = reloaded
        .put_message(build_test_message(&topic, 0, b"second-after-reput"))
        .await;
    assert_eq!(second.put_message_status(), PutMessageStatus::PutOk);
    assert_eq!(
        second
            .append_message_result()
            .expect("second append result")
            .logics_offset,
        1,
        "RocksDB catch-up must advance the Local WAL queue-offset allocator"
    );
}

#[test]
fn rocksdb_time_lookup_and_failure_mapping_stay_on_the_legacy_contract() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let topic = CheetahString::from_static_str("rocksdb-time-topic");
    let mut store = new_owned_test_store(&temp_dir);
    for (queue_offset, store_timestamp) in [(0, 1_000), (1, 2_000), (2, 3_000)] {
        store.local_file_store_mut().do_dispatch(&mut DispatchRequest {
            topic: topic.clone(),
            queue_id: 0,
            commit_log_offset: queue_offset * 100,
            msg_size: 10,
            consume_queue_offset: queue_offset,
            store_timestamp,
            success: true,
            ..DispatchRequest::default()
        });
    }

    assert_eq!(store.get_offset_in_queue_by_time(&topic, 0, 2_500), 2);
    assert_eq!(
        store.get_offset_in_queue_by_time_with_boundary(&topic, 0, 2_500, BoundaryType::Upper),
        1
    );

    store.close_rocksdb();
    let error = store
        .try_get_max_offset_in_queue(&topic, 0)
        .expect_err("closed RocksDB must expose a typed error");
    assert_eq!(error.kind(), StoreErrorKind::RocksDb);
    assert_eq!(store.get_max_offset_in_queue(&topic, 0), 0);
    assert_eq!(store.get_commit_log_offset_in_queue(&topic, 0, 0), -1);
    let flush_error = store.try_flush().expect_err("closed RocksDB flush must fail");
    assert_eq!(flush_error.kind(), StoreErrorKind::RocksDb);
    assert_eq!(
        store
            .health_snapshot()
            .last_flush_error
            .expect("flush failure must be reflected in health")
            .kind,
        StoreErrorKind::RocksDb
    );
}
