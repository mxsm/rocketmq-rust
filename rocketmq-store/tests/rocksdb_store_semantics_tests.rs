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
use std::sync::OnceLock;

use bytes::Bytes;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_model::common::boundary_type::BoundaryType;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_model::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_store::BrokerAdminStore;
use rocketmq_store::BrokerReadStore;
use rocketmq_store::BrokerStorePort;
use rocketmq_store::BrokerWriteStore;
use rocketmq_store::DispatchRequest;
use rocketmq_store::FlushDiskType;
use rocketmq_store::GetMessageStatus;
use rocketmq_store::MessageIndexRuntimeSnapshot;
use rocketmq_store::MessageIndexRuntimeSource;
use rocketmq_store::MessageStoreConfig;
use rocketmq_store::PutMessageStatus;
use rocketmq_store::QueryMessageRequest;
use rocketmq_store::RocksDBMessageStore;
use rocketmq_store::RocksDbIndexBuildConfig;
use rocketmq_store::RocksDbIndexBuildService;
use rocketmq_store::StoreComponent;
use rocketmq_store::StoreOperation;
use rocketmq_store::StorePorts;
use rocketmq_store::StoreRuntimeConfig;
use rocketmq_store::StoreType;
use tempfile::TempDir;

struct ReenabledIncompleteIndex;

impl MessageIndexRuntimeSource for ReenabledIncompleteIndex {
    fn snapshot(&self) -> MessageIndexRuntimeSnapshot {
        MessageIndexRuntimeSnapshot {
            enabled: true,
            incomplete: true,
        }
    }

    fn with_dispatch_admission(&self, dispatch: &mut dyn FnMut(bool)) {
        dispatch(true);
    }
}

struct DisabledCompleteIndex;

impl MessageIndexRuntimeSource for DisabledCompleteIndex {
    fn snapshot(&self) -> MessageIndexRuntimeSnapshot {
        MessageIndexRuntimeSnapshot {
            enabled: false,
            incomplete: false,
        }
    }

    fn with_dispatch_admission(&self, dispatch: &mut dyn FnMut(bool)) {
        dispatch(false);
    }
}

struct EnabledCompleteIndex;

impl MessageIndexRuntimeSource for EnabledCompleteIndex {
    fn snapshot(&self) -> MessageIndexRuntimeSnapshot {
        MessageIndexRuntimeSnapshot {
            enabled: true,
            incomplete: false,
        }
    }

    fn with_dispatch_admission(&self, dispatch: &mut dyn FnMut(bool)) {
        dispatch(true);
    }
}

fn rocksdb_service_context(name: &'static str) -> rocketmq_runtime::ChildServiceContext {
    static OWNER: OnceLock<rocketmq_runtime::RuntimeOwner> = OnceLock::new();
    OWNER
        .get_or_init(|| {
            rocketmq_runtime::RuntimeOwner::new(rocketmq_runtime::RuntimeConfig::default())
                .expect("RocksDB semantics-test runtime owner should start")
        })
        .root_context()
        .component(name)
}

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
    let broker_config = Arc::new(StoreRuntimeConfig::default());
    let topic_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>> = Arc::new(DashMap::new());

    RocksDBMessageStore::try_new(
        Arc::new(rocksdb_store_config(temp_dir)),
        rocketmq_store_local::commit_log::append::micro_batch::MicroBatchPolicy::disabled(1)
            .expect("valid test policy"),
        broker_config,
        topic_table,
        None,
        false,
        rocksdb_service_context("rocksdb-semantics-test-store"),
    )
    .expect("create RocksDB message store")
    .expect("test Timer Store configuration is valid")
}

fn new_owned_test_store_with_config(config: MessageStoreConfig) -> RocksDBMessageStore {
    let broker_config = Arc::new(StoreRuntimeConfig::default());
    let topic_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>> = Arc::new(DashMap::new());

    RocksDBMessageStore::try_new(
        Arc::new(config),
        rocketmq_store_local::commit_log::append::micro_batch::MicroBatchPolicy::disabled(1)
            .expect("valid test policy"),
        broker_config,
        topic_table,
        None,
        false,
        rocksdb_service_context("rocksdb-semantics-configured-test-store"),
    )
    .expect("create RocksDB message store")
    .expect("test Timer Store configuration is valid")
}

fn new_test_store(store: RocksDBMessageStore) -> StorePorts {
    StorePorts::rocksdb(store)
}

fn build_test_message(topic: &CheetahString, queue_id: i32, body: &'static [u8]) -> MessageExtBrokerInner {
    let mut msg = MessageExtBrokerInner::default();
    msg.set_topic(topic.clone());
    msg.message_ext_inner.set_queue_id(queue_id);
    msg.set_body(Bytes::from_static(body));
    msg
}

async fn assert_trait_reads_rocksdb_cq<MS: BrokerReadStore>(
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
    let runtime_info = store.get_runtime_info();
    assert_eq!(runtime_info["storeType"], "rocksdb");
    assert_eq!(runtime_info["rocksdbMaintenanceSupported"], "true");
    assert_eq!(runtime_info["rocksdbMaintenanceRunning"], "true");
    assert_eq!(runtime_info["messageRocksdbMaintenanceRunning"], "true");

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
    let runtime_info = store.get_runtime_info();
    assert_eq!(runtime_info["rocksdbMaintenanceRunning"], "false");
    assert_eq!(runtime_info["messageRocksdbMaintenanceRunning"], "false");
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
            rocketmq_model::common::message::MessageConst::INDEX_KEY_TYPE,
            key.as_str(),
            append_result.store_timestamp,
            append_result.store_timestamp,
            10,
            None,
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
    assert_eq!(
        store.message_index_safe_offset(),
        Some(0),
        "an available empty RocksDB index has a legitimate zero safe offset"
    );

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
            uniq_key: Some(uniq_key.clone()),
            success: true,
            ..DispatchRequest::default()
        })
        .expect("manual rocksdb index build should enqueue");
    store
        .rocksdb_index_service()
        .flush_pending()
        .expect("manual rocksdb index build should flush");
    assert_eq!(
        store.message_index_safe_offset(),
        Some(append_result.wrote_offset + i64::from(append_result.wrote_bytes)),
        "RocksDB must durably expose the exclusive indexed CommitLog offset"
    );

    assert!(store.install_message_index_runtime(Arc::new(ReenabledIncompleteIndex)));

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
    assert!(
        !result.index_query_safe,
        "a non-empty RocksDB result must retain the disabled-period safety gap"
    );

    let store_time_hour = append_result.store_timestamp - append_result.store_timestamp % 3_600_000;
    let exhausted_cursor = CheetahString::from_string(format!(
        "{store_time_hour}@{topic}@K@{key}@{uniq_key}@{}",
        append_result.wrote_offset
    ));
    let cursor_result = store
        .query_message_with_options(&QueryMessageRequest {
            topic: topic.clone(),
            key: key.clone(),
            index_type: Some(CheetahString::from_static_str("K")),
            max_num: 10,
            begin: append_result.store_timestamp,
            end: append_result.store_timestamp,
            last_key: Some(exhausted_cursor.clone()),
        })
        .await
        .expect("an exhausted RocksDB cursor should return an empty page");
    assert_eq!(cursor_result.buffer_total_size, 0);
    assert!(cursor_result.message_maped_list.is_empty());
    assert!(
        !cursor_result.index_query_safe,
        "an empty cursor page must not become a safe not-found result after an index gap"
    );

    assert!(store.install_message_index_runtime(Arc::new(DisabledCompleteIndex)));
    let disabled_hit = store
        .query_message(
            &topic,
            &key,
            10,
            append_result.store_timestamp,
            append_result.store_timestamp,
        )
        .await
        .expect("disabled RocksDB index query result");
    assert!(disabled_hit.buffer_total_size > 0);
    assert!(
        !disabled_hit.index_query_safe,
        "a disabled index cannot make a partial hit safe"
    );

    let disabled_cursor = store
        .query_message_with_options(&QueryMessageRequest {
            topic: topic.clone(),
            key: key.clone(),
            index_type: Some(CheetahString::from_static_str("K")),
            max_num: 10,
            begin: append_result.store_timestamp,
            end: append_result.store_timestamp,
            last_key: Some(exhausted_cursor),
        })
        .await
        .expect("disabled exhausted RocksDB cursor result");
    assert_eq!(disabled_cursor.buffer_total_size, 0);
    assert!(
        !disabled_cursor.index_query_safe,
        "a disabled index cannot report a safe empty page"
    );

    let mut rollback = build_test_message(&topic, 0, b"rocksdb-query-rollback-tail");
    rollback
        .message_ext_inner
        .set_sys_flag(MessageSysFlag::TRANSACTION_ROLLBACK_TYPE);
    let rollback_result = store.put_message(rollback).await;
    assert_eq!(rollback_result.put_message_status(), PutMessageStatus::PutOk);
    let rollback_append = rollback_result.append_message_result().expect("rollback append result");
    store
        .rocksdb_index_service()
        .build_index(&DispatchRequest {
            topic: topic.clone(),
            queue_id: 0,
            commit_log_offset: rollback_append.wrote_offset,
            msg_size: rollback_append.wrote_bytes,
            store_timestamp: rollback_append.store_timestamp,
            sys_flag: MessageSysFlag::TRANSACTION_ROLLBACK_TYPE,
            success: true,
            ..DispatchRequest::default()
        })
        .expect("rollback tail should be accepted without an index record");
    store
        .rocksdb_index_service()
        .flush_pending()
        .expect("rollback tail safe offset should flush");
    assert_eq!(
        store.message_index_safe_offset(),
        Some(rollback_append.wrote_offset + i64::from(rollback_append.wrote_bytes)),
        "an intentionally ignored rollback tail must advance the durable safe offset"
    );

    let exact_backend_root = TempDir::new().expect("create exact-backend temp dir");
    let mut config = rocksdb_store_config(&exact_backend_root);
    config.rocksdb_cq_double_write_enable = true;
    let mut exact_backend_store = new_owned_test_store_with_config(config);
    exact_backend_store.init().await.expect("init exact-backend store");
    assert!(exact_backend_store.load().await, "load exact-backend store");

    let mut first = build_test_message(&topic, 0, b"rocksdb-exact-backend-first");
    first.set_keys(key.clone());
    let first_result = exact_backend_store.put_message(first).await;
    let first_append = first_result.append_message_result().expect("first append result");
    exact_backend_store.reput_once().await;
    assert!(exact_backend_store.install_message_index_runtime(Arc::new(DisabledCompleteIndex)));
    let second_result = exact_backend_store
        .put_message(build_test_message(&topic, 0, b"rocksdb-exact-backend-second"))
        .await;
    let second_append = second_result.append_message_result().expect("second append result");
    assert!(exact_backend_store.install_message_index_runtime(Arc::new(EnabledCompleteIndex)));
    let mut second_dispatch = DispatchRequest {
        topic: topic.clone(),
        queue_id: 0,
        commit_log_offset: second_append.wrote_offset,
        msg_size: second_append.wrote_bytes,
        store_timestamp: second_append.store_timestamp,
        success: true,
        ..DispatchRequest::default()
    };
    exact_backend_store.get_dispatcher_list()[1].dispatch(&mut second_dispatch);

    let partial = exact_backend_store
        .query_message(
            &topic,
            &key,
            10,
            first_append.store_timestamp,
            first_append.store_timestamp,
        )
        .await
        .expect("partial RocksDB primary-index result");
    assert!(partial.buffer_total_size > 0);
    assert!(
        !partial.index_query_safe,
        "RocksDB hits must use the lagging primary-index watermark, not the caught-up local mirror"
    );

    let failure_root = TempDir::new().expect("create failure-latch temp dir");
    let failure_store = new_owned_test_store(&failure_root);
    let failure_service = RocksDbIndexBuildService::new(
        failure_store.message_rocksdb_storage(),
        RocksDbIndexBuildConfig {
            queue_capacity: 1,
            batch_size: 1,
        },
    )
    .expect("create bounded RocksDB index service");
    let failed = failure_service.build_index(&DispatchRequest {
        topic: topic.clone(),
        commit_log_offset: 0,
        msg_size: 100,
        store_timestamp: 1000000000000,
        keys: CheetahString::from_static_str("first-key second-key"),
        success: true,
        ..DispatchRequest::default()
    });
    assert!(failed.is_err(), "a request larger than the queue must fail");
    let later = failure_service.build_index(&DispatchRequest {
        topic,
        commit_log_offset: 100,
        msg_size: 100,
        store_timestamp: 1000000001000,
        keys: CheetahString::from_static_str("later-key"),
        success: true,
        ..DispatchRequest::default()
    });
    assert!(later.is_err(), "a later success must not pass a failed progress gap");
    failure_service
        .flush_pending()
        .expect("empty failed queue should flush");
    assert_eq!(
        failure_service
            .get_safe_dispatch_offset()
            .expect("read failure-latched safe offset"),
        -1,
        "a later request cannot mask the failed indexed message"
    );
    drop(failure_service);
    let restarted_failure_service = RocksDbIndexBuildService::new(
        failure_store.message_rocksdb_storage(),
        RocksDbIndexBuildConfig {
            queue_capacity: 1,
            batch_size: 1,
        },
    )
    .expect("restart failure-latched RocksDB index service");
    assert_eq!(
        restarted_failure_service
            .get_safe_dispatch_offset()
            .expect("read restarted failure latch"),
        -1,
        "the invalid frontier must survive a service restart"
    );
    assert!(
        restarted_failure_service
            .build_index(&DispatchRequest {
                topic: CheetahString::from_static_str("restart-later-topic"),
                commit_log_offset: 0,
                msg_size: 100,
                store_timestamp: 1000000002000,
                keys: CheetahString::from_static_str("restart-later-key"),
                success: true,
                ..DispatchRequest::default()
            })
            .is_err(),
        "restart must not let a later request cross the persisted invalid frontier"
    );

    let legacy_root = TempDir::new().expect("create legacy-index temp dir");
    let legacy_store = new_owned_test_store(&legacy_root);
    let legacy_storage = legacy_store.message_rocksdb_storage();
    legacy_storage
        .write_records_for_index(&[rocketmq_store::IndexRocksDbRecord::unique_key(
            "legacy-topic",
            "legacy-key",
            1000000000000,
            1000,
        )])
        .expect("seed a legacy index without a safe frontier");
    let legacy_service = RocksDbIndexBuildService::new(legacy_storage, RocksDbIndexBuildConfig::default())
        .expect("open legacy index service");
    assert_eq!(
        legacy_service.get_safe_dispatch_offset().expect("legacy safe offset"),
        0
    );
    assert!(
        legacy_service
            .build_index(&DispatchRequest {
                topic: CheetahString::from_static_str("legacy-topic"),
                commit_log_offset: 1100,
                msg_size: 100,
                store_timestamp: 1000000001000,
                keys: CheetahString::from_static_str("post-upgrade-key"),
                success: true,
                ..DispatchRequest::default()
            })
            .is_err(),
        "the first post-upgrade dispatch cannot leap across an unproven legacy frontier"
    );
    assert_eq!(
        legacy_service
            .get_safe_dispatch_offset()
            .expect("invalid legacy frontier"),
        -1
    );
}

#[tokio::test]
async fn rocksdb_index_safe_frontier_crosses_only_a_scanner_verified_commitlog_blank() {
    let temp_dir = TempDir::new().expect("create rollover temp dir");
    let mut config = rocksdb_store_config(&temp_dir);
    config.mapped_file_size_commit_log = 512;
    let mut store = new_owned_test_store_with_config(config);
    store.init().await.expect("init rollover store");
    assert!(store.load().await, "load rollover store");
    store.start().await.expect("start rollover store");
    let topic = CheetahString::from_static_str("af0s");

    let mut tail = MessageExtBrokerInner::default();
    tail.set_topic(topic.clone());
    tail.message_ext_inner.set_queue_id(0);
    tail.set_body(Bytes::from(vec![1_u8; 245]));
    tail.set_keys(CheetahString::from_static_str("tail-key"));
    let tail_result = store.put_message(tail).await;
    assert_eq!(tail_result.put_message_status(), PutMessageStatus::PutOk);
    assert_eq!(
        tail_result.append_message_result().expect("tail append").wrote_offset,
        0
    );

    let mut next = MessageExtBrokerInner::default();
    next.set_topic(topic.clone());
    next.message_ext_inner.set_queue_id(0);
    next.set_body(Bytes::from(vec![2_u8; 75]));
    next.set_keys(CheetahString::from_static_str("next-key"));
    let next_result = store.put_message(next).await;
    assert_eq!(next_result.put_message_status(), PutMessageStatus::PutOk);
    let next_append = next_result.append_message_result().expect("next-file append");
    assert_eq!(next_append.wrote_offset, 512);

    let expected_safe_offset = next_append.wrote_offset + i64::from(next_append.wrote_bytes);
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if store.message_index_safe_offset() == Some(expected_safe_offset) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("background Reput must preserve RocksDB safety across the BLANK event");
    for key in ["tail-key", "next-key"] {
        let result = store
            .query_message(&topic, &CheetahString::from_slice(key), 10, 0, i64::MAX)
            .await
            .expect("RocksDB query after mapped-file rollover");
        assert!(result.buffer_total_size > 0, "missing RocksDB index for {key}");
        assert!(result.index_query_safe, "verified BLANK must preserve RocksDB safety");
    }
    store.shutdown().await;

    let gap_root = TempDir::new().expect("create unverified-gap temp dir");
    let gap_store = new_owned_test_store(&gap_root);
    let gap_service =
        RocksDbIndexBuildService::new(gap_store.message_rocksdb_storage(), RocksDbIndexBuildConfig::default())
            .expect("create unverified-gap service");
    assert_eq!(
        gap_service.initialize_dispatch_frontier(1000).expect("seed frontier"),
        1000
    );
    gap_service
        .build_index(&DispatchRequest {
            topic: CheetahString::from_static_str("gap-topic"),
            commit_log_offset: 1000,
            msg_size: 80,
            store_timestamp: 1000000000000,
            keys: CheetahString::from_static_str("tail-key"),
            success: true,
            ..DispatchRequest::default()
        })
        .expect("contiguous tail dispatch");
    gap_service.flush_pending().expect("flush contiguous tail");
    assert!(gap_service
        .build_index(&DispatchRequest {
            topic: CheetahString::from_static_str("gap-topic"),
            commit_log_offset: 1100,
            msg_size: 50,
            store_timestamp: 1000000001000,
            keys: CheetahString::from_static_str("after-unverified-gap"),
            success: true,
            ..DispatchRequest::default()
        })
        .is_err());
    assert_eq!(
        gap_service.get_safe_dispatch_offset().expect("invalid gap frontier"),
        -1
    );
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
fn rocksdb_time_lookup_and_closed_backend_mapping_preserve_owner_context() {
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
    assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_BACKEND_UNAVAILABLE);
    assert_eq!(error.operation(), StoreOperation::QueryOffset);
    assert_eq!(error.component(), StoreComponent::RocksDb);
    assert!(std::error::Error::source(&error).is_none());
    assert_eq!(store.get_max_offset_in_queue(&topic, 0), 0);
    assert_eq!(store.get_commit_log_offset_in_queue(&topic, 0, 0), -1);
    let flush_error = store.try_flush().expect_err("closed RocksDB flush must fail");
    assert_eq!(flush_error.descriptor(), &rocketmq_error::STORAGE_BACKEND_UNAVAILABLE);
    assert_eq!(flush_error.operation(), StoreOperation::Flush);
    assert_eq!(flush_error.component(), StoreComponent::RocksDb);
    assert!(std::error::Error::source(&flush_error).is_none());
    let health_error = store
        .health_snapshot()
        .last_error
        .expect("flush failure must be reflected in health");
    assert_eq!(health_error, &rocketmq_error::STORAGE_BACKEND_UNAVAILABLE);
}
