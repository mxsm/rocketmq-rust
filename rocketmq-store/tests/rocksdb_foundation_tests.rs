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

use bytes::Bytes;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::MessageConst;
use rocketmq_store::base::commit_log_dispatcher::CommitLogDispatcher;
use rocketmq_store::base::dispatch_request::DispatchRequest;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::base::store_enum::StoreType;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::message_store::rocksdb_message_store::RocksDBMessageStore;
use rocketmq_store::message_store::GenericMessageStore;
use rocketmq_store::rocksdb::column_family::RocksDbColumnFamily;
use rocketmq_store::rocksdb::config::RocksDbColumnFamilyConfig;
use rocketmq_store::rocksdb::config::RocksDbCompactionStyle;
use rocketmq_store::rocksdb::config::RocksDbCompressionType;
use rocketmq_store::rocksdb::config::RocksDbConfig;
use rocketmq_store::rocksdb::config::RocksDbDataBlockIndexType;
use rocketmq_store::rocksdb::consume_queue::CommitLogDispatcherBuildRocksDbConsumeQueue;
use rocketmq_store::rocksdb::consume_queue::ConsumeQueueBatchEntry;
use rocketmq_store::rocksdb::consume_queue::ConsumeQueueBatchWriteRequest;
use rocketmq_store::rocksdb::consume_queue::ConsumeQueueOffsetUpdate;
use rocketmq_store::rocksdb::consume_queue::RocksDbConsumeQueueBatchWriter;
use rocketmq_store::rocksdb::consume_queue::RocksDbConsumeQueueGroupCommitConfig;
use rocketmq_store::rocksdb::consume_queue::RocksDbConsumeQueueGroupCommitService;
use rocketmq_store::rocksdb::consume_queue::RocksDbConsumeQueueStore;
use rocketmq_store::rocksdb::index::RocksDbIndexBuildConfig;
use rocketmq_store::rocksdb::index::RocksDbIndexBuildService;
use rocketmq_store::rocksdb::iterator::RocksDbRangeScanOptions;
use rocketmq_store::rocksdb::iterator::RocksDbScanOptions;
use rocketmq_store::rocksdb::key::ConsumeQueueKey;
use rocketmq_store::rocksdb::key::ConsumeQueueOffsetBoundary;
use rocketmq_store::rocksdb::key::ConsumeQueueOffsetKey;
use rocketmq_store::rocksdb::key::IndexRocksDbKey;
use rocketmq_store::rocksdb::key::TimerRocksDbKey;
use rocketmq_store::rocksdb::key::TransRocksDbKey;
use rocketmq_store::rocksdb::maintenance::RocksDbMaintenanceConfig;
use rocketmq_store::rocksdb::maintenance::RocksDbMaintenanceService;
use rocketmq_store::rocksdb::message::IndexRocksDbRecord;
use rocketmq_store::rocksdb::message::MessageRocksDbStorage;
use rocketmq_store::rocksdb::message::TimerRocksDbAction;
use rocketmq_store::rocksdb::message::TimerRocksDbRecord;
use rocketmq_store::rocksdb::message::TransRocksDbRecord;
use rocketmq_store::rocksdb::options::RocksDbOptionsFactory;
use rocketmq_store::rocksdb::options::RocksDbWriteProfile;
use rocketmq_store::rocksdb::store::KeyValueStore;
use rocketmq_store::rocksdb::store::RocksDbStore;
use rocketmq_store::rocksdb::store::RocksDbStoreState;
use rocketmq_store::rocksdb::timer::CommitLogDispatcherBuildRocksDbTimer;
use rocketmq_store::rocksdb::timer::RocksDbTimerBuildConfig;
use rocketmq_store::rocksdb::timer::RocksDbTimerBuildService;
use rocketmq_store::rocksdb::timer::TimerRocksDbBuildEntry;
use rocketmq_store::rocksdb::timer::PROPERTY_TIMER_ROLL_LABEL;
use rocketmq_store::rocksdb::transaction::CommitLogDispatcherBuildRocksDbTrans;
use rocketmq_store::rocksdb::transaction::RocksDbTransBuildConfig;
use rocketmq_store::rocksdb::transaction::RocksDbTransBuildService;
use rocketmq_store::rocksdb::transaction::PROPERTY_TRANS_OFFSET;
use rocketmq_store::rocksdb::transaction::RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC;
use rocketmq_store::rocksdb::transaction::RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC;
use rocketmq_store::rocksdb::value::ConsumeQueueOffsetValue;
use rocketmq_store::rocksdb::value::ConsumeQueueValue;
use rocketmq_store::rocksdb::value::IndexRocksDbValue;
use rocketmq_store::rocksdb::value::MaxPhysicalOffsetCheckpointValue;
use rocketmq_store::rocksdb::value::TimerRocksDbValue;
use rocketmq_store::rocksdb::value::TransRocksDbValue;
use rocketmq_store::timer::timer_message_store::TIMER_TOPIC;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

const KB: usize = 1024;
const MB: usize = 1024 * KB;
const GB: usize = 1024 * MB;
const MB_U64: u64 = 1024 * 1024;
const GB_U64: u64 = 1024 * MB_U64;

#[test]
fn rocksdb_config_defaults_match_java_baseline() {
    let config = RocksDbConfig::default();

    assert!(config.create_missing_column_families);
    assert!(config.manual_wal_flush);
    assert!(!config.sync_write);
    assert_eq!(config.max_background_jobs, 32);
    assert_eq!(config.max_subcompactions, 8);
    assert_eq!(config.max_open_files, -1);
    assert_eq!(config.write_buffer_size, 128 * MB);
    assert_eq!(config.max_write_buffer_number, 4);
    assert_eq!(config.block_size, 32 * KB);
    assert_eq!(config.compression_type, RocksDbCompressionType::Lz4);
    assert_eq!(config.compaction_style, RocksDbCompactionStyle::Universal);
    assert_eq!(config.bytes_per_sync, MB_U64);
    assert_eq!(config.max_log_file_size, GB);
    assert_eq!(config.keep_log_file_num, 5);
    assert_eq!(config.max_manifest_file_size, GB);
    assert!(!config.allow_concurrent_memtable_write);
    assert!(config.paranoid_checks);
    assert_eq!(config.compaction_readahead_size, 4 * MB);
    assert!(!config.use_direct_io_for_flush_and_compaction);
    assert!(!config.use_direct_reads);
    assert_eq!(config.flush_interval_ms, 0);
    assert_eq!(config.compaction_interval_ms, 0);
    assert_eq!(config.checkpoint_interval_ms, 0);
    assert_eq!(config.backup_interval_ms, 0);
    assert!(config.backup_dir.is_none());
}

#[test]
fn rocksdb_consume_queue_config_from_message_store_config_uses_java_default_path() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store_config = MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        ..MessageStoreConfig::default()
    };

    let rocksdb_config = RocksDbConfig::consume_queue_from_message_store_config(&store_config);

    assert!(rocksdb_config.enabled);
    assert_eq!(rocksdb_config.path, temp_dir.path().join("consumequeue_rocksdb"));
    assert_eq!(rocksdb_config.column_families.len(), 2);
    assert_eq!(
        rocksdb_config.column_families[0].name,
        RocksDbColumnFamily::Default.name()
    );
    assert_eq!(
        rocksdb_config.column_families[1].name,
        RocksDbColumnFamily::ConsumeQueueOffset.name()
    );
    assert!(rocksdb_config.manual_wal_flush);
    assert!(!rocksdb_config.wal_enabled);
    assert_eq!(rocksdb_config.write_profile(), RocksDbWriteProfile::DisableWal);
}

#[test]
fn rocksdb_consume_queue_config_can_use_legacy_consumequeue_path() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store_config = MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        use_separate_store_path_for_rocksdb_cq: false,
        ..MessageStoreConfig::default()
    };

    let rocksdb_config = RocksDbConfig::consume_queue_from_message_store_config(&store_config);

    assert_eq!(rocksdb_config.path, temp_dir.path().join("consumequeue"));
    assert_eq!(
        RocksDbConfig::consume_queue_conflict_path_from_message_store_config(&store_config),
        temp_dir.path().join("consumequeue_rocksdb")
    );
}

#[test]
fn rocksdb_config_maps_operational_intervals_and_backup_dir_from_message_store_config() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let backup_dir = temp_dir.path().join("rocksdb-backup");
    let store_config = MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        mem_table_flush_interval_ms: 500,
        clean_rocksdb_dirty_cq_interval_min: 3,
        rocksdb_checkpoint_interval_ms: 60_000,
        rocksdb_backup_interval_ms: 3_600_000,
        rocksdb_backup_dir: Some(CheetahString::from_string(backup_dir.to_string_lossy().to_string())),
        ..MessageStoreConfig::default()
    };

    let cq_config = RocksDbConfig::consume_queue_from_message_store_config(&store_config);
    let message_config = RocksDbConfig::message_from_message_store_config(&store_config);

    for rocksdb_config in [&cq_config, &message_config] {
        assert_eq!(rocksdb_config.flush_interval_ms, 500);
        assert_eq!(rocksdb_config.compaction_interval_ms, 3 * 60 * 1000);
        assert_eq!(rocksdb_config.checkpoint_interval_ms, 60_000);
        assert_eq!(rocksdb_config.backup_interval_ms, 3_600_000);
        assert_eq!(rocksdb_config.backup_dir.as_deref(), Some(backup_dir.as_path()));
    }
}

#[test]
fn rocksdb_message_config_from_message_store_config_uses_java_cf_layout() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store_config = MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        ..MessageStoreConfig::default()
    };

    let rocksdb_config = RocksDbConfig::message_from_message_store_config(&store_config);

    assert!(rocksdb_config.enabled);
    assert_eq!(rocksdb_config.path, temp_dir.path().join("rocksdbstore"));
    assert_eq!(
        rocksdb_config
            .column_families
            .iter()
            .map(|cf| cf.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            RocksDbColumnFamily::Default.name(),
            RocksDbColumnFamily::Timer.name(),
            RocksDbColumnFamily::Transaction.name()
        ]
    );
}

#[test]
fn rocksdb_column_family_configs_match_java_options_baseline() {
    let cq_default = RocksDbColumnFamilyConfig::consume_queue_default();
    assert_eq!(cq_default.name, RocksDbColumnFamily::Default.name());
    assert_eq!(cq_default.write_buffer_size, 128 * MB);
    assert_eq!(cq_default.max_write_buffer_number, 4);
    assert_eq!(cq_default.block_cache_size, 1024 * MB);
    assert_eq!(cq_default.block_size, 32 * KB);
    assert_eq!(cq_default.metadata_block_size, Some(4 * KB));
    assert_eq!(cq_default.format_version, 5);
    assert_eq!(
        cq_default.data_block_index_type,
        RocksDbDataBlockIndexType::BinaryAndHash
    );
    assert_eq!(cq_default.data_block_hash_ratio, Some(0.75));
    assert!(cq_default.whole_key_filtering);
    assert!(cq_default.pin_top_level_index_and_filter);
    assert_eq!(cq_default.compression_type, RocksDbCompressionType::Lz4);
    assert_eq!(cq_default.bottommost_compression_type, RocksDbCompressionType::Lz4);
    assert_eq!(cq_default.compaction_style, RocksDbCompactionStyle::Universal);
    assert_eq!(cq_default.max_compaction_bytes, Some(100 * GB_U64));
    assert_eq!(cq_default.soft_pending_compaction_bytes_limit, Some(100 * GB));
    assert_eq!(cq_default.hard_pending_compaction_bytes_limit, Some(256 * GB));
    assert_eq!(cq_default.level_zero_file_num_compaction_trigger, 2);
    assert_eq!(cq_default.level_zero_slowdown_writes_trigger, 8);
    assert_eq!(cq_default.level_zero_stop_writes_trigger, 10);
    assert_eq!(cq_default.target_file_size_base, 256 * MB_U64);
    assert_eq!(cq_default.target_file_size_multiplier, 2);
    assert!(cq_default.report_bg_io_stats);
    assert!(cq_default.optimize_filters_for_hits);

    let offset = RocksDbColumnFamilyConfig::consume_queue_offset();
    assert_eq!(offset.name, RocksDbColumnFamily::ConsumeQueueOffset.name());
    assert_eq!(offset.write_buffer_size, 64 * MB);
    assert_eq!(offset.block_cache_size, 128 * MB);
    assert_eq!(offset.metadata_block_size, None);
    assert_eq!(offset.data_block_index_type, RocksDbDataBlockIndexType::BinarySearch);
    assert_eq!(offset.data_block_hash_ratio, None);
    assert_eq!(offset.compression_type, RocksDbCompressionType::None);
    assert_eq!(offset.compaction_style, RocksDbCompactionStyle::Level);
    assert_eq!(offset.target_file_size_base, 64 * MB_U64);
    assert_eq!(offset.max_bytes_for_level_base, Some(256 * MB_U64));
    assert_eq!(offset.max_bytes_for_level_multiplier, Some(2.0));
    assert!(offset.inplace_update_support);

    let index = RocksDbColumnFamilyConfig::message_index_default();
    assert_eq!(index.write_buffer_size, 128 * MB);
    assert_eq!(index.max_write_buffer_number, 6);
    assert_eq!(index.block_size, 128 * KB);
    assert_eq!(index.metadata_block_size, Some(4 * KB));
    assert_eq!(index.data_block_index_type, RocksDbDataBlockIndexType::BinaryAndHash);
    assert_eq!(index.data_block_hash_ratio, Some(0.75));
    assert_eq!(index.compression_type, RocksDbCompressionType::None);
    assert_eq!(index.compaction_style, RocksDbCompactionStyle::Universal);
    assert_eq!(index.max_compaction_bytes, Some(256 * MB_U64));
    assert_eq!(index.level_zero_file_num_compaction_trigger, 8);
    assert_eq!(index.level_zero_slowdown_writes_trigger, 8);
    assert_eq!(index.level_zero_stop_writes_trigger, 20);

    let timer = RocksDbColumnFamilyConfig::message_timer();
    assert_eq!(timer.name, RocksDbColumnFamily::Timer.name());
    assert_eq!(timer.write_buffer_size, 256 * MB);
    assert_eq!(timer.max_write_buffer_number, 6);
    assert_eq!(timer.block_cache_size, 2048 * MB);
    assert_eq!(timer.block_size, 128 * KB);
    assert_eq!(timer.metadata_block_size, Some(4 * KB));
    assert_eq!(timer.data_block_index_type, RocksDbDataBlockIndexType::BinaryAndHash);
    assert_eq!(timer.data_block_hash_ratio, Some(0.75));
    assert_eq!(timer.compression_type, RocksDbCompressionType::Zstd);
    assert_eq!(timer.bottommost_compression_type, RocksDbCompressionType::None);
    assert_eq!(timer.compaction_style, RocksDbCompactionStyle::Level);
    assert_eq!(timer.max_compaction_bytes, Some(256 * MB_U64));
    assert_eq!(timer.max_bytes_for_level_base, Some(512 * MB_U64));

    let trans = RocksDbColumnFamilyConfig::message_transaction();
    assert_eq!(trans.name, RocksDbColumnFamily::Transaction.name());
    assert_eq!(trans.write_buffer_size, 128 * MB);
    assert_eq!(trans.max_write_buffer_number, 6);
    assert_eq!(trans.block_cache_size, 1024 * MB);
    assert_eq!(trans.block_size, 128 * KB);
    assert_eq!(trans.metadata_block_size, Some(4 * KB));
    assert_eq!(trans.data_block_index_type, RocksDbDataBlockIndexType::BinaryAndHash);
    assert_eq!(trans.data_block_hash_ratio, Some(0.75));
    assert_eq!(trans.compression_type, RocksDbCompressionType::None);
    assert_eq!(trans.compaction_style, RocksDbCompactionStyle::Universal);
    assert_eq!(trans.max_compaction_bytes, Some(100 * GB_U64));

    let pop = RocksDbColumnFamilyConfig::pop(RocksDbColumnFamily::PopState.name(), 256 * MB, 32 * MB);
    assert_eq!(pop.name, RocksDbColumnFamily::PopState.name());
    assert_eq!(pop.write_buffer_size, 32 * MB);
    assert_eq!(pop.block_cache_size, 256 * MB);
    assert_eq!(pop.metadata_block_size, Some(4 * KB));
    assert_eq!(pop.data_block_index_type, RocksDbDataBlockIndexType::BinaryAndHash);
    assert_eq!(pop.data_block_hash_ratio, Some(0.75));
    assert_eq!(pop.compression_type, RocksDbCompressionType::None);
    assert_eq!(pop.bottommost_compression_type, RocksDbCompressionType::None);
    assert_eq!(pop.compaction_style, RocksDbCompactionStyle::Universal);
    assert_eq!(pop.max_compaction_bytes, Some(100 * GB_U64));

    let broker_config = RocksDbColumnFamilyConfig::broker_config("topic", 4 * MB, 64 * MB);
    assert_eq!(broker_config.write_buffer_size, 64 * MB);
    assert_eq!(broker_config.block_cache_size, 4 * MB);
    assert_eq!(broker_config.metadata_block_size, None);
    assert_eq!(
        broker_config.data_block_index_type,
        RocksDbDataBlockIndexType::BinarySearch
    );
    assert_eq!(broker_config.data_block_hash_ratio, None);
    assert_eq!(broker_config.compression_type, RocksDbCompressionType::None);
    assert_eq!(broker_config.compaction_style, RocksDbCompactionStyle::Level);
    assert_eq!(broker_config.level_zero_file_num_compaction_trigger, 4);
    assert_eq!(broker_config.level_zero_stop_writes_trigger, 12);
    assert_eq!(broker_config.target_file_size_base, 64 * MB_U64);
    assert_eq!(broker_config.max_bytes_for_level_base, Some(256 * MB_U64));
    assert!(broker_config.cache_index_and_filter_blocks);
    assert!(broker_config.inplace_update_support);
}

#[test]
fn rocksdb_options_factory_accepts_java_aligned_db_and_cf_options() {
    let db_config = RocksDbConfig::default();
    RocksDbOptionsFactory::db_options(&db_config).expect("db options should build");

    for cf_config in [
        RocksDbColumnFamilyConfig::consume_queue_default(),
        RocksDbColumnFamilyConfig::consume_queue_offset(),
        RocksDbColumnFamilyConfig::message_index_default(),
        RocksDbColumnFamilyConfig::message_timer(),
        RocksDbColumnFamilyConfig::message_transaction(),
        RocksDbColumnFamilyConfig::pop(RocksDbColumnFamily::PopState.name(), 256 * MB, 32 * MB),
        RocksDbColumnFamilyConfig::broker_config("topic", 4 * MB, 64 * MB),
    ] {
        RocksDbOptionsFactory::cf_options(&cf_config).expect("cf options should build");
    }
}

#[test]
fn rocksdb_consume_queue_config_is_disabled_for_local_file_store_type() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store_config = MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::LocalFile,
        ..MessageStoreConfig::default()
    };

    let rocksdb_config = RocksDbConfig::consume_queue_from_message_store_config(&store_config);

    assert!(!rocksdb_config.enabled);
    assert_eq!(rocksdb_config.path, temp_dir.path().join("consumequeue_rocksdb"));
}

#[test]
fn rocksdb_message_store_exclusively_owns_local_file_root() {
    let source = include_str!("../src/message_store/rocksdb_message_store.rs");

    assert!(source.contains("local_file_store: Box<LocalFileMessageStore>"));
    assert!(!source.contains("use rocketmq_rust::ArcMut"));
    assert!(!source.contains("fn local_file_store_arc"));
}

#[test]
fn rocksdb_message_store_try_new_opens_real_rocksdb_consume_queue_backend() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        ..MessageStoreConfig::default()
    });

    let message_store = RocksDBMessageStore::try_new(
        Arc::clone(&message_store_config),
        Arc::new(BrokerConfig::default()),
        Arc::new(DashMap::<CheetahString, Arc<TopicConfig>>::new()),
        None,
        true,
    )
    .expect("rocksdb message store should construct");

    assert!(message_store.rocksdb_config().enabled);
    assert_eq!(
        message_store.rocksdb_config().path,
        temp_dir.path().join("consumequeue_rocksdb")
    );
    assert_eq!(
        message_store.message_rocksdb_config().path,
        temp_dir.path().join("rocksdbstore")
    );
    assert_eq!(
        message_store.local_file_store().message_store_config_ref().store_type,
        StoreType::RocksDB
    );

    message_store
        .consume_queue_store()
        .put_message_position(&[dispatch_request("TopicA", 3, 7, 700, 30, 11, 1_700_000_000_007)])
        .expect("rocksdb cq write should succeed");
    assert_eq!(
        message_store
            .consume_queue_store()
            .get("TopicA", 3, 7)
            .expect("rocksdb cq get should succeed"),
        Some(encode_java_cq_value(700, 30, 11, 1_700_000_000_007))
    );

    let message_rocksdb_storage = message_store.message_rocksdb_storage();
    message_rocksdb_storage
        .write_records_for_index(&[IndexRocksDbRecord::unique_key(
            "TopicA",
            "uniqA",
            1_700_000_000_007,
            700,
        )])
        .expect("message rocksdb index write should succeed");
    assert_eq!(
        message_rocksdb_storage
            .get_last_offset_py(RocksDbColumnFamily::Default.name())
            .expect("index last offset should read"),
        700
    );
}

#[test]
fn rocksdb_message_store_try_new_rejects_conflicting_consume_queue_path() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let conflict_dir = temp_dir.path().join("consumequeue");
    std::fs::create_dir_all(&conflict_dir).expect("conflict dir should be created");
    std::fs::write(conflict_dir.join("CURRENT"), b"MANIFEST-000001\n").expect("CURRENT marker should be written");
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        ..MessageStoreConfig::default()
    });

    let error = RocksDBMessageStore::try_new(
        message_store_config,
        Arc::new(BrokerConfig::default()),
        Arc::new(DashMap::<CheetahString, Arc<TopicConfig>>::new()),
        None,
        true,
    )
    .expect_err("conflicting consume queue rocksdb path should be rejected");

    assert!(error.to_string().contains("incompatible path"));
    assert!(error.to_string().contains("consumequeue"));
}

#[test]
fn rocksdb_message_store_close_closes_consume_queue_and_message_rocksdb() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        ..MessageStoreConfig::default()
    });

    let message_store = RocksDBMessageStore::try_new(
        Arc::clone(&message_store_config),
        Arc::new(BrokerConfig::default()),
        Arc::new(DashMap::<CheetahString, Arc<TopicConfig>>::new()),
        None,
        true,
    )
    .expect("rocksdb message store should construct");
    let message_rocksdb_storage = message_store.message_rocksdb_storage();

    message_store.close_rocksdb();

    assert!(
        message_store
            .consume_queue_store()
            .put_message_position(&[dispatch_request("TopicA", 3, 7, 700, 30, 11, 1_700_000_000_007)])
            .is_err(),
        "closed consume queue rocksdb should reject writes"
    );
    assert!(
        message_rocksdb_storage
            .write_records_for_index(&[IndexRocksDbRecord::unique_key(
                "TopicA",
                "uniqA",
                1_700_000_000_007,
                700,
            )])
            .is_err(),
        "closed message rocksdb should reject writes"
    );
}

#[test]
fn rocksdb_message_store_try_new_rejects_non_rocksdb_store_type() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::LocalFile,
        ..MessageStoreConfig::default()
    });

    let error = RocksDBMessageStore::try_new(
        message_store_config,
        Arc::new(BrokerConfig::default()),
        Arc::new(DashMap::<CheetahString, Arc<TopicConfig>>::new()),
        None,
        true,
    )
    .expect_err("rocksdb message store should reject local file store type");

    assert!(error.to_string().contains("requires store_type=RocksDB"));
}

#[test]
fn rocksdb_message_store_implements_message_store_trait_boundary() {
    fn assert_message_store<MS: MessageStore>() {}

    assert_message_store::<RocksDBMessageStore>();
}

#[test]
fn generic_message_store_implements_message_store_trait_boundary() {
    fn assert_message_store<MS: MessageStore>() {}

    assert_message_store::<GenericMessageStore>();
}

#[test]
fn column_family_names_match_java_storage_layout() {
    assert_eq!(RocksDbColumnFamily::Default.name(), "default");
    assert_eq!(RocksDbColumnFamily::ConsumeQueueOffset.name(), "offset");
    assert_eq!(RocksDbColumnFamily::Timer.name(), "timer");
    assert_eq!(RocksDbColumnFamily::Transaction.name(), "trans");
    assert_eq!(RocksDbColumnFamily::PopState.name(), "popState");
    assert_eq!(
        RocksDbColumnFamily::Config("kvDataVersion".to_string()).name(),
        "kvDataVersion"
    );
}

#[test]
fn consume_queue_key_codec_matches_java_big_endian_layout() {
    let key = ConsumeQueueKey {
        topic: "TopicA".to_string(),
        queue_id: 3,
        cq_offset: 42,
    };
    let mut actual = Vec::new();

    key.encode(&mut actual).expect("cq key should encode");

    let mut expected = Vec::new();
    expected.extend_from_slice(&6_i32.to_be_bytes());
    expected.push(1);
    expected.extend_from_slice(b"TopicA");
    expected.push(1);
    expected.extend_from_slice(&3_i32.to_be_bytes());
    expected.push(1);
    expected.extend_from_slice(&42_i64.to_be_bytes());

    assert_eq!(actual, expected);
}

#[test]
fn consume_queue_delete_range_keys_match_java_ctrl_bounds() {
    let (start_key, end_key) = ConsumeQueueKey::delete_range("TopicA", 3).expect("delete range keys should encode");
    let mut expected_start = Vec::new();
    expected_start.extend_from_slice(&6_i32.to_be_bytes());
    expected_start.push(1);
    expected_start.extend_from_slice(b"TopicA");
    expected_start.push(1);
    expected_start.extend_from_slice(&3_i32.to_be_bytes());
    expected_start.push(0);
    let mut expected_end = expected_start.clone();
    *expected_end.last_mut().expect("expected end should have boundary byte") = 2;

    assert_eq!(start_key, expected_start);
    assert_eq!(end_key, expected_end);
}

#[test]
fn consume_queue_value_codec_matches_java_28_byte_layout() {
    let value = ConsumeQueueValue {
        commit_log_physical_offset: 1024,
        body_size: 128,
        tag_hash_code: 0x0102_0304_0506_0708,
        msg_store_time: 1_700_000_000_000,
    };
    let mut actual = Vec::new();

    value.encode(&mut actual).expect("cq value should encode");

    let mut expected = Vec::with_capacity(ConsumeQueueValue::ENCODED_LEN);
    expected.extend_from_slice(&1024_i64.to_be_bytes());
    expected.extend_from_slice(&128_i32.to_be_bytes());
    expected.extend_from_slice(&0x0102_0304_0506_0708_i64.to_be_bytes());
    expected.extend_from_slice(&1_700_000_000_000_i64.to_be_bytes());

    assert_eq!(actual.len(), ConsumeQueueValue::ENCODED_LEN);
    assert_eq!(actual, expected);
    assert_eq!(
        ConsumeQueueValue::decode(&actual).expect("cq value should decode"),
        value
    );
}

#[test]
fn index_rocksdb_key_value_codec_matches_java_layout() {
    let store_time = 1_700_000_000_123_i64;
    let store_time_hour = (store_time / 3_600_000) * 3_600_000;
    let key =
        IndexRocksDbKey::normal_key("TopicA", "keyA", "uniqA", store_time, 1024).expect("index key should be valid");
    let mut actual_key = Vec::new();
    key.encode(&mut actual_key).expect("index key should encode");

    let mut expected_key = Vec::new();
    expected_key.extend_from_slice(&store_time_hour.to_be_bytes());
    expected_key.extend_from_slice(b"@TopicA@K@keyA@uniqA@");
    expected_key.extend_from_slice(&1024_i64.to_be_bytes());

    let value = IndexRocksDbValue { store_time };
    let mut actual_value = Vec::new();
    value.encode(&mut actual_value).expect("index value should encode");

    assert_eq!(actual_key, expected_key);
    assert_eq!(actual_value, store_time.to_be_bytes());
    assert_eq!(
        IndexRocksDbValue::decode(&actual_value)
            .expect("index value should decode")
            .store_time,
        store_time
    );
}

#[test]
fn index_rocksdb_unique_and_tag_key_codecs_match_java_layout() {
    let store_time = 1_700_000_000_123_i64;
    let store_time_hour = (store_time / 3_600_000) * 3_600_000;

    let tag_key =
        IndexRocksDbKey::tag_key("TopicA", "tagA", "uniqA", store_time, 1024).expect("tag key should be valid");
    let unique_key =
        IndexRocksDbKey::unique_key("TopicA", "uniqA", store_time, 1024).expect("unique key should be valid");

    let mut actual_tag = Vec::new();
    tag_key.encode(&mut actual_tag).expect("tag key should encode");
    let mut expected_tag = Vec::new();
    expected_tag.extend_from_slice(&store_time_hour.to_be_bytes());
    expected_tag.extend_from_slice(b"@TopicA@T@tagA@uniqA@");
    expected_tag.extend_from_slice(&1024_i64.to_be_bytes());

    let mut actual_unique = Vec::new();
    unique_key.encode(&mut actual_unique).expect("unique key should encode");
    let mut expected_unique = Vec::new();
    expected_unique.extend_from_slice(&store_time_hour.to_be_bytes());
    expected_unique.extend_from_slice(b"@TopicA@U@uniqA@");
    expected_unique.extend_from_slice(&1024_i64.to_be_bytes());

    assert_eq!(actual_tag, expected_tag);
    assert_eq!(actual_unique, expected_unique);
}

#[test]
fn timer_rocksdb_key_value_codec_matches_java_layout() {
    let key = TimerRocksDbKey {
        delay_time: 1_700_000_005_000,
        uniq_key: "uniqA".to_string(),
    };
    let value = TimerRocksDbValue {
        size_py: 128,
        offset_py: 1024,
    };
    let mut actual_key = Vec::new();
    let mut actual_value = Vec::new();

    key.encode(&mut actual_key).expect("timer key should encode");
    value.encode(&mut actual_value).expect("timer value should encode");

    let mut expected_key = Vec::new();
    expected_key.extend_from_slice(&1_700_000_005_000_i64.to_be_bytes());
    expected_key.extend_from_slice(b"uniqA");
    let mut expected_value = Vec::new();
    expected_value.extend_from_slice(&128_i32.to_be_bytes());
    expected_value.extend_from_slice(&1024_i64.to_be_bytes());

    assert_eq!(actual_key, expected_key);
    assert_eq!(actual_value, expected_value);
    assert_eq!(
        TimerRocksDbRecord::decode(&actual_key, &actual_value).expect("timer record should decode"),
        TimerRocksDbRecord {
            delay_time: 1_700_000_005_000,
            uniq_key: "uniqA".to_string(),
            offset_py: 1024,
            size_py: 128,
            action: TimerRocksDbAction::Put,
        }
    );
}

#[test]
fn trans_rocksdb_key_value_codec_matches_java_layout() {
    let key = TransRocksDbKey {
        offset_py: 1024,
        topic: "TopicA".to_string(),
        uniq_key: "uniqA".to_string(),
    };
    let value = TransRocksDbValue {
        check_times: 2,
        size_py: 128,
    };
    let mut actual_key = Vec::new();
    let mut actual_value = Vec::new();

    key.encode(&mut actual_key).expect("trans key should encode");
    value.encode(&mut actual_value).expect("trans value should encode");

    let mut expected_key = Vec::new();
    expected_key.extend_from_slice(&1024_i64.to_be_bytes());
    expected_key.extend_from_slice(b"@TopicA@uniqA");
    let mut expected_value = Vec::new();
    expected_value.extend_from_slice(&2_i32.to_be_bytes());
    expected_value.extend_from_slice(&128_i32.to_be_bytes());

    assert_eq!(actual_key, expected_key);
    assert_eq!(actual_value, expected_value);
    assert_eq!(
        TransRocksDbRecord::decode(&actual_key, &actual_value).expect("trans record should decode"),
        TransRocksDbRecord {
            offset_py: 1024,
            topic: "TopicA".to_string(),
            uniq_key: "uniqA".to_string(),
            check_times: 2,
            size_py: 128,
            is_op: false,
            delete: false,
        }
    );
}

#[test]
fn message_rocksdb_storage_writes_index_records_and_last_progress_keys() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store_config = MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        ..MessageStoreConfig::default()
    };
    let storage = MessageRocksDbStorage::open(RocksDbConfig::message_from_message_store_config(&store_config))
        .expect("message rocksdb storage should open");
    let store_time = 1_700_000_000_123_i64;

    storage
        .write_records_for_index(&[
            IndexRocksDbRecord::normal_key("TopicA", "keyA", "uniqA", store_time, 1024),
            IndexRocksDbRecord::unique_key("TopicA", "uniqA", store_time, 1024),
        ])
        .expect("index records should write");

    assert_eq!(
        storage
            .get_index_store_time(
                &IndexRocksDbKey::normal_key("TopicA", "keyA", "uniqA", store_time, 1024)
                    .expect("index key should be valid")
            )
            .expect("index record should read"),
        Some(store_time)
    );
    assert_eq!(
        storage
            .get_last_offset_py(RocksDbColumnFamily::Default.name())
            .expect("last offset should read"),
        1024
    );
    assert_eq!(
        storage
            .get_last_store_timestamp_for_index()
            .expect("last store timestamp should read"),
        store_time
    );
}

#[test]
fn message_rocksdb_storage_queries_index_offsets_by_java_hour_prefix() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store_config = MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        ..MessageStoreConfig::default()
    };
    let storage = MessageRocksDbStorage::open(RocksDbConfig::message_from_message_store_config(&store_config))
        .expect("message rocksdb storage should open");
    let base_hour = 1_700_000_000_123_i64 / 3_600_000 * 3_600_000;
    let begin = base_hour + 500;
    let first = base_hour + 1_000;
    let second = base_hour + 2_000;
    let filtered = base_hour + 10_000;
    storage
        .write_records_for_index(&[
            IndexRocksDbRecord::normal_key("TopicA", "KeyA", "UniqA", first, 100),
            IndexRocksDbRecord::normal_key("TopicA", "KeyA", "UniqB", second, 200),
            IndexRocksDbRecord::normal_key("TopicA", "KeyA", "UniqC", filtered, 300),
            IndexRocksDbRecord::normal_key("TopicA", "Other", "UniqD", second, 400),
            IndexRocksDbRecord::unique_key("TopicA", "UniqB", second, 200),
        ])
        .expect("index records should write");

    assert_eq!(
        storage
            .query_offsets_for_index("TopicA", MessageConst::INDEX_KEY_TYPE, "KeyA", begin, second, 10)
            .expect("index query should scan"),
        vec![100, 200]
    );
    assert_eq!(
        storage
            .query_offsets_for_index("TopicA", MessageConst::INDEX_UNIQUE_TYPE, "UniqB", begin, filtered, 10)
            .expect("unique index query should scan"),
        vec![200]
    );
    assert_eq!(
        storage
            .query_offsets_for_index("TopicA", MessageConst::INDEX_KEY_TYPE, "KeyA", begin, filtered, 1)
            .expect("limited index query should scan"),
        vec![100]
    );
}

#[test]
fn message_rocksdb_storage_writes_timer_records_with_java_delete_update_semantics() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store_config = MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        ..MessageStoreConfig::default()
    };
    let storage = MessageRocksDbStorage::open(RocksDbConfig::message_from_message_store_config(&store_config))
        .expect("message rocksdb storage should open");
    let key = TimerRocksDbKey {
        delay_time: 1_700_000_005_000,
        uniq_key: "uniqA".to_string(),
    };

    storage
        .write_records_for_timer(&[TimerRocksDbRecord {
            delay_time: key.delay_time,
            uniq_key: key.uniq_key.clone(),
            offset_py: 1024,
            size_py: 128,
            action: TimerRocksDbAction::Put,
        }])
        .expect("timer put should write");
    assert_eq!(
        storage
            .get_timer_record(&key)
            .expect("timer record should read")
            .map(|record| (record.offset_py, record.size_py)),
        Some((1024, 128))
    );

    storage
        .write_records_for_timer(&[
            TimerRocksDbRecord {
                delay_time: key.delay_time,
                uniq_key: key.uniq_key.clone(),
                offset_py: 1024,
                size_py: 128,
                action: TimerRocksDbAction::Delete,
            },
            TimerRocksDbRecord {
                delay_time: key.delay_time,
                uniq_key: key.uniq_key.clone(),
                offset_py: 2048,
                size_py: 256,
                action: TimerRocksDbAction::Update,
            },
        ])
        .expect("timer delete/update batch should write");

    assert_eq!(
        storage.get_timer_record(&key).expect("timer record should be deleted"),
        None
    );
}

#[test]
fn message_rocksdb_storage_scans_and_deletes_timer_records_with_java_range_bounds() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store_config = MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        ..MessageStoreConfig::default()
    };
    let storage = MessageRocksDbStorage::open(RocksDbConfig::message_from_message_store_config(&store_config))
        .expect("message rocksdb storage should open");
    storage
        .write_records_for_timer(&[
            TimerRocksDbRecord {
                delay_time: 1_000,
                uniq_key: "uniqA".to_string(),
                offset_py: 100,
                size_py: 10,
                action: TimerRocksDbAction::Put,
            },
            TimerRocksDbRecord {
                delay_time: 2_000,
                uniq_key: "uniqB".to_string(),
                offset_py: 200,
                size_py: 20,
                action: TimerRocksDbAction::Put,
            },
            TimerRocksDbRecord {
                delay_time: 3_000,
                uniq_key: "uniqC".to_string(),
                offset_py: 300,
                size_py: 30,
                action: TimerRocksDbAction::Put,
            },
        ])
        .expect("timer records should write");

    let scanned = storage
        .scan_records_for_timer(1_000, 3_000, 10, None)
        .expect("timer scan should read range");
    assert_eq!(
        scanned
            .iter()
            .map(|record| (record.delay_time, record.uniq_key.as_str(), record.offset_py))
            .collect::<Vec<_>>(),
        vec![(1_000, "uniqA", 100), (2_000, "uniqB", 200)]
    );

    let start_key = TimerRocksDbKey {
        delay_time: 1_000,
        uniq_key: "uniqA".to_string(),
    };
    let mut encoded_start_key = Vec::new();
    start_key
        .encode(&mut encoded_start_key)
        .expect("timer start key should encode");
    let scanned_after_start = storage
        .scan_records_for_timer(1_000, 3_000, 10, Some(&encoded_start_key))
        .expect("timer scan with start key should skip start key");
    assert_eq!(
        scanned_after_start
            .iter()
            .map(|record| (record.delay_time, record.uniq_key.as_str(), record.offset_py))
            .collect::<Vec<_>>(),
        vec![(2_000, "uniqB", 200)]
    );

    let first_page = storage
        .scan_records_for_timer(1_000, 4_000, 2, None)
        .expect("timer first page should read");
    assert_eq!(
        first_page
            .iter()
            .map(|record| (record.delay_time, record.uniq_key.as_str(), record.offset_py))
            .collect::<Vec<_>>(),
        vec![(1_000, "uniqA", 100), (2_000, "uniqB", 200)]
    );
    let mut last_page_key = Vec::new();
    TimerRocksDbKey {
        delay_time: first_page[1].delay_time,
        uniq_key: first_page[1].uniq_key.clone(),
    }
    .encode(&mut last_page_key)
    .expect("timer page key should encode");
    let second_page = storage
        .scan_records_for_timer(1_000, 4_000, 2, Some(&last_page_key))
        .expect("timer second page should read");
    assert_eq!(
        second_page
            .iter()
            .map(|record| (record.delay_time, record.uniq_key.as_str(), record.offset_py))
            .collect::<Vec<_>>(),
        vec![(3_000, "uniqC", 300)]
    );

    storage
        .delete_records_for_timer(1_000, 2_000)
        .expect("timer range delete should include upper delay time");
    assert!(storage
        .get_timer_record(&TimerRocksDbKey {
            delay_time: 1_000,
            uniq_key: "uniqA".to_string()
        })
        .expect("timer first read should succeed")
        .is_none());
    assert!(storage
        .get_timer_record(&TimerRocksDbKey {
            delay_time: 2_000,
            uniq_key: "uniqB".to_string()
        })
        .expect("timer second read should succeed")
        .is_none());
    assert_eq!(
        storage
            .get_timer_record(&TimerRocksDbKey {
                delay_time: 3_000,
                uniq_key: "uniqC".to_string()
            })
            .expect("timer third read should succeed")
            .map(|record| record.offset_py),
        Some(300)
    );
}

#[test]
fn message_rocksdb_storage_persists_timeline_checkpoint() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store_config = MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        ..MessageStoreConfig::default()
    };
    let storage = MessageRocksDbStorage::open(RocksDbConfig::message_from_message_store_config(&store_config))
        .expect("message rocksdb storage should open");

    assert_eq!(
        storage
            .get_timeline_checkpoint_for_timer()
            .expect("empty timeline checkpoint should read"),
        0
    );
    storage
        .write_timeline_checkpoint_for_timer(1_700_000_123_000)
        .expect("timeline checkpoint should write");
    assert_eq!(
        storage
            .get_timeline_checkpoint_for_timer()
            .expect("timeline checkpoint should read"),
        1_700_000_123_000
    );
    storage
        .delete_timeline_checkpoint_for_timer()
        .expect("timeline checkpoint should delete");
    assert_eq!(
        storage
            .get_timeline_checkpoint_for_timer()
            .expect("deleted timeline checkpoint should read"),
        0
    );
}

#[test]
fn rocksdb_timer_build_config_defaults_match_java_timeline_batching_baseline() {
    let config = RocksDbTimerBuildConfig::default();

    assert_eq!(config.queue_capacity, 100_000);
    assert_eq!(config.batch_size, 1000);
}

#[test]
fn rocksdb_timer_build_service_batches_records_and_persists_scan_checkpoint() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store_config = MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        ..MessageStoreConfig::default()
    };
    let storage = Arc::new(
        MessageRocksDbStorage::open(RocksDbConfig::message_from_message_store_config(&store_config))
            .expect("message rocksdb storage should open"),
    );
    let service = RocksDbTimerBuildService::new(
        Arc::clone(&storage),
        RocksDbTimerBuildConfig {
            queue_capacity: 4,
            batch_size: 1,
        },
    )
    .expect("timer build service should construct");

    assert_eq!(
        service
            .enqueue(TimerRocksDbBuildEntry {
                record: timer_record(2_000, "uniqB", 200, 20, TimerRocksDbAction::Put),
                queue_offset: 8,
            })
            .expect("timer record should enqueue"),
        1
    );
    assert_eq!(service.pending_len(), 1);
    assert_eq!(service.flush_pending().expect("timer records should flush"), 1);

    assert_eq!(
        storage
            .get_timer_record(&TimerRocksDbKey {
                delay_time: 2_000,
                uniq_key: "uniqB".to_string()
            })
            .expect("timer record should read")
            .expect("timer record should exist"),
        timer_record(2_000, "uniqB", 200, 20, TimerRocksDbAction::Put)
    );
    assert_eq!(
        service
            .get_dispatch_from_queue_offset()
            .expect("timer dispatch checkpoint should read"),
        Some(9)
    );
}

#[test]
fn rocksdb_timer_build_service_builds_put_delete_and_update_records_from_dispatch_request() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store_config = MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        ..MessageStoreConfig::default()
    };
    let storage = Arc::new(
        MessageRocksDbStorage::open(RocksDbConfig::message_from_message_store_config(&store_config))
            .expect("message rocksdb storage should open"),
    );
    let service = RocksDbTimerBuildService::new(
        Arc::clone(&storage),
        RocksDbTimerBuildConfig {
            queue_capacity: 4,
            batch_size: 4,
        },
    )
    .expect("timer build service should construct");

    assert_eq!(
        service
            .build_timer_index(&timer_dispatch_request(1_000, "uniqA", 100, 10, 0, None))
            .expect("timer put dispatch should enqueue"),
        1
    );
    assert_eq!(
        service
            .build_timer_index(&timer_dispatch_request(
                2_000,
                "uniqB",
                200,
                20,
                1,
                Some((PROPERTY_TIMER_ROLL_LABEL, "R"))
            ))
            .expect("timer update dispatch should enqueue"),
        1
    );
    assert_eq!(
        service
            .build_timer_index(&timer_dispatch_request(
                1_000,
                "unused",
                300,
                30,
                2,
                Some((MessageConst::PROPERTY_TIMER_DEL_UNIQKEY, "1_000+uniqA"))
            ))
            .expect("timer delete dispatch should enqueue"),
        1
    );
    assert_eq!(
        service
            .build_timer_index(&dispatch_request("NormalTopic", 0, 0, 400, 40, 0, 0))
            .expect("non timer topic should be ignored"),
        0
    );

    assert_eq!(service.flush_pending().expect("timer pending records should flush"), 3);
    assert!(storage
        .get_timer_record(&TimerRocksDbKey {
            delay_time: 1_000,
            uniq_key: "uniqA".to_string()
        })
        .expect("deleted timer record should read")
        .is_none());
    assert_eq!(
        storage
            .get_timer_record(&TimerRocksDbKey {
                delay_time: 2_000,
                uniq_key: "uniqB".to_string()
            })
            .expect("updated timer record should read")
            .expect("updated timer record should exist"),
        timer_record(2_000, "uniqB", 200, 20, TimerRocksDbAction::Put)
    );
    assert_eq!(
        service
            .get_dispatch_from_queue_offset()
            .expect("timer checkpoint should read after dispatch"),
        Some(3)
    );
}

#[test]
fn rocksdb_timer_dispatcher_respects_timer_rocksdb_enable_and_flushes_batches() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store_config = MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        ..MessageStoreConfig::default()
    };
    let storage = Arc::new(
        MessageRocksDbStorage::open(RocksDbConfig::message_from_message_store_config(&store_config))
            .expect("message rocksdb storage should open"),
    );
    let service = Arc::new(
        RocksDbTimerBuildService::new(
            Arc::clone(&storage),
            RocksDbTimerBuildConfig {
                queue_capacity: 4,
                batch_size: 2,
            },
        )
        .expect("timer build service should construct"),
    );

    let disabled_dispatcher = CommitLogDispatcherBuildRocksDbTimer::new(
        Arc::clone(&service),
        Arc::new(MessageStoreConfig {
            store_type: StoreType::RocksDB,
            timer_rocksdb_enable: false,
            ..MessageStoreConfig::default()
        }),
    );
    let mut disabled_request = timer_dispatch_request(1_000, "uniqA", 100, 10, 0, None);
    disabled_dispatcher.dispatch(&mut disabled_request);
    assert_eq!(service.pending_len(), 0);

    let enabled_dispatcher = CommitLogDispatcherBuildRocksDbTimer::new(
        Arc::clone(&service),
        Arc::new(MessageStoreConfig {
            store_type: StoreType::RocksDB,
            timer_rocksdb_enable: true,
            ..MessageStoreConfig::default()
        }),
    );
    let mut requests = [timer_dispatch_request(1_000, "uniqA", 100, 10, 0, None)];
    enabled_dispatcher.dispatch_batch(&mut requests);

    assert_eq!(service.pending_len(), 0);
    assert_eq!(
        storage
            .get_timer_record(&TimerRocksDbKey {
                delay_time: 1_000,
                uniq_key: "uniqA".to_string()
            })
            .expect("dispatcher-written timer record should read")
            .expect("dispatcher-written timer record should exist"),
        timer_record(1_000, "uniqA", 100, 10, TimerRocksDbAction::Put)
    );
    assert_eq!(
        enabled_dispatcher.dispatch_progress_offset(777),
        Some(777),
        "timer dispatch progress must stay in physical CommitLog offset units"
    );
}

#[test]
fn message_rocksdb_storage_writes_trans_records_and_op_deletes() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store_config = MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        ..MessageStoreConfig::default()
    };
    let storage = MessageRocksDbStorage::open(RocksDbConfig::message_from_message_store_config(&store_config))
        .expect("message rocksdb storage should open");
    let key = TransRocksDbKey {
        offset_py: 1024,
        topic: "TopicA".to_string(),
        uniq_key: "uniqA".to_string(),
    };

    storage
        .write_records_for_trans(&[TransRocksDbRecord {
            offset_py: key.offset_py,
            topic: key.topic.clone(),
            uniq_key: key.uniq_key.clone(),
            check_times: 2,
            size_py: 128,
            is_op: false,
            delete: false,
        }])
        .expect("trans half record should write");
    assert_eq!(
        storage
            .get_trans_record(&key)
            .expect("trans record should read")
            .map(|record| (record.check_times, record.size_py)),
        Some((2, 128))
    );
    assert_eq!(
        storage
            .get_last_offset_py(RocksDbColumnFamily::Transaction.name())
            .expect("trans last offset should read"),
        1024
    );

    storage
        .write_records_for_trans(&[TransRocksDbRecord {
            offset_py: key.offset_py,
            topic: key.topic.clone(),
            uniq_key: key.uniq_key.clone(),
            check_times: 0,
            size_py: 0,
            is_op: true,
            delete: false,
        }])
        .expect("trans op record should delete");

    assert_eq!(
        storage.get_trans_record(&key).expect("trans record should be deleted"),
        None
    );
}

#[test]
fn message_rocksdb_storage_scans_and_updates_trans_records_like_java() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store_config = MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        ..MessageStoreConfig::default()
    };
    let storage = MessageRocksDbStorage::open(RocksDbConfig::message_from_message_store_config(&store_config))
        .expect("message rocksdb storage should open");
    storage
        .write_records_for_trans(&[
            trans_record(100, "TopicA", "uniqA", 0, 10),
            trans_record(200, "TopicA", "uniqB", 1, 20),
            trans_record(300, "TopicB", "uniqC", 2, 30),
        ])
        .expect("trans records should write");

    let scanned = storage
        .scan_records_for_trans(10, None)
        .expect("trans scan should succeed");
    assert_eq!(
        scanned
            .iter()
            .map(|record| (record.offset_py, record.topic.as_str(), record.uniq_key.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (100, "TopicA", "uniqA"),
            (200, "TopicA", "uniqB"),
            (300, "TopicB", "uniqC")
        ]
    );

    let mut start_key = Vec::new();
    TransRocksDbKey {
        offset_py: 100,
        topic: "TopicA".to_string(),
        uniq_key: "uniqA".to_string(),
    }
    .encode(&mut start_key)
    .expect("trans start key should encode");
    let scanned_after_start = storage
        .scan_records_for_trans(10, Some(&start_key))
        .expect("trans scan with start key should skip start key");
    assert_eq!(
        scanned_after_start
            .iter()
            .map(|record| record.offset_py)
            .collect::<Vec<_>>(),
        vec![200, 300]
    );

    storage
        .update_records_for_trans(&[
            trans_record(200, "TopicA", "uniqB", 3, 22),
            TransRocksDbRecord {
                delete: true,
                ..trans_record(300, "TopicB", "uniqC", 2, 30)
            },
        ])
        .expect("trans update records should write");

    assert_eq!(
        storage
            .get_trans_record(&TransRocksDbKey {
                offset_py: 200,
                topic: "TopicA".to_string(),
                uniq_key: "uniqB".to_string()
            })
            .expect("updated trans record should read")
            .map(|record| (record.check_times, record.size_py)),
        Some((3, 22))
    );
    assert!(storage
        .get_trans_record(&TransRocksDbKey {
            offset_py: 300,
            topic: "TopicB".to_string(),
            uniq_key: "uniqC".to_string()
        })
        .expect("deleted trans record should read")
        .is_none());
}

#[test]
fn rocksdb_trans_build_config_defaults_match_java_batching_baseline() {
    let config = RocksDbTransBuildConfig::default();

    assert_eq!(config.queue_capacity, 100_000);
    assert_eq!(config.batch_size, 1000);
}

#[test]
fn rocksdb_trans_build_service_batches_records_into_message_rocksdb() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store_config = MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        ..MessageStoreConfig::default()
    };
    let storage = Arc::new(
        MessageRocksDbStorage::open(RocksDbConfig::message_from_message_store_config(&store_config))
            .expect("message rocksdb storage should open"),
    );
    let service = RocksDbTransBuildService::new(
        Arc::clone(&storage),
        RocksDbTransBuildConfig {
            queue_capacity: 4,
            batch_size: 1,
        },
    )
    .expect("trans build service should construct");

    assert_eq!(
        service
            .enqueue(trans_record(100, "TopicA", "uniqA", 0, 10))
            .expect("first trans record should enqueue"),
        1
    );
    assert_eq!(
        service
            .enqueue(trans_record(200, "TopicA", "uniqB", 1, 20))
            .expect("second trans record should enqueue"),
        1
    );
    assert_eq!(service.pending_len(), 2);
    assert_eq!(service.flush_pending().expect("pending trans records should flush"), 2);

    assert_eq!(
        storage
            .scan_records_for_trans(10, None)
            .expect("trans scan should read flushed records")
            .iter()
            .map(|record| record.offset_py)
            .collect::<Vec<_>>(),
        vec![100, 200]
    );
    assert_eq!(
        service
            .get_dispatch_from_phy_offset()
            .expect("trans dispatch offset should read"),
        Some(200)
    );
}

#[test]
fn rocksdb_trans_build_service_applies_backpressure_on_full_queue() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store_config = MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        ..MessageStoreConfig::default()
    };
    let storage = Arc::new(
        MessageRocksDbStorage::open(RocksDbConfig::message_from_message_store_config(&store_config))
            .expect("message rocksdb storage should open"),
    );
    let service = RocksDbTransBuildService::new(
        storage,
        RocksDbTransBuildConfig {
            queue_capacity: 1,
            batch_size: 1,
        },
    )
    .expect("trans build service should construct");

    assert!(service.enqueue(trans_record(100, "TopicA", "uniqA", 0, 10)).is_ok());
    assert!(
        service.enqueue(trans_record(200, "TopicA", "uniqB", 1, 20)).is_err(),
        "full bounded trans queue should reject additional records"
    );
}

#[test]
fn rocksdb_trans_build_service_builds_half_and_op_records_from_dispatch_request() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store_config = MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        ..MessageStoreConfig::default()
    };
    let storage = Arc::new(
        MessageRocksDbStorage::open(RocksDbConfig::message_from_message_store_config(&store_config))
            .expect("message rocksdb storage should open"),
    );
    let service = RocksDbTransBuildService::new(
        Arc::clone(&storage),
        RocksDbTransBuildConfig {
            queue_capacity: 4,
            batch_size: 2,
        },
    )
    .expect("trans build service should construct");

    assert_eq!(
        service
            .build_trans_index(&trans_dispatch_request(RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC, 100, 64, None))
            .expect("half trans dispatch should enqueue"),
        1
    );
    assert_eq!(
        service
            .build_trans_index(&trans_dispatch_request("NormalTopic", 200, 64, None))
            .expect("non rocksdb trans topic should be ignored"),
        0
    );
    assert_eq!(service.flush_pending().expect("half trans record should flush"), 1);

    assert_eq!(
        storage
            .get_trans_record(&TransRocksDbKey {
                offset_py: 100,
                topic: "RealTopic".to_string(),
                uniq_key: "tx-1".to_string()
            })
            .expect("half trans record should read")
            .expect("half trans record should exist"),
        trans_record(100, "RealTopic", "tx-1", 0, 64)
    );

    assert_eq!(
        service
            .build_trans_index(&trans_dispatch_request(
                RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC,
                200,
                32,
                Some(100)
            ))
            .expect("op trans dispatch should enqueue"),
        1
    );
    assert_eq!(service.flush_pending().expect("op trans record should flush"), 1);
    assert!(
        storage
            .get_trans_record(&TransRocksDbKey {
                offset_py: 100,
                topic: "RealTopic".to_string(),
                uniq_key: "tx-1".to_string()
            })
            .expect("deleted half trans record should read")
            .is_none(),
        "op trans record should delete the corresponding half record"
    );
}

#[test]
fn rocksdb_trans_dispatcher_respects_trans_rocksdb_enable_and_flushes_batches() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store_config = MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        ..MessageStoreConfig::default()
    };
    let storage = Arc::new(
        MessageRocksDbStorage::open(RocksDbConfig::message_from_message_store_config(&store_config))
            .expect("message rocksdb storage should open"),
    );
    let service = Arc::new(
        RocksDbTransBuildService::new(
            Arc::clone(&storage),
            RocksDbTransBuildConfig {
                queue_capacity: 4,
                batch_size: 2,
            },
        )
        .expect("trans build service should construct"),
    );

    let disabled_config = Arc::new(MessageStoreConfig {
        store_type: StoreType::RocksDB,
        trans_rocksdb_enable: false,
        ..MessageStoreConfig::default()
    });
    let disabled_dispatcher = CommitLogDispatcherBuildRocksDbTrans::new(Arc::clone(&service), disabled_config);
    let mut disabled_request = trans_dispatch_request(RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC, 100, 64, None);
    disabled_dispatcher.dispatch(&mut disabled_request);
    assert_eq!(service.pending_len(), 0);

    let enabled_config = Arc::new(MessageStoreConfig {
        store_type: StoreType::RocksDB,
        trans_rocksdb_enable: true,
        ..MessageStoreConfig::default()
    });
    let enabled_dispatcher = CommitLogDispatcherBuildRocksDbTrans::new(Arc::clone(&service), enabled_config);
    let mut requests = [trans_dispatch_request(RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC, 100, 64, None)];
    enabled_dispatcher.dispatch_batch(&mut requests);

    assert_eq!(service.pending_len(), 0);
    assert_eq!(
        storage
            .get_trans_record(&TransRocksDbKey {
                offset_py: 100,
                topic: "RealTopic".to_string(),
                uniq_key: "tx-1".to_string()
            })
            .expect("dispatcher-written trans record should read")
            .expect("dispatcher-written trans record should exist"),
        trans_record(100, "RealTopic", "tx-1", 0, 64)
    );
    assert_eq!(
        enabled_dispatcher.dispatch_progress_offset(0),
        Some(100),
        "dispatcher progress should be read from the trans column family"
    );
}

#[test]
fn rocksdb_index_build_config_defaults_match_java_batching_baseline() {
    let config = RocksDbIndexBuildConfig::default();

    assert_eq!(config.queue_capacity, 100_000);
    assert_eq!(config.batch_size, 1000);
}

#[test]
fn rocksdb_index_build_service_batches_dispatch_request_records_into_message_rocksdb() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store_config = MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        ..MessageStoreConfig::default()
    };
    let storage = Arc::new(
        MessageRocksDbStorage::open(RocksDbConfig::message_from_message_store_config(&store_config))
            .expect("message rocksdb storage should open"),
    );
    let service = RocksDbIndexBuildService::new(
        Arc::clone(&storage),
        RocksDbIndexBuildConfig {
            queue_capacity: 8,
            batch_size: 2,
        },
    )
    .expect("index build service should construct");
    let mut properties = std::collections::HashMap::new();
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_TAGS),
        CheetahString::from_static_str("TagA"),
    );
    let store_time = 1_700_000_000_123_i64;
    let request = DispatchRequest {
        topic: CheetahString::from_static_str("TopicA"),
        commit_log_offset: 1024,
        msg_size: 128,
        store_timestamp: store_time,
        keys: CheetahString::from_static_str("KeyA KeyB"),
        uniq_key: Some(CheetahString::from_static_str("UniqA")),
        properties_map: Some(properties),
        ..DispatchRequest::default()
    };

    assert_eq!(
        service
            .build_index(&request)
            .expect("dispatch request should enqueue index records"),
        4
    );
    assert_eq!(service.pending_len(), 4);
    assert_eq!(service.flush_pending().expect("pending index records should flush"), 4);
    assert_eq!(service.pending_len(), 0);

    for key in [
        IndexRocksDbKey::normal_key("TopicA", "KeyA", "UniqA", store_time, 1024)
            .expect("normal index key should encode"),
        IndexRocksDbKey::normal_key("TopicA", "KeyB", "UniqA", store_time, 1024)
            .expect("normal index key should encode"),
        IndexRocksDbKey::tag_key("TopicA", "TagA", "UniqA", store_time, 1024).expect("tag index key should encode"),
        IndexRocksDbKey::unique_key("TopicA", "UniqA", store_time, 1024).expect("unique index key should encode"),
    ] {
        assert_eq!(
            storage.get_index_store_time(&key).expect("index record should read"),
            Some(store_time)
        );
    }
    assert_eq!(
        service
            .get_dispatch_from_phy_offset()
            .expect("index progress should read"),
        Some(1024)
    );
}

#[test]
fn rocksdb_index_build_service_applies_backpressure_on_full_queue() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store_config = MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        ..MessageStoreConfig::default()
    };
    let storage = Arc::new(
        MessageRocksDbStorage::open(RocksDbConfig::message_from_message_store_config(&store_config))
            .expect("message rocksdb storage should open"),
    );
    let service = RocksDbIndexBuildService::new(
        storage,
        RocksDbIndexBuildConfig {
            queue_capacity: 1,
            batch_size: 1,
        },
    )
    .expect("index build service should construct");
    let request = DispatchRequest {
        topic: CheetahString::from_static_str("TopicA"),
        commit_log_offset: 1024,
        msg_size: 128,
        store_timestamp: 1_700_000_000_123,
        uniq_key: Some(CheetahString::from_static_str("UniqA")),
        ..DispatchRequest::default()
    };

    assert_eq!(service.build_index(&request).expect("first record should enqueue"), 1);
    assert!(
        service.build_index(&request).is_err(),
        "full bounded index queue should reject additional records"
    );
}

#[test]
fn consume_queue_offset_key_codec_matches_java_max_min_layout() {
    let key = ConsumeQueueOffsetKey {
        topic: "TopicA".to_string(),
        queue_id: 3,
        boundary: ConsumeQueueOffsetBoundary::Max,
    };
    let mut actual = Vec::new();

    key.encode(&mut actual).expect("offset key should encode");

    let mut expected = Vec::new();
    expected.extend_from_slice(&6_i32.to_be_bytes());
    expected.push(1);
    expected.extend_from_slice(b"TopicA");
    expected.push(1);
    expected.extend_from_slice(b"max");
    expected.push(1);
    expected.extend_from_slice(&3_i32.to_be_bytes());

    assert_eq!(actual, expected);

    let min_key = ConsumeQueueOffsetKey {
        topic: "TopicA".to_string(),
        queue_id: 3,
        boundary: ConsumeQueueOffsetBoundary::Min,
    };
    let mut min_actual = Vec::new();
    min_key.encode(&mut min_actual).expect("min offset key should encode");

    assert_eq!(&min_actual[12..15], b"min");
}

#[test]
fn consume_queue_offset_value_codec_matches_java_16_byte_layout() {
    let value = ConsumeQueueOffsetValue {
        commit_log_offset: 1024,
        consume_queue_offset: 42,
    };
    let mut actual = Vec::new();

    value.encode(&mut actual).expect("offset value should encode");

    let mut expected = Vec::with_capacity(ConsumeQueueOffsetValue::ENCODED_LEN);
    expected.extend_from_slice(&1024_i64.to_be_bytes());
    expected.extend_from_slice(&42_i64.to_be_bytes());

    assert_eq!(actual, expected);
    assert_eq!(
        ConsumeQueueOffsetValue::decode(&actual).expect("offset value should decode"),
        value
    );
}

#[test]
fn column_family_config_rejects_empty_name() {
    let config = RocksDbColumnFamilyConfig {
        name: String::new(),
        ..RocksDbColumnFamilyConfig::consume_queue_default()
    };

    assert!(config.validate().is_err());
}

#[test]
fn rocksdb_store_put_get_delete_and_batch_use_configured_column_families() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store = RocksDbStore::open(test_config(&temp_dir)).expect("rocksdb store should open");

    store
        .put_cf(RocksDbColumnFamily::Default.name(), b"key-a", b"value-a")
        .expect("put should succeed");
    assert_eq!(
        store
            .get_cf(RocksDbColumnFamily::Default.name(), b"key-a")
            .expect("get should succeed")
            .as_deref(),
        Some(&b"value-a"[..])
    );

    let mut batch = rocketmq_store::rocksdb::batch::RocksDbWriteBatch::with_capacity(2);
    batch.put_cf(RocksDbColumnFamily::ConsumeQueueOffset.name(), b"offset-a", b"1");
    batch.put_cf(RocksDbColumnFamily::ConsumeQueueOffset.name(), b"offset-b", b"2");
    store.write_batch(&batch).expect("batch should succeed");

    assert_eq!(
        store
            .get_cf(RocksDbColumnFamily::ConsumeQueueOffset.name(), b"offset-b")
            .expect("batch get should succeed")
            .as_deref(),
        Some(&b"2"[..])
    );

    store
        .delete_cf(RocksDbColumnFamily::Default.name(), b"key-a")
        .expect("delete should succeed");
    assert!(store
        .get_cf(RocksDbColumnFamily::Default.name(), b"key-a")
        .expect("get after delete should succeed")
        .is_none());
}

#[test]
fn rocksdb_store_creates_dynamic_cf_and_reopens_existing_cfs_like_java_config_storage() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let config = RocksDbConfig {
        column_families: vec![RocksDbColumnFamilyConfig::consume_queue_default()],
        ..test_config(&temp_dir)
    };
    let store = RocksDbStore::open_with_existing_column_families(config.clone()).expect("config rocksdb should open");

    let dynamic_cf = RocksDbColumnFamilyConfig {
        name: "topic".to_string(),
        ..RocksDbColumnFamilyConfig::consume_queue_default()
    };
    store
        .create_cf_if_missing(dynamic_cf)
        .expect("dynamic cf should be created");
    store
        .put_cf("topic", b"TopicA", br#"{"topicName":"TopicA"}"#)
        .expect("dynamic cf put should succeed");
    store.flush_wal(false).expect("manual wal flush should succeed");
    store.close();
    drop(store);

    let reopened =
        RocksDbStore::open_with_existing_column_families(config).expect("existing dynamic cf should be reopened");

    assert_eq!(
        reopened
            .get_cf("topic", b"TopicA")
            .expect("dynamic cf get should succeed")
            .expect("dynamic cf value should exist"),
        Bytes::from_static(br#"{"topicName":"TopicA"}"#)
    );
}

#[test]
fn rocksdb_store_prefix_and_range_scan_return_bounded_results() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store = RocksDbStore::open(test_config(&temp_dir)).expect("rocksdb store should open");

    for (key, value) in [
        (&b"cq:0001"[..], &b"a"[..]),
        (&b"cq:0002"[..], &b"b"[..]),
        (&b"cq:0003"[..], &b"c"[..]),
        (&b"other:0001"[..], &b"d"[..]),
    ] {
        store
            .put_cf(RocksDbColumnFamily::Default.name(), key, value)
            .expect("seed put should succeed");
    }

    let prefix_items = store
        .prefix_scan(&RocksDbScanOptions::prefix(
            RocksDbColumnFamily::Default.name(),
            b"cq:",
            2,
        ))
        .expect("prefix scan should succeed");
    assert_eq!(prefix_items.len(), 2);
    assert_eq!(prefix_items[0].key.as_ref(), b"cq:0001");
    assert_eq!(prefix_items[1].key.as_ref(), b"cq:0002");

    let range_items = store
        .range_scan(&RocksDbRangeScanOptions::new(
            RocksDbColumnFamily::Default.name(),
            b"cq:0002",
            b"cq:0004",
            10,
        ))
        .expect("range scan should succeed");
    assert_eq!(
        range_items.iter().map(|item| item.key.as_ref()).collect::<Vec<_>>(),
        vec![&b"cq:0002"[..], &b"cq:0003"[..]]
    );
}

#[tokio::test]
async fn rocksdb_store_blocking_prefix_and_range_scan_return_bounded_results() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store = RocksDbStore::open(test_config(&temp_dir)).expect("rocksdb store should open");

    for (key, value) in [
        (&b"timer:0001"[..], &b"a"[..]),
        (&b"timer:0002"[..], &b"b"[..]),
        (&b"timer:0003"[..], &b"c"[..]),
        (&b"other:0001"[..], &b"d"[..]),
    ] {
        store
            .put_cf(RocksDbColumnFamily::Default.name(), key, value)
            .expect("seed put should succeed");
    }

    let prefix_items = store
        .prefix_scan_blocking(RocksDbScanOptions::prefix(
            RocksDbColumnFamily::Default.name(),
            b"timer:",
            2,
        ))
        .await
        .expect("blocking prefix scan should succeed");
    assert_eq!(
        prefix_items.iter().map(|item| item.key.as_ref()).collect::<Vec<_>>(),
        vec![&b"timer:0001"[..], &b"timer:0002"[..]]
    );

    let range_items = store
        .range_scan_blocking(RocksDbRangeScanOptions::new(
            RocksDbColumnFamily::Default.name(),
            b"timer:0002",
            b"timer:0004",
            10,
        ))
        .await
        .expect("blocking range scan should succeed");
    assert_eq!(
        range_items.iter().map(|item| item.key.as_ref()).collect::<Vec<_>>(),
        vec![&b"timer:0002"[..], &b"timer:0003"[..]]
    );

    assert_eq!(store.metrics().scan_count, 2);
}

#[test]
fn rocksdb_snapshot_keeps_consistent_read_view() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store = RocksDbStore::open(RocksDbConfig {
        column_families: vec![RocksDbColumnFamilyConfig::consume_queue_default()],
        ..test_config(&temp_dir)
    })
    .expect("rocksdb store should open");

    store
        .put_cf(RocksDbColumnFamily::Default.name(), b"snapshot-key", b"before")
        .expect("initial put should succeed");
    let snapshot = store.snapshot().expect("snapshot should be created");
    store
        .put_cf(RocksDbColumnFamily::Default.name(), b"snapshot-key", b"after")
        .expect("second put should succeed");

    assert_eq!(
        snapshot
            .get_cf(RocksDbColumnFamily::Default.name(), b"snapshot-key")
            .expect("snapshot get should succeed")
            .as_deref(),
        Some(&b"before"[..])
    );
    assert_eq!(
        store
            .get_cf(RocksDbColumnFamily::Default.name(), b"snapshot-key")
            .expect("current get should succeed")
            .as_deref(),
        Some(&b"after"[..])
    );
}

#[test]
fn rocksdb_store_property_queries_expose_db_and_cf_observability() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store = RocksDbStore::open(test_config(&temp_dir)).expect("rocksdb store should open");
    store
        .put_cf(RocksDbColumnFamily::Default.name(), b"property-key", b"v1")
        .expect("default cf seed write should succeed");
    store
        .put_cf(
            RocksDbColumnFamily::ConsumeQueueOffset.name(),
            b"property-offset",
            b"v2",
        )
        .expect("offset cf seed write should succeed");

    assert!(store
        .property_value("rocksdb.stats")
        .expect("db stats property should read")
        .is_some());
    assert!(store
        .property_int_value_cf(RocksDbColumnFamily::Default.name(), "rocksdb.estimate-num-keys")
        .expect("default cf estimate should read")
        .is_some());
    assert!(store
        .property_int_value_cf(
            RocksDbColumnFamily::ConsumeQueueOffset.name(),
            "rocksdb.estimate-num-keys"
        )
        .expect("offset cf estimate should read")
        .is_some());
    let error = store
        .property_int_value_cf("missing_cf", "rocksdb.estimate-num-keys")
        .expect_err("missing cf property query should fail");
    assert!(error.to_string().contains("column family not found"));

    let metrics = store.metrics();
    assert_eq!(metrics.property_query_count, 3);
    assert_eq!(metrics.error_count, 1);
}

#[test]
fn rocksdb_store_rejects_writes_after_close() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store = RocksDbStore::open(test_config(&temp_dir)).expect("rocksdb store should open");

    store.close();

    assert!(store
        .put_cf(RocksDbColumnFamily::Default.name(), b"closed-key", b"value")
        .is_err());
}

#[test]
fn rocksdb_store_reloading_state_rejects_new_operations() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store = RocksDbStore::open(test_config(&temp_dir)).expect("rocksdb store should open");

    assert_eq!(store.state(), RocksDbStoreState::Open);
    store.mark_reloading();
    assert_eq!(store.state(), RocksDbStoreState::Reloading);

    let error = store
        .put_cf(RocksDbColumnFamily::Default.name(), b"reloading-key", b"value")
        .expect_err("reloading store should reject writes");
    assert!(error.to_string().contains("reloading"));

    store
        .mark_recovered()
        .expect("recovered store should reopen operations");
    assert_eq!(store.state(), RocksDbStoreState::Open);
    store
        .put_cf(RocksDbColumnFamily::Default.name(), b"recovered-key", b"value")
        .expect("recovered store should accept writes");
}

#[test]
fn rocksdb_store_closed_state_cannot_be_marked_recovered() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store = RocksDbStore::open(test_config(&temp_dir)).expect("rocksdb store should open");

    store.mark_reloading();
    store.close();

    let error = store
        .mark_recovered()
        .expect_err("closed store should not be marked recovered");
    assert!(error.to_string().contains("closed"));
    assert_eq!(store.state(), RocksDbStoreState::Closed);
}

#[test]
fn consume_queue_batch_writer_persists_cq_units_and_offsets() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store = RocksDbStore::open(test_config(&temp_dir)).expect("rocksdb store should open");
    let writer = RocksDbConsumeQueueBatchWriter::new(&store);

    let entry = ConsumeQueueBatchEntry {
        topic: "TopicA".to_string(),
        queue_id: 3,
        consume_queue_offset: 42,
        commit_log_physical_offset: 1024,
        body_size: 128,
        tag_hash_code: 0x0102_0304_0506_0708,
        msg_store_time: 1_700_000_000_000,
    };
    let offset_update = ConsumeQueueOffsetUpdate::max("TopicA", 3, 1024, 42);
    let request = ConsumeQueueBatchWriteRequest {
        entries: vec![entry],
        offset_updates: vec![offset_update],
        max_physical_offset: None,
    };

    let batch = writer.build_batch(&request).expect("batch should build");
    assert_eq!(batch.operations().len(), 2);

    writer.write(&request).expect("batch write should succeed");

    assert_eq!(
        writer.get_cq_value("TopicA", 3, 42).expect("cq value should read"),
        Some(ConsumeQueueValue {
            commit_log_physical_offset: 1024,
            body_size: 128,
            tag_hash_code: 0x0102_0304_0506_0708,
            msg_store_time: 1_700_000_000_000,
        })
    );
    assert_eq!(
        writer
            .get_offset_value("TopicA", 3, ConsumeQueueOffsetBoundary::Max)
            .expect("offset value should read"),
        Some(ConsumeQueueOffsetValue {
            commit_log_offset: 1024,
            consume_queue_offset: 42,
        })
    );
}

#[test]
fn group_commit_config_defaults_match_java_batching_baseline() {
    let config = RocksDbConsumeQueueGroupCommitConfig::default();

    assert_eq!(config.queue_capacity, 100_000);
    assert_eq!(config.batch_size, 256);
}

#[tokio::test]
async fn group_commit_service_drains_requests_and_flushes_on_shutdown() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store = Arc::new(RocksDbStore::open(test_config(&temp_dir)).expect("rocksdb store should open"));
    let service = RocksDbConsumeQueueGroupCommitService::start(
        Arc::clone(&store),
        RocksDbConsumeQueueGroupCommitConfig {
            queue_capacity: 8,
            batch_size: 2,
        },
    )
    .expect("group commit service should start");

    service
        .submit(ConsumeQueueBatchWriteRequest {
            entries: vec![ConsumeQueueBatchEntry {
                topic: "TopicA".to_string(),
                queue_id: 3,
                consume_queue_offset: 42,
                commit_log_physical_offset: 1024,
                body_size: 128,
                tag_hash_code: 7,
                msg_store_time: 1_700_000_000_000,
            }],
            offset_updates: vec![ConsumeQueueOffsetUpdate::max("TopicA", 3, 1024, 42)],
            max_physical_offset: None,
        })
        .await
        .expect("first request should enqueue");
    service
        .submit(ConsumeQueueBatchWriteRequest {
            entries: vec![ConsumeQueueBatchEntry {
                topic: "TopicA".to_string(),
                queue_id: 3,
                consume_queue_offset: 43,
                commit_log_physical_offset: 2048,
                body_size: 256,
                tag_hash_code: 8,
                msg_store_time: 1_700_000_000_001,
            }],
            offset_updates: vec![ConsumeQueueOffsetUpdate::max("TopicA", 3, 2048, 43)],
            max_physical_offset: None,
        })
        .await
        .expect("second request should enqueue");

    service.shutdown().await.expect("shutdown should drain queued requests");

    let writer = RocksDbConsumeQueueBatchWriter::new(store.as_ref());
    assert_eq!(
        writer
            .get_cq_value("TopicA", 3, 42)
            .expect("first cq value should read"),
        Some(ConsumeQueueValue {
            commit_log_physical_offset: 1024,
            body_size: 128,
            tag_hash_code: 7,
            msg_store_time: 1_700_000_000_000,
        })
    );
    assert_eq!(
        writer
            .get_cq_value("TopicA", 3, 43)
            .expect("second cq value should read"),
        Some(ConsumeQueueValue {
            commit_log_physical_offset: 2048,
            body_size: 256,
            tag_hash_code: 8,
            msg_store_time: 1_700_000_000_001,
        })
    );
    assert_eq!(
        writer
            .get_offset_value("TopicA", 3, ConsumeQueueOffsetBoundary::Max)
            .expect("offset value should read"),
        Some(ConsumeQueueOffsetValue {
            commit_log_offset: 2048,
            consume_queue_offset: 43,
        })
    );
}

#[test]
fn group_commit_service_rejects_invalid_queue_config() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store = Arc::new(RocksDbStore::open(test_config(&temp_dir)).expect("rocksdb store should open"));

    assert!(RocksDbConsumeQueueGroupCommitService::start(
        store,
        RocksDbConsumeQueueGroupCommitConfig {
            queue_capacity: 0,
            batch_size: 256,
        },
    )
    .is_err());
}

#[test]
fn consume_queue_range_query_returns_contiguous_values_until_first_missing() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store = RocksDbStore::open(test_config(&temp_dir)).expect("rocksdb store should open");
    let writer = RocksDbConsumeQueueBatchWriter::new(&store);
    let request = ConsumeQueueBatchWriteRequest {
        entries: vec![
            ConsumeQueueBatchEntry {
                topic: "TopicA".to_string(),
                queue_id: 3,
                consume_queue_offset: 0,
                commit_log_physical_offset: 100,
                body_size: 10,
                tag_hash_code: 1,
                msg_store_time: 1_700_000_000_000,
            },
            ConsumeQueueBatchEntry {
                topic: "TopicA".to_string(),
                queue_id: 3,
                consume_queue_offset: 2,
                commit_log_physical_offset: 300,
                body_size: 30,
                tag_hash_code: 3,
                msg_store_time: 1_700_000_000_002,
            },
        ],
        offset_updates: Vec::new(),
        max_physical_offset: None,
    };

    writer.write(&request).expect("seed write should succeed");

    let metrics_before = store.metrics();
    let values = writer
        .range_query_cq_values("TopicA", 3, 0, 3)
        .expect("range query should succeed");
    let metrics_after = store.metrics();
    assert_eq!(metrics_after.scan_count - metrics_before.scan_count, 1);
    assert_eq!(metrics_after.read_count - metrics_before.read_count, 0);
    assert_eq!(
        values,
        vec![ConsumeQueueValue {
            commit_log_physical_offset: 100,
            body_size: 10,
            tag_hash_code: 1,
            msg_store_time: 1_700_000_000_000,
        }]
    );

    let values_after_gap = writer
        .range_query_cq_values("TopicA", 3, 2, 3)
        .expect("range query after gap should succeed");
    let metrics_after_gap = store.metrics();
    assert_eq!(metrics_after_gap.scan_count - metrics_after.scan_count, 1);
    assert_eq!(metrics_after_gap.read_count - metrics_after.read_count, 0);
    assert_eq!(
        values_after_gap,
        vec![ConsumeQueueValue {
            commit_log_physical_offset: 300,
            body_size: 30,
            tag_hash_code: 3,
            msg_store_time: 1_700_000_000_002,
        }]
    );
}

#[test]
fn consume_queue_batch_writer_persists_max_physical_offset_checkpoint() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store = RocksDbStore::open(test_config(&temp_dir)).expect("rocksdb store should open");
    let writer = RocksDbConsumeQueueBatchWriter::new(&store);
    let request = ConsumeQueueBatchWriteRequest {
        entries: Vec::new(),
        offset_updates: Vec::new(),
        max_physical_offset: Some(4096),
    };

    let batch = writer.build_batch(&request).expect("checkpoint batch should build");
    assert_eq!(batch.operations().len(), 1);
    writer.write(&request).expect("checkpoint write should succeed");

    assert_eq!(
        writer
            .get_max_physical_offset_checkpoint()
            .expect("checkpoint should read"),
        MaxPhysicalOffsetCheckpointValue {
            max_physical_offset: 4096,
        }
    );
}

#[test]
fn rocksdb_consume_queue_store_puts_dispatch_request_and_returns_java_cq_bytes() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store = Arc::new(RocksDbStore::open(test_config(&temp_dir)).expect("rocksdb store should open"));
    let cq_store = RocksDbConsumeQueueStore::new(Arc::clone(&store));
    let request = dispatch_request("TopicA", 3, 42, 1024, 128, 7, 1_700_000_000_000);

    cq_store
        .put_message_position(&[request])
        .expect("dispatch request should persist");

    let value = cq_store
        .get("TopicA", 3, 42)
        .expect("cq get should succeed")
        .expect("cq value should exist");
    assert_eq!(value, encode_java_cq_value(1024, 128, 7, 1_700_000_000_000));
    assert_eq!(
        cq_store
            .get_max_offset_in_queue("TopicA", 3)
            .expect("max cq offset should read"),
        43
    );
    assert_eq!(
        cq_store
            .get_max_phy_offset_in_consume_queue("TopicA", 3)
            .expect("max phy offset should read"),
        Some(1024)
    );
    assert_eq!(
        cq_store
            .get_max_phy_offset_in_consume_queue_global()
            .expect("global max phy checkpoint should read"),
        1152
    );
}

#[test]
fn rocksdb_consume_queue_store_batch_range_query_stops_at_first_missing_offset() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store = Arc::new(RocksDbStore::open(test_config(&temp_dir)).expect("rocksdb store should open"));
    let cq_store = RocksDbConsumeQueueStore::new(store);
    let requests = vec![
        dispatch_request("TopicA", 3, 0, 100, 10, 1, 1_700_000_000_000),
        dispatch_request("TopicA", 3, 2, 300, 30, 3, 1_700_000_000_002),
    ];

    cq_store
        .put_message_position(&requests)
        .expect("dispatch batch should persist");

    let values = cq_store
        .range_query("TopicA", 3, 0, 3)
        .expect("range query should succeed");
    assert_eq!(values, vec![encode_java_cq_value(100, 10, 1, 1_700_000_000_000)]);
    assert_eq!(
        cq_store
            .get_max_offset_in_queue("TopicA", 3)
            .expect("max cq offset should read"),
        3
    );
    assert_eq!(
        cq_store
            .get_max_phy_offset_in_consume_queue_global()
            .expect("global checkpoint should read"),
        330
    );
}

#[test]
fn rocksdb_consume_queue_store_destroy_topic_queue_deletes_cq_range_and_offsets() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store = Arc::new(RocksDbStore::open(test_config(&temp_dir)).expect("rocksdb store should open"));
    let writer = RocksDbConsumeQueueBatchWriter::new(store.as_ref());
    writer
        .write(&ConsumeQueueBatchWriteRequest {
            entries: vec![
                ConsumeQueueBatchEntry {
                    topic: "TopicA".to_string(),
                    queue_id: 3,
                    consume_queue_offset: 0,
                    commit_log_physical_offset: 100,
                    body_size: 10,
                    tag_hash_code: 1,
                    msg_store_time: 1_700_000_000_000,
                },
                ConsumeQueueBatchEntry {
                    topic: "TopicA".to_string(),
                    queue_id: 3,
                    consume_queue_offset: 1,
                    commit_log_physical_offset: 120,
                    body_size: 12,
                    tag_hash_code: 2,
                    msg_store_time: 1_700_000_000_001,
                },
                ConsumeQueueBatchEntry {
                    topic: "TopicA".to_string(),
                    queue_id: 4,
                    consume_queue_offset: 0,
                    commit_log_physical_offset: 200,
                    body_size: 20,
                    tag_hash_code: 3,
                    msg_store_time: 1_700_000_000_002,
                },
                ConsumeQueueBatchEntry {
                    topic: "TopicB".to_string(),
                    queue_id: 3,
                    consume_queue_offset: 0,
                    commit_log_physical_offset: 300,
                    body_size: 30,
                    tag_hash_code: 4,
                    msg_store_time: 1_700_000_000_003,
                },
            ],
            offset_updates: vec![
                ConsumeQueueOffsetUpdate::min("TopicA", 3, 100, 0),
                ConsumeQueueOffsetUpdate::max("TopicA", 3, 120, 1),
                ConsumeQueueOffsetUpdate::min("TopicA", 4, 200, 0),
                ConsumeQueueOffsetUpdate::max("TopicA", 4, 200, 0),
                ConsumeQueueOffsetUpdate::min("TopicB", 3, 300, 0),
                ConsumeQueueOffsetUpdate::max("TopicB", 3, 300, 0),
            ],
            max_physical_offset: Some(330),
        })
        .expect("seed write should succeed");
    let cq_store = RocksDbConsumeQueueStore::new(Arc::clone(&store));

    cq_store
        .destroy_topic_queue("TopicA", 3)
        .expect("topic queue destroy should succeed");

    assert!(cq_store
        .get("TopicA", 3, 0)
        .expect("deleted cq get should succeed")
        .is_none());
    assert!(cq_store
        .get("TopicA", 3, 1)
        .expect("deleted cq get should succeed")
        .is_none());
    assert_eq!(
        writer
            .get_offset_value("TopicA", 3, ConsumeQueueOffsetBoundary::Min)
            .expect("deleted min offset read should succeed"),
        None
    );
    assert_eq!(
        writer
            .get_offset_value("TopicA", 3, ConsumeQueueOffsetBoundary::Max)
            .expect("deleted max offset read should succeed"),
        None
    );
    assert_eq!(
        cq_store
            .get("TopicA", 4, 0)
            .expect("same-topic other queue get should succeed")
            .expect("same-topic other queue should remain"),
        encode_java_cq_value(200, 20, 3, 1_700_000_000_002)
    );
    assert_eq!(
        cq_store
            .get("TopicB", 3, 0)
            .expect("other-topic same queue get should succeed")
            .expect("other-topic same queue should remain"),
        encode_java_cq_value(300, 30, 4, 1_700_000_000_003)
    );
    assert_eq!(
        cq_store
            .get_max_phy_offset_in_consume_queue_global()
            .expect("global checkpoint should remain"),
        330
    );
}

#[test]
fn rocksdb_consume_queue_store_destroy_topic_deletes_all_scanned_topic_queues() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store = Arc::new(RocksDbStore::open(test_config(&temp_dir)).expect("rocksdb store should open"));
    let cq_store = RocksDbConsumeQueueStore::new(Arc::clone(&store));
    cq_store
        .put_message_position(&[
            dispatch_request("TopicA", 0, 0, 100, 10, 1, 1_700_000_000_000),
            dispatch_request("TopicA", 1, 0, 200, 20, 2, 1_700_000_000_001),
            dispatch_request("TopicB", 0, 0, 300, 30, 3, 1_700_000_000_002),
        ])
        .expect("seed dispatch batch should persist");

    let mut queue_ids = cq_store
        .scan_queue_ids_in_topic("TopicA")
        .expect("topic queue scan should succeed");
    queue_ids.sort_unstable();
    assert_eq!(queue_ids, vec![0, 1]);

    let mut deleted_queue_ids = cq_store.destroy_topic("TopicA").expect("topic destroy should succeed");
    deleted_queue_ids.sort_unstable();
    assert_eq!(deleted_queue_ids, vec![0, 1]);

    assert!(cq_store
        .get("TopicA", 0, 0)
        .expect("deleted topic queue get should succeed")
        .is_none());
    assert!(cq_store
        .get("TopicA", 1, 0)
        .expect("deleted topic queue get should succeed")
        .is_none());
    assert_eq!(
        cq_store
            .get_max_offset_in_queue("TopicA", 0)
            .expect("deleted topic max offset should read as empty"),
        0
    );
    assert_eq!(
        cq_store
            .get("TopicB", 0, 0)
            .expect("other topic get should succeed")
            .expect("other topic queue should remain"),
        encode_java_cq_value(300, 30, 3, 1_700_000_000_002)
    );
}

#[test]
fn rocksdb_consume_queue_store_truncate_dirty_corrects_offsets_without_deleting_cq_values() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store = Arc::new(RocksDbStore::open(test_config(&temp_dir)).expect("rocksdb store should open"));
    let writer = RocksDbConsumeQueueBatchWriter::new(store.as_ref());
    writer
        .write(&ConsumeQueueBatchWriteRequest {
            entries: vec![
                ConsumeQueueBatchEntry {
                    topic: "TopicA".to_string(),
                    queue_id: 3,
                    consume_queue_offset: 0,
                    commit_log_physical_offset: 100,
                    body_size: 10,
                    tag_hash_code: 1,
                    msg_store_time: 1_700_000_000_000,
                },
                ConsumeQueueBatchEntry {
                    topic: "TopicA".to_string(),
                    queue_id: 3,
                    consume_queue_offset: 1,
                    commit_log_physical_offset: 200,
                    body_size: 20,
                    tag_hash_code: 2,
                    msg_store_time: 1_700_000_000_001,
                },
                ConsumeQueueBatchEntry {
                    topic: "TopicA".to_string(),
                    queue_id: 3,
                    consume_queue_offset: 2,
                    commit_log_physical_offset: 300,
                    body_size: 30,
                    tag_hash_code: 3,
                    msg_store_time: 1_700_000_000_002,
                },
                ConsumeQueueBatchEntry {
                    topic: "TopicB".to_string(),
                    queue_id: 0,
                    consume_queue_offset: 0,
                    commit_log_physical_offset: 180,
                    body_size: 18,
                    tag_hash_code: 4,
                    msg_store_time: 1_700_000_000_003,
                },
            ],
            offset_updates: vec![
                ConsumeQueueOffsetUpdate::min("TopicA", 3, 100, 0),
                ConsumeQueueOffsetUpdate::max("TopicA", 3, 300, 2),
                ConsumeQueueOffsetUpdate::min("TopicB", 0, 180, 0),
                ConsumeQueueOffsetUpdate::max("TopicB", 0, 180, 0),
            ],
            max_physical_offset: Some(330),
        })
        .expect("seed write should succeed");
    let cq_store = RocksDbConsumeQueueStore::new(Arc::clone(&store));

    cq_store
        .truncate_dirty(250)
        .expect("truncate dirty should correct offset table");

    assert_eq!(
        cq_store
            .get_max_phy_offset_in_consume_queue_global()
            .expect("global max phy should read"),
        250
    );
    assert_eq!(
        cq_store
            .get_max_offset_in_queue("TopicA", 3)
            .expect("topic-a corrected max offset should read"),
        2
    );
    assert_eq!(
        cq_store
            .get_max_phy_offset_in_consume_queue("TopicA", 3)
            .expect("topic-a corrected max phy should read"),
        Some(200)
    );
    assert_eq!(
        cq_store
            .get_max_offset_in_queue("TopicB", 0)
            .expect("topic-b max offset should remain"),
        1
    );
    assert_eq!(
        cq_store
            .get("TopicA", 3, 2)
            .expect("dirty cq value read should succeed")
            .expect("dirty cq value should remain for Java parity"),
        encode_java_cq_value(300, 30, 3, 1_700_000_000_002)
    );
}

#[tokio::test]
async fn rocksdb_consume_queue_store_clean_expired_runs_default_cf_manual_compaction_without_changing_offsets() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store = Arc::new(RocksDbStore::open(test_config(&temp_dir)).expect("rocksdb store should open"));
    let cq_store = RocksDbConsumeQueueStore::new(Arc::clone(&store));
    cq_store
        .put_message_position(&[
            dispatch_request("TopicA", 3, 0, 100, 10, 1, 1_700_000_000_000),
            dispatch_request("TopicA", 3, 1, 200, 20, 2, 1_700_000_000_001),
        ])
        .expect("seed dispatch batch should persist");
    assert_eq!(store.manual_compaction_count(), 0);

    cq_store
        .clean_expired(150)
        .await
        .expect("clean expired should compact default cf");

    assert_eq!(store.manual_compaction_count(), 1);
    assert_eq!(
        cq_store
            .get_max_offset_in_queue("TopicA", 3)
            .expect("max offset should be preserved"),
        2
    );
    assert_eq!(
        cq_store
            .get("TopicA", 3, 0)
            .expect("cq read should succeed")
            .expect("cq value should remain after compaction"),
        encode_java_cq_value(100, 10, 1, 1_700_000_000_000)
    );
}

#[tokio::test]
async fn rocksdb_store_checkpoint_can_be_opened_as_independent_readable_database() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store = RocksDbStore::open(test_config(&temp_dir)).expect("rocksdb store should open");
    store
        .put_cf(
            RocksDbColumnFamily::Default.name(),
            b"checkpoint-key",
            b"checkpoint-value",
        )
        .expect("seed write should succeed");
    store.flush().expect("flush before checkpoint should succeed");
    let checkpoint_dir = temp_dir.path().join("checkpoint");

    store
        .create_checkpoint(checkpoint_dir.clone())
        .await
        .expect("checkpoint should be created");

    let checkpoint_config = RocksDbConfig {
        enabled: true,
        path: checkpoint_dir,
        ..RocksDbConfig::default()
    };
    let checkpoint_store = RocksDbStore::open(checkpoint_config).expect("checkpoint rocksdb should open");
    assert_eq!(
        checkpoint_store
            .get_cf(RocksDbColumnFamily::Default.name(), b"checkpoint-key")
            .expect("checkpoint value should read")
            .expect("checkpoint value should exist"),
        Bytes::from_static(b"checkpoint-value")
    );
}

#[tokio::test]
async fn rocksdb_store_checkpoint_rejects_closed_store() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store = RocksDbStore::open(test_config(&temp_dir)).expect("rocksdb store should open");
    store.close();

    let error = store
        .create_checkpoint(temp_dir.path().join("closed-checkpoint"))
        .await
        .expect_err("closed store checkpoint should fail");

    assert!(error.to_string().contains("store is closed"));
}

#[tokio::test]
async fn rocksdb_store_backup_can_restore_latest_backup_as_readable_database() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store = RocksDbStore::open(test_config(&temp_dir)).expect("rocksdb store should open");
    store
        .put_cf(RocksDbColumnFamily::Default.name(), b"backup-key", b"backup-value")
        .expect("default cf seed write should succeed");
    store
        .put_cf(
            RocksDbColumnFamily::ConsumeQueueOffset.name(),
            b"backup-offset-key",
            b"backup-offset-value",
        )
        .expect("offset cf seed write should succeed");
    let backup_dir = temp_dir.path().join("backup");
    let restore_dir = temp_dir.path().join("restore");

    store
        .create_backup(backup_dir.clone())
        .await
        .expect("backup should be created");
    assert_eq!(store.metrics().backup_count, 1);

    RocksDbStore::restore_latest_backup(backup_dir, restore_dir.clone(), None)
        .await
        .expect("latest backup should restore");

    let restored_store = RocksDbStore::open(RocksDbConfig {
        enabled: true,
        path: restore_dir,
        ..RocksDbConfig::default()
    })
    .expect("restored rocksdb should open");
    assert_eq!(
        restored_store
            .get_cf(RocksDbColumnFamily::Default.name(), b"backup-key")
            .expect("restored default cf should read")
            .expect("restored default value should exist"),
        Bytes::from_static(b"backup-value")
    );
    assert_eq!(
        restored_store
            .get_cf(RocksDbColumnFamily::ConsumeQueueOffset.name(), b"backup-offset-key")
            .expect("restored offset cf should read")
            .expect("restored offset value should exist"),
        Bytes::from_static(b"backup-offset-value")
    );
}

#[tokio::test]
async fn rocksdb_store_backup_rejects_closed_store() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store = RocksDbStore::open(test_config(&temp_dir)).expect("rocksdb store should open");
    store.close();

    let error = store
        .create_backup(temp_dir.path().join("closed-backup"))
        .await
        .expect_err("closed store backup should fail");

    assert!(error.to_string().contains("store is closed"));
}

#[tokio::test]
async fn rocksdb_store_restore_latest_backup_fails_when_no_backup_exists() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let backup_dir = temp_dir.path().join("empty-backup");
    let restore_dir = temp_dir.path().join("restore-empty");

    let error = RocksDbStore::restore_latest_backup(backup_dir, restore_dir, None)
        .await
        .expect_err("restore should fail when no backup exists");

    assert!(error.to_string().contains("Restore"));
}

#[tokio::test]
async fn rocksdb_maintenance_service_run_once_executes_enabled_operations() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let rocksdb_config = RocksDbConfig {
        flush_interval_ms: 1,
        compaction_interval_ms: 1,
        checkpoint_interval_ms: 1,
        backup_interval_ms: 1,
        backup_dir: Some(temp_dir.path().join("maintenance-backup")),
        ..test_config(&temp_dir)
    };
    let checkpoint_root = RocksDbMaintenanceConfig::from_rocksdb_config(&rocksdb_config).checkpoint_root;
    let store = Arc::new(RocksDbStore::open(rocksdb_config.clone()).expect("rocksdb store should open"));
    store
        .put_cf(
            RocksDbColumnFamily::Default.name(),
            b"maintenance-key",
            b"maintenance-value",
        )
        .expect("maintenance seed write should succeed");
    let service = RocksDbMaintenanceService::new(Arc::clone(&store), rocksdb_config);

    service.run_once().await.expect("maintenance run should succeed");

    let metrics = store.metrics();
    assert_eq!(metrics.flush_count, 1);
    assert_eq!(metrics.manual_compaction_count, 1);
    assert_eq!(metrics.checkpoint_count, 1);
    assert_eq!(metrics.backup_count, 1);
    assert!(
        checkpoint_root
            .read_dir()
            .expect("checkpoint root should exist")
            .next()
            .is_some(),
        "maintenance checkpoint should create a checkpoint directory"
    );
}

#[tokio::test]
async fn rocksdb_maintenance_service_start_and_shutdown_are_graceful() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let rocksdb_config = RocksDbConfig {
        flush_interval_ms: 10,
        ..test_config(&temp_dir)
    };
    let store = Arc::new(RocksDbStore::open(rocksdb_config.clone()).expect("rocksdb store should open"));
    store
        .put_cf(RocksDbColumnFamily::Default.name(), b"maintenance-loop-key", b"value")
        .expect("maintenance loop seed write should succeed");
    let mut service = RocksDbMaintenanceService::new(Arc::clone(&store), rocksdb_config);

    service.start();
    assert!(service.is_running());

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
    while store.metrics().flush_count == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "maintenance service did not flush before deadline"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    service
        .shutdown_gracefully()
        .await
        .expect("maintenance shutdown should succeed");
    assert!(!service.is_running());
}

#[tokio::test]
async fn rocksdb_store_metrics_count_core_operations_and_errors() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store = RocksDbStore::open(test_config(&temp_dir)).expect("rocksdb store should open");
    assert_eq!(store.metrics(), Default::default());

    store
        .put_cf(RocksDbColumnFamily::Default.name(), b"metrics-a", b"1")
        .expect("put should succeed");
    store
        .get_cf(RocksDbColumnFamily::Default.name(), b"metrics-a")
        .expect("get should succeed");
    store
        .prefix_scan(&RocksDbScanOptions {
            cf: RocksDbColumnFamily::Default.name().to_string(),
            prefix: b"metrics".to_vec(),
            limit: 10,
        })
        .expect("prefix scan should succeed");
    store
        .range_scan(&RocksDbRangeScanOptions {
            cf: RocksDbColumnFamily::Default.name().to_string(),
            start: b"metrics".to_vec(),
            end: b"metrics-z".to_vec(),
            limit: 10,
        })
        .expect("range scan should succeed");
    store.flush().expect("flush should succeed");
    store
        .compact_range_cf(RocksDbColumnFamily::Default.name(), None, None)
        .expect("manual compaction should succeed");
    store
        .create_checkpoint(temp_dir.path().join("metrics-checkpoint"))
        .await
        .expect("checkpoint should succeed");
    let error = store
        .get_cf("missing_cf", b"metrics-a")
        .expect_err("missing cf should be recorded as an error");
    assert!(error.to_string().contains("column family not found"));

    let metrics = store.metrics();
    assert_eq!(metrics.write_count, 1);
    assert_eq!(metrics.read_count, 1);
    assert_eq!(metrics.scan_count, 2);
    assert_eq!(metrics.flush_count, 1);
    assert_eq!(metrics.manual_compaction_count, 1);
    assert_eq!(metrics.checkpoint_count, 1);
    assert_eq!(metrics.error_count, 1);

    let ticker_metrics = store.ticker_metrics();
    assert!(ticker_metrics.bytes_written > 0);
    assert!(ticker_metrics.times_written_self + ticker_metrics.times_written_other > 0);
    assert!(ticker_metrics.times_read > 0);
}

#[test]
fn rocksdb_consume_queue_dispatcher_batch_writes_java_cq_and_offset_entries() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store = Arc::new(RocksDbStore::open(test_config(&temp_dir)).expect("rocksdb store should open"));
    let cq_store = RocksDbConsumeQueueStore::new(Arc::clone(&store));
    let dispatcher = CommitLogDispatcherBuildRocksDbConsumeQueue::new(cq_store.clone());
    let mut requests = vec![
        dispatch_request("TopicA", 3, 0, 100, 10, 1, 1_700_000_000_000),
        dispatch_request("TopicA", 3, 1, 120, 12, 2, 1_700_000_000_001),
    ];

    dispatcher.dispatch_batch(&mut requests);

    assert_eq!(
        cq_store
            .get("TopicA", 3, 1)
            .expect("cq get should succeed")
            .expect("cq value should exist"),
        encode_java_cq_value(120, 12, 2, 1_700_000_000_001)
    );
    assert_eq!(
        cq_store
            .get_max_offset_in_queue("TopicA", 3)
            .expect("max cq offset should read"),
        2
    );
    assert_eq!(
        dispatcher.dispatch_progress_offset(0),
        Some(132),
        "global checkpoint should expose the highest persisted physical boundary"
    );
}

#[test]
fn rocksdb_message_store_dispatcher_dual_writes_commitlog_dispatch_to_rocksdb_cq() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        ..MessageStoreConfig::default()
    });
    let broker_config = Arc::new(BrokerConfig::default());
    let topic_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>> = Arc::new(DashMap::new());
    let mut message_store = RocksDBMessageStore::try_new(message_store_config, broker_config, topic_table, None, false)
        .expect("rocksdb message store should open");
    let mut request = dispatch_request("TopicA", 3, 7, 1024, 128, 7, 1_700_000_000_000);
    request.keys = CheetahString::from_static_str("KeyA");
    request.uniq_key = Some(CheetahString::from_static_str("UniqA"));
    let mut properties = std::collections::HashMap::new();
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_TAGS),
        CheetahString::from_static_str("TagA"),
    );
    request.properties_map = Some(properties);

    message_store.local_file_store_mut().do_dispatch(&mut request);
    message_store.flush();

    assert_eq!(message_store.get_dispatcher_list().len(), 2);
    assert!(
        message_store
            .local_file_store()
            .get_consume_queue(&CheetahString::from_static_str("TopicA"), 3)
            .is_none(),
        "default RocksDB mode must not maintain a hidden Local consume-queue mirror"
    );

    assert_eq!(
        message_store
            .consume_queue_store()
            .get("TopicA", 3, 7)
            .expect("rocksdb cq get should succeed")
            .expect("rocksdb cq value should exist"),
        encode_java_cq_value(1024, 128, 7, 1_700_000_000_000)
    );
    assert_eq!(
        message_store
            .consume_queue_store()
            .get_max_offset_in_queue("TopicA", 3)
            .expect("rocksdb max offset should read"),
        8
    );
    assert_eq!(
        message_store
            .message_rocksdb_storage()
            .query_offsets_for_index(
                "TopicA",
                MessageConst::INDEX_KEY_TYPE,
                "KeyA",
                1_700_000_000_000,
                1_700_000_000_000,
                10
            )
            .expect("rocksdb index query should scan"),
        vec![1024]
    );
    assert_eq!(
        message_store
            .message_rocksdb_storage()
            .query_offsets_for_index(
                "TopicA",
                MessageConst::INDEX_TAG_TYPE,
                "TagA",
                1_700_000_000_000,
                1_700_000_000_000,
                10
            )
            .expect("rocksdb tag index query should scan"),
        vec![1024]
    );
    message_store.close_rocksdb();
}

#[test]
fn rocksdb_message_store_registers_trans_dispatcher_when_enabled() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        trans_rocksdb_enable: true,
        ..MessageStoreConfig::default()
    });
    let message_store = RocksDBMessageStore::try_new(
        Arc::clone(&message_store_config),
        Arc::new(BrokerConfig::default()),
        Arc::new(DashMap::new()),
        None,
        false,
    )
    .expect("rocksdb message store should open");
    let trans_service = message_store
        .rocksdb_trans_service()
        .expect("transaction service should be enabled");

    let mut requests = [trans_dispatch_request(RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC, 100, 64, None)];
    for dispatcher in message_store.local_file_store().get_dispatcher_list() {
        dispatcher.dispatch_batch(&mut requests);
    }

    assert_eq!(trans_service.pending_len(), 0);
    assert_eq!(
        message_store
            .message_rocksdb_storage()
            .get_trans_record(&TransRocksDbKey {
                offset_py: 100,
                topic: "RealTopic".to_string(),
                uniq_key: "tx-1".to_string()
            })
            .expect("message-store-dispatched trans record should read")
            .expect("message-store-dispatched trans record should exist"),
        trans_record(100, "RealTopic", "tx-1", 0, 64)
    );

    message_store.close_rocksdb();
}

#[test]
fn rocksdb_message_store_keeps_trans_service_disabled_by_default() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        ..MessageStoreConfig::default()
    });
    let message_store = RocksDBMessageStore::try_new(
        Arc::clone(&message_store_config),
        Arc::new(BrokerConfig::default()),
        Arc::new(DashMap::new()),
        None,
        false,
    )
    .expect("rocksdb message store should open");

    assert!(message_store.rocksdb_trans_service().is_none());
    message_store.close_rocksdb();
}

#[test]
fn rocksdb_message_store_registers_timer_dispatcher_when_enabled() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        timer_rocksdb_enable: true,
        ..MessageStoreConfig::default()
    });
    let message_store = RocksDBMessageStore::try_new(
        Arc::clone(&message_store_config),
        Arc::new(BrokerConfig::default()),
        Arc::new(DashMap::new()),
        None,
        false,
    )
    .expect("rocksdb message store should open");
    let timer_service = message_store
        .rocksdb_timer_service()
        .expect("timer service should be enabled");

    let mut requests = [timer_dispatch_request(1_000, "uniqA", 100, 10, 0, None)];
    for dispatcher in message_store.local_file_store().get_dispatcher_list() {
        dispatcher.dispatch_batch(&mut requests);
    }

    assert_eq!(timer_service.pending_len(), 0);
    assert_eq!(
        message_store
            .message_rocksdb_storage()
            .get_timer_record(&TimerRocksDbKey {
                delay_time: 1_000,
                uniq_key: "uniqA".to_string()
            })
            .expect("message-store-dispatched timer record should read")
            .expect("message-store-dispatched timer record should exist"),
        timer_record(1_000, "uniqA", 100, 10, TimerRocksDbAction::Put)
    );

    message_store.close_rocksdb();
}

#[test]
fn rocksdb_message_store_keeps_timer_service_disabled_by_default() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        ..MessageStoreConfig::default()
    });
    let message_store = RocksDBMessageStore::try_new(
        Arc::clone(&message_store_config),
        Arc::new(BrokerConfig::default()),
        Arc::new(DashMap::new()),
        None,
        false,
    )
    .expect("rocksdb message store should open");

    assert!(message_store.rocksdb_timer_service().is_none());
    message_store.close_rocksdb();
}

#[test]
fn rocksdb_message_store_delete_topics_removes_rocksdb_consume_queue_state() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        ..MessageStoreConfig::default()
    });
    let broker_config = Arc::new(BrokerConfig::default());
    let topic_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>> = Arc::new(DashMap::new());
    let mut message_store = RocksDBMessageStore::try_new(message_store_config, broker_config, topic_table, None, false)
        .expect("rocksdb message store should open");
    let topic = CheetahString::from_static_str("TopicA");
    let mut request = dispatch_request("TopicA", 3, 7, 1024, 128, 7, 1_700_000_000_000);
    message_store.local_file_store_mut().do_dispatch(&mut request);
    assert_eq!(message_store.get_max_offset_in_queue(&topic, 3), 8);

    message_store.delete_topics(vec![&topic]);

    assert_eq!(message_store.get_max_offset_in_queue(&topic, 3), 0);
    assert!(message_store
        .consume_queue_store()
        .get("TopicA", 3, 7)
        .expect("rocksdb cq get should succeed after delete")
        .is_none());
    message_store.close_rocksdb();
}

#[test]
fn rocksdb_message_store_truncate_dirty_logic_files_corrects_rocksdb_consume_queue_offsets() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        ..MessageStoreConfig::default()
    });
    let broker_config = Arc::new(BrokerConfig::default());
    let topic_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>> = Arc::new(DashMap::new());
    let mut message_store = RocksDBMessageStore::try_new(message_store_config, broker_config, topic_table, None, false)
        .expect("rocksdb message store should open");
    let topic = CheetahString::from_static_str("TopicA");
    let mut first = dispatch_request("TopicA", 3, 0, 100, 10, 1, 1_700_000_000_000);
    let mut dirty = dispatch_request("TopicA", 3, 1, 300, 30, 2, 1_700_000_000_001);
    message_store.local_file_store_mut().do_dispatch(&mut first);
    message_store.local_file_store_mut().do_dispatch(&mut dirty);
    assert_eq!(message_store.get_max_offset_in_queue(&topic, 3), 2);

    message_store.truncate_dirty_logic_files(250);

    assert_eq!(message_store.get_max_offset_in_queue(&topic, 3), 1);
    assert_eq!(
        message_store
            .consume_queue_store()
            .get("TopicA", 3, 1)
            .expect("dirty cq value read should succeed")
            .expect("dirty cq value should remain for Java parity"),
        encode_java_cq_value(300, 30, 2, 1_700_000_000_001)
    );
    message_store.close_rocksdb();
}

#[tokio::test]
async fn rocksdb_message_store_clean_expired_consumer_queue_triggers_background_rocksdb_compaction() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
        store_type: StoreType::RocksDB,
        ..MessageStoreConfig::default()
    });
    let broker_config = Arc::new(BrokerConfig::default());
    let topic_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>> = Arc::new(DashMap::new());
    let mut message_store = RocksDBMessageStore::try_new(message_store_config, broker_config, topic_table, None, false)
        .expect("rocksdb message store should open");
    let mut request = dispatch_request("TopicA", 3, 0, 100, 10, 1, 1_700_000_000_000);
    message_store.local_file_store_mut().do_dispatch(&mut request);
    let rocksdb_store = message_store.rocksdb_store();
    let before = rocksdb_store.manual_compaction_count();

    message_store.clean_expired_consumer_queue();

    for _ in 0..50 {
        if rocksdb_store.manual_compaction_count() > before {
            message_store.close_rocksdb();
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    message_store.close_rocksdb();
    panic!("RocksDB clean expired should trigger a background manual compaction");
}

fn test_config(temp_dir: &TempDir) -> RocksDbConfig {
    RocksDbConfig {
        enabled: true,
        path: temp_dir.path().join("rocksdb"),
        ..RocksDbConfig::default()
    }
}

fn dispatch_request(
    topic: &str,
    queue_id: i32,
    consume_queue_offset: i64,
    commit_log_offset: i64,
    msg_size: i32,
    tags_code: i64,
    store_timestamp: i64,
) -> DispatchRequest {
    DispatchRequest {
        topic: CheetahString::from_string(topic.to_string()),
        queue_id,
        commit_log_offset,
        msg_size,
        tags_code,
        store_timestamp,
        consume_queue_offset,
        success: true,
        ..DispatchRequest::default()
    }
}

fn trans_record(
    offset_py: i64,
    topic: &'static str,
    uniq_key: &'static str,
    check_times: i32,
    size_py: i32,
) -> TransRocksDbRecord {
    TransRocksDbRecord {
        offset_py,
        topic: topic.to_string(),
        uniq_key: uniq_key.to_string(),
        check_times,
        size_py,
        is_op: false,
        delete: false,
    }
}

fn timer_record(
    delay_time: i64,
    uniq_key: &'static str,
    offset_py: i64,
    size_py: i32,
    action: TimerRocksDbAction,
) -> TimerRocksDbRecord {
    TimerRocksDbRecord {
        delay_time,
        uniq_key: uniq_key.to_string(),
        offset_py,
        size_py,
        action,
    }
}

fn trans_dispatch_request(
    topic: &'static str,
    commit_log_offset: i64,
    msg_size: i32,
    trans_offset: Option<i64>,
) -> DispatchRequest {
    let mut properties = HashMap::new();
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC),
        CheetahString::from_static_str("RealTopic"),
    );
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_TRANSACTION_ID),
        CheetahString::from_static_str("tx-1"),
    );
    if let Some(trans_offset) = trans_offset {
        properties.insert(
            CheetahString::from_static_str(PROPERTY_TRANS_OFFSET),
            CheetahString::from_string(trans_offset.to_string()),
        );
    }

    DispatchRequest {
        topic: CheetahString::from_static_str(topic),
        commit_log_offset,
        msg_size,
        properties_map: Some(properties),
        success: true,
        ..DispatchRequest::default()
    }
}

fn timer_dispatch_request(
    delay_time: i64,
    uniq_key: &'static str,
    commit_log_offset: i64,
    msg_size: i32,
    queue_offset: i64,
    extra_property: Option<(&'static str, &'static str)>,
) -> DispatchRequest {
    let mut properties = HashMap::new();
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_OUT_MS),
        CheetahString::from_string(delay_time.to_string()),
    );
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
        CheetahString::from_static_str(uniq_key),
    );
    if let Some((key, value)) = extra_property {
        properties.insert(
            CheetahString::from_static_str(key),
            CheetahString::from_static_str(value),
        );
    }

    DispatchRequest {
        topic: CheetahString::from_static_str(TIMER_TOPIC),
        commit_log_offset,
        msg_size,
        consume_queue_offset: queue_offset,
        properties_map: Some(properties),
        uniq_key: Some(CheetahString::from_static_str(uniq_key)),
        success: true,
        ..DispatchRequest::default()
    }
}

fn encode_java_cq_value(
    commit_log_physical_offset: i64,
    body_size: i32,
    tag_hash_code: i64,
    msg_store_time: i64,
) -> Bytes {
    let mut value = Vec::with_capacity(ConsumeQueueValue::ENCODED_LEN);
    ConsumeQueueValue {
        commit_log_physical_offset,
        body_size,
        tag_hash_code,
        msg_store_time,
    }
    .encode(&mut value)
    .expect("test cq value should encode");
    Bytes::from(value)
}
