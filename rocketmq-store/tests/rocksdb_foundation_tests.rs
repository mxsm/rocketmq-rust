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

use rocketmq_store::rocksdb::column_family::RocksDbColumnFamily;
use rocketmq_store::rocksdb::config::RocksDbColumnFamilyConfig;
use rocketmq_store::rocksdb::config::RocksDbCompactionStyle;
use rocketmq_store::rocksdb::config::RocksDbCompressionType;
use rocketmq_store::rocksdb::config::RocksDbConfig;
use rocketmq_store::rocksdb::consume_queue::ConsumeQueueBatchEntry;
use rocketmq_store::rocksdb::consume_queue::ConsumeQueueBatchWriteRequest;
use rocketmq_store::rocksdb::consume_queue::ConsumeQueueOffsetUpdate;
use rocketmq_store::rocksdb::consume_queue::RocksDbConsumeQueueBatchWriter;
use rocketmq_store::rocksdb::iterator::RocksDbRangeScanOptions;
use rocketmq_store::rocksdb::iterator::RocksDbScanOptions;
use rocketmq_store::rocksdb::key::ConsumeQueueKey;
use rocketmq_store::rocksdb::key::ConsumeQueueOffsetBoundary;
use rocketmq_store::rocksdb::key::ConsumeQueueOffsetKey;
use rocketmq_store::rocksdb::store::KeyValueStore;
use rocketmq_store::rocksdb::store::RocksDbStore;
use rocketmq_store::rocksdb::value::ConsumeQueueOffsetValue;
use rocketmq_store::rocksdb::value::ConsumeQueueValue;
use tempfile::TempDir;

#[test]
fn rocksdb_config_defaults_match_java_baseline() {
    let config = RocksDbConfig::default();

    assert!(config.create_missing_column_families);
    assert!(config.manual_wal_flush);
    assert!(!config.sync_write);
    assert_eq!(config.max_background_jobs, 32);
    assert_eq!(config.max_subcompactions, 8);
    assert_eq!(config.max_open_files, -1);
    assert_eq!(config.write_buffer_size, 128 * 1024 * 1024);
    assert_eq!(config.max_write_buffer_number, 4);
    assert_eq!(config.block_size, 32 * 1024);
    assert_eq!(config.compression_type, RocksDbCompressionType::Lz4);
    assert_eq!(config.compaction_style, RocksDbCompactionStyle::Universal);
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

#[test]
fn rocksdb_snapshot_keeps_consistent_read_view() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store = RocksDbStore::open(test_config(&temp_dir)).expect("rocksdb store should open");

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
fn rocksdb_store_rejects_writes_after_close() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let store = RocksDbStore::open(test_config(&temp_dir)).expect("rocksdb store should open");

    store.close();

    assert!(store
        .put_cf(RocksDbColumnFamily::Default.name(), b"closed-key", b"value")
        .is_err());
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

fn test_config(temp_dir: &TempDir) -> RocksDbConfig {
    RocksDbConfig {
        enabled: true,
        path: temp_dir.path().join("rocksdb"),
        ..RocksDbConfig::default()
    }
}
