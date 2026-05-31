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
use rocketmq_store::rocksdb::key::ConsumeQueueKey;
use rocketmq_store::rocksdb::value::ConsumeQueueValue;

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
fn column_family_config_rejects_empty_name() {
    let config = RocksDbColumnFamilyConfig {
        name: String::new(),
        ..RocksDbColumnFamilyConfig::consume_queue_default()
    };

    assert!(config.validate().is_err());
}
