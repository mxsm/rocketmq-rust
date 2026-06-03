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

#[cfg(feature = "rocksdb_store")]
pub(crate) mod rocksdb_store;

#[cfg(all(test, feature = "rocksdb_store"))]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    use rocketmq_store::rocksdb::column_family::RocksDbColumnFamily;

    use super::rocksdb_store::pop_rocksdb_path;
    use super::rocksdb_store::PopConsumerRecord;
    use super::rocksdb_store::PopConsumerRetryType;
    use super::rocksdb_store::PopConsumerRocksDbStore;
    use super::rocksdb_store::POP_ROCKSDB_DIRECTORY;

    #[test]
    fn pop_consumer_record_key_matches_java_big_endian_layout() {
        let record = pop_record(1, "GroupTest", "TopicTest", 2, 20_000, 100, "AttemptId", false);

        let key = record.key_bytes().expect("pop key should encode");
        let mut expected = Vec::new();
        expected.extend_from_slice(&20_001_i64.to_be_bytes());
        expected.extend_from_slice(b"GroupTest@TopicTest@");
        expected.extend_from_slice(&2_i32.to_be_bytes());
        expected.extend_from_slice(b"@");
        expected.extend_from_slice(&100_i64.to_be_bytes());

        assert_eq!(key, expected);
        assert_eq!(record.visibility_timeout(), 20_001);
        assert!(!record.is_retry());
    }

    #[test]
    fn pop_consumer_record_json_value_roundtrips_java_fields() {
        let mut record = pop_record(1_234_567_890, "GroupId", "TopicId", 3, 20, 100, "AttemptId", true);
        record.retry_flag = PopConsumerRetryType::RetryTopicV1.code();
        record.attempt_times = 2;

        let value = record.value_bytes().expect("pop value should encode");
        let decoded = PopConsumerRecord::decode(&value).expect("pop value should decode");

        assert_eq!(decoded, record);
        assert!(decoded.is_retry());
        let json = std::str::from_utf8(&value).expect("pop value should be JSON");
        assert!(json.contains("\"popTime\":1234567890"));
        assert!(json.contains("\"groupId\":\"GroupId\""));
        assert!(json.contains("\"suspend\":true"));
    }

    #[test]
    fn pop_rocksdb_store_config_uses_java_pop_state_cf_and_sync_wal() {
        let path = unique_temp_path("pop-config");
        let store = PopConsumerRocksDbStore::open(path.clone(), 256 * 1024 * 1024, 32 * 1024 * 1024)
            .expect("pop rocksdb store should open");

        let config = store.config();
        assert!(config.sync_write);
        assert!(config.wal_enabled);
        assert_eq!(
            config
                .column_families
                .iter()
                .map(|cf| cf.name.as_str())
                .collect::<Vec<_>>(),
            vec!["default", RocksDbColumnFamily::PopState.name()]
        );

        store.close();
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn pop_rocksdb_path_uses_java_kvstore_directory() {
        let root = PathBuf::from("store-root");

        assert_eq!(pop_rocksdb_path(&root), root.join(POP_ROCKSDB_DIRECTORY));
        assert_eq!(POP_ROCKSDB_DIRECTORY, "kvStore");
    }

    #[test]
    fn pop_rocksdb_store_writes_deletes_and_scans_expired_records_like_java() {
        let path = unique_temp_path("pop-store");
        let store = PopConsumerRocksDbStore::open(path.clone(), 256 * 1024 * 1024, 32 * 1024 * 1024)
            .expect("pop rocksdb store should open");

        let records = (0..3)
            .flat_map(|queue_id| {
                (0..5).map(move |index| {
                    pop_record(
                        index,
                        "GroupTest",
                        "TopicTest",
                        queue_id,
                        20_000,
                        100 + index,
                        "AttemptId",
                        false,
                    )
                })
            })
            .collect::<Vec<_>>();
        store.write_records(&records).expect("pop records should write");

        let deleted = records
            .iter()
            .filter(|record| record.queue_id < 2)
            .cloned()
            .collect::<Vec<_>>();
        store.delete_records(&deleted).expect("pop records should delete");

        let first = store
            .scan_expired_records(0, 20_002, 2)
            .expect("first expired scan should succeed");
        assert_eq!(first.len(), 2);
        store.delete_records(&first).expect("first scan records should delete");

        let second = store
            .scan_expired_records(0, 20_003, 2)
            .expect("second expired scan should succeed");
        assert_eq!(second.len(), 1);
        store
            .delete_records(&second)
            .expect("second scan records should delete");

        let third = store
            .scan_expired_records(0, 20_005, 3)
            .expect("third expired scan should succeed");
        assert_eq!(third.len(), 2);
        assert!(third.iter().all(|record| record.queue_id == 2));

        store.close();
        let _ = fs::remove_dir_all(path);
    }

    fn pop_record(
        pop_time: i64,
        group_id: &'static str,
        topic_id: &'static str,
        queue_id: i32,
        invisible_time: i64,
        offset: i64,
        attempt_id: &'static str,
        suspend: bool,
    ) -> PopConsumerRecord {
        PopConsumerRecord {
            pop_time,
            group_id: group_id.to_string(),
            topic_id: topic_id.to_string(),
            queue_id,
            retry_flag: PopConsumerRetryType::NormalTopic.code(),
            invisible_time,
            offset,
            attempt_times: 0,
            attempt_id: attempt_id.to_string(),
            suspend,
        }
    }

    fn unique_temp_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("rocketmq-rust-{name}-{nanos}"))
    }
}
