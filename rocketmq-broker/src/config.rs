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
pub(crate) mod rocksdb_manager;

#[cfg(all(test, feature = "rocksdb_store"))]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    use super::rocksdb_manager::RocksDbBrokerConfigManager;
    use super::rocksdb_manager::RocksDbBrokerConfigManagerConfig;
    use super::rocksdb_manager::RocksDbBrokerConfigStorageLayout;

    #[test]
    fn rocksdb_config_storage_paths_match_java_single_and_separate_layouts() {
        let root = PathBuf::from("store-root");

        assert_eq!(
            RocksDbBrokerConfigStorageLayout::topic_path(&root, true),
            root.join("config").join("metadata")
        );
        assert_eq!(
            RocksDbBrokerConfigStorageLayout::topic_path(&root, false),
            root.join("config").join("topics")
        );
        assert_eq!(
            RocksDbBrokerConfigStorageLayout::consumer_offset_path(&root, true),
            root.join("config").join("metadata")
        );
        assert_eq!(
            RocksDbBrokerConfigStorageLayout::consumer_offset_path(&root, false),
            root.join("config").join("consumerOffsets")
        );
        assert_eq!(
            RocksDbBrokerConfigStorageLayout::subscription_group_path(&root, true),
            root.join("config").join("metadata")
        );
        assert_eq!(
            RocksDbBrokerConfigStorageLayout::subscription_group_path(&root, false),
            root.join("config").join("subscriptionGroups")
        );
    }

    #[test]
    fn rocksdb_config_manager_writes_utf8_default_cf_and_version_cf_with_wal() {
        let path = unique_temp_path("broker-config");
        let manager = RocksDbBrokerConfigManager::open(RocksDbBrokerConfigManagerConfig::topic(path.clone()))
            .expect("config manager should open");

        assert!(manager.rocksdb_config().wal_enabled);
        assert!(!manager.rocksdb_config().sync_write);
        assert_eq!(manager.default_cf(), "topic");
        assert_eq!(manager.version_cf(), "topicVersion");

        manager
            .put_string("TopicA", r#"{"topicName":"TopicA"}"#)
            .expect("topic config should write");
        manager
            .put_cf_string("forbidden", "GroupA", r#"{"TopicA":1}"#)
            .expect("dynamic forbidden cf should write");
        manager.update_kv_data_version().expect("data version should write");
        manager.flush_wal().expect("wal flush should succeed");
        manager.close();
        drop(manager);

        let reopened = RocksDbBrokerConfigManager::open(RocksDbBrokerConfigManagerConfig::topic(path.clone()))
            .expect("config manager should reopen dynamic cf");
        let data_version = reopened
            .load_data_version()
            .expect("data version should load")
            .expect("data version should exist");

        assert_eq!(
            reopened
                .get_string("TopicA")
                .expect("topic config should read")
                .expect("topic config should exist"),
            r#"{"topicName":"TopicA"}"#
        );
        assert_eq!(
            reopened
                .get_cf_string("forbidden", "GroupA")
                .expect("forbidden config should read")
                .expect("forbidden config should exist"),
            r#"{"TopicA":1}"#
        );
        assert_eq!(data_version.counter(), 1);

        reopened.close();
        let _ = fs::remove_dir_all(path);
    }

    fn unique_temp_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("rocketmq-rust-{name}-{nanos}"))
    }
}
