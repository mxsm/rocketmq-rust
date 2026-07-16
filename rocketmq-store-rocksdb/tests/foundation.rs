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

use std::path::PathBuf;

use rocketmq_store_rocksdb::column_family::RocksDbColumnFamily;
use rocketmq_store_rocksdb::config::RocksDbConfig;
use rocketmq_store_rocksdb::config::RocksDbConfigSource;
use rocketmq_store_rocksdb::store::KeyValueStore;
use rocketmq_store_rocksdb::store::RocksDbStore;
use tempfile::TempDir;

struct TestConfigSource {
    root: PathBuf,
    backup: PathBuf,
}

impl RocksDbConfigSource for TestConfigSource {
    fn store_path_root_dir(&self) -> &str {
        self.root.to_str().expect("test root must be valid UTF-8")
    }

    fn rocksdb_store_enabled(&self) -> bool {
        true
    }

    fn use_separate_store_path_for_rocksdb_cq(&self) -> bool {
        true
    }

    fn mem_table_flush_interval_ms(&self) -> usize {
        500
    }

    fn clean_rocksdb_dirty_cq_interval_min(&self) -> usize {
        3
    }

    fn rocksdb_checkpoint_interval_ms(&self) -> usize {
        60_000
    }

    fn rocksdb_backup_interval_ms(&self) -> usize {
        3_600_000
    }

    fn rocksdb_backup_dir(&self) -> Option<&str> {
        self.backup.to_str()
    }
}

#[test]
fn config_projection_is_owned_without_store_facade_types() {
    let root = TempDir::new().expect("create test root");
    let source = TestConfigSource {
        root: root.path().to_path_buf(),
        backup: root.path().join("backup"),
    };

    let consume_queue = RocksDbConfig::consume_queue_from_message_store_config(&source);
    let message = RocksDbConfig::message_from_message_store_config(&source);

    assert!(consume_queue.enabled);
    assert_eq!(consume_queue.path, root.path().join("consumequeue_rocksdb"));
    assert_eq!(message.path, root.path().join("rocksdbstore"));
    assert_eq!(consume_queue.flush_interval_ms, 500);
    assert_eq!(consume_queue.compaction_interval_ms, 3 * 60 * 1000);
    assert_eq!(consume_queue.checkpoint_interval_ms, 60_000);
    assert_eq!(consume_queue.backup_interval_ms, 3_600_000);
    assert_eq!(consume_queue.backup_dir.as_deref(), Some(source.backup.as_path()));
}

#[test]
fn native_store_snapshot_and_reopen_preserve_column_family_data() {
    let root = TempDir::new().expect("create test root");
    let config = RocksDbConfig {
        enabled: true,
        path: root.path().join("rocksdb"),
        ..RocksDbConfig::default()
    };
    let default_cf = RocksDbColumnFamily::Default.name();

    let store = RocksDbStore::open(config.clone()).expect("open RocksDB store");
    store.put_cf(default_cf, b"key", b"value").expect("write value");
    store.flush().expect("flush value");

    let snapshot = store.snapshot().expect("create snapshot");
    assert_eq!(
        snapshot.get_cf(default_cf, b"key").expect("read snapshot").as_deref(),
        Some(b"value".as_slice())
    );
    drop(snapshot);
    drop(store);

    let reopened = RocksDbStore::open_with_existing_column_families(config).expect("reopen RocksDB store");
    assert_eq!(
        reopened
            .get_cf(default_cf, b"key")
            .expect("read reopened store")
            .as_deref(),
        Some(b"value".as_slice())
    );
}
