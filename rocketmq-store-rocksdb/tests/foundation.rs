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

use std::env;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::process::Stdio;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_store_api::StoreOperation;
use rocketmq_store_rocksdb::column_family::RocksDbColumnFamily;
use rocketmq_store_rocksdb::config::RocksDbConfig;
use rocketmq_store_rocksdb::config::RocksDbConfigSource;
use rocketmq_store_rocksdb::index::RocksDbIndexBuildConfig;
use rocketmq_store_rocksdb::index::RocksDbIndexBuildService;
use rocketmq_store_rocksdb::message::MessageRocksDbStorage;
use rocketmq_store_rocksdb::store::KeyValueStore;
use rocketmq_store_rocksdb::store::RocksDbStore;
use rocketmq_store_rocksdb::transaction::RocksDbTransBuildConfig;
use rocketmq_store_rocksdb::transaction::RocksDbTransBuildService;
use rocketmq_store_rocksdb::RocksDbOpenPlan;
use rocketmq_store_rocksdb::RocksDbResourceBudget;
use tempfile::TempDir;

const WAL_CRASH_ROOT_ENV: &str = "ROCKETMQ_ROCKSDB_WAL_CRASH_ROOT";
const WAL_SYNCED_ACK: &str = "ROCKETMQ_ROCKSDB_WAL_SYNCED";

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
    assert_eq!(consume_queue.block_cache_budget_bytes, 1024 * 1024 * 1024);
    assert_eq!(consume_queue.write_buffer_budget_bytes, 512 * 1024 * 1024);
    assert_eq!(message.block_cache_budget_bytes, consume_queue.block_cache_budget_bytes);
    assert_eq!(
        message.write_buffer_budget_bytes,
        consume_queue.write_buffer_budget_bytes
    );
}

#[test]
fn open_plan_validation_performs_no_filesystem_discovery_and_redacts_debug() {
    let root = TempDir::new().expect("create test root");
    let sensitive_root = root.path().join("sensitive-rocksdb-plan-path-canary");
    let unselected = sensitive_root.join("consumequeue");
    std::fs::create_dir_all(&unselected).expect("create unselected RocksDB directory");
    std::fs::write(unselected.join("CURRENT"), b"sensitive-manifest-canary").expect("write unselected RocksDB marker");
    let source = TestConfigSource {
        root: sensitive_root,
        backup: root.path().join("sensitive-rocksdb-plan-backup-canary"),
    };

    let (consume_queue, message) =
        RocksDbOpenPlan::from_message_store(&source).expect("pure configuration should produce plans");

    let rendered = format!("{consume_queue:?} {message:?}");
    assert!(!rendered.contains("sensitive-rocksdb-plan-path-canary"));
    assert!(!rendered.contains("sensitive-rocksdb-plan-backup-canary"));
    assert!(!rendered.contains("sensitive-manifest-canary"));
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

    let store = RocksDbStore::open(config.clone())
        .expect("open RocksDB store")
        .expect("valid RocksDB configuration");
    store
        .put_cf(StoreOperation::Append, default_cf, b"key", b"value")
        .expect("write value");
    store.flush(StoreOperation::Flush).expect("flush value");

    let snapshot = store.snapshot(StoreOperation::Read).expect("create snapshot");
    assert_eq!(
        snapshot
            .get_cf(StoreOperation::Read, default_cf, b"key")
            .expect("read snapshot")
            .as_deref(),
        Some(b"value".as_slice())
    );
    drop(snapshot);
    drop(store);

    let reopened = RocksDbStore::open_with_existing_column_families(config)
        .expect("reopen RocksDB store")
        .expect("valid RocksDB configuration");
    assert_eq!(
        reopened
            .get_cf(StoreOperation::Read, default_cf, b"key")
            .expect("read reopened store")
            .as_deref(),
        Some(b"value".as_slice())
    );
}

#[test]
fn multiple_databases_share_one_cache_and_write_buffer_budget() {
    let root = TempDir::new().expect("create test root");
    let block_cache_budget = 8 * 1024 * 1024;
    let write_buffer_budget = 4 * 1024 * 1024;
    let budget = Arc::new(
        RocksDbResourceBudget::new(block_cache_budget, write_buffer_budget).expect("create shared resource budget"),
    );
    let first_config = RocksDbConfig {
        enabled: true,
        path: root.path().join("first"),
        block_cache_budget_bytes: block_cache_budget,
        write_buffer_budget_bytes: write_buffer_budget,
        ..RocksDbConfig::default()
    };
    let second_config = RocksDbConfig {
        path: root.path().join("second"),
        ..first_config.clone()
    };
    let first_plan = RocksDbOpenPlan::from_config(first_config).expect("valid first configuration");
    let first = RocksDbStore::open_planned_with_metrics_and_resource_budget(
        first_plan,
        rocketmq_observability::metrics::rocksdb::RocksDbMetricsRecorder::noop(),
        Arc::clone(&budget),
    )
    .expect("open first database");
    let second_plan = RocksDbOpenPlan::from_config(second_config).expect("valid second configuration");
    let second = RocksDbStore::open_planned_with_metrics_and_resource_budget(
        second_plan,
        rocketmq_observability::metrics::rocksdb::RocksDbMetricsRecorder::noop(),
        Arc::clone(&budget),
    )
    .expect("open second database");

    assert!(Arc::ptr_eq(&first.resource_budget(), &second.resource_budget()));
    assert!(Arc::ptr_eq(&first.resource_budget(), &budget));
    assert_eq!(budget.block_cache_budget_bytes(), block_cache_budget);
    assert_eq!(budget.write_buffer_budget_bytes(), write_buffer_budget);

    let default_cf = RocksDbColumnFamily::Default.name();
    first
        .put_cf(StoreOperation::Append, default_cf, b"first", b"value")
        .expect("write first database");
    second
        .put_cf(StoreOperation::Append, default_cf, b"second", b"value")
        .expect("write second database");
    assert!(budget.block_cache_usage_bytes() <= block_cache_budget);
    assert!(budget.write_buffer_usage_bytes() <= write_buffer_budget);
}

#[test]
fn derived_offset_lookups_retain_query_offset_on_backend_failure() {
    let root = TempDir::new().expect("create test root");
    let source = TestConfigSource {
        root: root.path().to_path_buf(),
        backup: root.path().join("backup"),
    };
    let config = RocksDbConfig::message_from_message_store_config(&source);
    let storage = Arc::new(
        MessageRocksDbStorage::open(config)
            .expect("open message RocksDB")
            .expect("valid message RocksDB configuration"),
    );
    let index = RocksDbIndexBuildService::new(Arc::clone(&storage), RocksDbIndexBuildConfig::default())
        .expect("create index service");
    let transaction = RocksDbTransBuildService::new(Arc::clone(&storage), RocksDbTransBuildConfig::default())
        .expect("create transaction service");
    storage.store().close();

    let index_error = index
        .get_dispatch_from_phy_offset()
        .expect_err("closed index database must fail");
    let transaction_error = transaction
        .get_dispatch_from_phy_offset()
        .expect_err("closed transaction database must fail");

    assert_eq!(index_error.operation(), StoreOperation::QueryOffset);
    assert_eq!(transaction_error.operation(), StoreOperation::QueryOffset);
}

#[test]
#[ignore = "subprocess helper; the parent test terminates it after the WAL is synchronized"]
fn synchronized_wal_crash_writer_helper() {
    let path = env::var_os(WAL_CRASH_ROOT_ENV).expect("WAL crash root");
    let config = RocksDbConfig {
        enabled: true,
        path: PathBuf::from(path),
        wal_enabled: true,
        sync_write: false,
        manual_wal_flush: true,
        ..RocksDbConfig::default()
    };
    let store = RocksDbStore::open(config)
        .expect("open crash-writer RocksDB")
        .expect("valid RocksDB configuration");
    store
        .put_cf(
            StoreOperation::Append,
            RocksDbColumnFamily::Default.name(),
            b"wal-key",
            b"wal-value",
        )
        .expect("write WAL-backed value");
    store.flush_wal(StoreOperation::Flush, true).expect("synchronize WAL");
    println!("{WAL_SYNCED_ACK}");
    std::io::stdout().flush().expect("flush WAL acknowledgement");
    loop {
        std::thread::park();
    }
}

#[test]
fn synchronized_wal_recovers_after_process_termination() {
    let root = TempDir::new().expect("create WAL crash root");
    let database_path = root.path().join("rocksdb");
    let executable = env::current_exe().expect("resolve foundation test executable");
    let mut child = Command::new(executable)
        .args([
            "--exact",
            "synchronized_wal_crash_writer_helper",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(WAL_CRASH_ROOT_ENV, &database_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn WAL crash writer");
    let stdout = child.stdout.take().expect("capture WAL crash writer stdout");
    let (sender, receiver) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if sender.send(line).is_err() {
                break;
            }
        }
    });

    loop {
        match receiver.recv_timeout(Duration::from_secs(30)) {
            Ok(Ok(line)) if line.contains(WAL_SYNCED_ACK) => break,
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                let _ = child.kill();
                panic!("read WAL crash-writer output: {error}");
            }
            Err(error) => {
                let _ = child.kill();
                panic!("WAL crash writer did not acknowledge durable WAL: {error}");
            }
        }
    }
    child.kill().expect("terminate WAL crash writer");
    child.wait().expect("reap WAL crash writer");
    reader.join().expect("join WAL output reader");

    let config = RocksDbConfig {
        enabled: true,
        path: database_path,
        wal_enabled: true,
        sync_write: false,
        manual_wal_flush: true,
        ..RocksDbConfig::default()
    };
    let recovered = RocksDbStore::open_with_existing_column_families(config)
        .expect("recover RocksDB from WAL")
        .expect("valid RocksDB configuration");
    assert_eq!(
        recovered
            .get_cf(StoreOperation::Read, RocksDbColumnFamily::Default.name(), b"wal-key",)
            .expect("read WAL-recovered value")
            .as_deref(),
        Some(b"wal-value".as_slice())
    );
}
