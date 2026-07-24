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

//! Controller durability and storage-fault tests.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use openraft::storage::IOFlushed;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftStateMachine;
use openraft::RaftLogReader;
use parking_lot::RwLock;
use rocketmq_controller::config::ControllerConfig;
use rocketmq_controller::config::ControllerConfigReader;
use rocketmq_controller::config::StorageBackendType;
use rocketmq_controller::error::ControllerError;
use rocketmq_controller::error::Result;
use rocketmq_controller::openraft::LogStore;
use rocketmq_controller::openraft::StateMachine;
use rocketmq_controller::storage::StorageBackend;
use rocketmq_controller::storage::StorageStats;
use rocketmq_controller::typ::ControllerRequest;
use rocketmq_controller::typ::EntryPayload;
use rocketmq_controller::typ::LogEntry;
use rocketmq_controller::typ::LogId;
use rocketmq_controller::typ::TypeConfig;
use rocketmq_controller::typ::Vote;

#[derive(Default)]
struct FaultInjectingBackend {
    data: RwLock<HashMap<String, Vec<u8>>>,
    reject_batch: AtomicBool,
}

impl FaultInjectingBackend {
    fn rejecting_batches() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
            reject_batch: AtomicBool::new(true),
        }
    }
}

#[async_trait]
impl StorageBackend for FaultInjectingBackend {
    async fn put(&self, key: &str, value: &[u8]) -> Result<()> {
        self.data.write().insert(key.to_string(), value.to_vec());
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.data.read().get(key).cloned())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.data.write().remove(key);
        Ok(())
    }

    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self
            .data
            .read()
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect())
    }

    async fn batch_put(&self, items: Vec<(String, Vec<u8>)>) -> Result<()> {
        if self.reject_batch.load(Ordering::Acquire) {
            return Err(ControllerError::StorageError("injected batch failure".to_string()));
        }
        let mut data = self.data.write();
        for (key, value) in items {
            data.insert(key, value);
        }
        Ok(())
    }

    async fn batch_delete(&self, keys: Vec<String>) -> Result<()> {
        let mut data = self.data.write();
        for key in keys {
            data.remove(&key);
        }
        Ok(())
    }

    async fn write_batch(&self, puts: Vec<(String, Vec<u8>)>, deletes: Vec<String>) -> Result<()> {
        if self.reject_batch.load(Ordering::Acquire) {
            return Err(ControllerError::StorageError("injected batch failure".to_string()));
        }
        let mut data = self.data.write();
        for key in deletes {
            data.remove(&key);
        }
        for (key, value) in puts {
            data.insert(key, value);
        }
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.data.read().contains_key(key))
    }

    async fn clear(&self) -> Result<()> {
        self.data.write().clear();
        Ok(())
    }

    async fn sync(&self) -> Result<()> {
        Ok(())
    }

    async fn stats(&self) -> Result<StorageStats> {
        let data = self.data.read();
        Ok(StorageStats {
            key_count: data.len(),
            total_size: data.values().map(|value| value.len() as u64).sum(),
            backend_info: "fault-injecting".to_string(),
        })
    }
}

fn test_config() -> ControllerConfigReader {
    ControllerConfigReader::new(ControllerConfig::test_config())
}

fn broker_id_entry(index: u64) -> LogEntry {
    LogEntry {
        log_id: LogId {
            leader_id: Vote::new(1, 1).leader_id,
            index,
        },
        payload: EntryPayload::Normal(ControllerRequest::ApplyBrokerId {
            cluster_name: "fault-cluster".to_string(),
            broker_name: "broker-a".to_string(),
            broker_address: "127.0.0.1:10911".to_string(),
            applied_broker_id: 1,
            register_check_code: "check-code".to_string(),
        }),
    }
}

#[tokio::test]
async fn failed_state_batch_does_not_publish_candidate_state() {
    let backend = Arc::new(FaultInjectingBackend::rejecting_batches());
    let mut state_machine = StateMachine::open(test_config(), backend.clone())
        .await
        .expect("open state machine");

    let stream = futures::stream::iter([Ok::<_, std::io::Error>((broker_id_entry(1), None))]);
    RaftStateMachine::apply(&mut state_machine, stream)
        .await
        .expect_err("injected persistence failure");
    let before_retry = state_machine
        .read_view()
        .get_next_broker_id("fault-cluster", "broker-a")
        .response()
        .and_then(|header| header.next_broker_id)
        .expect("initial broker id");
    assert_eq!(before_retry, 1, "failed persistence must not mutate live state");

    backend.reject_batch.store(false, Ordering::Release);
    let retry_stream = futures::stream::iter([Ok::<_, std::io::Error>((broker_id_entry(1), None))]);
    RaftStateMachine::apply(&mut state_machine, retry_stream)
        .await
        .expect("retry committed state");
    let after_retry = state_machine
        .read_view()
        .get_next_broker_id("fault-cluster", "broker-a")
        .response()
        .and_then(|header| header.next_broker_id)
        .expect("next broker id");
    assert_eq!(after_retry, 2);
}

#[tokio::test]
async fn failed_vote_write_does_not_publish_in_memory_vote() {
    let backend = Arc::new(FaultInjectingBackend::rejecting_batches());
    let mut log_store = LogStore::open(backend).await.expect("open log store");
    let vote = Vote::new(2, 1);

    RaftLogStorage::save_vote(&mut log_store, &vote)
        .await
        .expect_err("injected vote persistence failure");
    assert_eq!(
        RaftLogReader::read_vote(&mut log_store).await.expect("read vote"),
        None,
        "failed durable vote must not be visible in memory"
    );
}

#[tokio::test]
async fn failed_log_append_does_not_publish_in_memory_log() {
    let backend = Arc::new(FaultInjectingBackend::rejecting_batches());
    let mut log_store = LogStore::open(backend).await.expect("open log store");

    RaftLogStorage::append(&mut log_store, [broker_id_entry(1)], IOFlushed::<TypeConfig>::noop())
        .await
        .expect_err("injected log persistence failure");
    let state = RaftLogStorage::get_log_state(&mut log_store)
        .await
        .expect("read log state");
    assert_eq!(state.last_log_id, None);
}

#[tokio::test]
async fn incomplete_persisted_state_is_rejected_on_open() {
    let backend = Arc::new(FaultInjectingBackend::default());
    backend
        .put("openraft/state_machine/replicas_info_manager", b"partial")
        .await
        .expect("inject partial state");

    let error = StateMachine::open(test_config(), backend)
        .await
        .err()
        .expect("partial state must fail");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
}

#[test]
fn production_default_storage_is_rocksdb() {
    assert_eq!(ControllerConfig::default().storage_backend, StorageBackendType::RocksDB);
}

#[cfg(feature = "storage-rocksdb")]
#[tokio::test]
async fn rocksdb_batch_survives_reopen() {
    use rocketmq_controller::storage::create_storage;
    use rocketmq_controller::storage::StorageConfig;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().expect("temp directory");
    let path = temp_dir.path().join("controller-rocksdb");
    {
        let backend = create_storage(StorageConfig::RocksDB { path: path.clone() })
            .await
            .expect("open RocksDB");
        backend
            .batch_put(vec![
                ("state".to_string(), b"committed".to_vec()),
                ("index".to_string(), b"42".to_vec()),
            ])
            .await
            .expect("durable batch");
        backend.sync().await.expect("sync WAL");
    }

    let reopened = create_storage(StorageConfig::RocksDB { path })
        .await
        .expect("reopen RocksDB");
    assert_eq!(
        reopened.get("state").await.expect("read state"),
        Some(b"committed".to_vec())
    );
    assert_eq!(reopened.get("index").await.expect("read index"), Some(b"42".to_vec()));
}
