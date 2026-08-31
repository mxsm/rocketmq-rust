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

use std::collections::BTreeMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use parking_lot::Mutex;
use parking_lot::RwLock;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_error::SerializationError;
use rocketmq_runtime::common::time_utils::current_millis;
use serde::Deserialize;
use serde::Serialize;

const CHECKPOINT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TopicMetric {
    count: i64,
    timestamp: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TransactionMetricsCheckpoint {
    version: u32,
    generation: u64,
    topics: BTreeMap<String, TopicMetric>,
}

#[derive(Debug)]
struct TransactionMetricsInner {
    checkpoint_path: PathBuf,
    topics: RwLock<BTreeMap<String, TopicMetric>>,
    generation: AtomicU64,
    revision: AtomicU64,
    dirty: AtomicBool,
    recovered_from_backup: AtomicBool,
    persist_lock: Mutex<()>,
}

/// Java-compatible per-topic count of prepared transaction messages that have
/// not yet reached a terminal state.
#[derive(Debug, Clone)]
pub(crate) struct TransactionMetrics {
    inner: Arc<TransactionMetricsInner>,
}

impl TransactionMetrics {
    pub(crate) fn open(checkpoint_path: impl Into<PathBuf>) -> RocketMQResult<Self> {
        let checkpoint_path = checkpoint_path.into();
        let backup_path = backup_path(&checkpoint_path);
        let (checkpoint, recovered_from_backup) = match read_checkpoint(&checkpoint_path) {
            Ok(Some(checkpoint)) => (checkpoint, false),
            Ok(None) => match read_checkpoint(&backup_path)? {
                Some(checkpoint) => (checkpoint, true),
                None => empty_checkpoint(),
            },
            Err(primary_error) => match read_checkpoint(&backup_path) {
                Ok(Some(checkpoint)) => (checkpoint, true),
                _ => return Err(primary_error),
            },
        };

        Ok(Self {
            inner: Arc::new(TransactionMetricsInner {
                checkpoint_path,
                topics: RwLock::new(checkpoint.topics),
                generation: AtomicU64::new(checkpoint.generation),
                revision: AtomicU64::new(0),
                dirty: AtomicBool::new(false),
                recovered_from_backup: AtomicBool::new(recovered_from_backup),
                persist_lock: Mutex::new(()),
            }),
        })
    }

    pub(crate) fn add_and_get(&self, topic: &str, delta: i64) -> i64 {
        let mut topics = self.inner.topics.write();
        let metric = topics.entry(topic.to_owned()).or_insert_with(|| TopicMetric {
            count: 0,
            timestamp: current_millis(),
        });
        metric.count = metric.count.wrapping_add(delta);
        metric.timestamp = current_millis();
        let count = metric.count;
        drop(topics);
        self.inner.revision.fetch_add(1, Ordering::AcqRel);
        self.inner.dirty.store(true, Ordering::Release);
        count
    }

    pub(crate) fn count(&self, topic: &str) -> i64 {
        self.inner.topics.read().get(topic).map_or(0, |metric| metric.count)
    }

    pub(crate) fn snapshot(&self) -> Vec<(String, i64)> {
        self.inner
            .topics
            .read()
            .iter()
            .map(|(topic, metric)| (topic.clone(), metric.count))
            .collect()
    }

    pub(crate) fn persist_if_dirty(&self) -> RocketMQResult<bool> {
        if !self.inner.dirty.load(Ordering::Acquire) {
            return Ok(false);
        }
        self.persist()?;
        Ok(true)
    }

    pub(crate) fn persist(&self) -> RocketMQResult<()> {
        let _guard = self.inner.persist_lock.lock();
        let snapshot_revision = self.inner.revision.load(Ordering::Acquire);
        let generation = self.inner.generation.load(Ordering::Acquire).saturating_add(1);
        let checkpoint = TransactionMetricsCheckpoint {
            version: CHECKPOINT_VERSION,
            generation,
            topics: self.inner.topics.read().clone(),
        };
        let body =
            serde_json::to_vec(&checkpoint).map_err(|error| SerializationError::source("serialize", "JSON", error))?;
        write_checkpoint(&self.inner.checkpoint_path, &body)?;
        self.inner.generation.store(generation, Ordering::Release);
        if self.inner.revision.load(Ordering::Acquire) == snapshot_revision {
            self.inner.dirty.store(false, Ordering::Release);
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn recovered_from_backup(&self) -> bool {
        self.inner.recovered_from_backup.load(Ordering::Acquire)
    }
}

fn empty_checkpoint() -> (TransactionMetricsCheckpoint, bool) {
    (
        TransactionMetricsCheckpoint {
            version: CHECKPOINT_VERSION,
            generation: 0,
            topics: BTreeMap::new(),
        },
        false,
    )
}

fn read_checkpoint(path: &Path) -> RocketMQResult<Option<TransactionMetricsCheckpoint>> {
    let body = match fs::read(path) {
        Ok(body) => body,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let checkpoint: TransactionMetricsCheckpoint =
        serde_json::from_slice(&body).map_err(|error| SerializationError::source("deserialize", "JSON", error))?;
    if checkpoint.version != CHECKPOINT_VERSION {
        return Err(RocketMQError::ConfigInvalidValue {
            key: "transactionMetrics.version",
            value: checkpoint.version.to_string(),
            reason: format!(
                "unsupported transaction metrics checkpoint version {}",
                checkpoint.version
            ),
        });
    }
    Ok(Some(checkpoint))
}

fn write_checkpoint(path: &Path, body: &[u8]) -> RocketMQResult<()> {
    let parent = path.parent().ok_or_else(|| RocketMQError::ConfigInvalidValue {
        key: "transactionMetrics.path",
        value: path.display().to_string(),
        reason: "checkpoint path must have a parent directory".into(),
    })?;
    fs::create_dir_all(parent)?;
    let temporary_path = temporary_path(path);
    let backup_path = backup_path(path);
    let mut temporary = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary_path)?;
    temporary.write_all(body)?;
    temporary.sync_all()?;
    drop(temporary);

    if path.exists() {
        if backup_path.exists() {
            fs::remove_file(&backup_path)?;
        }
        fs::rename(path, &backup_path)?;
    }
    fs::rename(&temporary_path, path)?;
    Ok(())
}

fn backup_path(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.bak", path.display()))
}

fn temporary_path(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.tmp", path.display()))
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;
    use std::fs;

    use rocketmq_error::RocketMQError;

    use super::read_checkpoint;

    #[test]
    fn invalid_checkpoint_preserves_json_source_and_public_boundary() {
        let directory = tempfile::tempdir().expect("create temporary checkpoint directory");
        let checkpoint_path = directory.path().join("transaction-metrics.json");
        fs::write(&checkpoint_path, b"not json").expect("write invalid checkpoint");

        let error = read_checkpoint(&checkpoint_path).expect_err("invalid checkpoint must fail to decode");

        assert_eq!(error.to_string(), "deserialize failed (JSON)");
        assert_eq!(error.boundary_view().message(), "Serialization failed");
        assert!(StdError::source(&error)
            .expect("outer error must preserve the source")
            .downcast_ref::<serde_json::Error>()
            .is_some());

        let RocketMQError::Serialization(serialization) = &error else {
            panic!("invalid JSON must map to a serialization error");
        };
        assert!(StdError::source(serialization)
            .expect("serialization error must preserve the JSON source")
            .downcast_ref::<serde_json::Error>()
            .is_some());
    }
}
