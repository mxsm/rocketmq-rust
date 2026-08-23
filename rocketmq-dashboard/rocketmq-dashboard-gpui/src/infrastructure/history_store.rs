// Copyright 2026 The RocketMQ Rust Authors
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

//! History persistence and lifecycle foundation. Delivery 02 records no metrics.

use std::{io, path::PathBuf, sync::Arc};

use rocketmq_runtime::{ChildServiceContext, TaskGroup, TaskId};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::config_store::{ConfigStoreError, write_json_atomically};

/// Non-metric History record used to prove durable CRUD before collection exists.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct HistoryRecord {
    /// Stable record identity.
    pub id: String,
    /// Safe lifecycle note; it is not an inferred operational metric.
    pub note: String,
}

/// JSON-backed History CRUD store.
pub struct HistoryStore {
    path: PathBuf,
    context: ChildServiceContext,
    gate: tokio::sync::Mutex<()>,
}

impl HistoryStore {
    /// Creates a store at an application-owned path.
    pub fn new(path: PathBuf, context: ChildServiceContext) -> Arc<Self> {
        Arc::new(Self {
            path,
            context,
            gate: tokio::sync::Mutex::new(()),
        })
    }

    /// Lists records in stable identifier order.
    pub async fn list(&self) -> Result<Vec<HistoryRecord>, ConfigStoreError> {
        let _guard = self.gate.lock().await;
        let path = self.path.clone();
        self.context
            .storage_io()
            .spawn_io("gpui-history-list", move || load_records(&path))
            .await
            .map_err(|error| ConfigStoreError::Runtime(error.to_string()))?
    }

    /// Inserts or replaces a record by identity.
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "Delivery 02 exposes durable CRUD before a History producer exists"
        )
    )]
    pub async fn upsert(&self, record: HistoryRecord) -> Result<(), ConfigStoreError> {
        let _guard = self.gate.lock().await;
        let path = self.path.clone();
        self.context
            .storage_io()
            .spawn_io("gpui-history-upsert", move || {
                let mut records = load_records(&path)?;
                records.retain(|existing| existing.id != record.id);
                records.push(record);
                records.sort_by(|left, right| left.id.cmp(&right.id));
                write_json_atomically(&path, &records)
            })
            .await
            .map_err(|error| ConfigStoreError::Runtime(error.to_string()))?
    }

    /// Deletes a record and reports whether it existed.
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "Delivery 02 exposes durable CRUD before a History producer exists"
        )
    )]
    pub async fn delete(&self, id: String) -> Result<bool, ConfigStoreError> {
        let _guard = self.gate.lock().await;
        let path = self.path.clone();
        self.context
            .storage_io()
            .spawn_io("gpui-history-delete", move || {
                let mut records = load_records(&path)?;
                let old_len = records.len();
                records.retain(|record| record.id != id);
                let removed = records.len() != old_len;
                if removed {
                    write_json_atomically(&path, &records)?;
                }
                Ok(removed)
            })
            .await
            .map_err(|error| ConfigStoreError::Runtime(error.to_string()))?
    }
}

/// Owned empty collector lifecycle; no sampling is performed in Delivery 02.
pub struct HistoryLifecycle {
    task_id: Option<TaskId>,
    cancellation: Option<CancellationToken>,
    owner: TaskGroup,
}

impl HistoryLifecycle {
    /// Starts one owner-cancellable empty collector when enabled.
    pub fn start(context: &ChildServiceContext, enabled: bool) -> Result<Self, ConfigStoreError> {
        if !enabled {
            return Ok(Self {
                task_id: None,
                cancellation: None,
                owner: context.task_group().clone(),
            });
        }
        let cancellation = context.task_spawner().cancellation_token().child_token();
        let task_cancellation = cancellation.clone();
        let task_id = context
            .spawn_service("gpui-history-lifecycle", async move {
                task_cancellation.cancelled().await;
            })
            .map_err(|error| ConfigStoreError::Runtime(error.to_string()))?;
        Ok(Self {
            task_id: Some(task_id),
            cancellation: Some(cancellation),
            owner: context.task_group().clone(),
        })
    }

    /// Returns whether the owned lifecycle task was started.
    pub fn is_started(&self) -> bool {
        self.task_id.is_some()
    }

    /// Cancels and awaits the owned empty collector without cancelling sibling services.
    pub async fn stop(&mut self) -> bool {
        if let Some(cancellation) = self.cancellation.take() {
            cancellation.cancel();
        }
        let Some(task_id) = self.task_id.take() else {
            return true;
        };
        self.owner.wait_task(task_id, std::time::Duration::from_secs(5)).await
    }
}

impl Drop for HistoryLifecycle {
    fn drop(&mut self) {
        if let Some(cancellation) = self.cancellation.take() {
            cancellation.cancel();
        }
    }
}

fn load_records(path: &PathBuf) -> Result<Vec<HistoryRecord>, ConfigStoreError> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(ConfigStoreError::Io {
                operation: "read history",
                path: path.clone(),
                source,
            });
        }
    };
    serde_json::from_slice(&bytes).map_err(|error| ConfigStoreError::InvalidDocument {
        path: path.clone(),
        summary: format!(
            "{:?} at line {}, column {}",
            error.classify(),
            error.line(),
            error.column()
        ),
    })
}

#[cfg(test)]
mod tests {
    use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};

    use super::*;

    #[test]
    fn history_store_round_trips_and_owned_lifecycle_starts_without_collecting() {
        let directory = tempfile::tempdir().expect("temp directory");
        let runtime = RuntimeOwner::new_with_memory_limit(
            RuntimeConfig::for_parallelism("history-test", 1),
            ProcessMemoryLimit::configured(256 * 1024 * 1024).expect("memory"),
        )
        .expect("runtime");
        let context = runtime.root_context().component("history");
        let store = HistoryStore::new(directory.path().join("history.json"), context.clone());
        let mut lifecycle = HistoryLifecycle::start(&context, true).expect("lifecycle");
        assert!(lifecycle.is_started());
        runtime.block_on(async {
            assert!(store.list().await.expect("empty").is_empty());
            store
                .upsert(HistoryRecord {
                    id: "startup".into(),
                    note: "warming up".into(),
                })
                .await
                .expect("upsert");
            assert_eq!(store.list().await.expect("list").len(), 1);
            assert!(store.delete("startup".into()).await.expect("delete"));
            assert!(store.list().await.expect("empty again").is_empty());
            assert!(lifecycle.stop().await);
            assert!(!lifecycle.is_started());
            lifecycle = HistoryLifecycle::start(&context, true).expect("restart lifecycle");
            assert!(lifecycle.is_started());
        });
        runtime.shutdown_runtime_blocking().expect("owned shutdown");
    }
}
