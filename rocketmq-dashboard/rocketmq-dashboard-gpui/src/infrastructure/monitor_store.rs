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

//! Monitor CRUD and lifecycle foundation without metric evaluation or UI.

use std::{io, path::PathBuf, sync::Arc};

use rocketmq_runtime::{ChildServiceContext, TaskGroup, TaskId};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::config_store::{ConfigStoreError, write_json_atomically};

/// Persisted Monitor rule contract for later Delivery 07 evaluation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MonitorRule {
    /// Stable rule identity.
    pub id: String,
    /// Consumer group selected by the operator.
    pub consumer_group: String,
    /// Whether future evaluation should admit the rule.
    pub enabled: bool,
}

/// JSON-backed Monitor CRUD store.
pub struct MonitorStore {
    path: PathBuf,
    context: ChildServiceContext,
    gate: tokio::sync::Mutex<()>,
}

impl MonitorStore {
    /// Creates a store at an application-owned path.
    pub fn new(path: PathBuf, context: ChildServiceContext) -> Arc<Self> {
        Arc::new(Self {
            path,
            context,
            gate: tokio::sync::Mutex::new(()),
        })
    }

    /// Lists rules in stable identifier order.
    pub async fn list(&self) -> Result<Vec<MonitorRule>, ConfigStoreError> {
        let _guard = self.gate.lock().await;
        let path = self.path.clone();
        self.context
            .storage_io()
            .spawn_io("gpui-monitor-list", move || load_rules(&path))
            .await
            .map_err(|error| ConfigStoreError::Runtime(error.to_string()))?
    }

    /// Inserts or replaces a rule by identity.
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "Delivery 02 exposes durable CRUD before the Monitor page exists"
        )
    )]
    pub async fn upsert(&self, rule: MonitorRule) -> Result<(), ConfigStoreError> {
        if rule.id.trim().is_empty() || rule.consumer_group.trim().is_empty() {
            return Err(ConfigStoreError::Validation(
                "Monitor id and consumer group must not be empty".into(),
            ));
        }
        let _guard = self.gate.lock().await;
        let path = self.path.clone();
        self.context
            .storage_io()
            .spawn_io("gpui-monitor-upsert", move || {
                let mut rules = load_rules(&path)?;
                rules.retain(|existing| existing.id != rule.id);
                rules.push(rule);
                rules.sort_by(|left, right| left.id.cmp(&right.id));
                write_json_atomically(&path, &rules)
            })
            .await
            .map_err(|error| ConfigStoreError::Runtime(error.to_string()))?
    }

    /// Deletes a rule and reports whether it existed.
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "Delivery 02 exposes durable CRUD before the Monitor page exists"
        )
    )]
    pub async fn delete(&self, id: String) -> Result<bool, ConfigStoreError> {
        let _guard = self.gate.lock().await;
        let path = self.path.clone();
        self.context
            .storage_io()
            .spawn_io("gpui-monitor-delete", move || {
                let mut rules = load_rules(&path)?;
                let old_len = rules.len();
                rules.retain(|rule| rule.id != id);
                let removed = rules.len() != old_len;
                if removed {
                    write_json_atomically(&path, &rules)?;
                }
                Ok(removed)
            })
            .await
            .map_err(|error| ConfigStoreError::Runtime(error.to_string()))?
    }
}

/// Owned no-op evaluation lifecycle for the later Monitor delivery.
pub struct MonitorLifecycle {
    task_id: Option<TaskId>,
    cancellation: Option<CancellationToken>,
    owner: TaskGroup,
}

impl MonitorLifecycle {
    /// Starts an owner-cancellable no-op service when enabled.
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
            .spawn_service("gpui-monitor-lifecycle", async move {
                task_cancellation.cancelled().await;
            })
            .map_err(|error| ConfigStoreError::Runtime(error.to_string()))?;
        Ok(Self {
            task_id: Some(task_id),
            cancellation: Some(cancellation),
            owner: context.task_group().clone(),
        })
    }

    /// Returns whether the no-op lifecycle service is owned.
    pub fn is_started(&self) -> bool {
        self.task_id.is_some()
    }

    /// Cancels and awaits the owned no-op evaluator without cancelling sibling services.
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

impl Drop for MonitorLifecycle {
    fn drop(&mut self) {
        if let Some(cancellation) = self.cancellation.take() {
            cancellation.cancel();
        }
    }
}

fn load_rules(path: &PathBuf) -> Result<Vec<MonitorRule>, ConfigStoreError> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(ConfigStoreError::Io {
                operation: "read monitors",
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
    fn monitor_store_round_trips_without_evaluating_rules() {
        let directory = tempfile::tempdir().expect("temp directory");
        let runtime = RuntimeOwner::new_with_memory_limit(
            RuntimeConfig::for_parallelism("monitor-test", 1),
            ProcessMemoryLimit::configured(256 * 1024 * 1024).expect("memory"),
        )
        .expect("runtime");
        let context = runtime.root_context().component("monitor");
        let store = MonitorStore::new(directory.path().join("monitors.json"), context.clone());
        let mut lifecycle = MonitorLifecycle::start(&context, true).expect("lifecycle");
        assert!(lifecycle.is_started());
        runtime.block_on(async {
            store
                .upsert(MonitorRule {
                    id: "payments".into(),
                    consumer_group: "payments-consumer".into(),
                    enabled: true,
                })
                .await
                .expect("upsert");
            assert_eq!(store.list().await.expect("list").len(), 1);
            assert!(store.delete("payments".into()).await.expect("delete"));
            assert!(store.list().await.expect("empty").is_empty());
            assert!(lifecycle.stop().await);
            assert!(!lifecycle.is_started());
            lifecycle = MonitorLifecycle::start(&context, true).expect("restart lifecycle");
            assert!(lifecycle.is_started());
        });
        runtime.shutdown_runtime_blocking().expect("owned shutdown");
    }
}
