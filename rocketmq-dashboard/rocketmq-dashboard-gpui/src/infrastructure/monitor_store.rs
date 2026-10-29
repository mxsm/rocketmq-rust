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

#[cfg(test)]
use std::path::PathBuf;
use std::sync::Arc;

use rocketmq_dashboard_common::{OperationActor, OperationResource};

use rocketmq_runtime::{ChildServiceContext, TaskGroup, TaskId};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::{
    config_store::{ConfigStoreFailure, DesktopConfigStore},
    environment_store::{MonitorMigrationState, StoredMonitorRule},
};

#[derive(Debug, thiserror::Error)]
#[error("Monitor id and consumer group must not be empty")]
#[cfg_attr(
    not(test),
    allow(dead_code, reason = "P01 retains Monitor write validation for the P09 rule editor")
)]
struct InvalidMonitorRule;

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

/// Monitor facade over the canonical configuration/rule/operation document.
pub struct MonitorStore {
    store: Arc<DesktopConfigStore>,
}

impl MonitorStore {
    /// Creates an isolated store without a separately composed configuration owner.
    #[cfg(test)]
    pub fn new(path: PathBuf, context: ChildServiceContext) -> Arc<Self> {
        Self::with_config_store(DesktopConfigStore::new(path.with_file_name("config.json"), context))
    }

    pub fn with_config_store(store: Arc<DesktopConfigStore>) -> Arc<Self> {
        Arc::new(Self { store })
    }

    async fn ensure_loaded(&self) -> Result<(), ConfigStoreFailure> {
        if self.store.environments().is_none() {
            self.store.load().await?;
        }
        Ok(())
    }

    /// Lists only the current environment. Incomplete legacy rules are separately recoverable.
    pub async fn list(&self) -> Result<Vec<MonitorRule>, ConfigStoreFailure> {
        self.ensure_loaded().await?;
        let environment = self.store.environment_snapshot()?.environment_id;
        Ok(self
            .store
            .monitor_rules()
            .await?
            .into_iter()
            .filter(|rule| rule.environment_id.as_ref() == Some(&environment))
            .map(|rule| rule.rule)
            .collect())
    }

    #[cfg(test)]
    pub async fn unassigned(&self) -> Result<Vec<StoredMonitorRule>, ConfigStoreFailure> {
        self.ensure_loaded().await?;
        Ok(self
            .store
            .monitor_rules()
            .await?
            .into_iter()
            .filter(|rule| rule.environment_id.is_none())
            .collect())
    }

    /// Persists a scoped rule as incomplete; P09 supplies thresholds before evaluation.
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "P01 retains durable Monitor mutation APIs for the P09 rule editor"
        )
    )]
    pub async fn upsert(&self, rule: MonitorRule) -> Result<(), ConfigStoreFailure> {
        if rule.id.trim().is_empty() || rule.consumer_group.trim().is_empty() {
            return Err(ConfigStoreFailure::validation(InvalidMonitorRule));
        }
        self.ensure_loaded().await?;
        let snapshot = self.store.environment_snapshot()?;
        let context = self.store.operation_context(
            snapshot.connection.revision,
            OperationActor::System,
            OperationResource::Monitor(rule.consumer_group.clone()),
        )?;
        let id = rule.id.clone();
        self.store
            .save_monitor(
                Some(StoredMonitorRule {
                    environment_id: Some(snapshot.environment_id),
                    rule,
                    migration_state: MonitorMigrationState::NeedsThresholds,
                }),
                id,
                context,
            )
            .await?;
        Ok(())
    }

    /// Deletes only from the current environment, with its operation record in the same commit.
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "P01 retains durable Monitor mutation APIs for the P09 rule editor"
        )
    )]
    pub async fn delete(&self, id: String) -> Result<bool, ConfigStoreFailure> {
        self.ensure_loaded().await?;
        let snapshot = self.store.environment_snapshot()?;
        let context = self.store.operation_context(
            snapshot.connection.revision,
            OperationActor::System,
            OperationResource::Monitor(id.clone()),
        )?;
        self.store.save_monitor(None, id, context).await
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
    pub fn start(context: &ChildServiceContext, enabled: bool) -> Result<Self, ConfigStoreFailure> {
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
            .map_err(ConfigStoreFailure::runtime)?;
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

#[cfg(test)]
mod tests {
    use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};

    use super::*;

    #[test]
    fn monitor_store_round_trips_without_evaluating_rules() {
        let directory = tempfile::tempdir().expect("temp directory");
        let runtime = RuntimeOwner::plan(RuntimeConfig::for_parallelism("monitor-test", 1))
            .expect("test runtime configuration is valid")
            .with_memory_limit(ProcessMemoryLimit::configured(256 * 1024 * 1024).expect("memory"))
            .build()
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
