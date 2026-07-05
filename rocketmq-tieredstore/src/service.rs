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

pub mod cleanup_service;
pub mod commit_log_recover_service;

use std::sync::Arc;

use rocketmq_error::RocketMQError;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::ScheduledTaskSnapshot;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use tokio_util::sync::CancellationToken;

use crate::config::TieredStoreConfig;
use crate::file::TieredFlatFileStore;
use crate::provider::TieredStoreProvider;
use crate::runtime;
use crate::service::cleanup_service::cleanup_worker_result;
use crate::service::cleanup_service::CleanupErrorSlot;
use crate::service::cleanup_service::CleanupService;
pub use crate::service::commit_log_recover_service::CommitLogRecoverService;
pub use crate::service::commit_log_recover_service::TieredRecoverResult;

pub struct TieredServiceSet<P>
where
    P: TieredStoreProvider,
{
    cleanup_group: tokio::sync::Mutex<Option<rocketmq_runtime::TaskGroup>>,
    cleanup_schedule: tokio::sync::Mutex<Option<ScheduledTaskGroup>>,
    cleanup_shutdown: tokio::sync::Mutex<Option<CancellationToken>>,
    cleanup_error: CleanupErrorSlot,
    parent_task_group: Option<TaskGroup>,
    _marker: std::marker::PhantomData<P>,
}

impl<P> TieredServiceSet<P>
where
    P: TieredStoreProvider,
{
    pub fn new() -> Self {
        Self::new_with_optional_task_group(None)
    }

    pub fn new_with_task_group(parent_task_group: TaskGroup) -> Self {
        Self::new_with_optional_task_group(Some(parent_task_group))
    }

    fn new_with_optional_task_group(parent_task_group: Option<TaskGroup>) -> Self {
        Self {
            cleanup_group: tokio::sync::Mutex::new(None),
            cleanup_schedule: tokio::sync::Mutex::new(None),
            cleanup_shutdown: tokio::sync::Mutex::new(None),
            cleanup_error: Arc::new(tokio::sync::Mutex::new(None)),
            parent_task_group,
            _marker: std::marker::PhantomData,
        }
    }

    pub async fn start_cleanup(
        &self,
        config: Arc<TieredStoreConfig>,
        flat_file_store: Arc<TieredFlatFileStore<P>>,
        shutdown: CancellationToken,
    ) -> Result<(), RocketMQError> {
        if !config.delete_file_enable {
            return Ok(());
        }
        if self.cleanup_group.lock().await.is_some() {
            return Ok(());
        }
        let service = CleanupService::new(config, flat_file_store, shutdown);
        let cleanup_shutdown = service.shutdown_token();
        let task_group = match self.parent_task_group.as_ref() {
            Some(parent_task_group) => {
                runtime::task_group_with_parent("rocketmq-tieredstore.cleanup", parent_task_group)
            }
            None => runtime::task_group("rocketmq-tieredstore.cleanup")?,
        };
        let scheduled_tasks = service.schedule(&task_group, self.cleanup_error.clone())?;
        *self.cleanup_shutdown.lock().await = Some(cleanup_shutdown);
        *self.cleanup_schedule.lock().await = Some(scheduled_tasks);
        *self.cleanup_group.lock().await = Some(task_group);
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), RocketMQError> {
        let _ = self.shutdown_with_report().await?;
        Ok(())
    }

    pub async fn shutdown_with_report(&self) -> Result<Option<ShutdownReport>, RocketMQError> {
        if let Some(shutdown) = self.cleanup_shutdown.lock().await.take() {
            shutdown.cancel();
        }
        self.cleanup_schedule.lock().await.take();
        let mut shutdown_report = None;
        if let Some(task_group) = self.cleanup_group.lock().await.take() {
            let report = task_group.shutdown(std::time::Duration::from_secs(5)).await;
            runtime::shutdown_report_result("tieredstore cleanup", report.clone())?;
            shutdown_report = Some(report);
        }
        let cleanup_error = self.cleanup_error.lock().await.take();
        cleanup_worker_result(cleanup_error)?;
        Ok(shutdown_report)
    }

    pub async fn task_count(&self) -> usize {
        let root_task_count = self
            .cleanup_group
            .lock()
            .await
            .as_ref()
            .map(rocketmq_runtime::TaskGroup::task_count)
            .unwrap_or_default();
        let scheduled_task_count = self
            .cleanup_schedule
            .lock()
            .await
            .as_ref()
            .map(|scheduled_tasks| scheduled_tasks.group().task_count())
            .unwrap_or_default();
        root_task_count + scheduled_task_count
    }

    pub async fn cleanup_schedule_snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.cleanup_schedule
            .lock()
            .await
            .as_ref()
            .map(ScheduledTaskGroup::snapshot)
            .unwrap_or_default()
    }
}

impl<P> Default for TieredServiceSet<P>
where
    P: TieredStoreProvider,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use rocketmq_error::RocketMQError;
    use rocketmq_runtime::RuntimeContext;

    use super::*;
    use crate::metadata::JsonMetadataStore;
    use crate::provider::MemoryProvider;

    #[tokio::test]
    async fn new_with_task_group_parents_cleanup_tasks() -> Result<(), RocketMQError> {
        let temp_dir = tempfile::tempdir().map_err(|err| RocketMQError::Internal(err.to_string()))?;
        let config = Arc::new(TieredStoreConfig {
            store_path_root_dir: temp_dir.path().to_path_buf(),
            backend_provider: "memory".to_owned(),
            delete_file_interval: Duration::from_millis(100),
            ..TieredStoreConfig::default()
        });
        let flat_file_store = Arc::new(TieredFlatFileStore::new(
            config.clone(),
            Arc::new(JsonMetadataStore::new(config.clone())),
            MemoryProvider::default(),
        ));
        let context = RuntimeContext::from_current("tieredstore-cleanup-parent-test");
        let service = context.service_context("tieredstore-cleanup");
        let services = TieredServiceSet::<MemoryProvider>::new_with_task_group(service.task_group().clone());

        services
            .start_cleanup(config, flat_file_store, CancellationToken::new())
            .await?;
        services.shutdown().await?;

        let report = service.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(
            report
                .children
                .iter()
                .any(|child| child.name == "rocketmq-tieredstore.cleanup"),
            "{}",
            report.to_json()
        );
        assert!(report.is_healthy(), "{}", report.to_json());
        Ok(())
    }
}
