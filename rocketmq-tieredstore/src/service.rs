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
use tokio_util::sync::CancellationToken;

use crate::config::TieredStoreConfig;
use crate::file::TieredFlatFileStore;
use crate::provider::TieredStoreProvider;
use crate::runtime;
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
    cleanup_error: Arc<tokio::sync::Mutex<Option<String>>>,
    _marker: std::marker::PhantomData<P>,
}

impl<P> TieredServiceSet<P>
where
    P: TieredStoreProvider,
{
    pub fn new() -> Self {
        Self {
            cleanup_group: tokio::sync::Mutex::new(None),
            cleanup_schedule: tokio::sync::Mutex::new(None),
            cleanup_shutdown: tokio::sync::Mutex::new(None),
            cleanup_error: Arc::new(tokio::sync::Mutex::new(None)),
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
        let task_group = runtime::task_group("rocketmq-tieredstore.cleanup")?;
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
        if let Some(error) = self.cleanup_error.lock().await.take() {
            return Err(RocketMQError::Internal(error));
        }
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
