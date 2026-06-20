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
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use rocketmq_error::RocketMQError;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::ScheduledTaskSnapshot;
use rocketmq_runtime::ShutdownReport;
use tracing::error;
use tracing::warn;

use crate::rocksdb::column_family::RocksDbColumnFamily;
use crate::rocksdb::config::RocksDbConfig;
use crate::rocksdb::store::RocksDbStore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RocksDbMaintenanceConfig {
    pub flush_interval: Option<Duration>,
    pub compaction_interval: Option<Duration>,
    pub checkpoint_interval: Option<Duration>,
    pub backup_interval: Option<Duration>,
    pub checkpoint_root: PathBuf,
    pub backup_dir: Option<PathBuf>,
    pub compaction_cf: String,
}

impl RocksDbMaintenanceConfig {
    pub fn from_rocksdb_config(config: &RocksDbConfig) -> Self {
        Self {
            flush_interval: interval_from_millis(config.flush_interval_ms),
            compaction_interval: interval_from_millis(config.compaction_interval_ms),
            checkpoint_interval: interval_from_millis(config.checkpoint_interval_ms),
            backup_interval: interval_from_millis(config.backup_interval_ms),
            checkpoint_root: default_checkpoint_root(&config.path),
            backup_dir: config.backup_dir.clone(),
            compaction_cf: RocksDbColumnFamily::Default.name().to_string(),
        }
    }

    fn enabled_operations(&self) -> usize {
        usize::from(self.flush_interval.is_some())
            + usize::from(self.compaction_interval.is_some())
            + usize::from(self.checkpoint_interval.is_some())
            + usize::from(self.backup_interval.is_some() && self.backup_dir.is_some())
    }
}

pub struct RocksDbMaintenanceService {
    store: Arc<RocksDbStore>,
    config: RocksDbMaintenanceConfig,
    worker_group: Option<rocketmq_runtime::TaskGroup>,
    scheduled_tasks: Option<ScheduledTaskGroup>,
}

impl RocksDbMaintenanceService {
    pub fn new(store: Arc<RocksDbStore>, config: RocksDbConfig) -> Self {
        Self {
            store,
            config: RocksDbMaintenanceConfig::from_rocksdb_config(&config),
            worker_group: None,
            scheduled_tasks: None,
        }
    }

    pub fn with_config(store: Arc<RocksDbStore>, config: RocksDbMaintenanceConfig) -> Self {
        Self {
            store,
            config,
            worker_group: None,
            scheduled_tasks: None,
        }
    }

    pub fn start(&mut self) {
        if self.is_running() || self.config.enabled_operations() == 0 {
            return;
        }

        let worker_group = match crate::rocksdb::runtime::task_group("rocksdb.maintenance") {
            Ok(worker_group) => worker_group,
            Err(error) => {
                error!(%error, "failed to create rocksdb maintenance task group");
                return;
            }
        };
        let scheduled_tasks = ScheduledTaskGroup::new(worker_group.child("scheduled"));

        self.schedule_enabled_operation(
            &scheduled_tasks,
            MaintenanceOperation::Flush,
            self.config.flush_interval,
        );
        self.schedule_enabled_operation(
            &scheduled_tasks,
            MaintenanceOperation::CompactDefaultCf,
            self.config.compaction_interval,
        );
        self.schedule_enabled_operation(
            &scheduled_tasks,
            MaintenanceOperation::Checkpoint,
            self.config.checkpoint_interval,
        );
        if self.config.backup_dir.is_some() {
            self.schedule_enabled_operation(
                &scheduled_tasks,
                MaintenanceOperation::Backup,
                self.config.backup_interval,
            );
        }
        self.scheduled_tasks = Some(scheduled_tasks);
        self.worker_group = Some(worker_group);
    }

    pub async fn shutdown_gracefully(&mut self) -> Result<(), RocketMQError> {
        let _ = self.shutdown_gracefully_with_report().await?;
        Ok(())
    }

    pub async fn shutdown_gracefully_with_report(&mut self) -> Result<Option<ShutdownReport>, RocketMQError> {
        self.scheduled_tasks.take();
        if let Some(worker_group) = self.worker_group.take() {
            let report = worker_group.shutdown(Duration::from_secs(5)).await;
            crate::rocksdb::runtime::shutdown_report_result("maintenance shutdown", report.clone())?;
            return Ok(Some(report));
        }
        Ok(None)
    }

    pub fn is_running(&self) -> bool {
        self.worker_group.is_some()
    }

    pub fn task_count(&self) -> usize {
        let root_count = self
            .worker_group
            .as_ref()
            .map(rocketmq_runtime::TaskGroup::task_count)
            .unwrap_or_default();
        let scheduled_count = self
            .scheduled_tasks
            .as_ref()
            .map(|scheduled_tasks| scheduled_tasks.group().task_count())
            .unwrap_or_default();
        root_count + scheduled_count
    }

    pub fn schedule_snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.scheduled_tasks
            .as_ref()
            .map(ScheduledTaskGroup::snapshot)
            .unwrap_or_default()
    }

    pub async fn run_once(&self) -> Result<(), RocketMQError> {
        if self.config.flush_interval.is_some() {
            self.store.flush()?;
        }
        if self.config.compaction_interval.is_some() {
            self.store
                .compact_range_cf_blocking(self.config.compaction_cf.clone(), None, None)
                .await?;
        }
        if self.config.checkpoint_interval.is_some() {
            let checkpoint_dir = next_checkpoint_dir(&self.config.checkpoint_root)?;
            self.store.create_checkpoint(checkpoint_dir).await?;
        }
        if self.config.backup_interval.is_some() {
            if let Some(backup_dir) = self.config.backup_dir.clone() {
                self.store.create_backup(backup_dir).await?;
            }
        }
        Ok(())
    }

    fn schedule_enabled_operation(
        &self,
        scheduled_tasks: &ScheduledTaskGroup,
        operation: MaintenanceOperation,
        interval: Option<Duration>,
    ) {
        let Some(interval) = interval else {
            return;
        };
        let store = Arc::clone(&self.store);
        let config = self.config.clone();
        let mut task_config = ScheduledTaskConfig::fixed_delay(operation.task_name(), interval);
        task_config.initial_delay = interval;
        if let Err(error) = scheduled_tasks.schedule_fixed_delay(task_config, move || {
            let store = Arc::clone(&store);
            let config = config.clone();
            async move {
                if let Err(error) = run_operation(&store, &config, operation).await {
                    warn!(error = %error, operation = ?operation, "rocksdb maintenance operation failed");
                }
            }
        }) {
            error!(%error, operation = ?operation, "failed to schedule rocksdb maintenance task");
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MaintenanceOperation {
    Flush,
    CompactDefaultCf,
    Checkpoint,
    Backup,
}

impl MaintenanceOperation {
    fn task_name(self) -> &'static str {
        match self {
            MaintenanceOperation::Flush => "rocksdb-maintenance-flush",
            MaintenanceOperation::CompactDefaultCf => "rocksdb-maintenance-compact-default-cf",
            MaintenanceOperation::Checkpoint => "rocksdb-maintenance-checkpoint",
            MaintenanceOperation::Backup => "rocksdb-maintenance-backup",
        }
    }
}

async fn run_operation(
    store: &RocksDbStore,
    config: &RocksDbMaintenanceConfig,
    operation: MaintenanceOperation,
) -> Result<(), RocketMQError> {
    match operation {
        MaintenanceOperation::Flush => store.flush(),
        MaintenanceOperation::CompactDefaultCf => {
            store
                .compact_range_cf_blocking(config.compaction_cf.clone(), None, None)
                .await
        }
        MaintenanceOperation::Checkpoint => {
            let checkpoint_dir = next_checkpoint_dir(&config.checkpoint_root)?;
            store.create_checkpoint(checkpoint_dir).await
        }
        MaintenanceOperation::Backup => match &config.backup_dir {
            Some(backup_dir) => store.create_backup(backup_dir.clone()).await,
            None => Ok(()),
        },
    }
}

fn interval_from_millis(interval_ms: usize) -> Option<Duration> {
    if interval_ms == 0 {
        None
    } else {
        Some(Duration::from_millis(interval_ms as u64))
    }
}

fn default_checkpoint_root(db_path: &std::path::Path) -> PathBuf {
    let db_name = db_path
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("rocksdb");
    db_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join(format!("{db_name}-checkpoints"))
}

fn next_checkpoint_dir(checkpoint_root: &std::path::Path) -> Result<PathBuf, RocketMQError> {
    std::fs::create_dir_all(checkpoint_root).map_err(|error| {
        RocketMQError::storage_write_failed("rocksdb", format!("Checkpoint: create root directory: {error}"))
    })?;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| RocketMQError::storage_write_failed("rocksdb", format!("Checkpoint: system time: {error}")))?
        .as_nanos();
    for suffix in 0..100_u32 {
        let checkpoint_dir = checkpoint_root.join(format!("{nanos}-{suffix}"));
        if !checkpoint_dir.exists() {
            return Ok(checkpoint_dir);
        }
    }
    Err(RocketMQError::storage_write_failed(
        "rocksdb",
        "Checkpoint: failed to allocate unique checkpoint directory",
    ))
}
