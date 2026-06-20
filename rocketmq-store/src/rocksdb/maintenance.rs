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
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
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
    shutdown_token: CancellationToken,
    worker_group: Option<rocketmq_runtime::TaskGroup>,
}

impl RocksDbMaintenanceService {
    pub fn new(store: Arc<RocksDbStore>, config: RocksDbConfig) -> Self {
        Self {
            store,
            config: RocksDbMaintenanceConfig::from_rocksdb_config(&config),
            shutdown_token: CancellationToken::new(),
            worker_group: None,
        }
    }

    pub fn with_config(store: Arc<RocksDbStore>, config: RocksDbMaintenanceConfig) -> Self {
        Self {
            store,
            config,
            shutdown_token: CancellationToken::new(),
            worker_group: None,
        }
    }

    pub fn start(&mut self) {
        if self.is_running() || self.config.enabled_operations() == 0 {
            return;
        }

        self.shutdown_token = CancellationToken::new();
        let worker_group = match crate::rocksdb::runtime::task_group("rocksdb.maintenance") {
            Ok(worker_group) => worker_group,
            Err(error) => {
                error!(%error, "failed to create rocksdb maintenance task group");
                return;
            }
        };

        self.spawn_enabled_operation(&worker_group, MaintenanceOperation::Flush, self.config.flush_interval);
        self.spawn_enabled_operation(
            &worker_group,
            MaintenanceOperation::CompactDefaultCf,
            self.config.compaction_interval,
        );
        self.spawn_enabled_operation(
            &worker_group,
            MaintenanceOperation::Checkpoint,
            self.config.checkpoint_interval,
        );
        if self.config.backup_dir.is_some() {
            self.spawn_enabled_operation(&worker_group, MaintenanceOperation::Backup, self.config.backup_interval);
        }
        self.worker_group = Some(worker_group);
    }

    pub async fn shutdown_gracefully(&mut self) -> Result<(), RocketMQError> {
        self.shutdown_token.cancel();
        if let Some(worker_group) = self.worker_group.take() {
            let report = worker_group.shutdown(Duration::from_secs(5)).await;
            crate::rocksdb::runtime::shutdown_report_result("maintenance shutdown", report)?;
        }
        Ok(())
    }

    pub fn is_running(&self) -> bool {
        self.worker_group.is_some()
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

    fn spawn_enabled_operation(
        &self,
        worker_group: &rocketmq_runtime::TaskGroup,
        operation: MaintenanceOperation,
        interval: Option<Duration>,
    ) {
        let Some(interval) = interval else {
            return;
        };
        let store = Arc::clone(&self.store);
        let config = self.config.clone();
        let shutdown = self.shutdown_token.clone();
        if let Err(error) = worker_group.spawn_service(operation.task_name(), async move {
            run_operation_loop(store, config, operation, interval, shutdown).await;
        }) {
            error!(%error, operation = ?operation, "failed to spawn rocksdb maintenance task");
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

async fn run_operation_loop(
    store: Arc<RocksDbStore>,
    config: RocksDbMaintenanceConfig,
    operation: MaintenanceOperation,
    interval: Duration,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = ticker.tick() => {
                if let Err(error) = run_operation(&store, &config, operation).await {
                    warn!(error = %error, operation = ?operation, "rocksdb maintenance operation failed");
                }
            }
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
