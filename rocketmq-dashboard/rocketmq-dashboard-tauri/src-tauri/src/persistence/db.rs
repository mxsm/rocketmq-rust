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

use super::schema;
use super::types::StorageHealth;
use super::types::StorageStats;
use crate::error::DashboardError;
use crate::error::DashboardResult;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::service_context::ChildServiceContext;
use rusqlite::Connection;
use std::ffi::OsString;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tauri::Manager;
use tokio::sync::oneshot;

pub(crate) fn resolve_data_path(app: &tauri::AppHandle) -> DashboardResult<PathBuf> {
    data_path(std::env::var_os("DASHBOARD_TAURI_DATA_DIR"), || {
        Ok(app.path().app_config_dir()?)
    })
}

fn data_path(
    configured: Option<OsString>,
    default_directory: impl FnOnce() -> DashboardResult<PathBuf>,
) -> DashboardResult<PathBuf> {
    let directory = match configured {
        Some(value) if value.is_empty() => {
            return Err(DashboardError::Configuration("empty data directory".into()));
        }
        Some(value) => PathBuf::from(value),
        None => default_directory()?.join("data"),
    };
    Ok(directory.join("dashboard.db"))
}

/// Called only at a blocking I/O boundary; connections remain inside that closure.
pub(crate) fn open_connection(path: &Path) -> DashboardResult<Connection> {
    if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    let connection = Connection::open(path)?;
    connection.busy_timeout(Duration::from_secs(5))?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    Ok(connection)
}

#[derive(Clone)]
pub(crate) struct StorageManager {
    path: Arc<PathBuf>,
    background: ChildServiceContext,
    operations: ChildServiceContext,
    accepting: Arc<Mutex<bool>>,
    stats: Arc<StorageStats>,
}

/// Ownership travels into the actual blocking closure, including after an executor
/// timeout. Shutdown must not mistake a timed-out waiter for completed disk I/O.
struct ActiveOperation(Arc<StorageStats>);

impl Drop for ActiveOperation {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
        self.0.drained.notify_waiters();
    }
}

impl StorageManager {
    pub(crate) fn new(path: PathBuf, context: ChildServiceContext) -> Self {
        Self {
            path: Arc::new(path),
            background: context.component("background"),
            operations: context.component("operations"),
            accepting: Arc::new(Mutex::new(true)),
            stats: Arc::new(StorageStats::default()),
        }
    }

    pub(crate) fn path(&self) -> &Path {
        self.path.as_ref()
    }

    pub(crate) fn health(&self) -> StorageHealth {
        self.stats.snapshot()
    }

    /// Background waiters may be cancelled; accepted I/O remains owned by `run`.
    pub(crate) fn start_background(
        &self,
        name: &'static str,
        future: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> DashboardResult<()> {
        let accepting = self.accepting.lock().map_err(|_| DashboardError::StorageClosed)?;
        if !*accepting {
            return Err(DashboardError::StorageClosed);
        }
        self.background
            .spawn_cancellable_service(name, future)
            .map(|_| ())
            .map_err(DashboardError::Persistence)
    }

    /// Accepted work is owned independently of the IPC waiter. Dropping that
    /// waiter does not cancel a transaction that may already have committed.
    pub(crate) async fn run<T, F>(&self, name: &'static str, operation: F) -> DashboardResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> DashboardResult<T> + Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        {
            let accepting = self.accepting.lock().map_err(|_| DashboardError::StorageClosed)?;
            if !*accepting {
                return Err(DashboardError::StorageClosed);
            }
            let path = self.path.clone();
            let executor = self.operations.storage_io().clone();
            let stats = self.stats.clone();
            stats.active.fetch_add(1, Ordering::AcqRel);
            let guard = ActiveOperation(stats.clone());
            self.operations
                .spawn_service(name, async move {
                    let result = executor
                        .spawn_io(name, move || {
                            let _guard = guard;
                            let result = open_connection(&path).and_then(|mut connection| operation(&mut connection));
                            if result.is_ok() {
                                stats.completed.fetch_add(1, Ordering::Relaxed);
                            } else {
                                stats.failed.fetch_add(1, Ordering::Relaxed);
                            }
                            result
                        })
                        .await
                        .map_err(DashboardError::Persistence)
                        .and_then(|result| result);
                    let _ = sender.send(result);
                })
                .map_err(DashboardError::Persistence)?;
        }
        receiver.await.map_err(|_| DashboardError::StorageClosed)?
    }

    pub(crate) async fn initialize(&self) -> DashboardResult<()> {
        self.run("storage-initialize", schema::initialize).await
    }

    pub(crate) async fn shutdown(&self, timeout: Duration) -> bool {
        // Admission and registration share this lock, so no accepted operation
        // can appear after shutdown begins draining the owned task groups.
        match self.accepting.lock() {
            Ok(mut accepting) => *accepting = false,
            Err(_) => return false,
        }
        let deadline = ShutdownDeadline::after(timeout);
        let background = self.background.task_group().shutdown_until(deadline).await;
        let operations = self.operations.task_group().shutdown_until(deadline).await;
        let drained = tokio::time::timeout_at(deadline.instant().into(), async {
            loop {
                let notified = self.stats.drained.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.stats.active.load(Ordering::Acquire) == 0 {
                    break;
                }
                notified.await;
            }
        })
        .await
        .is_ok();
        background.is_healthy() && operations.is_healthy() && drained
    }
}

#[cfg(test)]
mod tests;
