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
pub mod audit_repository;
pub mod backend;
pub mod environment_repository;
pub mod error;
pub mod file_store;
pub mod history_repository;
pub mod lease_repository;
pub mod migration;
pub mod monitor_repository;
pub mod session_repository;
pub mod sql_store;

#[cfg(test)]
mod contract_tests;

#[cfg(test)]
#[path = "persistence/history_capacity_tests.rs"]
mod history_capacity_tests;

#[cfg(test)]
#[path = "persistence/session_audit_docker_tests.rs"]
mod session_audit_docker_tests;

use crate::config::StorageConfig;
use crate::model::StorageBackend;
use crate::persistence::backend::PersistenceBackend;
use crate::persistence::error::PersistenceError;
use rocketmq_runtime::ChildServiceContext;
use serde::Deserialize;
use serde::Serialize;
use std::env;
use std::path::Path;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum StorageMode {
    SingleNode,
    MultiNode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum StorageStatus {
    Available,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageHealth {
    pub backend: StorageBackend,
    pub mode: StorageMode,
    pub status: StorageStatus,
    pub schema_version: Option<i64>,
    pub last_successful_write_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub available_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool_size: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idle_connections: Option<usize>,
}

/// An opaque identifier owned by the dashboard persistence boundary.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StorageId(pub String);

/// A monotonic revision assigned to a persisted value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Revision(pub u64);

/// A bounded page request shared by future persistence repositories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PageRequest {
    pub page: u32,
    pub page_size: u32,
}

impl Default for PageRequest {
    fn default() -> Self {
        Self { page: 1, page_size: 50 }
    }
}

/// An inclusive UTC epoch-millisecond range used by persisted history queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TimeRange {
    pub start_ms: i64,
    pub end_ms: i64,
}

impl TimeRange {
    /// Returns whether the range is ordered and can be used for a query.
    pub const fn is_valid(self) -> bool {
        self.start_ms <= self.end_ms
    }
}

/// The externally observable result of a repository transaction boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TransactionOutcome {
    Committed,
    RolledBack,
}

/// Shared persistence entry point. Future repositories are added here rather
/// than allowing services to open files or database connections directly.
pub struct DashboardPersistence {
    backend: PersistenceBackend,
}

impl DashboardPersistence {
    pub async fn initialize(
        config: &StorageConfig,
        service_context: ChildServiceContext,
    ) -> Result<Self, PersistenceError> {
        reject_legacy_interim_paths(config)?;
        config
            .validate()
            .map_err(|error| PersistenceError::InvalidConfig(error.to_string()))?;
        let backend = match config.backend {
            StorageBackend::File => PersistenceBackend::File(Box::new(
                file_store::FilePersistence::initialize(config, service_context).await?,
            )),
            StorageBackend::Sqlite | StorageBackend::MySql | StorageBackend::Postgres => {
                PersistenceBackend::Sql(sql_store::SqlPersistence::initialize(config, service_context).await?)
            }
        };
        let persistence = Self { backend };
        // Migrations cannot repair an ambiguous historical default identity.
        // Validate every decoded aggregate before the backend is exposed so a
        // restart fails closed instead of serving a pre-existing mismatch.
        persistence.validate_persisted_environments().await?;
        Ok(persistence)
    }

    pub const fn storage_backend(&self) -> StorageBackend {
        self.backend.storage_backend()
    }

    pub async fn storage_health(&self) -> StorageHealth {
        match &self.backend {
            PersistenceBackend::File(store) => store.storage_health().await,
            PersistenceBackend::Sql(store) => store.storage_health().await,
        }
    }

    async fn validate_persisted_environments(&self) -> Result<(), PersistenceError> {
        let _environments = self.list_environments().await?;
        Ok(())
    }
}

const LEGACY_INTERIM_CONFIG_PATH_ENV: &str = "DASHBOARD_WEB_INTERIM_CONFIG_PATH";
const LEGACY_MONITOR_STORE_PATH_ENV: &str = "DASHBOARD_WEB_MONITOR_STORE_PATH";

/// Rejects pre-repository files before selecting any backend. The obsolete
/// environment variables are read only as fail-closed detection inputs; their
/// contents are never loaded, migrated, or used as a fallback.
fn reject_legacy_interim_paths(config: &StorageConfig) -> Result<(), PersistenceError> {
    reject_legacy_paths(config, &legacy_override_paths())
}

fn reject_legacy_paths(config: &StorageConfig, overrides: &[PathBuf]) -> Result<(), PersistenceError> {
    if legacy_interim_paths(config)
        .into_iter()
        .chain(overrides.iter().cloned())
        .any(|path| path.exists())
    {
        return Err(PersistenceError::UnsupportedLayout);
    }
    Ok(())
}

fn legacy_override_paths() -> Vec<PathBuf> {
    [LEGACY_INTERIM_CONFIG_PATH_ENV, LEGACY_MONITOR_STORE_PATH_ENV]
        .into_iter()
        .filter_map(env::var_os)
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .collect()
}

fn legacy_interim_paths(config: &StorageConfig) -> Vec<PathBuf> {
    let mut paths = vec![
        PathBuf::from("data/dashboard-interim-config.json"),
        PathBuf::from("data/monitor/consumer-monitor-config.json"),
    ];
    let storage_root = storage_root_for_legacy_check(config);
    if let Some(root) = storage_root {
        // The deployed SQLite image used a database file directly under this
        // directory, so the former monitor file is its sibling rather than a
        // child of data/dashboard.
        paths.extend([
            root.join("dashboard-interim-config.json"),
            root.join("consumer-monitor-config.json"),
        ]);
        if let Some(data) = root.parent().filter(|data| {
            root.file_name().is_some_and(|name| name == "dashboard")
                && data.file_name().is_some_and(|name| name == "data")
        }) {
            paths.extend([
                data.join("dashboard-interim-config.json"),
                data.join("monitor/consumer-monitor-config.json"),
            ]);
        }
    }
    paths
}

fn storage_root_for_legacy_check(config: &StorageConfig) -> Option<&Path> {
    match config.backend {
        StorageBackend::Sqlite => config.data_path.parent(),
        StorageBackend::File | StorageBackend::MySql | StorageBackend::Postgres => Some(config.data_path.as_path()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SqlPoolConfig;
    use rocketmq_runtime::RuntimeConfig;
    use rocketmq_runtime::RuntimeOwner;

    #[test]
    fn every_storage_backend_rejects_documented_interim_paths_before_initialization() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let data = directory.path().join("data");
        std::fs::create_dir_all(data.join("monitor")).expect("former monitor directory");
        std::fs::write(data.join("dashboard-interim-config.json"), b"{}").expect("former config");
        std::fs::write(data.join("monitor/consumer-monitor-config.json"), b"{}").expect("former monitors");
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        owner.block_on(async {
            for backend in [
                StorageBackend::File,
                StorageBackend::Sqlite,
                StorageBackend::MySql,
                StorageBackend::Postgres,
            ] {
                let data_path = match backend {
                    StorageBackend::Sqlite => data.join("dashboard/dashboard.db"),
                    StorageBackend::File | StorageBackend::MySql | StorageBackend::Postgres => data.join("dashboard"),
                };
                let result = DashboardPersistence::initialize(
                    &StorageConfig {
                        backend,
                        data_path,
                        database_url: None,
                        pool: SqlPoolConfig::default(),
                    },
                    owner.root_context().component(format!("legacy-{backend:?}")),
                )
                .await;
                assert!(matches!(result, Err(PersistenceError::UnsupportedLayout)));
            }
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn every_storage_backend_rejects_deployed_and_custom_legacy_paths_before_initialization() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let deployed_root = directory.path().join("var/lib/rocketmq-dashboard");
        std::fs::create_dir_all(&deployed_root).expect("deployed dashboard directory");
        std::fs::write(deployed_root.join("consumer-monitor-config.json"), b"{}")
            .expect("former deployed monitor file");
        let custom = directory.path().join("custom-monitor-state.json");
        std::fs::write(&custom, b"{}").expect("former custom monitor file");
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        owner.block_on(async {
            for backend in [
                StorageBackend::File,
                StorageBackend::Sqlite,
                StorageBackend::MySql,
                StorageBackend::Postgres,
            ] {
                let deployed_path = match backend {
                    StorageBackend::Sqlite => deployed_root.join("dashboard.db"),
                    StorageBackend::File | StorageBackend::MySql | StorageBackend::Postgres => deployed_root.clone(),
                };
                let deployed = DashboardPersistence::initialize(
                    &StorageConfig {
                        backend,
                        data_path: deployed_path,
                        database_url: None,
                        pool: SqlPoolConfig::default(),
                    },
                    owner.root_context().component(format!("deployed-legacy-{backend:?}")),
                )
                .await;
                assert!(matches!(deployed, Err(PersistenceError::UnsupportedLayout)));

                let custom_config = StorageConfig {
                    backend,
                    data_path: directory.path().join(format!("custom-{backend:?}")),
                    database_url: None,
                    pool: SqlPoolConfig::default(),
                };
                assert!(matches!(
                    reject_legacy_paths(&custom_config, std::slice::from_ref(&custom)),
                    Err(PersistenceError::UnsupportedLayout)
                ));
            }
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }
}
