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
pub mod backend;
pub mod error;
pub mod file_store;
pub mod migration;
pub mod sql_store;

use crate::config::StorageConfig;
use crate::model::StorageBackend;
use crate::persistence::backend::PersistenceBackend;
use crate::persistence::error::PersistenceError;
use rocketmq_runtime::ChildServiceContext;
use serde::Deserialize;
use serde::Serialize;

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
        Ok(Self { backend })
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
}
