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

//! Offline backup operations for the dashboard's supported storage layouts.
//!
//! This module is deliberately below the HTTP boundary. It has no handler and
//! only the local operations binary invokes its destructive restore entry
//! point. SQL reads use one snapshot transaction and SQL restores use one
//! maintenance-locked write transaction.

use crate::persistence::DashboardPersistence;
use crate::persistence::backend::PersistenceBackend;
use crate::persistence::error::PersistenceError;

pub const BACKUP_FORMAT_VERSION: u32 = 1;
pub(super) const MAX_BACKUP_FILE_BYTES: u64 = 128 * 1024 * 1024;
pub(super) const MAX_BACKUP_LINE_BYTES: usize = 1024 * 1024;
pub(super) const COLLECTION_FILES: [&str; 6] = [
    "environments.ndjson",
    "endpoints.ndjson",
    "monitors.ndjson",
    "history.ndjson",
    "sessions.ndjson",
    "audit.ndjson",
];

#[path = "storage_operations_file.rs"]
mod file;
#[path = "storage_operations_format.rs"]
mod format;
#[path = "storage_operations_sql.rs"]
mod sql;
#[path = "storage_operations_validation.rs"]
mod validation;

pub use file::restore_file_target;
pub use format::BackupCounts;
pub use format::BackupData;
pub use format::BackupManifest;
pub use format::BackupScope;
pub use format::BackupSession;
pub use format::read_verified_backup;
pub use format::write_backup;

/// Creates a backup of one initialized storage backend. File storage is
/// offline by construction because `DashboardPersistence::initialize` owns
/// the directory lock; SQL delegates to its narrow snapshot transaction.
pub async fn snapshot(persistence: &DashboardPersistence) -> Result<BackupData, PersistenceError> {
    match &persistence.backend {
        PersistenceBackend::File(store) => file::snapshot_file(persistence, store).await,
        PersistenceBackend::Sql(store) => store.snapshot_for_operations().await,
    }
}

/// Restores a verified same-backend backup. The target has already been
/// initialized to the current format and must remain empty through the
/// backend-specific write transaction.
pub async fn restore(persistence: &DashboardPersistence, data: &BackupData) -> Result<(), PersistenceError> {
    validation::verify_data(data, Some(persistence.storage_backend()))?;
    match &persistence.backend {
        PersistenceBackend::Sql(store) => store.restore_for_operations(data).await,
        PersistenceBackend::File(_) => Err(PersistenceError::InvalidConfig(
            "file restore must publish an unopened target".to_string(),
        )),
    }
}

#[cfg(test)]
#[path = "storage_operations_tests.rs"]
mod tests;
