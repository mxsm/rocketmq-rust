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

use crate::{
    auth::SessionState,
    error::{CommandResult, DashboardError, DashboardResult},
    persistence::{StorageManager, schema::SCHEMA_VERSION},
};
use rusqlite::Connection;
use serde::Serialize;
use tauri::State;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StorageStatus {
    backend: &'static str,
    mode: &'static str,
    available: bool,
    schema_version: Option<i64>,
    observed_since_ms: i64,
    checked_at_ms: i64,
    last_write_ms: Option<i64>,
    database_bytes: Option<i64>,
    reusable_bytes: Option<i64>,
    disk_free_bytes: Option<i64>,
    error: Option<&'static str>,
}
impl StorageStatus {
    fn unavailable(observed_since_ms: i64) -> Self {
        Self {
            backend: "sqlite",
            mode: "singleNode",
            available: false,
            schema_version: None,
            observed_since_ms,
            checked_at_ms: chrono::Utc::now().timestamp_millis(),
            last_write_ms: None,
            database_bytes: None,
            reusable_bytes: None,
            disk_free_bytes: None,
            error: Some("Dashboard storage is unavailable. Retry the check."),
        }
    }
}
pub(crate) struct OpsManager {
    storage: StorageManager,
    observed_since_ms: i64,
}
impl OpsManager {
    pub(crate) fn new(storage: StorageManager) -> Self {
        Self {
            storage,
            observed_since_ms: chrono::Utc::now().timestamp_millis(),
        }
    }
    async fn status(&self) -> StorageStatus {
        let observed = self.observed_since_ms;
        self.storage
            .read("storage-status", move |connection| read_status(connection, observed))
            .await
            .unwrap_or_else(|_| StorageStatus::unavailable(observed))
    }
}

/// Read a coherent SQLite snapshot. No paths, credentials, or fabricated pool/disk metrics are returned.
pub(crate) fn read_status(connection: &mut Connection, observed_since_ms: i64) -> DashboardResult<StorageStatus> {
    let transaction = connection.transaction()?;
    let version: i64 =
        transaction.query_row("SELECT version FROM dashboard_schema WHERE id=1", [], |row| row.get(0))?;
    if version != SCHEMA_VERSION {
        return Err(DashboardError::UnsupportedStorageVersion);
    }
    let last_write_ms = transaction.query_row("SELECT last_write_ms FROM storage_activity WHERE id=1", [], |row| {
        row.get(0)
    })?;
    let page_size = transaction
        .query_row("PRAGMA page_size", [], |row| row.get::<_, i64>(0))
        .ok();
    let bytes = |pragma: &str| {
        transaction
            .query_row(pragma, [], |row| row.get::<_, i64>(0))
            .ok()
            .and_then(|pages| page_size.and_then(|size| pages.checked_mul(size)))
            .filter(|value| *value >= 0)
    };
    Ok(StorageStatus {
        backend: "sqlite",
        mode: "singleNode",
        available: true,
        schema_version: Some(version),
        observed_since_ms,
        checked_at_ms: chrono::Utc::now().timestamp_millis(),
        last_write_ms,
        database_bytes: bytes("PRAGMA page_count"),
        reusable_bytes: bytes("PRAGMA freelist_count"),
        disk_free_bytes: None,
        error: None,
    })
}

#[tauri::command]
pub async fn get_storage_status(
    session_id: String,
    session_state: State<'_, SessionState>,
    ops: State<'_, OpsManager>,
) -> CommandResult<StorageStatus> {
    match session_state.authorize_read_only(&session_id).await {
        Ok(_) => Ok(ops.status().await),
        Err(
            DashboardError::Io(_)
            | DashboardError::Database(_)
            | DashboardError::StorageClosed
            | DashboardError::Persistence(_),
        ) => Ok(StorageStatus::unavailable(ops.observed_since_ms)),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn committed_writes_advance_status_but_reads_and_rollbacks_do_not() {
        let mut db = Connection::open_in_memory().unwrap();
        crate::persistence::schema::initialize(&mut db).unwrap();
        assert_eq!(read_status(&mut db, 1).unwrap().last_write_ms, None);
        db.execute("INSERT INTO history_samples VALUES('env','broker-count','',1,2)", [])
            .unwrap();
        let written = read_status(&mut db, 1).unwrap().last_write_ms;
        assert!(written.is_some());
        // A sentinel lets the test detect accidental updates without timing or sleeps.
        db.execute("UPDATE storage_activity SET last_write_ms=123", []).unwrap();
        {
            let tx = db.transaction().unwrap();
            tx.execute("DELETE FROM history_samples", []).unwrap();
        }
        let status = read_status(&mut db, 1).unwrap();
        assert_eq!(status.last_write_ms, Some(123));
        assert_eq!(read_status(&mut db, 1).unwrap().last_write_ms, Some(123));
        assert!(status.database_bytes.unwrap() > 0);
        assert_eq!(status.disk_free_bytes, None);
        assert!(!serde_json::to_string(&status).unwrap().contains("path"));
    }
    #[test]
    fn missing_database_is_not_created_and_unavailable_capacity_is_unknown() {
        let path = std::env::temp_dir().join(format!("private-storage-{}.db", uuid::Uuid::new_v4()));
        assert!(crate::persistence::open_read_only(&path).is_err());
        assert!(!path.exists());
        let status = StorageStatus::unavailable(12);
        assert!(!status.available);
        assert_eq!(status.database_bytes, None);
        assert_eq!(status.disk_free_bytes, None);
        assert!(!serde_json::to_string(&status).unwrap().contains("private-storage"));
    }
}
