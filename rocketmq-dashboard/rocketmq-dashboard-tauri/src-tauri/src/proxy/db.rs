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

use anyhow::Result;
use chrono::Utc;
use rocketmq_dashboard_common::ProxyConfigSnapshot;
use rocketmq_dashboard_common::canonicalize_proxy_snapshot;
use rocketmq_dashboard_common::normalize_proxy_address;
use rusqlite::Connection;
use rusqlite::OptionalExtension;
use rusqlite::Transaction;
use rusqlite::params;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use tauri::AppHandle;
use tauri::Manager;

const DB_FILE_NAME: &str = "dashboard.db";
const PROXY_ADDRESSES_TABLE_SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS proxy_addresses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        address TEXT NOT NULL UNIQUE,
        is_current INTEGER NOT NULL DEFAULT 0,
        sort_order INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
";

#[derive(Debug, Clone)]
pub(crate) struct ProxyDb {
    db_path: PathBuf,
}

impl ProxyDb {
    pub(crate) fn new(app: &AppHandle) -> Result<Self> {
        let app_config_dir = app.path().app_config_dir()?;
        Ok(Self::from_path(app_config_dir.join(DB_FILE_NAME)))
    }

    pub(crate) fn from_path(db_path: impl Into<PathBuf>) -> Self {
        Self {
            db_path: db_path.into(),
        }
    }

    pub(crate) fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub(crate) fn init(&self) -> Result<()> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        transaction.execute_batch(PROXY_ADDRESSES_TABLE_SCHEMA)?;
        sanitize_proxy_rows(&transaction)?;
        repair_proxy_rows(&transaction)?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn connection(&self) -> Result<Connection> {
        self.ensure_parent_dir()?;

        let connection = Connection::open(&self.db_path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        Ok(connection)
    }

    pub(crate) fn load_snapshot(&self) -> Result<ProxyConfigSnapshot> {
        let connection = self.connection()?;
        load_snapshot_from_connection(&connection)
    }

    pub(crate) fn update_snapshot<F>(&self, operation: F) -> Result<ProxyConfigSnapshot>
    where
        F: FnOnce(&mut ProxyConfigSnapshot) -> Result<()>,
    {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let mut snapshot = load_snapshot_from_connection(&transaction)?;

        operation(&mut snapshot)?;

        let snapshot = canonicalize_proxy_snapshot(&snapshot)?;
        save_snapshot_to_transaction(&transaction, &snapshot)?;
        transaction.commit()?;
        Ok(snapshot)
    }

    fn ensure_parent_dir(&self) -> Result<()> {
        if let Some(parent) = self.db_path.parent() {
            fs::create_dir_all(parent)?;
        }

        Ok(())
    }
}

fn load_snapshot_from_connection(connection: &Connection) -> Result<ProxyConfigSnapshot> {
    let mut statement = connection.prepare(
        "
        SELECT address, is_current
        FROM proxy_addresses
        ORDER BY sort_order ASC, id ASC
        ",
    )?;
    let mut rows = statement.query([])?;
    let mut current_proxy_addr = None;
    let mut proxy_addr_list = Vec::new();

    while let Some(row) = rows.next()? {
        let address: String = row.get(0)?;
        let is_current = row.get::<_, i64>(1)? != 0;
        if is_current && current_proxy_addr.is_none() {
            current_proxy_addr = Some(address.clone());
        }
        proxy_addr_list.push(address);
    }

    Ok(canonicalize_proxy_snapshot(&ProxyConfigSnapshot {
        current_proxy_addr,
        proxy_addr_list,
    })?)
}

fn save_snapshot_to_transaction(transaction: &Transaction<'_>, snapshot: &ProxyConfigSnapshot) -> Result<()> {
    let snapshot = canonicalize_proxy_snapshot(snapshot)?;
    let now = Utc::now().to_rfc3339();

    transaction.execute("DELETE FROM proxy_addresses", [])?;

    for (index, address) in snapshot.proxy_addr_list.iter().enumerate() {
        transaction.execute(
            "
            INSERT INTO proxy_addresses (address, is_current, sort_order, created_at, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?4)
            ",
            params![
                address,
                bool_to_i64(snapshot.current_proxy_addr.as_deref() == Some(address.as_str())),
                index as i64,
                now
            ],
        )?;
    }

    Ok(())
}

fn repair_proxy_rows(transaction: &Transaction<'_>) -> Result<()> {
    let address_count: i64 = transaction.query_row("SELECT COUNT(*) FROM proxy_addresses", [], |row| row.get(0))?;

    if address_count == 0 {
        return Ok(());
    }

    let mut current_ids = Vec::new();
    let mut statement = transaction.prepare(
        "
        SELECT id
        FROM proxy_addresses
        WHERE is_current = 1
        ORDER BY sort_order ASC, id ASC
        ",
    )?;
    let rows = statement.query_map([], |row| row.get::<_, i64>(0))?;
    for row in rows {
        current_ids.push(row?);
    }
    drop(statement);

    if current_ids.is_empty() {
        let first_id: Option<i64> = transaction
            .query_row(
                "
                SELECT id
                FROM proxy_addresses
                ORDER BY sort_order ASC, id ASC
                LIMIT 1
                ",
                [],
                |row| row.get(0),
            )
            .optional()?;

        if let Some(first_id) = first_id {
            transaction.execute("UPDATE proxy_addresses SET is_current = 0", [])?;
            transaction.execute(
                "UPDATE proxy_addresses SET is_current = 1 WHERE id = ?1",
                params![first_id],
            )?;
        }
    } else if current_ids.len() > 1 {
        let keep_id = current_ids[0];
        transaction.execute("UPDATE proxy_addresses SET is_current = 0", [])?;
        transaction.execute(
            "UPDATE proxy_addresses SET is_current = 1 WHERE id = ?1",
            params![keep_id],
        )?;
    }

    Ok(())
}

fn sanitize_proxy_rows(transaction: &Transaction<'_>) -> Result<()> {
    let mut statement = transaction.prepare(
        "
        SELECT address, is_current
        FROM proxy_addresses
        ORDER BY sort_order ASC, id ASC
        ",
    )?;
    let rows = statement.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? != 0)))?;
    let mut normalized_rows: Vec<(String, bool)> = Vec::new();

    for row in rows {
        let (address, is_current) = row?;
        let Ok(normalized_address) = normalize_proxy_address(&address) else {
            continue;
        };

        if let Some((_, existing_is_current)) = normalized_rows
            .iter_mut()
            .find(|(existing_address, _)| existing_address == &normalized_address)
        {
            *existing_is_current |= is_current;
            continue;
        }

        normalized_rows.push((normalized_address, is_current));
    }
    drop(statement);

    transaction.execute("DELETE FROM proxy_addresses", [])?;

    if normalized_rows.is_empty() {
        return Ok(());
    }

    let current_index = normalized_rows
        .iter()
        .position(|(_, is_current)| *is_current)
        .unwrap_or(0);
    let now = Utc::now().to_rfc3339();

    for (index, (address, _)) in normalized_rows.iter().enumerate() {
        transaction.execute(
            "
            INSERT INTO proxy_addresses (address, is_current, sort_order, created_at, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?4)
            ",
            params![address, bool_to_i64(index == current_index), index as i64, now],
        )?;
    }

    Ok(())
}

fn bool_to_i64(value: bool) -> i64 {
    if value { 1 } else { 0 }
}

#[cfg(test)]
mod tests {
    use super::ProxyDb;
    use super::load_snapshot_from_connection;
    use rocketmq_dashboard_common::ProxyConfigSnapshot;
    use std::env;
    use std::fs;
    use std::path::PathBuf;
    use uuid::Uuid;

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new() -> Self {
            let path = env::temp_dir().join(format!("rocketmq-dashboard-tauri-proxy-db-tests-{}", Uuid::new_v4()));
            fs::create_dir_all(&path).expect("failed to create test directory");
            Self { path }
        }

        fn db_path(&self) -> PathBuf {
            self.path.join("nested").join("dashboard.db")
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn init_creates_proxy_table_without_default_addresses() {
        let test_dir = TestDir::new();
        let db = ProxyDb::from_path(test_dir.db_path());
        db.init().expect("database initialization should succeed");

        let snapshot = db.load_snapshot().expect("snapshot should load");

        assert!(snapshot.current_proxy_addr.is_none());
        assert!(snapshot.proxy_addr_list.is_empty());
        assert!(db.db_path().exists());
    }

    #[test]
    fn init_sanitizes_duplicate_addresses_after_normalization() {
        let test_dir = TestDir::new();
        let db = ProxyDb::from_path(test_dir.db_path());
        {
            let connection = db.connection().expect("connection should open");
            connection
                .execute_batch(
                    "
                    CREATE TABLE proxy_addresses (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        address TEXT NOT NULL UNIQUE,
                        is_current INTEGER NOT NULL DEFAULT 0,
                        sort_order INTEGER NOT NULL DEFAULT 0,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    ",
                )
                .expect("legacy table should be created");
            connection
                .execute(
                    "
                    INSERT INTO proxy_addresses (address, is_current, sort_order, created_at, updated_at)
                    VALUES (?1, 0, 0, ?3, ?3), (?2, 1, 1, ?3, ?3)
                    ",
                    rusqlite::params!["LOCALHOST:8080", " localhost : 8080 ", "2026-01-01T00:00:00Z"],
                )
                .expect("duplicate rows should insert");
        }

        db.init().expect("database initialization should sanitize duplicates");

        let snapshot = db.load_snapshot().expect("snapshot should load");

        assert_eq!(snapshot.proxy_addr_list, vec!["localhost:8080".to_string()]);
        assert_eq!(snapshot.current_proxy_addr.as_deref(), Some("localhost:8080"));
    }

    #[test]
    fn update_snapshot_persists_current_and_order() {
        let test_dir = TestDir::new();
        let db = ProxyDb::from_path(test_dir.db_path());
        db.init().expect("database initialization should succeed");

        db.update_snapshot(|snapshot| {
            *snapshot = ProxyConfigSnapshot {
                current_proxy_addr: Some("127.0.0.2:8080".to_string()),
                proxy_addr_list: vec!["127.0.0.1:8080".to_string(), "127.0.0.2:8080".to_string()],
            };
            Ok(())
        })
        .expect("snapshot should persist");

        let connection = db.connection().expect("connection should open");
        let snapshot = load_snapshot_from_connection(&connection).expect("snapshot should load");

        assert_eq!(snapshot.current_proxy_addr.as_deref(), Some("127.0.0.2:8080"));
        assert_eq!(
            snapshot.proxy_addr_list,
            vec!["127.0.0.1:8080".to_string(), "127.0.0.2:8080".to_string()]
        );
    }
}
