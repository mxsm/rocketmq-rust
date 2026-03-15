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

use anyhow::Result;
use chrono::Utc;
use rocketmq_dashboard_common::NameServerConfigSnapshot;
use rocketmq_dashboard_common::NameServerConfigStore;
use rocketmq_dashboard_common::NameServerConfigTransaction;
use rocketmq_dashboard_common::canonicalize_snapshot;
use rocketmq_dashboard_common::normalize_nameserver_address;
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
const DEFAULT_NAMESERVER_ADDRESS: &str = "127.0.0.1:9876";
const ADDRESSES_TABLE_SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS nameserver_addresses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        address TEXT NOT NULL UNIQUE,
        is_current INTEGER NOT NULL DEFAULT 0,
        sort_order INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
";
const SETTINGS_TABLE_SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS nameserver_settings (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        use_vip_channel INTEGER NOT NULL DEFAULT 1,
        use_tls INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
";

#[derive(Debug, Clone)]
pub(crate) struct NameServerDb {
    db_path: PathBuf,
}

impl NameServerDb {
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
        transaction.execute_batch(ADDRESSES_TABLE_SCHEMA)?;
        transaction.execute_batch(SETTINGS_TABLE_SCHEMA)?;
        sanitize_nameserver_rows(&transaction)?;
        repair_snapshot_tables(&transaction)?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn connection(&self) -> Result<Connection> {
        self.ensure_parent_dir()?;

        let connection = Connection::open(&self.db_path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        Ok(connection)
    }

    fn ensure_parent_dir(&self) -> Result<()> {
        if let Some(parent) = self.db_path.parent() {
            fs::create_dir_all(parent)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SqliteNameServerStore {
    db: NameServerDb,
}

impl SqliteNameServerStore {
    pub(crate) fn new(db: NameServerDb) -> Self {
        Self { db }
    }
}

struct SqliteNameServerTransaction<'tx> {
    transaction: Transaction<'tx>,
}

impl NameServerConfigTransaction for SqliteNameServerTransaction<'_> {
    fn load_snapshot(&mut self) -> Result<NameServerConfigSnapshot> {
        load_snapshot_from_connection(&self.transaction)
    }

    fn save_snapshot(&mut self, snapshot: &NameServerConfigSnapshot) -> Result<()> {
        save_snapshot_to_transaction(&self.transaction, snapshot)
    }
}

impl NameServerConfigStore for SqliteNameServerStore {
    fn load_snapshot(&self) -> Result<NameServerConfigSnapshot> {
        let connection = self.db.connection()?;
        load_snapshot_from_connection(&connection)
    }

    fn run_in_transaction<T, F>(&self, operation: F) -> Result<T>
    where
        F: FnOnce(&mut dyn NameServerConfigTransaction) -> Result<T>,
    {
        let mut connection = self.db.connection()?;
        let transaction = connection.transaction()?;
        let mut tx_store = SqliteNameServerTransaction { transaction };

        match operation(&mut tx_store) {
            Ok(value) => {
                tx_store.transaction.commit()?;
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }
}

fn load_snapshot_from_connection(connection: &Connection) -> Result<NameServerConfigSnapshot> {
    let mut statement = connection.prepare(
        "
        SELECT address, is_current
        FROM nameserver_addresses
        ORDER BY sort_order ASC, id ASC
        ",
    )?;
    let mut rows = statement.query([])?;
    let mut current_namesrv = None;
    let mut namesrv_addr_list = Vec::new();

    while let Some(row) = rows.next()? {
        let address: String = row.get(0)?;
        let is_current = row.get::<_, i64>(1)? != 0;
        if is_current && current_namesrv.is_none() {
            current_namesrv = Some(address.clone());
        }
        namesrv_addr_list.push(address);
    }

    let settings = connection
        .query_row(
            "
            SELECT use_vip_channel, use_tls
            FROM nameserver_settings
            WHERE id = 1
            ",
            [],
            |row| Ok((row.get::<_, i64>(0)? != 0, row.get::<_, i64>(1)? != 0)),
        )
        .optional()?;

    let (use_vip_channel, use_tls) = settings.unwrap_or((true, false));

    canonicalize_snapshot(&NameServerConfigSnapshot {
        current_namesrv,
        namesrv_addr_list,
        use_vip_channel,
        use_tls,
    })
}

fn save_snapshot_to_transaction(transaction: &Transaction<'_>, snapshot: &NameServerConfigSnapshot) -> Result<()> {
    let snapshot = canonicalize_snapshot(snapshot)?;
    let now = Utc::now().to_rfc3339();

    transaction.execute("DELETE FROM nameserver_addresses", [])?;

    for (index, address) in snapshot.namesrv_addr_list.iter().enumerate() {
        transaction.execute(
            "
            INSERT INTO nameserver_addresses (address, is_current, sort_order, created_at, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?4)
            ",
            params![
                address,
                bool_to_i64(snapshot.current_namesrv.as_deref() == Some(address.as_str())),
                index as i64,
                now
            ],
        )?;
    }

    transaction.execute(
        "
        INSERT INTO nameserver_settings (id, use_vip_channel, use_tls, created_at, updated_at)
        VALUES (1, ?1, ?2, ?3, ?3)
        ON CONFLICT(id) DO UPDATE SET
            use_vip_channel = excluded.use_vip_channel,
            use_tls = excluded.use_tls,
            updated_at = excluded.updated_at
        ",
        params![
            bool_to_i64(snapshot.use_vip_channel),
            bool_to_i64(snapshot.use_tls),
            now
        ],
    )?;

    Ok(())
}

fn repair_snapshot_tables(transaction: &Transaction<'_>) -> Result<()> {
    let now = Utc::now().to_rfc3339();
    let address_count: i64 =
        transaction.query_row("SELECT COUNT(*) FROM nameserver_addresses", [], |row| row.get(0))?;

    if address_count == 0 {
        transaction.execute(
            "
            INSERT INTO nameserver_addresses (address, is_current, sort_order, created_at, updated_at)
            VALUES (?1, 1, 0, ?2, ?2)
            ",
            params![DEFAULT_NAMESERVER_ADDRESS, now],
        )?;
    }

    transaction.execute(
        "
        INSERT OR IGNORE INTO nameserver_settings (id, use_vip_channel, use_tls, created_at, updated_at)
        VALUES (1, 1, 0, ?1, ?1)
        ",
        params![now],
    )?;

    let mut current_ids = Vec::new();
    let mut statement = transaction.prepare(
        "
        SELECT id
        FROM nameserver_addresses
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
                FROM nameserver_addresses
                ORDER BY sort_order ASC, id ASC
                LIMIT 1
                ",
                [],
                |row| row.get(0),
            )
            .optional()?;

        if let Some(first_id) = first_id {
            transaction.execute("UPDATE nameserver_addresses SET is_current = 0", [])?;
            transaction.execute(
                "UPDATE nameserver_addresses SET is_current = 1 WHERE id = ?1",
                params![first_id],
            )?;
        }
    } else if current_ids.len() > 1 {
        let keep_id = current_ids[0];
        transaction.execute("UPDATE nameserver_addresses SET is_current = 0", [])?;
        transaction.execute(
            "UPDATE nameserver_addresses SET is_current = 1 WHERE id = ?1",
            params![keep_id],
        )?;
    }

    Ok(())
}

fn sanitize_nameserver_rows(transaction: &Transaction<'_>) -> Result<()> {
    let mut statement = transaction.prepare(
        "
        SELECT address, is_current
        FROM nameserver_addresses
        ORDER BY sort_order ASC, id ASC
        ",
    )?;
    let rows = statement.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? != 0)))?;
    let mut normalized_rows: Vec<(String, bool)> = Vec::new();

    for row in rows {
        let (address, is_current) = row?;
        let Ok(normalized_address) = normalize_nameserver_address(&address) else {
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

    if normalized_rows.is_empty() {
        transaction.execute("DELETE FROM nameserver_addresses", [])?;
        return Ok(());
    }

    let current_index = normalized_rows
        .iter()
        .position(|(_, is_current)| *is_current)
        .unwrap_or(0);
    let now = Utc::now().to_rfc3339();

    transaction.execute("DELETE FROM nameserver_addresses", [])?;

    for (index, (address, _)) in normalized_rows.iter().enumerate() {
        transaction.execute(
            "
            INSERT INTO nameserver_addresses (address, is_current, sort_order, created_at, updated_at)
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
    use super::NameServerDb;
    use super::SqliteNameServerStore;
    use rocketmq_dashboard_common::NameServerConfigSnapshot;
    use rocketmq_dashboard_common::NameServerConfigStore;
    use std::env;
    use std::fs;
    use std::path::PathBuf;
    use uuid::Uuid;

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new() -> Self {
            let path = env::temp_dir().join(format!("rocketmq-dashboard-tauri-nameserver-tests-{}", Uuid::new_v4()));
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
    fn init_creates_nameserver_tables_and_default_snapshot() {
        let test_dir = TestDir::new();
        let db = NameServerDb::from_path(test_dir.db_path());
        db.init().expect("database initialization should succeed");

        let store = SqliteNameServerStore::new(db.clone());
        let snapshot = store.load_snapshot().expect("snapshot should load");

        assert_eq!(snapshot.current_namesrv.as_deref(), Some("127.0.0.1:9876"));
        assert_eq!(snapshot.namesrv_addr_list, vec!["127.0.0.1:9876".to_string()]);
        assert!(snapshot.use_vip_channel);
        assert!(!snapshot.use_tls);
        assert!(db.db_path().exists());
    }

    #[test]
    fn init_sanitizes_duplicate_addresses_after_normalization() {
        let test_dir = TestDir::new();
        let db = NameServerDb::from_path(test_dir.db_path());
        {
            let connection = db.connection().expect("connection should open");
            connection
                .execute_batch(
                    "
                    CREATE TABLE nameserver_addresses (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        address TEXT NOT NULL UNIQUE,
                        is_current INTEGER NOT NULL DEFAULT 0,
                        sort_order INTEGER NOT NULL DEFAULT 0,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    CREATE TABLE nameserver_settings (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        use_vip_channel INTEGER NOT NULL DEFAULT 1,
                        use_tls INTEGER NOT NULL DEFAULT 0,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    ",
                )
                .expect("legacy tables should be created");
            connection
                .execute(
                    "
                    INSERT INTO nameserver_addresses (address, is_current, sort_order, created_at, updated_at)
                    VALUES (?1, 0, 0, ?3, ?3), (?2, 1, 1, ?3, ?3)
                    ",
                    rusqlite::params!["LOCALHOST:9876", " localhost : 9876 ", "2025-01-01T00:00:00Z"],
                )
                .expect("duplicate rows should insert");
        }

        db.init().expect("database initialization should sanitize duplicates");

        let store = SqliteNameServerStore::new(db);
        let snapshot = store.load_snapshot().expect("snapshot should load");

        assert_eq!(snapshot.namesrv_addr_list, vec!["localhost:9876".to_string()]);
        assert_eq!(snapshot.current_namesrv.as_deref(), Some("localhost:9876"));
    }

    #[test]
    fn store_persists_replaced_snapshot_inside_transaction() {
        let test_dir = TestDir::new();
        let db = NameServerDb::from_path(test_dir.db_path());
        db.init().expect("database initialization should succeed");
        let store = SqliteNameServerStore::new(db);

        store
            .run_in_transaction(|transaction| {
                transaction.save_snapshot(&NameServerConfigSnapshot {
                    current_namesrv: Some("127.0.0.2:9876".to_string()),
                    namesrv_addr_list: vec!["127.0.0.1:9876".to_string(), "127.0.0.2:9876".to_string()],
                    use_vip_channel: false,
                    use_tls: true,
                })?;
                Ok(())
            })
            .expect("transaction should commit");

        let snapshot = store.load_snapshot().expect("snapshot should load");
        assert_eq!(snapshot.current_namesrv.as_deref(), Some("127.0.0.2:9876"));
        assert!(!snapshot.use_vip_channel);
        assert!(snapshot.use_tls);
    }
}
