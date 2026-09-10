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

use crate::error::DashboardError;
use crate::error::DashboardResult;
use rusqlite::Connection;
use rusqlite::TransactionBehavior;

pub(crate) const SCHEMA_VERSION: i64 = 1;

pub(crate) fn initialize(connection: &mut Connection) -> DashboardResult<()> {
    // IMMEDIATE serializes competing initializers before reading the version.
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let has_schema: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'dashboard_schema')",
        [],
        |row| row.get(0),
    )?;
    if !has_schema {
        let has_tables: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name NOT LIKE 'sqlite_%')",
            [],
            |row| row.get(0),
        )?;
        if has_tables {
            return Err(DashboardError::UnsupportedStorageVersion);
        }
    }
    transaction.execute_batch(
        "CREATE TABLE IF NOT EXISTS dashboard_schema (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            version INTEGER NOT NULL CHECK (version >= 0)
        );
        INSERT OR IGNORE INTO dashboard_schema (id, version) VALUES (1, 0);",
    )?;
    let version: i64 = transaction.query_row("SELECT version FROM dashboard_schema WHERE id = 1", [], |row| {
        row.get(0)
    })?;
    if has_schema && version != SCHEMA_VERSION {
        return Err(DashboardError::UnsupportedStorageVersion);
    }
    if version == 0 {
        transaction.execute_batch(
            "CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                must_change_password INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_login_at TEXT
            );
            CREATE TABLE IF NOT EXISTS nameserver_addresses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                address TEXT NOT NULL UNIQUE,
                is_current INTEGER NOT NULL DEFAULT 0,
                sort_order INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS nameserver_settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                use_vip_channel INTEGER NOT NULL DEFAULT 1,
                use_tls INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS proxy_addresses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                address TEXT NOT NULL UNIQUE,
                is_current INTEGER NOT NULL DEFAULT 0,
                sort_order INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );",
        )?;
        transaction.execute(
            "UPDATE dashboard_schema SET version = ?1 WHERE id = 1",
            [SCHEMA_VERSION],
        )?;
    }
    transaction.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_schema_reopens_without_resetting_data() {
        let mut connection = Connection::open_in_memory().unwrap();
        initialize(&mut connection).unwrap();
        connection.execute("INSERT INTO users (username, password_hash, created_at, updated_at) VALUES ('saved', 'hash', 'created', 'updated')", []).unwrap();
        initialize(&mut connection).unwrap();
        let username: String = connection
            .query_row("SELECT username FROM users", [], |row| row.get(0))
            .unwrap();
        assert_eq!(username, "saved");
    }

    #[test]
    fn unsupported_database_is_rejected_without_modification() {
        let mut connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch("CREATE TABLE old_data(value TEXT); INSERT INTO old_data VALUES ('keep');")
            .unwrap();
        assert!(matches!(
            initialize(&mut connection),
            Err(DashboardError::UnsupportedStorageVersion)
        ));
        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name = 'dashboard_schema'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
        let value: String = connection
            .query_row("SELECT value FROM old_data", [], |row| row.get(0))
            .unwrap();
        assert_eq!(value, "keep");
    }

    #[test]
    fn mismatched_version_is_rejected_without_reset() {
        let mut connection = Connection::open_in_memory().unwrap();
        initialize(&mut connection).unwrap();
        for version in [0, 99] {
            connection
                .execute("UPDATE dashboard_schema SET version = ?1", [version])
                .unwrap();
            assert!(matches!(
                initialize(&mut connection),
                Err(DashboardError::UnsupportedStorageVersion)
            ));
            let stored: i64 = connection
                .query_row("SELECT version FROM dashboard_schema", [], |row| row.get(0))
                .unwrap();
            assert_eq!(stored, version);
        }
    }
}
