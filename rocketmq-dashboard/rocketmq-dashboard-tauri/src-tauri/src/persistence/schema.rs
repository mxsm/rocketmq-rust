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

pub(crate) const SCHEMA_VERSION: i64 = 6;

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
            CREATE TABLE connection_metadata (id INTEGER PRIMARY KEY CHECK(id = 1), revision INTEGER NOT NULL CHECK(revision >= 0 AND revision < 9007199254740991));
            INSERT INTO connection_metadata(id, revision) VALUES (1, 0);
            CREATE TABLE endpoint_identity (
                kind TEXT NOT NULL CHECK(kind IN ('nameserver', 'proxy')),
                address TEXT NOT NULL,
                endpoint_id TEXT NOT NULL UNIQUE,
                environment_id TEXT UNIQUE,
                PRIMARY KEY(kind, address)
            );
            CREATE TABLE consumer_monitor_rules (
                environment_id TEXT NOT NULL,
                consumer_group TEXT NOT NULL,
                min_count INTEGER NOT NULL CHECK(min_count >= 0),
                max_diff_total INTEGER NOT NULL CHECK(max_diff_total >= 0),
                revision INTEGER NOT NULL CHECK(revision > 0 AND revision <= 9007199254740991),
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY(environment_id,consumer_group)
            );
            CREATE TABLE history_samples (
                environment_id TEXT NOT NULL,
                metric TEXT NOT NULL CHECK(metric IN ('broker-count', 'topic-count', 'topic-total-messages')),
                dimension TEXT NOT NULL DEFAULT '',
                timestamp_ms INTEGER NOT NULL CHECK(timestamp_ms >= 0),
                value REAL NOT NULL CHECK(value >= 0),
                PRIMARY KEY(environment_id, metric, dimension, timestamp_ms)
            );
            CREATE INDEX history_retention ON history_samples(timestamp_ms);
            CREATE TABLE audit_events (
                event_id TEXT PRIMARY KEY,
                request_id TEXT NOT NULL UNIQUE,
                actor TEXT,
                action TEXT NOT NULL,
                resource_type TEXT NOT NULL,
                resource_name TEXT,
                environment_id TEXT,
                outcome TEXT NOT NULL CHECK(outcome IN ('success', 'rejected', 'failed', 'partial', 'unknown')),
                detail_json TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL
            );
            CREATE INDEX audit_time ON audit_events(created_at_ms DESC, event_id DESC);
            CREATE INDEX audit_actor_time ON audit_events(actor, created_at_ms DESC, event_id DESC);
            CREATE INDEX audit_action_time ON audit_events(action, created_at_ms DESC, event_id DESC);
            CREATE TABLE sessions (
                id TEXT PRIMARY KEY,
                token_digest BLOB NOT NULL UNIQUE,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                created_at_ms INTEGER NOT NULL,
                expires_at_ms INTEGER NOT NULL,
                last_seen_at_ms INTEGER NOT NULL,
                revoked_at_ms INTEGER
            );
            CREATE INDEX sessions_user_id ON sessions(user_id, id);
            CREATE INDEX sessions_expiry ON sessions(expires_at_ms);
            CREATE INDEX sessions_revoked ON sessions(revoked_at_ms) WHERE revoked_at_ms IS NOT NULL;
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
