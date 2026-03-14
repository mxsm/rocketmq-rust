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

use crate::nameserver::types::NameServerError;
use crate::nameserver::types::NameServerResult;
use rusqlite::Connection;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use tauri::AppHandle;
use tauri::Manager;

const DB_FILE_NAME: &str = "dashboard.db";
const NAMESERVER_TABLES_SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS nameserver_addresses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        address TEXT NOT NULL UNIQUE,
        is_active INTEGER NOT NULL DEFAULT 0,
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

    CREATE UNIQUE INDEX IF NOT EXISTS idx_nameserver_addresses_single_active
    ON nameserver_addresses(is_active)
    WHERE is_active = 1;
";

#[derive(Debug, Clone)]
pub(crate) struct NameServerDb {
    db_path: PathBuf,
}

impl NameServerDb {
    pub(crate) fn new(app: &AppHandle) -> NameServerResult<Self> {
        let app_config_dir = app
            .path()
            .app_config_dir()
            .map_err(|error| NameServerError::AppPath(error.to_string()))?;

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

    pub(crate) fn init(&self) -> NameServerResult<()> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        transaction.execute_batch(NAMESERVER_TABLES_SCHEMA)?;
        if table_exists(&transaction, "ops_name_servers")? {
            transaction.execute(
                "
                INSERT OR IGNORE INTO nameserver_addresses (id, address, is_active, sort_order, created_at, updated_at)
                SELECT id, address, is_active, sort_order, created_at, updated_at
                FROM ops_name_servers
                ",
                [],
            )?;
        }

        if table_exists(&transaction, "ops_settings")? {
            transaction.execute(
                "
                INSERT OR IGNORE INTO nameserver_settings (id, use_vip_channel, use_tls, created_at, updated_at)
                SELECT id, use_vip_channel, use_tls, created_at, updated_at
                FROM ops_settings
                ",
                [],
            )?;
        }
        transaction.execute(
            "
            INSERT INTO nameserver_settings (id, use_vip_channel, use_tls, created_at, updated_at)
            SELECT 1, 1, 0, datetime('now'), datetime('now')
            WHERE NOT EXISTS (SELECT 1 FROM nameserver_settings WHERE id = 1)
            ",
            [],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn connection(&self) -> NameServerResult<Connection> {
        self.ensure_parent_dir()?;

        let connection = Connection::open(&self.db_path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        Ok(connection)
    }

    fn ensure_parent_dir(&self) -> NameServerResult<()> {
        if let Some(parent) = self.db_path.parent() {
            fs::create_dir_all(parent)?;
        }

        Ok(())
    }
}

fn table_exists(connection: &Connection, table_name: &str) -> NameServerResult<bool> {
    let table_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
        [table_name],
        |row| row.get(0),
    )?;

    Ok(table_count > 0)
}
