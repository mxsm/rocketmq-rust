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

use crate::nameserver::db::NameServerDb;
use crate::nameserver::types::NameServerError;
use crate::nameserver::types::NameServerHomePageResponse;
use crate::nameserver::types::NameServerMutationResponse;
use crate::nameserver::types::NameServerRecord;
use crate::nameserver::types::NameServerResult;
use chrono::Utc;
use rusqlite::OptionalExtension;
use rusqlite::params;

const DEFAULT_NAME_SERVER: &str = "127.0.0.1:9876";

#[derive(Debug, Clone)]
pub(crate) struct NameServerService {
    db: NameServerDb,
}

impl NameServerService {
    pub(crate) fn new(db: NameServerDb) -> Self {
        Self { db }
    }

    pub(crate) fn bootstrap_defaults(&self) -> NameServerResult<()> {
        let connection = self.db.connection()?;
        let name_server_count: i64 =
            connection.query_row("SELECT COUNT(*) FROM nameserver_addresses", [], |row| row.get(0))?;

        if name_server_count == 0 {
            let now = Utc::now().to_rfc3339();
            connection.execute(
                "
                INSERT INTO nameserver_addresses (address, is_active, sort_order, created_at, updated_at)
                VALUES (?1, 1, 0, ?2, ?2)
                ",
                params![DEFAULT_NAME_SERVER, now],
            )?;
            return Ok(());
        }

        let active_count: i64 = connection.query_row(
            "SELECT COUNT(*) FROM nameserver_addresses WHERE is_active = 1",
            [],
            |row| row.get(0),
        )?;

        if active_count == 0 {
            connection.execute(
                "
                UPDATE nameserver_addresses
                SET is_active = 1,
                    updated_at = ?1
                WHERE id = (
                    SELECT id
                    FROM nameserver_addresses
                    ORDER BY sort_order ASC, id ASC
                    LIMIT 1
                )
                ",
                params![Utc::now().to_rfc3339()],
            )?;
        }

        Ok(())
    }

    pub(crate) fn get_home_page(&self) -> NameServerResult<NameServerHomePageResponse> {
        let connection = self.db.connection()?;
        let name_servers = self.list_name_servers(&connection)?;
        let current_namesrv = name_servers
            .iter()
            .find(|record| record.is_active)
            .map(|record| record.address.clone());

        let (use_vip_channel, use_tls) = connection.query_row(
            "
            SELECT use_vip_channel, use_tls
            FROM nameserver_settings
            WHERE id = 1
            ",
            [],
            |row| Ok((row.get::<_, i64>(0)? != 0, row.get::<_, i64>(1)? != 0)),
        )?;

        Ok(NameServerHomePageResponse {
            success: true,
            message: "NameServer settings loaded".to_string(),
            namesrv_addr_list: name_servers.into_iter().map(|record| record.address).collect(),
            current_namesrv,
            use_vip_channel,
            use_tls,
        })
    }

    pub(crate) fn add_name_server(&self, address: &str) -> NameServerResult<NameServerMutationResponse> {
        let normalized_address = normalize_address(address)?;
        let now = Utc::now().to_rfc3339();
        let mut connection = self.db.connection()?;
        let transaction = connection.transaction()?;

        let existing = transaction
            .query_row(
                "SELECT 1 FROM nameserver_addresses WHERE address = ?1",
                params![normalized_address],
                |_| Ok(()),
            )
            .optional()?;

        if existing.is_some() {
            return Err(NameServerError::Validation("Address already exists".to_string()));
        }

        let next_sort_order: i64 = transaction.query_row(
            "SELECT COALESCE(MAX(sort_order), -1) + 1 FROM nameserver_addresses",
            [],
            |row| row.get(0),
        )?;
        let has_active = transaction
            .query_row(
                "SELECT 1 FROM nameserver_addresses WHERE is_active = 1 LIMIT 1",
                [],
                |_| Ok(()),
            )
            .optional()?
            .is_some();

        transaction.execute(
            "
            INSERT INTO nameserver_addresses (address, is_active, sort_order, created_at, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?4)
            ",
            params![normalized_address, if has_active { 0 } else { 1 }, next_sort_order, now],
        )?;
        transaction.commit()?;

        Ok(NameServerMutationResponse {
            success: true,
            message: "Name Server added".to_string(),
        })
    }

    pub(crate) fn switch_name_server(&self, address: &str) -> NameServerResult<NameServerMutationResponse> {
        let normalized_address = normalize_address(address)?;
        let now = Utc::now().to_rfc3339();
        let mut connection = self.db.connection()?;
        let transaction = connection.transaction()?;

        let target = transaction
            .query_row(
                "
                SELECT id, address, is_active
                FROM nameserver_addresses
                WHERE address = ?1
                ",
                params![normalized_address],
                map_name_server_record,
            )
            .optional()?;

        let Some(target_record) = target else {
            return Err(NameServerError::Validation("Name Server address not found".to_string()));
        };

        if target_record.is_active {
            return Ok(NameServerMutationResponse {
                success: true,
                message: "Name Server is already active".to_string(),
            });
        }

        transaction.execute(
            "
            UPDATE nameserver_addresses
            SET is_active = 0,
                updated_at = ?1
            WHERE is_active = 1
            ",
            params![now],
        )?;
        transaction.execute(
            "
            UPDATE nameserver_addresses
            SET is_active = 1,
                updated_at = ?1
            WHERE id = ?2
            ",
            params![now, target_record.id],
        )?;
        transaction.commit()?;

        Ok(NameServerMutationResponse {
            success: true,
            message: format!("Switched to Name Server: {}", target_record.address),
        })
    }

    pub(crate) fn delete_name_server(&self, address: &str) -> NameServerResult<NameServerMutationResponse> {
        let normalized_address = normalize_address(address)?;
        let mut connection = self.db.connection()?;
        let transaction = connection.transaction()?;

        let target = transaction
            .query_row(
                "
                SELECT id, address, is_active
                FROM nameserver_addresses
                WHERE address = ?1
                ",
                params![normalized_address],
                map_name_server_record,
            )
            .optional()?;

        let Some(target_record) = target else {
            return Err(NameServerError::Validation("Name Server address not found".to_string()));
        };

        if target_record.is_active {
            return Err(NameServerError::Validation(
                "Cannot remove the active Name Server".to_string(),
            ));
        }

        let name_server_count: i64 =
            transaction.query_row("SELECT COUNT(*) FROM nameserver_addresses", [], |row| row.get(0))?;
        if name_server_count <= 1 {
            return Err(NameServerError::Validation(
                "Cannot remove the last Name Server".to_string(),
            ));
        }

        transaction.execute(
            "DELETE FROM nameserver_addresses WHERE id = ?1",
            params![target_record.id],
        )?;
        transaction.commit()?;

        Ok(NameServerMutationResponse {
            success: true,
            message: "Name Server removed".to_string(),
        })
    }

    pub(crate) fn update_vip_channel(&self, enabled: bool) -> NameServerResult<NameServerMutationResponse> {
        self.update_settings_flag("use_vip_channel", enabled, "VIP Channel updated")
    }

    pub(crate) fn update_use_tls(&self, enabled: bool) -> NameServerResult<NameServerMutationResponse> {
        self.update_settings_flag("use_tls", enabled, "TLS setting updated")
    }

    fn update_settings_flag(
        &self,
        column_name: &str,
        enabled: bool,
        success_message: &str,
    ) -> NameServerResult<NameServerMutationResponse> {
        let connection = self.db.connection()?;
        let now = Utc::now().to_rfc3339();
        let sql = format!(
            "
            UPDATE nameserver_settings
            SET {column_name} = ?1,
                updated_at = ?2
            WHERE id = 1
            "
        );

        connection.execute(&sql, params![if enabled { 1 } else { 0 }, now])?;

        Ok(NameServerMutationResponse {
            success: true,
            message: success_message.to_string(),
        })
    }

    fn list_name_servers(&self, connection: &rusqlite::Connection) -> NameServerResult<Vec<NameServerRecord>> {
        let mut statement = connection.prepare(
            "
            SELECT id, address, is_active
            FROM nameserver_addresses
            ORDER BY sort_order ASC, id ASC
            ",
        )?;
        let rows = statement.query_map([], map_name_server_record)?;
        let name_servers = rows.collect::<Result<Vec<_>, _>>()?;
        Ok(name_servers)
    }
}

fn normalize_address(address: &str) -> NameServerResult<String> {
    let normalized = address.trim();
    if normalized.is_empty() {
        return Err(NameServerError::Validation("Please enter a valid address".to_string()));
    }

    let Some((host, port)) = normalized.rsplit_once(':') else {
        return Err(NameServerError::Validation(
            "Name Server address must use host:port format".to_string(),
        ));
    };

    let normalized_host = host.trim();
    let normalized_port = port.trim();

    if normalized_host.is_empty() || normalized_port.is_empty() {
        return Err(NameServerError::Validation(
            "Name Server address must use host:port format".to_string(),
        ));
    }

    normalized_port
        .parse::<u16>()
        .map_err(|_| NameServerError::Validation("Name Server port must be a valid number".to_string()))?;

    Ok(format!("{normalized_host}:{normalized_port}"))
}

fn map_name_server_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<NameServerRecord> {
    Ok(NameServerRecord {
        id: row.get(0)?,
        address: row.get(1)?,
        is_active: row.get::<_, i64>(2)? != 0,
    })
}

#[cfg(test)]
mod tests {
    use super::NameServerService;
    use crate::nameserver::db::NameServerDb;
    use std::env;
    use std::fs;
    use std::path::PathBuf;
    use uuid::Uuid;

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new() -> Self {
            let path = env::temp_dir().join(format!(
                "rocketmq-dashboard-tauri-nameserver-service-tests-{}",
                Uuid::new_v4()
            ));
            fs::create_dir_all(&path).expect("failed to create test directory");
            Self { path }
        }

        fn db_path(&self) -> PathBuf {
            self.path.join("dashboard.db")
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    struct TestContext {
        _test_dir: TestDir,
        service: NameServerService,
    }

    fn setup_service() -> TestContext {
        let test_dir = TestDir::new();
        let db = NameServerDb::from_path(test_dir.db_path());
        db.init().expect("database initialization should succeed");
        let service = NameServerService::new(db);
        service.bootstrap_defaults().expect("bootstrap should succeed");

        TestContext {
            _test_dir: test_dir,
            service,
        }
    }

    #[test]
    fn bootstrap_creates_default_name_server() {
        let context = setup_service();
        let service = &context.service;

        let response = service.get_home_page().expect("home page should load");

        assert_eq!(response.namesrv_addr_list, vec!["127.0.0.1:9876".to_string()]);
        assert_eq!(response.current_namesrv.as_deref(), Some("127.0.0.1:9876"));
        assert!(response.use_vip_channel);
        assert!(!response.use_tls);
    }

    #[test]
    fn add_and_switch_name_server_updates_current_selection() {
        let context = setup_service();
        let service = &context.service;

        service
            .add_name_server("192.168.1.20:9876")
            .expect("second address should be added");
        service
            .switch_name_server("192.168.1.20:9876")
            .expect("switch should succeed");

        let response = service.get_home_page().expect("home page should load");
        assert_eq!(response.current_namesrv.as_deref(), Some("192.168.1.20:9876"));
        assert_eq!(response.namesrv_addr_list.len(), 2);
    }

    #[test]
    fn delete_active_name_server_is_rejected() {
        let context = setup_service();
        let service = &context.service;

        let error = service
            .delete_name_server("127.0.0.1:9876")
            .expect_err("active address removal should fail");

        assert!(error.to_string().contains("Cannot remove the active Name Server"));
    }
}
