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
use crate::persistence::error::PersistenceError;
use sqlx::Acquire;
use sqlx::MySqlPool;
use sqlx::PgPool;
use sqlx::SqlitePool;
use sqlx::{MySqlConnection, PgConnection, SqliteConnection};

const SQLITE_INITIAL: &str = include_str!("../../migrations/sqlite/0001_initial.sql");
const SQLITE_PHASE_TWO: &str = include_str!("../../migrations/sqlite/0002_environment_endpoint_constraints.sql");
const SQLITE_PHASE_THREE: &str = include_str!("../../migrations/sqlite/0003_history_retention.sql");
const MYSQL_INITIAL: &str = include_str!("../../migrations/mysql/0001_initial.sql");
const MYSQL_PHASE_TWO: &str = include_str!("../../migrations/mysql/0002_environment_endpoint_constraints.sql");
const MYSQL_PHASE_THREE: &str = include_str!("../../migrations/mysql/0003_history_retention.sql");
const POSTGRES_INITIAL: &str = include_str!("../../migrations/postgres/0001_initial.sql");
const POSTGRES_PHASE_TWO: &str = include_str!("../../migrations/postgres/0002_environment_endpoint_constraints.sql");
const POSTGRES_PHASE_THREE: &str = include_str!("../../migrations/postgres/0003_history_retention.sql");
const MYSQL_MIGRATION_LOCK: &str = "rocketmq_dashboard_schema_migration";
const POSTGRES_MIGRATION_LOCK: i64 = 7_246_920_002;

pub async fn migrate_sqlite(pool: &SqlitePool) -> Result<i64, PersistenceError> {
    let mut connection = pool.acquire().await.map_err(map_query_error)?;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *connection)
        .await
        .map_err(map_query_error)?;
    let result = migrate_sqlite_locked(&mut connection).await;
    match result {
        Ok(version) => {
            sqlx::query("COMMIT")
                .execute(&mut *connection)
                .await
                .map_err(map_query_error)?;
            Ok(version)
        }
        Err(error) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut *connection).await;
            Err(error)
        }
    }
}

pub async fn migrate_mysql(pool: &MySqlPool) -> Result<i64, PersistenceError> {
    let mut connection = pool.acquire().await.map_err(map_query_error)?;
    let locked: i64 = sqlx::query_scalar("SELECT GET_LOCK(?, 30)")
        .bind(MYSQL_MIGRATION_LOCK)
        .fetch_one(&mut *connection)
        .await
        .map_err(map_query_error)?;
    if locked != 1 {
        return Err(PersistenceError::Timeout);
    }
    let result = migrate_mysql_locked(&mut connection).await;
    let _ = sqlx::query_scalar::<_, i64>("SELECT RELEASE_LOCK(?)")
        .bind(MYSQL_MIGRATION_LOCK)
        .fetch_one(&mut *connection)
        .await;
    result
}

pub async fn migrate_postgres(pool: &PgPool) -> Result<i64, PersistenceError> {
    let mut connection = pool.acquire().await.map_err(map_query_error)?;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(POSTGRES_MIGRATION_LOCK)
        .execute(&mut *connection)
        .await
        .map_err(map_query_error)?;
    let result = migrate_postgres_locked(&mut connection).await;
    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(POSTGRES_MIGRATION_LOCK)
        .execute(&mut *connection)
        .await;
    result
}

async fn migrate_sqlite_locked(connection: &mut SqliteConnection) -> Result<i64, PersistenceError> {
    for statement in statements(SQLITE_INITIAL) {
        sqlx::query(statement)
            .execute(&mut *connection)
            .await
            .map_err(|_| PersistenceError::MigrationFailed)?;
    }
    let version: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM dashboard_schema_migration")
        .fetch_one(&mut *connection)
        .await
        .map_err(map_query_error)?;
    if version < 2 {
        migrate_sqlite_phase_two(connection).await?;
    }
    if version < 3 {
        for statement in statements(SQLITE_PHASE_THREE) {
            sqlx::query(statement)
                .execute(&mut *connection)
                .await
                .map_err(|_| PersistenceError::MigrationFailed)?;
        }
    }
    schema_version_sqlite_connection(connection).await
}

async fn migrate_sqlite_phase_two(connection: &mut SqliteConnection) -> Result<(), PersistenceError> {
    if !sqlite_column_exists(connection, "role").await? {
        sqlx::query(migration_statement(SQLITE_PHASE_TWO, 0)?)
            .execute(&mut *connection)
            .await
            .map_err(|_| PersistenceError::MigrationFailed)?;
    }
    // Existing active endpoints predate endpoint roles. Preserve the active
    // selection while backfilling the new role column before its constraints
    // are published.
    sqlx::query(migration_statement(SQLITE_PHASE_TWO, 1)?)
        .execute(&mut *connection)
        .await
        .map_err(|_| PersistenceError::MigrationFailed)?;
    if !sqlite_column_exists(connection, "is_enabled").await? {
        sqlx::query(migration_statement(SQLITE_PHASE_TWO, 2)?)
            .execute(&mut *connection)
            .await
            .map_err(|_| PersistenceError::MigrationFailed)?;
    }
    sqlx::query(migration_statement(SQLITE_PHASE_TWO, 3)?)
        .execute(&mut *connection)
        .await
        .map_err(|_| PersistenceError::MigrationFailed)?;
    sqlx::query(migration_statement(SQLITE_PHASE_TWO, 4)?)
        .execute(&mut *connection)
        .await
        .map_err(|_| PersistenceError::MigrationFailed)?;
    Ok(())
}

async fn migrate_mysql_locked(connection: &mut MySqlConnection) -> Result<i64, PersistenceError> {
    for statement in statements(MYSQL_INITIAL) {
        sqlx::query(statement)
            .execute(&mut *connection)
            .await
            .map_err(|_| PersistenceError::MigrationFailed)?;
    }
    let version: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM dashboard_schema_migration")
        .fetch_one(&mut *connection)
        .await
        .map_err(map_query_error)?;
    if version < 2 {
        if !mysql_column_exists(connection, "role").await? {
            sqlx::query(migration_statement(MYSQL_PHASE_TWO, 0)?)
                .execute(&mut *connection)
                .await
                .map_err(|_| PersistenceError::MigrationFailed)?;
        }
        sqlx::query(migration_statement(MYSQL_PHASE_TWO, 1)?)
            .execute(&mut *connection)
            .await
            .map_err(|_| PersistenceError::MigrationFailed)?;
        if !mysql_column_exists(connection, "is_enabled").await? {
            sqlx::query(migration_statement(MYSQL_PHASE_TWO, 2)?)
                .execute(&mut *connection)
                .await
                .map_err(|_| PersistenceError::MigrationFailed)?;
        }
        if !mysql_column_exists(connection, "active_endpoint_type").await? {
            sqlx::query(migration_statement(MYSQL_PHASE_TWO, 3)?)
                .execute(&mut *connection)
                .await
                .map_err(|_| PersistenceError::MigrationFailed)?;
        }
        if !mysql_index_exists(connection, "dashboard_endpoint_one_active_per_type_uq").await? {
            sqlx::query(migration_statement(MYSQL_PHASE_TWO, 4)?)
                .execute(&mut *connection)
                .await
                .map_err(|_| PersistenceError::MigrationFailed)?;
        }
        sqlx::query(migration_statement(MYSQL_PHASE_TWO, 5)?)
            .execute(&mut *connection)
            .await
            .map_err(|_| PersistenceError::MigrationFailed)?;
    }
    if version < 3 {
        for statement in statements(MYSQL_PHASE_THREE) {
            sqlx::query(statement)
                .execute(&mut *connection)
                .await
                .map_err(|_| PersistenceError::MigrationFailed)?;
        }
    }
    schema_version_mysql_connection(connection).await
}

async fn migrate_postgres_locked(connection: &mut PgConnection) -> Result<i64, PersistenceError> {
    let mut transaction = connection.begin().await.map_err(map_query_error)?;
    for statement in statements(POSTGRES_INITIAL) {
        sqlx::query(statement)
            .execute(&mut *transaction)
            .await
            .map_err(|_| PersistenceError::MigrationFailed)?;
    }
    let version: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM dashboard_schema_migration")
        .fetch_one(&mut *transaction)
        .await
        .map_err(map_query_error)?;
    if version < 2 {
        sqlx::query(migration_statement(POSTGRES_PHASE_TWO, 0)?)
            .execute(&mut *transaction)
            .await
            .map_err(|_| PersistenceError::MigrationFailed)?;
        sqlx::query(migration_statement(POSTGRES_PHASE_TWO, 1)?)
            .execute(&mut *transaction)
            .await
            .map_err(|_| PersistenceError::MigrationFailed)?;
        sqlx::query(migration_statement(POSTGRES_PHASE_TWO, 2)?)
            .execute(&mut *transaction)
            .await
            .map_err(|_| PersistenceError::MigrationFailed)?;
        sqlx::query(migration_statement(POSTGRES_PHASE_TWO, 3)?)
            .execute(&mut *transaction)
            .await
            .map_err(|_| PersistenceError::MigrationFailed)?;
        sqlx::query(migration_statement(POSTGRES_PHASE_TWO, 4)?)
            .execute(&mut *transaction)
            .await
            .map_err(|_| PersistenceError::MigrationFailed)?;
    }
    if version < 3 {
        for statement in statements(POSTGRES_PHASE_THREE) {
            sqlx::query(statement)
                .execute(&mut *transaction)
                .await
                .map_err(|_| PersistenceError::MigrationFailed)?;
        }
    }
    transaction.commit().await.map_err(map_query_error)?;
    schema_version_postgres_connection(connection).await
}

async fn sqlite_column_exists(connection: &mut SqliteConnection, column: &str) -> Result<bool, PersistenceError> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pragma_table_info('dashboard_endpoint') WHERE name = ?")
        .bind(column)
        .fetch_one(&mut *connection)
        .await
        .map_err(map_query_error)?;
    Ok(count > 0)
}

async fn mysql_column_exists(connection: &mut MySqlConnection, column: &str) -> Result<bool, PersistenceError> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.columns \
         WHERE table_schema = DATABASE() AND table_name = 'dashboard_endpoint' AND column_name = ?",
    )
    .bind(column)
    .fetch_one(&mut *connection)
    .await
    .map_err(map_query_error)?;
    Ok(count > 0)
}

async fn mysql_index_exists(connection: &mut MySqlConnection, index: &str) -> Result<bool, PersistenceError> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.statistics \
         WHERE table_schema = DATABASE() AND table_name = 'dashboard_endpoint' AND index_name = ?",
    )
    .bind(index)
    .fetch_one(&mut *connection)
    .await
    .map_err(map_query_error)?;
    Ok(count > 0)
}

async fn schema_version_mysql_connection(connection: &mut MySqlConnection) -> Result<i64, PersistenceError> {
    sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM dashboard_schema_migration")
        .fetch_one(&mut *connection)
        .await
        .map_err(map_query_error)
}

async fn schema_version_sqlite_connection(connection: &mut SqliteConnection) -> Result<i64, PersistenceError> {
    sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM dashboard_schema_migration")
        .fetch_one(&mut *connection)
        .await
        .map_err(map_query_error)
}

async fn schema_version_postgres_connection(connection: &mut PgConnection) -> Result<i64, PersistenceError> {
    sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM dashboard_schema_migration")
        .fetch_one(&mut *connection)
        .await
        .map_err(map_query_error)
}

pub async fn schema_version_sqlite(pool: &SqlitePool) -> Result<i64, PersistenceError> {
    sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM dashboard_schema_migration")
        .fetch_one(pool)
        .await
        .map_err(map_query_error)
}

pub async fn schema_version_mysql(pool: &MySqlPool) -> Result<i64, PersistenceError> {
    sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM dashboard_schema_migration")
        .fetch_one(pool)
        .await
        .map_err(map_query_error)
}

pub async fn schema_version_postgres(pool: &PgPool) -> Result<i64, PersistenceError> {
    sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM dashboard_schema_migration")
        .fetch_one(pool)
        .await
        .map_err(map_query_error)
}

fn statements(script: &str) -> impl Iterator<Item = &str> {
    script
        .split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
}

fn migration_statement(script: &str, index: usize) -> Result<&str, PersistenceError> {
    statements(script).nth(index).ok_or(PersistenceError::MigrationFailed)
}

fn map_query_error(error: sqlx::Error) -> PersistenceError {
    if matches!(error, sqlx::Error::PoolTimedOut) {
        PersistenceError::Timeout
    } else {
        PersistenceError::Query(error)
    }
}

#[cfg(test)]
mod tests {
    use super::{MYSQL_MIGRATION_LOCK, SQLITE_INITIAL, migrate_mysql, migrate_postgres, migrate_sqlite, statements};
    use sqlx::SqlitePool;
    use sqlx::mysql::MySqlPoolOptions;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use std::time::Duration;

    async fn sqlite_pool(path: &std::path::Path) -> SqlitePool {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .busy_timeout(Duration::from_secs(5));
        SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .expect("open SQLite pool")
    }

    async fn apply_sqlite_initial(pool: &SqlitePool) {
        for statement in statements(SQLITE_INITIAL) {
            sqlx::query(statement)
                .execute(pool)
                .await
                .expect("apply initial SQLite schema");
        }
    }

    #[test]
    fn migration_statement_splitter_keeps_each_ddl_statement() {
        let statements =
            statements("CREATE TABLE example (id INTEGER); INSERT INTO example VALUES (1);").collect::<Vec<_>>();
        assert_eq!(
            statements,
            vec!["CREATE TABLE example (id INTEGER)", "INSERT INTO example VALUES (1)"]
        );
    }

    #[tokio::test]
    async fn sqlite_phase_two_recovers_after_a_partial_ddl_state() {
        let directory = tempfile::tempdir().expect("temp directory");
        let pool = sqlite_pool(&directory.path().join("dashboard.db")).await;
        apply_sqlite_initial(&pool).await;
        sqlx::query(
            "INSERT INTO dashboard_environment (environment_id, name, use_vip_channel, use_tls, revision, created_at_ms, updated_at_ms) \
             VALUES ('sqlite-role-backfill', 'sqlite-role-backfill', 0, 0, 1, 0, 0)",
        )
        .execute(&pool)
        .await
        .expect("seed old SQLite environment");
        sqlx::query(
            "INSERT INTO dashboard_endpoint (endpoint_id, environment_id, endpoint_type, address, is_active, sort_order, created_at_ms, updated_at_ms) \
             VALUES ('sqlite-role-backfill-endpoint', 'sqlite-role-backfill', 'proxy', '127.0.0.1:8080', 1, 0, 0, 0)",
        )
        .execute(&pool)
        .await
        .expect("seed old active SQLite endpoint");
        sqlx::query("ALTER TABLE dashboard_endpoint ADD COLUMN role VARCHAR(32) NOT NULL DEFAULT 'secondary'")
            .execute(&pool)
            .await
            .expect("simulate the first phase-two DDL");

        assert_eq!(migrate_sqlite(&pool).await.expect("resume migration"), 3);
        let enabled_column: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pragma_table_info('dashboard_endpoint') WHERE name = 'is_enabled'",
        )
        .fetch_one(&pool)
        .await
        .expect("inspect is_enabled column");
        let active_index: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pragma_index_list('dashboard_endpoint') \
             WHERE name = 'dashboard_endpoint_one_active_per_type_uq'",
        )
        .fetch_one(&pool)
        .await
        .expect("inspect active endpoint index");
        let active_role: String = sqlx::query_scalar(
            "SELECT role FROM dashboard_endpoint WHERE endpoint_id = 'sqlite-role-backfill-endpoint'",
        )
        .fetch_one(&pool)
        .await
        .expect("inspect SQLite role backfill");
        assert_eq!(enabled_column, 1);
        assert_eq!(active_index, 1);
        assert_eq!(active_role, "primary");
    }

    #[tokio::test]
    async fn sqlite_concurrent_startup_serializes_the_phase_two_migration() {
        let directory = tempfile::tempdir().expect("temp directory");
        let database_path = directory.path().join("dashboard.db");
        let first = sqlite_pool(&database_path).await;
        let second = sqlite_pool(&database_path).await;
        apply_sqlite_initial(&first).await;

        let (first_version, second_version) = tokio::join!(migrate_sqlite(&first), migrate_sqlite(&second));
        assert_eq!(first_version.expect("first migration"), 3);
        assert_eq!(second_version.expect("second migration"), 3);
    }

    #[tokio::test]
    #[ignore = "requires docker-compose.storage-test.yml"]
    async fn mysql_phase_two_recovers_after_a_partial_ddl_state() {
        let url = std::env::var("ROCKETMQ_DASHBOARD_STORAGE_TEST_MYSQL_URL")
            .expect("ROCKETMQ_DASHBOARD_STORAGE_TEST_MYSQL_URL must be set by the storage test runner");
        let pool = MySqlPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("connect MySQL storage test database");
        assert_eq!(migrate_mysql(&pool).await.expect("prepare migration"), 3);

        // Leave the first phase-two DDL in place, then roll the remaining
        // schema changes back to emulate a connection loss between DDLs.
        // The next call must inspect information_schema and finish safely.
        let lock: i64 = sqlx::query_scalar("SELECT GET_LOCK(?, 30)")
            .bind(MYSQL_MIGRATION_LOCK)
            .fetch_one(&pool)
            .await
            .expect("acquire migration lock for partial DDL setup");
        assert_eq!(lock, 1, "migration setup must acquire the advisory lock");
        sqlx::query("DELETE FROM dashboard_schema_migration WHERE version >= 2")
            .execute(&pool)
            .await
            .expect("remove migration marker");
        sqlx::query("DROP INDEX dashboard_endpoint_one_active_per_type_uq ON dashboard_endpoint")
            .execute(&pool)
            .await
            .expect("drop active endpoint index");
        sqlx::query("ALTER TABLE dashboard_endpoint DROP COLUMN active_endpoint_type")
            .execute(&pool)
            .await
            .expect("drop generated active endpoint column");
        sqlx::query("ALTER TABLE dashboard_endpoint DROP COLUMN is_enabled")
            .execute(&pool)
            .await
            .expect("drop second phase-two column");
        let released: i64 = sqlx::query_scalar("SELECT RELEASE_LOCK(?)")
            .bind(MYSQL_MIGRATION_LOCK)
            .fetch_one(&pool)
            .await
            .expect("release migration lock after partial DDL setup");
        assert_eq!(released, 1, "migration setup must release the advisory lock");

        assert_eq!(migrate_mysql(&pool).await.expect("resume migration"), 3);
        let active_index: i64 = sqlx::query_scalar(
            "SELECT COUNT(DISTINCT index_name) FROM information_schema.statistics \
             WHERE table_schema = DATABASE() AND table_name = 'dashboard_endpoint' \
             AND index_name = 'dashboard_endpoint_one_active_per_type_uq'",
        )
        .fetch_one(&pool)
        .await
        .expect("inspect recovered active endpoint index");
        assert_eq!(active_index, 1);
    }

    #[tokio::test]
    #[ignore = "requires docker-compose.storage-test.yml"]
    async fn mysql_concurrent_startup_serializes_migration_work() {
        let url = std::env::var("ROCKETMQ_DASHBOARD_STORAGE_TEST_MYSQL_URL")
            .expect("ROCKETMQ_DASHBOARD_STORAGE_TEST_MYSQL_URL must be set by the storage test runner");
        let pool = MySqlPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .expect("connect MySQL storage test database");

        let (first, second) = tokio::join!(migrate_mysql(&pool), migrate_mysql(&pool));
        assert_eq!(first.expect("first concurrent migration"), 3);
        assert_eq!(second.expect("second concurrent migration"), 3);
    }

    #[tokio::test]
    #[ignore = "requires docker-compose.storage-test.yml"]
    async fn mysql_phase_two_backfills_active_endpoint_roles() {
        let url = std::env::var("ROCKETMQ_DASHBOARD_STORAGE_TEST_MYSQL_URL")
            .expect("ROCKETMQ_DASHBOARD_STORAGE_TEST_MYSQL_URL must be set by the storage test runner");
        let pool = MySqlPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("connect MySQL storage test database");
        assert_eq!(migrate_mysql(&pool).await.expect("prepare migration"), 3);
        let suffix = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
        let environment_id = format!("mbf-{suffix}");
        let endpoint_id = format!("mbe-{suffix}");
        sqlx::query(
            "INSERT INTO dashboard_environment (environment_id, name, use_vip_channel, use_tls, revision, created_at_ms, updated_at_ms) \
             VALUES (?, ?, FALSE, FALSE, 1, 0, 0)",
        )
        .bind(&environment_id)
        .bind(&environment_id)
        .execute(&pool)
        .await
        .expect("seed MySQL environment");
        sqlx::query(
            "INSERT INTO dashboard_endpoint (endpoint_id, environment_id, endpoint_type, address, role, is_enabled, is_active, sort_order, created_at_ms, updated_at_ms) \
             VALUES (?, ?, 'proxy', '127.0.0.1:8080', 'secondary', TRUE, TRUE, 0, 0, 0)",
        )
        .bind(&endpoint_id)
        .bind(&environment_id)
        .execute(&pool)
        .await
        .expect("seed active MySQL endpoint with legacy role");
        sqlx::query("DELETE FROM dashboard_schema_migration WHERE version >= 2")
            .execute(&pool)
            .await
            .expect("remove MySQL phase-two marker");
        assert_eq!(migrate_mysql(&pool).await.expect("rerun MySQL phase two"), 3);
        let role: String = sqlx::query_scalar("SELECT role FROM dashboard_endpoint WHERE endpoint_id = ?")
            .bind(&endpoint_id)
            .fetch_one(&pool)
            .await
            .expect("inspect MySQL role backfill");
        assert_eq!(role, "primary");
    }

    #[tokio::test]
    #[ignore = "requires docker-compose.storage-test.yml"]
    async fn postgres_concurrent_startup_serializes_migration_work() {
        let url = std::env::var("ROCKETMQ_DASHBOARD_STORAGE_TEST_POSTGRES_URL")
            .expect("ROCKETMQ_DASHBOARD_STORAGE_TEST_POSTGRES_URL must be set by the storage test runner");
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .expect("connect PostgreSQL storage test database");
        let (first, second) = tokio::join!(migrate_postgres(&pool), migrate_postgres(&pool));
        assert_eq!(first.expect("first PostgreSQL migration"), 3);
        assert_eq!(second.expect("second PostgreSQL migration"), 3);
    }

    #[tokio::test]
    #[ignore = "requires docker-compose.storage-test.yml"]
    async fn postgres_phase_two_backfills_active_endpoint_roles() {
        let url = std::env::var("ROCKETMQ_DASHBOARD_STORAGE_TEST_POSTGRES_URL")
            .expect("ROCKETMQ_DASHBOARD_STORAGE_TEST_POSTGRES_URL must be set by the storage test runner");
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("connect PostgreSQL storage test database");
        assert_eq!(migrate_postgres(&pool).await.expect("prepare migration"), 3);
        let suffix = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
        let environment_id = format!("pbf-{suffix}");
        let endpoint_id = format!("pbe-{suffix}");
        sqlx::query(
            "INSERT INTO dashboard_environment (environment_id, name, use_vip_channel, use_tls, revision, created_at_ms, updated_at_ms) \
             VALUES ($1, $2, FALSE, FALSE, 1, 0, 0)",
        )
        .bind(&environment_id)
        .bind(&environment_id)
        .execute(&pool)
        .await
        .expect("seed PostgreSQL environment");
        sqlx::query(
            "INSERT INTO dashboard_endpoint (endpoint_id, environment_id, endpoint_type, address, role, is_enabled, is_active, sort_order, created_at_ms, updated_at_ms) \
             VALUES ($1, $2, 'proxy', '127.0.0.1:8080', 'secondary', TRUE, TRUE, 0, 0, 0)",
        )
        .bind(&endpoint_id)
        .bind(&environment_id)
        .execute(&pool)
        .await
        .expect("seed active PostgreSQL endpoint with legacy role");
        sqlx::query("DELETE FROM dashboard_schema_migration WHERE version >= 2")
            .execute(&pool)
            .await
            .expect("remove PostgreSQL phase-two marker");
        assert_eq!(migrate_postgres(&pool).await.expect("rerun PostgreSQL phase two"), 3);
        let role: String = sqlx::query_scalar("SELECT role FROM dashboard_endpoint WHERE endpoint_id = $1")
            .bind(&endpoint_id)
            .fetch_one(&pool)
            .await
            .expect("inspect PostgreSQL role backfill");
        assert_eq!(role, "primary");
    }
}
