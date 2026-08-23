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
use sqlx::MySqlPool;
use sqlx::PgPool;
use sqlx::SqlitePool;

const SQLITE_INITIAL: &str = include_str!("../../migrations/sqlite/0001_initial.sql");
const MYSQL_INITIAL: &str = include_str!("../../migrations/mysql/0001_initial.sql");
const POSTGRES_INITIAL: &str = include_str!("../../migrations/postgres/0001_initial.sql");

pub async fn migrate_sqlite(pool: &SqlitePool) -> Result<i64, PersistenceError> {
    for statement in statements(SQLITE_INITIAL) {
        sqlx::query(statement)
            .execute(pool)
            .await
            .map_err(|_| PersistenceError::MigrationFailed)?;
    }
    schema_version_sqlite(pool).await
}

pub async fn migrate_mysql(pool: &MySqlPool) -> Result<i64, PersistenceError> {
    for statement in statements(MYSQL_INITIAL) {
        sqlx::query(statement)
            .execute(pool)
            .await
            .map_err(|_| PersistenceError::MigrationFailed)?;
    }
    schema_version_mysql(pool).await
}

pub async fn migrate_postgres(pool: &PgPool) -> Result<i64, PersistenceError> {
    for statement in statements(POSTGRES_INITIAL) {
        sqlx::query(statement)
            .execute(pool)
            .await
            .map_err(|_| PersistenceError::MigrationFailed)?;
    }
    schema_version_postgres(pool).await
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

fn map_query_error(error: sqlx::Error) -> PersistenceError {
    if matches!(error, sqlx::Error::PoolTimedOut) {
        PersistenceError::Timeout
    } else {
        PersistenceError::Query(error)
    }
}

#[cfg(test)]
mod tests {
    use super::statements;

    #[test]
    fn migration_statement_splitter_keeps_each_ddl_statement() {
        let statements =
            statements("CREATE TABLE example (id INTEGER); INSERT INTO example VALUES (1);").collect::<Vec<_>>();
        assert_eq!(
            statements,
            vec!["CREATE TABLE example (id INTEGER)", "INSERT INTO example VALUES (1)"]
        );
    }
}
