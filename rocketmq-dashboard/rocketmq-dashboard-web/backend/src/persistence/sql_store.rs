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
use crate::config::SqlPoolConfig;
use crate::config::StorageConfig;
use crate::model::ConsumerMonitorRule;
use crate::model::DashboardEnvironment;
use crate::model::Endpoint;
use crate::model::EndpointId;
use crate::model::EndpointRole;
use crate::model::EndpointType;
use crate::model::EnvironmentId;
use crate::model::StorageBackend;
use crate::persistence::Revision;
use crate::persistence::StorageHealth;
use crate::persistence::StorageMode;
use crate::persistence::StorageStatus;
use crate::persistence::environment_repository::validate_environment_identity;
use crate::persistence::environment_repository::validate_loaded_environment;
use crate::persistence::error::PersistenceError;
use crate::persistence::migration;
use rocketmq_runtime::ChildServiceContext;
use sqlx::Connection;
use sqlx::MySqlPool;
use sqlx::PgPool;
use sqlx::QueryBuilder;
use sqlx::Row;
use sqlx::SqlitePool;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::sqlite::SqliteJournalMode;
use sqlx::sqlite::SqlitePoolOptions;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::timeout;

pub enum DatabasePool {
    Sqlite(SqlitePool),
    MySql(MySqlPool),
    Postgres(PgPool),
}

pub struct SqlPersistence {
    pool: DatabasePool,
    backend: StorageBackend,
    schema_version: i64,
}

impl SqlPersistence {
    pub async fn initialize(
        config: &StorageConfig,
        service_context: ChildServiceContext,
    ) -> Result<Self, PersistenceError> {
        config
            .validate()
            .map_err(|error| PersistenceError::InvalidConfig(error.to_string()))?;
        match config.backend {
            StorageBackend::Sqlite => Self::initialize_sqlite(config, service_context).await,
            StorageBackend::MySql => Self::initialize_mysql(config).await,
            StorageBackend::Postgres => Self::initialize_postgres(config).await,
            StorageBackend::File => Err(PersistenceError::InvalidConfig(
                "File storage does not use a SQL pool".to_string(),
            )),
        }
    }

    pub const fn storage_backend(&self) -> StorageBackend {
        self.backend
    }

    pub const fn schema_version(&self) -> i64 {
        self.schema_version
    }

    pub fn sqlite_pool(&self) -> Option<&SqlitePool> {
        match &self.pool {
            DatabasePool::Sqlite(pool) => Some(pool),
            DatabasePool::MySql(_) | DatabasePool::Postgres(_) => None,
        }
    }

    pub fn mysql_pool(&self) -> Option<&MySqlPool> {
        match &self.pool {
            DatabasePool::MySql(pool) => Some(pool),
            DatabasePool::Sqlite(_) | DatabasePool::Postgres(_) => None,
        }
    }

    pub fn postgres_pool(&self) -> Option<&PgPool> {
        match &self.pool {
            DatabasePool::Postgres(pool) => Some(pool),
            DatabasePool::Sqlite(_) | DatabasePool::MySql(_) => None,
        }
    }

    pub async fn storage_health(&self) -> StorageHealth {
        let schema_version = self.probe().await.ok();
        let (pool_size, idle_connections) = self.pool_metrics();
        StorageHealth {
            backend: self.backend,
            mode: match self.backend {
                StorageBackend::Sqlite => StorageMode::SingleNode,
                StorageBackend::MySql | StorageBackend::Postgres => StorageMode::MultiNode,
                StorageBackend::File => StorageMode::SingleNode,
            },
            status: if schema_version.is_some() {
                StorageStatus::Available
            } else {
                StorageStatus::Unavailable
            },
            schema_version,
            last_successful_write_at: None,
            available_bytes: None,
            pool_size: Some(pool_size),
            idle_connections: Some(idle_connections),
        }
    }

    fn pool_metrics(&self) -> (u32, usize) {
        match &self.pool {
            DatabasePool::Sqlite(pool) => (pool.size(), pool.num_idle()),
            DatabasePool::MySql(pool) => (pool.size(), pool.num_idle()),
            DatabasePool::Postgres(pool) => (pool.size(), pool.num_idle()),
        }
    }

    async fn initialize_sqlite(
        config: &StorageConfig,
        service_context: ChildServiceContext,
    ) -> Result<Self, PersistenceError> {
        let database_path = config.data_path.clone();
        ensure_sqlite_parent(&service_context, database_path.clone()).await?;
        let options = SqliteConnectOptions::new()
            .filename(database_path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_millis(config.pool.acquire_timeout_ms));
        let pool = connect_with_timeout(
            config.pool.connect_timeout_ms,
            sqlite_pool_options(&config.pool).connect_with(options),
        )
        .await?;
        reject_legacy_sqlite_layout(&pool).await?;
        let schema_version = migration::migrate_sqlite(&pool).await?;
        let persistence = Self {
            pool: DatabasePool::Sqlite(pool),
            backend: StorageBackend::Sqlite,
            schema_version,
        };
        persistence.probe().await?;
        Ok(persistence)
    }

    async fn initialize_mysql(config: &StorageConfig) -> Result<Self, PersistenceError> {
        let url = config
            .database_url
            .as_deref()
            .ok_or_else(|| PersistenceError::InvalidConfig("MySQL URL is missing".to_string()))?;
        let pool = connect_with_timeout(
            config.pool.connect_timeout_ms,
            mysql_pool_options(&config.pool).connect(url),
        )
        .await?;
        let schema_version = migration::migrate_mysql(&pool).await?;
        let persistence = Self {
            pool: DatabasePool::MySql(pool),
            backend: StorageBackend::MySql,
            schema_version,
        };
        persistence.probe().await?;
        Ok(persistence)
    }

    async fn initialize_postgres(config: &StorageConfig) -> Result<Self, PersistenceError> {
        let url = config
            .database_url
            .as_deref()
            .ok_or_else(|| PersistenceError::InvalidConfig("PostgreSQL URL is missing".to_string()))?;
        let pool = connect_with_timeout(
            config.pool.connect_timeout_ms,
            postgres_pool_options(&config.pool).connect(url),
        )
        .await?;
        let schema_version = migration::migrate_postgres(&pool).await?;
        let persistence = Self {
            pool: DatabasePool::Postgres(pool),
            backend: StorageBackend::Postgres,
            schema_version,
        };
        persistence.probe().await?;
        Ok(persistence)
    }

    async fn probe(&self) -> Result<i64, PersistenceError> {
        let schema_version = match &self.pool {
            DatabasePool::Sqlite(pool) => {
                sqlx::query("SELECT 1")
                    .execute(pool)
                    .await
                    .map_err(map_connection_error)?;
                migration::schema_version_sqlite(pool).await?
            }
            DatabasePool::MySql(pool) => {
                sqlx::query("SELECT 1")
                    .execute(pool)
                    .await
                    .map_err(map_connection_error)?;
                migration::schema_version_mysql(pool).await?
            }
            DatabasePool::Postgres(pool) => {
                sqlx::query("SELECT 1")
                    .execute(pool)
                    .await
                    .map_err(map_connection_error)?;
                migration::schema_version_postgres(pool).await?
            }
        };
        if schema_version == self.schema_version {
            Ok(schema_version)
        } else {
            Err(PersistenceError::MigrationFailed)
        }
    }
}

async fn reject_legacy_sqlite_layout(pool: &SqlitePool) -> Result<(), PersistenceError> {
    let legacy_table_exists: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'dashboard_config')",
    )
    .fetch_one(pool)
    .await
    .map_err(map_connection_error)?;
    if legacy_table_exists != 0 {
        Err(PersistenceError::UnsupportedLayout)
    } else {
        Ok(())
    }
}

async fn ensure_sqlite_parent(
    service_context: &ChildServiceContext,
    database_path: PathBuf,
) -> Result<(), PersistenceError> {
    service_context
        .storage_io()
        .spawn_io("dashboard-sqlite-storage-directory", move || {
            if let Some(parent) = database_path.parent() {
                std::fs::create_dir_all(parent).map_err(PersistenceError::Io)?;
            }
            Ok(())
        })
        .await
        .map_err(PersistenceError::Runtime)?
}

fn sqlite_pool_options(config: &SqlPoolConfig) -> SqlitePoolOptions {
    SqlitePoolOptions::new()
        .min_connections(config.min_connections)
        .max_connections(config.max_connections)
        .acquire_timeout(Duration::from_millis(config.acquire_timeout_ms))
        .idle_timeout(Some(Duration::from_secs(config.idle_timeout_secs)))
        .max_lifetime(Some(Duration::from_secs(config.max_lifetime_secs)))
}

fn mysql_pool_options(config: &SqlPoolConfig) -> MySqlPoolOptions {
    MySqlPoolOptions::new()
        .min_connections(config.min_connections)
        .max_connections(config.max_connections)
        .acquire_timeout(Duration::from_millis(config.acquire_timeout_ms))
        .idle_timeout(Some(Duration::from_secs(config.idle_timeout_secs)))
        .max_lifetime(Some(Duration::from_secs(config.max_lifetime_secs)))
}

fn postgres_pool_options(config: &SqlPoolConfig) -> PgPoolOptions {
    PgPoolOptions::new()
        .min_connections(config.min_connections)
        .max_connections(config.max_connections)
        .acquire_timeout(Duration::from_millis(config.acquire_timeout_ms))
        .idle_timeout(Some(Duration::from_secs(config.idle_timeout_secs)))
        .max_lifetime(Some(Duration::from_secs(config.max_lifetime_secs)))
}

fn map_connection_error(error: sqlx::Error) -> PersistenceError {
    if matches!(error, sqlx::Error::PoolTimedOut) {
        PersistenceError::Timeout
    } else {
        PersistenceError::ConnectionUnavailable
    }
}

async fn connect_with_timeout<T>(
    timeout_ms: u64,
    connection: impl std::future::Future<Output = Result<T, sqlx::Error>>,
) -> Result<T, PersistenceError> {
    timeout(Duration::from_millis(timeout_ms), connection)
        .await
        .map_err(|_| PersistenceError::Timeout)?
        .map_err(map_connection_error)
}

impl SqlPersistence {
    pub(crate) async fn load_environment(
        &self,
        environment_id: &EnvironmentId,
    ) -> Result<DashboardEnvironment, PersistenceError> {
        let environment = match &self.pool {
            DatabasePool::Sqlite(pool) => {
                load_environment_in_sqlite_transaction(pool, "environment_id", &environment_id.0).await
            }
            DatabasePool::MySql(pool) => {
                load_environment_in_mysql_transaction(pool, "environment_id", &environment_id.0).await
            }
            DatabasePool::Postgres(pool) => {
                load_environment_in_postgres_transaction(pool, "environment_id", &environment_id.0).await
            }
        }?;
        environment.ok_or(PersistenceError::NotFound)
    }

    pub(crate) async fn load_environment_by_name(
        &self,
        name: &str,
    ) -> Result<Option<DashboardEnvironment>, PersistenceError> {
        match &self.pool {
            DatabasePool::Sqlite(pool) => load_environment_in_sqlite_transaction(pool, "name", name).await,
            DatabasePool::MySql(pool) => load_environment_in_mysql_transaction(pool, "name", name).await,
            DatabasePool::Postgres(pool) => load_environment_in_postgres_transaction(pool, "name", name).await,
        }
    }

    pub(crate) async fn list_environments(&self) -> Result<Vec<DashboardEnvironment>, PersistenceError> {
        match &self.pool {
            DatabasePool::Sqlite(pool) => list_environments_in_sqlite_transaction(pool).await,
            DatabasePool::MySql(pool) => list_environments_in_mysql_transaction(pool).await,
            DatabasePool::Postgres(pool) => list_environments_in_postgres_transaction(pool).await,
        }
    }

    pub(crate) async fn create_environment(
        &self,
        environment: DashboardEnvironment,
    ) -> Result<DashboardEnvironment, PersistenceError> {
        environment.validate().map_err(PersistenceError::InvalidConfig)?;
        validate_environment_identity(&environment)?;
        if environment.revision != Revision(1) {
            return Err(PersistenceError::InvalidConfig(
                "new environments must start at revision 1".to_string(),
            ));
        }
        match &self.pool {
            DatabasePool::Sqlite(pool) => crate::insert_environment_in_pool!(pool, &environment),
            DatabasePool::MySql(pool) => crate::insert_environment_in_pool!(pool, &environment),
            DatabasePool::Postgres(pool) => crate::insert_environment_in_pool!(pool, &environment),
        }?;
        Ok(environment)
    }

    pub(crate) async fn update_environment(
        &self,
        expected_revision: Revision,
        mut candidate: DashboardEnvironment,
    ) -> Result<DashboardEnvironment, PersistenceError> {
        candidate.validate().map_err(PersistenceError::InvalidConfig)?;
        validate_environment_identity(&candidate)?;
        candidate.revision = Revision(expected_revision.0.checked_add(1).ok_or(PersistenceError::Conflict)?);
        match &self.pool {
            DatabasePool::Sqlite(pool) => crate::update_environment_in_pool!(pool, expected_revision, &candidate),
            DatabasePool::MySql(pool) => crate::update_environment_in_pool!(pool, expected_revision, &candidate),
            DatabasePool::Postgres(pool) => crate::update_environment_in_pool!(pool, expected_revision, &candidate),
        }?;
        Ok(candidate)
    }

    pub(crate) async fn delete_environment(
        &self,
        environment_id: &EnvironmentId,
        expected_revision: Revision,
    ) -> Result<bool, PersistenceError> {
        match &self.pool {
            DatabasePool::Sqlite(pool) => {
                crate::delete_environment_in_pool!(pool, environment_id, expected_revision)
            }
            DatabasePool::MySql(pool) => {
                crate::delete_environment_in_pool!(pool, environment_id, expected_revision)
            }
            DatabasePool::Postgres(pool) => {
                crate::delete_environment_in_pool!(pool, environment_id, expected_revision)
            }
        }
    }

    pub(crate) async fn list_monitor_rules(
        &self,
        environment_id: &EnvironmentId,
    ) -> Result<Vec<ConsumerMonitorRule>, PersistenceError> {
        match &self.pool {
            DatabasePool::Sqlite(pool) => list_monitor_rules_in_sqlite_transaction(pool, environment_id).await,
            DatabasePool::MySql(pool) => list_monitor_rules_in_mysql_transaction(pool, environment_id).await,
            DatabasePool::Postgres(pool) => list_monitor_rules_in_postgres_transaction(pool, environment_id).await,
        }
    }

    pub(crate) async fn upsert_monitor_rule(
        &self,
        rule: ConsumerMonitorRule,
        expected_revision: Revision,
    ) -> Result<ConsumerMonitorRule, PersistenceError> {
        rule.validate().map_err(PersistenceError::InvalidConfig)?;
        match &self.pool {
            DatabasePool::Sqlite(pool) => {
                upsert_monitor_rule_in_sqlite_transaction(pool, rule, expected_revision).await
            }
            DatabasePool::MySql(pool) => crate::upsert_monitor_rule_in_pool!(pool, rule, expected_revision),
            DatabasePool::Postgres(pool) => crate::upsert_monitor_rule_in_pool!(pool, rule, expected_revision),
        }
    }

    pub(crate) async fn delete_monitor_rule(
        &self,
        environment_id: &EnvironmentId,
        consumer_group: &str,
        expected_revision: Revision,
    ) -> Result<bool, PersistenceError> {
        if consumer_group.trim().is_empty() {
            return Err(PersistenceError::InvalidConfig(
                "consumer group is required".to_string(),
            ));
        }
        match &self.pool {
            DatabasePool::Sqlite(pool) => {
                crate::delete_monitor_rule_in_pool!(pool, environment_id, consumer_group, expected_revision)
            }
            DatabasePool::MySql(pool) => {
                crate::delete_monitor_rule_in_pool!(pool, environment_id, consumer_group, expected_revision)
            }
            DatabasePool::Postgres(pool) => {
                crate::delete_monitor_rule_in_pool!(pool, environment_id, consumer_group, expected_revision)
            }
        }
    }
}

async fn load_environment_in_sqlite_transaction(
    pool: &SqlitePool,
    column: &'static str,
    value: &str,
) -> Result<Option<DashboardEnvironment>, PersistenceError> {
    let mut transaction = pool.begin().await.map_err(map_query_error)?;
    let environment = load_environment_from_sqlite_connection(&mut transaction, column, value).await?;
    transaction.commit().await.map_err(map_query_error)?;
    Ok(environment)
}

async fn load_environment_in_mysql_transaction(
    pool: &MySqlPool,
    column: &'static str,
    value: &str,
) -> Result<Option<DashboardEnvironment>, PersistenceError> {
    // A consistent snapshot is meaningful only at REPEATABLE READ. Set this
    // for the next transaction on the borrowed connection instead of relying
    // on a server default that may be READ COMMITTED.
    let mut connection = pool.acquire().await.map_err(map_query_error)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *connection)
        .await
        .map_err(map_query_error)?;
    let mut transaction = connection
        .begin_with("START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .await
        .map_err(map_query_error)?;
    let result = load_environment_from_mysql_connection(&mut transaction, column, value).await;
    match result {
        Ok(environment) => {
            transaction.commit().await.map_err(map_query_error)?;
            Ok(environment)
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

async fn load_environment_in_postgres_transaction(
    pool: &PgPool,
    column: &'static str,
    value: &str,
) -> Result<Option<DashboardEnvironment>, PersistenceError> {
    let mut transaction = pool
        .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await
        .map_err(map_query_error)?;
    let result = load_environment_from_postgres_connection(&mut transaction, column, value).await;
    match result {
        Ok(environment) => {
            transaction.commit().await.map_err(map_query_error)?;
            Ok(environment)
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

async fn list_environments_in_sqlite_transaction(
    pool: &SqlitePool,
) -> Result<Vec<DashboardEnvironment>, PersistenceError> {
    let mut transaction = pool.begin().await.map_err(map_query_error)?;
    let result = async {
        let ids: Vec<String> =
            sqlx::query_scalar("SELECT environment_id FROM dashboard_environment ORDER BY name, environment_id")
                .fetch_all(&mut *transaction)
                .await
                .map_err(map_query_error)?;
        let mut environments = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(environment) =
                load_environment_from_sqlite_connection(&mut transaction, "environment_id", &id).await?
            {
                environments.push(environment);
            }
        }
        Ok(environments)
    }
    .await;
    match result {
        Ok(environments) => {
            transaction.commit().await.map_err(map_query_error)?;
            Ok(environments)
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

async fn list_environments_in_mysql_transaction(
    pool: &MySqlPool,
) -> Result<Vec<DashboardEnvironment>, PersistenceError> {
    let mut connection = pool.acquire().await.map_err(map_query_error)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *connection)
        .await
        .map_err(map_query_error)?;
    let mut transaction = connection
        .begin_with("START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .await
        .map_err(map_query_error)?;
    let result = async {
        let ids: Vec<String> =
            sqlx::query_scalar("SELECT environment_id FROM dashboard_environment ORDER BY name, environment_id")
                .fetch_all(&mut *transaction)
                .await
                .map_err(map_query_error)?;
        let mut environments = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(environment) =
                load_environment_from_mysql_connection(&mut transaction, "environment_id", &id).await?
            {
                environments.push(environment);
            }
        }
        Ok(environments)
    }
    .await;
    match result {
        Ok(environments) => {
            transaction.commit().await.map_err(map_query_error)?;
            Ok(environments)
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

async fn list_environments_in_postgres_transaction(
    pool: &PgPool,
) -> Result<Vec<DashboardEnvironment>, PersistenceError> {
    let mut transaction = pool
        .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await
        .map_err(map_query_error)?;
    let result = async {
        let ids: Vec<String> =
            sqlx::query_scalar("SELECT environment_id FROM dashboard_environment ORDER BY name, environment_id")
                .fetch_all(&mut *transaction)
                .await
                .map_err(map_query_error)?;
        let mut environments = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(environment) =
                load_environment_from_postgres_connection(&mut transaction, "environment_id", &id).await?
            {
                environments.push(environment);
            }
        }
        Ok(environments)
    }
    .await;
    match result {
        Ok(environments) => {
            transaction.commit().await.map_err(map_query_error)?;
            Ok(environments)
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

macro_rules! load_monitor_rules_from_transaction {
    ($transaction:expr, $environment_id:expr) => {{
        let mut environment = QueryBuilder::new("SELECT 1 FROM dashboard_environment WHERE environment_id = ");
        environment.push_bind(&$environment_id.0);
        if environment
            .build()
            .fetch_optional(&mut *$transaction)
            .await
            .map_err(map_query_error)?
            .is_none()
        {
            Err(PersistenceError::NotFound)
        } else {
            let mut query = QueryBuilder::new(
                "SELECT consumer_group, min_count, max_diff_total, revision, created_at_ms, updated_at_ms \
                 FROM consumer_monitor_rule WHERE environment_id = ",
            );
            query.push_bind(&$environment_id.0);
            query.push(" ORDER BY consumer_group");
            query
                .build()
                .fetch_all(&mut *$transaction)
                .await
                .map_err(map_query_error)?
                .into_iter()
                .map(|row| {
                    Ok(ConsumerMonitorRule {
                        environment_id: $environment_id.clone(),
                        consumer_group: row.try_get("consumer_group").map_err(map_query_error)?,
                        min_count: row.try_get("min_count").map_err(map_query_error)?,
                        max_diff_total: row.try_get("max_diff_total").map_err(map_query_error)?,
                        revision: revision_from_database(row.try_get("revision").map_err(map_query_error)?)?,
                        created_at_ms: row.try_get("created_at_ms").map_err(map_query_error)?,
                        updated_at_ms: row.try_get("updated_at_ms").map_err(map_query_error)?,
                    })
                })
                .collect::<Result<Vec<_>, PersistenceError>>()
        }
    }};
}

async fn list_monitor_rules_in_sqlite_transaction(
    pool: &SqlitePool,
    environment_id: &EnvironmentId,
) -> Result<Vec<ConsumerMonitorRule>, PersistenceError> {
    let mut transaction = pool.begin().await.map_err(map_query_error)?;
    let result = load_monitor_rules_from_transaction!(transaction, environment_id);
    match result {
        Ok(rules) => {
            transaction.commit().await.map_err(map_query_error)?;
            Ok(rules)
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

async fn list_monitor_rules_in_mysql_transaction(
    pool: &MySqlPool,
    environment_id: &EnvironmentId,
) -> Result<Vec<ConsumerMonitorRule>, PersistenceError> {
    let mut connection = pool.acquire().await.map_err(map_query_error)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *connection)
        .await
        .map_err(map_query_error)?;
    let mut transaction = connection
        .begin_with("START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .await
        .map_err(map_query_error)?;
    let result = load_monitor_rules_from_transaction!(transaction, environment_id);
    match result {
        Ok(rules) => {
            transaction.commit().await.map_err(map_query_error)?;
            Ok(rules)
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

async fn list_monitor_rules_in_postgres_transaction(
    pool: &PgPool,
    environment_id: &EnvironmentId,
) -> Result<Vec<ConsumerMonitorRule>, PersistenceError> {
    let mut transaction = pool
        .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await
        .map_err(map_query_error)?;
    let result = load_monitor_rules_from_transaction!(transaction, environment_id);
    match result {
        Ok(rules) => {
            transaction.commit().await.map_err(map_query_error)?;
            Ok(rules)
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

async fn load_environment_from_sqlite_connection(
    connection: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    column: &'static str,
    value: &str,
) -> Result<Option<DashboardEnvironment>, PersistenceError> {
    match column {
        "environment_id" => crate::load_environment_from_pool!(&mut **connection, "environment_id", value),
        "name" => crate::load_environment_from_pool!(&mut **connection, "name", value),
        _ => Err(PersistenceError::InvalidConfig(
            "invalid environment lookup column".to_string(),
        )),
    }
}

async fn load_environment_from_mysql_connection(
    connection: &mut sqlx::Transaction<'_, sqlx::MySql>,
    column: &'static str,
    value: &str,
) -> Result<Option<DashboardEnvironment>, PersistenceError> {
    match column {
        "environment_id" => crate::load_environment_from_pool!(&mut **connection, "environment_id", value),
        "name" => crate::load_environment_from_pool!(&mut **connection, "name", value),
        _ => Err(PersistenceError::InvalidConfig(
            "invalid environment lookup column".to_string(),
        )),
    }
}

async fn load_environment_from_postgres_connection(
    connection: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    column: &'static str,
    value: &str,
) -> Result<Option<DashboardEnvironment>, PersistenceError> {
    match column {
        "environment_id" => crate::load_environment_from_pool!(&mut **connection, "environment_id", value),
        "name" => crate::load_environment_from_pool!(&mut **connection, "name", value),
        _ => Err(PersistenceError::InvalidConfig(
            "invalid environment lookup column".to_string(),
        )),
    }
}

#[macro_export]
macro_rules! load_environment_from_pool {
    ($pool:expr, $column:literal, $value:expr) => {{
        let mut environment_query = QueryBuilder::new(concat!(
            "SELECT environment_id, name, use_vip_channel, use_tls, revision, created_at_ms, updated_at_ms ",
            "FROM dashboard_environment WHERE ",
            $column,
            " = "
        ));
        environment_query.push_bind($value);
        if let Some(environment_row) = environment_query
            .build()
            .fetch_optional($pool)
            .await
            .map_err(map_query_error)?
        {
        let environment_id: String = environment_row.try_get("environment_id").map_err(map_query_error)?;
        let revision: i64 = environment_row.try_get("revision").map_err(map_query_error)?;
        let mut endpoint_query = QueryBuilder::new(
            "SELECT endpoint_id, endpoint_type, address, role, is_enabled, is_active, sort_order, created_at_ms, updated_at_ms \
             FROM dashboard_endpoint WHERE environment_id = ",
        );
        endpoint_query.push_bind(&environment_id);
        endpoint_query.push(
            " ORDER BY CASE endpoint_type WHEN 'nameserver' THEN 0 WHEN 'proxy' THEN 1 ELSE 2 END, \
             sort_order, endpoint_id",
        );
        let endpoints = endpoint_query
            .build()
            .fetch_all($pool)
            .await
            .map_err(map_query_error)?
            .into_iter()
            .map(|row| {
                let endpoint_type: String = row.try_get("endpoint_type").map_err(map_query_error)?;
                let role: String = row.try_get("role").map_err(map_query_error)?;
                Ok(Endpoint {
                    endpoint_id: EndpointId(row.try_get("endpoint_id").map_err(map_query_error)?),
                    endpoint_type: EndpointType::parse(&endpoint_type).map_err(|_| PersistenceError::CorruptedData)?,
                    address: row.try_get("address").map_err(map_query_error)?,
                    role: EndpointRole::parse(&role).map_err(|_| PersistenceError::CorruptedData)?,
                    is_enabled: row.try_get("is_enabled").map_err(map_query_error)?,
                    is_active: row.try_get("is_active").map_err(map_query_error)?,
                    sort_order: row.try_get("sort_order").map_err(map_query_error)?,
                    created_at_ms: row.try_get("created_at_ms").map_err(map_query_error)?,
                    updated_at_ms: row.try_get("updated_at_ms").map_err(map_query_error)?,
                })
            })
            .collect::<Result<Vec<_>, PersistenceError>>()?;
        let environment = DashboardEnvironment {
            environment_id: EnvironmentId(environment_id),
            name: environment_row.try_get("name").map_err(map_query_error)?,
            use_vip_channel: environment_row.try_get("use_vip_channel").map_err(map_query_error)?,
            use_tls: environment_row.try_get("use_tls").map_err(map_query_error)?,
            revision: revision_from_database(revision)?,
            created_at_ms: environment_row.try_get("created_at_ms").map_err(map_query_error)?,
            updated_at_ms: environment_row.try_get("updated_at_ms").map_err(map_query_error)?,
            endpoints,
        };
        Ok(Some(validate_loaded_environment(environment)?))
        } else {
            Ok(None)
        }
    }};
}

#[macro_export]
macro_rules! insert_endpoints_in_transaction {
    ($transaction:expr, $environment:expr) => {{
        for endpoint in &$environment.endpoints {
            let mut query = QueryBuilder::new(
                "INSERT INTO dashboard_endpoint (endpoint_id, environment_id, endpoint_type, address, role, is_enabled, is_active, \
                 sort_order, created_at_ms, updated_at_ms) VALUES (",
            );
            let mut separated = query.separated(", ");
            separated.push_bind(&endpoint.endpoint_id.0);
            separated.push_bind(&$environment.environment_id.0);
            separated.push_bind(endpoint.endpoint_type.as_str());
            separated.push_bind(&endpoint.address);
            separated.push_bind(endpoint.role.as_str());
            separated.push_bind(endpoint.is_enabled);
            separated.push_bind(endpoint.is_active);
            separated.push_bind(endpoint.sort_order);
            separated.push_bind(endpoint.created_at_ms);
            separated.push_bind(endpoint.updated_at_ms);
            separated.push_unseparated(")");
            query.build().execute(&mut *$transaction).await.map_err(map_query_error)?;
        }
    }};
}

#[macro_export]
macro_rules! insert_environment_in_pool {
    ($pool:expr, $environment:expr) => {{
        let mut transaction = $pool.begin().await.map_err(map_query_error)?;
        let mut query = QueryBuilder::new(
            "INSERT INTO dashboard_environment (environment_id, name, use_vip_channel, use_tls, revision, \
             created_at_ms, updated_at_ms) VALUES (",
        );
        let mut separated = query.separated(", ");
        separated.push_bind(&$environment.environment_id.0);
        separated.push_bind(&$environment.name);
        separated.push_bind($environment.use_vip_channel);
        separated.push_bind($environment.use_tls);
        separated.push_bind(revision_to_database($environment.revision)?);
        separated.push_bind($environment.created_at_ms);
        separated.push_bind($environment.updated_at_ms);
        separated.push_unseparated(")");
        query
            .build()
            .execute(&mut *transaction)
            .await
            .map_err(map_query_error)?;
        $crate::insert_endpoints_in_transaction!(transaction, $environment);
        transaction.commit().await.map_err(map_query_error)
    }};
}

#[macro_export]
macro_rules! update_environment_in_pool {
    ($pool:expr, $expected_revision:expr, $environment:expr) => {{
        let mut transaction = $pool.begin().await.map_err(map_query_error)?;
        let mut update = QueryBuilder::new("UPDATE dashboard_environment SET name = ");
        update.push_bind(&$environment.name);
        update.push(", use_vip_channel = ");
        update.push_bind($environment.use_vip_channel);
        update.push(", use_tls = ");
        update.push_bind($environment.use_tls);
        update.push(", revision = ");
        update.push_bind(revision_to_database($environment.revision)?);
        update.push(", updated_at_ms = ");
        update.push_bind($environment.updated_at_ms);
        update.push(" WHERE environment_id = ");
        update.push_bind(&$environment.environment_id.0);
        update.push(" AND revision = ");
        update.push_bind(revision_to_database($expected_revision)?);
        if update
            .build()
            .execute(&mut *transaction)
            .await
            .map_err(map_query_error)?
            .rows_affected()
            != 1
        {
            return Err(PersistenceError::Conflict);
        }
        let mut delete = QueryBuilder::new("DELETE FROM dashboard_endpoint WHERE environment_id = ");
        delete.push_bind(&$environment.environment_id.0);
        delete
            .build()
            .execute(&mut *transaction)
            .await
            .map_err(map_query_error)?;
        $crate::insert_endpoints_in_transaction!(transaction, $environment);
        transaction.commit().await.map_err(map_query_error)
    }};
}

#[macro_export]
macro_rules! delete_environment_in_pool {
    ($pool:expr, $environment_id:expr, $expected_revision:expr) => {{
        let mut transaction = $pool.begin().await.map_err(map_query_error)?;
        let mut delete = QueryBuilder::new("DELETE FROM dashboard_environment WHERE environment_id = ");
        delete.push_bind(&$environment_id.0);
        delete.push(" AND revision = ");
        delete.push_bind(revision_to_database($expected_revision)?);
        let deleted = delete
            .build()
            .execute(&mut *transaction)
            .await
            .map_err(map_query_error)?
            .rows_affected();
        if deleted == 1 {
            transaction.commit().await.map_err(map_query_error)?;
            Ok(true)
        } else {
            let mut exists = QueryBuilder::new("SELECT 1 FROM dashboard_environment WHERE environment_id = ");
            exists.push_bind(&$environment_id.0);
            let exists = exists
                .build()
                .fetch_optional(&mut *transaction)
                .await
                .map_err(map_query_error)?
                .is_some();
            if exists {
                Err(PersistenceError::Conflict)
            } else {
                transaction.commit().await.map_err(map_query_error)?;
                Ok(false)
            }
        }
    }};
}

#[macro_export]
macro_rules! upsert_monitor_rule_in_pool {
    ($pool:expr, $rule:expr, $expected_revision:expr) => {{
        let mut transaction = $pool.begin().await.map_err(map_query_error)?;
        $crate::upsert_monitor_rule_in_transaction!(transaction, $rule, $expected_revision)
    }};
}

#[macro_export]
macro_rules! upsert_monitor_rule_in_transaction {
    ($transaction:ident, $rule:expr, $expected_revision:expr) => {{
        let mut environment = QueryBuilder::new("SELECT 1 FROM dashboard_environment WHERE environment_id = ");
        environment.push_bind(&$rule.environment_id.0);
        if environment
            .build()
            .fetch_optional(&mut *$transaction)
            .await
            .map_err(map_query_error)?
            .is_none()
        {
            return Err(PersistenceError::NotFound);
        }
        let mut current_query = QueryBuilder::new(
            "SELECT revision, created_at_ms FROM consumer_monitor_rule WHERE environment_id = ",
        );
        current_query.push_bind(&$rule.environment_id.0);
        current_query.push(" AND consumer_group = ");
        current_query.push_bind(&$rule.consumer_group);
        let current = current_query
            .build()
            .fetch_optional(&mut *$transaction)
            .await
            .map_err(map_query_error)?;
        let now_ms = chrono::Utc::now().timestamp_millis();
        let mut persisted = $rule;
        if let Some(current) = current {
            let current_revision = revision_from_database(current.try_get("revision").map_err(map_query_error)?)?;
            if current_revision != $expected_revision {
                return Err(PersistenceError::Conflict);
            }
            persisted.revision = Revision(current_revision.0.checked_add(1).ok_or(PersistenceError::Conflict)?);
            persisted.created_at_ms = current.try_get("created_at_ms").map_err(map_query_error)?;
            persisted.updated_at_ms = now_ms;
            let mut update = QueryBuilder::new("UPDATE consumer_monitor_rule SET min_count = ");
            update.push_bind(persisted.min_count);
            update.push(", max_diff_total = ");
            update.push_bind(persisted.max_diff_total);
            update.push(", revision = ");
            update.push_bind(revision_to_database(persisted.revision)?);
            update.push(", updated_at_ms = ");
            update.push_bind(persisted.updated_at_ms);
            update.push(" WHERE environment_id = ");
            update.push_bind(&persisted.environment_id.0);
            update.push(" AND consumer_group = ");
            update.push_bind(&persisted.consumer_group);
            update.push(" AND revision = ");
            update.push_bind(revision_to_database($expected_revision)?);
            if update.build().execute(&mut *$transaction).await.map_err(map_query_error)?.rows_affected() != 1 {
                return Err(PersistenceError::Conflict);
            }
        } else {
            if $expected_revision != Revision(0) {
                return Err(PersistenceError::Conflict);
            }
            persisted.revision = Revision(1);
            persisted.created_at_ms = now_ms;
            persisted.updated_at_ms = now_ms;
            let mut insert = QueryBuilder::new(
                "INSERT INTO consumer_monitor_rule (environment_id, consumer_group, min_count, max_diff_total, revision, \
                 created_at_ms, updated_at_ms) VALUES (",
            );
            let mut separated = insert.separated(", ");
            separated.push_bind(&persisted.environment_id.0);
            separated.push_bind(&persisted.consumer_group);
            separated.push_bind(persisted.min_count);
            separated.push_bind(persisted.max_diff_total);
            separated.push_bind(revision_to_database(persisted.revision)?);
            separated.push_bind(persisted.created_at_ms);
            separated.push_bind(persisted.updated_at_ms);
            separated.push_unseparated(")");
            insert.build().execute(&mut *$transaction).await.map_err(map_query_error)?;
        }
        $transaction.commit().await.map_err(map_query_error)?;
        Ok(persisted)
    }};
}

/// SQLite's deferred transactions allow two writers to observe a missing rule
/// before either insert. Beginning IMMEDIATE reserves the single writer slot
/// before that read, so the loser observes revision 1 and returns the shared
/// optimistic-concurrency conflict rather than a transient lock error.
async fn upsert_monitor_rule_in_sqlite_transaction(
    pool: &SqlitePool,
    rule: ConsumerMonitorRule,
    expected_revision: Revision,
) -> Result<ConsumerMonitorRule, PersistenceError> {
    let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await.map_err(map_query_error)?;
    crate::upsert_monitor_rule_in_transaction!(transaction, rule, expected_revision)
}

#[macro_export]
macro_rules! delete_monitor_rule_in_pool {
    ($pool:expr, $environment_id:expr, $consumer_group:expr, $expected_revision:expr) => {{
        let mut transaction = $pool.begin().await.map_err(map_query_error)?;
        let mut environment = QueryBuilder::new("SELECT 1 FROM dashboard_environment WHERE environment_id = ");
        environment.push_bind(&$environment_id.0);
        if environment
            .build()
            .fetch_optional(&mut *transaction)
            .await
            .map_err(map_query_error)?
            .is_none()
        {
            return Err(PersistenceError::NotFound);
        }
        let mut delete = QueryBuilder::new("DELETE FROM consumer_monitor_rule WHERE environment_id = ");
        delete.push_bind(&$environment_id.0);
        delete.push(" AND consumer_group = ");
        delete.push_bind($consumer_group);
        delete.push(" AND revision = ");
        delete.push_bind(revision_to_database($expected_revision)?);
        let deleted = delete
            .build()
            .execute(&mut *transaction)
            .await
            .map_err(map_query_error)?
            .rows_affected();
        if deleted == 1 {
            transaction.commit().await.map_err(map_query_error)?;
            Ok(true)
        } else {
            let mut owner = QueryBuilder::new("SELECT 1 FROM dashboard_environment WHERE environment_id = ");
            owner.push_bind(&$environment_id.0);
            if owner
                .build()
                .fetch_optional(&mut *transaction)
                .await
                .map_err(map_query_error)?
                .is_none()
            {
                return Err(PersistenceError::NotFound);
            }
            let mut exists = QueryBuilder::new("SELECT 1 FROM consumer_monitor_rule WHERE environment_id = ");
            exists.push_bind(&$environment_id.0);
            exists.push(" AND consumer_group = ");
            exists.push_bind($consumer_group);
            let found = exists
                .build()
                .fetch_optional(&mut *transaction)
                .await
                .map_err(map_query_error)?
                .is_some();
            if found {
                Err(PersistenceError::Conflict)
            } else {
                transaction.commit().await.map_err(map_query_error)?;
                Ok(false)
            }
        }
    }};
}

fn revision_to_database(revision: Revision) -> Result<i64, PersistenceError> {
    i64::try_from(revision.0).map_err(|_| PersistenceError::InvalidConfig("revision is too large".to_string()))
}

fn revision_from_database(revision: i64) -> Result<Revision, PersistenceError> {
    u64::try_from(revision)
        .map(Revision)
        .map_err(|_| PersistenceError::CorruptedData)
}

fn map_query_error(error: sqlx::Error) -> PersistenceError {
    match error {
        sqlx::Error::PoolTimedOut => PersistenceError::Timeout,
        sqlx::Error::Database(ref database_error) if database_error.is_unique_violation() => PersistenceError::Conflict,
        // File storage validates the owning environment before every monitor
        // operation.  Map SQL foreign-key checks to the same repository-level
        // result in case another writer removes an environment between that
        // check and the monitor statement.
        sqlx::Error::Database(ref database_error) if database_error.is_foreign_key_violation() => {
            PersistenceError::NotFound
        }
        error => PersistenceError::Query(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SqlPoolConfig;
    use crate::model::DashboardConfigView;
    use rocketmq_runtime::RuntimeConfig;
    use rocketmq_runtime::RuntimeOwner;

    fn sqlite_config(path: PathBuf) -> StorageConfig {
        StorageConfig {
            backend: StorageBackend::Sqlite,
            data_path: path,
            database_url: None,
            pool: SqlPoolConfig::default(),
        }
    }

    #[test]
    fn sqlite_initialization_uses_a_real_file_and_migrations_are_idempotent() {
        let directory = tempfile::tempdir().expect("temp dir");
        let database_path = directory.path().join("dashboard.db");
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        owner.block_on(async {
            let config = sqlite_config(database_path.clone());
            let first = SqlPersistence::initialize(&config, owner.root_context().component("sqlite-first"))
                .await
                .expect("first SQLite initialization");
            assert_eq!(first.schema_version(), 2);
            let health = first.storage_health().await;
            assert!(matches!(health.status, StorageStatus::Available));
            assert!(health.pool_size.is_some());
            assert!(health.idle_connections.is_some());
            assert_schema_metadata(&first).await;
            drop(first);
            let second = SqlPersistence::initialize(&config, owner.root_context().component("sqlite-second"))
                .await
                .expect("second SQLite initialization");
            assert_eq!(second.schema_version(), 2);
        });
        assert!(database_path.exists());
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn closed_sqlite_pool_reports_storage_as_unavailable() {
        let directory = tempfile::tempdir().expect("temp dir");
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        owner.block_on(async {
            let store = SqlPersistence::initialize(
                &sqlite_config(directory.path().join("dashboard.db")),
                owner.root_context().component("sqlite-health"),
            )
            .await
            .expect("SQLite initialization");
            store.sqlite_pool().expect("SQLite pool").close().await;
            assert_eq!(store.storage_health().await.status, StorageStatus::Unavailable);
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn missing_migration_table_reports_storage_as_unavailable() {
        let directory = tempfile::tempdir().expect("temp dir");
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        owner.block_on(async {
            let store = SqlPersistence::initialize(
                &sqlite_config(directory.path().join("dashboard.db")),
                owner.root_context().component("sqlite-migration-health"),
            )
            .await
            .expect("SQLite initialization");
            sqlx::query("DROP TABLE dashboard_schema_migration")
                .execute(store.sqlite_pool().expect("SQLite pool"))
                .await
                .expect("drop migration table");
            assert_eq!(store.storage_health().await.status, StorageStatus::Unavailable);
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn legacy_sqlite_config_table_is_rejected() {
        let directory = tempfile::tempdir().expect("temp dir");
        let database_path = directory.path().join("legacy.db");
        create_legacy_sqlite_table(&database_path);
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        owner.block_on(async {
            let result = SqlPersistence::initialize(
                &sqlite_config(database_path),
                owner.root_context().component("legacy-sqlite"),
            )
            .await;
            assert!(matches!(result, Err(PersistenceError::UnsupportedLayout)));
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn sqlite_read_snapshot_keeps_monitor_owner_and_rules_aligned_during_concurrent_delete() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        owner.block_on(async {
            let store = SqlPersistence::initialize(
                &sqlite_config(directory.path().join("dashboard.db")),
                owner.root_context().component("sqlite-snapshot"),
            )
            .await
            .expect("initialize SQLite persistence");
            let mut environment = DashboardEnvironment::bootstrap(&DashboardConfigView::default(), 1);
            environment.environment_id = EnvironmentId::new();
            environment.name = format!("sqlite-snapshot-{}", environment.environment_id.0);
            let environment = store.create_environment(environment).await.expect("create environment");
            store
                .upsert_monitor_rule(
                    ConsumerMonitorRule {
                        environment_id: environment.environment_id.clone(),
                        consumer_group: "snapshot-group".to_string(),
                        min_count: 1,
                        max_diff_total: 10,
                        revision: Revision(0),
                        created_at_ms: 0,
                        updated_at_ms: 0,
                    },
                    Revision(0),
                )
                .await
                .expect("create monitor rule");
            let pool = store.sqlite_pool().expect("SQLite pool");
            let mut reader = pool.acquire().await.expect("acquire snapshot reader");
            sqlx::query("BEGIN")
                .execute(&mut *reader)
                .await
                .expect("start read transaction");
            let owner_visible: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM dashboard_environment WHERE environment_id = ?")
                    .bind(&environment.environment_id.0)
                    .fetch_one(&mut *reader)
                    .await
                    .expect("read environment owner");
            assert_eq!(owner_visible, 1);

            assert!(
                store
                    .delete_environment(&environment.environment_id, environment.revision)
                    .await
                    .expect("delete environment concurrently")
            );
            let rules_visible: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM consumer_monitor_rule WHERE environment_id = ?")
                    .bind(&environment.environment_id.0)
                    .fetch_one(&mut *reader)
                    .await
                    .expect("read monitor rules from the same snapshot");
            assert_eq!(
                rules_visible, 1,
                "a snapshot cannot combine the old owner with deleted rules"
            );
            sqlx::query("COMMIT")
                .execute(&mut *reader)
                .await
                .expect("commit read transaction");
            assert!(matches!(
                store.list_monitor_rules(&environment.environment_id).await,
                Err(PersistenceError::NotFound)
            ));
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    #[ignore = "requires docker-compose.storage-test.yml"]
    fn docker_sqlite_initializes_a_mounted_database_file() {
        let path = std::env::var("ROCKETMQ_DASHBOARD_STORAGE_TEST_SQLITE_PATH")
            .expect("ROCKETMQ_DASHBOARD_STORAGE_TEST_SQLITE_PATH must be set by the storage test runner");
        initialize_docker_backend(StorageConfig {
            backend: StorageBackend::Sqlite,
            data_path: path.into(),
            database_url: None,
            pool: SqlPoolConfig::default(),
        });
    }

    #[test]
    #[ignore = "requires docker-compose.storage-test.yml"]
    fn docker_mysql_initializes_a_real_mysql_database() {
        let url = std::env::var("ROCKETMQ_DASHBOARD_STORAGE_TEST_MYSQL_URL")
            .expect("ROCKETMQ_DASHBOARD_STORAGE_TEST_MYSQL_URL must be set by the storage test runner");
        initialize_docker_backend(StorageConfig {
            backend: StorageBackend::MySql,
            data_path: "unused".into(),
            database_url: Some(url),
            pool: SqlPoolConfig::default(),
        });
    }

    #[test]
    #[ignore = "requires docker-compose.storage-test.yml"]
    fn docker_postgres_initializes_a_real_postgres_database() {
        let url = std::env::var("ROCKETMQ_DASHBOARD_STORAGE_TEST_POSTGRES_URL")
            .expect("ROCKETMQ_DASHBOARD_STORAGE_TEST_POSTGRES_URL must be set by the storage test runner");
        initialize_docker_backend(StorageConfig {
            backend: StorageBackend::Postgres,
            data_path: "unused".into(),
            database_url: Some(url),
            pool: SqlPoolConfig::default(),
        });
    }

    fn initialize_docker_backend(config: StorageConfig) {
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        owner.block_on(async {
            let first =
                SqlPersistence::initialize(&config, owner.root_context().component("docker-storage-test-first"))
                    .await
                    .expect("first storage initialization");
            assert_eq!(first.schema_version(), 2);
            assert_eq!(first.storage_health().await.status, StorageStatus::Available);
            assert_schema_metadata(&first).await;
            drop(first);
            let second =
                SqlPersistence::initialize(&config, owner.root_context().component("docker-storage-test-second"))
                    .await
                    .expect("second storage initialization");
            assert_eq!(second.schema_version(), 2);
            assert_eq!(second.storage_health().await.status, StorageStatus::Available);
        });
        if config.backend == StorageBackend::Sqlite {
            assert!(config.data_path.exists(), "SQLite must initialize a real database file");
        }
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    async fn assert_schema_metadata(persistence: &SqlPersistence) {
        match &persistence.pool {
            DatabasePool::Sqlite(pool) => {
                let environment_name: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM pragma_table_info('dashboard_environment') \
                     WHERE name = 'name' AND type = 'VARCHAR(128)' AND \"notnull\" = 1",
                )
                .fetch_one(pool)
                .await
                .expect("SQLite environment name column");
                let unique_name: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) \
                     FROM pragma_index_list('dashboard_environment') AS indexes \
                     JOIN pragma_index_info(indexes.name) AS columns \
                       ON 1 = 1 \
                     WHERE indexes.[unique] = 1 AND columns.name = 'name'",
                )
                .fetch_one(pool)
                .await
                .expect("SQLite environment name unique constraint");
                let retention_index: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM pragma_index_list('dashboard_metric_sample') \
                     WHERE name = 'dashboard_metric_sample_retention_idx'",
                )
                .fetch_one(pool)
                .await
                .expect("SQLite metric retention index");
                let environment_fk: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM pragma_foreign_key_list('dashboard_metric_sample') \
                     WHERE \"table\" = 'dashboard_environment'",
                )
                .fetch_one(pool)
                .await
                .expect("SQLite metric environment foreign key");
                assert_eq!(environment_name, 1);
                assert_eq!(unique_name, 1);
                assert_eq!(retention_index, 1);
                assert_eq!(environment_fk, 1);
            }
            DatabasePool::MySql(pool) => {
                let environment_name: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM information_schema.columns \
                     WHERE table_schema = DATABASE() AND table_name = 'dashboard_environment' \
                       AND column_name = 'name' AND data_type = 'varchar' \
                       AND character_maximum_length = 128 AND is_nullable = 'NO'",
                )
                .fetch_one(pool)
                .await
                .expect("MySQL environment name column");
                let unique_name: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM information_schema.statistics \
                     WHERE table_schema = DATABASE() AND table_name = 'dashboard_environment' \
                       AND column_name = 'name' AND non_unique = 0",
                )
                .fetch_one(pool)
                .await
                .expect("MySQL environment name unique constraint");
                let retention_index: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM information_schema.statistics \
                     WHERE table_schema = DATABASE() AND table_name = 'dashboard_metric_sample' \
                       AND index_name = 'dashboard_metric_sample_retention_idx' \
                       AND column_name = 'bucket_ms'",
                )
                .fetch_one(pool)
                .await
                .expect("MySQL metric retention index");
                let environment_fk: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM information_schema.key_column_usage \
                     WHERE table_schema = DATABASE() AND table_name = 'dashboard_metric_sample' \
                       AND column_name = 'environment_id' \
                       AND referenced_table_name = 'dashboard_environment'",
                )
                .fetch_one(pool)
                .await
                .expect("MySQL metric environment foreign key");
                assert_eq!(environment_name, 1);
                assert_eq!(unique_name, 1);
                assert_eq!(retention_index, 1);
                assert_eq!(environment_fk, 1);
            }
            DatabasePool::Postgres(pool) => {
                let environment_name: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM information_schema.columns \
                     WHERE table_schema = current_schema() AND table_name = 'dashboard_environment' \
                       AND column_name = 'name' AND data_type = 'character varying' \
                       AND character_maximum_length = 128 AND is_nullable = 'NO'",
                )
                .fetch_one(pool)
                .await
                .expect("PostgreSQL environment name column");
                let unique_name: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) \
                     FROM information_schema.table_constraints constraints \
                     JOIN information_schema.key_column_usage columns \
                       ON constraints.constraint_name = columns.constraint_name \
                      AND constraints.table_schema = columns.table_schema \
                     WHERE constraints.table_schema = current_schema() \
                       AND constraints.table_name = 'dashboard_environment' \
                       AND constraints.constraint_type = 'UNIQUE' AND columns.column_name = 'name'",
                )
                .fetch_one(pool)
                .await
                .expect("PostgreSQL environment name unique constraint");
                let retention_index: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM pg_indexes \
                     WHERE schemaname = current_schema() \
                       AND tablename = 'dashboard_metric_sample' \
                       AND indexname = 'dashboard_metric_sample_retention_idx' \
                       AND indexdef LIKE '%(bucket_ms)%'",
                )
                .fetch_one(pool)
                .await
                .expect("PostgreSQL metric retention index");
                let environment_fk: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM information_schema.key_column_usage \
                     WHERE table_schema = current_schema() AND table_name = 'dashboard_metric_sample' \
                       AND column_name = 'environment_id' \
                       AND position_in_unique_constraint IS NOT NULL",
                )
                .fetch_one(pool)
                .await
                .expect("PostgreSQL metric environment foreign key");
                assert_eq!(environment_name, 1);
                assert_eq!(unique_name, 1);
                assert_eq!(retention_index, 1);
                assert_eq!(environment_fk, 1);
            }
        }
    }

    fn create_legacy_sqlite_table(path: &std::path::Path) {
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        owner.block_on(async {
            let options = SqliteConnectOptions::new().filename(path).create_if_missing(true);
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(options)
                .await
                .expect("open legacy SQLite database");
            sqlx::query("CREATE TABLE dashboard_config (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
                .execute(&pool)
                .await
                .expect("create legacy table");
            pool.close().await;
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }
}
