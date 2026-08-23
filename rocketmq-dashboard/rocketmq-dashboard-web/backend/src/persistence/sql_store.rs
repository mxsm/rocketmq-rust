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
use crate::model::StorageBackend;
use crate::persistence::StorageHealth;
use crate::persistence::StorageMode;
use crate::persistence::StorageStatus;
use crate::persistence::error::PersistenceError;
use crate::persistence::migration;
use rocketmq_runtime::ChildServiceContext;
use sqlx::MySqlPool;
use sqlx::PgPool;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SqlPoolConfig;
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
            assert_eq!(first.schema_version(), 1);
            let health = first.storage_health().await;
            assert!(matches!(health.status, StorageStatus::Available));
            assert!(health.pool_size.is_some());
            assert!(health.idle_connections.is_some());
            assert_schema_metadata(&first).await;
            drop(first);
            let second = SqlPersistence::initialize(&config, owner.root_context().component("sqlite-second"))
                .await
                .expect("second SQLite initialization");
            assert_eq!(second.schema_version(), 1);
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
            assert_eq!(first.schema_version(), 1);
            assert_eq!(first.storage_health().await.status, StorageStatus::Available);
            assert_schema_metadata(&first).await;
            drop(first);
            let second =
                SqlPersistence::initialize(&config, owner.root_context().component("docker-storage-test-second"))
                    .await
                    .expect("second storage initialization");
            assert_eq!(second.schema_version(), 1);
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
