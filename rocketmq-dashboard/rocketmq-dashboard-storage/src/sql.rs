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

//! Native-only schema. Concrete driver matches keep every driver independently optional.

use std::{str::FromStr, time::Duration};

use rocketmq_dashboard_common::{EnvironmentHistoryPoint, EnvironmentId, HistoryPoint, HistoryRetention};
use serde::de::DeserializeOwned;
use sqlx::{ConnectOptions, QueryBuilder, Row};

use crate::{
    session::SessionDocument, DatabaseConnection, DesktopDocument, DesktopSnapshot, HistoryAppendOutcome,
    HistoryDocument, HistoryLease, ImportSnapshot, SessionId, SessionPage, SessionQuery, SessionRecord, StorageEngine,
    StorageError, StorageErrorKind, StorageRevision, StorageStatus,
};

#[path = "sql_history.rs"]
mod sql_history;

enum Pool {
    #[cfg(feature = "sqlite")]
    Sqlite(sqlx::SqlitePool),
    #[cfg(feature = "mysql")]
    MySql(sqlx::MySqlPool),
    #[cfg(feature = "postgres")]
    Postgres(sqlx::PgPool),
}

// Each expansion is inferred against its concrete pool. No driver is pulled into another
// feature's build, and QueryBuilder supplies the driver's real parameter placeholders.
macro_rules! with_pool {
    ($self:expr, $pool:ident, $body:block) => {
        match &$self.pool {
            #[cfg(feature = "sqlite")]
            Pool::Sqlite($pool) => $body,
            #[cfg(feature = "mysql")]
            Pool::MySql($pool) => $body,
            #[cfg(feature = "postgres")]
            Pool::Postgres($pool) => $body,
        }
    };
}

pub(crate) use with_pool;

pub(crate) struct SqlStore {
    pool: Pool,
    engine: StorageEngine,
    context: rocketmq_runtime::ChildServiceContext,
}

impl SqlStore {
    pub(crate) async fn connect(
        engine: StorageEngine,
        connection: DatabaseConnection,
        context: rocketmq_runtime::ChildServiceContext,
    ) -> Result<Self, StorageError> {
        let pool = match engine {
            #[cfg(feature = "sqlite")]
            StorageEngine::Sqlite => {
                let options = sqlx::sqlite::SqliteConnectOptions::from_str(connection.as_str())
                    .map_err(|_| connection_error())?
                    .create_if_missing(true)
                    .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
                    .busy_timeout(Duration::from_secs(5))
                    .disable_statement_logging();
                Pool::Sqlite(
                    sqlx::sqlite::SqlitePoolOptions::new()
                        .max_connections(4)
                        .acquire_timeout(Duration::from_secs(5))
                        .connect_with(options)
                        .await
                        .map_err(|_| connection_error())?,
                )
            }
            #[cfg(feature = "mysql")]
            StorageEngine::MySql => {
                let options = sqlx::mysql::MySqlConnectOptions::from_str(connection.as_str())
                    .map_err(|_| connection_error())?
                    .disable_statement_logging();
                Pool::MySql(
                    sqlx::mysql::MySqlPoolOptions::new()
                        .max_connections(4)
                        .acquire_timeout(Duration::from_secs(5))
                        .connect_with(options)
                        .await
                        .map_err(|_| connection_error())?,
                )
            }
            #[cfg(feature = "postgres")]
            StorageEngine::Postgres => {
                let options = sqlx::postgres::PgConnectOptions::from_str(connection.as_str())
                    .map_err(|_| connection_error())?
                    .disable_statement_logging();
                Pool::Postgres(
                    sqlx::postgres::PgPoolOptions::new()
                        .max_connections(4)
                        .acquire_timeout(Duration::from_secs(5))
                        .connect_with(options)
                        .await
                        .map_err(|_| connection_error())?,
                )
            }
            _ => return Err(StorageError::new(StorageErrorKind::DriverUnavailable { engine })),
        };
        let store = Self { pool, engine, context };
        if let Err(error) = store.migrate().await {
            store.close().await?;
            return Err(error);
        }
        Ok(store)
    }

    async fn migrate(&self) -> Result<(), StorageError> {
        with_pool!(self, pool, {
            sqlx::query("CREATE TABLE IF NOT EXISTS native_dashboard_schema (singleton INTEGER PRIMARY KEY, schema_version INTEGER NOT NULL)")
                .execute(pool).await.map_err(query_error)?;
            let insert = match self.engine {
                StorageEngine::MySql => "INSERT IGNORE INTO native_dashboard_schema (singleton, schema_version) VALUES (1, 1)",
                _ => "INSERT INTO native_dashboard_schema (singleton, schema_version) VALUES (1, 1) ON CONFLICT (singleton) DO NOTHING",
            };
            sqlx::query(insert).execute(pool).await.map_err(query_error)?;
            let row = sqlx::query("SELECT schema_version FROM native_dashboard_schema WHERE singleton = 1")
                .fetch_one(pool)
                .await
                .map_err(query_error)?;
            let version: i32 = row.try_get(0).map_err(query_error)?;
            if version != 1 {
                return Err(StorageError::unsupported_schema(
                    version.try_into().unwrap_or(u32::MAX),
                    1,
                ));
            }
            let payload = if self.engine == StorageEngine::MySql {
                "LONGTEXT"
            } else {
                "TEXT"
            };
            let table_options = if self.engine == StorageEngine::MySql {
                " ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4"
            } else {
                ""
            };
            let documents = format!("CREATE TABLE IF NOT EXISTS native_dashboard_document (document_name VARCHAR(32) PRIMARY KEY, storage_revision BIGINT NOT NULL, payload {payload} NOT NULL, written_at_ms BIGINT NOT NULL){table_options}");
            sqlx::query(&documents).execute(pool).await.map_err(query_error)?;
            let leases = if self.engine == StorageEngine::MySql {
                "CREATE TABLE IF NOT EXISTS native_dashboard_history_lease (environment_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY, holder_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL, fencing_token BIGINT NOT NULL, expires_at_ms BIGINT NOT NULL) ENGINE=InnoDB"
            } else {
                "CREATE TABLE IF NOT EXISTS native_dashboard_history_lease (environment_id VARCHAR(64) PRIMARY KEY, holder_id VARCHAR(128) NOT NULL, fencing_token BIGINT NOT NULL, expires_at_ms BIGINT NOT NULL)"
            };
            sqlx::query(leases).execute(pool).await.map_err(query_error)?;
            Ok(())
        })
    }

    fn clock(&self) -> &'static str {
        match self.engine {
            StorageEngine::Sqlite => "CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)",
            StorageEngine::MySql => {
                "CAST(TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP(3)) DIV 1000 AS SIGNED)"
            }
            StorageEngine::Postgres => "CAST(FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000) AS BIGINT)",
            StorageEngine::File => "0",
        }
    }

    fn begin_statement(&self) -> &'static str {
        if self.engine == StorageEngine::Sqlite {
            "BEGIN IMMEDIATE"
        } else {
            "BEGIN"
        }
    }

    fn row_lock(&self) -> &'static str {
        if self.engine == StorageEngine::Sqlite {
            ""
        } else {
            " FOR UPDATE"
        }
    }

    async fn read_payload(&self, name: &'static str) -> Result<Option<(StorageRevision, String)>, StorageError> {
        with_pool!(self, pool, {
            let mut query = QueryBuilder::new(
                "SELECT storage_revision, payload FROM native_dashboard_document WHERE document_name = ",
            );
            query.push_bind(name);
            let row = query.build().fetch_optional(pool).await.map_err(query_error)?;
            row.map(|row| {
                Ok((
                    StorageRevision(row.try_get(0).map_err(query_error)?),
                    row.try_get(1).map_err(query_error)?,
                ))
            })
            .transpose()
        })
    }

    pub(crate) async fn load_desktop(&self) -> Result<Option<DesktopSnapshot>, StorageError> {
        let Some((storage_revision, payload)) = self.read_payload("desktop").await? else {
            return Ok(None);
        };
        let document: DesktopDocument = decode(&payload)?;
        document.validate()?;
        Ok(Some(DesktopSnapshot {
            storage_revision,
            document,
        }))
    }

    pub(crate) async fn initialize(&self, document: DesktopDocument) -> Result<DesktopSnapshot, StorageError> {
        self.import(ImportSnapshot {
            document,
            history: HistoryDocument::default(),
            sessions: SessionDocument::empty(),
        })
        .await
    }

    pub(crate) async fn compare_exchange_desktop(
        &self,
        expected: StorageRevision,
        document: DesktopDocument,
    ) -> Result<DesktopSnapshot, StorageError> {
        let next = expected.next()?;
        let payload = encode(&document)?;
        with_pool!(self, pool, {
            let mut query = QueryBuilder::new("UPDATE native_dashboard_document SET payload = ");
            query
                .push_bind(&payload)
                .push(", storage_revision = ")
                .push_bind(next.0)
                .push(", written_at_ms = ")
                .push(self.clock())
                .push(" WHERE document_name = 'desktop' AND storage_revision = ")
                .push_bind(expected.0);
            if query.build().execute(pool).await.map_err(query_error)?.rows_affected() != 1 {
                return Err(StorageError::conflict());
            }
            Ok(DesktopSnapshot {
                storage_revision: next,
                document,
            })
        })
    }

    pub(crate) async fn import(&self, snapshot: ImportSnapshot) -> Result<DesktopSnapshot, StorageError> {
        let documents = [
            ("desktop", encode(&snapshot.document)?),
            ("history", encode(&snapshot.history)?),
            ("sessions", encode(&snapshot.sessions)?),
        ];
        with_pool!(self, pool, {
            let mut transaction = pool.begin_with(self.begin_statement()).await.map_err(query_error)?;
            for (name, payload) in &documents {
                let mut query = QueryBuilder::new("INSERT INTO native_dashboard_document (document_name, storage_revision, payload, written_at_ms) VALUES (");
                query
                    .push_bind(*name)
                    .push(", 0, ")
                    .push_bind(payload)
                    .push(", ")
                    .push(self.clock())
                    .push(")");
                if let Err(error) = query.build().execute(&mut *transaction).await {
                    let error = if is_unique(&error) {
                        StorageError::new(StorageErrorKind::AlreadyInitialized)
                    } else {
                        query_error(error)
                    };
                    transaction.rollback().await.map_err(query_error)?;
                    return Err(error);
                }
            }
            transaction.commit().await.map_err(query_error)?;
            Ok(DesktopSnapshot {
                storage_revision: StorageRevision(0),
                document: snapshot.document,
            })
        })
    }

    pub(crate) async fn history(&self) -> Result<HistoryDocument, StorageError> {
        let Some((_, payload)) = self.read_payload("history").await? else {
            return Err(StorageError::new(StorageErrorKind::Uninitialized));
        };
        let history: HistoryDocument = decode(&payload)?;
        history.validate()?;
        Ok(history)
    }

    async fn update_sessions<R>(
        &self,
        update: impl FnOnce(&mut SessionDocument) -> Result<R, StorageError>,
    ) -> Result<R, StorageError> {
        with_pool!(self, pool, {
            let mut transaction = pool.begin_with(self.begin_statement()).await.map_err(query_error)?;
            let mut select = QueryBuilder::new(
                "SELECT storage_revision, payload FROM native_dashboard_document WHERE document_name = 'sessions'",
            );
            select.push(self.row_lock());
            let row = select
                .build()
                .fetch_optional(&mut *transaction)
                .await
                .map_err(query_error)?
                .ok_or_else(|| StorageError::new(StorageErrorKind::Uninitialized))?;
            let revision = StorageRevision(row.try_get(0).map_err(query_error)?);
            let payload: String = row.try_get(1).map_err(query_error)?;
            let mut document: SessionDocument = decode(&payload)?;
            document.validate()?;
            let before = document.clone();
            let result = update(&mut document)?;
            document.validate()?;
            if document != before {
                let mut query = QueryBuilder::new("UPDATE native_dashboard_document SET payload = ");
                query
                    .push_bind(encode(&document)?)
                    .push(", storage_revision = ")
                    .push_bind(revision.next()?.0)
                    .push(", written_at_ms = ")
                    .push(self.clock())
                    .push(" WHERE document_name = 'sessions' AND storage_revision = ")
                    .push_bind(revision.0);
                if query
                    .build()
                    .execute(&mut *transaction)
                    .await
                    .map_err(query_error)?
                    .rows_affected()
                    != 1
                {
                    return Err(StorageError::conflict());
                }
            }
            transaction.commit().await.map_err(query_error)?;
            Ok(result)
        })
    }

    pub(crate) async fn create_session(&self, session: SessionRecord, now: u64) -> Result<(), StorageError> {
        self.update_sessions(|document| document.create(session, now)).await
    }

    pub(crate) async fn access_session(&self, id: SessionId, now: u64) -> Result<Option<SessionRecord>, StorageError> {
        self.update_sessions(|document| Ok(document.access(&id, now))).await
    }

    pub(crate) async fn sessions(&self, query: SessionQuery) -> Result<SessionPage, StorageError> {
        let Some((_, payload)) = self.read_payload("sessions").await? else {
            return Err(StorageError::new(StorageErrorKind::Uninitialized));
        };
        let document: SessionDocument = decode(&payload)?;
        document.validate()?;
        document.query(query)
    }

    pub(crate) async fn revoke_session(&self, id: SessionId, now: u64) -> Result<bool, StorageError> {
        self.update_sessions(|document| Ok(document.revoke(&id, now))).await
    }

    pub(crate) async fn revoke_all_sessions(&self, username: String, now: u64) -> Result<usize, StorageError> {
        self.update_sessions(|document| document.revoke_all(&username, now))
            .await
    }

    pub(crate) async fn status(&self) -> Result<StorageStatus, StorageError> {
        let desktop = self.load_desktop().await?;
        with_pool!(self, pool, {
            let row = sqlx::query("SELECT schema_version FROM native_dashboard_schema WHERE singleton = 1")
                .fetch_one(pool)
                .await
                .map_err(query_error)?;
            let version: i32 = row.try_get(0).map_err(query_error)?;
            if version != 1 {
                return Err(StorageError::unsupported_schema(
                    version.try_into().unwrap_or(u32::MAX),
                    1,
                ));
            }
            let row = sqlx::query("SELECT MAX(written_at_ms) FROM native_dashboard_document")
                .fetch_one(pool)
                .await
                .map_err(query_error)?;
            let last_write: Option<i64> = row.try_get(0).map_err(query_error)?;
            Ok(StorageStatus {
                engine: self.engine,
                schema_version: 1,
                document_schema_version: desktop.as_ref().map(|desktop| desktop.document.schema_version),
                initialized: desktop.is_some(),
                last_successful_write_epoch_ms: last_write.and_then(|value| value.try_into().ok()),
                pool_connections: Some(pool.size()),
                pool_idle_connections: Some(pool.num_idle()),
            })
        })
    }

    pub(crate) async fn close(&self) -> Result<(), StorageError> {
        with_pool!(self, pool, {
            pool.close().await;
            Ok(())
        })
    }
}

fn connection_error() -> StorageError {
    StorageError::new(StorageErrorKind::Connection)
}

#[cfg(test)]
#[path = "sql_tests.rs"]
mod tests;

fn is_unique(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(error) if error.is_unique_violation())
}

pub(super) fn query_error(error: sqlx::Error) -> StorageError {
    match error {
        sqlx::Error::PoolClosed => StorageError::new(StorageErrorKind::Closed),
        sqlx::Error::Database(error)
            if error.is_unique_violation()
                || error
                    .code()
                    .is_some_and(|code| matches!(code.as_ref(), "40001" | "40P01" | "1213" | "1205" | "5" | "6")) =>
        {
            StorageError::conflict()
        }
        _ => StorageError::new(StorageErrorKind::Query),
    }
}

pub(super) fn decode<T: DeserializeOwned>(payload: &str) -> Result<T, StorageError> {
    serde_json::from_str(payload).map_err(|_| StorageError::new(StorageErrorKind::InvalidDocument))
}

pub(super) fn encode(value: &impl serde::Serialize) -> Result<String, StorageError> {
    serde_json::to_string(value).map_err(StorageError::validation)
}
