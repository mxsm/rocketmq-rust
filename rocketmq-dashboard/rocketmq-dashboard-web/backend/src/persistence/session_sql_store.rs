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

use super::DatabasePool;
use super::MySqlPool;
use super::PersistenceError;
use super::PgPool;
use super::Row;
use super::SqlPersistence;
use super::SqlitePool;
use super::map_query_error;
use crate::model::AuditEvent;
use crate::model::NewSession;
use crate::model::SessionRecord;
use crate::model::SessionTokenHash;
use crate::persistence::session_repository::SessionCursor;
use crate::persistence::session_repository::SessionPage;
use crate::persistence::session_repository::SessionQuery;
use sqlx::mysql::MySqlRow;

impl SqlPersistence {
    pub(crate) async fn create_session_with_audit(
        &self,
        session: NewSession,
        audit: AuditEvent,
    ) -> Result<(), PersistenceError> {
        self.create_session_with_audit_capped(session, audit, usize::MAX, 0)
            .await
    }

    pub(crate) async fn create_session_with_audit_capped(
        &self,
        session: NewSession,
        audit: AuditEvent,
        max_active_sessions: usize,
        now_ms: i64,
    ) -> Result<(), PersistenceError> {
        match &self.pool {
            DatabasePool::Sqlite(pool) => {
                let mut transaction = pool.begin().await.map_err(map_query_error)?;
                enforce_session_cap_sqlite(&mut transaction, &session.username, now_ms, max_active_sessions).await?;
                sqlx::query("INSERT INTO dashboard_session (session_id, token_hash, username, created_at_ms, expires_at_ms, last_seen_at_ms, revoked_at_ms) VALUES (?, ?, ?, ?, ?, ?, NULL)")
                    .bind(&session.session_id).bind(session.token_hash.bytes().to_vec()).bind(&session.username).bind(session.created_at_ms).bind(session.expires_at_ms).bind(session.created_at_ms)
                    .execute(&mut *transaction).await.map_err(map_query_error)?;
                insert_audit_sqlite(&mut transaction, audit).await?;
                transaction.commit().await.map_err(map_query_error)?;
            }
            DatabasePool::MySql(pool) => {
                let mut transaction = pool.begin().await.map_err(map_query_error)?;
                enforce_session_cap_mysql(&mut transaction, &session.username, now_ms, max_active_sessions).await?;
                sqlx::query("INSERT INTO dashboard_session (session_id, token_hash, username, created_at_ms, expires_at_ms, last_seen_at_ms, revoked_at_ms) VALUES (?, ?, ?, ?, ?, ?, NULL)")
                    .bind(&session.session_id).bind(session.token_hash.bytes().to_vec()).bind(&session.username).bind(session.created_at_ms).bind(session.expires_at_ms).bind(session.created_at_ms)
                    .execute(&mut *transaction).await.map_err(map_query_error)?;
                insert_audit_mysql(&mut transaction, audit).await?;
                transaction.commit().await.map_err(map_query_error)?;
            }
            DatabasePool::Postgres(pool) => {
                let mut transaction = pool.begin().await.map_err(map_query_error)?;
                enforce_session_cap_postgres(&mut transaction, &session.username, now_ms, max_active_sessions).await?;
                sqlx::query("INSERT INTO dashboard_session (session_id, token_hash, username, created_at_ms, expires_at_ms, last_seen_at_ms, revoked_at_ms) VALUES ($1, $2, $3, $4, $5, $6, NULL)")
                    .bind(&session.session_id).bind(session.token_hash.bytes().to_vec()).bind(&session.username).bind(session.created_at_ms).bind(session.expires_at_ms).bind(session.created_at_ms)
                    .execute(&mut *transaction).await.map_err(map_query_error)?;
                insert_audit_postgres(&mut transaction, audit).await?;
                transaction.commit().await.map_err(map_query_error)?;
            }
        }
        Ok(())
    }

    pub(crate) async fn create_session(&self, session: NewSession) -> Result<(), PersistenceError> {
        match &self.pool {
            DatabasePool::Sqlite(pool) => {
                sqlx::query(
                    "INSERT INTO dashboard_session (session_id, token_hash, username, created_at_ms, expires_at_ms, last_seen_at_ms, revoked_at_ms) VALUES (?, ?, ?, ?, ?, ?, NULL)",
                )
                .bind(session.session_id)
                .bind(session.token_hash.bytes().to_vec())
                .bind(session.username)
                .bind(session.created_at_ms)
                .bind(session.expires_at_ms)
                .bind(session.created_at_ms)
                .execute(pool)
                .await
                .map_err(map_query_error)?;
            }
            DatabasePool::MySql(pool) => {
                sqlx::query(
                    "INSERT INTO dashboard_session (session_id, token_hash, username, created_at_ms, expires_at_ms, last_seen_at_ms, revoked_at_ms) VALUES (?, ?, ?, ?, ?, ?, NULL)",
                )
                .bind(session.session_id)
                .bind(session.token_hash.bytes().to_vec())
                .bind(session.username)
                .bind(session.created_at_ms)
                .bind(session.expires_at_ms)
                .bind(session.created_at_ms)
                .execute(pool)
                .await
                .map_err(map_query_error)?;
            }
            DatabasePool::Postgres(pool) => {
                sqlx::query(
                    "INSERT INTO dashboard_session (session_id, token_hash, username, created_at_ms, expires_at_ms, last_seen_at_ms, revoked_at_ms) VALUES ($1, $2, $3, $4, $5, $6, NULL)",
                )
                .bind(session.session_id)
                .bind(session.token_hash.bytes().to_vec())
                .bind(session.username)
                .bind(session.created_at_ms)
                .bind(session.expires_at_ms)
                .bind(session.created_at_ms)
                .execute(pool)
                .await
                .map_err(map_query_error)?;
            }
        }
        Ok(())
    }

    pub(crate) async fn find_session(
        &self,
        token_hash: &SessionTokenHash,
    ) -> Result<Option<SessionRecord>, PersistenceError> {
        match &self.pool {
            DatabasePool::Sqlite(pool) => find_sqlite(pool, token_hash).await,
            DatabasePool::MySql(pool) => find_mysql(pool, token_hash).await,
            DatabasePool::Postgres(pool) => find_postgres(pool, token_hash).await,
        }
    }

    pub(crate) async fn touch_session(
        &self,
        token_hash: &SessionTokenHash,
        observed_at_ms: i64,
    ) -> Result<bool, PersistenceError> {
        let affected = match &self.pool {
            DatabasePool::Sqlite(pool) => sqlx::query(
                "UPDATE dashboard_session SET last_seen_at_ms = CASE WHEN last_seen_at_ms < ? THEN ? ELSE last_seen_at_ms END WHERE token_hash = ? AND revoked_at_ms IS NULL",
            )
            .bind(observed_at_ms).bind(observed_at_ms).bind(token_hash.bytes().to_vec()).execute(pool).await.map_err(map_query_error)?.rows_affected(),
            DatabasePool::MySql(pool) => sqlx::query(
                "UPDATE dashboard_session SET last_seen_at_ms = CASE WHEN last_seen_at_ms < ? THEN ? ELSE last_seen_at_ms END WHERE token_hash = ? AND revoked_at_ms IS NULL",
            )
            .bind(observed_at_ms).bind(observed_at_ms).bind(token_hash.bytes().to_vec()).execute(pool).await.map_err(map_query_error)?.rows_affected(),
            DatabasePool::Postgres(pool) => sqlx::query(
                "UPDATE dashboard_session SET last_seen_at_ms = GREATEST(last_seen_at_ms, $1) WHERE token_hash = $2 AND revoked_at_ms IS NULL",
            )
            .bind(observed_at_ms).bind(token_hash.bytes().to_vec()).execute(pool).await.map_err(map_query_error)?.rows_affected(),
        };
        Ok(affected > 0)
    }

    pub(crate) async fn revoke_session(
        &self,
        token_hash: &SessionTokenHash,
        revoked_at_ms: i64,
    ) -> Result<bool, PersistenceError> {
        let affected = match &self.pool {
            DatabasePool::Sqlite(pool) => sqlx::query(
                "UPDATE dashboard_session SET revoked_at_ms = ? WHERE token_hash = ? AND revoked_at_ms IS NULL",
            )
            .bind(revoked_at_ms)
            .bind(token_hash.bytes().to_vec())
            .execute(pool)
            .await
            .map_err(map_query_error)?
            .rows_affected(),
            DatabasePool::MySql(pool) => sqlx::query(
                "UPDATE dashboard_session SET revoked_at_ms = ? WHERE token_hash = ? AND revoked_at_ms IS NULL",
            )
            .bind(revoked_at_ms)
            .bind(token_hash.bytes().to_vec())
            .execute(pool)
            .await
            .map_err(map_query_error)?
            .rows_affected(),
            DatabasePool::Postgres(pool) => sqlx::query(
                "UPDATE dashboard_session SET revoked_at_ms = $1 WHERE token_hash = $2 AND revoked_at_ms IS NULL",
            )
            .bind(revoked_at_ms)
            .bind(token_hash.bytes().to_vec())
            .execute(pool)
            .await
            .map_err(map_query_error)?
            .rows_affected(),
        };
        Ok(affected > 0)
    }

    pub(crate) async fn revoke_session_with_audit(
        &self,
        token_hash: &SessionTokenHash,
        revoked_at_ms: i64,
        audit: AuditEvent,
    ) -> Result<bool, PersistenceError> {
        let changed = match &self.pool {
            DatabasePool::Sqlite(pool) => {
                let mut transaction = pool.begin().await.map_err(map_query_error)?;
                let changed = sqlx::query(
                    "UPDATE dashboard_session SET revoked_at_ms = ? WHERE token_hash = ? AND revoked_at_ms IS NULL",
                )
                .bind(revoked_at_ms)
                .bind(token_hash.bytes().to_vec())
                .execute(&mut *transaction)
                .await
                .map_err(map_query_error)?
                .rows_affected()
                    > 0;
                if changed {
                    insert_audit_sqlite(&mut transaction, audit).await?;
                }
                transaction.commit().await.map_err(map_query_error)?;
                changed
            }
            DatabasePool::MySql(pool) => {
                let mut transaction = pool.begin().await.map_err(map_query_error)?;
                let changed = sqlx::query(
                    "UPDATE dashboard_session SET revoked_at_ms = ? WHERE token_hash = ? AND revoked_at_ms IS NULL",
                )
                .bind(revoked_at_ms)
                .bind(token_hash.bytes().to_vec())
                .execute(&mut *transaction)
                .await
                .map_err(map_query_error)?
                .rows_affected()
                    > 0;
                if changed {
                    insert_audit_mysql(&mut transaction, audit).await?;
                }
                transaction.commit().await.map_err(map_query_error)?;
                changed
            }
            DatabasePool::Postgres(pool) => {
                let mut transaction = pool.begin().await.map_err(map_query_error)?;
                let changed = sqlx::query(
                    "UPDATE dashboard_session SET revoked_at_ms = $1 WHERE token_hash = $2 AND revoked_at_ms IS NULL",
                )
                .bind(revoked_at_ms)
                .bind(token_hash.bytes().to_vec())
                .execute(&mut *transaction)
                .await
                .map_err(map_query_error)?
                .rows_affected()
                    > 0;
                if changed {
                    insert_audit_postgres(&mut transaction, audit).await?;
                }
                transaction.commit().await.map_err(map_query_error)?;
                changed
            }
        };
        Ok(changed)
    }

    pub(crate) async fn revoke_all_sessions(
        &self,
        username: &str,
        revoked_at_ms: i64,
    ) -> Result<u64, PersistenceError> {
        let affected = match &self.pool {
            DatabasePool::Sqlite(pool) => sqlx::query(
                "UPDATE dashboard_session SET revoked_at_ms = ? WHERE username = ? AND revoked_at_ms IS NULL AND expires_at_ms > ?",
            )
            .bind(revoked_at_ms)
            .bind(username)
            .bind(revoked_at_ms)
            .execute(pool)
            .await
            .map_err(map_query_error)?
            .rows_affected(),
            DatabasePool::MySql(pool) => sqlx::query(
                "UPDATE dashboard_session SET revoked_at_ms = ? WHERE username = ? AND revoked_at_ms IS NULL AND expires_at_ms > ?",
            )
            .bind(revoked_at_ms)
            .bind(username)
            .bind(revoked_at_ms)
            .execute(pool)
            .await
            .map_err(map_query_error)?
            .rows_affected(),
            DatabasePool::Postgres(pool) => sqlx::query(
                "UPDATE dashboard_session SET revoked_at_ms = $1 WHERE username = $2 AND revoked_at_ms IS NULL AND expires_at_ms > $3",
            )
            .bind(revoked_at_ms)
            .bind(username)
            .bind(revoked_at_ms)
            .execute(pool)
            .await
            .map_err(map_query_error)?
            .rows_affected(),
        };
        Ok(affected)
    }

    pub(crate) async fn revoke_all_sessions_with_audit(
        &self,
        username: &str,
        revoked_at_ms: i64,
        audit: AuditEvent,
    ) -> Result<u64, PersistenceError> {
        let affected = match &self.pool {
            DatabasePool::Sqlite(pool) => {
                let mut transaction = pool.begin().await.map_err(map_query_error)?;
                let affected = sqlx::query(
                    "UPDATE dashboard_session SET revoked_at_ms = ? WHERE username = ? AND revoked_at_ms IS NULL AND expires_at_ms > ?",
                )
                .bind(revoked_at_ms)
                .bind(username)
                .bind(revoked_at_ms)
                .execute(&mut *transaction)
                .await
                .map_err(map_query_error)?
                .rows_affected();
                insert_audit_sqlite(&mut transaction, audit).await?;
                transaction.commit().await.map_err(map_query_error)?;
                affected
            }
            DatabasePool::MySql(pool) => {
                let mut transaction = pool.begin().await.map_err(map_query_error)?;
                let affected = sqlx::query(
                    "UPDATE dashboard_session SET revoked_at_ms = ? WHERE username = ? AND revoked_at_ms IS NULL AND expires_at_ms > ?",
                )
                .bind(revoked_at_ms)
                .bind(username)
                .bind(revoked_at_ms)
                .execute(&mut *transaction)
                .await
                .map_err(map_query_error)?
                .rows_affected();
                insert_audit_mysql(&mut transaction, audit).await?;
                transaction.commit().await.map_err(map_query_error)?;
                affected
            }
            DatabasePool::Postgres(pool) => {
                let mut transaction = pool.begin().await.map_err(map_query_error)?;
                let affected = sqlx::query(
                    "UPDATE dashboard_session SET revoked_at_ms = $1 WHERE username = $2 AND revoked_at_ms IS NULL AND expires_at_ms > $3",
                )
                .bind(revoked_at_ms)
                .bind(username)
                .bind(revoked_at_ms)
                .execute(&mut *transaction)
                .await
                .map_err(map_query_error)?
                .rows_affected();
                insert_audit_postgres(&mut transaction, audit).await?;
                transaction.commit().await.map_err(map_query_error)?;
                affected
            }
        };
        Ok(affected)
    }

    pub(crate) async fn list_sessions(&self, query: SessionQuery) -> Result<SessionPage, PersistenceError> {
        match &self.pool {
            DatabasePool::Sqlite(pool) => list_sqlite(pool, query).await,
            DatabasePool::MySql(pool) => list_mysql(pool, query).await,
            DatabasePool::Postgres(pool) => list_postgres(pool, query).await,
        }
    }

    pub(crate) async fn delete_sessions_before(&self, cutoff_ms: i64, limit: usize) -> Result<u64, PersistenceError> {
        // All engines use a bounded key selection before deletion. This keeps
        // cleanup from turning a normal maintenance tick into an unbounded
        // transaction.
        match &self.pool {
            DatabasePool::Sqlite(pool) => delete_before_sqlite(pool, cutoff_ms, limit).await,
            DatabasePool::MySql(pool) => delete_before_mysql(pool, cutoff_ms, limit).await,
            DatabasePool::Postgres(pool) => delete_before_postgres(pool, cutoff_ms, limit).await,
        }
    }
}

async fn enforce_session_cap_sqlite(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    username: &str,
    now_ms: i64,
    max_active_sessions: usize,
) -> Result<(), PersistenceError> {
    if max_active_sessions == usize::MAX {
        return Ok(());
    }
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM dashboard_session WHERE username = ? AND revoked_at_ms IS NULL AND expires_at_ms > ?",
    )
    .bind(username)
    .bind(now_ms)
    .fetch_one(&mut **transaction)
    .await
    .map_err(map_query_error)?;
    (count < max_active_sessions as i64)
        .then_some(())
        .ok_or(PersistenceError::Conflict)
}

async fn enforce_session_cap_mysql(
    transaction: &mut sqlx::Transaction<'_, sqlx::MySql>,
    username: &str,
    now_ms: i64,
    max_active_sessions: usize,
) -> Result<(), PersistenceError> {
    if max_active_sessions == usize::MAX {
        return Ok(());
    }
    // The username/active index makes this a next-key range lock, including
    // the empty range, until the create-and-audit transaction commits.
    let records = sqlx::query(
        "SELECT token_hash FROM dashboard_session WHERE username = ? AND revoked_at_ms IS NULL AND expires_at_ms > ? FOR UPDATE",
    )
    .bind(username)
    .bind(now_ms)
    .fetch_all(&mut **transaction)
    .await
    .map_err(map_query_error)?;
    (records.len() < max_active_sessions)
        .then_some(())
        .ok_or(PersistenceError::Conflict)
}

async fn enforce_session_cap_postgres(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    username: &str,
    now_ms: i64,
    max_active_sessions: usize,
) -> Result<(), PersistenceError> {
    if max_active_sessions == usize::MAX {
        return Ok(());
    }
    // PostgreSQL predicate locks alone do not protect an empty range at the
    // normal isolation level. A transaction-scoped advisory lock serializes
    // this user's create path without exposing a token-derived identifier.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
        .bind(username)
        .execute(&mut **transaction)
        .await
        .map_err(map_query_error)?;
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM dashboard_session WHERE username = $1 AND revoked_at_ms IS NULL AND expires_at_ms > $2",
    )
    .bind(username)
    .bind(now_ms)
    .fetch_one(&mut **transaction)
    .await
    .map_err(map_query_error)?;
    (count < max_active_sessions as i64)
        .then_some(())
        .ok_or(PersistenceError::Conflict)
}

async fn find_sqlite(
    pool: &SqlitePool,
    token_hash: &SessionTokenHash,
) -> Result<Option<SessionRecord>, PersistenceError> {
    sqlx::query("SELECT session_id, token_hash, username, created_at_ms, expires_at_ms, last_seen_at_ms, revoked_at_ms FROM dashboard_session WHERE token_hash = ?")
        .bind(token_hash.bytes().to_vec())
        .fetch_optional(pool)
        .await
        .map_err(map_query_error)?
        .map(session_from_row)
        .transpose()
}

async fn find_mysql(
    pool: &MySqlPool,
    token_hash: &SessionTokenHash,
) -> Result<Option<SessionRecord>, PersistenceError> {
    sqlx::query("SELECT CAST(session_id AS BINARY) AS session_id, token_hash, username, created_at_ms, expires_at_ms, last_seen_at_ms, revoked_at_ms FROM dashboard_session WHERE token_hash = ?")
        .bind(token_hash.bytes().to_vec())
        .fetch_optional(pool)
        .await
        .map_err(map_query_error)?
        .map(session_from_mysql_row)
        .transpose()
}

async fn find_postgres(
    pool: &PgPool,
    token_hash: &SessionTokenHash,
) -> Result<Option<SessionRecord>, PersistenceError> {
    sqlx::query("SELECT session_id, token_hash, username, created_at_ms, expires_at_ms, last_seen_at_ms, revoked_at_ms FROM dashboard_session WHERE token_hash = $1")
        .bind(token_hash.bytes().to_vec())
        .fetch_optional(pool)
        .await
        .map_err(map_query_error)?
        .map(session_from_row)
        .transpose()
}

async fn list_sqlite(pool: &SqlitePool, query: SessionQuery) -> Result<SessionPage, PersistenceError> {
    let rows = sqlx::query("SELECT session_id, token_hash, username, created_at_ms, expires_at_ms, last_seen_at_ms, revoked_at_ms FROM dashboard_session WHERE (? IS NULL OR username = ?) AND (? IS NULL OR created_at_ms < ? OR (created_at_ms = ? AND session_id < ?)) ORDER BY created_at_ms DESC, session_id DESC LIMIT ?")
        .bind(query.username.clone()).bind(query.username.clone())
        .bind(query.cursor.as_ref().map(|cursor| cursor.created_at_ms)).bind(query.cursor.as_ref().map(|cursor| cursor.created_at_ms)).bind(query.cursor.as_ref().map(|cursor| cursor.created_at_ms)).bind(query.cursor.as_ref().map(|cursor| cursor.session_id.clone()))
        .bind((query.limit + 1) as i64)
        .fetch_all(pool)
        .await
        .map_err(map_query_error)?;
    page_from_rows(rows, query)
}

async fn list_mysql(pool: &MySqlPool, query: SessionQuery) -> Result<SessionPage, PersistenceError> {
    let rows = sqlx::query("SELECT CAST(session_id AS BINARY) AS session_id, token_hash, username, created_at_ms, expires_at_ms, last_seen_at_ms, revoked_at_ms FROM dashboard_session WHERE (? IS NULL OR username = ?) AND (? IS NULL OR created_at_ms < ? OR (created_at_ms = ? AND session_id < ?)) ORDER BY created_at_ms DESC, session_id DESC LIMIT ?")
        .bind(query.username.clone()).bind(query.username.clone())
        .bind(query.cursor.as_ref().map(|cursor| cursor.created_at_ms)).bind(query.cursor.as_ref().map(|cursor| cursor.created_at_ms)).bind(query.cursor.as_ref().map(|cursor| cursor.created_at_ms)).bind(query.cursor.as_ref().map(|cursor| cursor.session_id.clone()))
        .bind((query.limit + 1) as i64)
        .fetch_all(pool)
        .await
        .map_err(map_query_error)?;
    mysql_page_from_rows(rows, query)
}

async fn list_postgres(pool: &PgPool, query: SessionQuery) -> Result<SessionPage, PersistenceError> {
    let rows = sqlx::query("SELECT session_id, token_hash, username, created_at_ms, expires_at_ms, last_seen_at_ms, revoked_at_ms FROM dashboard_session WHERE ($1::text IS NULL OR username = $2) AND ($3::bigint IS NULL OR created_at_ms < $4 OR (created_at_ms = $5 AND session_id < $6)) ORDER BY created_at_ms DESC, session_id DESC LIMIT $7")
        .bind(query.username.clone()).bind(query.username.clone())
        .bind(query.cursor.as_ref().map(|cursor| cursor.created_at_ms)).bind(query.cursor.as_ref().map(|cursor| cursor.created_at_ms)).bind(query.cursor.as_ref().map(|cursor| cursor.created_at_ms)).bind(query.cursor.as_ref().map(|cursor| cursor.session_id.clone()))
        .bind((query.limit + 1) as i64)
        .fetch_all(pool)
        .await
        .map_err(map_query_error)?;
    page_from_rows(rows, query)
}

fn page_from_rows<R: Row>(rows: Vec<R>, query: SessionQuery) -> Result<SessionPage, PersistenceError>
where
    for<'r> &'r str: sqlx::ColumnIndex<R>,
    for<'r> Vec<u8>: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'r> String: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'r> i64: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'r> Option<i64>: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
{
    let mut records = rows.into_iter().map(session_from_row).collect::<Result<Vec<_>, _>>()?;
    let has_more = records.len() > query.limit;
    records.truncate(query.limit);
    let next_cursor = if has_more {
        records.last().map(|record| SessionCursor {
            created_at_ms: record.created_at_ms,
            session_id: record.session_id.clone(),
        })
    } else {
        None
    };
    Ok(SessionPage { records, next_cursor })
}

fn session_from_row<R: Row>(row: R) -> Result<SessionRecord, PersistenceError>
where
    for<'r> &'r str: sqlx::ColumnIndex<R>,
    for<'r> Vec<u8>: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'r> String: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'r> i64: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'r> Option<i64>: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
{
    let bytes: Vec<u8> = row.try_get("token_hash").map_err(|_| PersistenceError::CorruptedData)?;
    let token_hash = bytes_to_hash(bytes)?;
    Ok(SessionRecord {
        session_id: row.try_get("session_id").map_err(|_| PersistenceError::CorruptedData)?,
        token_hash,
        username: row.try_get("username").map_err(|_| PersistenceError::CorruptedData)?,
        created_at_ms: row
            .try_get("created_at_ms")
            .map_err(|_| PersistenceError::CorruptedData)?,
        expires_at_ms: row
            .try_get("expires_at_ms")
            .map_err(|_| PersistenceError::CorruptedData)?,
        last_seen_at_ms: row
            .try_get("last_seen_at_ms")
            .map_err(|_| PersistenceError::CorruptedData)?,
        revoked_at_ms: row
            .try_get("revoked_at_ms")
            .map_err(|_| PersistenceError::CorruptedData)?,
    })
}

fn mysql_page_from_rows(rows: Vec<MySqlRow>, query: SessionQuery) -> Result<SessionPage, PersistenceError> {
    let mut records = rows
        .into_iter()
        .map(session_from_mysql_row)
        .collect::<Result<Vec<_>, _>>()?;
    let has_more = records.len() > query.limit;
    records.truncate(query.limit);
    let next_cursor = has_more
        .then(|| {
            records.last().map(|record| SessionCursor {
                created_at_ms: record.created_at_ms,
                session_id: record.session_id.clone(),
            })
        })
        .flatten();
    Ok(SessionPage { records, next_cursor })
}

fn session_from_mysql_row(row: MySqlRow) -> Result<SessionRecord, PersistenceError> {
    let bytes: Vec<u8> = row.try_get("token_hash").map_err(|_| PersistenceError::CorruptedData)?;
    let username = String::from_utf8(
        row.try_get::<Vec<u8>, _>("username")
            .map_err(|_| PersistenceError::CorruptedData)?,
    )
    .map_err(|_| PersistenceError::CorruptedData)?;
    Ok(SessionRecord {
        session_id: String::from_utf8(
            row.try_get::<Vec<u8>, _>("session_id")
                .map_err(|_| PersistenceError::CorruptedData)?,
        )
        .map_err(|_| PersistenceError::CorruptedData)?,
        token_hash: bytes_to_hash(bytes)?,
        username,
        created_at_ms: row
            .try_get("created_at_ms")
            .map_err(|_| PersistenceError::CorruptedData)?,
        expires_at_ms: row
            .try_get("expires_at_ms")
            .map_err(|_| PersistenceError::CorruptedData)?,
        last_seen_at_ms: row
            .try_get("last_seen_at_ms")
            .map_err(|_| PersistenceError::CorruptedData)?,
        revoked_at_ms: row
            .try_get("revoked_at_ms")
            .map_err(|_| PersistenceError::CorruptedData)?,
    })
}

fn bytes_to_hash(bytes: Vec<u8>) -> Result<SessionTokenHash, PersistenceError> {
    let bytes: [u8; 32] = bytes.try_into().map_err(|_| PersistenceError::CorruptedData)?;
    Ok(SessionTokenHash(bytes))
}

async fn insert_audit_sqlite(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    event: AuditEvent,
) -> Result<(), PersistenceError> {
    let detail = event
        .detail
        .map(|value| serde_json::to_string(&value))
        .transpose()
        .map_err(PersistenceError::Serialization)?;
    sqlx::query("INSERT INTO dashboard_audit_event (event_id, request_id, actor_kind, actor_username, action, resource_type, resource_name, environment_id, outcome, detail_json, created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(event.event_id).bind(event.request_id).bind(event.actor.kind.code().to_string()).bind(event.actor.username)
        .bind(event.action.code().to_string()).bind(event.resource_type.code().to_string()).bind(event.resource_name)
        .bind(event.environment_id.map(|id| id.0)).bind(event.outcome.code().to_string()).bind(detail).bind(event.created_at_ms)
        .execute(&mut **transaction).await.map_err(map_query_error)?;
    Ok(())
}

async fn insert_audit_mysql(
    transaction: &mut sqlx::Transaction<'_, sqlx::MySql>,
    event: AuditEvent,
) -> Result<(), PersistenceError> {
    let detail = event
        .detail
        .map(|value| serde_json::to_string(&value))
        .transpose()
        .map_err(PersistenceError::Serialization)?;
    sqlx::query("INSERT INTO dashboard_audit_event (event_id, request_id, actor_kind, actor_username, action, resource_type, resource_name, environment_id, outcome, detail_json, created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(event.event_id).bind(event.request_id).bind(event.actor.kind.code().to_string()).bind(event.actor.username)
        .bind(event.action.code().to_string()).bind(event.resource_type.code().to_string()).bind(event.resource_name)
        .bind(event.environment_id.map(|id| id.0)).bind(event.outcome.code().to_string()).bind(detail).bind(event.created_at_ms)
        .execute(&mut **transaction).await.map_err(map_query_error)?;
    Ok(())
}

async fn insert_audit_postgres(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    event: AuditEvent,
) -> Result<(), PersistenceError> {
    let detail = event
        .detail
        .map(|value| serde_json::to_string(&value))
        .transpose()
        .map_err(PersistenceError::Serialization)?;
    sqlx::query("INSERT INTO dashboard_audit_event (event_id, request_id, actor_kind, actor_username, action, resource_type, resource_name, environment_id, outcome, detail_json, created_at_ms) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)")
        .bind(event.event_id).bind(event.request_id).bind(event.actor.kind.code().to_string()).bind(event.actor.username)
        .bind(event.action.code().to_string()).bind(event.resource_type.code().to_string()).bind(event.resource_name)
        .bind(event.environment_id.map(|id| id.0)).bind(event.outcome.code().to_string()).bind(detail).bind(event.created_at_ms)
        .execute(&mut **transaction).await.map_err(map_query_error)?;
    Ok(())
}

async fn delete_before_sqlite(pool: &SqlitePool, cutoff: i64, limit: usize) -> Result<u64, PersistenceError> {
    let result = sqlx::query("DELETE FROM dashboard_session WHERE token_hash IN (SELECT token_hash FROM dashboard_session WHERE COALESCE(revoked_at_ms, expires_at_ms) < ? ORDER BY COALESCE(revoked_at_ms, expires_at_ms) LIMIT ?)")
        .bind(cutoff).bind(limit as i64).execute(pool).await.map_err(map_query_error)?;
    Ok(result.rows_affected())
}

async fn delete_before_mysql(pool: &MySqlPool, cutoff: i64, limit: usize) -> Result<u64, PersistenceError> {
    let result = sqlx::query("DELETE FROM dashboard_session WHERE token_hash IN (SELECT token_hash FROM (SELECT token_hash FROM dashboard_session WHERE COALESCE(revoked_at_ms, expires_at_ms) < ? ORDER BY COALESCE(revoked_at_ms, expires_at_ms) LIMIT ?) AS cleanup_rows)")
        .bind(cutoff).bind(limit as i64).execute(pool).await.map_err(map_query_error)?;
    Ok(result.rows_affected())
}

async fn delete_before_postgres(pool: &PgPool, cutoff: i64, limit: usize) -> Result<u64, PersistenceError> {
    let result = sqlx::query("DELETE FROM dashboard_session WHERE token_hash IN (SELECT token_hash FROM dashboard_session WHERE COALESCE(revoked_at_ms, expires_at_ms) < $1 ORDER BY COALESCE(revoked_at_ms, expires_at_ms) LIMIT $2)")
        .bind(cutoff).bind(limit as i64).execute(pool).await.map_err(map_query_error)?;
    Ok(result.rows_affected())
}
