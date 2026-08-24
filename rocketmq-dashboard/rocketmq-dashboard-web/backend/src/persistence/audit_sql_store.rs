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
use super::PersistenceError;
use super::SqlPersistence;
use super::map_query_error;
use crate::model::AuditAction;
use crate::model::AuditActor;
use crate::model::AuditActorKind;
use crate::model::AuditEvent;
use crate::model::AuditOutcome;
use crate::model::AuditResourceType;
use crate::model::EnvironmentId;
use crate::persistence::audit_repository::AuditCursor;
use crate::persistence::audit_repository::AuditPage;
use crate::persistence::audit_repository::AuditQuery;
use sqlx::MySql;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Sqlite;
use sqlx::Transaction;
use sqlx::mysql::MySqlRow;

impl SqlPersistence {
    pub(crate) async fn append_audit_event(&self, event: AuditEvent) -> Result<(), PersistenceError> {
        let detail = event
            .detail
            .map(|value| serde_json::to_string(&value))
            .transpose()
            .map_err(PersistenceError::Serialization)?;
        match &self.pool {
            DatabasePool::Sqlite(pool) => {
                sqlx::query("INSERT INTO dashboard_audit_event (event_id, request_id, actor_kind, actor_username, action, resource_type, resource_name, environment_id, outcome, detail_json, created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                    .bind(event.event_id).bind(event.request_id).bind(event.actor.kind.code()).bind(event.actor.username)
                    .bind(event.action.code()).bind(event.resource_type.code()).bind(event.resource_name)
                    .bind(event.environment_id.map(|id| id.0)).bind(event.outcome.code()).bind(detail).bind(event.created_at_ms)
                    .execute(pool).await.map_err(map_query_error)?;
            }
            DatabasePool::MySql(pool) => {
                sqlx::query("INSERT INTO dashboard_audit_event (event_id, request_id, actor_kind, actor_username, action, resource_type, resource_name, environment_id, outcome, detail_json, created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                    .bind(event.event_id).bind(event.request_id).bind(event.actor.kind.code()).bind(event.actor.username)
                    .bind(event.action.code()).bind(event.resource_type.code()).bind(event.resource_name)
                    .bind(event.environment_id.map(|id| id.0)).bind(event.outcome.code()).bind(detail).bind(event.created_at_ms)
                    .execute(pool).await.map_err(map_query_error)?;
            }
            DatabasePool::Postgres(pool) => {
                sqlx::query("INSERT INTO dashboard_audit_event (event_id, request_id, actor_kind, actor_username, action, resource_type, resource_name, environment_id, outcome, detail_json, created_at_ms) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)")
                    .bind(event.event_id).bind(event.request_id).bind(event.actor.kind.code()).bind(event.actor.username)
                    .bind(event.action.code()).bind(event.resource_type.code()).bind(event.resource_name)
                    .bind(event.environment_id.map(|id| id.0)).bind(event.outcome.code()).bind(detail).bind(event.created_at_ms)
                    .execute(pool).await.map_err(map_query_error)?;
            }
        }
        Ok(())
    }

    pub(crate) async fn query_audit_events(&self, query: AuditQuery) -> Result<AuditPage, PersistenceError> {
        match &self.pool {
            DatabasePool::Sqlite(pool) => query_sqlite(pool, query).await,
            DatabasePool::MySql(pool) => query_mysql(pool, query).await,
            DatabasePool::Postgres(pool) => query_postgres(pool, query).await,
        }
    }

    pub(crate) async fn delete_audit_before(&self, cutoff_ms: i64, limit: usize) -> Result<u64, PersistenceError> {
        match &self.pool {
            DatabasePool::Sqlite(pool) => {
                let result = sqlx::query("DELETE FROM dashboard_audit_event WHERE event_id IN (SELECT event_id FROM dashboard_audit_event WHERE created_at_ms < ? ORDER BY created_at_ms, event_id LIMIT ?)")
                    .bind(cutoff_ms)
                    .bind(limit as i64)
                    .execute(pool)
                    .await
                    .map_err(map_query_error)?;
                Ok(result.rows_affected())
            }
            DatabasePool::MySql(pool) => {
                // MySQL does not permit selecting from a table being deleted
                // without an additional derived-table level.
                let result = sqlx::query("DELETE FROM dashboard_audit_event WHERE event_id IN (SELECT event_id FROM (SELECT event_id FROM dashboard_audit_event WHERE created_at_ms < ? ORDER BY created_at_ms, event_id LIMIT ?) AS cleanup_rows)")
                    .bind(cutoff_ms)
                    .bind(limit as i64)
                    .execute(pool)
                    .await
                    .map_err(map_query_error)?;
                Ok(result.rows_affected())
            }
            DatabasePool::Postgres(pool) => {
                let result = sqlx::query("DELETE FROM dashboard_audit_event WHERE event_id IN (SELECT event_id FROM dashboard_audit_event WHERE created_at_ms < $1 ORDER BY created_at_ms, event_id LIMIT $2)")
                    .bind(cutoff_ms)
                    .bind(limit as i64)
                    .execute(pool)
                    .await
                    .map_err(map_query_error)?;
                Ok(result.rows_affected())
            }
        }
    }
}

/// Inserts a safe, already-built audit event on the same SQLite transaction
/// as the local aggregate mutation. Keeping this helper database-specific
/// avoids widening the persistence facade to a dynamic executor.
pub(super) async fn append_sqlite_audit_event_in_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
    event: &AuditEvent,
) -> Result<(), PersistenceError> {
    let detail = audit_detail(event)?;
    sqlx::query("INSERT INTO dashboard_audit_event (event_id, request_id, actor_kind, actor_username, action, resource_type, resource_name, environment_id, outcome, detail_json, created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(&event.event_id)
        .bind(&event.request_id)
        .bind(event.actor.kind.code())
        .bind(event.actor.username.as_deref())
        .bind(event.action.code())
        .bind(event.resource_type.code())
        .bind(event.resource_name.as_deref())
        .bind(event.environment_id.as_ref().map(|id| id.0.as_str()))
        .bind(event.outcome.code())
        .bind(detail)
        .bind(event.created_at_ms)
        .execute(&mut **transaction)
        .await
        .map_err(map_query_error)?;
    Ok(())
}

/// Inserts a safe, already-built audit event on the same MySQL transaction
/// as the local aggregate mutation.
pub(super) async fn append_mysql_audit_event_in_transaction(
    transaction: &mut Transaction<'_, MySql>,
    event: &AuditEvent,
) -> Result<(), PersistenceError> {
    let detail = audit_detail(event)?;
    sqlx::query("INSERT INTO dashboard_audit_event (event_id, request_id, actor_kind, actor_username, action, resource_type, resource_name, environment_id, outcome, detail_json, created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(&event.event_id)
        .bind(&event.request_id)
        .bind(event.actor.kind.code())
        .bind(event.actor.username.as_deref())
        .bind(event.action.code())
        .bind(event.resource_type.code())
        .bind(event.resource_name.as_deref())
        .bind(event.environment_id.as_ref().map(|id| id.0.as_str()))
        .bind(event.outcome.code())
        .bind(detail)
        .bind(event.created_at_ms)
        .execute(&mut **transaction)
        .await
        .map_err(map_query_error)?;
    Ok(())
}

/// Inserts a safe, already-built audit event on the same PostgreSQL
/// transaction as the local aggregate mutation.
pub(super) async fn append_postgres_audit_event_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    event: &AuditEvent,
) -> Result<(), PersistenceError> {
    let detail = audit_detail(event)?;
    sqlx::query("INSERT INTO dashboard_audit_event (event_id, request_id, actor_kind, actor_username, action, resource_type, resource_name, environment_id, outcome, detail_json, created_at_ms) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)")
        .bind(&event.event_id)
        .bind(&event.request_id)
        .bind(event.actor.kind.code())
        .bind(event.actor.username.as_deref())
        .bind(event.action.code())
        .bind(event.resource_type.code())
        .bind(event.resource_name.as_deref())
        .bind(event.environment_id.as_ref().map(|id| id.0.as_str()))
        .bind(event.outcome.code())
        .bind(detail)
        .bind(event.created_at_ms)
        .execute(&mut **transaction)
        .await
        .map_err(map_query_error)?;
    Ok(())
}

fn audit_detail(event: &AuditEvent) -> Result<Option<String>, PersistenceError> {
    event
        .detail
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(PersistenceError::Serialization)
}

async fn query_sqlite(pool: &super::SqlitePool, query: AuditQuery) -> Result<AuditPage, PersistenceError> {
    let bindings = AuditQueryBindings::from_query(&query);
    let rows = sqlx::query(SQLITE_AUDIT_QUERY)
        .bind(bindings.start_ms)
        .bind(bindings.end_ms)
        .bind(&bindings.actor)
        .bind(&bindings.actor)
        .bind(&bindings.action)
        .bind(&bindings.action)
        .bind(&bindings.outcome)
        .bind(&bindings.outcome)
        .bind(&bindings.environment_id)
        .bind(&bindings.environment_id)
        .bind(bindings.cursor_created_at_ms)
        .bind(bindings.cursor_created_at_ms)
        .bind(bindings.cursor_created_at_ms)
        .bind(&bindings.cursor_event_id)
        .bind(bindings.limit_plus_one)
        .fetch_all(pool)
        .await
        .map_err(map_query_error)?;
    audit_page_from_rows(rows, query.limit)
}

async fn query_mysql(pool: &super::MySqlPool, query: AuditQuery) -> Result<AuditPage, PersistenceError> {
    let bindings = AuditQueryBindings::from_query(&query);
    let rows = sqlx::query(MYSQL_AUDIT_QUERY)
        .bind(bindings.start_ms)
        .bind(bindings.end_ms)
        .bind(&bindings.actor)
        .bind(&bindings.actor)
        .bind(&bindings.action)
        .bind(&bindings.action)
        .bind(&bindings.outcome)
        .bind(&bindings.outcome)
        .bind(&bindings.environment_id)
        .bind(&bindings.environment_id)
        .bind(bindings.cursor_created_at_ms)
        .bind(bindings.cursor_created_at_ms)
        .bind(bindings.cursor_created_at_ms)
        .bind(&bindings.cursor_event_id)
        .bind(bindings.limit_plus_one)
        .fetch_all(pool)
        .await
        .map_err(map_query_error)?;
    mysql_audit_page_from_rows(rows, query.limit)
}

async fn query_postgres(pool: &super::PgPool, query: AuditQuery) -> Result<AuditPage, PersistenceError> {
    let bindings = AuditQueryBindings::from_query(&query);
    let rows = sqlx::query(POSTGRES_AUDIT_QUERY)
        .bind(bindings.start_ms)
        .bind(bindings.end_ms)
        .bind(&bindings.actor)
        .bind(&bindings.action)
        .bind(&bindings.outcome)
        .bind(&bindings.environment_id)
        .bind(bindings.cursor_created_at_ms)
        .bind(&bindings.cursor_event_id)
        .bind(bindings.limit_plus_one)
        .fetch_all(pool)
        .await
        .map_err(map_query_error)?;
    audit_page_from_rows(rows, query.limit)
}

const SQLITE_AUDIT_QUERY: &str = concat!(
    "SELECT event_id, request_id, actor_kind, actor_username, action, resource_type, resource_name, environment_id, outcome, detail_json, created_at_ms FROM dashboard_audit_event",
    " WHERE created_at_ms >= ? AND created_at_ms <= ?",
    " AND (? IS NULL OR COALESCE(actor_username, actor_kind) = ?)",
    " AND (? IS NULL OR action = ?)",
    " AND (? IS NULL OR outcome = ?)",
    " AND (? IS NULL OR environment_id = ?)",
    " AND (? IS NULL OR created_at_ms < ? OR (created_at_ms = ? AND event_id < ?))",
    " ORDER BY created_at_ms DESC, event_id DESC LIMIT ?"
);
const MYSQL_AUDIT_QUERY: &str = concat!(
    "SELECT event_id, request_id, actor_kind, actor_username, action, resource_type, resource_name, environment_id, outcome, detail_json, created_at_ms FROM dashboard_audit_event",
    " WHERE created_at_ms >= ? AND created_at_ms <= ?",
    " AND (? IS NULL OR COALESCE(actor_username, CAST(actor_kind AS BINARY)) = CAST(? AS BINARY))",
    " AND (? IS NULL OR action = ?)",
    " AND (? IS NULL OR outcome = ?)",
    " AND (? IS NULL OR environment_id = ?)",
    " AND (? IS NULL OR created_at_ms < ? OR (created_at_ms = ? AND event_id < ?))",
    " ORDER BY created_at_ms DESC, event_id DESC LIMIT ?"
);
const POSTGRES_AUDIT_QUERY: &str = concat!(
    "SELECT event_id, request_id, actor_kind, actor_username, action, resource_type, resource_name, environment_id, outcome, detail_json, created_at_ms FROM dashboard_audit_event",
    " WHERE created_at_ms >= $1 AND created_at_ms <= $2",
    " AND ($3::text IS NULL OR COALESCE(actor_username, actor_kind) = $3)",
    " AND ($4::text IS NULL OR action = $4)",
    " AND ($5::text IS NULL OR outcome = $5)",
    " AND ($6::text IS NULL OR environment_id = $6)",
    " AND ($7::bigint IS NULL OR created_at_ms < $7 OR (created_at_ms = $7 AND event_id < $8))",
    " ORDER BY created_at_ms DESC, event_id DESC LIMIT $9"
);

struct AuditQueryBindings {
    start_ms: i64,
    end_ms: i64,
    actor: Option<String>,
    action: Option<String>,
    outcome: Option<String>,
    environment_id: Option<String>,
    cursor_created_at_ms: Option<i64>,
    cursor_event_id: Option<String>,
    limit_plus_one: i64,
}

fn mysql_audit_page_from_rows(rows: Vec<MySqlRow>, limit: usize) -> Result<AuditPage, PersistenceError> {
    let mut events = rows
        .into_iter()
        .map(mysql_audit_event_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    let has_more = events.len() > limit;
    events.truncate(limit);
    let next_cursor = has_more
        .then(|| {
            events.last().map(|event| AuditCursor {
                created_at_ms: event.created_at_ms,
                event_id: event.event_id.clone(),
            })
        })
        .flatten();
    Ok(AuditPage { events, next_cursor })
}

fn mysql_audit_event_from_row(row: MySqlRow) -> Result<AuditEvent, PersistenceError> {
    let actor_kind =
        AuditActorKind::parse(&mysql_string(&row, "actor_kind")?).ok_or(PersistenceError::CorruptedData)?;
    let action = AuditAction::parse(&mysql_string(&row, "action")?).ok_or(PersistenceError::CorruptedData)?;
    let resource_type =
        AuditResourceType::parse(&mysql_string(&row, "resource_type")?).ok_or(PersistenceError::CorruptedData)?;
    let outcome = AuditOutcome::parse(&mysql_string(&row, "outcome")?).ok_or(PersistenceError::CorruptedData)?;
    let detail = row
        .try_get::<Option<String>, _>("detail_json")
        .map_err(|_| PersistenceError::CorruptedData)?
        .map(|encoded| serde_json::from_str(&encoded).map_err(|_| PersistenceError::CorruptedData))
        .transpose()?;
    Ok(AuditEvent {
        event_id: mysql_string(&row, "event_id")?,
        request_id: mysql_string(&row, "request_id")?,
        actor: AuditActor {
            kind: actor_kind,
            username: mysql_optional_utf8(&row, "actor_username")?,
        },
        action,
        resource_type,
        resource_name: mysql_optional_utf8(&row, "resource_name")?,
        environment_id: mysql_optional_utf8(&row, "environment_id")?.map(EnvironmentId),
        outcome,
        detail,
        created_at_ms: row
            .try_get("created_at_ms")
            .map_err(|_| PersistenceError::CorruptedData)?,
    })
}

fn mysql_string(row: &MySqlRow, column: &str) -> Result<String, PersistenceError> {
    String::from_utf8(
        row.try_get::<Vec<u8>, _>(column)
            .map_err(|_| PersistenceError::CorruptedData)?,
    )
    .map_err(|_| PersistenceError::CorruptedData)
}

fn mysql_optional_utf8(row: &MySqlRow, column: &str) -> Result<Option<String>, PersistenceError> {
    row.try_get::<Option<Vec<u8>>, _>(column)
        .map_err(|_| PersistenceError::CorruptedData)?
        .map(|bytes| String::from_utf8(bytes).map_err(|_| PersistenceError::CorruptedData))
        .transpose()
}

impl AuditQueryBindings {
    fn from_query(query: &AuditQuery) -> Self {
        Self {
            start_ms: query.start_ms,
            end_ms: query.end_ms,
            actor: query.actor.clone(),
            action: query.action.map(|value| value.code().to_string()),
            outcome: query.outcome.map(|value| value.code().to_string()),
            environment_id: query.environment_id.clone(),
            cursor_created_at_ms: query.cursor.as_ref().map(|cursor| cursor.created_at_ms),
            cursor_event_id: query.cursor.as_ref().map(|cursor| cursor.event_id.clone()),
            limit_plus_one: (query.limit + 1) as i64,
        }
    }
}

fn audit_page_from_rows<R>(rows: Vec<R>, limit: usize) -> Result<AuditPage, PersistenceError>
where
    R: super::Row,
    for<'r> &'r str: sqlx::ColumnIndex<R>,
    for<'r> String: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'r> Option<String>: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'r> i64: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
{
    let mut events = rows
        .into_iter()
        .map(audit_event_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    let has_more = events.len() > limit;
    events.truncate(limit);
    let next_cursor = has_more
        .then(|| {
            events.last().map(|event| AuditCursor {
                created_at_ms: event.created_at_ms,
                event_id: event.event_id.clone(),
            })
        })
        .flatten();
    Ok(AuditPage { events, next_cursor })
}

fn audit_event_from_row<R>(row: R) -> Result<AuditEvent, PersistenceError>
where
    R: super::Row,
    for<'r> &'r str: sqlx::ColumnIndex<R>,
    for<'r> String: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'r> Option<String>: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'r> i64: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
{
    let actor_kind = AuditActorKind::parse(
        &row.try_get::<String, _>("actor_kind")
            .map_err(|_| PersistenceError::CorruptedData)?,
    )
    .ok_or(PersistenceError::CorruptedData)?;
    let action = AuditAction::parse(
        &row.try_get::<String, _>("action")
            .map_err(|_| PersistenceError::CorruptedData)?,
    )
    .ok_or(PersistenceError::CorruptedData)?;
    let resource_type = AuditResourceType::parse(
        &row.try_get::<String, _>("resource_type")
            .map_err(|_| PersistenceError::CorruptedData)?,
    )
    .ok_or(PersistenceError::CorruptedData)?;
    let outcome = AuditOutcome::parse(
        &row.try_get::<String, _>("outcome")
            .map_err(|_| PersistenceError::CorruptedData)?,
    )
    .ok_or(PersistenceError::CorruptedData)?;
    let detail = row
        .try_get::<Option<String>, _>("detail_json")
        .map_err(|_| PersistenceError::CorruptedData)?
        .map(|encoded| serde_json::from_str(&encoded).map_err(|_| PersistenceError::CorruptedData))
        .transpose()?;
    Ok(AuditEvent {
        event_id: row.try_get("event_id").map_err(|_| PersistenceError::CorruptedData)?,
        request_id: row.try_get("request_id").map_err(|_| PersistenceError::CorruptedData)?,
        actor: AuditActor {
            kind: actor_kind,
            username: row
                .try_get("actor_username")
                .map_err(|_| PersistenceError::CorruptedData)?,
        },
        action,
        resource_type,
        resource_name: row
            .try_get("resource_name")
            .map_err(|_| PersistenceError::CorruptedData)?,
        environment_id: row
            .try_get::<Option<String>, _>("environment_id")
            .map_err(|_| PersistenceError::CorruptedData)?
            .map(EnvironmentId),
        outcome,
        detail,
        created_at_ms: row
            .try_get("created_at_ms")
            .map_err(|_| PersistenceError::CorruptedData)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::NewSession;
    use crate::model::SessionTokenHash;
    use crate::model::StorageBackend;
    use crate::persistence::migration;
    use serde_json::json;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn store() -> SqlPersistence {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("open SQLite storage");
        let schema_version = migration::migrate_sqlite(&pool).await.expect("migrate SQLite storage");
        SqlPersistence {
            pool: DatabasePool::Sqlite(pool),
            backend: StorageBackend::Sqlite,
            schema_version,
        }
    }

    fn event(id: &str, created_at_ms: i64) -> AuditEvent {
        AuditEvent {
            event_id: id.to_string(),
            request_id: uuid::Uuid::now_v7().to_string(),
            actor: AuditActor::admin("operator"),
            action: AuditAction::TopicCreate,
            resource_type: AuditResourceType::Topic,
            resource_name: Some("orders".to_string()),
            environment_id: None,
            outcome: AuditOutcome::Succeeded,
            detail: Some(json!({"target":"orders"})),
            created_at_ms,
        }
    }

    #[tokio::test]
    async fn sqlite_audit_query_uses_descending_stable_keyset_pagination() {
        let store = store().await;
        let first = uuid::Uuid::now_v7().to_string();
        let second = uuid::Uuid::now_v7().to_string();
        let third = uuid::Uuid::now_v7().to_string();
        store
            .append_audit_event(event(&first, 100))
            .await
            .expect("append first");
        store
            .append_audit_event(event(&second, 100))
            .await
            .expect("append second");
        store.append_audit_event(event(&third, 99)).await.expect("append third");

        let first_page = store
            .query_audit_events(AuditQuery {
                start_ms: 0,
                end_ms: 100,
                actor: None,
                action: None,
                outcome: None,
                environment_id: None,
                cursor: None,
                limit: 2,
            })
            .await
            .expect("query first page");
        assert_eq!(first_page.events.len(), 2);
        let cursor = first_page.next_cursor.expect("next cursor");
        store
            .append_audit_event(event(&uuid::Uuid::now_v7().to_string(), 101))
            .await
            .expect("append newer event");
        let second_page = store
            .query_audit_events(AuditQuery {
                start_ms: 0,
                end_ms: 101,
                actor: None,
                action: None,
                outcome: None,
                environment_id: None,
                cursor: Some(cursor),
                limit: 2,
            })
            .await
            .expect("query second page");
        assert_eq!(second_page.events.len(), 1);
        assert_eq!(second_page.events[0].event_id, third);
    }

    #[tokio::test]
    async fn sqlite_session_audit_transaction_rolls_back_when_the_audit_insert_fails() {
        let store = store().await;
        let duplicate = uuid::Uuid::now_v7().to_string();
        store
            .append_audit_event(event(&duplicate, 1))
            .await
            .expect("seed audit event");
        let hash = SessionTokenHash([7; 32]);
        let result = store
            .create_session_with_audit(
                NewSession {
                    session_id: uuid::Uuid::now_v7().to_string(),
                    token_hash: hash,
                    username: "operator".to_string(),
                    created_at_ms: 2,
                    expires_at_ms: 3,
                },
                event(&duplicate, 2),
            )
            .await;
        assert!(result.is_err());
        assert!(store.find_session(&hash).await.expect("read session").is_none());
    }

    #[tokio::test]
    async fn sqlite_capped_session_create_leaves_no_extra_session_or_audit_event() {
        let store = store().await;
        let first_hash = SessionTokenHash([8; 32]);
        store
            .create_session_with_audit_capped(
                NewSession {
                    session_id: uuid::Uuid::now_v7().to_string(),
                    token_hash: first_hash,
                    username: "operator".to_string(),
                    created_at_ms: 1,
                    expires_at_ms: 1_000,
                },
                event(&uuid::Uuid::now_v7().to_string(), 1),
                1,
                2,
            )
            .await
            .expect("create first capped session");
        assert!(
            store
                .find_session(&first_hash)
                .await
                .expect("read first session")
                .is_some()
        );
        let second_hash = SessionTokenHash([9; 32]);
        let rejected = store
            .create_session_with_audit_capped(
                NewSession {
                    session_id: uuid::Uuid::now_v7().to_string(),
                    token_hash: second_hash,
                    username: "operator".to_string(),
                    created_at_ms: 2,
                    expires_at_ms: 1_000,
                },
                event(&uuid::Uuid::now_v7().to_string(), 2),
                1,
                2,
            )
            .await;
        assert!(matches!(rejected, Err(PersistenceError::Conflict)));
        assert!(
            store
                .find_session(&second_hash)
                .await
                .expect("read second session")
                .is_none()
        );
        let events = store
            .query_audit_events(AuditQuery {
                start_ms: 0,
                end_ms: 10,
                actor: None,
                action: None,
                outcome: None,
                environment_id: None,
                cursor: None,
                limit: 10,
            })
            .await
            .expect("query audit events");
        assert_eq!(events.events.len(), 1);
    }
}
