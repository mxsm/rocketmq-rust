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

use super::format::BackupData;
use super::format::BackupEndpoint;
use super::format::BackupSession;
use super::validation::attach_endpoints;
use super::validation::parse_token_hash;
use crate::model::AuditAction;
use crate::model::AuditActor;
use crate::model::AuditActorKind;
use crate::model::AuditEvent;
use crate::model::AuditOutcome;
use crate::model::AuditResourceType;
use crate::model::ConsumerMonitorRule;
use crate::model::DashboardEnvironment;
use crate::model::EnvironmentId;
use crate::model::MetricSample;
use crate::model::SessionTokenHash;
use crate::model::StorageBackend;
use crate::persistence::error::PersistenceError;
use crate::persistence::sql_store::SqlPersistence;
use serde_json::Value;
use sqlx::Database;
use sqlx::MySqlConnection;
use sqlx::MySqlPool;
use sqlx::PgConnection;
use sqlx::PgPool;
use sqlx::Row;
use sqlx::SqliteConnection;
use sqlx::SqlitePool;

macro_rules! finish_read_transaction {
    ($connection:expr, $result:expr) => {{
        match $result {
            Ok(data) => {
                sqlx::query("COMMIT")
                    .execute(&mut **$connection)
                    .await
                    .map_err(PersistenceError::Query)?;
                Ok(data)
            }
            Err(error) => {
                let _ = sqlx::query("ROLLBACK").execute(&mut **$connection).await;
                Err(error)
            }
        }
    }};
}

macro_rules! finish_write_transaction {
    ($connection:expr, $result:expr) => {{
        match $result {
            Ok(()) => sqlx::query("COMMIT")
                .execute(&mut **$connection)
                .await
                .map_err(PersistenceError::Query)
                .map(|_| ()),
            Err(error) => {
                let _ = sqlx::query("ROLLBACK").execute(&mut **$connection).await;
                Err(error)
            }
        }
    }};
}

impl SqlPersistence {
    pub(crate) async fn snapshot_for_operations(&self) -> Result<BackupData, PersistenceError> {
        if let Some(pool) = self.sqlite_pool() {
            return snapshot_sqlite(pool).await;
        }
        if let Some(pool) = self.mysql_pool() {
            return snapshot_mysql(pool).await;
        }
        if let Some(pool) = self.postgres_pool() {
            return snapshot_postgres(pool).await;
        }
        Err(PersistenceError::ConnectionUnavailable)
    }

    pub(crate) async fn restore_for_operations(&self, data: &BackupData) -> Result<(), PersistenceError> {
        if let Some(pool) = self.sqlite_pool() {
            return restore_sqlite(pool, data).await;
        }
        if let Some(pool) = self.mysql_pool() {
            return restore_mysql(pool, data).await;
        }
        if let Some(pool) = self.postgres_pool() {
            return restore_postgres(pool, data).await;
        }
        Err(PersistenceError::ConnectionUnavailable)
    }
}

async fn snapshot_sqlite(pool: &SqlitePool) -> Result<BackupData, PersistenceError> {
    let mut connection = pool.acquire().await.map_err(PersistenceError::Query)?;
    sqlx::query("BEGIN")
        .execute(&mut *connection)
        .await
        .map_err(PersistenceError::Query)?;
    let result = read_snapshot_rows_sqlite(&mut connection).await;
    finish_read_transaction!(&mut connection, result)
}

async fn snapshot_mysql(pool: &MySqlPool) -> Result<BackupData, PersistenceError> {
    let mut connection = pool.acquire().await.map_err(PersistenceError::Query)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *connection)
        .await
        .map_err(PersistenceError::Query)?;
    sqlx::query("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
        .execute(&mut *connection)
        .await
        .map_err(PersistenceError::Query)?;
    let result = read_snapshot_rows_mysql(&mut connection).await;
    finish_read_transaction!(&mut connection, result)
}

async fn snapshot_postgres(pool: &PgPool) -> Result<BackupData, PersistenceError> {
    let mut connection = pool.acquire().await.map_err(PersistenceError::Query)?;
    sqlx::query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *connection)
        .await
        .map_err(PersistenceError::Query)?;
    let result = read_snapshot_rows_postgres(&mut connection).await;
    finish_read_transaction!(&mut connection, result)
}

macro_rules! read_snapshot_rows {
    ($connection:expr) => {{
        let environments = sqlx::query("SELECT environment_id, name, use_vip_channel, use_tls, revision, created_at_ms, updated_at_ms FROM dashboard_environment ORDER BY environment_id")
            .fetch_all(&mut *$connection).await.map_err(PersistenceError::Query)?;
        let endpoints = sqlx::query("SELECT endpoint_id, environment_id, endpoint_type, address, role, is_enabled, is_active, sort_order, created_at_ms, updated_at_ms FROM dashboard_endpoint ORDER BY environment_id, endpoint_id")
            .fetch_all(&mut *$connection).await.map_err(PersistenceError::Query)?;
        let monitors = sqlx::query("SELECT environment_id, consumer_group, min_count, max_diff_total, revision, created_at_ms, updated_at_ms FROM consumer_monitor_rule ORDER BY environment_id, consumer_group")
            .fetch_all(&mut *$connection).await.map_err(PersistenceError::Query)?;
        let history = sqlx::query("SELECT environment_id, metric_name, bucket_ms, dimensions_json, value FROM dashboard_history_sample ORDER BY environment_id, metric_name, bucket_ms, dimensions_json")
            .fetch_all(&mut *$connection).await.map_err(PersistenceError::Query)?;
        let sessions = sqlx::query("SELECT session_id, token_hash, username, created_at_ms, expires_at_ms, last_seen_at_ms, revoked_at_ms FROM dashboard_session ORDER BY session_id")
            .fetch_all(&mut *$connection).await.map_err(PersistenceError::Query)?;
        let audit = sqlx::query("SELECT event_id, request_id, actor_kind, actor_username, action, resource_type, resource_name, environment_id, outcome, detail_json, created_at_ms FROM dashboard_audit_event ORDER BY event_id")
            .fetch_all(&mut *$connection).await.map_err(PersistenceError::Query)?;
        decode_sql_snapshot(environments, endpoints, monitors, history, sessions, audit)
    }};
}

async fn read_snapshot_rows_sqlite(connection: &mut SqliteConnection) -> Result<BackupData, PersistenceError> {
    read_snapshot_rows!(connection)
}

async fn read_snapshot_rows_mysql(connection: &mut MySqlConnection) -> Result<BackupData, PersistenceError> {
    read_snapshot_rows!(connection)
}

async fn read_snapshot_rows_postgres(connection: &mut PgConnection) -> Result<BackupData, PersistenceError> {
    read_snapshot_rows!(connection)
}

fn decode_sql_snapshot<R>(
    environments: Vec<R>,
    endpoints: Vec<R>,
    monitors: Vec<R>,
    history: Vec<R>,
    sessions: Vec<R>,
    audit: Vec<R>,
) -> Result<BackupData, PersistenceError>
where
    R: Row,
    for<'r> bool: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'r> i64: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'r> i32: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'r> f64: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'r> String: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'r> Vec<u8>: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    usize: sqlx::ColumnIndex<R>,
{
    let mut data = BackupData::with_backend(match R::Database::NAME {
        "SQLite" => StorageBackend::Sqlite,
        "MySQL" => StorageBackend::MySql,
        "PostgreSQL" => StorageBackend::Postgres,
        _ => return Err(PersistenceError::ConnectionUnavailable),
    });
    for row in environments {
        data.environments.push(DashboardEnvironment {
            environment_id: EnvironmentId(row.try_get(0).map_err(PersistenceError::Query)?),
            name: row.try_get(1).map_err(PersistenceError::Query)?,
            use_vip_channel: row.try_get(2).map_err(PersistenceError::Query)?,
            use_tls: row.try_get(3).map_err(PersistenceError::Query)?,
            revision: crate::persistence::Revision(
                row.try_get::<i64, _>(4)
                    .map_err(PersistenceError::Query)?
                    .try_into()
                    .map_err(|_| PersistenceError::CorruptedData)?,
            ),
            created_at_ms: row.try_get(5).map_err(PersistenceError::Query)?,
            updated_at_ms: row.try_get(6).map_err(PersistenceError::Query)?,
            endpoints: Vec::new(),
        });
    }
    let endpoint_rows = endpoints
        .into_iter()
        .map(|row| {
            Ok(BackupEndpoint {
                environment_id: EnvironmentId(row.try_get(1).map_err(PersistenceError::Query)?),
                endpoint: crate::model::Endpoint {
                    endpoint_id: crate::model::EndpointId(row.try_get(0).map_err(PersistenceError::Query)?),
                    endpoint_type: crate::model::EndpointType::parse(
                        &row.try_get::<String, _>(2).map_err(PersistenceError::Query)?,
                    )
                    .map_err(PersistenceError::InvalidConfig)?,
                    address: row.try_get(3).map_err(PersistenceError::Query)?,
                    role: crate::model::EndpointRole::parse(
                        &row.try_get::<String, _>(4).map_err(PersistenceError::Query)?,
                    )
                    .map_err(PersistenceError::InvalidConfig)?,
                    is_enabled: row.try_get(5).map_err(PersistenceError::Query)?,
                    is_active: row.try_get(6).map_err(PersistenceError::Query)?,
                    sort_order: row.try_get(7).map_err(PersistenceError::Query)?,
                    created_at_ms: row.try_get(8).map_err(PersistenceError::Query)?,
                    updated_at_ms: row.try_get(9).map_err(PersistenceError::Query)?,
                },
            })
        })
        .collect::<Result<Vec<_>, PersistenceError>>()?;
    attach_endpoints(&mut data.environments, endpoint_rows)?;
    for row in monitors {
        data.monitors.push(ConsumerMonitorRule {
            environment_id: EnvironmentId(row.try_get(0).map_err(PersistenceError::Query)?),
            consumer_group: row.try_get(1).map_err(PersistenceError::Query)?,
            min_count: row.try_get(2).map_err(PersistenceError::Query)?,
            max_diff_total: row.try_get(3).map_err(PersistenceError::Query)?,
            revision: crate::persistence::Revision(
                row.try_get::<i64, _>(4)
                    .map_err(PersistenceError::Query)?
                    .try_into()
                    .map_err(|_| PersistenceError::CorruptedData)?,
            ),
            created_at_ms: row.try_get(5).map_err(PersistenceError::Query)?,
            updated_at_ms: row.try_get(6).map_err(PersistenceError::Query)?,
        });
    }
    for row in history {
        let dimensions: Vec<crate::model::MetricDimension> =
            serde_json::from_str(&row.try_get::<String, _>(3).map_err(PersistenceError::Query)?)
                .map_err(|_| PersistenceError::CorruptedData)?;
        data.history.push(MetricSample {
            environment_id: EnvironmentId(row.try_get(0).map_err(PersistenceError::Query)?),
            metric: row.try_get(1).map_err(PersistenceError::Query)?,
            bucket_ms: row.try_get(2).map_err(PersistenceError::Query)?,
            dimensions,
            value: row.try_get(4).map_err(PersistenceError::Query)?,
        });
    }
    for row in sessions {
        let bytes: Vec<u8> = row.try_get(1).map_err(PersistenceError::Query)?;
        let array: [u8; 32] = bytes.try_into().map_err(|_| PersistenceError::CorruptedData)?;
        data.sessions.push(BackupSession {
            session_id: row.try_get(0).map_err(PersistenceError::Query)?,
            token_hash: SessionTokenHash(array).lower_hex(),
            username: row.try_get(2).map_err(PersistenceError::Query)?,
            created_at_ms: row.try_get(3).map_err(PersistenceError::Query)?,
            expires_at_ms: row.try_get(4).map_err(PersistenceError::Query)?,
            last_seen_at_ms: row.try_get(5).map_err(PersistenceError::Query)?,
            revoked_at_ms: row.try_get(6).map_err(PersistenceError::Query)?,
        });
    }
    for row in audit {
        let detail: Option<String> = row.try_get(9).map_err(PersistenceError::Query)?;
        data.audit.push(AuditEvent {
            event_id: row.try_get(0).map_err(PersistenceError::Query)?,
            request_id: row.try_get(1).map_err(PersistenceError::Query)?,
            actor: AuditActor {
                kind: AuditActorKind::parse(&row.try_get::<String, _>(2).map_err(PersistenceError::Query)?)
                    .ok_or(PersistenceError::CorruptedData)?,
                username: row.try_get(3).map_err(PersistenceError::Query)?,
            },
            action: AuditAction::parse(&row.try_get::<String, _>(4).map_err(PersistenceError::Query)?)
                .ok_or(PersistenceError::CorruptedData)?,
            resource_type: AuditResourceType::parse(&row.try_get::<String, _>(5).map_err(PersistenceError::Query)?)
                .ok_or(PersistenceError::CorruptedData)?,
            resource_name: row.try_get(6).map_err(PersistenceError::Query)?,
            environment_id: row
                .try_get::<Option<String>, _>(7)
                .map_err(PersistenceError::Query)?
                .map(EnvironmentId),
            outcome: AuditOutcome::parse(&row.try_get::<String, _>(8).map_err(PersistenceError::Query)?)
                .ok_or(PersistenceError::CorruptedData)?,
            detail: detail
                .map(|value| serde_json::from_str::<Value>(&value).map_err(|_| PersistenceError::CorruptedData))
                .transpose()?,
            created_at_ms: row.try_get(10).map_err(PersistenceError::Query)?,
        });
    }
    data.refresh_counts()?;
    Ok(data)
}

const MYSQL_MAINTENANCE_LOCK: &str = "rocketmq_dashboard_schema_migration";
const POSTGRES_MAINTENANCE_LOCK: i64 = 7_246_920_002;

async fn restore_sqlite(pool: &SqlitePool, data: &BackupData) -> Result<(), PersistenceError> {
    let mut connection = pool.acquire().await.map_err(PersistenceError::Query)?;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *connection)
        .await
        .map_err(PersistenceError::Query)?;
    let result = restore_rows_sqlite(&mut connection, data).await;
    finish_write_transaction!(&mut connection, result)
}

async fn restore_mysql(pool: &MySqlPool, data: &BackupData) -> Result<(), PersistenceError> {
    let mut connection = pool.acquire().await.map_err(PersistenceError::Query)?;
    let acquired: i64 = sqlx::query_scalar("SELECT GET_LOCK(?, 30)")
        .bind(MYSQL_MAINTENANCE_LOCK)
        .fetch_one(&mut *connection)
        .await
        .map_err(PersistenceError::Query)?;
    if acquired != 1 {
        return Err(PersistenceError::Timeout);
    }
    let begin = sqlx::query("START TRANSACTION")
        .execute(&mut *connection)
        .await
        .map_err(PersistenceError::Query);
    let result = match begin {
        Ok(_) => {
            let rows = restore_rows_mysql(&mut connection, data).await;
            finish_write_transaction!(&mut connection, rows)
        }
        Err(error) => Err(error),
    };
    let _ = sqlx::query_scalar::<_, i64>("SELECT RELEASE_LOCK(?)")
        .bind(MYSQL_MAINTENANCE_LOCK)
        .fetch_one(&mut *connection)
        .await;
    result
}

async fn restore_postgres(pool: &PgPool, data: &BackupData) -> Result<(), PersistenceError> {
    let mut connection = pool.acquire().await.map_err(PersistenceError::Query)?;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(POSTGRES_MAINTENANCE_LOCK)
        .execute(&mut *connection)
        .await
        .map_err(PersistenceError::Query)?;
    let begin = sqlx::query("BEGIN")
        .execute(&mut *connection)
        .await
        .map_err(PersistenceError::Query);
    let result = match begin {
        Ok(_) => {
            let rows = restore_rows_postgres(&mut connection, data).await;
            finish_write_transaction!(&mut connection, rows)
        }
        Err(error) => Err(error),
    };
    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(POSTGRES_MAINTENANCE_LOCK)
        .execute(&mut *connection)
        .await;
    result
}

macro_rules! restore_rows {
    ($connection:expr, $data:expr) => {{
        let existing: i64 = sqlx::query_scalar(
            "SELECT (SELECT COUNT(*) FROM dashboard_environment) + (SELECT COUNT(*) FROM dashboard_endpoint) + (SELECT COUNT(*) FROM consumer_monitor_rule) + (SELECT COUNT(*) FROM dashboard_history_sample) + (SELECT COUNT(*) FROM dashboard_session) + (SELECT COUNT(*) FROM dashboard_audit_event) + (SELECT COUNT(*) FROM dashboard_task_lease)",
        )
        .fetch_one(&mut *$connection)
        .await
        .map_err(PersistenceError::Query)?;
        if existing != 0 {
            return Err(PersistenceError::Conflict);
        }
        for environment in &$data.environments {
            let mut statement = sqlx::QueryBuilder::new(
                "INSERT INTO dashboard_environment (environment_id, name, use_vip_channel, use_tls, revision, created_at_ms, updated_at_ms) VALUES (",
            );
            {
                let mut values = statement.separated(", ");
                values.push_bind(&environment.environment_id.0);
                values.push_bind(&environment.name);
                values.push_bind(environment.use_vip_channel);
                values.push_bind(environment.use_tls);
                values.push_bind(i64::try_from(environment.revision.0).map_err(|_| PersistenceError::Capacity)?);
                values.push_bind(environment.created_at_ms);
                values.push_bind(environment.updated_at_ms);
            }
            statement.push(")");
            statement.build().execute(&mut *$connection).await.map_err(PersistenceError::Query)?;
        }
        for environment in &$data.environments {
            for endpoint in &environment.endpoints {
                let mut statement = sqlx::QueryBuilder::new(
                    "INSERT INTO dashboard_endpoint (endpoint_id, environment_id, endpoint_type, address, role, is_enabled, is_active, sort_order, created_at_ms, updated_at_ms) VALUES (",
                );
                {
                    let mut values = statement.separated(", ");
                    values.push_bind(&endpoint.endpoint_id.0);
                    values.push_bind(&environment.environment_id.0);
                    values.push_bind(endpoint.endpoint_type.as_str());
                    values.push_bind(&endpoint.address);
                    values.push_bind(endpoint.role.as_str());
                    values.push_bind(endpoint.is_enabled);
                    values.push_bind(endpoint.is_active);
                    values.push_bind(endpoint.sort_order);
                    values.push_bind(endpoint.created_at_ms);
                    values.push_bind(endpoint.updated_at_ms);
                }
                statement.push(")");
                statement.build().execute(&mut *$connection).await.map_err(PersistenceError::Query)?;
            }
        }
        for monitor in &$data.monitors {
            let mut statement = sqlx::QueryBuilder::new(
                "INSERT INTO consumer_monitor_rule (environment_id, consumer_group, min_count, max_diff_total, revision, created_at_ms, updated_at_ms) VALUES (",
            );
            {
                let mut values = statement.separated(", ");
                values.push_bind(&monitor.environment_id.0);
                values.push_bind(&monitor.consumer_group);
                values.push_bind(monitor.min_count);
                values.push_bind(monitor.max_diff_total);
                values.push_bind(i64::try_from(monitor.revision.0).map_err(|_| PersistenceError::Capacity)?);
                values.push_bind(monitor.created_at_ms);
                values.push_bind(monitor.updated_at_ms);
            }
            statement.push(")");
            statement.build().execute(&mut *$connection).await.map_err(PersistenceError::Query)?;
        }
        for history in &$data.history {
            let mut statement = sqlx::QueryBuilder::new(
                "INSERT INTO dashboard_history_sample (environment_id, metric_name, bucket_ms, dimensions_json, value) VALUES (",
            );
            {
                let mut values = statement.separated(", ");
                values.push_bind(&history.environment_id.0);
                values.push_bind(&history.metric);
                values.push_bind(history.bucket_ms);
                values.push_bind(history.dimensions_json().map_err(PersistenceError::InvalidConfig)?);
                values.push_bind(history.value);
            }
            statement.push(")");
            statement.build().execute(&mut *$connection).await.map_err(PersistenceError::Query)?;
        }
        for session in &$data.sessions {
            let token_hash = parse_token_hash(&session.token_hash)?;
            let mut statement = sqlx::QueryBuilder::new(
                "INSERT INTO dashboard_session (session_id, token_hash, username, created_at_ms, expires_at_ms, last_seen_at_ms, revoked_at_ms) VALUES (",
            );
            {
                let mut values = statement.separated(", ");
                values.push_bind(&session.session_id);
                values.push_bind(token_hash.bytes().to_vec());
                values.push_bind(&session.username);
                values.push_bind(session.created_at_ms);
                values.push_bind(session.expires_at_ms);
                values.push_bind(session.last_seen_at_ms);
                values.push_bind(session.revoked_at_ms);
            }
            statement.push(")");
            statement.build().execute(&mut *$connection).await.map_err(PersistenceError::Query)?;
        }
        for audit in &$data.audit {
            let detail = audit.detail.as_ref().map(serde_json::to_string).transpose().map_err(PersistenceError::Serialization)?;
            let mut statement = sqlx::QueryBuilder::new(
                "INSERT INTO dashboard_audit_event (event_id, request_id, actor_kind, actor_username, action, resource_type, resource_name, environment_id, outcome, detail_json, created_at_ms) VALUES (",
            );
            {
                let mut values = statement.separated(", ");
                values.push_bind(&audit.event_id);
                values.push_bind(&audit.request_id);
                values.push_bind(audit.actor.kind.code());
                values.push_bind(audit.actor.username.as_deref());
                values.push_bind(audit.action.code());
                values.push_bind(audit.resource_type.code());
                values.push_bind(audit.resource_name.as_deref());
                values.push_bind(audit.environment_id.as_ref().map(|value| value.0.as_str()));
                values.push_bind(audit.outcome.code());
                values.push_bind(detail);
                values.push_bind(audit.created_at_ms);
            }
            statement.push(")");
            statement.build().execute(&mut *$connection).await.map_err(PersistenceError::Query)?;
        }
        let restored: i64 = sqlx::query_scalar(
            "SELECT (SELECT COUNT(*) FROM dashboard_environment) + (SELECT COUNT(*) FROM dashboard_endpoint) + (SELECT COUNT(*) FROM consumer_monitor_rule) + (SELECT COUNT(*) FROM dashboard_history_sample) + (SELECT COUNT(*) FROM dashboard_session) + (SELECT COUNT(*) FROM dashboard_audit_event)",
        )
        .fetch_one(&mut *$connection)
        .await
        .map_err(PersistenceError::Query)?;
        let expected = $data.manifest.counts.environments
            + $data.manifest.counts.endpoints
            + $data.manifest.counts.monitors
            + $data.manifest.counts.history
            + $data.manifest.counts.sessions
            + $data.manifest.counts.audit;
        if u64::try_from(restored).ok() != Some(expected) {
            return Err(PersistenceError::CorruptedData);
        }
        Ok(())
    }};
}

async fn restore_rows_sqlite(connection: &mut SqliteConnection, data: &BackupData) -> Result<(), PersistenceError> {
    restore_rows!(connection, data)
}

async fn restore_rows_mysql(connection: &mut MySqlConnection, data: &BackupData) -> Result<(), PersistenceError> {
    restore_rows!(connection, data)
}

async fn restore_rows_postgres(connection: &mut PgConnection, data: &BackupData) -> Result<(), PersistenceError> {
    restore_rows!(connection, data)
}
