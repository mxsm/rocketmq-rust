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
use super::SqlPersistence;
use super::map_query_error;
use crate::model::EnvironmentId;
use crate::model::MetricSample;
use crate::persistence::error::PersistenceError;
use crate::persistence::history_repository::HistoryPage;
use crate::persistence::history_repository::HistoryQuery;
use crate::persistence::history_repository::HistoryRetentionResult;
use crate::persistence::history_repository::is_valid_history_timestamp_ms;
use crate::persistence::history_repository::page_samples;
use crate::persistence::lease_repository::HistoryLease;
use sqlx::QueryBuilder;
use sqlx::Row;
use std::collections::BTreeMap;

const SQLITE_CLOCK_MS: &str = "CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)";
const MYSQL_CLOCK_MS: &str = "TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP(3)) DIV 1000";
const POSTGRES_CLOCK_MS: &str = "FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT";

impl SqlPersistence {
    pub(crate) async fn append_history(
        &self,
        samples: Vec<MetricSample>,
        lease: Option<&HistoryLease>,
    ) -> Result<(), PersistenceError> {
        let lease = lease.ok_or(PersistenceError::Conflict)?;
        let environment_id = samples
            .first()
            .map(|sample| &sample.environment_id)
            .ok_or_else(|| PersistenceError::InvalidConfig("history append batch is empty".to_string()))?;
        if samples.iter().any(|sample| sample.environment_id != *environment_id)
            || lease.environment_id() != environment_id
        {
            return Err(PersistenceError::Conflict);
        }
        match &self.pool {
            DatabasePool::Sqlite(pool) => append_sqlite(pool, &samples, lease).await,
            DatabasePool::MySql(pool) => append_mysql(pool, &samples, lease).await,
            DatabasePool::Postgres(pool) => append_postgres(pool, &samples, lease).await,
        }
    }

    pub(crate) async fn query_history(&self, mut query: HistoryQuery) -> Result<HistoryPage, PersistenceError> {
        query.validate_and_normalize()?;
        let samples = match &self.pool {
            DatabasePool::Sqlite(pool) => query_sqlite(pool, &query).await,
            DatabasePool::MySql(pool) => query_mysql(pool, &query).await,
            DatabasePool::Postgres(pool) => query_postgres(pool, &query).await,
        }?;
        page_samples(&query, samples)
    }

    pub(crate) async fn delete_history_before(
        &self,
        environment_id: &EnvironmentId,
        cutoff_ms: i64,
        batch_size: u32,
        lease: Option<&HistoryLease>,
    ) -> Result<HistoryRetentionResult, PersistenceError> {
        if !is_valid_history_timestamp_ms(cutoff_ms) {
            return Err(PersistenceError::InvalidConfig(
                "history retention request is invalid".to_string(),
            ));
        }
        let lease = lease.ok_or(PersistenceError::Conflict)?;
        if lease.environment_id() != environment_id {
            return Err(PersistenceError::Conflict);
        }
        match &self.pool {
            DatabasePool::Sqlite(pool) => delete_sqlite(pool, environment_id, cutoff_ms, batch_size, lease).await,
            DatabasePool::MySql(pool) => delete_mysql(pool, environment_id, cutoff_ms, batch_size, lease).await,
            DatabasePool::Postgres(pool) => delete_postgres(pool, environment_id, cutoff_ms, batch_size, lease).await,
        }
    }
}

macro_rules! lease_is_current {
    ($executor:expr, $clock:expr, $lease:expr $(, $suffix:literal)?) => {{
        let mut statement = QueryBuilder::new(
            "SELECT fencing_token FROM dashboard_task_lease WHERE lease_name = ",
        );
        statement.push_bind(&$lease.name);
        statement.push(" AND holder_id = ");
        statement.push_bind(&$lease.holder_id);
        statement.push(" AND fencing_token = ");
        statement.push_bind($lease.fencing_token);
        statement.push(" AND expires_at_ms > ");
        statement.push($clock);
        $(statement.push($suffix);)?
        if statement
            .build()
            .fetch_optional($executor)
            .await
            .map_err(map_query_error)?
            .is_none()
        {
            return Err(PersistenceError::Conflict);
        }
    }};
}

macro_rules! append_rows {
    ($executor:expr, $samples:expr, $decode:expr) => {{
        let mut existing_query = QueryBuilder::new(
            "SELECT environment_id, metric_name, bucket_ms, dimensions_json, value \
             FROM dashboard_history_sample WHERE ",
        );
        let mut first = true;
        for sample in $samples {
            let dimensions = sample.dimensions_json().map_err(PersistenceError::InvalidConfig)?;
            if !first {
                existing_query.push(" OR ");
            }
            first = false;
            existing_query.push("(environment_id = ");
            existing_query.push_bind(sample.environment_id.0.clone());
            existing_query.push(" AND metric_name = ");
            existing_query.push_bind(sample.metric.clone());
            existing_query.push(" AND bucket_ms = ");
            existing_query.push_bind(sample.bucket_ms);
            existing_query.push(" AND dimensions_json = ");
            existing_query.push_bind(dimensions);
            existing_query.push(")");
        }
        let existing = existing_query
            .build()
            .fetch_all($executor)
            .await
            .map_err(map_query_error)?
            .into_iter()
            .map($decode)
            .collect::<Result<Vec<_>, PersistenceError>>()?
            .into_iter()
            .map(|sample| {
                let key = (
                    sample.environment_id.0.clone(),
                    sample.metric.clone(),
                    sample.bucket_ms,
                    sample.dimensions_json().map_err(PersistenceError::InvalidConfig)?,
                );
                Ok((key, sample.value))
            })
            .collect::<Result<BTreeMap<_, _>, PersistenceError>>()?;
        let mut missing = Vec::new();
        for sample in $samples {
            let dimensions = sample.dimensions_json().map_err(PersistenceError::InvalidConfig)?;
            let key = (
                sample.environment_id.0.clone(),
                sample.metric.clone(),
                sample.bucket_ms,
                dimensions,
            );
            if let Some(value) = existing.get(&key) {
                if value.to_bits() != sample.value.to_bits() {
                    return Err(PersistenceError::Conflict);
                }
            }
            if !existing.contains_key(&key) {
                missing.push(sample);
            }
        }
        if !missing.is_empty() {
            let mut insert = QueryBuilder::new(
                "INSERT INTO dashboard_history_sample (environment_id, metric_name, bucket_ms, dimensions_json, value) VALUES ",
            );
            let mut first = true;
            for sample in missing {
                let dimensions = sample.dimensions_json().map_err(PersistenceError::InvalidConfig)?;
                if !first {
                    insert.push(", ");
                }
                first = false;
                insert.push("(");
                insert.push_bind(sample.environment_id.0.clone());
                insert.push(", ");
                insert.push_bind(sample.metric.clone());
                insert.push(", ");
                insert.push_bind(sample.bucket_ms);
                insert.push(", ");
                insert.push_bind(dimensions);
                insert.push(", ");
                insert.push_bind(sample.value);
                insert.push(")");
            }
            insert.build().execute($executor).await.map_err(map_query_error)?;
        }
    }};
}

async fn append_sqlite(
    pool: &sqlx::SqlitePool,
    samples: &[MetricSample],
    lease: &HistoryLease,
) -> Result<(), PersistenceError> {
    let mut connection = pool.acquire().await.map_err(map_query_error)?;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *connection)
        .await
        .map_err(map_query_error)?;
    let result = async {
        lease_is_current!(&mut *connection, SQLITE_CLOCK_MS, lease);
        append_rows!(&mut *connection, samples, history_sample_from_sqlite);
        Ok(())
    }
    .await;
    finish_sqlite(&mut connection, result).await
}

async fn append_mysql(
    pool: &sqlx::MySqlPool,
    samples: &[MetricSample],
    lease: &HistoryLease,
) -> Result<(), PersistenceError> {
    let mut transaction = pool.begin().await.map_err(map_query_error)?;
    let result = async {
        lease_is_current!(&mut *transaction, MYSQL_CLOCK_MS, lease, " FOR UPDATE");
        append_rows!(&mut *transaction, samples, history_sample_from_mysql);
        Ok(())
    }
    .await;
    finish_transaction(transaction, result).await
}

async fn append_postgres(
    pool: &sqlx::PgPool,
    samples: &[MetricSample],
    lease: &HistoryLease,
) -> Result<(), PersistenceError> {
    let mut transaction = pool.begin().await.map_err(map_query_error)?;
    let result = async {
        lease_is_current!(&mut *transaction, POSTGRES_CLOCK_MS, lease, " FOR UPDATE");
        append_rows!(&mut *transaction, samples, history_sample_from_postgres);
        Ok(())
    }
    .await;
    finish_transaction(transaction, result).await
}

macro_rules! query_rows {
    ($executor:expr, $query:expr, $decode:expr) => {{
        let dimensions = $query.dimensions_json()?;
        let cursor = $query.cursor_key()?;
        let mut statement = QueryBuilder::new(
            "SELECT environment_id, metric_name, bucket_ms, dimensions_json, value \
             FROM dashboard_history_sample WHERE environment_id = ",
        );
        statement.push_bind(&$query.environment_id.0);
        statement.push(" AND metric_name = ");
        statement.push_bind(&$query.metric);
        statement.push(" AND dimensions_json = ");
        statement.push_bind(&dimensions);
        statement.push(" AND bucket_ms >= ");
        statement.push_bind($query.range.start_ms);
        statement.push(" AND bucket_ms <= ");
        statement.push_bind($query.range.end_ms);
        if let Some((bucket_ms, cursor_dimensions)) = cursor {
            statement.push(" AND (bucket_ms > ");
            statement.push_bind(bucket_ms);
            statement.push(" OR (bucket_ms = ");
            statement.push_bind(bucket_ms);
            statement.push(" AND dimensions_json > ");
            statement.push_bind(cursor_dimensions);
            statement.push("))");
        }
        statement.push(" ORDER BY bucket_ms ASC, dimensions_json ASC LIMIT ");
        statement.push_bind(i64::from($query.limit) + 1);
        statement
            .build()
            .fetch_all($executor)
            .await
            .map_err(map_query_error)?
            .into_iter()
            .map($decode)
            .collect::<Result<Vec<_>, PersistenceError>>()
    }};
}

fn history_sample_from_sqlite(row: sqlx::sqlite::SqliteRow) -> Result<MetricSample, PersistenceError> {
    history_sample_from_text_fields(
        row.try_get("environment_id").map_err(map_query_error)?,
        row.try_get("metric_name").map_err(map_query_error)?,
        row.try_get("bucket_ms").map_err(map_query_error)?,
        row.try_get("dimensions_json").map_err(map_query_error)?,
        row.try_get("value").map_err(map_query_error)?,
    )
}

fn history_sample_from_mysql(row: sqlx::mysql::MySqlRow) -> Result<MetricSample, PersistenceError> {
    history_sample_from_binary_fields(
        row.try_get("environment_id").map_err(map_query_error)?,
        row.try_get("metric_name").map_err(map_query_error)?,
        row.try_get("bucket_ms").map_err(map_query_error)?,
        row.try_get("dimensions_json").map_err(map_query_error)?,
        row.try_get("value").map_err(map_query_error)?,
    )
}

fn history_sample_from_postgres(row: sqlx::postgres::PgRow) -> Result<MetricSample, PersistenceError> {
    history_sample_from_text_fields(
        row.try_get("environment_id").map_err(map_query_error)?,
        row.try_get("metric_name").map_err(map_query_error)?,
        row.try_get("bucket_ms").map_err(map_query_error)?,
        row.try_get("dimensions_json").map_err(map_query_error)?,
        row.try_get("value").map_err(map_query_error)?,
    )
}

fn history_sample_from_binary_fields(
    environment_id: Vec<u8>,
    metric: Vec<u8>,
    bucket_ms: i64,
    dimensions_json: Vec<u8>,
    value: f64,
) -> Result<MetricSample, PersistenceError> {
    history_sample_from_text_fields(
        String::from_utf8(environment_id).map_err(|_| PersistenceError::CorruptedData)?,
        String::from_utf8(metric).map_err(|_| PersistenceError::CorruptedData)?,
        bucket_ms,
        String::from_utf8(dimensions_json).map_err(|_| PersistenceError::CorruptedData)?,
        value,
    )
}

fn history_sample_from_text_fields(
    environment_id: String,
    metric: String,
    bucket_ms: i64,
    dimensions_json: String,
    value: f64,
) -> Result<MetricSample, PersistenceError> {
    let mut sample = MetricSample {
        environment_id: EnvironmentId(environment_id),
        metric,
        bucket_ms,
        dimensions: serde_json::from_str(&dimensions_json).map_err(|_| PersistenceError::CorruptedData)?,
        value,
    };
    sample.normalize().map_err(|_| PersistenceError::CorruptedData)?;
    Ok(sample)
}

macro_rules! history_has_more {
    ($executor:expr, $environment_id:expr, $cutoff:expr) => {{
        let mut select = QueryBuilder::new("SELECT 1 FROM dashboard_history_sample WHERE environment_id = ");
        select.push_bind(&$environment_id.0);
        select.push(" AND bucket_ms < ");
        select.push_bind($cutoff);
        select.push(" LIMIT 1");
        select
            .build()
            .fetch_optional($executor)
            .await
            .map_err(map_query_error)?
            .is_some()
    }};
}

async fn query_sqlite(pool: &sqlx::SqlitePool, query: &HistoryQuery) -> Result<Vec<MetricSample>, PersistenceError> {
    query_rows!(pool, query, history_sample_from_sqlite)
}

async fn query_mysql(pool: &sqlx::MySqlPool, query: &HistoryQuery) -> Result<Vec<MetricSample>, PersistenceError> {
    query_rows!(pool, query, history_sample_from_mysql)
}

async fn query_postgres(pool: &sqlx::PgPool, query: &HistoryQuery) -> Result<Vec<MetricSample>, PersistenceError> {
    query_rows!(pool, query, history_sample_from_postgres)
}

async fn delete_sqlite(
    pool: &sqlx::SqlitePool,
    environment_id: &EnvironmentId,
    cutoff: i64,
    batch: u32,
    lease: &HistoryLease,
) -> Result<HistoryRetentionResult, PersistenceError> {
    let mut connection = pool.acquire().await.map_err(map_query_error)?;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *connection)
        .await
        .map_err(map_query_error)?;
    let result = async {
        lease_is_current!(&mut *connection, SQLITE_CLOCK_MS, lease);
        let deleted = sqlx::query(
            "DELETE FROM dashboard_history_sample WHERE rowid IN (\
             SELECT rowid FROM dashboard_history_sample WHERE environment_id = ? AND bucket_ms < ? \
             ORDER BY bucket_ms, environment_id, metric_name, dimensions_json LIMIT ?)",
        )
        .bind(&environment_id.0)
        .bind(cutoff)
        .bind(i64::from(batch))
        .execute(&mut *connection)
        .await
        .map_err(map_query_error)?
        .rows_affected();
        let has_more = history_has_more!(&mut *connection, environment_id, cutoff);
        Ok(HistoryRetentionResult { deleted, has_more })
    }
    .await;
    finish_sqlite(&mut connection, result).await
}

async fn delete_mysql(
    pool: &sqlx::MySqlPool,
    environment_id: &EnvironmentId,
    cutoff: i64,
    batch: u32,
    lease: &HistoryLease,
) -> Result<HistoryRetentionResult, PersistenceError> {
    let mut transaction = pool.begin().await.map_err(map_query_error)?;
    let result = async {
        lease_is_current!(&mut *transaction, MYSQL_CLOCK_MS, lease, " FOR UPDATE");
        let deleted = sqlx::query(
            "DELETE FROM dashboard_history_sample WHERE environment_id = ? AND bucket_ms < ? \
             ORDER BY bucket_ms, environment_id, metric_name, dimensions_json LIMIT ?",
        )
        .bind(&environment_id.0)
        .bind(cutoff)
        .bind(i64::from(batch))
        .execute(&mut *transaction)
        .await
        .map_err(map_query_error)?
        .rows_affected();
        let has_more = history_has_more!(&mut *transaction, environment_id, cutoff);
        Ok(HistoryRetentionResult { deleted, has_more })
    }
    .await;
    finish_transaction(transaction, result).await
}

async fn delete_postgres(
    pool: &sqlx::PgPool,
    environment_id: &EnvironmentId,
    cutoff: i64,
    batch: u32,
    lease: &HistoryLease,
) -> Result<HistoryRetentionResult, PersistenceError> {
    let mut transaction = pool.begin().await.map_err(map_query_error)?;
    let result = async {
        lease_is_current!(&mut *transaction, POSTGRES_CLOCK_MS, lease, " FOR UPDATE");
        let deleted = sqlx::query(
            "WITH candidates AS (\
             SELECT ctid FROM dashboard_history_sample WHERE environment_id = $1 AND bucket_ms < $2 \
             ORDER BY bucket_ms, environment_id, metric_name, dimensions_json LIMIT $3) \
             DELETE FROM dashboard_history_sample WHERE ctid IN (SELECT ctid FROM candidates)",
        )
        .bind(&environment_id.0)
        .bind(cutoff)
        .bind(i64::from(batch))
        .execute(&mut *transaction)
        .await
        .map_err(map_query_error)?
        .rows_affected();
        let has_more = history_has_more!(&mut *transaction, environment_id, cutoff);
        Ok(HistoryRetentionResult { deleted, has_more })
    }
    .await;
    finish_transaction(transaction, result).await
}

async fn finish_sqlite<T>(
    connection: &mut sqlx::SqliteConnection,
    result: Result<T, PersistenceError>,
) -> Result<T, PersistenceError> {
    match result {
        Ok(value) => {
            sqlx::query("COMMIT")
                .execute(&mut *connection)
                .await
                .map_err(map_query_error)?;
            Ok(value)
        }
        Err(error) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut *connection).await;
            Err(error)
        }
    }
}

async fn finish_transaction<DB, T>(
    transaction: sqlx::Transaction<'_, DB>,
    result: Result<T, PersistenceError>,
) -> Result<T, PersistenceError>
where
    DB: sqlx::Database,
{
    match result {
        Ok(value) => {
            transaction.commit().await.map_err(map_query_error)?;
            Ok(value)
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

#[cfg(test)]
#[path = "history_sql_store_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "history_sql_store_contract_tests.rs"]
mod contract_tests;
