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
use crate::persistence::error::PersistenceError;
use crate::persistence::lease_repository::HistoryLease;
use sqlx::QueryBuilder;
use sqlx::Row;

const SQLITE_CLOCK_MS: &str = "CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)";
const MYSQL_CLOCK_MS: &str = "TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP(3)) DIV 1000";
const POSTGRES_CLOCK_MS: &str = "FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT";

impl SqlPersistence {
    pub(crate) async fn acquire_history_lease(
        &self,
        environment_id: &EnvironmentId,
        holder_id: &str,
        ttl_ms: i64,
    ) -> Result<Option<HistoryLease>, PersistenceError> {
        match &self.pool {
            DatabasePool::Sqlite(pool) => acquire_sqlite(pool, environment_id, holder_id, ttl_ms).await,
            DatabasePool::MySql(pool) => acquire_mysql(pool, environment_id, holder_id, ttl_ms).await,
            DatabasePool::Postgres(pool) => acquire_postgres(pool, environment_id, holder_id, ttl_ms).await,
        }
    }

    pub(crate) async fn renew_history_lease(
        &self,
        lease: &HistoryLease,
        ttl_ms: i64,
    ) -> Result<Option<HistoryLease>, PersistenceError> {
        match &self.pool {
            DatabasePool::Sqlite(pool) => renew_sqlite(pool, lease, ttl_ms).await,
            DatabasePool::MySql(pool) => renew_mysql(pool, lease, ttl_ms).await,
            DatabasePool::Postgres(pool) => renew_postgres(pool, lease, ttl_ms).await,
        }
    }

    pub(crate) async fn release_history_lease(&self, lease: &HistoryLease) -> Result<bool, PersistenceError> {
        match &self.pool {
            DatabasePool::Sqlite(pool) => release_sqlite(pool, lease).await,
            DatabasePool::MySql(pool) => release_mysql(pool, lease).await,
            DatabasePool::Postgres(pool) => release_postgres(pool, lease).await,
        }
    }
}

macro_rules! acquire_lease {
    ($executor:expr, $clock:expr, $environment:expr, $holder:expr, $ttl:expr) => {
        acquire_lease!(@inner $executor, $clock, $environment, $holder, $ttl, "", "")
    };
    ($executor:expr, $clock:expr, $environment:expr, $holder:expr, $ttl:expr, $lock:literal) => {
        acquire_lease!(@inner $executor, $clock, $environment, $holder, $ttl, $lock, "")
    };
    ($executor:expr, $clock:expr, $environment:expr, $holder:expr, $ttl:expr, $lock:literal, $insert_suffix:literal) => {
        acquire_lease!(@inner $executor, $clock, $environment, $holder, $ttl, $lock, $insert_suffix)
    };
    (@inner $executor:expr, $clock:expr, $environment:expr, $holder:expr, $ttl:expr, $lock:expr, $insert_suffix:expr) => {{
        let name = HistoryLease::name_for($environment);
        let clock_query = format!("SELECT {}", $clock);
        let now: i64 = sqlx::query_scalar(&clock_query)
            .fetch_one($executor)
            .await
            .map_err(map_query_error)?;
        let expires_at_ms = now.checked_add($ttl).ok_or(PersistenceError::Conflict)?;
        let mut current = QueryBuilder::new(
            "SELECT expires_at_ms, fencing_token FROM dashboard_task_lease WHERE lease_name = ",
        );
        current.push_bind(&name);
        current.push($lock);
        let current = current.build().fetch_optional($executor).await.map_err(map_query_error)?;
        if let Some(row) = current {
            let expires: i64 = row.try_get("expires_at_ms").map_err(map_query_error)?;
            let old_token: i64 = row.try_get("fencing_token").map_err(map_query_error)?;
            if expires > now {
                Ok(None)
            } else {
                let token = old_token.checked_add(1).ok_or(PersistenceError::Conflict)?;
                let mut update = QueryBuilder::new("UPDATE dashboard_task_lease SET holder_id = ");
                update.push_bind($holder);
                update.push(", expires_at_ms = ");
                update.push_bind(expires_at_ms);
                update.push(", fencing_token = ");
                update.push_bind(token);
                update.push(" WHERE lease_name = ");
                update.push_bind(&name);
                update.push(" AND fencing_token = ");
                update.push_bind(old_token);
                update.push(" AND expires_at_ms <= ");
                update.push($clock);
                let acquired = update.build().execute($executor).await.map_err(map_query_error)?.rows_affected() == 1;
                Ok(acquired.then(|| HistoryLease::new(
                    $environment.clone(),
                    $holder.to_string(),
                    token,
                    expires_at_ms,
                )))
            }
        } else {
            let mut insert = QueryBuilder::new(
                "INSERT INTO dashboard_task_lease (lease_name, holder_id, expires_at_ms, fencing_token) VALUES (",
            );
            insert.push_bind(&name);
            insert.push(", ");
            insert.push_bind($holder);
            insert.push(", ");
            insert.push_bind(expires_at_ms);
            insert.push(", 1)");
            insert.push($insert_suffix);
            let acquired = insert.build().execute($executor).await.map_err(map_query_error)?.rows_affected() == 1;
            Ok(acquired.then(|| HistoryLease::new(
                $environment.clone(),
                $holder.to_string(),
                1,
                expires_at_ms,
            )))
        }
    }};
}

macro_rules! renew_or_release {
    ($executor:expr, $clock:expr, $lease:expr, $ttl:expr, $release:expr) => {{
        let mut update = QueryBuilder::new("UPDATE dashboard_task_lease SET expires_at_ms = ");
        update.push($clock);
        if !$release {
            update.push(" + ");
            update.push_bind($ttl);
        }
        update.push(" WHERE lease_name = ");
        update.push_bind(&$lease.name);
        update.push(" AND holder_id = ");
        update.push_bind(&$lease.holder_id);
        update.push(" AND fencing_token = ");
        update.push_bind($lease.fencing_token);
        update.push(" AND expires_at_ms > ");
        update.push($clock);
        update
            .build()
            .execute($executor)
            .await
            .map_err(map_query_error)?
            .rows_affected()
    }};
}

async fn acquire_sqlite(
    pool: &sqlx::SqlitePool,
    environment_id: &EnvironmentId,
    holder: &str,
    ttl: i64,
) -> Result<Option<HistoryLease>, PersistenceError> {
    let mut connection = pool.acquire().await.map_err(map_query_error)?;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *connection)
        .await
        .map_err(map_query_error)?;
    let result = acquire_lease!(&mut *connection, SQLITE_CLOCK_MS, environment_id, holder, ttl);
    finish_sqlite(&mut connection, result).await
}

async fn acquire_mysql(
    pool: &sqlx::MySqlPool,
    environment_id: &EnvironmentId,
    holder: &str,
    ttl: i64,
) -> Result<Option<HistoryLease>, PersistenceError> {
    for _ in 0..3 {
        match acquire_mysql_once(pool, environment_id, holder, ttl).await {
            Ok(lease) => return Ok(lease),
            Err(error) if mysql_lease_retryable(&error) => tokio::task::yield_now().await,
            Err(error) => return Err(error),
        }
    }
    // A contended standby acquire is an ordinary condition, not a storage
    // outage. The next renewal tick will retry without exposing a stale error.
    Ok(None)
}

async fn acquire_mysql_once(
    pool: &sqlx::MySqlPool,
    environment_id: &EnvironmentId,
    holder: &str,
    ttl: i64,
) -> Result<Option<HistoryLease>, PersistenceError> {
    let name = HistoryLease::name_for(environment_id);
    let clock_query = format!("SELECT {MYSQL_CLOCK_MS}");
    let now: i64 = sqlx::query_scalar(&clock_query)
        .fetch_one(pool)
        .await
        .map_err(map_query_error)?;
    let expires_at_ms = now.checked_add(ttl).ok_or(PersistenceError::Conflict)?;
    // INSERT IGNORE is used only to create the missing lease row. It avoids a
    // preceding SELECT ... FOR UPDATE gap lock; all update paths below remain
    // exact holder/token/expiry checks and never silence other write errors.
    let inserted = sqlx::query(
        "INSERT IGNORE INTO dashboard_task_lease \
         (lease_name, holder_id, expires_at_ms, fencing_token) VALUES (?, ?, ?, 1)",
    )
    .bind(&name)
    .bind(holder)
    .bind(expires_at_ms)
    .execute(pool)
    .await
    .map_err(map_query_error)?
    .rows_affected();
    if inserted == 1 {
        return Ok(Some(HistoryLease::new(
            environment_id.clone(),
            holder.to_string(),
            1,
            expires_at_ms,
        )));
    }

    let mut transaction = pool.begin().await.map_err(map_query_error)?;
    let result = async {
        let now: i64 = sqlx::query_scalar(&clock_query)
            .fetch_one(&mut *transaction)
            .await
            .map_err(map_query_error)?;
        let expires_at_ms = now.checked_add(ttl).ok_or(PersistenceError::Conflict)?;
        let row = sqlx::query(
            "SELECT expires_at_ms, fencing_token FROM dashboard_task_lease \
             WHERE lease_name = ? FOR UPDATE",
        )
        .bind(&name)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_query_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let expires: i64 = row.try_get("expires_at_ms").map_err(map_query_error)?;
        let old_token: i64 = row.try_get("fencing_token").map_err(map_query_error)?;
        if expires > now {
            return Ok(None);
        }
        let token = old_token.checked_add(1).ok_or(PersistenceError::Conflict)?;
        let updated = sqlx::query(
            "UPDATE dashboard_task_lease SET holder_id = ?, expires_at_ms = ?, fencing_token = ? \
             WHERE lease_name = ? AND fencing_token = ? AND expires_at_ms <= \
             TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP(3)) DIV 1000",
        )
        .bind(holder)
        .bind(expires_at_ms)
        .bind(token)
        .bind(&name)
        .bind(old_token)
        .execute(&mut *transaction)
        .await
        .map_err(map_query_error)?
        .rows_affected();
        Ok((updated == 1).then(|| HistoryLease::new(environment_id.clone(), holder.to_string(), token, expires_at_ms)))
    }
    .await;
    finish_transaction(transaction, result).await
}

fn mysql_lease_retryable(error: &PersistenceError) -> bool {
    let PersistenceError::Query(sqlx::Error::Database(database)) = error else {
        return false;
    };
    matches!(database.code().as_deref(), Some("1205" | "1213" | "1062"))
}

async fn acquire_postgres(
    pool: &sqlx::PgPool,
    environment_id: &EnvironmentId,
    holder: &str,
    ttl: i64,
) -> Result<Option<HistoryLease>, PersistenceError> {
    let mut transaction = pool.begin().await.map_err(map_query_error)?;
    let result = acquire_lease!(
        &mut *transaction,
        POSTGRES_CLOCK_MS,
        environment_id,
        holder,
        ttl,
        " FOR UPDATE",
        " ON CONFLICT (lease_name) DO NOTHING"
    );
    finish_transaction(transaction, result).await
}

async fn renew_sqlite(
    pool: &sqlx::SqlitePool,
    lease: &HistoryLease,
    ttl: i64,
) -> Result<Option<HistoryLease>, PersistenceError> {
    let mut connection = pool.acquire().await.map_err(map_query_error)?;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *connection)
        .await
        .map_err(map_query_error)?;
    let result = async {
        if renew_or_release!(&mut *connection, SQLITE_CLOCK_MS, lease, ttl, false) != 1 {
            return Ok(None);
        }
        let clock_query = format!("SELECT {SQLITE_CLOCK_MS}");
        let now: i64 = sqlx::query_scalar(&clock_query)
            .fetch_one(&mut *connection)
            .await
            .map_err(map_query_error)?;
        Ok(Some(HistoryLease {
            expires_at_ms: now.checked_add(ttl).ok_or(PersistenceError::Conflict)?,
            ..lease.clone()
        }))
    }
    .await;
    finish_sqlite(&mut connection, result).await
}

async fn renew_mysql(
    pool: &sqlx::MySqlPool,
    lease: &HistoryLease,
    ttl: i64,
) -> Result<Option<HistoryLease>, PersistenceError> {
    let mut transaction = pool.begin().await.map_err(map_query_error)?;
    let result = async {
        if renew_or_release!(&mut *transaction, MYSQL_CLOCK_MS, lease, ttl, false) != 1 {
            return Ok(None);
        }
        let clock_query = format!("SELECT {MYSQL_CLOCK_MS}");
        let now: i64 = sqlx::query_scalar(&clock_query)
            .fetch_one(&mut *transaction)
            .await
            .map_err(map_query_error)?;
        Ok(Some(HistoryLease {
            expires_at_ms: now.checked_add(ttl).ok_or(PersistenceError::Conflict)?,
            ..lease.clone()
        }))
    }
    .await;
    finish_transaction(transaction, result).await
}

async fn renew_postgres(
    pool: &sqlx::PgPool,
    lease: &HistoryLease,
    ttl: i64,
) -> Result<Option<HistoryLease>, PersistenceError> {
    let mut transaction = pool.begin().await.map_err(map_query_error)?;
    let result = async {
        if renew_or_release!(&mut *transaction, POSTGRES_CLOCK_MS, lease, ttl, false) != 1 {
            return Ok(None);
        }
        let clock_query = format!("SELECT {POSTGRES_CLOCK_MS}");
        let now: i64 = sqlx::query_scalar(&clock_query)
            .fetch_one(&mut *transaction)
            .await
            .map_err(map_query_error)?;
        Ok(Some(HistoryLease {
            expires_at_ms: now.checked_add(ttl).ok_or(PersistenceError::Conflict)?,
            ..lease.clone()
        }))
    }
    .await;
    finish_transaction(transaction, result).await
}

async fn release_sqlite(pool: &sqlx::SqlitePool, lease: &HistoryLease) -> Result<bool, PersistenceError> {
    let mut connection = pool.acquire().await.map_err(map_query_error)?;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *connection)
        .await
        .map_err(map_query_error)?;
    let result = async { Ok(renew_or_release!(&mut *connection, SQLITE_CLOCK_MS, lease, 0_i64, true) == 1) }.await;
    finish_sqlite(&mut connection, result).await
}

async fn release_mysql(pool: &sqlx::MySqlPool, lease: &HistoryLease) -> Result<bool, PersistenceError> {
    let mut transaction = pool.begin().await.map_err(map_query_error)?;
    let result = async { Ok(renew_or_release!(&mut *transaction, MYSQL_CLOCK_MS, lease, 0_i64, true) == 1) }.await;
    finish_transaction(transaction, result).await
}

async fn release_postgres(pool: &sqlx::PgPool, lease: &HistoryLease) -> Result<bool, PersistenceError> {
    let mut transaction = pool.begin().await.map_err(map_query_error)?;
    let result = async { Ok(renew_or_release!(&mut *transaction, POSTGRES_CLOCK_MS, lease, 0_i64, true) == 1) }.await;
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
