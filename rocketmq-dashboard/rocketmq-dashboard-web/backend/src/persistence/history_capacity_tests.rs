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

use super::TimeRange;
use crate::config::SqlPoolConfig;
use crate::config::StorageConfig;
use crate::model::EnvironmentId;
use crate::model::MetricSample;
use crate::model::StorageBackend;
use crate::persistence::file_store::FilePersistence;
use crate::persistence::history_repository::HistoryQuery;
use crate::persistence::history_repository::page_samples;
use crate::persistence::sql_store::SqlPersistence;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ScopeId;
use sqlx::Row;
use std::time::Instant;

const SAMPLE_COUNT: usize = 10_000;
const APPEND_BATCH: usize = 500;
const PAGE_SIZE: u32 = 200;
const DAY_MS: i64 = 86_400_000;

fn json_field_equals(value: &serde_json::Value, field: &str, expected: &str) -> bool {
    match value {
        serde_json::Value::Object(values) => {
            values
                .get(field)
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| value == expected)
                || values.values().any(|value| json_field_equals(value, field, expected))
        }
        serde_json::Value::Array(values) => values.iter().any(|value| json_field_equals(value, field, expected)),
        _ => false,
    }
}

#[test]
#[ignore = "requires docker-compose.storage-test.yml"]
fn docker_file_history_capacity_baseline() {
    let data_path = std::env::var("ROCKETMQ_DASHBOARD_STORAGE_TEST_FILE_PATH")
        .expect("ROCKETMQ_DASHBOARD_STORAGE_TEST_FILE_PATH must be set by the storage test runner");
    let owner = RuntimeOwner::new().expect("runtime owner");
    owner.block_on(async {
        let store = FilePersistence::initialize(
            &StorageConfig {
                backend: StorageBackend::File,
                data_path: data_path.into(),
                database_url: None,
                pool: SqlPoolConfig::default(),
            },
            owner.root_context().component("history-capacity-file"),
        )
        .await
        .expect("file persistence");
        run_file_capacity(&store).await;
    });
    owner.shutdown_runtime_blocking().expect("runtime shutdown");
}

#[test]
#[ignore = "requires docker-compose.storage-test.yml"]
fn docker_sqlite_history_capacity_baseline() {
    let data_path = std::env::var("ROCKETMQ_DASHBOARD_STORAGE_TEST_SQLITE_PATH")
        .expect("ROCKETMQ_DASHBOARD_STORAGE_TEST_SQLITE_PATH must be set by the storage test runner");
    run_sql_capacity(StorageBackend::Sqlite, None, data_path.into());
}

#[test]
#[ignore = "requires docker-compose.storage-test.yml"]
fn docker_mysql_history_capacity_baseline() {
    let database_url = std::env::var("ROCKETMQ_DASHBOARD_STORAGE_TEST_MYSQL_URL")
        .expect("ROCKETMQ_DASHBOARD_STORAGE_TEST_MYSQL_URL must be set by the storage test runner");
    run_sql_capacity(StorageBackend::MySql, Some(database_url), "unused".into());
}

#[test]
#[ignore = "requires docker-compose.storage-test.yml"]
fn docker_postgres_history_capacity_baseline() {
    let database_url = std::env::var("ROCKETMQ_DASHBOARD_STORAGE_TEST_POSTGRES_URL")
        .expect("ROCKETMQ_DASHBOARD_STORAGE_TEST_POSTGRES_URL must be set by the storage test runner");
    run_sql_capacity(StorageBackend::Postgres, Some(database_url), "unused".into());
}

fn query(environment_id: EnvironmentId, cursor: Option<String>) -> HistoryQuery {
    let mut query = HistoryQuery {
        environment_id,
        metric: "capacity".to_string(),
        range: TimeRange {
            start_ms: 0,
            end_ms: DAY_MS * 20,
        },
        dimensions: Vec::new(),
        limit: PAGE_SIZE,
        cursor,
    };
    query.validate_and_normalize().expect("valid capacity query");
    query
}

fn samples(environment_id: &EnvironmentId, batch: usize) -> Vec<MetricSample> {
    let day = i64::try_from(batch).expect("batch day");
    (0..APPEND_BATCH)
        .map(|offset| MetricSample {
            environment_id: environment_id.clone(),
            metric: "capacity".to_string(),
            bucket_ms: day * DAY_MS + i64::try_from(offset).expect("sample offset") * 1_000,
            dimensions: Vec::new(),
            value: (batch * APPEND_BATCH + offset) as f64,
        })
        .collect()
}

async fn run_file_capacity(store: &FilePersistence) {
    let environment_id = EnvironmentId::new();
    let first = samples(&environment_id, 0);
    let started = Instant::now();
    store.append_history(first).await.expect("file append 500");
    let append_500 = started.elapsed();
    for batch in 1..(SAMPLE_COUNT / APPEND_BATCH) {
        store
            .append_history(samples(&environment_id, batch))
            .await
            .expect("file append batch");
    }
    let first_page_started = Instant::now();
    let first_page = page_samples(
        &query(environment_id.clone(), None),
        store
            .read_history(&query(environment_id.clone(), None))
            .await
            .expect("file first page read"),
    )
    .expect("file first page");
    let first_page_elapsed = first_page_started.elapsed();
    assert_eq!(first_page.samples.len(), PAGE_SIZE as usize);
    let continuation_started = Instant::now();
    let continuation = page_samples(
        &query(environment_id.clone(), first_page.next_cursor.clone()),
        store
            .read_history(&query(environment_id.clone(), first_page.next_cursor.clone()))
            .await
            .expect("file continuation read"),
    )
    .expect("file continuation");
    let continuation_elapsed = continuation_started.elapsed();
    assert_eq!(continuation.samples.len(), PAGE_SIZE as usize);
    let retention_started = Instant::now();
    let mut deleted = 0_u64;
    let mut result = store
        .delete_history_before(&environment_id, DAY_MS * 20, 1)
        .await
        .expect("file retention");
    deleted += result.deleted;
    while result.has_more {
        result = store
            .delete_history_before(&environment_id, DAY_MS * 20, 1)
            .await
            .expect("file retention convergence");
        deleted += result.deleted;
    }
    assert_eq!(deleted, SAMPLE_COUNT as u64);
    eprintln!(
        "history-capacity backend=file samples={SAMPLE_COUNT} append500_ms={} first_page200_ms={} continuation200_ms={} retention10000_ms={}",
        append_500.as_millis(),
        first_page_elapsed.as_millis(),
        continuation_elapsed.as_millis(),
        retention_started.elapsed().as_millis(),
    );
}

fn run_sql_capacity(backend: StorageBackend, database_url: Option<String>, data_path: std::path::PathBuf) {
    let owner = RuntimeOwner::new().expect("runtime owner");
    owner.block_on(async {
        let store = SqlPersistence::initialize(
            &StorageConfig {
                backend,
                data_path,
                database_url,
                pool: SqlPoolConfig::default(),
            },
            owner.root_context().component(
                ScopeId::try_new(format!("history-capacity-{backend:?}"))
                    .expect("test scope has the fixed non-empty history capacity prefix"),
            ),
        )
        .await
        .expect("SQL persistence");
        let environment_id = EnvironmentId::new();
        let lease = store
            .acquire_history_lease(&environment_id, "capacity-holder", 30_000)
            .await
            .expect("capacity lease")
            .expect("capacity lease acquired");
        let started = Instant::now();
        store
            .append_history(samples(&environment_id, 0), Some(&lease))
            .await
            .expect("SQL append 500");
        let append_500 = started.elapsed();
        for batch in 1..(SAMPLE_COUNT / APPEND_BATCH) {
            store
                .append_history(samples(&environment_id, batch), Some(&lease))
                .await
                .expect("SQL append batch");
        }
        assert_query_index(&store, backend, &environment_id).await;
        let first_page_started = Instant::now();
        let first_page = store
            .query_history(query(environment_id.clone(), None))
            .await
            .expect("SQL first page");
        let first_page_elapsed = first_page_started.elapsed();
        assert_eq!(first_page.samples.len(), PAGE_SIZE as usize);
        let continuation_started = Instant::now();
        let continuation = store
            .query_history(query(environment_id.clone(), first_page.next_cursor.clone()))
            .await
            .expect("SQL continuation");
        let continuation_elapsed = continuation_started.elapsed();
        assert_eq!(continuation.samples.len(), PAGE_SIZE as usize);
        let retention_started = Instant::now();
        let mut deleted = 0_u64;
        let mut result = store
            .delete_history_before(&environment_id, DAY_MS * 20, APPEND_BATCH as u32, Some(&lease))
            .await
            .expect("SQL retention");
        deleted += result.deleted;
        while result.has_more {
            result = store
                .delete_history_before(&environment_id, DAY_MS * 20, APPEND_BATCH as u32, Some(&lease))
                .await
                .expect("SQL retention convergence");
            deleted += result.deleted;
        }
        assert_eq!(deleted, SAMPLE_COUNT as u64);
        eprintln!(
            "history-capacity backend={backend:?} samples={SAMPLE_COUNT} append500_ms={} first_page200_ms={} continuation200_ms={} retention10000_ms={}",
            append_500.as_millis(),
            first_page_elapsed.as_millis(),
            continuation_elapsed.as_millis(),
            retention_started.elapsed().as_millis(),
        );
    });
    owner.shutdown_runtime_blocking().expect("runtime shutdown");
}

async fn assert_query_index(store: &SqlPersistence, backend: StorageBackend, environment_id: &EnvironmentId) {
    let environment_id = &environment_id.0;
    match backend {
        StorageBackend::Sqlite => {
            let rows = sqlx::query(
                "EXPLAIN QUERY PLAN SELECT bucket_ms FROM dashboard_history_sample \
                 WHERE environment_id = ? AND metric_name = ? AND dimensions_json = ? \
                 AND bucket_ms >= ? AND bucket_ms <= ? ORDER BY bucket_ms, dimensions_json LIMIT ?",
            )
            .bind(environment_id)
            .bind("capacity")
            .bind("[]")
            .bind(0_i64)
            .bind(DAY_MS * 20)
            .bind(i64::from(PAGE_SIZE))
            .fetch_all(store.sqlite_pool().expect("SQLite pool"))
            .await
            .expect("SQLite explain");
            assert!(
                rows.iter()
                    .filter_map(|row| row.try_get::<String, _>("detail").ok())
                    .any(|detail| { detail.contains("dashboard_history_sample_query_idx") }),
                "SQLite query plan must use dashboard_history_sample_query_idx"
            );
        }
        StorageBackend::MySql => {
            let plan = sqlx::query_scalar::<_, String>(
                "EXPLAIN FORMAT=JSON SELECT bucket_ms FROM dashboard_history_sample \
                 WHERE environment_id = ? AND metric_name = ? AND dimensions_json = ? \
                 AND bucket_ms >= ? AND bucket_ms <= ? ORDER BY bucket_ms, dimensions_json LIMIT ?",
            )
            .bind(environment_id.as_bytes())
            .bind(b"capacity".as_slice())
            .bind(b"[]".as_slice())
            .bind(0_i64)
            .bind(DAY_MS * 20)
            .bind(i64::from(PAGE_SIZE))
            .fetch_one(store.mysql_pool().expect("MySQL pool"))
            .await
            .expect("MySQL explain");
            let plan = serde_json::from_str::<serde_json::Value>(&plan).expect("MySQL JSON explain");
            eprintln!("history-capacity explain backend=mysql plan={plan}");
            assert!(
                (json_field_equals(&plan, "key", "dashboard_history_sample_query_idx")
                    || json_field_equals(&plan, "key", "dashboard_history_sample_retention_idx"))
                    && !json_field_equals(&plan, "access_type", "ALL"),
                "MySQL query plan must use a dashboard history index without an ALL scan: {plan}"
            );
        }
        StorageBackend::Postgres => {
            let rows = sqlx::query_scalar::<_, String>(
                "EXPLAIN (COSTS OFF) SELECT bucket_ms FROM dashboard_history_sample \
                 WHERE environment_id = $1 AND metric_name = $2 AND dimensions_json = $3 \
                 AND bucket_ms >= $4 AND bucket_ms <= $5 ORDER BY bucket_ms, dimensions_json LIMIT $6",
            )
            .bind(environment_id)
            .bind("capacity")
            .bind("[]")
            .bind(0_i64)
            .bind(DAY_MS * 20)
            .bind(i64::from(PAGE_SIZE))
            .fetch_all(store.postgres_pool().expect("PostgreSQL pool"))
            .await
            .expect("PostgreSQL explain");
            eprintln!("history-capacity explain backend=postgres plans={rows:?}");
            assert!(
                rows.iter().any(|line| {
                    (line.contains("Index Scan")
                        || line.contains("Index Only Scan")
                        || line.contains("Bitmap Index Scan"))
                        && (line.contains("dashboard_history_sample_query_idx")
                            || line.contains("dashboard_history_sample_retention_idx"))
                }),
                "PostgreSQL query plan must use a dashboard history index: {rows:?}"
            );
        }
        StorageBackend::File => unreachable!("File does not have a SQL EXPLAIN plan"),
    }
}
