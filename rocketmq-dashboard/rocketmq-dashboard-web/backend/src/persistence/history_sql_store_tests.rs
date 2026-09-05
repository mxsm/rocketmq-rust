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

use super::SqlPersistence;
use crate::config::SqlPoolConfig;
use crate::config::StorageConfig;
use crate::model::EnvironmentId;
use crate::model::MetricSample;
use crate::model::StorageBackend;
use crate::persistence::TimeRange;
use crate::persistence::error::PersistenceError;
use crate::persistence::history_repository::HistoryQuery;
use rocketmq_runtime::RuntimeOwner;

fn sample(environment_id: EnvironmentId, bucket_ms: i64, value: f64) -> MetricSample {
    MetricSample {
        environment_id,
        metric: "broker-count".to_string(),
        bucket_ms,
        dimensions: Vec::new(),
        value,
    }
}

#[test]
fn sqlite_history_is_idempotent_conflict_checked_and_reopens() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let owner = RuntimeOwner::new().expect("runtime owner");
    owner.block_on(async {
        let config = StorageConfig {
            backend: StorageBackend::Sqlite,
            data_path: directory.path().join("dashboard.db"),
            database_url: None,
            pool: SqlPoolConfig::default(),
        };
        let environment_id = EnvironmentId::new();
        let store = SqlPersistence::initialize(&config, owner.root_context().component("history-sqlite"))
            .await
            .expect("SQLite persistence");
        let lease = store
            .acquire_history_lease(&environment_id, "test-holder", 30_000)
            .await
            .expect("lease query")
            .expect("lease acquired");
        let initial = sample(environment_id.clone(), 1_000, 3.0);
        store
            .append_history(vec![initial.clone()], Some(&lease))
            .await
            .expect("first append");
        store
            .append_history(vec![initial.clone()], Some(&lease))
            .await
            .expect("idempotent append");
        let page = store
            .query_history(HistoryQuery {
                environment_id: environment_id.clone(),
                metric: "broker-count".to_string(),
                range: TimeRange {
                    start_ms: 0,
                    end_ms: 2_000,
                },
                dimensions: Vec::new(),
                limit: 10,
                cursor: None,
            })
            .await
            .expect("history query");
        assert_eq!(page.samples, vec![initial.clone()]);
        assert!(matches!(
            store
                .append_history(vec![sample(environment_id.clone(), 1_000, 4.0)], Some(&lease))
                .await,
            Err(PersistenceError::Conflict)
        ));
        drop(store);

        let reopened = SqlPersistence::initialize(&config, owner.root_context().component("history-sqlite-reopen"))
            .await
            .expect("reopen SQLite persistence");
        let page = reopened
            .query_history(HistoryQuery {
                environment_id,
                metric: "broker-count".to_string(),
                range: TimeRange {
                    start_ms: 0,
                    end_ms: 2_000,
                },
                dimensions: Vec::new(),
                limit: 10,
                cursor: None,
            })
            .await
            .expect("reopened history query");
        assert_eq!(page.samples, vec![initial]);
    });
    owner.shutdown_runtime_blocking().expect("runtime shutdown");
}

#[test]
fn sqlite_history_lease_rejects_stale_holder_writes() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let owner = RuntimeOwner::new().expect("runtime owner");
    owner.block_on(async {
        let config = StorageConfig {
            backend: StorageBackend::Sqlite,
            data_path: directory.path().join("dashboard.db"),
            database_url: None,
            pool: SqlPoolConfig::default(),
        };
        let store = SqlPersistence::initialize(&config, owner.root_context().component("history-lease"))
            .await
            .expect("SQLite persistence");
        let environment_id = EnvironmentId::new();
        let first = store
            .acquire_history_lease(&environment_id, "first-holder", 30_000)
            .await
            .expect("first lease query")
            .expect("first lease acquired");
        sqlx::query("UPDATE dashboard_task_lease SET expires_at_ms = 0 WHERE lease_name = ?")
            .bind(&first.name)
            .execute(store.sqlite_pool().expect("SQLite pool"))
            .await
            .expect("expire first lease");

        let second = store
            .acquire_history_lease(&environment_id, "second-holder", 30_000)
            .await
            .expect("second lease query")
            .expect("second lease acquired");
        assert_eq!(second.fencing_token, first.fencing_token + 1);
        assert!(
            store
                .renew_history_lease(&first, 30_000)
                .await
                .expect("stale renewal query")
                .is_none()
        );
        assert!(!store.release_history_lease(&first).await.expect("stale release query"));
        assert!(matches!(
            store
                .append_history(vec![sample(environment_id.clone(), 1_000, 1.0)], Some(&first))
                .await,
            Err(PersistenceError::Conflict)
        ));
        assert!(matches!(
            store
                .delete_history_before(&environment_id, 3_000, 1, Some(&first))
                .await,
            Err(PersistenceError::Conflict)
        ));
        store
            .append_history(vec![sample(environment_id.clone(), 2_000, 2.0)], Some(&second))
            .await
            .expect("current holder append");
        assert!(matches!(
            store
                .append_history(vec![sample(EnvironmentId::new(), 2_000, 2.0)], Some(&second))
                .await,
            Err(PersistenceError::Conflict)
        ));
        assert!(matches!(
            store
                .delete_history_before(&EnvironmentId::new(), 3_000, 1, Some(&second))
                .await,
            Err(PersistenceError::Conflict)
        ));
        assert!(
            store
                .release_history_lease(&second)
                .await
                .expect("current release query")
        );
        assert!(matches!(
            store
                .append_history(vec![sample(environment_id, 3_000, 3.0)], Some(&second))
                .await,
            Err(PersistenceError::Conflict)
        ));
    });
    owner.shutdown_runtime_blocking().expect("runtime shutdown");
}

#[test]
fn sqlite_history_retention_reports_exact_convergence() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let owner = RuntimeOwner::new().expect("runtime owner");
    owner.block_on(async {
        let store = SqlPersistence::initialize(
            &StorageConfig {
                backend: StorageBackend::Sqlite,
                data_path: directory.path().join("dashboard.db"),
                database_url: None,
                pool: SqlPoolConfig::default(),
            },
            owner.root_context().component("history-retention"),
        )
        .await
        .expect("SQLite persistence");
        let environment_id = EnvironmentId::new();
        let lease = store
            .acquire_history_lease(&environment_id, "test-holder", 30_000)
            .await
            .expect("lease query")
            .expect("lease acquired");
        store
            .append_history(
                vec![
                    sample(environment_id.clone(), 1_000, 1.0),
                    sample(environment_id.clone(), 2_000, 2.0),
                ],
                Some(&lease),
            )
            .await
            .expect("exact batch append");
        let other_environment_id = EnvironmentId::new();
        let other_lease = store
            .acquire_history_lease(&other_environment_id, "other-holder", 30_000)
            .await
            .expect("other lease query")
            .expect("other lease acquired");
        let other_sample = sample(other_environment_id.clone(), 1_000, 9.0);
        store
            .append_history(vec![other_sample.clone()], Some(&other_lease))
            .await
            .expect("other environment append");
        let exact = store
            .delete_history_before(&environment_id, 3_000, 2, Some(&lease))
            .await
            .expect("exact batch retention");
        assert_eq!(exact.deleted, 2);
        assert!(!exact.has_more);
        assert_eq!(
            store
                .query_history(HistoryQuery {
                    environment_id: other_environment_id,
                    metric: "broker-count".to_string(),
                    range: TimeRange {
                        start_ms: 0,
                        end_ms: 2_000,
                    },
                    dimensions: Vec::new(),
                    limit: 10,
                    cursor: None,
                })
                .await
                .expect("other environment history")
                .samples,
            vec![other_sample]
        );

        store
            .append_history(
                vec![
                    sample(environment_id.clone(), 4_000, 4.0),
                    sample(environment_id.clone(), 5_000, 5.0),
                    sample(environment_id.clone(), 6_000, 6.0),
                ],
                Some(&lease),
            )
            .await
            .expect("over-batch append");
        let first = store
            .delete_history_before(&environment_id, 7_000, 2, Some(&lease))
            .await
            .expect("first over-batch retention");
        assert_eq!(first.deleted, 2);
        assert!(first.has_more);
        let second = store
            .delete_history_before(&environment_id, 7_000, 2, Some(&lease))
            .await
            .expect("second over-batch retention");
        assert_eq!(second.deleted, 1);
        assert!(!second.has_more);
    });
    owner.shutdown_runtime_blocking().expect("runtime shutdown");
}

#[test]
#[ignore = "requires docker-compose.storage-test.yml"]
fn docker_mysql_history_lease_contract() {
    let database_url = std::env::var("ROCKETMQ_DASHBOARD_STORAGE_TEST_MYSQL_URL")
        .expect("ROCKETMQ_DASHBOARD_STORAGE_TEST_MYSQL_URL must be set by the storage test runner");
    docker_history_lease_contract(StorageBackend::MySql, database_url);
}

#[test]
#[ignore = "requires docker-compose.storage-test.yml"]
fn docker_postgres_history_lease_contract() {
    let database_url = std::env::var("ROCKETMQ_DASHBOARD_STORAGE_TEST_POSTGRES_URL")
        .expect("ROCKETMQ_DASHBOARD_STORAGE_TEST_POSTGRES_URL must be set by the storage test runner");
    docker_history_lease_contract(StorageBackend::Postgres, database_url);
}

fn docker_history_lease_contract(backend: StorageBackend, database_url: String) {
    let owner = RuntimeOwner::new().expect("runtime owner");
    owner.block_on(async {
        let store = SqlPersistence::initialize(
            &StorageConfig {
                backend,
                data_path: "unused".into(),
                database_url: Some(database_url),
                pool: SqlPoolConfig::default(),
            },
            owner.root_context().component("docker-history-lease"),
        )
        .await
        .expect("SQL persistence");
        if backend == StorageBackend::MySql {
            let concurrent_environment = EnvironmentId::new();
            let (left, right) = tokio::join!(
                store.acquire_history_lease(&concurrent_environment, "concurrent-left", 30_000),
                store.acquire_history_lease(&concurrent_environment, "concurrent-right", 30_000),
            );
            assert_eq!(
                [left.expect("left acquire"), right.expect("right acquire")]
                    .into_iter()
                    .flatten()
                    .count(),
                1,
                "exactly one MySQL contender acquires a missing lease row"
            );
        }
        let environment_id = EnvironmentId::new();
        let first = store
            .acquire_history_lease(&environment_id, "first-holder", 30_000)
            .await
            .expect("first lease query")
            .expect("first lease acquired");
        assert!(
            store
                .acquire_history_lease(&environment_id, "second-holder", 30_000)
                .await
                .expect("active lease query")
                .is_none()
        );

        let initial = sample(environment_id.clone(), 1_000, 3.0);
        store
            .append_history(vec![initial.clone()], Some(&first))
            .await
            .expect("first append");
        store
            .append_history(vec![initial.clone()], Some(&first))
            .await
            .expect("idempotent append");
        match backend {
            StorageBackend::MySql => {
                sqlx::query("UPDATE dashboard_task_lease SET expires_at_ms = 0 WHERE lease_name = ?")
                    .bind(&first.name)
                    .execute(store.mysql_pool().expect("MySQL pool"))
                    .await
                    .expect("expire first lease");
            }
            StorageBackend::Postgres => {
                sqlx::query("UPDATE dashboard_task_lease SET expires_at_ms = 0 WHERE lease_name = $1")
                    .bind(&first.name)
                    .execute(store.postgres_pool().expect("PostgreSQL pool"))
                    .await
                    .expect("expire first lease");
            }
            StorageBackend::File | StorageBackend::Sqlite => unreachable!("SQL Docker test backend"),
        }
        let second = store
            .acquire_history_lease(&environment_id, "second-holder", 30_000)
            .await
            .expect("second lease query")
            .expect("second lease acquired");
        assert_eq!(second.fencing_token, first.fencing_token + 1);
        assert!(
            store
                .renew_history_lease(&first, 30_000)
                .await
                .expect("stale renewal query")
                .is_none()
        );
        assert!(!store.release_history_lease(&first).await.expect("stale release query"));
        assert!(matches!(
            store
                .append_history(vec![sample(environment_id.clone(), 2_000, 4.0)], Some(&first))
                .await,
            Err(PersistenceError::Conflict)
        ));
        assert!(matches!(
            store
                .delete_history_before(&environment_id, 2_000, 1, Some(&first))
                .await,
            Err(PersistenceError::Conflict)
        ));
        let page = store
            .query_history(HistoryQuery {
                environment_id: environment_id.clone(),
                metric: "broker-count".to_string(),
                range: TimeRange {
                    start_ms: 0,
                    end_ms: 2_000,
                },
                dimensions: Vec::new(),
                limit: 10,
                cursor: None,
            })
            .await
            .expect("history query");
        assert_eq!(page.samples, vec![initial]);
        let mut retention = store
            .delete_history_before(&environment_id, 2_000, 1, Some(&second))
            .await
            .expect("retention");
        assert_eq!(retention.deleted, 1);
        while retention.has_more {
            retention = store
                .delete_history_before(&environment_id, 2_000, 1, Some(&second))
                .await
                .expect("retention convergence");
        }
        assert!(
            store
                .query_history(HistoryQuery {
                    environment_id: environment_id.clone(),
                    metric: "broker-count".to_string(),
                    range: TimeRange {
                        start_ms: 0,
                        end_ms: 2_000,
                    },
                    dimensions: Vec::new(),
                    limit: 10,
                    cursor: None,
                })
                .await
                .expect("retained history query")
                .samples
                .is_empty()
        );
        assert!(
            store
                .release_history_lease(&second)
                .await
                .expect("current release query")
        );
        assert!(matches!(
            store
                .append_history(vec![sample(environment_id, 3_000, 5.0)], Some(&second))
                .await,
            Err(PersistenceError::Conflict)
        ));
    });
    owner.shutdown_runtime_blocking().expect("runtime shutdown");
}
