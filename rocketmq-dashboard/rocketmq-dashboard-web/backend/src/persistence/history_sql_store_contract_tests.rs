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
use crate::model::MetricDimension;
use crate::model::MetricSample;
use crate::model::StorageBackend;
use crate::persistence::TimeRange;
use crate::persistence::error::PersistenceError;
use crate::persistence::history_repository::HistoryQuery;
use crate::persistence::lease_repository::HistoryLease;
use rocketmq_runtime::RuntimeOwner;

#[test]
fn sqlite_history_cursor_multidimension_atomicity_and_cutoff_contract() {
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
            owner.root_context().component("sqlite-history-contract"),
        )
        .await
        .expect("SQLite persistence");
        let environment_id = EnvironmentId::new();
        let lease = store
            .acquire_history_lease(&environment_id, "sqlite-contract", 30_000)
            .await
            .expect("SQLite lease")
            .expect("SQLite lease acquired");
        assert_invalid_time_boundaries(&store, &environment_id, &lease).await;

        let mut multi = sample(
            environment_id.clone(),
            "multi",
            1_000,
            vec![
                MetricDimension {
                    key: "zone".to_string(),
                    value: "west".to_string(),
                },
                MetricDimension {
                    key: "cluster".to_string(),
                    value: "primary".to_string(),
                },
            ],
            1.0,
        );
        multi.normalize().expect("multi-dimensional normalization");
        store
            .append_history(vec![multi.clone()], Some(&lease))
            .await
            .expect("multi append");
        assert_eq!(
            store
                .query_history(query(
                    environment_id.clone(),
                    "multi",
                    vec![
                        MetricDimension {
                            key: "zone".to_string(),
                            value: "west".to_string(),
                        },
                        MetricDimension {
                            key: "cluster".to_string(),
                            value: "primary".to_string(),
                        },
                    ],
                    10,
                    None,
                ))
                .await
                .expect("normalized multi query")
                .samples,
            vec![multi]
        );

        let cursor_samples = (0..3)
            .map(|offset| {
                sample(
                    environment_id.clone(),
                    "cursor",
                    2_000 + offset,
                    Vec::new(),
                    offset as f64,
                )
            })
            .collect::<Vec<_>>();
        store
            .append_history(cursor_samples.clone(), Some(&lease))
            .await
            .expect("cursor append");
        let first = store
            .query_history(query(environment_id.clone(), "cursor", Vec::new(), 1, None))
            .await
            .expect("first cursor page");
        let second = store
            .query_history(query(
                environment_id.clone(),
                "cursor",
                Vec::new(),
                1,
                first.next_cursor.clone(),
            ))
            .await
            .expect("second cursor page");
        assert_ne!(first.samples[0].bucket_ms, second.samples[0].bucket_ms);

        let existing = sample(environment_id.clone(), "atomic", 3_000, Vec::new(), 1.0);
        let new_sample = sample(environment_id.clone(), "atomic", 3_001, Vec::new(), 2.0);
        store
            .append_history(vec![existing.clone()], Some(&lease))
            .await
            .expect("atomic setup");
        assert!(matches!(
            store
                .append_history(
                    vec![
                        new_sample,
                        sample(environment_id.clone(), "atomic", 3_000, Vec::new(), 3.0)
                    ],
                    Some(&lease),
                )
                .await,
            Err(PersistenceError::Conflict)
        ));
        assert_eq!(
            store
                .query_history(query(environment_id.clone(), "atomic", Vec::new(), 10, None))
                .await
                .expect("atomic query")
                .samples,
            vec![existing]
        );

        let cutoff_environment = EnvironmentId::new();
        let cutoff_lease = store
            .acquire_history_lease(&cutoff_environment, "sqlite-cutoff", 30_000)
            .await
            .expect("cutoff lease")
            .expect("cutoff lease acquired");
        let old = sample(cutoff_environment.clone(), "cutoff", 4_000, Vec::new(), 1.0);
        let exact = sample(cutoff_environment.clone(), "cutoff", 4_001, Vec::new(), 2.0);
        store
            .append_history(vec![old, exact.clone()], Some(&cutoff_lease))
            .await
            .expect("cutoff setup");
        assert_eq!(
            store
                .delete_history_before(&cutoff_environment, 4_001, 10, Some(&cutoff_lease))
                .await
                .expect("exact cutoff retention")
                .deleted,
            1
        );
        assert_eq!(
            store
                .query_history(query(cutoff_environment, "cutoff", Vec::new(), 10, None))
                .await
                .expect("exact cutoff query")
                .samples,
            vec![exact]
        );
    });
    owner.shutdown_runtime_blocking().expect("runtime shutdown");
}

#[test]
#[ignore = "requires docker-compose.storage-test.yml"]
fn docker_mysql_history_identity_cursor_and_retention_contract() {
    let database_url = std::env::var("ROCKETMQ_DASHBOARD_STORAGE_TEST_MYSQL_URL")
        .expect("ROCKETMQ_DASHBOARD_STORAGE_TEST_MYSQL_URL must be set by the storage test runner");
    docker_history_identity_cursor_and_retention_contract(StorageBackend::MySql, database_url);
}

#[test]
#[ignore = "requires docker-compose.storage-test.yml"]
fn docker_postgres_history_identity_cursor_and_retention_contract() {
    let database_url = std::env::var("ROCKETMQ_DASHBOARD_STORAGE_TEST_POSTGRES_URL")
        .expect("ROCKETMQ_DASHBOARD_STORAGE_TEST_POSTGRES_URL must be set by the storage test runner");
    docker_history_identity_cursor_and_retention_contract(StorageBackend::Postgres, database_url);
}

fn sample(
    environment_id: EnvironmentId,
    metric: &str,
    bucket_ms: i64,
    dimensions: Vec<MetricDimension>,
    value: f64,
) -> MetricSample {
    MetricSample {
        environment_id,
        metric: metric.to_string(),
        bucket_ms,
        dimensions,
        value,
    }
}

fn query(
    environment_id: EnvironmentId,
    metric: &str,
    dimensions: Vec<MetricDimension>,
    limit: u32,
    cursor: Option<String>,
) -> HistoryQuery {
    let mut query = HistoryQuery {
        environment_id,
        metric: metric.to_string(),
        range: TimeRange {
            start_ms: 0,
            end_ms: 100_000,
        },
        dimensions,
        limit,
        cursor,
    };
    query.validate_and_normalize().expect("valid history query");
    query
}

fn docker_history_identity_cursor_and_retention_contract(backend: StorageBackend, database_url: String) {
    let owner = RuntimeOwner::new().expect("runtime owner");
    owner.block_on(async {
        let store = SqlPersistence::initialize(
            &StorageConfig {
                backend,
                data_path: "unused".into(),
                database_url: Some(database_url),
                pool: SqlPoolConfig::default(),
            },
            owner.root_context().component("docker-history-identity-contract"),
        )
        .await
        .expect("SQL persistence");

        // Binary (MySQL) and C-collated (PostgreSQL) identity must preserve
        // spaces, case, and UTF-8 bytes rather than applying a collation.
        for (index, identity) in ["x", "x ", "X", "节点"].into_iter().enumerate() {
            let environment_id = EnvironmentId(identity.to_string());
            let lease = store
                .acquire_history_lease(&environment_id, &format!("exact-{index}"), 30_000)
                .await
                .expect("identity lease")
                .expect("identity lease acquired");
            let record = sample(
                environment_id.clone(),
                identity,
                1_000,
                vec![MetricDimension {
                    key: "label".to_string(),
                    value: identity.to_string(),
                }],
                index as f64,
            );
            store
                .append_history(vec![record.clone()], Some(&lease))
                .await
                .expect("exact identity append");
            assert_eq!(
                store
                    .query_history(query(
                        environment_id,
                        identity,
                        vec![MetricDimension {
                            key: "label".to_string(),
                            value: identity.to_string(),
                        }],
                        10,
                        None,
                    ))
                    .await
                    .expect("exact identity query")
                    .samples,
                vec![record]
            );
        }

        let environment_id = EnvironmentId::new();
        let lease = store
            .acquire_history_lease(&environment_id, "contract-holder", 30_000)
            .await
            .expect("contract lease")
            .expect("contract lease acquired");
        assert_invalid_time_boundaries(&store, &environment_id, &lease).await;
        let mut normalized = sample(
            environment_id.clone(),
            "multi-dimensional",
            2_000,
            vec![
                MetricDimension {
                    key: "zone".to_string(),
                    value: "west".to_string(),
                },
                MetricDimension {
                    key: "cluster".to_string(),
                    value: "primary".to_string(),
                },
            ],
            1.0,
        );
        normalized.normalize().expect("normalized dimensions");
        store
            .append_history(vec![normalized.clone()], Some(&lease))
            .await
            .expect("multi-dimensional append");
        let normalized_page = store
            .query_history(query(
                environment_id.clone(),
                "multi-dimensional",
                vec![
                    MetricDimension {
                        key: "zone".to_string(),
                        value: "west".to_string(),
                    },
                    MetricDimension {
                        key: "cluster".to_string(),
                        value: "primary".to_string(),
                    },
                ],
                10,
                None,
            ))
            .await
            .expect("normalized query");
        assert_eq!(normalized_page.samples, vec![normalized]);

        let cursor_samples = (0..3)
            .map(|offset| {
                sample(
                    environment_id.clone(),
                    "cursor",
                    10_000 + offset * 1_000,
                    Vec::new(),
                    offset as f64,
                )
            })
            .collect::<Vec<_>>();
        store
            .append_history(cursor_samples.clone(), Some(&lease))
            .await
            .expect("cursor samples");
        let first_page = store
            .query_history(query(environment_id.clone(), "cursor", Vec::new(), 1, None))
            .await
            .expect("first cursor page");
        let second_page = store
            .query_history(query(
                environment_id.clone(),
                "cursor",
                Vec::new(),
                1,
                first_page.next_cursor.clone(),
            ))
            .await
            .expect("second cursor page");
        let third_page = store
            .query_history(query(
                environment_id.clone(),
                "cursor",
                Vec::new(),
                1,
                second_page.next_cursor.clone(),
            ))
            .await
            .expect("third cursor page");
        assert_eq!(
            [first_page.samples, second_page.samples, third_page.samples]
                .into_iter()
                .flatten()
                .map(|sample| sample.bucket_ms)
                .collect::<Vec<_>>(),
            cursor_samples
                .into_iter()
                .map(|sample| sample.bucket_ms)
                .collect::<Vec<_>>()
        );
        assert!(third_page.next_cursor.is_none());

        let existing = sample(environment_id.clone(), "atomic", 20_000, Vec::new(), 1.0);
        store
            .append_history(vec![existing.clone()], Some(&lease))
            .await
            .expect("existing atomic sample");
        let new_sample = sample(environment_id.clone(), "atomic", 21_000, Vec::new(), 2.0);
        let conflicting = sample(environment_id.clone(), "atomic", 20_000, Vec::new(), 3.0);
        assert!(matches!(
            store
                .append_history(vec![new_sample.clone(), conflicting], Some(&lease))
                .await,
            Err(PersistenceError::Conflict)
        ));
        assert_eq!(
            store
                .query_history(query(environment_id.clone(), "atomic", Vec::new(), 10, None))
                .await
                .expect("atomic rollback query")
                .samples,
            vec![existing]
        );

        let boundary_environment = EnvironmentId::new();
        let boundary_lease = store
            .acquire_history_lease(&boundary_environment, "boundary-holder", 30_000)
            .await
            .expect("boundary lease")
            .expect("boundary lease acquired");
        let boundary_old = sample(boundary_environment.clone(), "boundary", 30_000, Vec::new(), 1.0);
        let boundary_exact = sample(boundary_environment.clone(), "boundary", 31_000, Vec::new(), 2.0);
        store
            .append_history(vec![boundary_old, boundary_exact.clone()], Some(&boundary_lease))
            .await
            .expect("boundary samples");
        let deleted = store
            .delete_history_before(&boundary_environment, 31_000, 1, Some(&boundary_lease))
            .await
            .expect("exact boundary retention");
        assert_eq!(deleted.deleted, 1);
        assert_eq!(
            store
                .query_history(query(boundary_environment, "boundary", Vec::new(), 10, None))
                .await
                .expect("boundary query")
                .samples,
            vec![boundary_exact]
        );

        let other_environment = EnvironmentId::new();
        let other_lease = store
            .acquire_history_lease(&other_environment, "other-environment", 30_000)
            .await
            .expect("other lease")
            .expect("other lease acquired");
        let other = sample(other_environment.clone(), "isolated", 40_000, Vec::new(), 9.0);
        let own = sample(environment_id.clone(), "isolated", 40_000, Vec::new(), 1.0);
        store
            .append_history(vec![other.clone()], Some(&other_lease))
            .await
            .expect("other append");
        store.append_history(vec![own], Some(&lease)).await.expect("own append");
        store
            .delete_history_before(&environment_id, 41_000, 10, Some(&lease))
            .await
            .expect("isolated retention");
        assert_eq!(
            store
                .query_history(query(other_environment, "isolated", Vec::new(), 10, None))
                .await
                .expect("other environment survives")
                .samples,
            vec![other]
        );
    });
    owner.shutdown_runtime_blocking().expect("runtime shutdown");
}

async fn assert_invalid_time_boundaries(store: &SqlPersistence, environment_id: &EnvironmentId, lease: &HistoryLease) {
    assert!(matches!(
        store
            .query_history(HistoryQuery {
                environment_id: environment_id.clone(),
                metric: "invalid-time".to_string(),
                range: TimeRange {
                    start_ms: 0,
                    end_ms: i64::MAX,
                },
                dimensions: Vec::new(),
                limit: 1,
                cursor: None,
            })
            .await,
        Err(PersistenceError::InvalidConfig(_))
    ));
    assert!(matches!(
        store
            .delete_history_before(environment_id, i64::MAX, 1, Some(lease))
            .await,
        Err(PersistenceError::InvalidConfig(_))
    ));
}
