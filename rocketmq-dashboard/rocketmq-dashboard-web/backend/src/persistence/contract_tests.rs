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
use super::DashboardPersistence;
use super::Revision;
use crate::config::SqlPoolConfig;
use crate::config::StorageConfig;
use crate::model::ConsumerMonitorRule;
use crate::model::DashboardConfigView;
use crate::model::DashboardEnvironment;
use crate::model::Endpoint;
use crate::model::EndpointId;
use crate::model::EndpointRole;
use crate::model::EndpointType;
use crate::model::EnvironmentId;
use crate::model::StorageBackend;
use crate::persistence::backend::PersistenceBackend;
use crate::persistence::environment_repository::DEFAULT_ENVIRONMENT_ID;
use crate::persistence::error::PersistenceError;
use rocketmq_runtime::RuntimeOwner;
use sqlx::MySqlPool;
use sqlx::PgPool;
use sqlx::SqlitePool;
use std::path::PathBuf;

#[test]
fn file_repository_contract_persists_environment_and_monitor_revisions() {
    let directory = tempfile::tempdir().expect("temp directory");
    let config = StorageConfig {
        backend: StorageBackend::File,
        data_path: directory.path().join("file"),
        database_url: None,
        pool: SqlPoolConfig::default(),
    };
    run_contract(config);
}

#[test]
fn sqlite_repository_contract_persists_environment_and_monitor_revisions() {
    let directory = tempfile::tempdir().expect("temp directory");
    let config = StorageConfig {
        backend: StorageBackend::Sqlite,
        data_path: directory.path().join("sqlite/dashboard.db"),
        database_url: None,
        pool: SqlPoolConfig::default(),
    };
    run_contract(config);
}

#[test]
#[ignore = "requires docker-compose.storage-test.yml"]
fn docker_mysql_repository_contract() {
    let url = std::env::var("ROCKETMQ_DASHBOARD_STORAGE_TEST_MYSQL_URL")
        .expect("storage test runner must provide the MySQL URL");
    run_contract(StorageConfig {
        backend: StorageBackend::MySql,
        data_path: "unused".into(),
        database_url: Some(url),
        pool: SqlPoolConfig::default(),
    });
}

#[test]
#[ignore = "requires docker-compose.storage-test.yml"]
fn docker_postgres_repository_contract() {
    let url = std::env::var("ROCKETMQ_DASHBOARD_STORAGE_TEST_POSTGRES_URL")
        .expect("storage test runner must provide the PostgreSQL URL");
    run_contract(StorageConfig {
        backend: StorageBackend::Postgres,
        data_path: "unused".into(),
        database_url: Some(url),
        pool: SqlPoolConfig::default(),
    });
}

fn run_contract(config: StorageConfig) {
    let owner = RuntimeOwner::new().expect("runtime owner");
    owner.block_on(async {
        let first = DashboardPersistence::initialize(&config, owner.root_context().component("contract-first"))
            .await
            .expect("initialize first persistence");
        let mut wrong_default_id = DashboardEnvironment::bootstrap(&DashboardConfigView::default(), 1);
        wrong_default_id.environment_id = EnvironmentId::new();
        assert!(matches!(
            first.create_environment(wrong_default_id).await,
            Err(PersistenceError::InvalidConfig(_))
        ));
        let mut wrong_default_name = DashboardEnvironment::bootstrap(&DashboardConfigView::default(), 1);
        wrong_default_name.environment_id = EnvironmentId(DEFAULT_ENVIRONMENT_ID.to_string());
        wrong_default_name.name = "not-default".to_string();
        assert!(matches!(
            first.create_environment(wrong_default_name).await,
            Err(PersistenceError::InvalidConfig(_))
        ));
        let mut environment = DashboardEnvironment::bootstrap(&DashboardConfigView::default(), 1);
        environment.environment_id = EnvironmentId::new();
        environment.name = format!("contract-{}", environment.environment_id.0);
        environment.endpoints.reverse();
        let environment = first.create_environment(environment).await.expect("create environment");
        assert_endpoint_order(&environment.endpoints);
        let mut duplicate_name = environment.clone();
        duplicate_name.environment_id = EnvironmentId::new();
        duplicate_name.revision = Revision(1);
        assert!(matches!(
            first.create_environment(duplicate_name).await,
            Err(PersistenceError::Conflict)
        ));
        let mut differently_named = DashboardEnvironment::bootstrap(&DashboardConfigView::default(), 1);
        differently_named.environment_id = EnvironmentId::new();
        differently_named.name = format!("contract-other-{}", differently_named.environment_id.0);
        let differently_named = first
            .create_environment(differently_named)
            .await
            .expect("create differently named environment");
        let mut rename_source = DashboardEnvironment::bootstrap(&DashboardConfigView::default(), 1);
        rename_source.environment_id = EnvironmentId::new();
        rename_source.name = format!("contract-rename-source-{}", rename_source.environment_id.0);
        let rename_source = first
            .create_environment(rename_source)
            .await
            .expect("create environment for concurrent name collision");
        let shared_name = format!("contract-shared-name-{}", rename_source.environment_id.0);
        let mut same_name_create = DashboardEnvironment::bootstrap(&DashboardConfigView::default(), 1);
        same_name_create.environment_id = EnvironmentId::new();
        same_name_create.name = shared_name.clone();
        let mut same_name_update = rename_source.clone();
        same_name_update.name = shared_name;
        let (created, renamed) = tokio::join!(
            first.create_environment(same_name_create),
            first.update_environment(rename_source.revision, same_name_update)
        );
        let name_collision_winner = match (created, renamed) {
            (Ok(created), Err(PersistenceError::Conflict)) => created,
            (Err(PersistenceError::Conflict), Ok(renamed)) => renamed,
            (created, renamed) => {
                panic!("concurrent environment name results were unexpected: {created:?}, {renamed:?}")
            }
        };
        assert!(
            first
                .delete_environment(&name_collision_winner.environment_id, name_collision_winner.revision)
                .await
                .expect("remove concurrent name winner")
        );
        if name_collision_winner.environment_id != rename_source.environment_id {
            assert!(
                first
                    .delete_environment(&rename_source.environment_id, rename_source.revision)
                    .await
                    .expect("remove concurrent name source")
            );
        }
        let mut duplicate_update = differently_named.clone();
        duplicate_update.name = environment.name.clone();
        assert!(matches!(
            first
                .update_environment(differently_named.revision, duplicate_update)
                .await,
            Err(PersistenceError::Conflict)
        ));
        assert!(
            first
                .delete_environment(&differently_named.environment_id, differently_named.revision)
                .await
                .expect("delete differently named environment")
        );
        assert_eq!(
            first
                .load_environment(&environment.environment_id)
                .await
                .expect("load environment"),
            environment
        );

        let mut candidate = environment.clone();
        candidate.use_tls = !candidate.use_tls;
        candidate.updated_at_ms = 2;
        let updated = first
            .update_environment(environment.revision, candidate)
            .await
            .expect("CAS environment update");
        assert_eq!(updated.revision, Revision(environment.revision.0 + 1));
        assert!(matches!(
            first
                .update_environment(environment.revision, environment.clone())
                .await,
            Err(PersistenceError::Conflict)
        ));

        let mut switch_candidate = updated.clone();
        let now = 3;
        for endpoint in &mut switch_candidate.endpoints {
            if endpoint.endpoint_type == EndpointType::Proxy {
                endpoint.is_active = false;
                endpoint.role = EndpointRole::Secondary;
            }
        }
        switch_candidate.endpoints.push(Endpoint {
            endpoint_id: EndpointId::new(),
            endpoint_type: EndpointType::Proxy,
            address: "127.0.0.2:8080".to_string(),
            role: EndpointRole::Primary,
            is_enabled: true,
            is_active: true,
            sort_order: 1,
            created_at_ms: now,
            updated_at_ms: now,
        });
        switch_candidate.updated_at_ms = now;
        let switched = first
            .update_environment(updated.revision, switch_candidate)
            .await
            .expect("switch active endpoint atomically");
        assert_eq!(
            switched
                .endpoints
                .iter()
                .filter(|endpoint| endpoint.endpoint_type == EndpointType::Proxy && endpoint.is_active)
                .count(),
            1
        );
        assert_endpoint_order(&switched.endpoints);

        let mut left = switched.clone();
        left.use_tls = !left.use_tls;
        let mut right = switched.clone();
        right.use_vip_channel = !right.use_vip_channel;
        let (left, right) = tokio::join!(
            first.update_environment(switched.revision, left),
            first.update_environment(switched.revision, right)
        );
        let concurrent = match (left, right) {
            (Ok(environment), Err(PersistenceError::Conflict)) | (Err(PersistenceError::Conflict), Ok(environment)) => {
                environment
            }
            (left, right) => panic!("concurrent CAS results were unexpected: {left:?}, {right:?}"),
        };

        let mut invalid = concurrent.clone();
        for endpoint in &mut invalid.endpoints {
            if endpoint.endpoint_type == EndpointType::Proxy {
                endpoint.is_active = true;
            }
        }
        assert!(matches!(
            first.update_environment(concurrent.revision, invalid).await,
            Err(PersistenceError::InvalidConfig(_))
        ));
        assert_eq!(
            first
                .load_environment(&concurrent.environment_id)
                .await
                .expect("failed candidate must not replace persisted environment"),
            concurrent
        );

        let orphan_id = EnvironmentId::new();
        let orphan_rule = ConsumerMonitorRule {
            environment_id: orphan_id.clone(),
            consumer_group: "orphan-contract-group".to_string(),
            min_count: 1,
            max_diff_total: 10,
            revision: Revision(0),
            created_at_ms: 0,
            updated_at_ms: 0,
        };
        assert!(matches!(
            first.list_monitor_rules(&orphan_id).await,
            Err(PersistenceError::NotFound)
        ));
        assert!(matches!(
            first.upsert_monitor_rule(orphan_rule, Revision(0)).await,
            Err(PersistenceError::NotFound)
        ));
        assert!(matches!(
            first
                .delete_monitor_rule(&orphan_id, "orphan-contract-group", Revision(0))
                .await,
            Err(PersistenceError::NotFound)
        ));

        let concurrent_monitor_create = ConsumerMonitorRule {
            environment_id: concurrent.environment_id.clone(),
            consumer_group: "concurrent-monitor-create".to_string(),
            min_count: 1,
            max_diff_total: 10,
            revision: Revision(0),
            created_at_ms: 0,
            updated_at_ms: 0,
        };
        let (created_left, created_right) = tokio::join!(
            first.upsert_monitor_rule(concurrent_monitor_create.clone(), Revision(0)),
            first.upsert_monitor_rule(concurrent_monitor_create.clone(), Revision(0))
        );
        let concurrent_monitor_rule = match (created_left, created_right) {
            (Ok(rule), Err(PersistenceError::Conflict)) | (Err(PersistenceError::Conflict), Ok(rule)) => rule,
            (left, right) => panic!("concurrent monitor creates were unexpected: {left:?}, {right:?}"),
        };
        assert_eq!(concurrent_monitor_rule.revision, Revision(1));
        assert_eq!(
            first
                .list_monitor_rules(&concurrent.environment_id)
                .await
                .expect("list concurrent monitor create"),
            vec![concurrent_monitor_rule]
        );
        assert!(
            first
                .delete_monitor_rule(&concurrent.environment_id, "concurrent-monitor-create", Revision(1),)
                .await
                .expect("remove concurrent monitor rule")
        );

        let rule = ConsumerMonitorRule {
            environment_id: concurrent.environment_id.clone(),
            consumer_group: "contract-group".to_string(),
            min_count: 1,
            max_diff_total: 10,
            revision: Revision(0),
            created_at_ms: 0,
            updated_at_ms: 0,
        };
        let rule = first
            .upsert_monitor_rule(rule, Revision(0))
            .await
            .expect("create monitor rule");
        assert_eq!(rule.revision, Revision(1));
        assert!(matches!(
            first.upsert_monitor_rule(rule.clone(), Revision(0)).await,
            Err(PersistenceError::Conflict)
        ));
        assert_eq!(
            first
                .list_monitor_rules(&concurrent.environment_id)
                .await
                .expect("list monitor rules"),
            vec![rule.clone()]
        );
        let mut updated_rule = rule.clone();
        updated_rule.min_count = 2;
        let updated_rule = first
            .upsert_monitor_rule(updated_rule, rule.revision)
            .await
            .expect("CAS monitor update");
        assert_eq!(updated_rule.revision, Revision(2));
        assert!(matches!(
            first.upsert_monitor_rule(rule.clone(), rule.revision).await,
            Err(PersistenceError::Conflict)
        ));
        assert!(matches!(
            first
                .delete_monitor_rule(&concurrent.environment_id, "contract-group", rule.revision)
                .await,
            Err(PersistenceError::Conflict)
        ));
        assert!(
            first
                .delete_monitor_rule(&concurrent.environment_id, "contract-group", updated_rule.revision)
                .await
                .expect("CAS monitor delete")
        );
        assert!(
            first
                .list_monitor_rules(&concurrent.environment_id)
                .await
                .expect("list deleted monitor rules")
                .is_empty()
        );
        let restored_rule = first
            .upsert_monitor_rule(rule, Revision(0))
            .await
            .expect("recreate monitor rule after delete");
        assert_eq!(restored_rule.revision, Revision(1));
        drop(first);

        let second = DashboardPersistence::initialize(&config, owner.root_context().component("contract-second"))
            .await
            .expect("restart persistence");
        assert_eq!(
            second
                .load_environment(&concurrent.environment_id)
                .await
                .expect("recover environment"),
            concurrent
        );
        assert_eq!(
            second
                .list_monitor_rules(&environment.environment_id)
                .await
                .expect("recover monitor rule")
                .len(),
            1
        );
        let recovered = second
            .load_environment(&environment.environment_id)
            .await
            .expect("recover for delete");
        assert!(
            second
                .delete_environment(&environment.environment_id, recovered.revision)
                .await
                .expect("delete non-default environment")
        );
        assert!(matches!(
            second.load_environment(&environment.environment_id).await,
            Err(PersistenceError::NotFound)
        ));
        assert!(matches!(
            second.list_monitor_rules(&environment.environment_id).await,
            Err(PersistenceError::NotFound)
        ));
        let default_environment = second
            .create_environment(DashboardEnvironment::bootstrap(&DashboardConfigView::default(), 4))
            .await
            .expect("create fixed default environment before corruption test");
        let cleanup = corrupt_default_environment_identity(&second, &default_environment, &config).await;
        drop(second);
        assert!(matches!(
            DashboardPersistence::initialize(&config, owner.root_context().component("contract-corrupt-default")).await,
            Err(PersistenceError::InvalidConfig(_))
        ));
        cleanup_corrupted_default_environment(cleanup).await;
        let repaired =
            DashboardPersistence::initialize(&config, owner.root_context().component("contract-repaired-default"))
                .await
                .expect("valid default identity must restart after cleanup");
        drop(repaired);
    });
    owner.shutdown_runtime_blocking().expect("runtime owner shutdown");
}

enum DefaultIdentityCleanup {
    File { corrupt_snapshot: PathBuf },
    Sqlite { pool: SqlitePool, invalid_id: String },
    MySql { pool: MySqlPool, invalid_id: String },
    Postgres { pool: PgPool, invalid_id: String },
}

async fn corrupt_default_environment_identity(
    persistence: &DashboardPersistence,
    default_environment: &DashboardEnvironment,
    config: &StorageConfig,
) -> DefaultIdentityCleanup {
    let mut corrupt = default_environment.clone();
    corrupt.environment_id = EnvironmentId::new();
    let invalid_id = corrupt.environment_id.0.clone();
    match &persistence.backend {
        PersistenceBackend::File(store) => {
            store
                .write_snapshot(
                    "environments/default",
                    default_environment.revision.0 + 1,
                    serde_json::to_value(corrupt).expect("serialize corrupt default"),
                )
                .await
                .expect("write corrupt default snapshot");
            DefaultIdentityCleanup::File {
                corrupt_snapshot: config
                    .data_path
                    .join("environments/default/snapshots")
                    .join(format!("{:020}.json", default_environment.revision.0 + 1)),
            }
        }
        PersistenceBackend::Sql(store) => match store.storage_backend() {
            StorageBackend::Sqlite => {
                let pool = store.sqlite_pool().expect("SQLite pool").clone();
                sqlx::query("DELETE FROM dashboard_endpoint WHERE environment_id = ?")
                    .bind(&default_environment.environment_id.0)
                    .execute(&pool)
                    .await
                    .expect("remove default endpoints before corruption");
                sqlx::query("UPDATE dashboard_environment SET environment_id = ? WHERE environment_id = ?")
                    .bind(&invalid_id)
                    .bind(&default_environment.environment_id.0)
                    .execute(&pool)
                    .await
                    .expect("corrupt SQLite default identity");
                DefaultIdentityCleanup::Sqlite { pool, invalid_id }
            }
            StorageBackend::MySql => {
                let pool = store.mysql_pool().expect("MySQL pool").clone();
                sqlx::query("DELETE FROM dashboard_endpoint WHERE environment_id = ?")
                    .bind(&default_environment.environment_id.0)
                    .execute(&pool)
                    .await
                    .expect("remove default endpoints before corruption");
                sqlx::query("UPDATE dashboard_environment SET environment_id = ? WHERE environment_id = ?")
                    .bind(&invalid_id)
                    .bind(&default_environment.environment_id.0)
                    .execute(&pool)
                    .await
                    .expect("corrupt MySQL default identity");
                DefaultIdentityCleanup::MySql { pool, invalid_id }
            }
            StorageBackend::Postgres => {
                let pool = store.postgres_pool().expect("PostgreSQL pool").clone();
                sqlx::query("DELETE FROM dashboard_endpoint WHERE environment_id = $1")
                    .bind(&default_environment.environment_id.0)
                    .execute(&pool)
                    .await
                    .expect("remove default endpoints before corruption");
                sqlx::query("UPDATE dashboard_environment SET environment_id = $1 WHERE environment_id = $2")
                    .bind(&invalid_id)
                    .bind(&default_environment.environment_id.0)
                    .execute(&pool)
                    .await
                    .expect("corrupt PostgreSQL default identity");
                DefaultIdentityCleanup::Postgres { pool, invalid_id }
            }
            StorageBackend::File => unreachable!("SQL persistence cannot report the File backend"),
        },
    }
}

async fn cleanup_corrupted_default_environment(cleanup: DefaultIdentityCleanup) {
    match cleanup {
        DefaultIdentityCleanup::File { corrupt_snapshot } => {
            std::fs::remove_file(corrupt_snapshot).expect("remove corrupt File default snapshot");
        }
        DefaultIdentityCleanup::Sqlite { pool, invalid_id } => {
            sqlx::query("DELETE FROM dashboard_environment WHERE environment_id = ?")
                .bind(invalid_id)
                .execute(&pool)
                .await
                .expect("remove corrupt SQLite default");
        }
        DefaultIdentityCleanup::MySql { pool, invalid_id } => {
            sqlx::query("DELETE FROM dashboard_environment WHERE environment_id = ?")
                .bind(invalid_id)
                .execute(&pool)
                .await
                .expect("remove corrupt MySQL default");
        }
        DefaultIdentityCleanup::Postgres { pool, invalid_id } => {
            sqlx::query("DELETE FROM dashboard_environment WHERE environment_id = $1")
                .bind(invalid_id)
                .execute(&pool)
                .await
                .expect("remove corrupt PostgreSQL default");
        }
    }
}

fn assert_endpoint_order(endpoints: &[Endpoint]) {
    for pair in endpoints.windows(2) {
        let left = &pair[0];
        let right = &pair[1];
        let left_key = endpoint_sort_key(left);
        let right_key = endpoint_sort_key(right);
        assert!(
            left_key <= right_key,
            "endpoint order must be stable: {left_key:?} > {right_key:?}"
        );
    }
}

fn endpoint_sort_key(endpoint: &Endpoint) -> (u8, i32, &str) {
    (
        match endpoint.endpoint_type {
            EndpointType::Nameserver => 0,
            EndpointType::Proxy => 1,
        },
        endpoint.sort_order,
        &endpoint.endpoint_id.0,
    )
}
