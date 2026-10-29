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

use crate::config::SqlPoolConfig;
use crate::config::StorageConfig;
use crate::model::AuditAction;
use crate::model::AuditActor;
use crate::model::AuditEvent;
use crate::model::AuditOutcome;
use crate::model::AuditResourceType;
use crate::model::ConsumerMonitorRule;
use crate::model::DashboardConfigView;
use crate::model::DashboardEnvironment;
use crate::model::NewSession;
use crate::model::SessionTokenHash;
use crate::model::StorageEngine;
use crate::persistence::DashboardPersistence;
use crate::persistence::Revision;
use crate::persistence::StorageMode;
use crate::persistence::StorageStatus;
use crate::persistence::audit_repository::AuditQuery;
use crate::persistence::session_repository::SessionQuery;
use rocketmq_runtime::RuntimeOwner;

#[test]
fn local_session_audit_contracts_reopen_file_and_sqlite() {
    let directory = tempfile::tempdir().expect("temporary storage directory");
    let owner = RuntimeOwner::new().expect("runtime owner");
    owner.block_on(async {
        for (index, backend) in [StorageEngine::File, StorageEngine::Sqlite].into_iter().enumerate() {
            let store = run_session_contract(
                &owner,
                StorageConfig {
                    backend,
                    data_path: directory.path().join(format!("session-audit-{index}")),
                    database_url: None,
                    pool: SqlPoolConfig::default(),
                },
                index as u8,
            )
            .await;
            if backend == StorageEngine::Sqlite {
                assert_session_audit_rollback(&store, "storage-contract-1").await;
            }
        }
    });
    owner.shutdown_runtime_blocking().expect("runtime shutdown");
}

#[test]
#[ignore = "requires fresh docker-compose.storage-test.yml volumes"]
fn docker_session_audit_contracts_cover_file_sqlite_mysql_and_postgres() {
    let owner = RuntimeOwner::new().expect("runtime owner");
    owner.block_on(async {
        let configs = [
            StorageConfig {
                backend: StorageEngine::File,
                data_path: std::env::var("ROCKETMQ_DASHBOARD_STORAGE_TEST_FILE_PATH")
                    .expect("ROCKETMQ_DASHBOARD_STORAGE_TEST_FILE_PATH")
                    .into(),
                database_url: None,
                pool: SqlPoolConfig::default(),
            },
            StorageConfig {
                backend: StorageEngine::Sqlite,
                data_path: std::env::var("ROCKETMQ_DASHBOARD_STORAGE_TEST_SQLITE_PATH")
                    .expect("ROCKETMQ_DASHBOARD_STORAGE_TEST_SQLITE_PATH")
                    .into(),
                database_url: None,
                pool: SqlPoolConfig::default(),
            },
            StorageConfig {
                backend: StorageEngine::MySql,
                data_path: "unused".into(),
                database_url: Some(
                    std::env::var("ROCKETMQ_DASHBOARD_STORAGE_TEST_MYSQL_URL")
                        .expect("ROCKETMQ_DASHBOARD_STORAGE_TEST_MYSQL_URL"),
                ),
                pool: SqlPoolConfig::default(),
            },
            StorageConfig {
                backend: StorageEngine::Postgres,
                data_path: "unused".into(),
                database_url: Some(
                    std::env::var("ROCKETMQ_DASHBOARD_STORAGE_TEST_POSTGRES_URL")
                        .expect("ROCKETMQ_DASHBOARD_STORAGE_TEST_POSTGRES_URL"),
                ),
                pool: SqlPoolConfig::default(),
            },
        ];
        for (index, config) in configs.into_iter().enumerate() {
            run_contract(&owner, config, index as u8).await;
        }
    });
    owner.shutdown_runtime_blocking().expect("runtime shutdown");
}

async fn run_session_contract(owner: &RuntimeOwner, config: StorageConfig, discriminator: u8) -> DashboardPersistence {
    let hash = SessionTokenHash([discriminator.saturating_add(1); 32]);
    let username = format!("storage-contract-{discriminator}");
    let store = DashboardPersistence::initialize(
        &config,
        owner.root_context().component("session-audit-storage-contract"),
    )
    .await
    .expect("initialize storage backend");
    let create = audit_event(AuditAction::SessionCreate, &username, 1);
    store
        .create_session_with_audit(
            NewSession {
                session_id: uuid::Uuid::now_v7().to_string(),
                token_hash: hash,
                username: username.clone(),
                created_at_ms: 1,
                expires_at_ms: 10_000,
            },
            create,
        )
        .await
        .expect("create session with audit");
    assert!(
        store
            .find_session(&hash)
            .await
            .unwrap_or_else(|error| panic!("find session for {:?}: {error:?}", config.backend))
            .is_some()
    );
    // Only network SQL engines support independent Backend instances. File's
    // directory lock and SQLite's single-node deployment are not replication.
    let peer = if matches!(config.backend, StorageEngine::MySql | StorageEngine::Postgres) {
        let peer = DashboardPersistence::initialize(
            &config,
            owner.root_context().component("session-audit-independent-peer"),
        )
        .await
        .expect("open independent SQL instance");
        assert_eq!(
            peer.find_session(&hash).await.expect("peer reads created session"),
            store.find_session(&hash).await.expect("primary reads created session")
        );
        Some(peer)
    } else {
        None
    };
    peer.as_ref()
        .unwrap_or(&store)
        .revoke_session_with_audit(&hash, 2, audit_event(AuditAction::SessionRevokeCurrent, &username, 2))
        .await
        .expect("revoke session with audit");
    assert!(
        store
            .find_session(&hash)
            .await
            .expect("find revoked session")
            .is_some_and(|session| session.revoked_at_ms == Some(2))
    );
    let page = store
        .query_audit_events(AuditQuery {
            start_ms: 0,
            end_ms: 10,
            actor: Some(username.clone()),
            action: None,
            outcome: None,
            environment_id: None,
            cursor: None,
            limit: 10,
        })
        .await
        .expect("query audit history");
    assert_eq!(page.events.len(), 2);
    if let Some(peer) = &peer {
        assert_eq!(
            peer.query_audit_events(audit_query(&username))
                .await
                .expect("peer reads shared audit history")
                .events,
            page.events
        );
    }
    let revoked_session = store.find_session(&hash).await.expect("session before reopen");
    drop(peer);
    drop(store);
    let store =
        DashboardPersistence::initialize(&config, owner.root_context().component("session-audit-storage-reopen"))
            .await
            .expect("reopen selected storage engine");
    assert_eq!(
        store.find_session(&hash).await.expect("session after reopen"),
        revoked_session
    );
    assert_eq!(
        store
            .query_audit_events(audit_query(&username))
            .await
            .expect("audit history after reopen")
            .events,
        page.events
    );
    let status = crate::service::storage_status(&store).await;
    assert_eq!(status.backend, config.backend);
    assert_eq!(status.status, StorageStatus::Available);
    assert_eq!(
        status.mode,
        match config.backend {
            StorageEngine::File | StorageEngine::Sqlite => StorageMode::SingleNode,
            StorageEngine::MySql | StorageEngine::Postgres => StorageMode::MultiNode,
        }
    );
    let status_json = serde_json::to_string(&status).expect("serialize safe status");
    assert!(!status_json.contains(config.data_path.to_string_lossy().as_ref()));
    if let Some(database_url) = &config.database_url {
        assert!(!status_json.contains(database_url));
    }
    assert!(!status_json.contains("databaseUrl"));
    assert!(!status_json.contains("dataPath"));
    store
}

async fn run_contract(owner: &RuntimeOwner, config: StorageConfig, discriminator: u8) {
    let username = format!("storage-contract-{discriminator}");
    let store = run_session_contract(owner, config.clone(), discriminator).await;
    // The public list is capped at 200 records, but cleanup and revoke-all
    // must retain their complete semantic scope across a larger account.
    // This also proves the File transaction marker no longer inherits the
    // former 32-transition limit.
    for index in 10_u8..250 {
        store
            .create_session(NewSession {
                session_id: uuid::Uuid::now_v7().to_string(),
                token_hash: SessionTokenHash([index; 32]),
                username: username.clone(),
                created_at_ms: 1,
                expires_at_ms: if index < 43 { 10_000 } else { 2 },
            })
            .await
            .unwrap_or_else(|error| panic!("seed session {index} for {:?}: {error:?}", config.backend));
    }
    let first_page = store
        .list_sessions(SessionQuery {
            username: Some(username.clone()),
            cursor: None,
            limit: 200,
        })
        .await
        .expect("first 200-session page");
    assert_eq!(first_page.records.len(), 200);
    let second_page = store
        .list_sessions(SessionQuery {
            username: Some(username.clone()),
            cursor: first_page.next_cursor,
            limit: 200,
        })
        .await
        .expect("remaining session page");
    assert_eq!(first_page.records.len() + second_page.records.len(), 241);
    assert_eq!(
        store
            .delete_sessions_before(3, 500)
            .await
            .expect("delete more than 32 expired sessions"),
        208
    );
    assert_eq!(
        store
            .revoke_all_sessions_with_audit(&username, 3, audit_event(AuditAction::SessionRevokeAll, &username, 3),)
            .await
            .expect("revoke more than 32 active sessions"),
        33
    );
    let revoke_all_page = store
        .query_audit_events(AuditQuery {
            start_ms: 0,
            end_ms: 10,
            actor: Some(username.clone()),
            action: Some(AuditAction::SessionRevokeAll),
            outcome: Some(AuditOutcome::Succeeded),
            environment_id: None,
            cursor: None,
            limit: 10,
        })
        .await
        .expect("query revoke-all audit");
    assert_eq!(revoke_all_page.events.len(), 1);

    // Configuration and monitor mutations must publish their durable state
    // and successful audit event together for every storage backend. The
    // mutable application configuration is intentionally not involved here:
    // repository success is the publish boundary exercised by the services.
    let mut environment = DashboardEnvironment::bootstrap(&DashboardConfigView::default(), 10);
    environment.environment_id = crate::model::EnvironmentId::new();
    environment.name = format!("audit-contract-environment-{discriminator}");
    let environment = store
        .create_environment(environment)
        .await
        .expect("create environment for atomic configuration audit");
    let mut updated_environment = environment.clone();
    updated_environment.use_tls = !updated_environment.use_tls;
    updated_environment.updated_at_ms = 11;
    let updated_environment = store
        .update_environment_with_audit(
            environment.revision,
            updated_environment,
            resource_audit_event(
                AuditAction::ConfigTlsSet,
                AuditResourceType::Environment,
                &username,
                Some(environment.environment_id.clone()),
                11,
            ),
        )
        .await
        .expect("update environment and append audit in one transaction");
    assert_eq!(
        store
            .load_environment(&environment.environment_id)
            .await
            .expect("load atomically updated environment"),
        updated_environment
    );
    assert_eq!(
        store
            .query_audit_events(AuditQuery {
                start_ms: 0,
                end_ms: 20,
                actor: Some(username.clone()),
                action: Some(AuditAction::ConfigTlsSet),
                outcome: Some(AuditOutcome::Succeeded),
                environment_id: Some(environment.environment_id.0.clone()),
                cursor: None,
                limit: 10,
            })
            .await
            .expect("query configuration audit")
            .events
            .len(),
        1
    );

    let monitor_group = format!("audit-contract-monitor-{discriminator}");
    let monitor = store
        .upsert_monitor_rule_with_audit(
            ConsumerMonitorRule {
                environment_id: environment.environment_id.clone(),
                consumer_group: monitor_group.clone(),
                min_count: 1,
                max_diff_total: 10,
                revision: Revision(0),
                created_at_ms: 0,
                updated_at_ms: 0,
            },
            Revision(0),
            resource_audit_event(
                AuditAction::MonitorUpsert,
                AuditResourceType::Monitor,
                &username,
                Some(environment.environment_id.clone()),
                12,
            ),
        )
        .await
        .expect("upsert monitor and append audit in one transaction");
    assert_eq!(
        store
            .list_monitor_rules(&environment.environment_id)
            .await
            .expect("list atomically created monitor"),
        vec![monitor.clone()]
    );
    assert!(
        store
            .delete_monitor_rule_with_audit(
                &environment.environment_id,
                &monitor_group,
                monitor.revision,
                resource_audit_event(
                    AuditAction::MonitorDelete,
                    AuditResourceType::Monitor,
                    &username,
                    Some(environment.environment_id.clone()),
                    13,
                ),
            )
            .await
            .expect("delete monitor and append audit in one transaction")
    );
    assert!(
        store
            .list_monitor_rules(&environment.environment_id)
            .await
            .expect("list atomically deleted monitor")
            .is_empty()
    );
    for action in [AuditAction::MonitorUpsert, AuditAction::MonitorDelete] {
        assert_eq!(
            store
                .query_audit_events(AuditQuery {
                    start_ms: 0,
                    end_ms: 20,
                    actor: Some(username.clone()),
                    action: Some(action),
                    outcome: Some(AuditOutcome::Succeeded),
                    environment_id: Some(environment.environment_id.0.clone()),
                    cursor: None,
                    limit: 10,
                })
                .await
                .expect("query monitor audit")
                .events
                .len(),
            1
        );
    }

    // A duplicate primary key makes the audit insert fail after the SQL
    // transaction has staged its aggregate write. File storage uses the
    // prepared-marker interruption test below instead: a journal is
    // append-only there and intentionally does not reject duplicate IDs.
    // Exercise this failure mode for SQLite, MySQL, and Postgres so each
    // backend proves that neither configuration nor monitor state escapes
    // without its successful audit event.
    if !matches!(config.backend, StorageEngine::File) {
        assert_session_audit_rollback(&store, &username).await;
        let duplicate_configuration_audit = resource_audit_event(
            AuditAction::ConfigTlsSet,
            AuditResourceType::Environment,
            &username,
            Some(environment.environment_id.clone()),
            14,
        );
        store
            .append_audit_event(duplicate_configuration_audit.clone())
            .await
            .expect("seed duplicate configuration audit id");
        let mut rejected_environment = updated_environment.clone();
        rejected_environment.use_tls = !rejected_environment.use_tls;
        rejected_environment.updated_at_ms = 14;
        assert!(
            store
                .update_environment_with_audit(
                    updated_environment.revision,
                    rejected_environment,
                    duplicate_configuration_audit,
                )
                .await
                .is_err(),
            "duplicate configuration audit must roll back {:?} environment state",
            config.backend
        );
        assert_eq!(
            store
                .load_environment(&environment.environment_id)
                .await
                .expect("configuration audit failure must retain prior environment"),
            updated_environment
        );

        let rejected_monitor_group = format!("audit-contract-rejected-monitor-{discriminator}");
        let duplicate_monitor_audit = resource_audit_event(
            AuditAction::MonitorUpsert,
            AuditResourceType::Monitor,
            &username,
            Some(environment.environment_id.clone()),
            15,
        );
        store
            .append_audit_event(duplicate_monitor_audit.clone())
            .await
            .expect("seed duplicate monitor audit id");
        assert!(
            store
                .upsert_monitor_rule_with_audit(
                    ConsumerMonitorRule {
                        environment_id: environment.environment_id.clone(),
                        consumer_group: rejected_monitor_group,
                        min_count: 1,
                        max_diff_total: 10,
                        revision: Revision(0),
                        created_at_ms: 0,
                        updated_at_ms: 0,
                    },
                    Revision(0),
                    duplicate_monitor_audit,
                )
                .await
                .is_err(),
            "duplicate monitor audit must roll back {:?} monitor state",
            config.backend
        );
        assert!(
            store
                .list_monitor_rules(&environment.environment_id)
                .await
                .expect("monitor audit failure must retain prior rules")
                .is_empty()
        );
    }
}

fn audit_query(username: &str) -> AuditQuery {
    AuditQuery {
        start_ms: 0,
        end_ms: 10,
        actor: Some(username.to_string()),
        action: None,
        outcome: None,
        environment_id: None,
        cursor: None,
        limit: 10,
    }
}

async fn assert_session_audit_rollback(store: &DashboardPersistence, username: &str) {
    let hash = SessionTokenHash([250; 32]);
    let duplicate_create_audit = audit_event(AuditAction::SessionCreate, username, 16);
    store
        .append_audit_event(duplicate_create_audit.clone())
        .await
        .expect("seed duplicate session-create audit id");
    let session = NewSession {
        session_id: uuid::Uuid::now_v7().to_string(),
        token_hash: hash,
        username: username.to_string(),
        created_at_ms: 16,
        expires_at_ms: 10_000,
    };
    assert!(
        store
            .create_session_with_audit(session.clone(), duplicate_create_audit)
            .await
            .is_err()
    );
    assert!(
        store
            .find_session(&hash)
            .await
            .expect("rolled-back session create")
            .is_none()
    );

    store
        .create_session_with_audit(session, audit_event(AuditAction::SessionCreate, username, 17))
        .await
        .expect("create session before revoke rollback");
    let active_session = store
        .find_session(&hash)
        .await
        .expect("active session before failed revoke");
    let duplicate_revoke_audit = audit_event(AuditAction::SessionRevokeCurrent, username, 18);
    store
        .append_audit_event(duplicate_revoke_audit.clone())
        .await
        .expect("seed duplicate session-revoke audit id");
    assert!(
        store
            .revoke_session_with_audit(&hash, 18, duplicate_revoke_audit)
            .await
            .is_err()
    );
    assert_eq!(
        store.find_session(&hash).await.expect("session after failed revoke"),
        active_session
    );
    assert!(active_session.is_some_and(|session| session.revoked_at_ms.is_none()));
}

fn audit_event(action: AuditAction, username: &str, created_at_ms: i64) -> AuditEvent {
    resource_audit_event(action, AuditResourceType::Session, username, None, created_at_ms)
}

fn resource_audit_event(
    action: AuditAction,
    resource_type: AuditResourceType,
    username: &str,
    environment_id: Option<crate::model::EnvironmentId>,
    created_at_ms: i64,
) -> AuditEvent {
    AuditEvent {
        event_id: uuid::Uuid::now_v7().to_string(),
        request_id: uuid::Uuid::now_v7().to_string(),
        actor: AuditActor::admin(username),
        action,
        resource_type,
        resource_name: Some(username.to_string()),
        environment_id,
        outcome: AuditOutcome::Succeeded,
        detail: None,
        created_at_ms,
    }
}
