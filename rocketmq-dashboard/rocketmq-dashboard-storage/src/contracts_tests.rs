// Copyright 2026 The RocketMQ Rust Authors
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

use std::time::Duration;

use rocketmq_dashboard_common::{
    AuditAcceptedOperation, AuditAction, AuditFacts, AuditFilter, AuditOutcome, AuditQuery, EnvironmentId,
    HistoryMetricKind, HistoryPoint, HistoryRetention, MonitorThresholds, OperationActor, OperationContext,
    OperationResource,
};
use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};

use crate::*;

pub(crate) fn runtime() -> RuntimeOwner {
    RuntimeOwner::plan(RuntimeConfig::for_parallelism("native-storage-test", 2))
        .unwrap()
        .with_memory_limit(ProcessMemoryLimit::configured(512 * 1024 * 1024).unwrap())
        .build()
        .unwrap()
}

pub(crate) fn operation(document: &DesktopDocument, id: &str) -> AuditAcceptedOperation {
    AuditAcceptedOperation {
        context: OperationContext {
            environment_id: document.active_environment_id.clone(),
            configuration_revision: document.settings.revision,
            request_id: id.into(),
            actor: OperationActor::LocalOperator {
                username: "operator".into(),
            },
            resource: OperationResource::Topic("orders".into()),
        },
        action: AuditAction::TopicCreated,
        accepted_at_epoch_ms: 100,
        topic_fields: Default::default(),
        consumer_fields: Default::default(),
    }
}

fn point(timestamp: u64, value: f64) -> HistoryPoint {
    HistoryPoint {
        metric: HistoryMetricKind::TopicMessages,
        series_identity: "orders".into(),
        timestamp_epoch_ms: timestamp,
        value,
        source_revision: 7,
    }
}

pub(crate) async fn exercise_contract(store: &DashboardStore, peer: &DashboardStore) {
    let original = store.load_desktop().await.unwrap().unwrap();
    let mut accepted = original.document.clone();
    let operation = operation(&accepted, "request-accepted-native");
    accepted.audit.accept(operation.clone()).unwrap();
    let pending = store
        .compare_exchange_desktop(original.storage_revision, accepted)
        .await
        .unwrap();
    assert_eq!(pending.document.settings.revision, original.document.settings.revision);
    assert!(pending.storage_revision.value() > original.storage_revision.value());
    let mut stale = original.document;
    stale.settings.authentication_enabled = true;
    assert_eq!(
        peer.compare_exchange_desktop(original.storage_revision, stale)
            .await
            .unwrap_err()
            .kind(),
        StorageErrorKind::Conflict
    );
    let mut loaded = peer.load_desktop().await.unwrap().unwrap();
    assert_eq!(loaded.document.audit.pending(), &[operation.clone()]);
    assert!(!loaded.document.settings.authentication_enabled);

    let mut monitor = operation.context.clone();
    monitor.request_id = "request-monitor-native".into();
    monitor.resource = OperationResource::Monitor("orders-workers".into());
    loaded.document.monitors.rules.push(StoredMonitorRule {
        environment_id: Some(monitor.environment_id.clone()),
        rule: MonitorRule {
            id: "orders-workers".into(),
            consumer_group: "orders-workers".into(),
            enabled: true,
        },
        migration_state: MonitorMigrationState::Ready,
        thresholds: Some(MonitorThresholds {
            min_count: 1,
            max_diff_total: 500,
        }),
        revision: 1,
        updated_at_epoch_ms: Some(101),
    });
    loaded.document.monitors.revision_counter = 1;
    loaded
        .document
        .audit
        .finish_local(monitor, AuditAction::MonitorSaved, 101)
        .unwrap();
    let committed = peer
        .compare_exchange_desktop(loaded.storage_revision, loaded.document)
        .await
        .unwrap();
    let observed = store.load_desktop().await.unwrap().unwrap();
    assert_eq!(observed, committed);
    assert_eq!(observed.document.monitors.rules.len(), 1);
    let query = AuditQuery {
        filter: AuditFilter {
            start_epoch_ms: 0,
            end_epoch_ms: 200,
            actor: None,
            action: Some(AuditAction::MonitorSaved),
            outcome: None,
            environment_id: None,
        },
        cursor: None,
        limit: 10,
    };
    assert_eq!(observed.document.audit.query(query).unwrap().events.len(), 1);
    let mut terminal = observed.document;
    terminal
        .audit
        .finish(operation.clone(), AuditOutcome::Succeeded, AuditFacts::default(), 102)
        .unwrap();
    terminal
        .audit
        .finish(operation, AuditOutcome::Failed, AuditFacts::default(), 103)
        .unwrap();
    assert!(terminal.audit.pending().is_empty());
    let committed = store
        .compare_exchange_desktop(observed.storage_revision, terminal)
        .await
        .unwrap();

    // Two independent native clients observe one token. Exactly one complete journal wins.
    let mut left = committed.document.clone();
    let left_operation = operation_for(&left, "request-left-native");
    left.audit.accept(left_operation).unwrap();
    let mut right = committed.document.clone();
    let right_operation = operation_for(&right, "request-right-native");
    right.audit.accept(right_operation).unwrap();
    let (left, right) = tokio::join!(
        store.compare_exchange_desktop(committed.storage_revision, left),
        peer.compare_exchange_desktop(committed.storage_revision, right),
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    assert_eq!(
        left.err().or_else(|| right.err()).unwrap().kind(),
        StorageErrorKind::Conflict
    );
    assert_eq!(
        store
            .load_desktop()
            .await
            .unwrap()
            .unwrap()
            .document
            .audit
            .pending()
            .len(),
        1
    );

    let mut latest = peer.load_desktop().await.unwrap().unwrap();
    let first_environment = latest.document.active_environment_id.clone();
    let second_environment = EnvironmentId::parse("env-native-other").unwrap();
    latest.document.profiles.push(EnvironmentProfile::from_config(
        second_environment.clone(),
        "测试环境 Other".into(),
        &DesktopConfig::default(),
    ));
    peer.compare_exchange_desktop(latest.storage_revision, latest.document)
        .await
        .unwrap();
    let (first_grant, peer_grant) = tokio::join!(
        store.acquire_history_lease(
            first_environment.clone(),
            "native-first".into(),
            Duration::from_secs(600)
        ),
        peer.acquire_history_lease(
            first_environment.clone(),
            "native-competitor".into(),
            Duration::from_secs(600)
        ),
    );
    let first_grant = first_grant.unwrap();
    let peer_grant = peer_grant.unwrap();
    assert_eq!(
        usize::from(first_grant.is_some()) + usize::from(peer_grant.is_some()),
        1
    );
    let lease = first_grant.or(peer_grant).unwrap();
    assert!(peer
        .acquire_history_lease(
            first_environment.clone(),
            "native-peer".into(),
            Duration::from_secs(600)
        )
        .await
        .unwrap()
        .is_none());
    let retention = HistoryRetention {
        max_points_per_series: 2,
        max_series: 2,
        max_total_points: 3,
    };
    assert_eq!(
        store
            .append_history(
                lease.clone(),
                vec![point(1, 1.0), point(2, 2.0), point(3, 3.0)],
                retention
            )
            .await
            .unwrap(),
        HistoryAppendOutcome::Retained
    );
    assert_eq!(
        store
            .append_history(lease.clone(), vec![point(0, 0.0)], retention)
            .await
            .unwrap(),
        HistoryAppendOutcome::NotRetained
    );
    assert_eq!(
        store
            .append_history(lease.clone(), vec![point(4, f64::NAN)], retention)
            .await
            .unwrap(),
        HistoryAppendOutcome::NotRetained
    );
    let second_lease = peer
        .acquire_history_lease(
            second_environment.clone(),
            "native-other".into(),
            Duration::from_secs(600),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        peer.append_history(second_lease.clone(), vec![point(4, 4.0), point(5, 5.0)], retention)
            .await
            .unwrap(),
        HistoryAppendOutcome::Retained
    );
    let history = store.history().await.unwrap();
    assert_eq!(history.points.len(), 3);
    assert_eq!(
        history
            .points
            .iter()
            .filter(|point| point.environment_id.as_ref() == Some(&first_environment))
            .count(),
        1
    );
    assert_eq!(
        history
            .points
            .iter()
            .filter(|point| point.environment_id.as_ref() == Some(&second_environment))
            .count(),
        2
    );
    assert!(store.release_history_lease(lease.clone()).await.unwrap());
    let replacement = peer
        .acquire_history_lease(first_environment, "native-replacement".into(), Duration::from_secs(600))
        .await
        .unwrap()
        .unwrap();
    assert!(replacement.fencing_token() > lease.fencing_token());
    assert_eq!(
        store
            .append_history(lease.clone(), vec![point(6, 6.0)], retention)
            .await
            .unwrap_err()
            .kind(),
        StorageErrorKind::LeaseLost
    );
    assert!(store
        .renew_history_lease(lease.clone(), Duration::from_secs(600))
        .await
        .unwrap()
        .is_none());
    assert!(!store.release_history_lease(lease).await.unwrap());
    assert!(peer
        .renew_history_lease(replacement.clone(), Duration::from_secs(600))
        .await
        .unwrap()
        .is_some());
    assert_eq!(
        peer.append_history(
            second_lease.clone(),
            vec![point(6, 6.0)],
            HistoryRetention {
                max_points_per_series: 0,
                ..retention
            }
        )
        .await
        .unwrap(),
        HistoryAppendOutcome::NotRetained
    );
    assert!(store.history().await.unwrap().points.is_empty());
    assert!(store.status().await.unwrap().last_successful_write_epoch_ms.is_some());
    peer.release_history_lease(second_lease).await.unwrap();
    peer.release_history_lease(replacement).await.unwrap();

    let now = crate::store::now_ms().unwrap() as u64;
    let alice_first = SessionRecord::new("alice".into(), now, now + 60_000).unwrap();
    let alice_second = SessionRecord::new("alice".into(), now, now + 60_000).unwrap();
    let bob = SessionRecord::new("bob".into(), now, now + 60_000).unwrap();
    store.create_session(alice_first.clone(), now).await.unwrap();
    peer.create_session(alice_second.clone(), now).await.unwrap();
    peer.create_session(bob.clone(), now).await.unwrap();
    let first_page = store
        .sessions(SessionQuery {
            username: Some("alice".into()),
            limit: 1,
            cursor: None,
        })
        .await
        .unwrap();
    assert_eq!(first_page.sessions[0].id, alice_second.id);
    let alice_later = SessionRecord::new("alice".into(), now, now + 60_000).unwrap();
    peer.create_session(alice_later, now).await.unwrap();
    let second_page = peer
        .sessions(SessionQuery {
            username: Some("alice".into()),
            limit: 1,
            cursor: first_page.next_cursor,
        })
        .await
        .unwrap();
    assert_eq!(second_page.sessions[0].id, alice_first.id);
    assert!(second_page.next_cursor.is_none());
    assert_eq!(
        peer.access_session(alice_first.id.clone(), now + 1)
            .await
            .unwrap()
            .unwrap()
            .last_seen_at_epoch_ms,
        now + 1
    );
    assert_eq!(peer.revoke_all_sessions("alice".into(), now + 2).await.unwrap(), 3);
    assert!(store.access_session(alice_first.id, now + 3).await.unwrap().is_none());
    assert!(store.access_session(bob.id.clone(), now + 3).await.unwrap().is_some());
    assert!(store
        .access_session(bob.id.clone(), now + 60_000)
        .await
        .unwrap()
        .is_none());
    assert!(peer.revoke_session(bob.id.clone(), now + 4).await.unwrap());
    assert!(!store.revoke_session(bob.id, now + 5).await.unwrap());
}

fn operation_for(document: &DesktopDocument, request: &str) -> AuditAcceptedOperation {
    operation(document, request)
}

#[test]
fn file_contract_preserves_canonical_document_and_sessions_across_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime();
    runtime.block_on(async {
        let path = directory.path().join("config.json");
        let context = runtime.root_context().component("file");
        let store = DashboardStore::open(
            StorageOptions::File {
                config_path: path.clone(),
            },
            context.clone(),
        )
        .await
        .unwrap();
        exercise_contract(&store, &store).await;
        let document = store.load_desktop().await.unwrap().unwrap().document;
        store.close().await.unwrap();
        assert_eq!(store.load_desktop().await.unwrap_err().kind(), StorageErrorKind::Closed);
        let reopened = DashboardStore::open(
            StorageOptions::File {
                config_path: path.clone(),
            },
            context,
        )
        .await
        .unwrap();
        assert_eq!(reopened.load_desktop().await.unwrap().unwrap().document, document);
        assert_eq!(
            reopened.sessions(SessionQuery::default()).await.unwrap().sessions.len(),
            4
        );
        reopened.close().await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(value["schemaVersion"], 4);
        assert!(value.get("storageRevision").is_none());
        assert!(value.get("sessions").is_none());
    });
}

#[test]
fn legacy_migration_and_import_failures_preserve_unattributed_data_and_original_sources() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime();
    runtime.block_on(async {
        let path = directory.path().join("config.json");
        let mut legacy = DesktopConfig::default();
        legacy.schema_version = 1;
        legacy.revision = 8;
        let config_bytes = serde_json::to_vec(&legacy).unwrap();
        std::fs::write(&path, &config_bytes).unwrap();
        let history_bytes = serde_json::to_vec(&serde_json::json!({ "schemaVersion": 1, "points": [point(100, 2.5)] })).unwrap();
        std::fs::write(path.with_file_name("history.json"), &history_bytes).unwrap();
        let context = runtime.root_context().component("import");
        let imported = ImportSnapshot::read_files(path.clone(), context.clone()).await.unwrap();
        assert_eq!(imported.document.settings.revision, 8);
        assert!(imported.history.points[0].environment_id.is_none());
        assert_eq!(std::fs::read(&path).unwrap(), config_bytes);
        assert_eq!(std::fs::read(path.with_file_name("history.json")).unwrap(), history_bytes);
        std::fs::write(path.with_file_name("history.json"), b"{broken").unwrap();
        assert!(matches!(ImportSnapshot::read_files(path.clone(), context.clone()).await, Err(error) if error.kind() == StorageErrorKind::InvalidDocument));
        assert_eq!(std::fs::read(&path).unwrap(), config_bytes);
        std::fs::write(&path, br#"{"schemaVersion":999}"#).unwrap();
        assert!(matches!(DashboardStore::open(StorageOptions::File { config_path: path.clone() }, context).await, Err(error) if matches!(error.kind(), StorageErrorKind::UnsupportedSchema { found: 999, .. })));
        assert_eq!(std::fs::read(path).unwrap(), br#"{"schemaVersion":999}"#);
    });
}

#[test]
fn uncompiled_driver_returns_explicit_error_and_secret_input_is_not_formatted() {
    let runtime = runtime();
    runtime.block_on(async {
        for engine in [StorageEngine::Sqlite, StorageEngine::MySql, StorageEngine::Postgres]
            .into_iter()
            .filter(|engine| !engine.is_compiled())
        {
            let options = StorageOptions::Database {
                engine,
                connection: DatabaseConnection::new("user:SECRET-CREDENTIAL@unavailable".into()),
            };
            assert!(!format!("{options:?}").contains("SECRET-CREDENTIAL"));
            let error = match DashboardStore::open(options, runtime.root_context().component("unavailable")).await {
                Ok(_) => panic!("uncompiled driver must not fall back"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), StorageErrorKind::DriverUnavailable { engine });
            assert!(!format!("{error:?} {error}").contains("SECRET-CREDENTIAL"));
        }
        for engine in [StorageEngine::Sqlite, StorageEngine::MySql, StorageEngine::Postgres]
            .into_iter()
            .filter(|engine| engine.is_compiled())
        {
            let invalid = match engine {
                StorageEngine::Sqlite => "sqlite://:memory:?mode=not-a-mode&unknown=SECRET-CREDENTIAL",
                StorageEngine::MySql => "mysql://operator:SECRET-CREDENTIAL@127.0.0.1:not-a-port/native",
                StorageEngine::Postgres => "postgres://operator:SECRET-CREDENTIAL@127.0.0.1:not-a-port/native",
                StorageEngine::File => unreachable!(),
            };
            let error = match DashboardStore::open(
                StorageOptions::Database {
                    engine,
                    connection: DatabaseConnection::new(invalid.into()),
                },
                runtime.root_context().component("connection-failure"),
            )
            .await
            {
                Ok(_) => panic!("invalid connection must fail"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), StorageErrorKind::Connection);
            assert!(!format!("{error:?} {error}").contains("SECRET-CREDENTIAL"));
            assert!(std::error::Error::source(&error).is_none());
        }
    });
}

#[test]
fn file_writer_child() {
    let Some(path) = std::env::var_os("ROCKETMQ_NATIVE_STORAGE_WRITER_TEST_PATH") else {
        return;
    };
    let runtime = runtime();
    let result = runtime.block_on(DashboardStore::open(
        StorageOptions::File {
            config_path: path.into(),
        },
        runtime.root_context().component("child"),
    ));
    assert_eq!(
        result.is_err(),
        std::env::var_os("ROCKETMQ_NATIVE_STORAGE_EXPECT_LOCKED").is_some()
    );
}

#[test]
fn file_writer_is_exclusive_across_processes_and_release_allows_recovery() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    let runtime = runtime();
    let store = runtime
        .block_on(DashboardStore::open(
            StorageOptions::File {
                config_path: path.clone(),
            },
            runtime.root_context().component("owner"),
        ))
        .unwrap();
    let child = |locked: bool| {
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "contracts_tests::file_writer_child"])
            .env("ROCKETMQ_NATIVE_STORAGE_WRITER_TEST_PATH", &path);
        if locked {
            command.env("ROCKETMQ_NATIVE_STORAGE_EXPECT_LOCKED", "1");
        } else {
            command.env_remove("ROCKETMQ_NATIVE_STORAGE_EXPECT_LOCKED");
        }
        assert!(command.status().unwrap().success());
    };
    child(true);
    runtime.block_on(store.close()).unwrap();
    child(false);
}
