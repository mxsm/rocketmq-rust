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

use std::sync::atomic::AtomicU64;

use rocketmq_dashboard_common::{AuditFilter, AuditQuery, MessageSearchQuery, OperationActor};
use rocketmq_dashboard_storage::{DashboardStore, DatabaseConnection, DesktopConfig, DesktopDocument, StorageOptions};
use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};

use super::*;
use crate::{
    infrastructure::auth_state::{LOGIN_PASSWORD_ENV, LOGIN_USERNAME_ENV, MapEnvironment},
    services::messages::{MessageRequestScope, test_support::FakeMessageBackend},
};

const USERNAME: &str = "native-session-test-operator";
const PASSWORD: &str = "session-test-password";

struct Clock(AtomicU64);
impl SessionClock for Clock {
    fn now(&self) -> Result<u64, UiError> {
        Ok(self.0.load(Ordering::Acquire))
    }
}

/// Server URLs must identify disposable dedicated databases, as for native storage contracts.
/// Two independently opened pools exercise the actual selected engine and shared authority.
fn exercise_native_session_clients(engine: StorageEngine, connection: String) {
    let directory = tempfile::tempdir().unwrap();
    let runtime = RuntimeOwner::plan(RuntimeConfig::for_parallelism("native-session-sql", 2))
        .unwrap()
        .with_memory_limit(ProcessMemoryLimit::configured(512 * 1024 * 1024).unwrap())
        .build()
        .unwrap();
    runtime.block_on(async {
        let options = || StorageOptions::Database {
            engine,
            connection: DatabaseConnection::new(connection.clone()),
        };
        let storage_a = runtime.root_context().component("storage-a");
        let storage_b = runtime.root_context().component("storage-b");
        let native_a = Arc::new(DashboardStore::open(options(), storage_a.clone()).await.unwrap());
        let native_b = Arc::new(DashboardStore::open(options(), storage_b.clone()).await.unwrap());
        if let Some(mut snapshot) = native_a.load_desktop().await.unwrap() {
            let mut config = snapshot.document.config().unwrap();
            config.auth.enabled = true;
            config.auth.credential_source = rocketmq_dashboard_common::CredentialSourceKind::None;
            config.scope = rocketmq_dashboard_common::ConnectionScope::NameServer;
            config.foundations.history_enabled = false;
            config.foundations.monitor_enabled = false;
            config.revision = config.revision.checked_add(1).unwrap();
            snapshot.document.apply_config(&config).unwrap();
            native_a
                .compare_exchange_desktop(snapshot.storage_revision, snapshot.document)
                .await
                .unwrap();
        } else {
            let mut document = DesktopDocument::from_legacy(DesktopConfig::default(), Vec::new()).unwrap();
            document.settings.authentication_enabled = true;
            native_a.initialize(document).await.unwrap();
        }
        // A repeated fixture run ends only its own old test operator sessions.
        native_a
            .revoke_all_sessions(USERNAME.into(), now_epoch_ms().unwrap())
            .await
            .unwrap();
        let config_a = DesktopConfigStore::with_native_storage(
            directory.path().join("a/config.json"),
            storage_a,
            native_a.clone(),
        );
        let config_b = DesktopConfigStore::with_native_storage(
            directory.path().join("b/config.json"),
            storage_b,
            native_b.clone(),
        );
        let auth = || {
            DesktopAuthState::new(Arc::new(MapEnvironment::new([
                (LOGIN_USERNAME_ENV, USERNAME),
                (LOGIN_PASSWORD_ENV, PASSWORD),
            ])))
        };
        let auth_a = auth();
        let auth_b = auth();
        let context_a = runtime.root_context().component("app-a");
        let context_b = runtime.root_context().component("app-b");
        let backend_a = Arc::new(FakeMessageBackend::default());
        let services_a = AppServices::desktop_with_fake_provider(
            config_a.clone(),
            auth_a.clone(),
            context_a.clone(),
            runtime.root_context().component("history-a"),
            runtime.root_context().component("monitor-a"),
        )
        .with_message_backend(backend_a.clone());
        let services_b = AppServices::desktop_with_fake_provider(
            config_b,
            auth_b.clone(),
            context_b.clone(),
            runtime.root_context().component("history-b"),
            runtime.root_context().component("monitor-b"),
        );
        services_a.bootstrap().await.unwrap();
        services_b.bootstrap().await.unwrap();
        services_a.authenticate(USERNAME, PASSWORD).await.unwrap();
        services_b.authenticate(USERNAME, PASSWORD).await.unwrap();
        let query = || SessionQuery {
            username: Some(USERNAME.into()),
            limit: 100,
            cursor: None,
        };
        let page_a = services_a.query_sessions(query()).await.unwrap();
        let page_b = services_b.query_sessions(query()).await.unwrap();
        assert_eq!(page_a.engine, engine);
        assert_eq!(page_b.engine, engine);
        assert_eq!(page_a.rows.iter().filter(|row| row.current).count(), 1);
        assert_eq!(page_b.rows.iter().filter(|row| row.current).count(), 1);
        let revoked = services_b
            .revoke_all_sessions(RevokeAllRequest {
                username: USERNAME.into(),
                confirmation: USERNAME.into(),
            })
            .await
            .unwrap();
        assert_eq!(revoked.revoked, 2);
        assert!(revoked.current_ended);
        assert!(!auth_b.session().is_authenticated());
        let scope = MessageRequestScope {
            revision: services_a.connection_state().config.revision,
            epoch: 1,
        };
        assert_eq!(
            services_a
                .query_messages(
                    scope,
                    MessageSearchQuery::Key {
                        topic: "orders".into(),
                        key: "key".into()
                    }
                )
                .await
                .unwrap_err()
                .code(),
            UiErrorCode::Authentication
        );
        assert!(
            backend_a.calls().query.is_empty(),
            "peer-revoked access must never reach the Broker backend"
        );
        assert!(!auth_a.session().is_authenticated());
        let document = native_a.load_desktop().await.unwrap().unwrap().document;
        let audit = document
            .audit
            .query(AuditQuery {
                filter: AuditFilter {
                    actor: Some(OperationActor::LocalOperator {
                        username: USERNAME.into(),
                    }),
                    action: Some(AuditAction::SessionRevokeAll),
                    ..AuditFilter::recent(now_epoch_ms().unwrap())
                },
                limit: 100,
                cursor: None,
            })
            .unwrap();
        assert!(
            audit
                .events
                .iter()
                .any(|event| event.outcome == AuditOutcome::Succeeded && event.facts.applied_targets == 2),
            "the first client must see the second client's terminal revocation event"
        );

        services_a.authenticate(USERNAME, PASSWORD).await.unwrap();
        let current = auth_a.session();
        let id = current.session_id().unwrap();
        let record = native_b
            .sessions(query())
            .await
            .unwrap()
            .sessions
            .into_iter()
            .find(|record| record.id == id)
            .unwrap();
        let clock = Arc::new(Clock(AtomicU64::new(record.created_at_epoch_ms + 1)));
        let validator = SessionService {
            config: config_a,
            auth: auth_a.clone(),
            context: runtime.root_context().component("expiry-validator"),
            clock: clock.clone(),
            polling_started: AtomicBool::new(false),
        };
        validator.validate().await.unwrap();
        let accessed = native_b
            .sessions(query())
            .await
            .unwrap()
            .sessions
            .into_iter()
            .find(|record| record.id == id)
            .unwrap();
        assert!(accessed.last_seen_at_epoch_ms > record.created_at_epoch_ms);
        assert_eq!(accessed.expires_at_epoch_ms, record.expires_at_epoch_ms);
        clock.0.store(record.expires_at_epoch_ms, Ordering::Release);
        assert_eq!(
            validator.validate().await.unwrap_err().code(),
            UiErrorCode::Authentication
        );
        assert!(!auth_a.session().is_authenticated());
        assert!(
            native_b
                .access_session(id, record.expires_at_epoch_ms)
                .await
                .unwrap()
                .is_none()
        );

        services_a.authenticate(USERNAME, PASSWORD).await.unwrap();
        native_a.close().await.unwrap();
        assert!(
            services_a
                .protected_access_for(services_a.current_session())
                .await
                .is_err()
        );
        assert!(!auth_a.session().is_authenticated());
        assert!(
            native_b.status().await.unwrap().initialized,
            "closing one client's pool must leave its peer authority available"
        );
        assert!(
            context_a
                .task_group()
                .shutdown(Duration::from_secs(5))
                .await
                .is_healthy()
        );
        assert!(
            context_b
                .task_group()
                .shutdown(Duration::from_secs(5))
                .await
                .is_healthy()
        );
        native_b
            .revoke_all_sessions(USERNAME.into(), now_epoch_ms().unwrap())
            .await
            .unwrap();
        native_b.close().await.unwrap();
    });
    assert!(runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}

#[cfg(feature = "storage-sqlite")]
#[test]
fn native_sqlite_two_client_session_revoke_expiry_and_disconnect() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sessions.sqlite3");
    exercise_native_session_clients(
        StorageEngine::Sqlite,
        format!("sqlite://{}", path.to_string_lossy().replace('\\', "/")),
    );
}

#[cfg(feature = "storage-mysql")]
#[test]
#[ignore = "requires a disposable dedicated database in ROCKETMQ_DASHBOARD_NATIVE_MYSQL_TEST_URL"]
fn native_mysql_two_client_session_revoke_expiry_and_disconnect() {
    exercise_native_session_clients(
        StorageEngine::MySql,
        std::env::var("ROCKETMQ_DASHBOARD_NATIVE_MYSQL_TEST_URL").expect("isolated native MySQL fixture required"),
    );
}

#[cfg(feature = "storage-postgres")]
#[test]
#[ignore = "requires a disposable dedicated database in ROCKETMQ_DASHBOARD_NATIVE_POSTGRES_TEST_URL"]
fn native_postgres_two_client_session_revoke_expiry_and_disconnect() {
    exercise_native_session_clients(
        StorageEngine::Postgres,
        std::env::var("ROCKETMQ_DASHBOARD_NATIVE_POSTGRES_TEST_URL")
            .expect("isolated native PostgreSQL fixture required"),
    );
}
