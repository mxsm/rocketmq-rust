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

use rocketmq_dashboard_storage::{DashboardStore, StorageOptions};
use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};

use super::*;
use crate::infrastructure::auth_state::{LOGIN_PASSWORD_ENV, LOGIN_USERNAME_ENV, MapEnvironment};

pub(crate) struct TestClock(AtomicU64);

impl SessionClock for TestClock {
    fn now(&self) -> Result<u64, UiError> {
        Ok(self.0.load(Ordering::Acquire))
    }
}

pub(crate) struct Fixture {
    _directory: tempfile::TempDir,
    pub runtime: RuntimeOwner,
    pub config: Arc<DesktopConfigStore>,
    pub native: Arc<DashboardStore>,
    pub service: Arc<SessionService>,
    pub auth: Arc<DesktopAuthState>,
    pub clock: Arc<TestClock>,
}

impl Fixture {
    pub fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let runtime = RuntimeOwner::plan(RuntimeConfig::for_parallelism("native-session-test", 2))
            .unwrap()
            .with_memory_limit(ProcessMemoryLimit::configured(256 * 1024 * 1024).unwrap())
            .build()
            .unwrap();
        let storage_context = runtime.root_context().component("storage");
        let path = directory.path().join("config.json");
        let native = Arc::new(
            runtime
                .block_on(DashboardStore::open(
                    StorageOptions::File {
                        config_path: path.clone(),
                    },
                    storage_context.clone(),
                ))
                .unwrap(),
        );
        runtime.block_on(async {
            let mut snapshot = native.load_desktop().await.unwrap().unwrap();
            snapshot.document.settings.authentication_enabled = true;
            native
                .compare_exchange_desktop(snapshot.storage_revision, snapshot.document)
                .await
                .unwrap();
        });
        let config = DesktopConfigStore::with_native_storage(path, storage_context, native.clone());
        runtime.block_on(config.load()).unwrap();
        let auth = DesktopAuthState::new(Arc::new(MapEnvironment::new([
            (LOGIN_USERNAME_ENV, "operator"),
            (LOGIN_PASSWORD_ENV, "private-password"),
        ])));
        let clock = Arc::new(TestClock(AtomicU64::new(now_epoch_ms().unwrap())));
        let service = Arc::new(SessionService {
            config: config.clone(),
            auth: auth.clone(),
            context: runtime.root_context().component("sessions"),
            clock: clock.clone(),
            polling_started: AtomicBool::new(false),
        });
        Self {
            _directory: directory,
            runtime,
            config,
            native,
            service,
            auth,
            clock,
        }
    }

    pub fn login(&self) -> SessionState {
        self.runtime
            .block_on(
                self.service
                    .authenticate(self.service.capture_login(0), "operator", "private-password"),
            )
            .unwrap()
    }

    pub fn wait_for_terminal(&self, started: tokio::sync::oneshot::Receiver<()>, cx: &mut gpui::VisualTestContext) {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.service
            .context
            .spawn("session-test-terminal-wait", TaskKind::Other, async move {
                let _ = sender.send(tokio::time::timeout(Duration::from_secs(5), started).await);
            })
            .unwrap();
        // Keep GPUI running through admission while Tokio owns the real timeout and IO.
        cx.background_executor.allow_parking();
        cx.background_executor.block_test(receiver).unwrap().unwrap().unwrap();
    }

    pub fn finish(self) {
        self.runtime.block_on(async {
            let report = self.service.context.task_group().shutdown(Duration::from_secs(5)).await;
            assert!(
                report.is_healthy(),
                "session work must cancel and finish before storage closes: {report:?}"
            );
            self.native.close().await.unwrap();
        });
        self.runtime.shutdown_runtime_blocking().unwrap();
    }
}

#[test]
fn native_session_login_persists_absolute_expiry_and_access_then_expires_without_extending_it() {
    let fixture = Fixture::new();
    let signed_in = fixture.login();
    let first = fixture
        .runtime
        .block_on(fixture.native.sessions(SessionQuery::default()))
        .unwrap()
        .sessions
        .remove(0);
    assert!(signed_in.is_authenticated());
    assert_eq!(
        first.expires_at_epoch_ms - first.created_at_epoch_ms,
        SESSION_TTL.as_millis() as u64
    );
    fixture.clock.0.store(first.created_at_epoch_ms + 42, Ordering::Release);
    fixture.runtime.block_on(fixture.service.validate()).unwrap();
    let accessed = fixture
        .runtime
        .block_on(fixture.native.sessions(SessionQuery::default()))
        .unwrap()
        .sessions
        .remove(0);
    assert_eq!(accessed.last_seen_at_epoch_ms, first.created_at_epoch_ms + 42);
    assert_eq!(accessed.expires_at_epoch_ms, first.expires_at_epoch_ms);
    let page = fixture
        .runtime
        .block_on(
            fixture
                .service
                .query(SessionQuery::default(), fixture.service.snapshot()),
        )
        .unwrap();
    assert_eq!(page.rows.iter().filter(|row| row.current).count(), 1);
    let capability = String::from(first.id.clone());
    let diagnostic = format!("{signed_in:?} {:?} {first:?} {page:?}", fixture.auth.session());
    assert!(!diagnostic.contains(&capability));
    assert!(!diagnostic.contains("private-password"));
    fixture.clock.0.store(first.expires_at_epoch_ms, Ordering::Release);
    assert_eq!(
        fixture.runtime.block_on(fixture.service.validate()).unwrap_err().code(),
        UiErrorCode::Authentication
    );
    assert!(!fixture.auth.session().is_authenticated());
    fixture.finish();
}

#[test]
fn native_session_delayed_logout_revokes_only_its_captured_session_and_replacement_changes_identity() {
    let fixture = Fixture::new();
    let first = fixture.login();
    let old = fixture.service.end_local();
    assert!(!fixture.auth.session().is_authenticated());
    let replacement = fixture.login();
    assert_ne!(
        first, replacement,
        "a same-username replacement is a different local authority"
    );
    fixture
        .runtime
        .block_on(fixture.service.revoke_captured(old.clone()))
        .unwrap();
    assert_eq!(fixture.service.snapshot(), replacement);
    fixture.runtime.block_on(fixture.service.validate()).unwrap();
    assert!(!fixture.auth.sign_out_if_current(&old));
    let rows = fixture
        .runtime
        .block_on(fixture.native.sessions(SessionQuery::default()))
        .unwrap()
        .sessions;
    assert_eq!(rows.iter().filter(|row| row.revoked_at_epoch_ms.is_some()).count(), 1);
    assert_eq!(
        rows.iter()
            .filter(|row| row.is_active(fixture.clock.now().unwrap()))
            .count(),
        1
    );
    fixture.finish();
}

#[test]
fn native_session_a_login_captured_before_logout_cannot_install_later() {
    let fixture = Fixture::new();
    let captured = fixture.service.capture_login(0);
    fixture.service.end_local();
    let error = fixture
        .runtime
        .block_on(fixture.service.authenticate(captured, "operator", "private-password"))
        .unwrap_err();
    assert_eq!(error.code(), UiErrorCode::Authentication);
    assert!(!fixture.auth.session().is_authenticated());
    assert!(
        fixture
            .runtime
            .block_on(fixture.native.sessions(SessionQuery::default()))
            .unwrap()
            .sessions
            .is_empty()
    );
    fixture.finish();
}

#[test]
fn native_session_logout_revokes_the_record_even_when_peer_configuration_makes_audit_capture_stale() {
    let fixture = Fixture::new();
    let context = fixture.runtime.root_context().component("stale-logout");
    let services = AppServices::desktop_with_fake_provider(
        fixture.config.clone(),
        fixture.auth.clone(),
        context.clone(),
        fixture.runtime.root_context().component("logout-history"),
        fixture.runtime.root_context().component("logout-monitor"),
    );
    fixture.runtime.block_on(async {
        services.bootstrap().await.unwrap();
        fixture
            .service
            .authenticate(fixture.service.capture_login(0), "operator", "private-password")
            .await
            .unwrap();
        let id = fixture.auth.session().session_id().unwrap();
        let peer = DesktopConfigStore::with_native_storage(
            fixture.config.path().to_path_buf(),
            fixture.runtime.root_context().component("peer-config"),
            fixture.native.clone(),
        );
        let mut config = peer.load().await.unwrap();
        config.transport.use_tls = !config.transport.use_tls;
        assert_eq!(peer.save_next(config).await.unwrap().revision, 1);
        assert_eq!(fixture.config.load().await.unwrap().revision, 1);
        assert_eq!(services.connection_state().config.revision, 0);

        let logout = services.sign_out();
        assert!(
            !fixture.auth.session().is_authenticated(),
            "local access ends immediately"
        );
        assert!(
            logout.await.is_err(),
            "stale Audit attribution remains an explicit warning"
        );
        assert!(
            fixture
                .native
                .access_session(id, fixture.clock.now().unwrap())
                .await
                .unwrap()
                .is_none(),
            "Audit capture failure must not leave the captured persistent record usable"
        );
        assert!(context.task_group().shutdown(Duration::from_secs(5)).await.is_healthy());
    });
    fixture.finish();
}

#[test]
fn native_session_query_is_username_filtered_bounded_and_marks_only_the_current_record() {
    let fixture = Fixture::new();
    fixture.login();
    fixture.runtime.block_on(async {
        let now = fixture.clock.now().unwrap();
        for _ in 0..26 {
            fixture
                .native
                .create_session(SessionRecord::new("other".into(), now, now + 60_000).unwrap(), now)
                .await
                .unwrap();
        }
        let first = fixture
            .service
            .query(
                SessionQuery {
                    username: Some("other".into()),
                    cursor: None,
                    limit: 25,
                },
                fixture.service.snapshot(),
            )
            .await
            .unwrap();
        assert_eq!(first.rows.len(), 25);
        assert!(first.rows.iter().all(|row| row.username == "other" && !row.current));
        fixture
            .native
            .create_session(SessionRecord::new("other".into(), now, now + 60_000).unwrap(), now)
            .await
            .unwrap();
        let second = fixture
            .service
            .query(
                SessionQuery {
                    username: Some("other".into()),
                    cursor: first.next_cursor,
                    limit: 25,
                },
                fixture.service.snapshot(),
            )
            .await
            .unwrap();
        assert_eq!(
            second.rows.len(),
            1,
            "later creations must not enter the existing cursor window"
        );
        assert!(second.next_cursor.is_none());
        assert!(
            fixture
                .service
                .revoke_all(RevokeAllRequest {
                    username: "other".into(),
                    confirmation: "operator".into()
                })
                .await
                .is_err()
        );
        assert_eq!(
            fixture
                .native
                .sessions(SessionQuery {
                    username: Some("other".into()),
                    ..SessionQuery::default()
                })
                .await
                .unwrap()
                .sessions
                .iter()
                .filter(|record| record.is_active(now))
                .count(),
            27
        );
        let result = fixture
            .service
            .revoke_all(RevokeAllRequest {
                username: "other".into(),
                confirmation: "other".into(),
            })
            .await
            .unwrap();
        assert_eq!(result.revoked, 27);
        assert!(!result.current_ended);
        assert!(fixture.auth.session().is_authenticated());
    });
    fixture.finish();
}

#[test]
fn native_session_owned_poll_observes_revoke_and_shutdown_finishes_before_authority_close() {
    let fixture = Fixture::new();
    fixture.login();
    let mut changes = fixture.auth.changes();
    fixture.runtime.block_on(async {
        fixture
            .native
            .revoke_all_sessions("operator".into(), fixture.clock.now().unwrap())
            .await
            .unwrap();
        fixture.service.start().unwrap();
        tokio::time::timeout(Duration::from_secs(5), changes.changed())
            .await
            .unwrap()
            .unwrap();
        assert!(!fixture.auth.session().is_authenticated());
    });
    fixture.finish();
}

#[test]
fn native_session_storage_failure_fails_closed_and_does_not_create_anonymous_operator_access() {
    let fixture = Fixture::new();
    fixture.login();
    fixture.runtime.block_on(fixture.native.close()).unwrap();
    assert!(fixture.runtime.block_on(fixture.service.validate()).is_err());
    assert!(!fixture.auth.session().is_authenticated());
    assert!(
        fixture
            .runtime
            .block_on(
                fixture
                    .service
                    .query(SessionQuery::default(), fixture.service.snapshot())
            )
            .is_err()
    );
    fixture.finish();
}

#[test]
fn native_session_queued_owner_never_validates_or_ends_a_replacement() {
    let fixture = Fixture::new();
    let context = fixture.runtime.root_context().component("queued-session-owner");
    let services = AppServices::desktop_with_fake_provider(
        fixture.config.clone(),
        fixture.auth.clone(),
        context.clone(),
        fixture.runtime.root_context().component("queued-history"),
        fixture.runtime.root_context().component("queued-monitor"),
    );
    fixture.runtime.block_on(services.bootstrap()).unwrap();
    let old = fixture.login();
    let replacement = fixture.login();
    assert_ne!(old, replacement);
    fixture.runtime.block_on(async {
        fixture.native.close().await.unwrap();
        assert_eq!(
            services
                .protected_access_for(Some(old.clone()))
                .await
                .unwrap_err()
                .code(),
            UiErrorCode::Authentication
        );
        assert!(
            fixture
                .service
                .query(SessionQuery::default(), old.clone())
                .await
                .is_err()
        );
        assert!(fixture.service.validate_marker(&old).await.is_err());
        assert_eq!(fixture.service.snapshot(), replacement);
        assert!(fixture.auth.session().is_authenticated());

        // A current action still fails closed when its authority cannot be verified.
        assert!(services.protected_access_for(Some(replacement)).await.is_err());
        assert!(!fixture.auth.session().is_authenticated());
        assert!(context.task_group().shutdown(Duration::from_secs(5)).await.is_healthy());
    });
    fixture.finish();
}

#[test]
fn native_session_replacement_rejects_a_query_response_captured_by_the_previous_login() {
    use crate::services::messages::{
        MessageRequestScope,
        test_support::{FakeMessageBackend, page},
    };
    use rocketmq_dashboard_common::MessageSearchQuery;

    let fixture = Fixture::new();
    let context = fixture.runtime.root_context().component("query-admission");
    let backend = Arc::new(FakeMessageBackend::default());
    let services = AppServices::desktop_with_fake_provider(
        fixture.config.clone(),
        fixture.auth.clone(),
        context.clone(),
        fixture.runtime.root_context().component("history"),
        fixture.runtime.root_context().component("monitor"),
    )
    .with_message_backend(backend.clone());
    fixture.runtime.block_on(async {
        services.bootstrap().await.unwrap();
        fixture.service.authenticate(fixture.service.capture_login(0), "operator", "private-password").await.unwrap();
        backend.queue_query(Ok(page()));
        let (started, release, _) = backend.hold_next_query();
        let mut query = Box::pin(services.query_messages(MessageRequestScope { revision: 0, epoch: 1 }, MessageSearchQuery::Key { topic: "orders".into(), key: "key".into() }));
        tokio::select! { result = &mut query => panic!("query finished before its backend gate: {result:?}"), result = started => result.unwrap() }
        fixture.service.authenticate(fixture.service.capture_login(0), "operator", "private-password").await.unwrap();
        release.send(()).unwrap();
        assert_eq!(query.await.unwrap_err().code(), UiErrorCode::Authentication);
        assert!(fixture.auth.session().is_authenticated());
        assert_eq!(backend.calls().query.len(), 1);
        assert!(context.task_group().shutdown(Duration::from_secs(5)).await.is_healthy());
    });
    fixture.finish();
}

#[test]
fn native_session_self_revoke_denies_new_work_but_keeps_an_already_accepted_mutation_and_its_actor() {
    use crate::services::messages::{
        MessageRequestScope,
        test_support::{FakeMessageBackend, row},
    };
    use rocketmq_dashboard_common::{
        AuditFilter, AuditQuery, ConnectionScope, MessageConsumeCode, MessageConsumeCommand, MessageConsumeReceipt,
        MessageSearchQuery, OperationActor,
    };

    let fixture = Fixture::new();
    let context = fixture.runtime.root_context().component("mutation-admission");
    let backend = Arc::new(FakeMessageBackend::default());
    let services = AppServices::desktop_with_fake_provider(
        fixture.config.clone(),
        fixture.auth.clone(),
        context.clone(),
        fixture.runtime.root_context().component("history"),
        fixture.runtime.root_context().component("monitor"),
    )
    .with_message_backend(backend.clone());
    fixture.runtime.block_on(async {
        services.bootstrap().await.unwrap();
        fixture.service.authenticate(fixture.service.capture_login(0), "operator", "private-password").await.unwrap();
        backend.queue_consume(Ok(MessageConsumeReceipt { code: MessageConsumeCode::Success, reported_success: true, remark_returned: false }));
        let (started, release, _) = backend.hold_next_consume();
        let scope = MessageRequestScope { revision: 0, epoch: 1 };
        let mut consume = Box::pin(services.consume_message(scope, MessageConsumeCommand { identity: row().identity, consumer_group: "workers".into(), client_id: None, scope: ConnectionScope::NameServer }));
        tokio::select! { result = &mut consume => panic!("consume finished before its backend gate: {result:?}"), result = started => result.unwrap() }
        services.revoke_all_sessions(RevokeAllRequest { username: "operator".into(), confirmation: "operator".into() }).await.unwrap();
        assert!(!fixture.auth.session().is_authenticated());
        assert_eq!(services.query_messages(scope, MessageSearchQuery::Key { topic: "orders".into(), key: "key".into() }).await.unwrap_err().code(), UiErrorCode::Authentication);
        assert!(backend.calls().query.is_empty());
        drop(consume);
        release.send(()).unwrap();
        assert!(context.task_group().shutdown(Duration::from_secs(5)).await.is_healthy());
        let document = fixture.native.load_desktop().await.unwrap().unwrap().document;
        let events = document.audit.query(AuditQuery { filter: AuditFilter { action: Some(AuditAction::MessageConsumed), ..AuditFilter::recent(now_epoch_ms().unwrap()) }, limit: 25, cursor: None }).unwrap().events;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].outcome, AuditOutcome::Succeeded);
        assert_eq!(events[0].operation.context.actor, OperationActor::LocalOperator { username: "operator".into() });
        assert_eq!(backend.calls().consume.len(), 1);
    });
    fixture.finish();
}

#[test]
fn native_session_history_sampling_revalidates_before_each_background_request() {
    use std::sync::atomic::AtomicUsize;

    use crate::{
        infrastructure::history_collector::{HistorySampleFuture, HistorySampler},
        services::sessions_history::ProtectedHistorySampler,
    };

    struct CountingSampler(AtomicUsize);

    impl HistorySampler for CountingSampler {
        fn sample_topics(&self) -> HistorySampleFuture<'_> {
            Box::pin(async {
                self.0.fetch_add(1, Ordering::AcqRel);
                Ok(Vec::new())
            })
        }

        fn sample_brokers(&self) -> HistorySampleFuture<'_> {
            Box::pin(async {
                self.0.fetch_add(1, Ordering::AcqRel);
                Ok(Vec::new())
            })
        }
    }

    let fixture = Fixture::new();
    fixture.login();
    let backend = Arc::new(CountingSampler(AtomicUsize::new(0)));
    let sampler = ProtectedHistorySampler::new(fixture.service.clone(), backend.clone());
    fixture.runtime.block_on(async {
        sampler.sample_topics().await.unwrap();
        sampler.sample_brokers().await.unwrap();
        sampler.sample_topics_scoped(None).await.unwrap();
        sampler.sample_brokers_scoped(None).await.unwrap();
        assert_eq!(backend.0.load(Ordering::Acquire), 4);

        // No polling task is running. The sampler must observe revocation on its own admission.
        fixture
            .native
            .revoke_all_sessions("operator".into(), fixture.clock.now().unwrap())
            .await
            .unwrap();
        assert!(sampler.sample_topics().await.is_err());
        assert!(sampler.sample_brokers().await.is_err());
        assert!(sampler.sample_topics_scoped(None).await.is_err());
        assert!(sampler.sample_brokers_scoped(None).await.is_err());
        assert_eq!(backend.0.load(Ordering::Acquire), 4);
        assert!(!fixture.auth.session().is_authenticated());
    });
    fixture.finish();
}
