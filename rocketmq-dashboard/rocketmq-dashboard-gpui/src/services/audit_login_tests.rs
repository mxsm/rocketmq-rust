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

//! Login identity comes from the owned authentication result, including delayed terminal recovery.

use super::*;

const PASSWORD: &str = "private-password";

struct ChangingEnvironment {
    username: parking_lot::RwLock<String>,
    reads: AtomicUsize,
}

impl ChangingEnvironment {
    fn new(username: &str) -> Arc<Self> {
        Arc::new(Self {
            username: parking_lot::RwLock::new(username.into()),
            reads: AtomicUsize::new(0),
        })
    }

    fn replace_operator(&self, username: &str) {
        *self.username.write() = username.into();
    }
}

impl EnvironmentReader for ChangingEnvironment {
    fn read(&self, name: &'static str) -> Option<String> {
        self.reads.fetch_add(1, Ordering::AcqRel);
        match name {
            LOGIN_USERNAME_ENV => Some(self.username.read().clone()),
            LOGIN_PASSWORD_ENV => Some(PASSWORD.into()),
            _ => None,
        }
    }
}

fn operator(username: &str) -> OperationActor {
    OperationActor::LocalOperator {
        username: username.into(),
    }
}

fn finish(fixture: Fixture) {
    fixture.runtime.block_on(async {
        assert!(
            fixture
                .mutations
                .task_group()
                .shutdown(Duration::from_secs(5))
                .await
                .is_healthy()
        );
        fixture.store.close().await.unwrap();
    });
    assert!(fixture.runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}

fn assert_login_actor(authentication_enabled: bool) {
    let fixture = Fixture::new();
    let environment = fixture.store.environments().unwrap().active_id;
    let verified = fixture.runtime.block_on(async {
        if authentication_enabled {
            let (progress, _) = tokio::sync::mpsc::unbounded_channel();
            fixture
                .services
                .mutate_with_progress(crate::services::ConfigMutation::SetAuthEnabled(true), progress)
                .await
                .unwrap();
        }
        assert_eq!(
            fixture.services.connection_state().config.auth.enabled,
            authentication_enabled
        );
        let error = fixture
            .services
            .authenticate("unverified-form-user", "wrong-password")
            .await
            .unwrap_err();
        assert_eq!(error.code(), UiErrorCode::Authentication);
        fixture.services.authenticate("operator", PASSWORD).await.unwrap()
    });
    let events: Vec<_> = fixture
        .events()
        .into_iter()
        .filter(|event| event.operation.action == AuditAction::LocalLogin)
        .collect();
    assert_eq!(events.len(), 2);
    let successful = events
        .iter()
        .find(|event| event.outcome == AuditOutcome::Succeeded)
        .unwrap();
    assert_eq!(successful.operation.context.actor, operator("operator"));
    assert_eq!(successful.operation.context.environment_id, environment);
    assert_eq!(
        successful.operation.context.configuration_revision,
        fixture.services.connection_state().config.revision
    );
    assert_eq!(successful.facts.authenticated_actor, Some(operator("operator")));
    let rejected = events
        .iter()
        .find(|event| event.outcome == AuditOutcome::Rejected)
        .unwrap();
    assert_eq!(rejected.operation.context.actor, OperationActor::LocalDesktop);
    assert!(rejected.facts.authenticated_actor.is_none());
    assert_eq!(rejected.facts.failure_code, Some(AuditFailureCode::Authentication));
    assert_ne!(
        successful.operation.context.request_id,
        rejected.operation.context.request_id
    );
    let token = String::from(fixture.auth.session().session_id().unwrap());
    let serialized = serde_json::to_string(&events).unwrap();
    for forbidden in ["unverified-form-user", "wrong-password", PASSWORD, token.as_str()] {
        assert!(!serialized.contains(forbidden));
    }
    let diagnostics = format!("{events:?} {verified:?}");
    assert!(
        !diagnostics.contains("operator"),
        "Debug must redact the verified display identity"
    );
    assert!(
        crate::infrastructure::audit_store::persisted_journal(fixture.store.path())
            .pending()
            .is_empty(),
        "login does not persist a pre-verification pending actor"
    );
    finish(fixture);
}

#[test]
fn audit_login_with_authentication_enabled_records_verified_actor_and_rejects_claimed_identity() {
    assert_login_actor(true);
}

#[test]
fn audit_login_with_authentication_disabled_still_records_the_actual_verified_result() {
    // The service supports explicit authentication while the local login policy is disabled.
    assert_login_actor(false);
}

#[test]
fn audit_login_delayed_verified_result_keeps_its_actor_after_a_replacement_login() {
    let environment = ChangingEnvironment::new("verified-a");
    let fixture = Fixture::with_environment(environment.clone());
    fixture.runtime.block_on(async {
        let operation = fixture
            .services
            .accept_operation(0, AuditAction::LocalLogin, OperationResource::LocalSession)
            .unwrap();
        let request_id = operation.as_ref().unwrap().context.request_id.clone();
        let sessions = fixture.services.sessions.as_ref().unwrap().clone();
        let captured = sessions.capture_login(0);
        let (started, verified_ready) = tokio::sync::oneshot::channel();
        let (release, wait) = tokio::sync::oneshot::channel();
        // Real SessionService verification/persistence installs A. Hold its owned result before
        // the Audit boundary sees it, so reading the global session after await would select B.
        let mut first = Box::pin(
            fixture
                .services
                .run_mutation("held-verified-login", operation, async move {
                    let result = sessions.authenticate(captured, "verified-a", PASSWORD).await?;
                    started.send(()).unwrap();
                    wait.await.unwrap();
                    Ok(result)
                }),
        );
        tokio::select! {
            result = &mut first => panic!("verified result returned before release: {result:?}"),
            result = verified_ready => result.unwrap(),
        }
        assert!(
            fixture
                .services
                .audit
                .as_ref()
                .unwrap()
                .store
                .pending()
                .await
                .unwrap()
                .is_empty()
        );
        environment.replace_operator("verified-b");
        let second = fixture.services.authenticate("verified-b", PASSWORD).await.unwrap();
        let current = fixture.auth.session();
        release.send(()).unwrap();
        let first_result = first.await.unwrap();
        assert_eq!(first_result.username.as_deref(), Some("verified-a"));
        assert_eq!(fixture.services.current_session(), Some(second));
        assert_eq!(fixture.auth.session(), current, "finishing A cannot replace or end B");
        let events = fixture.events();
        let first_event = events
            .iter()
            .find(|event| event.operation.context.request_id == request_id)
            .unwrap();
        assert_eq!(first_event.operation.context.actor, operator("verified-a"));
        assert_eq!(first_event.facts.authenticated_actor, Some(operator("verified-a")));
        assert!(
            events
                .iter()
                .any(|event| event.operation.action == AuditAction::LocalLogin
                    && event.operation.context.actor == operator("verified-b")
                    && event.outcome == AuditOutcome::Succeeded)
        );
    });
    finish(fixture);
}

#[test]
fn audit_login_failed_terminal_retry_preserves_verified_actor_without_reauthentication() {
    let environment = ChangingEnvironment::new("verified-a");
    let fixture = Fixture::with_environment(environment.clone());
    fixture.runtime.block_on(async {
        let (started, release) = fixture.services.hold_next_audit_terminal();
        let mut first = fixture.services.authenticate("verified-a", PASSWORD);
        tokio::select! {
            result = &mut first => panic!("terminal completed before release: {result:?}"),
            result = started => result.unwrap(),
        }
        let first_token = String::from(fixture.auth.session().session_id().unwrap());
        let backup = fixture.directory.path().join("login-terminal-backup.json");
        std::fs::rename(fixture.store.path(), &backup).unwrap();
        std::fs::create_dir(fixture.store.path()).unwrap();
        release.send(()).unwrap();
        assert_eq!(first.await.unwrap().username.as_deref(), Some("verified-a"));
        assert!(fixture.services.audit_write_warning());
        let queued = fixture
            .services
            .audit
            .as_ref()
            .unwrap()
            .failed_writes
            .lock()
            .values()
            .next()
            .unwrap()
            .write
            .clone();
        std::fs::remove_dir(fixture.store.path()).unwrap();
        std::fs::rename(backup, fixture.store.path()).unwrap();
        assert_eq!(queued.operation.context.actor, operator("verified-a"));
        environment.replace_operator("verified-b");
        let second = fixture.services.authenticate("verified-b", PASSWORD).await.unwrap();
        let current = fixture.auth.session();
        let second_token = String::from(current.session_id().unwrap());
        let native = fixture.store.native_storage().await.unwrap();
        let before: Vec<_> = native
            .sessions(Default::default())
            .await
            .unwrap()
            .sessions
            .into_iter()
            .map(|record| record.id)
            .collect();
        assert_eq!(before.len(), 2);
        let credential_reads = environment.reads.load(Ordering::Acquire);
        fixture.services.retry_audit_writes().await.unwrap();
        fixture.services.retry_audit_writes().await.unwrap();
        assert_eq!(
            environment.reads.load(Ordering::Acquire),
            credential_reads,
            "retry must not authenticate again"
        );
        let after: Vec<_> = native
            .sessions(Default::default())
            .await
            .unwrap()
            .sessions
            .into_iter()
            .map(|record| record.id)
            .collect();
        assert_eq!(before, after, "terminal-only retries cannot create or replay sessions");
        assert_eq!(fixture.services.current_session(), Some(second));
        assert_eq!(fixture.auth.session(), current);
        assert!(!fixture.services.audit_write_warning());
        let events = fixture.events();
        assert_eq!(
            events.len(),
            2,
            "repeated retry must retain one terminal event per login"
        );
        let first_event = events
            .iter()
            .find(|event| event.operation.context.request_id == queued.operation.context.request_id)
            .unwrap();
        assert_eq!(first_event.operation, queued.operation);
        assert_eq!(first_event.operation.context.actor, operator("verified-a"));
        assert_eq!(first_event.facts.authenticated_actor, Some(operator("verified-a")));
        assert_eq!(first_event.outcome, AuditOutcome::Succeeded);
        let serialized = serde_json::to_string(&events).unwrap();
        for forbidden in [PASSWORD, first_token.as_str(), second_token.as_str()] {
            assert!(!serialized.contains(forbidden));
        }
    });
    finish(fixture);
}

#[test]
fn audit_login_new_verified_attribution_does_not_backfill_a_historical_desktop_actor() {
    let fixture = Fixture::new();
    fixture.runtime.block_on(async {
        let historical = fixture
            .services
            .accept_operation(0, AuditAction::LocalLogin, OperationResource::LocalSession)
            .unwrap()
            .unwrap();
        let request_id = historical.context.request_id.clone();
        fixture
            .services
            .audit
            .as_ref()
            .unwrap()
            .store
            .finish(
                historical,
                AuditOutcome::Succeeded,
                AuditFacts {
                    authenticated_actor: Some(operator("historically-verified")),
                    ..Default::default()
                },
                now_epoch_ms().unwrap(),
            )
            .await
            .unwrap();
        let before = fixture.events().remove(0);
        assert_eq!(before.operation.context.actor, OperationActor::LocalDesktop);
        fixture.services.authenticate("operator", PASSWORD).await.unwrap();
        fixture.services.retry_audit_writes().await.unwrap();
        let events = fixture.events();
        let retained = events
            .iter()
            .find(|event| event.operation.context.request_id == request_id)
            .unwrap();
        assert_eq!(
            retained, &before,
            "new login attribution must never rewrite a terminal record"
        );
        assert!(
            events
                .iter()
                .any(|event| event.operation.context.actor == operator("operator"))
        );
    });
    finish(fixture);
}
