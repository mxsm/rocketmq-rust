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

use super::*;
use crate::auth::types::CommonResponse;
use crate::auth::{AuthDb, AuthService, SessionState};
use crate::message::types::{MessageBatchResendResponse, MessageResendResult};
use rocketmq_runtime::{RuntimeConfig, RuntimeOwner};
use std::sync::atomic::{AtomicBool, Ordering};

struct Fixture {
    owner: Option<RuntimeOwner>,
    storage: StorageManager,
    audit: AuditManager,
    sessions: SessionState,
    directory: std::path::PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let owner = RuntimeOwner::plan(RuntimeConfig::server_default("audit-test"))
            .unwrap()
            .build()
            .unwrap();
        let directory = std::env::temp_dir().join(format!("tauri-audit-{}", Uuid::new_v4()));
        let storage = StorageManager::new(
            directory.join("dashboard.db"),
            owner.root_context().component("storage"),
        );
        owner.block_on(storage.initialize()).unwrap();
        let auth = AuthService::with_initial_password(AuthDb::from_path(storage.path()), "initial-secret");
        let bootstrap = auth.clone();
        owner
            .block_on(storage.run("bootstrap", move |_| bootstrap.bootstrap_default_admin()))
            .unwrap();
        let sessions = SessionState::new(storage.clone(), auth).unwrap();
        let audit = AuditManager::new(storage.clone(), owner.root_context().component("audit"));
        Self {
            owner: Some(owner),
            storage,
            audit,
            sessions,
            directory,
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.take() {
            assert!(owner.block_on(self.audit.shutdown(Duration::from_secs(5))));
            assert!(owner.block_on(self.storage.shutdown(Duration::from_secs(5))));
            let _ = owner.shutdown_runtime_blocking();
        }
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}
fn result(success: bool) -> MessageResendResult {
    MessageResendResult {
        success,
        message: "secret-result-text".into(),
        consumer_group: "test-group".into(),
        topic: "test-topic".into(),
        msg_id: "private-id".into(),
        consume_result: None,
        remark: None,
    }
}

#[test]
fn success_rejection_partial_and_unknown_each_have_one_safe_terminal_record() {
    let fixture = Fixture::new();
    fixture.owner.as_ref().unwrap().block_on(async {
        fixture
            .audit
            .execute(AuditAccess::Login, AuditAction::Login, None, |_| async {
                Ok(CommonResponse {
                    message: "secret-response".into(),
                })
            })
            .await
            .unwrap();
        let called = Arc::new(AtomicBool::new(false));
        let operation_called = called.clone();
        let rejected = fixture
            .audit
            .execute(
                AuditAccess::dashboard(&fixture.sessions, "secret-invalid-token".into()),
                AuditAction::UpsertTopic,
                Some("topic".into()),
                move |_| async move {
                    operation_called.store(true, Ordering::SeqCst);
                    Ok(result(true))
                },
            )
            .await
            .unwrap_err();
        assert_eq!(rejected.code, "auth.session.invalid");
        assert!(!called.load(Ordering::SeqCst));
        fixture
            .audit
            .execute(AuditAccess::Login, AuditAction::BatchResendDlq, None, |_| async {
                Ok(MessageBatchResendResponse {
                    items: vec![result(true), result(false)],
                    total: 2,
                    success_count: 1,
                    failure_count: 1,
                })
            })
            .await
            .unwrap();
        fixture
            .audit
            .execute::<CommonResponse, _, _>(AuditAccess::Login, AuditAction::SendMessage, None, |_| async {
                Err(DashboardError::Internal("secret-network-error"))
            })
            .await
            .unwrap_err();
        let page = fixture.audit.query(AuditQuery::default()).await.unwrap();
        assert_eq!(page.items.len(), 4);
        let mut outcomes = page
            .items
            .iter()
            .map(|event| event.outcome.as_str())
            .collect::<Vec<_>>();
        outcomes.sort();
        assert_eq!(outcomes, ["partial", "rejected", "success", "unknown"]);
        assert!(
            page.items
                .iter()
                .find(|event| event.outcome == "unknown")
                .unwrap()
                .detail
                .result_unknown
        );
        let json = serde_json::to_string(&page).unwrap();
        for secret in [
            "secret-response",
            "secret-invalid-token",
            "secret-result-text",
            "private-id",
            "secret-network-error",
        ] {
            assert!(!json.contains(secret));
        }
        assert!(page.items.iter().all(|event| event.environment_id.is_none()));
    });
}

#[test]
fn successful_remote_result_survives_audit_storage_failure_without_retry() {
    let fixture = Fixture::new();
    fixture.owner.as_ref().unwrap().block_on(async {
        fixture
            .storage
            .run("break-audit", |connection| {
                connection.execute("DROP TABLE audit_events", [])?;
                Ok(())
            })
            .await
            .unwrap();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let executed = calls.clone();
        let response = fixture
            .audit
            .execute(AuditAccess::Login, AuditAction::ResendDlq, None, move |_| async move {
                executed.fetch_add(1, Ordering::SeqCst);
                Ok(result(true))
            })
            .await
            .unwrap();
        assert!(response.result.success);
        assert!(response.audit_warning.is_some());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let json = serde_json::to_value(response).unwrap();
        assert_eq!(json["success"], true);
        assert!(json["auditWarning"].is_string());
    });
}

#[test]
fn dropped_ipc_waiter_keeps_operation_and_shutdown_waits_for_terminal_audit() {
    let fixture = Fixture::new();
    fixture.owner.as_ref().unwrap().block_on(async {
        let (entered, entered_rx) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        let mut waiting = Box::pin(fixture.audit.execute(
            AuditAccess::Login,
            AuditAction::SendMessage,
            None,
            move |_| async move {
                entered.send(()).unwrap();
                release_rx.await.unwrap();
                Ok(result(true))
            },
        ));
        tokio::select! { _ = entered_rx => {}, _ = &mut waiting => panic!("operation completed before release") }
        drop(waiting);
        let mut shutdown = Box::pin(fixture.audit.shutdown(Duration::from_secs(5)));
        std::future::poll_fn(|context| {
            assert!(shutdown.as_mut().poll(context).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        release.send(()).unwrap();
        assert!(shutdown.await);
        assert_eq!(fixture.audit.query(AuditQuery::default()).await.unwrap().items.len(), 1);
        assert!(
            fixture
                .audit
                .execute(AuditAccess::Login, AuditAction::Login, None, |_| async {
                    Ok(result(true))
                })
                .await
                .is_err()
        );
    });
}

#[test]
fn query_filters_and_cursor_do_not_duplicate_rows_with_equal_timestamps() {
    let fixture = Fixture::new();
    fixture.owner.as_ref().unwrap().block_on(async {
        fixture
            .storage
            .run("seed-audit", |connection| {
                for index in 0..4 {
                    db::insert(
                        connection,
                        &AuditEvent {
                            event_id: Uuid::new_v4().to_string(),
                            request_id: Uuid::new_v4().to_string(),
                            actor: Some("admin".into()),
                            action: "topic.delete".into(),
                            resource_type: "topic".into(),
                            resource_name: Some(format!("topic-{index}")),
                            environment_id: None,
                            outcome: "success".into(),
                            detail: Summary::count(1, 0).detail,
                            created_at_ms: 1000,
                        },
                    )?;
                }
                Ok(())
            })
            .await
            .unwrap();
        let mut cursor = None;
        let mut ids = std::collections::HashSet::new();
        loop {
            let page = fixture
                .audit
                .query(AuditQuery {
                    from_ms: Some(1000),
                    to_ms: Some(1000),
                    actor: Some("admin".into()),
                    action: Some("topic.delete".into()),
                    outcome: Some(Outcome::Success),
                    limit: Some(2),
                    cursor,
                    ..Default::default()
                })
                .await
                .unwrap();
            for item in page.items {
                assert!(ids.insert(item.event_id));
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(ids.len(), 4);
        assert!(
            fixture
                .audit
                .query(AuditQuery {
                    actor: Some("other".into()),
                    ..Default::default()
                })
                .await
                .unwrap()
                .items
                .is_empty()
        );
        assert!(
            fixture
                .audit
                .query(AuditQuery {
                    cursor: Some("invalid".into()),
                    ..Default::default()
                })
                .await
                .is_err()
        );
    });
}

#[test]
fn account_mutations_commit_their_audit_once_and_rollback_if_recording_fails() {
    let fixture = Fixture::new();
    fixture.owner.as_ref().unwrap().block_on(async {
        let sessions = fixture.sessions.clone();
        let login = fixture
            .audit
            .execute(AuditAccess::Login, AuditAction::Login, None, move |audit| async move {
                sessions
                    .with_audit(audit)
                    .login("admin".into(), "initial-secret".into())
                    .await
            })
            .await
            .unwrap();
        let token = login.result.session_id;
        let page = fixture.audit.query(AuditQuery::default()).await.unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].actor.as_deref(), Some("admin"));
        assert!(!serde_json::to_string(&page).unwrap().contains(&token));
        fixture
            .audit
            .execute(
                AuditAccess::dashboard(&fixture.sessions, token.clone()),
                AuditAction::DeleteTopic,
                Some("protected".into()),
                |_| async { Ok(result(true)) },
            )
            .await
            .unwrap_err();
        let rejected = fixture
            .audit
            .query(AuditQuery {
                outcome: Some(Outcome::Rejected),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(rejected.items[0].actor.as_deref(), Some("admin"));
        fixture
            .storage
            .run("break-audit", |connection| {
                connection.execute("DROP TABLE audit_events", [])?;
                Ok(())
            })
            .await
            .unwrap();
        let sessions = fixture.sessions.clone();
        let change_token = token.clone();
        let failure = fixture
            .audit
            .execute(
                AuditAccess::account(&fixture.sessions, token.clone()),
                AuditAction::ChangePassword,
                None,
                move |audit| async move {
                    sessions
                        .with_audit(audit)
                        .change_password(change_token, "initial-secret".into(), "new-secret-password".into())
                        .await?;
                    Ok(CommonResponse {
                        message: "changed".into(),
                    })
                },
            )
            .await
            .unwrap_err();
        assert!(failure.audit_warning.is_some());
        assert!(
            fixture
                .sessions
                .require_session(&token)
                .await
                .unwrap()
                .must_change_password
        );
        fixture
            .sessions
            .login("admin".into(), "initial-secret".into())
            .await
            .unwrap();
        assert!(
            fixture
                .sessions
                .login("admin".into(), "new-secret-password".into())
                .await
                .is_err()
        );
    });
}
