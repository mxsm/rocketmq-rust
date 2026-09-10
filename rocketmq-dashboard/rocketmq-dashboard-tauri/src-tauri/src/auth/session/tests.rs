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

use super::*;
use crate::auth::AuthDb;
use crate::error::CommandError;
use rocketmq_runtime::{RuntimeConfig, RuntimeOwner};
use std::sync::atomic::{AtomicI64, Ordering};

struct Fixture {
    owner: Option<RuntimeOwner>,
    storage: StorageManager,
    service: SessionState,
    clock: Arc<AtomicI64>,
    directory: std::path::PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let owner = RuntimeOwner::plan(RuntimeConfig::server_default("session-test"))
            .unwrap()
            .build()
            .unwrap();
        let directory = std::env::temp_dir().join(format!("tauri-sessions-{}", Uuid::new_v4()));
        let storage = StorageManager::new(
            directory.join("dashboard.db"),
            owner.root_context().component("storage"),
        );
        owner.block_on(storage.initialize()).unwrap();
        let auth = AuthService::with_initial_password(AuthDb::from_path(storage.path()), "change-me-now");
        let bootstrap = auth.clone();
        owner
            .block_on(storage.run("bootstrap", move |_| bootstrap.bootstrap_default_admin()))
            .unwrap();
        let clock = Arc::new(AtomicI64::new(1_800_000_000_000));
        let test_clock = clock.clone();
        let service = SessionState {
            storage: storage.clone(),
            auth,
            ttl_ms: 1000,
            clock: Arc::new(move || test_clock.load(Ordering::SeqCst)),
        };
        Self {
            owner: Some(owner),
            storage,
            service,
            clock,
            directory,
        }
    }

    async fn ready_login(&self) -> AuthSessionResponse {
        let first = self
            .service
            .login("admin".into(), "change-me-now".into())
            .await
            .unwrap();
        self.service
            .change_password(first.session_id, "change-me-now".into(), "better-secret".into())
            .await
            .unwrap();
        self.service
            .login("admin".into(), "better-secret".into())
            .await
            .unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.take() {
            assert!(owner.block_on(self.storage.shutdown(Duration::from_secs(5))));
            let _ = owner.shutdown_runtime_blocking();
        }
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

#[test]
fn expiry_is_absolute_and_restoration_reads_persisted_state() {
    let fixture = Fixture::new();
    fixture.owner.as_ref().unwrap().block_on(async {
        let login = fixture.ready_login().await;
        // New facade has no session cache; it reopens the on-disk database.
        let restored_service = SessionState {
            storage: fixture.storage.clone(),
            ..fixture.service.clone()
        };
        assert_eq!(
            restored_service
                .restore(login.session_id.clone())
                .await
                .unwrap()
                .current_user
                .username,
            "admin"
        );
        fixture.clock.fetch_add(999, Ordering::SeqCst);
        restored_service.authorize_dashboard(&login.session_id).await.unwrap();
        let page = restored_service
            .list(login.session_id.clone(), None, None, None)
            .await
            .unwrap();
        let current = page.items.iter().find(|row| row.current).unwrap();
        assert_eq!(current.expires_at_ms - current.last_seen_at_ms, 1);
        fixture.clock.fetch_add(1, Ordering::SeqCst);
        assert!(matches!(
            restored_service.require_session(&login.session_id).await,
            Err(DashboardError::Unauthenticated)
        ));
    });
}

#[test]
fn password_change_revokes_every_old_token_and_enforces_initial_password() {
    let fixture = Fixture::new();
    fixture.owner.as_ref().unwrap().block_on(async {
        let first = fixture
            .service
            .login("admin".into(), "change-me-now".into())
            .await
            .unwrap();
        let second = fixture
            .service
            .login("admin".into(), "change-me-now".into())
            .await
            .unwrap();
        assert!(
            fixture
                .service
                .profile(first.session_id.clone())
                .await
                .unwrap()
                .must_change_password
        );
        assert!(matches!(
            fixture.service.authorize_dashboard(&first.session_id).await,
            Err(DashboardError::PasswordChangeRequired)
        ));
        assert!(matches!(
            fixture.service.list(first.session_id.clone(), None, None, None).await,
            Err(DashboardError::PasswordChangeRequired)
        ));
        let failure = fixture
            .service
            .change_password(first.session_id.clone(), "wrong".into(), "better-secret".into())
            .await
            .unwrap_err();
        assert_eq!(CommandError::from(failure).code, "auth.credentials.invalid");
        assert!(fixture.service.require_session(&first.session_id).await.is_ok());
        fixture
            .service
            .change_password(first.session_id.clone(), "change-me-now".into(), "better-secret".into())
            .await
            .unwrap();
        assert!(fixture.service.require_session(&first.session_id).await.is_err());
        assert!(fixture.service.require_session(&second.session_id).await.is_err());
        assert!(
            fixture
                .service
                .login("admin".into(), "change-me-now".into())
                .await
                .is_err()
        );
        let replacement = fixture
            .service
            .login("admin".into(), "better-secret".into())
            .await
            .unwrap();
        assert!(!replacement.current_user.must_change_password);
        fixture
            .service
            .authorize_dashboard(&replacement.session_id)
            .await
            .unwrap();
    });
}

#[test]
fn pagination_redaction_revocation_and_account_isolation() {
    let fixture = Fixture::new();
    fixture.owner.as_ref().unwrap().block_on(async {
        let first = fixture.ready_login().await;
        let second = fixture
            .service
            .login("admin".into(), "better-secret".into())
            .await
            .unwrap();
        let mut cursor = None;
        let mut ids = std::collections::HashSet::new();
        let mut current_count = 0;
        loop {
            let page = fixture
                .service
                .list(first.session_id.clone(), Some("admin".into()), cursor, Some(1))
                .await
                .unwrap();
            let json = serde_json::to_string(&page).unwrap();
            assert!(!json.contains(&first.session_id));
            assert!(!json.contains(&second.session_id));
            assert!(!json.contains("digest"));
            for row in page.items {
                assert!(ids.insert(row.id));
                current_count += usize::from(row.current);
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(ids.len(), 3);
        assert_eq!(current_count, 1);
        assert!(
            fixture
                .service
                .list(first.session_id.clone(), Some("other".into()), None, None)
                .await
                .is_err()
        );
        assert!(
            fixture
                .service
                .revoke(first.session_id.clone(), "other".into())
                .await
                .is_err()
        );
        assert!(
            fixture
                .service
                .list(first.session_id.clone(), None, Some("bad cursor".into()), None)
                .await
                .is_err()
        );
        let response = fixture
            .service
            .revoke(first.session_id.clone(), "admin".into())
            .await
            .unwrap();
        assert_eq!(response.revoked_count, 2);
        assert!(response.current_session_revoked);
        assert!(fixture.service.require_session(&first.session_id).await.is_err());
        assert!(fixture.service.require_session(&second.session_id).await.is_err());
    });
}

#[test]
fn disabled_accounts_and_logout_are_authoritative() {
    let fixture = Fixture::new();
    fixture.owner.as_ref().unwrap().block_on(async {
        let login = fixture.ready_login().await;
        fixture
            .storage
            .run("disable", |connection| {
                connection.execute("UPDATE users SET is_active = 0", [])?;
                Ok(())
            })
            .await
            .unwrap();
        assert!(fixture.service.restore(login.session_id.clone()).await.is_err());
        assert!(
            fixture
                .service
                .login("admin".into(), "better-secret".into())
                .await
                .is_err()
        );
        fixture
            .storage
            .run("enable", |connection| {
                connection.execute("UPDATE users SET is_active = 1", [])?;
                Ok(())
            })
            .await
            .unwrap();
        fixture.service.logout(login.session_id.clone()).await.unwrap();
        fixture.service.logout(login.session_id.clone()).await.unwrap();
        assert!(fixture.service.require_session(&login.session_id).await.is_err());
    });
}

#[test]
fn retention_is_bounded_and_keeps_live_sessions() {
    let fixture = Fixture::new();
    fixture.owner.as_ref().unwrap().block_on(async {
        let login = fixture.ready_login().await;
        let now = fixture.clock.load(Ordering::SeqCst);
        fixture
            .storage
            .run("seed-retention", move |connection| {
                let user_id: i64 = connection.query_row("SELECT id FROM users", [], |row| row.get(0))?;
                let transaction = connection.transaction()?;
                for index in 0..505 {
                    transaction.execute(
                        "INSERT INTO sessions VALUES (?1, ?2, ?3, 0, ?4, 0, NULL)",
                        params![
                            format!("expired-{index}"),
                            format!("hash-{index}"),
                            user_id,
                            now - RETENTION_MS
                        ],
                    )?;
                }
                transaction.commit()?;
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(fixture.service.cleanup().await.unwrap(), 500);
        assert_eq!(fixture.service.cleanup().await.unwrap(), 5);
        assert_eq!(fixture.service.cleanup().await.unwrap(), 0);
        fixture.service.authorize_dashboard(&login.session_id).await.unwrap();
        fixture.service.start_cleanup().unwrap();
    });
}

#[test]
fn ttl_configuration_is_validated_without_changing_process_environment() {
    assert_eq!(ttl_millis(None).unwrap(), 28_800_000);
    assert_eq!(ttl_millis(Some("60")).unwrap(), 60_000);
    for value in ["", "0", "-1", "invalid", "9223372036854775807"] {
        assert!(ttl_millis(Some(value)).is_err());
    }
}
