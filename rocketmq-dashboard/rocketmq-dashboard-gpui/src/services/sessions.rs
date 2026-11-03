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

//! Native persistent sessions using the same authority as configuration and P11 Audit.

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use rocketmq_dashboard_common::{AuditAction, AuditFacts, AuditOutcome, OperationResource};
use rocketmq_dashboard_storage::{
    SessionCursor, SessionQuery, SessionRecord, StorageEngine, StorageError, StorageErrorKind,
};
use rocketmq_runtime::{ChildServiceContext, TaskKind};

use super::{
    AppServices, SessionState,
    audit::{MutationAudit, now_epoch_ms},
    auth_ui_error, config_ui_error,
};
use crate::{
    infrastructure::{
        auth_state::{DesktopAuthState, LocalSession},
        config_store::DesktopConfigStore,
    },
    state::{UiError, UiErrorCode},
};

/// Sessions have an absolute eight-hour lifetime. Access never extends that lifetime.
const SESSION_TTL: Duration = Duration::from_secs(8 * 60 * 60);
const VALIDATION_INTERVAL: Duration = Duration::from_secs(5);
const VALIDATION_TIMEOUT: Duration = Duration::from_secs(3);

pub(crate) trait SessionClock: Send + Sync {
    fn now(&self) -> Result<u64, UiError>;
}

struct SystemSessionClock;

impl SessionClock for SystemSessionClock {
    fn now(&self) -> Result<u64, UiError> {
        now_epoch_ms()
    }
}

/// A display projection deliberately has no session capability or persisted identifier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SessionRow {
    pub username: String,
    pub created_at_epoch_ms: u64,
    pub expires_at_epoch_ms: u64,
    pub last_seen_at_epoch_ms: u64,
    pub revoked_at_epoch_ms: Option<u64>,
    pub current: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SessionPageView {
    pub rows: Vec<SessionRow>,
    pub next_cursor: Option<SessionCursor>,
    pub engine: StorageEngine,
    pub observed_at_epoch_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RevokeAllRequest {
    pub username: String,
    pub confirmation: String,
}

impl RevokeAllRequest {
    pub fn validate(&self) -> Result<(), UiError> {
        validate_username(&self.username)?;
        if self.confirmation != self.username {
            return Err(UiError::new(
                "Type the exact username to revoke its sessions.",
                UiErrorCode::Validation,
                false,
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SessionRevocation {
    pub revoked: usize,
    pub current_ended: bool,
}

impl MutationAudit for SessionRevocation {
    fn audit(&self) -> (AuditOutcome, AuditFacts) {
        (
            AuditOutcome::Succeeded,
            AuditFacts {
                applied_targets: u32::try_from(self.revoked).unwrap_or(u32::MAX),
                ..AuditFacts::default()
            },
        )
    }
}

/// Captured access is compared locally after async work, so replacement logins cannot receive
/// an older session's response. The capability stays inside the service/auth owners.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProtectedAccess {
    session: Option<SessionState>,
    pub configuration_revision: u64,
}

pub(super) struct SessionLogin {
    session: LocalSession,
    configuration_revision: u64,
}

pub(crate) struct SessionService {
    config: Arc<DesktopConfigStore>,
    auth: Arc<DesktopAuthState>,
    context: ChildServiceContext,
    clock: Arc<dyn SessionClock>,
    polling_started: AtomicBool,
}

impl SessionService {
    pub(super) fn new(
        config: Arc<DesktopConfigStore>,
        auth: Arc<DesktopAuthState>,
        context: ChildServiceContext,
    ) -> Arc<Self> {
        Arc::new(Self {
            config,
            auth,
            context,
            clock: Arc::new(SystemSessionClock),
            polling_started: AtomicBool::new(false),
        })
    }

    /// The application TaskGroup owns polling and cancels/awaits it before the authority closes.
    pub(super) fn start(self: &Arc<Self>) -> Result<(), UiError> {
        if self.polling_started.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let service = Arc::clone(self);
        let cancellation = self.context.task_spawner().cancellation_token();
        if let Err(error) = self
            .context
            .spawn("gpui-session-validation", TaskKind::Other, async move {
                let mut ticks = tokio::time::interval(VALIDATION_INTERVAL);
                ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => break,
                        _ = ticks.tick() => {
                            // Also load config in anonymous mode: its revision stamp drives the
                            // shared-config watcher when a peer enables auth or changes profiles.
                            // Validation clears only a captured established session on failure.
                            tokio::select! {
                                biased;
                                _ = cancellation.cancelled() => break,
                                _ = service.validate() => {},
                            }
                        }
                    }
                }
            })
        {
            self.polling_started.store(false, Ordering::Release);
            return Err(super::runtime_unavailable(error));
        }
        Ok(())
    }

    pub(super) fn capture_login(&self, configuration_revision: u64) -> SessionLogin {
        SessionLogin {
            session: self.auth.session(),
            configuration_revision,
        }
    }

    pub(super) async fn authenticate(
        &self,
        login: SessionLogin,
        username: &str,
        password: &str,
    ) -> Result<SessionState, UiError> {
        let expected = login.session;
        if self.auth.session() != expected {
            return Err(session_changed());
        }
        let config = self.config.load().await.map_err(config_ui_error)?;
        if config.revision != login.configuration_revision {
            return Err(session_changed());
        }
        self.auth
            .verify_credentials(username, password)
            .map_err(auth_ui_error)?;
        let store = self.config.native_storage().await.map_err(config_ui_error)?;
        let now = self.clock.now()?;
        let expires = now
            .checked_add(u64::try_from(SESSION_TTL.as_millis()).map_err(|_| session_changed())?)
            .ok_or_else(session_changed)?;
        let record = SessionRecord::new(username.to_owned(), now, expires).map_err(storage_error)?;
        store.create_session(record.clone(), now).await.map_err(storage_error)?;
        let configuration = self.config.load().await.map_err(config_ui_error);
        if !configuration
            .as_ref()
            .is_ok_and(|config| config.revision == login.configuration_revision)
        {
            let _ = store.revoke_session(record.id, now).await;
            return Err(configuration.err().unwrap_or_else(session_changed));
        }
        if self.auth.session() != expected {
            let _ = store.revoke_session(record.id, now).await;
            return Err(session_changed());
        }
        if let Some(old) = expected.session_id()
            && let Err(error) = store.revoke_session(old, now).await
        {
            let _ = store.revoke_session(record.id, now).await;
            self.auth.sign_out_if_current(&expected);
            return Err(storage_error(error));
        }
        let Some(installed) = self.auth.install_session(&expected, &record) else {
            let _ = store.revoke_session(record.id, now).await;
            return Err(session_changed());
        };
        Ok(SessionState::from_local(&installed))
    }

    pub(super) fn snapshot(&self) -> SessionState {
        SessionState::from_local(&self.auth.session())
    }

    pub(super) fn changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.auth.changes()
    }

    /// End local access before persistence. Capturing this once prevents a delayed logout from
    /// revoking a replacement login while its terminal audit write is still waiting.
    pub(super) fn end_local(&self) -> LocalSession {
        let captured = self.auth.session();
        self.auth.sign_out_if_current(&captured);
        captured
    }

    pub(super) async fn revoke_captured(&self, captured: LocalSession) -> Result<(), UiError> {
        let Some(id) = captured.session_id() else {
            return Ok(());
        };
        let store = self.config.native_storage().await.map_err(config_ui_error)?;
        store
            .revoke_session(id, self.clock.now()?)
            .await
            .map_err(storage_error)?;
        Ok(())
    }

    pub(super) async fn validate(&self) -> Result<ProtectedAccess, UiError> {
        let expected = self.auth.session();
        self.validate_session(expected).await
    }

    pub(super) async fn validate_marker(&self, marker: &SessionState) -> Result<ProtectedAccess, UiError> {
        let expected = self.auth.session();
        if SessionState::from_local(&expected) != *marker {
            return Err(session_changed());
        }
        self.validate_session(expected).await
    }

    async fn validate_session(&self, expected: LocalSession) -> Result<ProtectedAccess, UiError> {
        if self.auth.session() != expected {
            return Err(session_changed());
        }
        let result = tokio::time::timeout(VALIDATION_TIMEOUT, self.validate_captured(&expected))
            .await
            .unwrap_or_else(|_| {
                Err(UiError::new(
                    "Persistent session verification timed out. Restore the selected storage and sign in again.",
                    UiErrorCode::Authentication,
                    true,
                ))
            });
        if result.is_err() && expected.is_authenticated() {
            self.auth.sign_out_if_current(&expected);
        }
        result
    }

    async fn validate_captured(&self, expected: &LocalSession) -> Result<ProtectedAccess, UiError> {
        // The setting is authoritative too: another SQL client may have changed it.
        let config = self.config.load().await.map_err(config_ui_error)?;
        if !config.auth.enabled && !expected.is_authenticated() {
            return Ok(ProtectedAccess {
                session: None,
                configuration_revision: config.revision,
            });
        }
        let id = expected.session_id().ok_or_else(session_changed)?;
        let now = self.clock.now()?;
        if expected.expires_at_epoch_ms().is_none_or(|expiry| expiry <= now) {
            return Err(session_changed());
        }
        let store = self.config.native_storage().await.map_err(config_ui_error)?;
        let record = store
            .access_session(id, now)
            .await
            .map_err(storage_error)?
            .ok_or_else(session_changed)?;
        if record.username.as_str() != expected.username().unwrap_or_default() || self.auth.session() != *expected {
            return Err(session_changed());
        }
        Ok(ProtectedAccess {
            session: Some(SessionState::from_local(expected)),
            configuration_revision: config.revision,
        })
    }

    pub(super) fn access_is_current(&self, access: &ProtectedAccess) -> bool {
        access
            .session
            .as_ref()
            .is_none_or(|session| session == &self.snapshot())
    }

    async fn operator(&self, expected: &SessionState) -> Result<LocalSession, UiError> {
        let marker = self.validate_marker(expected).await?.session.ok_or_else(|| {
            UiError::new(
                "Enable local login and sign in to manage persistent sessions.",
                UiErrorCode::Authentication,
                false,
            )
        })?;
        let session = self.auth.session();
        if SessionState::from_local(&session) != marker {
            return Err(session_changed());
        }
        Ok(session)
    }

    async fn query(&self, query: SessionQuery, expected: SessionState) -> Result<SessionPageView, UiError> {
        let operator = self.operator(&expected).await?;
        let store = self.config.native_storage().await.map_err(config_ui_error)?;
        let page = store.sessions(query).await.map_err(storage_error)?;
        let status = store.status().await.map_err(storage_error)?;
        if self.auth.session() != operator {
            return Err(session_changed());
        }
        let current = operator.session_id();
        Ok(SessionPageView {
            rows: page
                .sessions
                .into_iter()
                .map(|record| SessionRow {
                    current: current.as_ref() == Some(&record.id),
                    username: record.username,
                    created_at_epoch_ms: record.created_at_epoch_ms,
                    expires_at_epoch_ms: record.expires_at_epoch_ms,
                    last_seen_at_epoch_ms: record.last_seen_at_epoch_ms,
                    revoked_at_epoch_ms: record.revoked_at_epoch_ms,
                })
                .collect(),
            next_cursor: page.next_cursor,
            engine: status.engine,
            observed_at_epoch_ms: self.clock.now()?,
        })
    }

    async fn revoke_all(&self, request: RevokeAllRequest) -> Result<SessionRevocation, UiError> {
        request.validate()?;
        let current = self.auth.session();
        let store = self.config.native_storage().await.map_err(config_ui_error)?;
        let revoked = store
            .revoke_all_sessions(request.username.clone(), self.clock.now()?)
            .await
            .map_err(storage_error)?;
        let current_ended =
            current.username() == Some(request.username.as_str()) && self.auth.sign_out_if_current(&current);
        Ok(SessionRevocation { revoked, current_ended })
    }
}

impl AppServices {
    /// Fixture setup uses real persistent authentication without adding setup Audit events.
    #[cfg(test)]
    pub(crate) async fn establish_session_for_test(
        &self,
        username: &str,
        password: &str,
    ) -> Result<SessionState, UiError> {
        self.backend.authenticate(username, password).await
    }
    pub(crate) fn current_session(&self) -> Option<SessionState> {
        self.sessions.as_ref().map(|service| service.snapshot())
    }
    pub(crate) fn session_changes(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        self.sessions.as_ref().map(|service| service.changes())
    }

    pub(crate) async fn protected_access_for(
        &self,
        expected_session: Option<SessionState>,
    ) -> Result<Option<ProtectedAccess>, UiError> {
        let Some(service) = self.sessions.clone() else {
            return Ok(None);
        };
        let revision = self.connection_state().config.revision;
        let captured = service.auth.session();
        if Some(SessionState::from_local(&captured)) != expected_session {
            return Err(session_changed());
        }
        let work = async move {
            let access = service.validate_session(captured).await?;
            if Some(service.snapshot()) != expected_session {
                return Err(session_changed());
            }
            if access.configuration_revision != revision {
                return Err(UiError::new(
                    "Configuration changed in another desktop. Reload settings before continuing.",
                    UiErrorCode::Configuration,
                    true,
                ));
            }
            Ok(Some(access))
        };
        match &self.runtime_bridge {
            Some(runtime) => {
                let validation = super::RuntimeBridge {
                    context: runtime.context.clone(),
                    #[cfg(test)]
                    completion: None,
                };
                validation.run("gpui-session-admission", work).await
            }
            None => work.await,
        }
    }

    pub(crate) fn protected_access_is_current(&self, access: &Option<ProtectedAccess>) -> bool {
        match (&self.sessions, access) {
            (Some(service), Some(access)) => {
                access.configuration_revision == self.connection_state().config.revision
                    && service.access_is_current(access)
            }
            (None, None) => true,
            _ => false,
        }
    }

    pub(super) async fn run_protected_query<T: Send + 'static>(
        &self,
        name: &'static str,
        future: impl Future<Output = Result<T, UiError>> + Send + 'static,
    ) -> Result<T, UiError> {
        let services = self.clone();
        let expected_session = self.current_session();
        let work = async move {
            let access = services.protected_access_for(expected_session.clone()).await?;
            if services.current_session() != expected_session || !services.protected_access_is_current(&access) {
                return Err(session_changed());
            }
            let result = future.await;
            if services.current_session() != expected_session || !services.protected_access_is_current(&access) {
                return Err(session_changed());
            }
            result
        };
        match &self.runtime_bridge {
            Some(runtime) => runtime.run(name, work).await,
            None => work.await,
        }
    }

    pub(crate) async fn query_sessions(&self, query: SessionQuery) -> Result<SessionPageView, UiError> {
        let service = self.sessions.clone().ok_or_else(unavailable)?;
        let expected = service.snapshot();
        let runtime = self.runtime_bridge.as_ref().ok_or_else(unavailable)?;
        runtime
            .run(
                "gpui-sessions-query",
                async move { service.query(query, expected).await },
            )
            .await
    }

    pub(crate) async fn revalidate_session_page(&self, page: SessionPageView) -> Result<SessionPageView, UiError> {
        let service = self.sessions.clone().ok_or_else(unavailable)?;
        let expected = service.snapshot();
        let runtime = self.runtime_bridge.as_ref().ok_or_else(unavailable)?;
        runtime
            .run("gpui-sessions-query", async move {
                service.operator(&expected).await?;
                Ok(page)
            })
            .await
    }

    pub(crate) async fn revoke_all_sessions(&self, request: RevokeAllRequest) -> Result<SessionRevocation, UiError> {
        request.validate()?;
        let service = self.sessions.clone().ok_or_else(unavailable)?;
        if !service.snapshot().is_authenticated() {
            return Err(UiError::new(
                "Enable local login and sign in to manage persistent sessions.",
                UiErrorCode::Authentication,
                false,
            ));
        }
        let operation = self.accept_operation(
            self.connection_state().config.revision,
            AuditAction::SessionRevokeAll,
            OperationResource::SessionUser(request.username.clone()),
        )?;
        self.run_mutation("gpui-session-revoke-all", operation, async move {
            service.revoke_all(request).await
        })
        .await
    }
}

pub(crate) fn validate_username(username: &str) -> Result<(), UiError> {
    if username.trim().is_empty() || username.len() > 256 || username.chars().any(char::is_control) {
        return Err(UiError::new(
            "Enter an exact username of 1–256 bytes without control characters.",
            UiErrorCode::Validation,
            false,
        ));
    }
    Ok(())
}

fn unavailable() -> UiError {
    UiError::new(
        "Native persistent session storage is unavailable.",
        UiErrorCode::CapabilityUnavailable,
        false,
    )
}
fn storage_error(source: StorageError) -> UiError {
    if source.kind() == StorageErrorKind::Validation {
        return UiError::caused_by(
            "The native session request failed validation.",
            UiErrorCode::Validation,
            false,
            source,
        );
    }
    UiError::caused_by(
        "Persistent session storage could not be verified. Restore the selected storage and sign in again.",
        UiErrorCode::Configuration,
        true,
        source,
    )
}
fn session_changed() -> UiError {
    UiError::new(
        "The session ended, expired, or changed. Sign in again.",
        UiErrorCode::Authentication,
        false,
    )
}

#[cfg(test)]
#[path = "sessions_tests.rs"]
pub(crate) mod tests;

#[cfg(all(
    test,
    any(feature = "storage-sqlite", feature = "storage-mysql", feature = "storage-postgres")
))]
#[path = "sessions_sql_tests.rs"]
mod sql_tests;
