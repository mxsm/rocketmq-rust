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

use crate::model::AuditEvent;
use crate::model::NewSession;
use crate::model::SessionRecord;
use crate::model::SessionTokenHash;
use crate::persistence::DashboardPersistence;
use crate::persistence::backend::PersistenceBackend;
use crate::persistence::error::PersistenceError;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionCursor {
    pub created_at_ms: i64,
    pub session_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionQuery {
    pub username: Option<String>,
    pub cursor: Option<SessionCursor>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionPage {
    pub records: Vec<SessionRecord>,
    pub next_cursor: Option<SessionCursor>,
}

/// Repository contract for persisted sessions. All implementations store only
/// the 32-byte digest, never the login token supplied by the browser.
#[allow(async_fn_in_trait)]
pub trait SessionRepository {
    async fn create_session(&self, session: NewSession) -> Result<(), PersistenceError>;
    async fn find_session(&self, token_hash: &SessionTokenHash) -> Result<Option<SessionRecord>, PersistenceError>;
    async fn revoke_session(&self, token_hash: &SessionTokenHash, revoked_at_ms: i64)
    -> Result<bool, PersistenceError>;
    async fn revoke_all_sessions(&self, username: &str, revoked_at_ms: i64) -> Result<u64, PersistenceError>;
    async fn list_sessions(&self, query: SessionQuery) -> Result<SessionPage, PersistenceError>;
    async fn delete_sessions_before(&self, cutoff_ms: i64, limit: usize) -> Result<u64, PersistenceError>;
    async fn create_session_with_audit(&self, session: NewSession, audit: AuditEvent) -> Result<(), PersistenceError>;
    async fn revoke_session_with_audit(
        &self,
        token_hash: &SessionTokenHash,
        revoked_at_ms: i64,
        audit: AuditEvent,
    ) -> Result<bool, PersistenceError>;
    async fn revoke_all_sessions_with_audit(
        &self,
        username: &str,
        revoked_at_ms: i64,
        audit: AuditEvent,
    ) -> Result<u64, PersistenceError>;
}

impl DashboardPersistence {
    pub async fn create_session(&self, session: NewSession) -> Result<(), PersistenceError> {
        validate_new_session(&session)?;
        match &self.backend {
            PersistenceBackend::File(store) => store.create_session(session).await,
            PersistenceBackend::Sql(store) => store.create_session(session).await,
        }
    }

    pub async fn create_session_with_audit(
        &self,
        session: NewSession,
        audit: AuditEvent,
    ) -> Result<(), PersistenceError> {
        validate_new_session(&session)?;
        match &self.backend {
            PersistenceBackend::File(store) => store.create_session_with_audit(session, audit).await,
            PersistenceBackend::Sql(store) => store.create_session_with_audit(session, audit).await,
        }
    }

    /// Atomically enforces the per-user active-session limit while creating
    /// the durable session and its successful-login audit event.
    pub async fn create_session_with_audit_capped(
        &self,
        session: NewSession,
        audit: AuditEvent,
        max_active_sessions: usize,
        now_ms: i64,
    ) -> Result<(), PersistenceError> {
        validate_new_session(&session)?;
        if max_active_sessions == 0 || max_active_sessions > 32 {
            return Err(PersistenceError::InvalidConfig(
                "maximum active sessions must be between 1 and 32".to_string(),
            ));
        }
        match &self.backend {
            PersistenceBackend::File(store) => {
                store
                    .create_session_with_audit_capped(session, audit, max_active_sessions, now_ms)
                    .await
            }
            PersistenceBackend::Sql(store) => {
                store
                    .create_session_with_audit_capped(session, audit, max_active_sessions, now_ms)
                    .await
            }
        }
    }

    pub async fn find_session(&self, token_hash: &SessionTokenHash) -> Result<Option<SessionRecord>, PersistenceError> {
        match &self.backend {
            PersistenceBackend::File(store) => store.find_session(token_hash).await,
            PersistenceBackend::Sql(store) => store.find_session(token_hash).await,
        }
    }

    /// Records a successful authentication observation without extending the
    /// immutable session expiry. A missing or revoked record is not touched.
    pub async fn touch_session(
        &self,
        token_hash: &SessionTokenHash,
        observed_at_ms: i64,
    ) -> Result<bool, PersistenceError> {
        match &self.backend {
            PersistenceBackend::File(store) => store.touch_session(token_hash, observed_at_ms).await,
            PersistenceBackend::Sql(store) => store.touch_session(token_hash, observed_at_ms).await,
        }
    }

    pub async fn revoke_session(
        &self,
        token_hash: &SessionTokenHash,
        revoked_at_ms: i64,
    ) -> Result<bool, PersistenceError> {
        match &self.backend {
            PersistenceBackend::File(store) => store.revoke_session(token_hash, revoked_at_ms).await,
            PersistenceBackend::Sql(store) => store.revoke_session(token_hash, revoked_at_ms).await,
        }
    }

    pub async fn revoke_session_with_audit(
        &self,
        token_hash: &SessionTokenHash,
        revoked_at_ms: i64,
        audit: AuditEvent,
    ) -> Result<bool, PersistenceError> {
        match &self.backend {
            PersistenceBackend::File(store) => store.revoke_session_with_audit(token_hash, revoked_at_ms, audit).await,
            PersistenceBackend::Sql(store) => store.revoke_session_with_audit(token_hash, revoked_at_ms, audit).await,
        }
    }

    pub async fn revoke_all_sessions(&self, username: &str, revoked_at_ms: i64) -> Result<u64, PersistenceError> {
        validate_username(username)?;
        match &self.backend {
            PersistenceBackend::File(store) => store.revoke_all_sessions(username, revoked_at_ms).await,
            PersistenceBackend::Sql(store) => store.revoke_all_sessions(username, revoked_at_ms).await,
        }
    }

    pub async fn revoke_all_sessions_with_audit(
        &self,
        username: &str,
        revoked_at_ms: i64,
        audit: AuditEvent,
    ) -> Result<u64, PersistenceError> {
        validate_username(username)?;
        match &self.backend {
            PersistenceBackend::File(store) => {
                store
                    .revoke_all_sessions_with_audit(username, revoked_at_ms, audit)
                    .await
            }
            PersistenceBackend::Sql(store) => {
                store
                    .revoke_all_sessions_with_audit(username, revoked_at_ms, audit)
                    .await
            }
        }
    }

    pub async fn list_sessions(&self, query: SessionQuery) -> Result<SessionPage, PersistenceError> {
        validate_session_query(&query)?;
        match &self.backend {
            PersistenceBackend::File(store) => store.list_sessions(query).await,
            PersistenceBackend::Sql(store) => store.list_sessions(query).await,
        }
    }

    pub async fn delete_sessions_before(&self, cutoff_ms: i64, limit: usize) -> Result<u64, PersistenceError> {
        if limit == 0 || limit > 1_000 {
            return Err(PersistenceError::InvalidConfig(
                "session cleanup batch must be between 1 and 1000".to_string(),
            ));
        }
        match &self.backend {
            PersistenceBackend::File(store) => store.delete_sessions_before(cutoff_ms, limit).await,
            PersistenceBackend::Sql(store) => store.delete_sessions_before(cutoff_ms, limit).await,
        }
    }
}

impl SessionRepository for DashboardPersistence {
    async fn create_session(&self, session: NewSession) -> Result<(), PersistenceError> {
        Self::create_session(self, session).await
    }

    async fn find_session(&self, token_hash: &SessionTokenHash) -> Result<Option<SessionRecord>, PersistenceError> {
        Self::find_session(self, token_hash).await
    }

    async fn revoke_session(
        &self,
        token_hash: &SessionTokenHash,
        revoked_at_ms: i64,
    ) -> Result<bool, PersistenceError> {
        Self::revoke_session(self, token_hash, revoked_at_ms).await
    }

    async fn revoke_all_sessions(&self, username: &str, revoked_at_ms: i64) -> Result<u64, PersistenceError> {
        Self::revoke_all_sessions(self, username, revoked_at_ms).await
    }

    async fn list_sessions(&self, query: SessionQuery) -> Result<SessionPage, PersistenceError> {
        Self::list_sessions(self, query).await
    }

    async fn delete_sessions_before(&self, cutoff_ms: i64, limit: usize) -> Result<u64, PersistenceError> {
        Self::delete_sessions_before(self, cutoff_ms, limit).await
    }

    async fn create_session_with_audit(&self, session: NewSession, audit: AuditEvent) -> Result<(), PersistenceError> {
        Self::create_session_with_audit(self, session, audit).await
    }

    async fn revoke_session_with_audit(
        &self,
        token_hash: &SessionTokenHash,
        revoked_at_ms: i64,
        audit: AuditEvent,
    ) -> Result<bool, PersistenceError> {
        Self::revoke_session_with_audit(self, token_hash, revoked_at_ms, audit).await
    }

    async fn revoke_all_sessions_with_audit(
        &self,
        username: &str,
        revoked_at_ms: i64,
        audit: AuditEvent,
    ) -> Result<u64, PersistenceError> {
        Self::revoke_all_sessions_with_audit(self, username, revoked_at_ms, audit).await
    }
}

pub(crate) fn validate_new_session(session: &NewSession) -> Result<(), PersistenceError> {
    validate_username(&session.username)?;
    if uuid::Uuid::parse_str(&session.session_id).is_err() {
        return Err(PersistenceError::InvalidConfig(
            "session identifier is invalid".to_string(),
        ));
    }
    if session.created_at_ms < 0 || session.expires_at_ms <= session.created_at_ms {
        return Err(PersistenceError::InvalidConfig(
            "session timestamps are invalid".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_username(username: &str) -> Result<(), PersistenceError> {
    if username.is_empty() || username.len() > 128 || username.chars().any(char::is_control) {
        return Err(PersistenceError::InvalidConfig(
            "session username is invalid".to_string(),
        ));
    }
    Ok(())
}

fn validate_session_query(query: &SessionQuery) -> Result<(), PersistenceError> {
    if query.limit == 0 || query.limit > 200 {
        return Err(PersistenceError::InvalidConfig(
            "session page size must be between 1 and 200".to_string(),
        ));
    }
    if let Some(username) = &query.username {
        validate_username(username)?;
    }
    if let Some(cursor) = &query.cursor
        && uuid::Uuid::parse_str(&cursor.session_id).is_err()
    {
        return Err(PersistenceError::InvalidConfig("session cursor is invalid".to_string()));
    }
    Ok(())
}
