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

use super::service::{AuthService, hash_password, validate_new_password, verify_password};
use super::types::{
    AuthSessionResponse, BootstrapStatus, RevokeSessionsResponse, SessionPage, SessionUser, SessionView, UserProfile,
};
use crate::error::{DashboardError, DashboardResult};
use crate::persistence::StorageManager;
use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

const DEFAULT_TTL_SECS: i64 = 8 * 60 * 60;
const RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1000;
const CLEANUP_BATCH: i64 = 500;

#[derive(Clone)]
pub(crate) struct SessionState {
    storage: StorageManager,
    auth: AuthService,
    ttl_ms: i64,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl SessionState {
    pub(crate) fn new(storage: StorageManager, auth: AuthService) -> DashboardResult<Self> {
        let configured = std::env::var("DASHBOARD_TAURI_SESSION_TTL_SECS")
            .map(Some)
            .or_else(|error| match error {
                std::env::VarError::NotPresent => Ok(None),
                _ => Err(DashboardError::Configuration("invalid session TTL".into())),
            })?;
        Ok(Self {
            storage,
            auth,
            ttl_ms: ttl_millis(configured.as_deref())?,
            clock: Arc::new(|| Utc::now().timestamp_millis()),
        })
    }

    pub(crate) fn start_cleanup(&self) -> DashboardResult<()> {
        let service = self.clone();
        self.storage.start_background("session-cleanup", async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60 * 60));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if service.cleanup().await.is_err() {
                    log::warn!("Session retention cleanup could not complete");
                }
            }
        })
    }

    async fn cleanup(&self) -> DashboardResult<usize> {
        let cutoff = (self.clock)().saturating_sub(RETENTION_MS);
        self.storage
            .run("session-retention", move |connection| {
                Ok(connection.execute(
                    "DELETE FROM sessions WHERE id IN (
                    SELECT id FROM sessions WHERE expires_at_ms <= ?1
                    UNION SELECT id FROM sessions WHERE revoked_at_ms <= ?1 LIMIT ?2
                )",
                    params![cutoff, CLEANUP_BATCH],
                )?)
            })
            .await
    }

    pub(crate) async fn login(&self, username: String, password: String) -> DashboardResult<AuthSessionResponse> {
        let auth = self.auth.clone();
        let clock = self.clock.clone();
        let ttl = self.ttl_ms;
        self.storage
            .run("session-login", move |connection| {
                let user = auth.authenticate(&username, &password)?;
                let now = clock();
                let expires = now
                    .checked_add(ttl)
                    .ok_or_else(|| DashboardError::Configuration("session TTL overflow".into()))?;
                // The safe identifier is independent of the bearer credential. Only the digest is persisted.
                let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
                let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let inserted = transaction.execute(
                    "INSERT INTO sessions(id, token_digest, user_id, created_at_ms, expires_at_ms, last_seen_at_ms)
                 SELECT ?1, ?2, id, ?3, ?4, ?3 FROM users WHERE id = ?5 AND password_hash = ?6 AND is_active = 1",
                    params![
                        Uuid::new_v4().to_string(),
                        digest(&token),
                        now,
                        expires,
                        user.id,
                        user.password_hash
                    ],
                )?;
                if inserted != 1 {
                    return Err(DashboardError::Unauthenticated);
                }
                transaction.execute(
                    "UPDATE users SET last_login_at = ?1 WHERE id = ?2",
                    params![Utc::now().to_rfc3339(), user.id],
                )?;
                let session = lookup(&transaction, &digest(&token), now)?;
                transaction.commit()?;
                Ok(AuthSessionResponse {
                    session_id: token,
                    current_user: session,
                })
            })
            .await
    }

    pub(crate) async fn require_session(&self, token: &str) -> DashboardResult<SessionUser> {
        let hash = digest(token);
        let clock = self.clock.clone();
        self.storage
            .run("session-authorize", move |connection| {
                let now = clock();
                let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let session = lookup(&transaction, &hash, now)?;
                transaction.execute(
                    "UPDATE sessions SET last_seen_at_ms = MAX(last_seen_at_ms, ?1) WHERE token_digest = ?2",
                    params![now, hash],
                )?;
                transaction.commit()?;
                Ok(session)
            })
            .await
    }

    pub(crate) async fn authorize_dashboard(&self, token: &str) -> DashboardResult<SessionUser> {
        let session = self.require_session(token).await?;
        require_password_changed(&session)?;
        Ok(session)
    }

    pub(crate) async fn restore(&self, token: String) -> DashboardResult<AuthSessionResponse> {
        let current_user = self.require_session(&token).await?;
        Ok(AuthSessionResponse {
            session_id: token,
            current_user,
        })
    }

    pub(crate) async fn logout(&self, token: String) -> DashboardResult<()> {
        let hash = digest(&token);
        let clock = self.clock.clone();
        self.storage
            .run("session-logout", move |connection| {
                connection.execute(
                    "UPDATE sessions SET revoked_at_ms = ?1 WHERE token_digest = ?2 AND revoked_at_ms IS NULL",
                    params![clock(), hash],
                )?;
                Ok(())
            })
            .await
    }

    pub(crate) async fn profile(&self, token: String) -> DashboardResult<UserProfile> {
        let hash = digest(&token);
        let auth = self.auth.clone();
        let clock = self.clock.clone();
        self.storage
            .run("session-profile", move |connection| {
                let session = lookup(connection, &hash, clock())?;
                auth.get_user_profile(session.user_id)?
                    .filter(|user| user.is_active)
                    .ok_or(DashboardError::Unauthenticated)
            })
            .await
    }

    pub(crate) async fn bootstrap_status(&self) -> DashboardResult<BootstrapStatus> {
        let auth = self.auth.clone();
        self.storage
            .run("auth-bootstrap-status", move |_| auth.get_bootstrap_status())
            .await
    }

    pub(crate) async fn change_password(&self, token: String, old: String, new: String) -> DashboardResult<()> {
        let auth = self.auth.clone();
        let hash = digest(&token);
        let clock = self.clock.clone();
        self.storage
            .run("session-change-password", move |connection| {
                let session = lookup(connection, &hash, clock())?;
                let user = auth
                    .find_user_by_id(session.user_id)?
                    .ok_or(DashboardError::Unauthenticated)?;
                if !verify_password(&old, &user.password_hash)? {
                    return Err(DashboardError::Authentication("invalid credentials".into()));
                }
                validate_new_password(&old, &new)?;
                let password_hash = hash_password(&new)?;
                // Hash outside the write transaction, then revalidate both credential and password snapshot.
                let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let now = clock();
                lookup(&transaction, &hash, now)?;
                let changed = transaction.execute(
                    "UPDATE users SET password_hash = ?1, must_change_password = 0, updated_at = ?2
                 WHERE id = ?3 AND password_hash = ?4 AND is_active = 1",
                    params![password_hash, Utc::now().to_rfc3339(), user.id, user.password_hash],
                )?;
                if changed != 1 {
                    return Err(DashboardError::Unauthenticated);
                }
                transaction.execute(
                    "UPDATE sessions SET revoked_at_ms = ?1 WHERE user_id = ?2 AND revoked_at_ms IS NULL",
                    params![now, user.id],
                )?;
                transaction.commit()?;
                Ok(())
            })
            .await
    }

    pub(crate) async fn list(
        &self,
        token: String,
        username: Option<String>,
        cursor: Option<String>,
        limit: Option<usize>,
    ) -> DashboardResult<SessionPage> {
        let limit = limit.unwrap_or(25);
        if !(1..=100).contains(&limit) || cursor.as_ref().is_some_and(|value| Uuid::parse_str(value).is_err()) {
            return Err(DashboardError::Validation("invalid session pagination".into()));
        }
        let hash = digest(&token);
        let clock = self.clock.clone();
        self.storage
            .run("session-list", move |connection| {
                let transaction = connection.transaction()?;
                let actor = lookup(&transaction, &hash, clock())?;
                require_password_changed(&actor)?;
                // Local accounts have no cross-account administrator role. Session management is self-service.
                let target = username.unwrap_or_else(|| actor.username.clone());
                if target != actor.username {
                    return Err(DashboardError::Validation("session account mismatch".into()));
                }
                let mut items = {
                    let mut query = transaction.prepare(
                        "SELECT id, created_at_ms, expires_at_ms, last_seen_at_ms, revoked_at_ms, token_digest = ?1
                     FROM sessions WHERE user_id = ?2 AND (?3 IS NULL OR id > ?3) ORDER BY id LIMIT ?4",
                    )?;
                    query
                        .query_map(params![hash, actor.user_id, cursor, (limit + 1) as i64], |row| {
                            Ok(SessionView {
                                id: row.get(0)?,
                                username: target.clone(),
                                created_at_ms: row.get(1)?,
                                expires_at_ms: row.get(2)?,
                                last_seen_at_ms: row.get(3)?,
                                revoked_at_ms: row.get(4)?,
                                current: row.get(5)?,
                            })
                        })?
                        .collect::<Result<Vec<_>, _>>()?
                };
                let next_cursor = if items.len() > limit {
                    items.truncate(limit);
                    items.last().map(|item| item.id.clone())
                } else {
                    None
                };
                transaction.commit()?;
                Ok(SessionPage { items, next_cursor })
            })
            .await
    }

    pub(crate) async fn revoke(&self, token: String, username: String) -> DashboardResult<RevokeSessionsResponse> {
        let hash = digest(&token);
        let clock = self.clock.clone();
        self.storage
            .run("session-revoke-account", move |connection| {
                let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let now = clock();
                let actor = lookup(&transaction, &hash, now)?;
                require_password_changed(&actor)?;
                if username != actor.username {
                    return Err(DashboardError::Validation("session account mismatch".into()));
                }
                let revoked_count = transaction.execute(
                    "UPDATE sessions SET revoked_at_ms = ?1 WHERE user_id = ?2 AND revoked_at_ms IS NULL",
                    params![now, actor.user_id],
                )?;
                transaction.commit()?;
                Ok(RevokeSessionsResponse {
                    revoked_count,
                    current_session_revoked: true,
                })
            })
            .await
    }
}

fn require_password_changed(session: &SessionUser) -> DashboardResult<()> {
    if session.must_change_password {
        Err(DashboardError::PasswordChangeRequired)
    } else {
        Ok(())
    }
}

fn lookup(connection: &Connection, digest: &[u8], now: i64) -> DashboardResult<SessionUser> {
    connection
        .query_row(
            "SELECT u.id, u.username, u.must_change_password, u.created_at FROM sessions s
         JOIN users u ON u.id = s.user_id WHERE s.token_digest = ?1 AND s.expires_at_ms > ?2
         AND s.revoked_at_ms IS NULL AND u.is_active = 1",
            params![digest, now],
            |row| {
                Ok(SessionUser {
                    user_id: row.get(0)?,
                    username: row.get(1)?,
                    must_change_password: row.get(2)?,
                    created_at: row.get(3)?,
                })
            },
        )
        .optional()?
        .ok_or(DashboardError::Unauthenticated)
}

fn digest(token: &str) -> Vec<u8> {
    Sha256::digest(token.as_bytes()).to_vec()
}

fn ttl_millis(value: Option<&str>) -> DashboardResult<i64> {
    let seconds = match value {
        Some(value) => value
            .parse::<i64>()
            .map_err(|_| DashboardError::Configuration("invalid session TTL".into()))?,
        None => DEFAULT_TTL_SECS,
    };
    // Bound configuration to one year, avoiding impractical lifetimes and timestamp overflow.
    if !(1..=365 * 24 * 60 * 60).contains(&seconds) {
        return Err(DashboardError::Configuration(
            "session TTL outside supported range".into(),
        ));
    }
    Ok(seconds * 1000)
}

#[cfg(test)]
mod tests;
