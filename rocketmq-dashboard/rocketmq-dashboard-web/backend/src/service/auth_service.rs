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

use crate::config::AuthConfig;
use crate::error::DashboardError;
use crate::model::AuditAction;
use crate::model::AuditActor;
use crate::model::AuditEvent;
use crate::model::AuditOutcome;
use crate::model::AuditResourceType;
use crate::model::AuthenticatedActor;
use crate::model::LoginRequest;
use crate::model::NewSession;
use crate::model::SessionAuthenticationFailure;
use crate::model::SessionListItem;
use crate::model::SessionListPage;
use crate::model::SessionTokenHash;
use crate::model::SessionView;
use crate::persistence::session_repository::SessionCursor;
use crate::persistence::session_repository::SessionQuery;
use crate::state::AppState;
use axum::http::HeaderValue;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use sha2::Digest;
use sha2::Sha256;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

/// Authentication configuration and operations. The durable repository is the
/// sole session authority; this type deliberately does not cache sessions.
#[derive(Clone)]
pub struct AuthState {
    pub(crate) config: AuthConfig,
    allowed_origin: Option<HeaderValue>,
}

impl AuthState {
    pub fn new(config: AuthConfig) -> Result<Self, DashboardError> {
        let allowed_origin = config.cors_origin()?;
        Ok(Self { config, allowed_origin })
    }

    pub const fn login_required(&self) -> bool {
        self.config.login_required
    }

    pub const fn cookie_secure(&self) -> bool {
        self.config.cookie_secure
    }

    pub fn allowed_origin(&self) -> Option<HeaderValue> {
        self.allowed_origin.clone()
    }

    fn local_actor(&self) -> AuthenticatedActor {
        AuthenticatedActor {
            actor: AuditActor::local_operator(),
            request_id: uuid::Uuid::now_v7().to_string(),
            session_hash: None,
        }
    }
}

pub async fn authenticate(state: &AppState, token: Option<&str>) -> Result<AuthenticatedActor, DashboardError> {
    match authenticate_session_token(state, token).await? {
        Ok(actor) => Ok(actor),
        Err(_) => Err(authentication_required()),
    }
}

/// Performs the persistent session lookup without flattening an ordinary
/// invalid, expired, or revoked credential into a storage failure. The
/// session-status middleware uses this to clear a stale HttpOnly cookie while
/// keeping actual repository failures fail-closed.
pub async fn authenticate_session_token(
    state: &AppState,
    token: Option<&str>,
) -> Result<Result<AuthenticatedActor, SessionAuthenticationFailure>, DashboardError> {
    if !state.auth_state.login_required() {
        return Ok(Ok(state.auth_state.local_actor()));
    }
    let Some(token) = token else {
        return Ok(Err(SessionAuthenticationFailure::Invalid));
    };
    let token_hash = token_hash(token);
    let record = state.persistence.find_session(&token_hash).await?;
    let Some(record) = record else {
        return Ok(Err(SessionAuthenticationFailure::Invalid));
    };
    let now = now_millis();
    if record.revoked_at_ms.is_some() {
        return Ok(Err(SessionAuthenticationFailure::Revoked));
    }
    if now >= record.expires_at_ms {
        return Ok(Err(SessionAuthenticationFailure::Expired));
    }
    // A successful persistent lookup is also the only point at which the
    // operational last-seen timestamp advances; it never refreshes expiry.
    let _ = state.persistence.touch_session(&token_hash, now).await?;
    Ok(Ok(AuthenticatedActor {
        actor: AuditActor::admin(record.username),
        request_id: uuid::Uuid::now_v7().to_string(),
        session_hash: Some(token_hash),
    }))
}

pub async fn session_view(state: &AppState, actor: Option<&AuthenticatedActor>) -> Result<SessionView, DashboardError> {
    if !state.auth_state.login_required() {
        return Ok(SessionView {
            login_required: false,
            authenticated: true,
            username: None,
            session_id: None,
            login_time: None,
            auth_reason: None,
        });
    }
    let Some(actor) = actor else {
        return Ok(SessionView {
            login_required: true,
            authenticated: false,
            username: None,
            session_id: None,
            login_time: None,
            auth_reason: None,
        });
    };
    let login_time = if let Some(hash) = actor.session_hash {
        state
            .persistence
            .find_session(&hash)
            .await?
            .map(|session| session.created_at_ms)
    } else {
        None
    };
    Ok(SessionView {
        login_required: true,
        authenticated: true,
        username: actor.actor.username.clone(),
        // Only the successful login response returns a token. Session reads
        // must never replay a credential to a caller or log it through DTOs.
        session_id: None,
        login_time,
        auth_reason: None,
    })
}

pub async fn login(state: &AppState, request: LoginRequest) -> Result<SessionView, DashboardError> {
    if !state.auth_state.login_required() {
        return session_view(state, Some(&state.auth_state.local_actor())).await;
    }
    if request.username != state.auth_state.config.username || request.password != state.auth_state.config.password {
        return Err(DashboardError::Auth("Invalid username or password".to_string()));
    }

    let now = now_millis();
    let token = generate_token()?;
    let expires_at_ms = now
        .checked_add(
            i64::try_from(state.auth_state.config.session_ttl_secs)
                .map_err(|_| DashboardError::Config("session TTL is too large".to_string()))?
                .saturating_mul(1_000),
        )
        .ok_or_else(|| DashboardError::Config("session expiry is invalid".to_string()))?;
    state
        .persistence
        .create_session_with_audit_capped(
            NewSession {
                session_id: uuid::Uuid::now_v7().to_string(),
                token_hash: token_hash(&token),
                username: request.username.clone(),
                created_at_ms: now,
                expires_at_ms,
            },
            AuditEvent {
                event_id: uuid::Uuid::now_v7().to_string(),
                request_id: uuid::Uuid::now_v7().to_string(),
                actor: AuditActor::admin(request.username.clone()),
                action: AuditAction::SessionCreate,
                resource_type: AuditResourceType::Session,
                resource_name: Some(request.username.clone()),
                environment_id: None,
                outcome: AuditOutcome::Succeeded,
                detail: None,
                created_at_ms: now,
            },
            state.auth_state.config.max_active_sessions as usize,
            now,
        )
        .await
        .map_err(|error| {
            if matches!(error, crate::persistence::error::PersistenceError::Conflict) {
                DashboardError::Auth("Maximum active sessions reached".to_string())
            } else {
                error.into()
            }
        })?;
    Ok(SessionView {
        login_required: true,
        authenticated: true,
        username: Some(request.username),
        // Compatibility for existing header clients. This is the only DTO
        // path that contains the newly generated plaintext token.
        session_id: Some(token),
        login_time: Some(now),
        auth_reason: None,
    })
}

pub async fn logout(state: &AppState, actor: &AuthenticatedActor) -> Result<SessionView, DashboardError> {
    if let Some(hash) = actor.session_hash {
        let now = now_millis();
        state
            .persistence
            .revoke_session_with_audit(
                &hash,
                now,
                AuditEvent {
                    event_id: uuid::Uuid::now_v7().to_string(),
                    request_id: actor.request_id.clone(),
                    actor: actor.actor.clone(),
                    action: AuditAction::SessionRevokeCurrent,
                    resource_type: AuditResourceType::Session,
                    resource_name: actor.actor.username.clone(),
                    environment_id: None,
                    outcome: AuditOutcome::Succeeded,
                    detail: None,
                    created_at_ms: now,
                },
            )
            .await?;
    }
    Ok(SessionView {
        login_required: state.auth_state.login_required(),
        authenticated: !state.auth_state.login_required(),
        username: None,
        session_id: None,
        login_time: None,
        auth_reason: None,
    })
}

pub async fn list_sessions(
    state: &AppState,
    actor: &AuthenticatedActor,
    username: Option<String>,
    cursor: Option<String>,
    limit: usize,
) -> Result<SessionListPage, DashboardError> {
    let page = state
        .persistence
        .list_sessions(SessionQuery {
            username,
            cursor: cursor.as_deref().map(parse_session_cursor).transpose()?,
            limit,
        })
        .await?;
    Ok(SessionListPage {
        items: page
            .records
            .into_iter()
            .map(|record| SessionListItem {
                session_id: record.session_id,
                current: actor.session_hash.is_some_and(|current| current == record.token_hash),
                username: record.username,
                created_at_ms: record.created_at_ms,
                expires_at_ms: record.expires_at_ms,
                last_seen_at_ms: record.last_seen_at_ms,
                revoked_at_ms: record.revoked_at_ms,
            })
            .collect(),
        next_cursor: page
            .next_cursor
            .map(|next| format!("{},{}", next.created_at_ms, next.session_id)),
    })
}

pub async fn revoke_all_sessions(
    state: &AppState,
    actor: &AuthenticatedActor,
    username: String,
) -> Result<u64, DashboardError> {
    if username.is_empty() || username.len() > 128 {
        return Err(DashboardError::Validation("username is invalid".to_string()));
    }
    let now = now_millis();
    state
        .persistence
        .revoke_all_sessions_with_audit(
            &username,
            now,
            AuditEvent {
                event_id: uuid::Uuid::now_v7().to_string(),
                request_id: actor.request_id.clone(),
                actor: actor.actor.clone(),
                action: AuditAction::SessionRevokeAll,
                resource_type: AuditResourceType::Session,
                resource_name: Some(username.clone()),
                environment_id: None,
                outcome: AuditOutcome::Succeeded,
                detail: None,
                created_at_ms: now,
            },
        )
        .await
        .map_err(Into::into)
}

fn parse_session_cursor(value: &str) -> Result<SessionCursor, DashboardError> {
    let (created_at_ms, session_id) = value
        .split_once(',')
        .ok_or_else(|| DashboardError::Validation("session cursor is invalid".to_string()))?;
    if uuid::Uuid::parse_str(session_id).is_err() {
        return Err(DashboardError::Validation("session cursor is invalid".to_string()));
    }
    Ok(SessionCursor {
        created_at_ms: created_at_ms
            .parse()
            .map_err(|_| DashboardError::Validation("session cursor is invalid".to_string()))?,
        session_id: session_id.to_string(),
    })
}

pub fn token_hash(token: &str) -> SessionTokenHash {
    let digest = Sha256::digest(token.as_bytes());
    let mut hash = [0; 32];
    hash.copy_from_slice(&digest);
    SessionTokenHash(hash)
}

fn generate_token() -> Result<String, DashboardError> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes)
        .map_err(|_| DashboardError::Internal("Could not generate session token".to_string()))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn authentication_required() -> DashboardError {
    DashboardError::Auth("Authentication required".to_string())
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::token_hash;

    #[test]
    fn token_hash_is_fixed_width_and_does_not_retain_plaintext() {
        let token = "a plaintext token which must never be persisted";
        let hash = token_hash(token);
        assert_eq!(hash.bytes().len(), 32);
        assert_ne!(hash.lower_hex(), token);
    }
}
