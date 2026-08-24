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
use std::fmt;

use crate::model::AuditActor;
use rocketmq_error::REDACTED;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

impl fmt::Debug for LoginRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LoginRequest")
            .field("username", &self.username)
            .field("password", &REDACTED)
            .finish()
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SessionView {
    pub login_required: bool,
    pub authenticated: bool,
    pub username: Option<String>,
    pub session_id: Option<String>,
    pub login_time: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_reason: Option<String>,
}

/// A non-secret reason a supplied credential no longer establishes a
/// dashboard session. It is returned only by the session-status endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionAuthenticationFailure {
    Invalid,
    Expired,
    Revoked,
}

impl SessionAuthenticationFailure {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Invalid => "invalid",
            Self::Expired => "expired",
            Self::Revoked => "revoked",
        }
    }
}

/// A fixed-size SHA-256 token digest. The corresponding plaintext session
/// token is never serialised or written to disk.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionTokenHash(pub [u8; 32]);

impl SessionTokenHash {
    pub const fn bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn lower_hex(&self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = String::with_capacity(64);
        for byte in self.0 {
            encoded.push(HEX[usize::from(byte >> 4)] as char);
            encoded.push(HEX[usize::from(byte & 0x0f)] as char);
        }
        encoded
    }
}

impl fmt::Debug for SessionTokenHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SessionTokenHash(<redacted>)")
    }
}

/// Storage record used by the session repository. This type is intentionally
/// separate from `SessionView`, which is safe to return to the browser.
#[derive(Clone, PartialEq, Eq)]
pub struct SessionRecord {
    /// Non-secret public identifier used for session administration cursors.
    /// The digest remains storage-only and is never exposed in an API cursor.
    pub session_id: String,
    pub token_hash: SessionTokenHash,
    pub username: String,
    pub created_at_ms: i64,
    pub expires_at_ms: i64,
    pub last_seen_at_ms: i64,
    pub revoked_at_ms: Option<i64>,
}

impl fmt::Debug for SessionRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionRecord")
            .field("session_id", &self.session_id)
            .field("token_hash", &REDACTED)
            .field("username", &self.username)
            .field("created_at_ms", &self.created_at_ms)
            .field("expires_at_ms", &self.expires_at_ms)
            .field("last_seen_at_ms", &self.last_seen_at_ms)
            .field("revoked_at_ms", &self.revoked_at_ms)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct NewSession {
    pub session_id: String,
    pub token_hash: SessionTokenHash,
    pub username: String,
    pub created_at_ms: i64,
    pub expires_at_ms: i64,
}

impl fmt::Debug for NewSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NewSession")
            .field("session_id", &self.session_id)
            .field("token_hash", &REDACTED)
            .field("username", &self.username)
            .field("created_at_ms", &self.created_at_ms)
            .field("expires_at_ms", &self.expires_at_ms)
            .finish()
    }
}

/// Request-scoped identity installed by the authentication middleware.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedActor {
    pub actor: AuditActor,
    pub request_id: String,
    pub session_hash: Option<SessionTokenHash>,
}

/// Safe administrator session projection. No token or token derivative is
/// returned; identifiers are only used for keyset pagination.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionListItem {
    pub session_id: String,
    pub username: String,
    pub created_at_ms: i64,
    pub expires_at_ms: i64,
    pub last_seen_at_ms: i64,
    pub revoked_at_ms: Option<i64>,
    pub current: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionListPage {
    pub items: Vec<SessionListItem>,
    pub next_cursor: Option<String>,
}

impl fmt::Debug for SessionView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionView")
            .field("login_required", &self.login_required)
            .field("authenticated", &self.authenticated)
            .field("username", &self.username)
            .field("session_id", &self.session_id.as_ref().map(|_| REDACTED))
            .field("login_time", &self.login_time)
            .field("auth_reason", &self.auth_reason)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_model_debug_redacts_password_and_session_id() {
        let login = LoginRequest {
            username: "admin".to_string(),
            password: "dashboard-password".to_string(),
        };
        let session = SessionView {
            login_required: true,
            authenticated: true,
            username: Some("admin".to_string()),
            session_id: Some("dashboard-session-token".to_string()),
            login_time: Some(1),
            auth_reason: None,
        };

        let login_debug = format!("{login:?}");
        let session_debug = format!("{session:?}");

        assert!(login_debug.contains(REDACTED));
        assert!(session_debug.contains(REDACTED));
        assert!(!login_debug.contains("dashboard-password"));
        assert!(!session_debug.contains("dashboard-session-token"));
    }
}
