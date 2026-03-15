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

use serde::Deserialize;
use serde::Serialize;
use std::fmt;

pub(crate) type AuthResult<T> = Result<T, AuthError>;

#[derive(Debug)]
pub(crate) enum AuthError {
    Io(std::io::Error),
    Database(rusqlite::Error),
    PasswordHash(password_hash::Error),
    AppPath(String),
    Validation(String),
    Authentication(String),
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::Database(error) => write!(f, "Database error: {error}"),
            Self::PasswordHash(error) => write!(f, "Password hash error: {error}"),
            Self::AppPath(message) => write!(f, "Application path error: {message}"),
            Self::Validation(message) => write!(f, "Validation error: {message}"),
            Self::Authentication(message) => write!(f, "Authentication error: {message}"),
        }
    }
}

impl std::error::Error for AuthError {}

impl From<std::io::Error> for AuthError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<rusqlite::Error> for AuthError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Database(error)
    }
}

impl From<password_hash::Error> for AuthError {
    fn from(error: password_hash::Error) -> Self {
        Self::PasswordHash(error)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SessionUser {
    pub(crate) session_id: String,
    pub(crate) user_id: i64,
    pub(crate) username: String,
    pub(crate) must_change_password: bool,
    pub(crate) created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UserProfile {
    pub(crate) session_id: String,
    pub(crate) user_id: i64,
    pub(crate) username: String,
    pub(crate) is_active: bool,
    pub(crate) must_change_password: bool,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) last_login_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AuthSessionResponse {
    pub(crate) success: bool,
    pub(crate) message: String,
    pub(crate) session_id: Option<String>,
    pub(crate) current_user: Option<SessionUser>,
    pub(crate) must_change_password: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UserProfileResponse {
    pub(crate) success: bool,
    pub(crate) message: String,
    pub(crate) profile: Option<UserProfile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CommonResponse {
    pub(crate) success: bool,
    pub(crate) message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BootstrapStatus {
    pub(crate) username: String,
    pub(crate) created: bool,
    pub(crate) has_default_admin: bool,
    pub(crate) must_change_password: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct UserRecord {
    pub(crate) id: i64,
    pub(crate) username: String,
    pub(crate) password_hash: String,
    pub(crate) is_active: bool,
    pub(crate) must_change_password: bool,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) last_login_at: Option<String>,
}
