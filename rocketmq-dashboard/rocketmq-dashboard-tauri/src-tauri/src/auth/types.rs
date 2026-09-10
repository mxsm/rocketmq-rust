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

pub(crate) type AuthResult<T> = crate::error::DashboardResult<T>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SessionUser {
    pub(crate) user_id: i64,
    pub(crate) username: String,
    pub(crate) must_change_password: bool,
    pub(crate) created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UserProfile {
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
    pub(crate) session_id: String,
    pub(crate) current_user: SessionUser,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CommonResponse {
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

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SessionView {
    pub(crate) id: String,
    pub(crate) username: String,
    pub(crate) created_at_ms: i64,
    pub(crate) expires_at_ms: i64,
    pub(crate) last_seen_at_ms: i64,
    pub(crate) revoked_at_ms: Option<i64>,
    pub(crate) current: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SessionPage {
    pub(crate) items: Vec<SessionView>,
    pub(crate) next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RevokeSessionsResponse {
    pub(crate) revoked_count: usize,
    pub(crate) current_session_revoked: bool,
}
