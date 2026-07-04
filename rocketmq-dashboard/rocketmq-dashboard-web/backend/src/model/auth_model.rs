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
        };

        let login_debug = format!("{login:?}");
        let session_debug = format!("{session:?}");

        assert!(login_debug.contains(REDACTED));
        assert!(session_debug.contains(REDACTED));
        assert!(!login_debug.contains("dashboard-password"));
        assert!(!session_debug.contains("dashboard-session-token"));
    }
}
