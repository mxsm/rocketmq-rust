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
use crate::model::LoginRequest;
use crate::model::SessionView;
use crate::state::AppState;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct AuthState {
    config: AuthConfig,
    session: RwLock<Option<SessionView>>,
}

impl AuthState {
    pub fn new(config: AuthConfig) -> Self {
        Self {
            config,
            session: RwLock::new(None),
        }
    }

    async fn current_session(&self) -> SessionView {
        if !self.config.login_required {
            return SessionView {
                login_required: false,
                authenticated: true,
                username: None,
                session_id: None,
                login_time: None,
            };
        }

        self.session.read().await.clone().unwrap_or(SessionView {
            login_required: true,
            authenticated: false,
            username: None,
            session_id: None,
            login_time: None,
        })
    }

    pub async fn is_authorized(&self, session_id: Option<&str>) -> bool {
        if !self.config.login_required {
            return true;
        }

        let Some(session_id) = session_id else {
            return false;
        };
        self.session
            .read()
            .await
            .as_ref()
            .filter(|session| session.authenticated)
            .and_then(|session| session.session_id.as_deref())
            .is_some_and(|current_session_id| current_session_id == session_id)
    }

    async fn login(&self, request: LoginRequest) -> Result<SessionView, DashboardError> {
        if !self.config.login_required {
            return Ok(self.current_session().await);
        }
        if request.username != self.config.username || request.password != self.config.password {
            return Err(DashboardError::Auth("Invalid username or password".to_string()));
        }

        let now = now_millis();
        let session = SessionView {
            login_required: true,
            authenticated: true,
            username: Some(request.username),
            session_id: Some(format!("dashboard-web-{}-{now}", std::process::id())),
            login_time: Some(now),
        };
        *self.session.write().await = Some(session.clone());
        Ok(session)
    }

    async fn logout(&self) -> SessionView {
        *self.session.write().await = None;
        self.current_session().await
    }
}

pub async fn session(state: &AppState) -> Result<SessionView, DashboardError> {
    Ok(state.auth_state.current_session().await)
}

pub async fn login(state: &AppState, request: LoginRequest) -> Result<SessionView, DashboardError> {
    state.auth_state.login(request).await
}

pub async fn logout(state: &AppState) -> Result<SessionView, DashboardError> {
    Ok(state.auth_state.logout().await)
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::AuthState;
    use crate::config::AuthConfig;
    use crate::model::LoginRequest;

    #[tokio::test]
    async fn auth_state_validates_session_id() {
        let state = AuthState::new(AuthConfig {
            login_required: true,
            username: "admin".to_string(),
            password: "rocketmq".to_string(),
        });

        assert!(!state.is_authorized(None).await);
        let session = state
            .login(LoginRequest {
                username: "admin".to_string(),
                password: "rocketmq".to_string(),
            })
            .await
            .expect("login");
        let session_id = session.session_id.expect("session id");

        assert!(state.is_authorized(Some(&session_id)).await);
        assert!(!state.is_authorized(Some("invalid")).await);
    }
}
