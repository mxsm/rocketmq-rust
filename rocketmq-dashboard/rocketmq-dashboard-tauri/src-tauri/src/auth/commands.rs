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

use crate::auth::service::AuthService;
use crate::auth::session::SessionState;
use crate::auth::types::AuthSessionResponse;
use crate::auth::types::BootstrapStatus;
use crate::auth::types::CommonResponse;
use crate::auth::types::SessionUser;
use tauri::State;

#[tauri::command]
pub fn login(
    username: String,
    password: String,
    auth_service: State<'_, AuthService>,
    session_state: State<'_, SessionState>,
) -> AuthSessionResponse {
    match auth_service.authenticate(&username, &password) {
        Ok(user) => {
            if let Err(error) = auth_service.update_last_login(user.id) {
                log::warn!("Failed to update last login for {}: {}", username, error);
            }

            let session = session_state.create_session(&user);
            AuthSessionResponse {
                success: true,
                message: "Login successful".to_string(),
                session_id: Some(session.session_id.clone()),
                current_user: Some(session.clone()),
                must_change_password: session.must_change_password,
            }
        }
        Err(error) => {
            log::warn!("Login failed for {}: {}", username, error);
            AuthSessionResponse {
                success: false,
                message: error.to_string().replace("Authentication error: ", ""),
                session_id: None,
                current_user: None,
                must_change_password: false,
            }
        }
    }
}

#[tauri::command]
pub fn logout(session_id: String, session_state: State<'_, SessionState>) -> CommonResponse {
    session_state.remove_session(&session_id);

    CommonResponse {
        success: true,
        message: "Logged out successfully".to_string(),
    }
}

#[tauri::command]
pub fn restore_session(
    session_id: String,
    auth_service: State<'_, AuthService>,
    session_state: State<'_, SessionState>,
) -> AuthSessionResponse {
    let Some(session) = session_state.get_session(&session_id) else {
        return AuthSessionResponse {
            success: false,
            message: "Session not found".to_string(),
            session_id: None,
            current_user: None,
            must_change_password: false,
        };
    };

    match auth_service.find_user_by_id(session.user_id) {
        Ok(Some(user)) if user.is_active => {
            let refreshed_session = SessionUser {
                session_id: session.session_id.clone(),
                user_id: user.id,
                username: user.username,
                must_change_password: user.must_change_password,
                created_at: session.created_at,
            };
            session_state.upsert_session(refreshed_session.clone());

            AuthSessionResponse {
                success: true,
                message: "Session restored".to_string(),
                session_id: Some(refreshed_session.session_id.clone()),
                current_user: Some(refreshed_session.clone()),
                must_change_password: refreshed_session.must_change_password,
            }
        }
        Ok(_) => {
            session_state.remove_session(&session_id);
            AuthSessionResponse {
                success: false,
                message: "Session is no longer valid".to_string(),
                session_id: None,
                current_user: None,
                must_change_password: false,
            }
        }
        Err(error) => {
            log::error!("Failed to restore session {}: {}", session_id, error);
            AuthSessionResponse {
                success: false,
                message: "Failed to restore session".to_string(),
                session_id: None,
                current_user: None,
                must_change_password: false,
            }
        }
    }
}

#[tauri::command]
pub fn change_password(
    session_id: String,
    old_password: String,
    new_password: String,
    auth_service: State<'_, AuthService>,
    session_state: State<'_, SessionState>,
) -> CommonResponse {
    let Some(session) = session_state.get_session(&session_id) else {
        return CommonResponse {
            success: false,
            message: "Session not found".to_string(),
        };
    };

    match auth_service.change_password(session.user_id, &old_password, &new_password) {
        Ok(()) => {
            session_state.mark_password_changed(&session_id);
            CommonResponse {
                success: true,
                message: "Password updated successfully".to_string(),
            }
        }
        Err(error) => {
            log::warn!(
                "Password change failed for session {} and user {}: {}",
                session_id,
                session.username,
                error
            );
            CommonResponse {
                success: false,
                message: error
                    .to_string()
                    .replace("Authentication error: ", "")
                    .replace("Validation error: ", ""),
            }
        }
    }
}

#[tauri::command]
pub fn get_auth_bootstrap_status(auth_service: State<'_, AuthService>) -> BootstrapStatus {
    match auth_service.get_bootstrap_status() {
        Ok(status) => status,
        Err(error) => {
            log::error!("Failed to load bootstrap status: {}", error);
            BootstrapStatus {
                username: "admin".to_string(),
                created: false,
                has_default_admin: false,
                must_change_password: false,
            }
        }
    }
}
