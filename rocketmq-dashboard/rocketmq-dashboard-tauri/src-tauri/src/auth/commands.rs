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
use crate::auth::types::UserProfileResponse;
use tauri::State;

fn load_current_user_profile(
    session_id: &str,
    auth_service: &AuthService,
    session_state: &SessionState,
) -> UserProfileResponse {
    let Some(session) = session_state.get_session(session_id) else {
        return UserProfileResponse {
            success: false,
            message: "Session not found".to_string(),
            profile: None,
        };
    };

    match auth_service.get_user_profile(&session.session_id, session.user_id) {
        Ok(Some(profile)) if profile.is_active => UserProfileResponse {
            success: true,
            message: "Profile loaded".to_string(),
            profile: Some(profile),
        },
        Ok(_) => {
            session_state.remove_session(session_id);
            UserProfileResponse {
                success: false,
                message: "User profile is no longer available".to_string(),
                profile: None,
            }
        }
        Err(error) => {
            log::error!("Failed to load profile for session {}: {}", session_id, error);
            UserProfileResponse {
                success: false,
                message: "Failed to load user profile".to_string(),
                profile: None,
            }
        }
    }
}

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
pub fn get_current_user_profile(
    session_id: String,
    auth_service: State<'_, AuthService>,
    session_state: State<'_, SessionState>,
) -> UserProfileResponse {
    load_current_user_profile(&session_id, auth_service.inner(), session_state.inner())
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

#[cfg(test)]
mod tests {
    use crate::auth::db::AuthDb;
    use crate::auth::service::AuthService;
    use crate::auth::session::SessionState;
    use std::env;
    use std::fs;
    use std::path::PathBuf;
    use uuid::Uuid;

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new() -> Self {
            let path = env::temp_dir().join(format!(
                "rocketmq-dashboard-tauri-auth-command-tests-{}",
                Uuid::new_v4()
            ));
            fs::create_dir_all(&path).expect("failed to create test directory");
            Self { path }
        }

        fn db_path(&self) -> PathBuf {
            self.path.join("dashboard.db")
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    struct TestContext {
        _test_dir: TestDir,
        auth_service: AuthService,
        session_state: SessionState,
    }

    fn setup_context() -> TestContext {
        let test_dir = TestDir::new();
        let db = AuthDb::from_path(test_dir.db_path());
        db.init().expect("database initialization should succeed");
        let auth_service = AuthService::with_initial_password(db, "change-me-now");
        auth_service
            .bootstrap_default_admin()
            .expect("bootstrap should succeed");

        TestContext {
            _test_dir: test_dir,
            auth_service,
            session_state: SessionState::default(),
        }
    }

    #[test]
    fn get_current_user_profile_returns_active_profile() {
        let context = setup_context();
        let user = context
            .auth_service
            .authenticate("admin", "change-me-now")
            .expect("authentication should succeed");
        context
            .auth_service
            .update_last_login(user.id)
            .expect("last login should update");
        let session = context.session_state.create_session(&user);

        let response =
            super::load_current_user_profile(&session.session_id, &context.auth_service, &context.session_state);

        assert!(response.success);
        let profile = response.profile.expect("profile should exist");
        assert_eq!(profile.session_id, session.session_id);
        assert_eq!(profile.user_id, user.id);
        assert!(profile.last_login_at.is_some());
    }

    #[test]
    fn get_current_user_profile_rejects_unknown_session() {
        let context = setup_context();

        let response =
            super::load_current_user_profile("missing-session", &context.auth_service, &context.session_state);

        assert!(!response.success);
        assert_eq!(response.message, "Session not found");
        assert!(response.profile.is_none());
    }
}
