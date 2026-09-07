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
use crate::auth::types::UserProfile;
use crate::error::CommandResult;
use crate::error::DashboardError;
use crate::error::DashboardResult;
use tauri::State;

fn load_current_user_profile(
    session_id: &str,
    auth_service: &AuthService,
    session_state: &SessionState,
) -> DashboardResult<UserProfile> {
    let session = session_state.require_session(session_id)?;
    match auth_service.get_user_profile(session.user_id)? {
        Some(profile) if profile.is_active => Ok(profile),
        _ => {
            session_state.remove_session(session_id);
            Err(DashboardError::Unauthenticated)
        }
    }
}

fn login_user(
    username: &str,
    password: &str,
    auth_service: &AuthService,
    session_state: &SessionState,
) -> DashboardResult<AuthSessionResponse> {
    let user = auth_service.authenticate(username, password)?;
    auth_service.update_last_login(user.id)?;
    let session = session_state.create_session(&user);

    Ok(AuthSessionResponse {
        session_id: session.session_id.clone(),
        current_user: session,
    })
}

#[tauri::command]
pub fn login(
    username: String,
    password: String,
    auth_service: State<'_, AuthService>,
    session_state: State<'_, SessionState>,
) -> CommandResult<AuthSessionResponse> {
    login_user(&username, &password, auth_service.inner(), session_state.inner()).map_err(Into::into)
}

#[tauri::command]
pub fn logout(session_id: String, session_state: State<'_, SessionState>) -> CommandResult<CommonResponse> {
    session_state
        .require_session(&session_id)
        .map_err(crate::error::CommandError::from)?;
    session_state.remove_session(&session_id);
    Ok(CommonResponse {
        message: "Logged out successfully".to_string(),
    })
}

fn restore_user_session(
    session_id: &str,
    auth_service: &AuthService,
    session_state: &SessionState,
) -> DashboardResult<AuthSessionResponse> {
    let session = session_state.require_session(session_id)?;
    match auth_service.find_user_by_id(session.user_id)? {
        Some(user) if user.is_active => {
            let refreshed_session = SessionUser {
                session_id: session.session_id,
                user_id: user.id,
                username: user.username,
                must_change_password: user.must_change_password,
                created_at: session.created_at,
            };
            session_state.upsert_session(refreshed_session.clone());
            Ok(AuthSessionResponse {
                session_id: refreshed_session.session_id.clone(),
                current_user: refreshed_session,
            })
        }
        _ => {
            session_state.remove_session(session_id);
            Err(DashboardError::Unauthenticated)
        }
    }
}

#[tauri::command]
pub fn restore_session(
    session_id: String,
    auth_service: State<'_, AuthService>,
    session_state: State<'_, SessionState>,
) -> CommandResult<AuthSessionResponse> {
    restore_user_session(&session_id, auth_service.inner(), session_state.inner()).map_err(Into::into)
}

fn change_user_password(
    session_id: &str,
    old_password: &str,
    new_password: &str,
    auth_service: &AuthService,
    session_state: &SessionState,
) -> DashboardResult<CommonResponse> {
    let session = session_state.require_session(session_id)?;
    auth_service.change_password(session.user_id, old_password, new_password)?;
    session_state.mark_password_changed(session_id);
    Ok(CommonResponse {
        message: "Password updated successfully".to_string(),
    })
}

#[tauri::command]
pub fn change_password(
    session_id: String,
    old_password: String,
    new_password: String,
    auth_service: State<'_, AuthService>,
    session_state: State<'_, SessionState>,
) -> CommandResult<CommonResponse> {
    change_user_password(
        &session_id,
        &old_password,
        &new_password,
        auth_service.inner(),
        session_state.inner(),
    )
    .map_err(Into::into)
}

#[tauri::command]
pub fn get_current_user_profile(
    session_id: String,
    auth_service: State<'_, AuthService>,
    session_state: State<'_, SessionState>,
) -> CommandResult<UserProfile> {
    load_current_user_profile(&session_id, auth_service.inner(), session_state.inner()).map_err(Into::into)
}

#[tauri::command]
pub fn get_auth_bootstrap_status(auth_service: State<'_, AuthService>) -> CommandResult<BootstrapStatus> {
    auth_service.get_bootstrap_status().map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::load_current_user_profile;
    use super::login_user;
    use super::restore_user_session;
    use crate::auth::db::AuthDb;
    use crate::auth::service::AuthService;
    use crate::auth::session::SessionState;
    use crate::error::CommandError;
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
    fn auth_command_matrix_enforces_session_and_password_state() {
        let context = setup_context();
        let missing = load_current_user_profile("missing", &context.auth_service, &context.session_state)
            .expect_err("missing session should fail");
        assert_eq!(CommandError::from(missing).code, "auth.session.invalid");

        let login = login_user("admin", "change-me-now", &context.auth_service, &context.session_state)
            .expect("authentication should succeed");
        assert!(context.session_state.authorize_dashboard(&login.session_id).is_err());

        context.session_state.mark_password_changed(&login.session_id);
        assert!(context.session_state.authorize_dashboard(&login.session_id).is_ok());
        assert!(load_current_user_profile(&login.session_id, &context.auth_service, &context.session_state).is_ok());
    }

    #[test]
    fn authentication_failure_uses_structured_redacted_error() {
        let context = setup_context();
        let error = login_user("admin", "wrong-password", &context.auth_service, &context.session_state)
            .expect_err("invalid credentials should fail");
        let public = CommandError::from(error);

        assert_eq!(public.code, "auth.credentials.invalid");
        assert_eq!(public.message, "Authentication failed.");
    }

    #[test]
    fn restoring_unknown_session_is_an_error() {
        let context = setup_context();
        let error = restore_user_session("missing", &context.auth_service, &context.session_state)
            .expect_err("unknown session should fail");
        assert_eq!(CommandError::from(error).code, "auth.session.invalid");
    }
}
