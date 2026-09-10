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

use super::SessionState;
use super::types::{
    AuthSessionResponse, BootstrapStatus, CommonResponse, RevokeSessionsResponse, SessionPage, UserProfile,
};
use crate::audit::{AuditAccess, AuditAction, AuditManager, Audited};
use crate::error::CommandResult;
use tauri::State;

#[tauri::command]
pub async fn login(
    username: String,
    password: String,
    session_state: State<'_, SessionState>,
    audit_manager: State<'_, AuditManager>,
) -> CommandResult<Audited<AuthSessionResponse>> {
    let access = AuditAccess::Login;
    let session_state = session_state.inner().clone();
    audit_manager
        .execute(access, AuditAction::Login, None, move |audit| async move {
            let session_state = session_state.with_audit(audit);
            session_state.login(username, password).await
        })
        .await
}

#[tauri::command]
pub async fn logout(
    session_id: String,
    session_state: State<'_, SessionState>,
    audit_manager: State<'_, AuditManager>,
) -> CommandResult<Audited<CommonResponse>> {
    let access = AuditAccess::account(&session_state, session_id.clone());
    let session_state = session_state.inner().clone();
    audit_manager
        .execute(access, AuditAction::Logout, None, move |audit| async move {
            let session_state = session_state.with_audit(audit);
            session_state.logout(session_id).await?;
            Ok(CommonResponse {
                message: "Logged out successfully".into(),
            })
        })
        .await
}

#[tauri::command]
pub async fn restore_session(
    session_id: String,
    session_state: State<'_, SessionState>,
) -> CommandResult<AuthSessionResponse> {
    session_state.restore(session_id).await.map_err(Into::into)
}

#[tauri::command]
pub async fn change_password(
    session_id: String,
    old_password: String,
    new_password: String,
    session_state: State<'_, SessionState>,
    audit_manager: State<'_, AuditManager>,
) -> CommandResult<Audited<CommonResponse>> {
    let access = AuditAccess::account(&session_state, session_id.clone());
    let session_state = session_state.inner().clone();
    audit_manager
        .execute(access, AuditAction::ChangePassword, None, move |audit| async move {
            let session_state = session_state.with_audit(audit);
            session_state
                .change_password(session_id, old_password, new_password)
                .await?;
            Ok(CommonResponse {
                message: "Password updated. All sessions were revoked; sign in again.".into(),
            })
        })
        .await
}

#[tauri::command]
pub async fn get_current_user_profile(
    session_id: String,
    session_state: State<'_, SessionState>,
) -> CommandResult<UserProfile> {
    session_state.profile(session_id).await.map_err(Into::into)
}

#[tauri::command]
pub async fn get_auth_bootstrap_status(session_state: State<'_, SessionState>) -> CommandResult<BootstrapStatus> {
    session_state.bootstrap_status().await.map_err(Into::into)
}

#[tauri::command]
pub async fn list_sessions(
    session_id: String,
    username: Option<String>,
    cursor: Option<String>,
    limit: Option<usize>,
    session_state: State<'_, SessionState>,
) -> CommandResult<SessionPage> {
    session_state
        .list(session_id, username, cursor, limit)
        .await
        .map_err(Into::into)
}

#[tauri::command]
pub async fn revoke_user_sessions(
    session_id: String,
    username: String,
    session_state: State<'_, SessionState>,
    audit_manager: State<'_, AuditManager>,
) -> CommandResult<Audited<RevokeSessionsResponse>> {
    let access = AuditAccess::account(&session_state, session_id.clone());
    let session_state = session_state.inner().clone();
    audit_manager
        .execute(access, AuditAction::RevokeSessions, None, move |audit| async move {
            let session_state = session_state.with_audit(audit);
            session_state.revoke(session_id, username).await
        })
        .await
}
