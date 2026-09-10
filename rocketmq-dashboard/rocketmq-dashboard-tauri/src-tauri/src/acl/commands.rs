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

use super::{AclManager, types::*};
use crate::{
    audit::{AuditAccess, AuditAction, AuditManager, Audited},
    auth::SessionState,
    connection::ConnectionManager,
    error::{CommandResult, authorize_command},
};
use tauri::State;
#[tauri::command]
pub async fn list_acl_users(
    session_id: String,
    scope: AclScope,
    acl_manager: State<'_, AclManager>,
    session_state: State<'_, SessionState>,
    connection_manager: State<'_, ConnectionManager>,
    expected_revision: i64,
) -> CommandResult<Vec<AclUser>> {
    authorize_command(&session_id, &session_state).await?;
    connection_manager.check_revision(expected_revision)?;
    let result = acl_manager.list_users(scope).await.map_err(Into::into);
    connection_manager.check_revision(expected_revision)?;
    result
}

#[tauri::command]
pub async fn create_acl_user(
    session_id: String,
    request: AclUserChange,
    acl_manager: State<'_, AclManager>,
    session_state: State<'_, SessionState>,
    audit_manager: State<'_, AuditManager>,
    connection_manager: State<'_, ConnectionManager>,
    expected_revision: i64,
) -> CommandResult<Audited<AclUserResult>> {
    let access = AuditAccess::dashboard(&session_state, session_id);
    let manager = acl_manager.inner().clone();
    let connection = connection_manager.inner().clone();
    let resource = format!("{}@{}", request.username, request.scope.broker_addr);
    audit_manager
        .execute(
            access,
            AuditAction::CreateAclUser,
            Some(resource),
            move |audit| async move {
                let _lease = connection.mutation_lease(expected_revision, &audit).await?;
                manager.change_user(request, AclUserOperation::Create).await
            },
        )
        .await
}

#[tauri::command]
pub async fn update_acl_user(
    session_id: String,
    request: AclUserChange,
    acl_manager: State<'_, AclManager>,
    session_state: State<'_, SessionState>,
    audit_manager: State<'_, AuditManager>,
    connection_manager: State<'_, ConnectionManager>,
    expected_revision: i64,
) -> CommandResult<Audited<AclUserResult>> {
    let access = AuditAccess::dashboard(&session_state, session_id);
    let manager = acl_manager.inner().clone();
    let connection = connection_manager.inner().clone();
    let resource = format!("{}@{}", request.username, request.scope.broker_addr);
    audit_manager
        .execute(
            access,
            AuditAction::UpdateAclUser,
            Some(resource),
            move |audit| async move {
                let _lease = connection.mutation_lease(expected_revision, &audit).await?;
                manager.change_user(request, AclUserOperation::Update).await
            },
        )
        .await
}

#[tauri::command]
pub async fn delete_acl_user(
    session_id: String,
    request: AclUserDelete,
    acl_manager: State<'_, AclManager>,
    session_state: State<'_, SessionState>,
    audit_manager: State<'_, AuditManager>,
    connection_manager: State<'_, ConnectionManager>,
    expected_revision: i64,
) -> CommandResult<Audited<AclUserResult>> {
    let access = AuditAccess::dashboard(&session_state, session_id);
    let manager = acl_manager.inner().clone();
    let connection = connection_manager.inner().clone();
    let resource = format!("{}@{}", request.username, request.scope.broker_addr);
    audit_manager
        .execute(
            access,
            AuditAction::DeleteAclUser,
            Some(resource),
            move |audit| async move {
                let _lease = connection.mutation_lease(expected_revision, &audit).await?;
                manager.delete_user(request).await
            },
        )
        .await
}

#[tauri::command]
pub async fn list_acl_policies(
    session_id: String,
    scope: AclScope,
    acl_manager: State<'_, AclManager>,
    session_state: State<'_, SessionState>,
    connection_manager: State<'_, ConnectionManager>,
    expected_revision: i64,
) -> CommandResult<Vec<super::policy::PolicyView>> {
    authorize_command(&session_id, &session_state).await?;
    connection_manager.check_revision(expected_revision)?;
    let result = acl_manager.list_policies(scope).await.map_err(Into::into);
    connection_manager.check_revision(expected_revision)?;
    result
}

#[tauri::command]
pub async fn create_acl_policy(
    session_id: String,
    request: super::policy::PolicyChange,
    acl_manager: State<'_, AclManager>,
    session_state: State<'_, SessionState>,
    audit_manager: State<'_, AuditManager>,
    connection_manager: State<'_, ConnectionManager>,
    expected_revision: i64,
) -> CommandResult<Audited<super::policy::PolicyResult>> {
    let access = AuditAccess::dashboard(&session_state, session_id);
    let manager = acl_manager.inner().clone();
    let connection = connection_manager.inner().clone();
    let resource = format!("{}@{}", request.subject, request.scope.broker_addr);
    audit_manager
        .execute(
            access,
            AuditAction::CreateAclPolicy,
            Some(resource),
            move |audit| async move {
                let _lease = connection.mutation_lease(expected_revision, &audit).await?;
                manager
                    .change_policy(request, super::policy::PolicyOperation::Create)
                    .await
            },
        )
        .await
}

#[tauri::command]
pub async fn update_acl_policy(
    session_id: String,
    request: super::policy::PolicyChange,
    acl_manager: State<'_, AclManager>,
    session_state: State<'_, SessionState>,
    audit_manager: State<'_, AuditManager>,
    connection_manager: State<'_, ConnectionManager>,
    expected_revision: i64,
) -> CommandResult<Audited<super::policy::PolicyResult>> {
    let access = AuditAccess::dashboard(&session_state, session_id);
    let manager = acl_manager.inner().clone();
    let connection = connection_manager.inner().clone();
    let resource = format!("{}@{}", request.subject, request.scope.broker_addr);
    audit_manager
        .execute(
            access,
            AuditAction::UpdateAclPolicy,
            Some(resource),
            move |audit| async move {
                let _lease = connection.mutation_lease(expected_revision, &audit).await?;
                manager
                    .change_policy(request, super::policy::PolicyOperation::Update)
                    .await
            },
        )
        .await
}

#[tauri::command]
pub async fn delete_acl_policy(
    session_id: String,
    request: super::policy::PolicyDelete,
    acl_manager: State<'_, AclManager>,
    session_state: State<'_, SessionState>,
    audit_manager: State<'_, AuditManager>,
    connection_manager: State<'_, ConnectionManager>,
    expected_revision: i64,
) -> CommandResult<Audited<super::policy::PolicyResult>> {
    let access = AuditAccess::dashboard(&session_state, session_id);
    let manager = acl_manager.inner().clone();
    let connection = connection_manager.inner().clone();
    let resource = format!(
        "{}/{}/{}@{}",
        request.subject,
        request.policy_type.as_str(),
        request.resource,
        request.scope.broker_addr
    );
    audit_manager
        .execute(
            access,
            AuditAction::DeleteAclPolicy,
            Some(resource),
            move |audit| async move {
                let _lease = connection.mutation_lease(expected_revision, &audit).await?;
                manager.delete_policy(request).await
            },
        )
        .await
}
