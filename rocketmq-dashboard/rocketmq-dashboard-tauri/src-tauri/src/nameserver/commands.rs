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

use crate::audit::{AuditAccess, AuditAction, AuditManager, Audited};
use crate::nameserver::NameServerManager;
use crate::nameserver::types::NameServerHomePageView;
use rocketmq_dashboard_common::NameServerMutationResult;
use tauri::State;

#[tauri::command]
pub async fn get_name_server_home_page(
    session_id: String,
    nameserver_manager: State<'_, NameServerManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<NameServerHomePageView> {
    authorize_command(&session_id, &session_state).await?;
    nameserver_manager.home_page_info().await.map_err(Into::into)
}

#[tauri::command]
pub async fn add_name_server(
    session_id: String,
    address: String,
    nameserver_manager: State<'_, NameServerManager>,
    session_state: State<'_, SessionState>,
    audit_manager: State<'_, AuditManager>,
) -> CommandResult<Audited<NameServerMutationResult>> {
    let access = AuditAccess::dashboard(&session_state, session_id);
    let nameserver_manager = nameserver_manager.inner().clone();
    let local_audit = audit_manager.inner().clone();
    audit_manager
        .execute(access, AuditAction::AddNameServer, None, move |_audit| async move {
            local_audit
                .run_local(move || nameserver_manager.add_name_server(&address))
                .await
        })
        .await
}

#[tauri::command]
pub async fn switch_name_server(
    session_id: String,
    address: String,
    nameserver_manager: State<'_, NameServerManager>,
    session_state: State<'_, SessionState>,
    audit_manager: State<'_, AuditManager>,
) -> CommandResult<Audited<NameServerMutationResult>> {
    let access = AuditAccess::dashboard(&session_state, session_id);
    let nameserver_manager = nameserver_manager.inner().clone();
    let local_audit = audit_manager.inner().clone();
    audit_manager
        .execute(access, AuditAction::SwitchNameServer, None, move |_audit| async move {
            local_audit
                .run_local(move || nameserver_manager.switch_name_server(&address))
                .await
        })
        .await
}

#[tauri::command]
pub async fn delete_name_server(
    session_id: String,
    address: String,
    nameserver_manager: State<'_, NameServerManager>,
    session_state: State<'_, SessionState>,
    audit_manager: State<'_, AuditManager>,
) -> CommandResult<Audited<NameServerMutationResult>> {
    let access = AuditAccess::dashboard(&session_state, session_id);
    let nameserver_manager = nameserver_manager.inner().clone();
    let local_audit = audit_manager.inner().clone();
    audit_manager
        .execute(access, AuditAction::DeleteNameServer, None, move |_audit| async move {
            local_audit
                .run_local(move || nameserver_manager.delete_name_server(&address))
                .await
        })
        .await
}

#[tauri::command]
pub async fn update_vip_channel(
    session_id: String,
    enabled: bool,
    nameserver_manager: State<'_, NameServerManager>,
    session_state: State<'_, SessionState>,
    audit_manager: State<'_, AuditManager>,
) -> CommandResult<Audited<NameServerMutationResult>> {
    let access = AuditAccess::dashboard(&session_state, session_id);
    let nameserver_manager = nameserver_manager.inner().clone();
    let local_audit = audit_manager.inner().clone();
    audit_manager
        .execute(access, AuditAction::UpdateVip, None, move |_audit| async move {
            local_audit
                .run_local(move || nameserver_manager.update_vip_channel(enabled))
                .await
        })
        .await
}

#[tauri::command]
pub async fn update_use_tls(
    session_id: String,
    enabled: bool,
    nameserver_manager: State<'_, NameServerManager>,
    session_state: State<'_, SessionState>,
    audit_manager: State<'_, AuditManager>,
) -> CommandResult<Audited<NameServerMutationResult>> {
    let access = AuditAccess::dashboard(&session_state, session_id);
    let nameserver_manager = nameserver_manager.inner().clone();
    let local_audit = audit_manager.inner().clone();
    audit_manager
        .execute(access, AuditAction::UpdateTls, None, move |_audit| async move {
            local_audit
                .run_local(move || nameserver_manager.update_use_tls(enabled))
                .await
        })
        .await
}
use crate::auth::SessionState;
use crate::error::CommandResult;
use crate::error::authorize_command;
