// Copyright 2026 The RocketMQ Rust Authors
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
use crate::auth::SessionState;
use crate::connection::{
    ConnectionChange, ConnectionManager, ConnectionMutationResult, ConnectionProjection, EndpointKind,
};
use crate::error::{CommandResult, authorize_command};
use crate::nameserver::{NameServerManager, types::NameServerHomePageView};
use tauri::State;

#[tauri::command]
pub async fn get_name_server_home_page(
    session_id: String,
    session_state: State<'_, SessionState>,
    nameserver_manager: State<'_, NameServerManager>,
    connection_manager: State<'_, ConnectionManager>,
) -> CommandResult<ConnectionProjection<NameServerHomePageView>> {
    authorize_command(&session_id, &session_state).await?;
    let settings = connection_manager.snapshot()?;
    let value = nameserver_manager
        .home_page_for_snapshot(settings.nameserver.clone())
        .await?;
    Ok(ConnectionProjection { value, settings })
}

#[tauri::command]
pub async fn add_name_server(
    session_id: String,
    address: String,
    expected_revision: i64,
    session_state: State<'_, SessionState>,
    connection_manager: State<'_, ConnectionManager>,
    audit_manager: State<'_, AuditManager>,
) -> CommandResult<Audited<ConnectionMutationResult>> {
    let access = AuditAccess::dashboard(&session_state, session_id);
    let manager = connection_manager.inner().clone();
    audit_manager
        .execute(access, AuditAction::AddNameServer, None, move |audit| async move {
            manager
                .change(
                    expected_revision,
                    ConnectionChange::Add {
                        kind: EndpointKind::NameServer,
                        address,
                    },
                    audit,
                )
                .await
        })
        .await
}

#[tauri::command]
pub async fn switch_name_server(
    session_id: String,
    address: String,
    expected_revision: i64,
    session_state: State<'_, SessionState>,
    connection_manager: State<'_, ConnectionManager>,
    audit_manager: State<'_, AuditManager>,
) -> CommandResult<Audited<ConnectionMutationResult>> {
    let access = AuditAccess::dashboard(&session_state, session_id);
    let manager = connection_manager.inner().clone();
    audit_manager
        .execute(access, AuditAction::SwitchNameServer, None, move |audit| async move {
            manager
                .change(
                    expected_revision,
                    ConnectionChange::Switch {
                        kind: EndpointKind::NameServer,
                        address,
                    },
                    audit,
                )
                .await
        })
        .await
}

#[tauri::command]
pub async fn delete_name_server(
    session_id: String,
    address: String,
    expected_revision: i64,
    session_state: State<'_, SessionState>,
    connection_manager: State<'_, ConnectionManager>,
    audit_manager: State<'_, AuditManager>,
) -> CommandResult<Audited<ConnectionMutationResult>> {
    let access = AuditAccess::dashboard(&session_state, session_id);
    let manager = connection_manager.inner().clone();
    audit_manager
        .execute(access, AuditAction::DeleteNameServer, None, move |audit| async move {
            manager
                .change(
                    expected_revision,
                    ConnectionChange::Delete {
                        kind: EndpointKind::NameServer,
                        address,
                    },
                    audit,
                )
                .await
        })
        .await
}

#[tauri::command]
pub async fn update_vip_channel(
    session_id: String,
    enabled: bool,
    expected_revision: i64,
    session_state: State<'_, SessionState>,
    connection_manager: State<'_, ConnectionManager>,
    audit_manager: State<'_, AuditManager>,
) -> CommandResult<Audited<ConnectionMutationResult>> {
    let access = AuditAccess::dashboard(&session_state, session_id);
    let manager = connection_manager.inner().clone();
    audit_manager
        .execute(access, AuditAction::UpdateVip, None, move |audit| async move {
            manager
                .change(expected_revision, ConnectionChange::Vip(enabled), audit)
                .await
        })
        .await
}

#[tauri::command]
pub async fn update_use_tls(
    session_id: String,
    enabled: bool,
    expected_revision: i64,
    session_state: State<'_, SessionState>,
    connection_manager: State<'_, ConnectionManager>,
    audit_manager: State<'_, AuditManager>,
) -> CommandResult<Audited<ConnectionMutationResult>> {
    let access = AuditAccess::dashboard(&session_state, session_id);
    let manager = connection_manager.inner().clone();
    audit_manager
        .execute(access, AuditAction::UpdateTls, None, move |audit| async move {
            manager
                .change(expected_revision, ConnectionChange::Tls(enabled), audit)
                .await
        })
        .await
}
