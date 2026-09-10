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
use rocketmq_dashboard_common::ProxyConfigSnapshot;
use tauri::State;

#[tauri::command]
pub async fn get_proxy_home_page(
    session_id: String,
    session_state: State<'_, SessionState>,
    connection_manager: State<'_, ConnectionManager>,
) -> CommandResult<ConnectionProjection<ProxyConfigSnapshot>> {
    authorize_command(&session_id, &session_state).await?;
    let settings = connection_manager.snapshot()?;
    Ok(ConnectionProjection {
        value: settings.proxy.clone(),
        settings,
    })
}

#[tauri::command]
pub async fn add_proxy_addr(
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
        .execute(access, AuditAction::AddProxy, None, move |audit| async move {
            manager
                .change(
                    expected_revision,
                    ConnectionChange::Add {
                        kind: EndpointKind::Proxy,
                        address,
                    },
                    audit,
                )
                .await
        })
        .await
}

#[tauri::command]
pub async fn switch_proxy_addr(
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
        .execute(access, AuditAction::SwitchProxy, None, move |audit| async move {
            manager
                .change(
                    expected_revision,
                    ConnectionChange::Switch {
                        kind: EndpointKind::Proxy,
                        address,
                    },
                    audit,
                )
                .await
        })
        .await
}

#[tauri::command]
pub async fn delete_proxy_addr(
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
        .execute(access, AuditAction::DeleteProxy, None, move |audit| async move {
            manager
                .change(
                    expected_revision,
                    ConnectionChange::Delete {
                        kind: EndpointKind::Proxy,
                        address,
                    },
                    audit,
                )
                .await
        })
        .await
}
