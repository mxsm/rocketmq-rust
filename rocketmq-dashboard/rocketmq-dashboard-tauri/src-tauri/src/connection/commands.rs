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

use super::{
    ConnectionChange, ConnectionManager, ConnectionMutationResult, ConnectionSettingsView, ReplaceNameServersRequest,
};
use crate::audit::{AuditAccess, AuditAction, AuditManager, Audited};
use crate::auth::SessionState;
use crate::error::{CommandResult, authorize_command};
use tauri::State;

#[tauri::command]
pub async fn get_connection_settings(
    session_id: String,
    session_state: State<'_, SessionState>,
    connection_manager: State<'_, ConnectionManager>,
) -> CommandResult<ConnectionSettingsView> {
    authorize_command(&session_id, &session_state).await?;
    connection_manager.snapshot().map_err(Into::into)
}

#[tauri::command]
pub async fn replace_name_servers(
    session_id: String,
    request: ReplaceNameServersRequest,
    session_state: State<'_, SessionState>,
    connection_manager: State<'_, ConnectionManager>,
    audit_manager: State<'_, AuditManager>,
) -> CommandResult<Audited<ConnectionMutationResult>> {
    let access = AuditAccess::dashboard(&session_state, session_id);
    let manager = connection_manager.inner().clone();
    audit_manager
        .execute(access, AuditAction::ReplaceNameServers, None, move |audit| async move {
            manager
                .change(
                    request.expected_revision,
                    ConnectionChange::Replace {
                        addresses: request.addresses,
                        current_endpoint: request.current_endpoint,
                    },
                    audit,
                )
                .await
        })
        .await
}
