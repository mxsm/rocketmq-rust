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

use super::AuditManager;
use super::types::{AuditPage, AuditQuery};
use crate::auth::SessionState;
use crate::error::{CommandResult, authorize_command};
use tauri::State;

#[tauri::command]
pub async fn query_audit_events(
    session_id: String,
    query: AuditQuery,
    session_state: State<'_, SessionState>,
    audit_manager: State<'_, AuditManager>,
) -> CommandResult<AuditPage> {
    authorize_command(&session_id, &session_state).await?;
    audit_manager.query(query).await.map_err(Into::into)
}
