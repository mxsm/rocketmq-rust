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

mod repository;
use crate::{
    audit::{AuditAccess, AuditAction, AuditContext, AuditManager, Audited},
    auth::SessionState,
    connection::ConnectionManager,
    error::{CommandResult, DashboardError, DashboardResult, authorize_command},
    persistence::StorageManager,
};
use serde::{Deserialize, Serialize};
use tauri::State;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MonitorRule {
    consumer_group: String,
    min_count: i64,
    max_diff_total: i64,
    revision: i64,
    created_at_ms: i64,
    updated_at_ms: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SaveRule {
    consumer_group: String,
    min_count: i64,
    max_diff_total: i64,
    expected_revision: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DeleteRule {
    consumer_group: String,
    expected_revision: i64,
}

#[derive(Serialize)]
pub(crate) struct MutationReceipt {
    message: &'static str,
}
impl crate::audit::types::AuditReceipt for MutationReceipt {
    fn summary(&self) -> crate::audit::types::Summary {
        crate::audit::types::Summary::count(1, 0)
    }
}

#[derive(Clone)]
pub(crate) struct MonitorManager {
    storage: StorageManager,
    connections: ConnectionManager,
}
impl MonitorManager {
    pub(crate) fn new(storage: StorageManager, connections: ConnectionManager) -> Self {
        Self { storage, connections }
    }
    fn environment(&self) -> DashboardResult<String> {
        self.connections
            .snapshot()?
            .environment_id
            .ok_or_else(|| DashboardError::Configuration("Select a NameServer environment.".into()))
    }
    async fn list(&self) -> DashboardResult<Vec<MonitorRule>> {
        let environment = self.environment()?;
        self.storage
            .run("monitor-list", move |connection| {
                repository::list(connection, &environment)
            })
            .await
    }
    async fn change(&self, revision: i64, change: Change, audit: AuditContext) -> DashboardResult<MutationReceipt> {
        let _lease = self.connections.mutation_lease(revision, &audit).await?;
        let environment = self.environment()?;
        self.storage
            .run("monitor-change", move |connection| {
                repository::change(connection, &environment, change, &audit)?;
                Ok(MutationReceipt {
                    message: "Monitor rule saved.",
                })
            })
            .await
    }
}
enum Change {
    Save(SaveRule),
    Delete(DeleteRule),
}

#[tauri::command]
pub async fn list_consumer_monitor_rules(
    session_id: String,
    expected_revision: i64,
    session_state: State<'_, SessionState>,
    connection_manager: State<'_, ConnectionManager>,
    monitor: State<'_, MonitorManager>,
) -> CommandResult<Vec<MonitorRule>> {
    authorize_command(&session_id, &session_state).await?;
    connection_manager.check_revision(expected_revision)?;
    let result = monitor.list().await.map_err(Into::into);
    connection_manager.check_revision(expected_revision)?;
    result
}

#[tauri::command]
pub async fn save_consumer_monitor_rule(
    session_id: String,
    expected_revision: i64,
    request: SaveRule,
    session_state: State<'_, SessionState>,
    monitor: State<'_, MonitorManager>,
    audit_manager: State<'_, AuditManager>,
) -> CommandResult<Audited<MutationReceipt>> {
    let access = AuditAccess::dashboard(&session_state, session_id);
    let manager = monitor.inner().clone();
    audit_manager
        .execute(
            access,
            AuditAction::SaveMonitor,
            Some(request.consumer_group.clone()),
            move |audit| async move { manager.change(expected_revision, Change::Save(request), audit).await },
        )
        .await
}

#[tauri::command]
pub async fn delete_consumer_monitor_rule(
    session_id: String,
    expected_revision: i64,
    request: DeleteRule,
    session_state: State<'_, SessionState>,
    monitor: State<'_, MonitorManager>,
    audit_manager: State<'_, AuditManager>,
) -> CommandResult<Audited<MutationReceipt>> {
    let access = AuditAccess::dashboard(&session_state, session_id);
    let manager = monitor.inner().clone();
    audit_manager
        .execute(
            access,
            AuditAction::DeleteMonitor,
            Some(request.consumer_group.clone()),
            move |audit| async move { manager.change(expected_revision, Change::Delete(request), audit).await },
        )
        .await
}
