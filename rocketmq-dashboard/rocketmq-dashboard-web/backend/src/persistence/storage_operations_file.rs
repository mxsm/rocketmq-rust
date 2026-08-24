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

use super::format::BackupData;
use super::format::BackupSession;
use super::format::sync_directory;
use super::validation::parse_token_hash;
use super::validation::verify_data;
use crate::config::StorageConfig;
use crate::model::NewSession;
use crate::model::StorageBackend;
use crate::persistence::DashboardPersistence;
use crate::persistence::audit_repository::AuditQuery;
use crate::persistence::error::PersistenceError;
use crate::persistence::file_store::FilePersistence;
use crate::persistence::session_repository::SessionQuery;
use rocketmq_runtime::ChildServiceContext;
use std::fs;

pub(super) async fn snapshot_file(
    persistence: &DashboardPersistence,
    store: &FilePersistence,
) -> Result<BackupData, PersistenceError> {
    let mut data = BackupData::with_backend(StorageBackend::File);
    data.environments = persistence.list_environments().await?;
    for environment in &data.environments {
        data.monitors
            .extend(persistence.list_monitor_rules(&environment.environment_id).await?);
    }
    data.history = store.snapshot_history_for_operations().await?;
    let mut cursor = None;
    loop {
        let page = persistence
            .list_sessions(SessionQuery {
                username: None,
                cursor: cursor.take(),
                limit: 200,
            })
            .await?;
        data.sessions.extend(page.records.into_iter().map(BackupSession::from));
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    let mut cursor = None;
    loop {
        let page = persistence
            .query_audit_events(AuditQuery {
                start_ms: 0,
                end_ms: i64::MAX,
                actor: None,
                action: None,
                outcome: None,
                environment_id: None,
                cursor: cursor.take(),
                limit: 200,
            })
            .await?;
        data.audit.extend(page.events);
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    data.refresh_counts()?;
    Ok(data)
}

/// Populates a sibling File layout and publishes it with a single directory
/// rename. The configured target must not exist; an existing target could be
/// an active dashboard layout and is never replaced by this command.
pub async fn restore_file_target(
    data: &BackupData,
    target_config: &StorageConfig,
    service_context: ChildServiceContext,
) -> Result<(), PersistenceError> {
    verify_data(data, Some(StorageBackend::File))?;
    let target = &target_config.data_path;
    if target.exists() {
        return Err(PersistenceError::Conflict);
    }
    let parent = target
        .parent()
        .ok_or_else(|| PersistenceError::InvalidConfig("storage target parent is missing".to_string()))?;
    fs::create_dir_all(parent).map_err(PersistenceError::Io)?;
    let target_name = target
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| PersistenceError::InvalidConfig("storage target is invalid".to_string()))?;
    let stage_path = parent.join(format!(".{target_name}.restore-{}", uuid::Uuid::now_v7()));
    let mut stage_config = target_config.clone();
    stage_config.data_path = stage_path.clone();
    let stage =
        DashboardPersistence::initialize(&stage_config, service_context.component("storage-restore-stage")).await?;
    let result = import_file_records(&stage, data).await;
    drop(stage);
    if let Err(error) = result {
        let _ = fs::remove_dir_all(&stage_path);
        return Err(error);
    }
    fs::rename(&stage_path, target).map_err(PersistenceError::Io)?;
    sync_directory(parent)
}

async fn import_file_records(target: &DashboardPersistence, data: &BackupData) -> Result<(), PersistenceError> {
    for environment in &data.environments {
        target.create_environment(environment.clone()).await?;
        for revision in 1..environment.revision.0 {
            target
                .update_environment(crate::persistence::Revision(revision), environment.clone())
                .await?;
        }
    }
    for monitor in &data.monitors {
        target
            .upsert_monitor_rule(monitor.clone(), crate::persistence::Revision(0))
            .await?;
        for revision in 1..monitor.revision.0 {
            target
                .upsert_monitor_rule(monitor.clone(), crate::persistence::Revision(revision))
                .await?;
        }
    }
    for history in &data.history {
        target.append_history(vec![history.clone()], None).await?;
    }
    for session in &data.sessions {
        target
            .create_session(NewSession {
                session_id: session.session_id.clone(),
                token_hash: parse_token_hash(&session.token_hash)?,
                username: session.username.clone(),
                created_at_ms: session.created_at_ms,
                expires_at_ms: session.expires_at_ms,
            })
            .await?;
        let token_hash = parse_token_hash(&session.token_hash)?;
        if session.last_seen_at_ms > session.created_at_ms {
            target.touch_session(&token_hash, session.last_seen_at_ms).await?;
        }
        if let Some(revoked_at_ms) = session.revoked_at_ms {
            target.revoke_session(&token_hash, revoked_at_ms).await?;
        }
    }
    for audit in &data.audit {
        target.append_audit_event(audit.clone()).await?;
    }
    Ok(())
}
