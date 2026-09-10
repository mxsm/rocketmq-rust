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
use crate::connection::ConnectionManager;
use crate::consumer::scope::{ScopedConsumerGroupRequest, ScopedConsumerListRequest};
use crate::consumer::service::ConsumerManager;
use crate::consumer::types::ConsumerConfigView;
use crate::consumer::types::ConsumerConnectionView;
use crate::consumer::types::ConsumerGroupListItem;
use crate::consumer::types::ConsumerGroupListResponse;
use crate::consumer::types::ConsumerMutationResult;
use crate::consumer::types::ConsumerTopicDetailView;
use crate::error::CommandResult;
use crate::error::authorize_command;
use rocketmq_dashboard_common::ConsumerConfigQueryRequest;
use rocketmq_dashboard_common::ConsumerConnectionQueryRequest;
use rocketmq_dashboard_common::ConsumerCreateOrUpdateRequest;
use rocketmq_dashboard_common::ConsumerDeleteRequest;
use rocketmq_dashboard_common::ConsumerGroupListRequest;
use rocketmq_dashboard_common::ConsumerGroupRefreshRequest;
use rocketmq_dashboard_common::ConsumerTopicDetailQueryRequest;
use tauri::State;

#[tauri::command]
pub async fn query_consumer_groups(
    session_id: String,
    request: ScopedConsumerListRequest,
    consumer_manager: State<'_, ConsumerManager>,
    session_state: State<'_, SessionState>,
    expected_revision: i64,
    connection_manager: State<'_, ConnectionManager>,
) -> CommandResult<ConsumerGroupListResponse> {
    authorize_command(&session_id, &session_state).await?;
    connection_manager.check_revision(expected_revision)?;
    let address = request.scope.address(&connection_manager.snapshot()?)?;
    let request = ConsumerGroupListRequest {
        skip_sys_group: request.skip_sys_group,
        address,
    };
    let result = consumer_manager
        .query_consumer_groups(request)
        .await
        .map_err(Into::into);
    connection_manager.check_revision(expected_revision)?;
    result
}

#[tauri::command]
pub async fn refresh_consumer_group(
    session_id: String,
    request: ScopedConsumerGroupRequest,
    consumer_manager: State<'_, ConsumerManager>,
    session_state: State<'_, SessionState>,
    expected_revision: i64,
    connection_manager: State<'_, ConnectionManager>,
) -> CommandResult<ConsumerGroupListItem> {
    authorize_command(&session_id, &session_state).await?;
    connection_manager.check_revision(expected_revision)?;
    let address = request.scope.address(&connection_manager.snapshot()?)?;
    let request = ConsumerGroupRefreshRequest {
        consumer_group: request.consumer_group,
        address,
    };
    let result = consumer_manager
        .refresh_consumer_group(request)
        .await
        .map_err(Into::into);
    connection_manager.check_revision(expected_revision)?;
    result
}

#[tauri::command]
pub async fn refresh_all_consumer_groups(
    session_id: String,
    request: ScopedConsumerListRequest,
    consumer_manager: State<'_, ConsumerManager>,
    session_state: State<'_, SessionState>,
    expected_revision: i64,
    connection_manager: State<'_, ConnectionManager>,
) -> CommandResult<ConsumerGroupListResponse> {
    authorize_command(&session_id, &session_state).await?;
    connection_manager.check_revision(expected_revision)?;
    let address = request.scope.address(&connection_manager.snapshot()?)?;
    let request = ConsumerGroupListRequest {
        skip_sys_group: request.skip_sys_group,
        address,
    };
    let result = consumer_manager
        .refresh_all_consumer_groups(request)
        .await
        .map_err(Into::into);
    connection_manager.check_revision(expected_revision)?;
    result
}

#[tauri::command]
pub async fn query_consumer_connection(
    session_id: String,
    request: ScopedConsumerGroupRequest,
    consumer_manager: State<'_, ConsumerManager>,
    session_state: State<'_, SessionState>,
    expected_revision: i64,
    connection_manager: State<'_, ConnectionManager>,
) -> CommandResult<ConsumerConnectionView> {
    authorize_command(&session_id, &session_state).await?;
    connection_manager.check_revision(expected_revision)?;
    let address = request.scope.address(&connection_manager.snapshot()?)?;
    let request = ConsumerConnectionQueryRequest {
        consumer_group: request.consumer_group,
        address,
    };
    let result = consumer_manager
        .query_consumer_connection(request)
        .await
        .map_err(Into::into);
    connection_manager.check_revision(expected_revision)?;
    result
}

#[tauri::command]
pub async fn query_consumer_topic_detail(
    session_id: String,
    request: ScopedConsumerGroupRequest,
    consumer_manager: State<'_, ConsumerManager>,
    session_state: State<'_, SessionState>,
    expected_revision: i64,
    connection_manager: State<'_, ConnectionManager>,
) -> CommandResult<ConsumerTopicDetailView> {
    authorize_command(&session_id, &session_state).await?;
    connection_manager.check_revision(expected_revision)?;
    let address = request.scope.address(&connection_manager.snapshot()?)?;
    let request = ConsumerTopicDetailQueryRequest {
        consumer_group: request.consumer_group,
        address,
    };
    let result = consumer_manager
        .query_consumer_topic_detail(request)
        .await
        .map_err(Into::into);
    connection_manager.check_revision(expected_revision)?;
    result
}

#[tauri::command]
pub async fn query_consumer_config(
    session_id: String,
    request: ConsumerConfigQueryRequest,
    consumer_manager: State<'_, ConsumerManager>,
    session_state: State<'_, SessionState>,
    expected_revision: i64,
    connection_manager: State<'_, ConnectionManager>,
) -> CommandResult<ConsumerConfigView> {
    authorize_command(&session_id, &session_state).await?;
    connection_manager.check_revision(expected_revision)?;
    let result = consumer_manager
        .query_consumer_config(request)
        .await
        .map_err(Into::into);
    connection_manager.check_revision(expected_revision)?;
    result
}

#[tauri::command]
pub async fn query_consumer_config_summary(
    session_id: String,
    consumer_group: String,
    consumer_manager: State<'_, ConsumerManager>,
    session_state: State<'_, SessionState>,
    expected_revision: i64,
    connection_manager: State<'_, ConnectionManager>,
) -> CommandResult<super::service::config_summary::ConsumerConfigSummary> {
    authorize_command(&session_id, &session_state).await?;
    connection_manager.check_revision(expected_revision)?;
    let result = consumer_manager
        .query_consumer_config_summary(consumer_group)
        .await
        .map_err(Into::into);
    connection_manager.check_revision(expected_revision)?;
    result
}

#[tauri::command]
pub async fn create_or_update_consumer_group(
    session_id: String,
    request: ConsumerCreateOrUpdateRequest,
    consumer_manager: State<'_, ConsumerManager>,
    session_state: State<'_, SessionState>,
    audit_manager: State<'_, AuditManager>,
    connection_manager: State<'_, ConnectionManager>,
    expected_revision: i64,
) -> CommandResult<Audited<ConsumerMutationResult>> {
    let connection_manager = connection_manager.inner().clone();
    let access = AuditAccess::dashboard(&session_state, session_id);
    let consumer_manager = consumer_manager.inner().clone();
    audit_manager
        .execute(
            access,
            AuditAction::UpsertConsumer,
            Some(request.consumer_group.clone()),
            move |audit| async move {
                let _lease = connection_manager.mutation_lease(expected_revision, &audit).await?;
                consumer_manager.create_or_update_consumer_group(request).await
            },
        )
        .await
}

#[tauri::command]
pub async fn delete_consumer_group(
    session_id: String,
    request: ConsumerDeleteRequest,
    consumer_manager: State<'_, ConsumerManager>,
    session_state: State<'_, SessionState>,
    audit_manager: State<'_, AuditManager>,
    connection_manager: State<'_, ConnectionManager>,
    expected_revision: i64,
) -> CommandResult<Audited<ConsumerMutationResult>> {
    let connection_manager = connection_manager.inner().clone();
    let access = AuditAccess::dashboard(&session_state, session_id);
    let consumer_manager = consumer_manager.inner().clone();
    audit_manager
        .execute(
            access,
            AuditAction::DeleteConsumer,
            Some(request.consumer_group.clone()),
            move |audit| async move {
                let _lease = connection_manager.mutation_lease(expected_revision, &audit).await?;
                consumer_manager.delete_consumer_group(request).await
            },
        )
        .await
}
use crate::auth::SessionState;
