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
use crate::connection::ConnectionManager;
use crate::topic::batch::TopicBatchResult;
use crate::topic::service::TopicManager;
use crate::topic::types::TopicConfigView;
use crate::topic::types::TopicConsumerGroupListResponse;
use crate::topic::types::TopicConsumerInfoResponse;
use crate::topic::types::TopicListResponse;
use crate::topic::types::TopicMutationResult;
use crate::topic::types::TopicRouteView;
use crate::topic::types::TopicSendMessageResult;
use crate::topic::types::TopicStatusView;
use rocketmq_dashboard_common::DeleteTopicByBrokerRequest;
use rocketmq_dashboard_common::DeleteTopicRequest;
use rocketmq_dashboard_common::ResetOffsetRequest;
use rocketmq_dashboard_common::SendTopicMessageRequest;
use rocketmq_dashboard_common::TopicConfigQueryRequest;
use rocketmq_dashboard_common::TopicConfigRequest;
use rocketmq_dashboard_common::TopicListRequest;
use rocketmq_dashboard_common::TopicQueryRequest;
use tauri::State;

#[tauri::command]
pub async fn get_topic_list(
    session_id: String,
    request: TopicListRequest,
    topic_manager: State<'_, TopicManager>,
    session_state: State<'_, SessionState>,
    expected_revision: i64,
    connection_manager: State<'_, ConnectionManager>,
) -> CommandResult<TopicListResponse> {
    authorize_command(&session_id, &session_state).await?;
    connection_manager.check_revision(expected_revision)?;
    let result = topic_manager.get_topic_list(request).await.map_err(Into::into);
    connection_manager.check_revision(expected_revision)?;
    result
}

#[tauri::command]
pub async fn get_topic_route(
    session_id: String,
    request: TopicQueryRequest,
    topic_manager: State<'_, TopicManager>,
    session_state: State<'_, SessionState>,
    expected_revision: i64,
    connection_manager: State<'_, ConnectionManager>,
) -> CommandResult<TopicRouteView> {
    authorize_command(&session_id, &session_state).await?;
    connection_manager.check_revision(expected_revision)?;
    let result = topic_manager.get_topic_route(request).await.map_err(Into::into);
    connection_manager.check_revision(expected_revision)?;
    result
}

#[tauri::command]
pub async fn get_topic_stats(
    session_id: String,
    request: TopicQueryRequest,
    topic_manager: State<'_, TopicManager>,
    session_state: State<'_, SessionState>,
    expected_revision: i64,
    connection_manager: State<'_, ConnectionManager>,
) -> CommandResult<TopicStatusView> {
    authorize_command(&session_id, &session_state).await?;
    connection_manager.check_revision(expected_revision)?;
    let result = topic_manager.get_topic_stats(request).await.map_err(Into::into);
    connection_manager.check_revision(expected_revision)?;
    result
}

#[tauri::command]
pub async fn get_topic_config(
    session_id: String,
    request: TopicConfigQueryRequest,
    topic_manager: State<'_, TopicManager>,
    session_state: State<'_, SessionState>,
    expected_revision: i64,
    connection_manager: State<'_, ConnectionManager>,
) -> CommandResult<TopicConfigView> {
    authorize_command(&session_id, &session_state).await?;
    connection_manager.check_revision(expected_revision)?;
    let result = topic_manager.get_topic_config(request).await.map_err(Into::into);
    connection_manager.check_revision(expected_revision)?;
    result
}

#[tauri::command]
pub async fn create_or_update_topic(
    session_id: String,
    request: TopicConfigRequest,
    mode: crate::topic::guard::TopicWriteMode,
    topic_manager: State<'_, TopicManager>,
    session_state: State<'_, SessionState>,
    audit_manager: State<'_, AuditManager>,
    connection_manager: State<'_, ConnectionManager>,
    expected_revision: i64,
) -> CommandResult<Audited<TopicBatchResult>> {
    let connection_manager = connection_manager.inner().clone();
    let access = AuditAccess::dashboard(&session_state, session_id);
    let topic_manager = topic_manager.inner().clone();
    audit_manager
        .execute(
            access,
            AuditAction::UpsertTopic,
            Some(request.topic_name.clone()),
            move |audit| async move {
                let _lease = connection_manager.mutation_lease(expected_revision, &audit).await?;
                topic_manager.create_or_update_topic(request, mode).await
            },
        )
        .await
}

#[tauri::command]
pub async fn delete_topic(
    session_id: String,
    request: DeleteTopicRequest,
    topic_manager: State<'_, TopicManager>,
    session_state: State<'_, SessionState>,
    audit_manager: State<'_, AuditManager>,
    connection_manager: State<'_, ConnectionManager>,
    expected_revision: i64,
) -> CommandResult<Audited<TopicBatchResult>> {
    let connection_manager = connection_manager.inner().clone();
    let access = AuditAccess::dashboard(&session_state, session_id);
    let topic_manager = topic_manager.inner().clone();
    audit_manager
        .execute(
            access,
            AuditAction::DeleteTopic,
            Some(request.topic.clone()),
            move |audit| async move {
                let _lease = connection_manager.mutation_lease(expected_revision, &audit).await?;
                topic_manager.delete_topic(request).await
            },
        )
        .await
}

#[tauri::command]
pub async fn delete_topic_by_broker(
    session_id: String,
    request: DeleteTopicByBrokerRequest,
    topic_manager: State<'_, TopicManager>,
    session_state: State<'_, SessionState>,
    audit_manager: State<'_, AuditManager>,
    connection_manager: State<'_, ConnectionManager>,
    expected_revision: i64,
) -> CommandResult<Audited<TopicBatchResult>> {
    let connection_manager = connection_manager.inner().clone();
    let access = AuditAccess::dashboard(&session_state, session_id);
    let topic_manager = topic_manager.inner().clone();
    audit_manager
        .execute(
            access,
            AuditAction::DeleteTopicByBroker,
            Some(request.topic.clone()),
            move |audit| async move {
                let _lease = connection_manager.mutation_lease(expected_revision, &audit).await?;
                topic_manager.delete_topic_by_broker(request).await
            },
        )
        .await
}

#[tauri::command]
pub async fn get_topic_consumer_groups(
    session_id: String,
    request: TopicQueryRequest,
    topic_manager: State<'_, TopicManager>,
    session_state: State<'_, SessionState>,
    expected_revision: i64,
    connection_manager: State<'_, ConnectionManager>,
) -> CommandResult<TopicConsumerGroupListResponse> {
    authorize_command(&session_id, &session_state).await?;
    connection_manager.check_revision(expected_revision)?;
    let result = topic_manager
        .get_topic_consumer_groups(request)
        .await
        .map_err(Into::into);
    connection_manager.check_revision(expected_revision)?;
    result
}

#[tauri::command]
pub async fn get_topic_consumers(
    session_id: String,
    request: TopicQueryRequest,
    topic_manager: State<'_, TopicManager>,
    session_state: State<'_, SessionState>,
    expected_revision: i64,
    connection_manager: State<'_, ConnectionManager>,
) -> CommandResult<TopicConsumerInfoResponse> {
    authorize_command(&session_id, &session_state).await?;
    connection_manager.check_revision(expected_revision)?;
    let result = topic_manager.get_topic_consumers(request).await.map_err(Into::into);
    connection_manager.check_revision(expected_revision)?;
    result
}

#[tauri::command]
pub async fn reset_consumer_offset(
    session_id: String,
    request: ResetOffsetRequest,
    topic_manager: State<'_, TopicManager>,
    session_state: State<'_, SessionState>,
    audit_manager: State<'_, AuditManager>,
    connection_manager: State<'_, ConnectionManager>,
    expected_revision: i64,
) -> CommandResult<Audited<TopicMutationResult>> {
    let connection_manager = connection_manager.inner().clone();
    let access = AuditAccess::dashboard(&session_state, session_id);
    let topic_manager = topic_manager.inner().clone();
    audit_manager
        .execute(access, AuditAction::ResetOffset, None, move |audit| async move {
            let _lease = connection_manager.mutation_lease(expected_revision, &audit).await?;
            topic_manager.reset_consumer_offset(request).await
        })
        .await
}

#[tauri::command]
pub async fn skip_message_accumulate(
    session_id: String,
    request: ResetOffsetRequest,
    topic_manager: State<'_, TopicManager>,
    session_state: State<'_, SessionState>,
    audit_manager: State<'_, AuditManager>,
    connection_manager: State<'_, ConnectionManager>,
    expected_revision: i64,
) -> CommandResult<Audited<TopicMutationResult>> {
    let connection_manager = connection_manager.inner().clone();
    let access = AuditAccess::dashboard(&session_state, session_id);
    let topic_manager = topic_manager.inner().clone();
    audit_manager
        .execute(access, AuditAction::SkipMessages, None, move |audit| async move {
            let _lease = connection_manager.mutation_lease(expected_revision, &audit).await?;
            topic_manager.skip_message_accumulate(request).await
        })
        .await
}

#[tauri::command]
pub async fn send_topic_message(
    session_id: String,
    request: SendTopicMessageRequest,
    topic_manager: State<'_, TopicManager>,
    session_state: State<'_, SessionState>,
    audit_manager: State<'_, AuditManager>,
    connection_manager: State<'_, ConnectionManager>,
    expected_revision: i64,
) -> CommandResult<Audited<TopicSendMessageResult>> {
    let connection_manager = connection_manager.inner().clone();
    let access = AuditAccess::dashboard(&session_state, session_id);
    let topic_manager = topic_manager.inner().clone();
    audit_manager
        .execute(
            access,
            AuditAction::SendMessage,
            Some(request.topic.clone()),
            move |audit| async move {
                let _lease = connection_manager.mutation_lease(expected_revision, &audit).await?;
                topic_manager.send_topic_message(request).await
            },
        )
        .await
}
use crate::auth::SessionState;
use crate::error::CommandResult;
use crate::error::authorize_command;
