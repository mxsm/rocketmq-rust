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

use crate::message::service::MessageManager;
use crate::message::types::DlqBatchMessageExportView;
use crate::message::types::DlqMessageExportView;
use crate::message::types::MessageBatchResendResponse;
use crate::message::types::MessageDetailView;
use crate::message::types::MessagePageResponse;
use crate::message::types::MessageResendResult;
use crate::message::types::MessageSummaryListResponse;
use crate::message::types::MessageTraceDetailView;
use rocketmq_dashboard_common::DlqBatchExportMessageRequest;
use rocketmq_dashboard_common::DlqBatchResendMessageRequest;
use rocketmq_dashboard_common::DlqMessagePageQueryRequest;
use rocketmq_dashboard_common::DlqResendMessageRequest;
use rocketmq_dashboard_common::DlqViewMessageRequest;
use rocketmq_dashboard_common::MessageDirectConsumeRequest;
use rocketmq_dashboard_common::MessageIdQueryRequest;
use rocketmq_dashboard_common::MessageKeyQueryRequest;
use rocketmq_dashboard_common::MessagePageQueryRequest;
use rocketmq_dashboard_common::MessageTraceQueryRequest;
use rocketmq_dashboard_common::ViewMessageRequest;
use tauri::State;

#[tauri::command]
pub async fn query_message_by_topic_key(
    session_id: String,
    request: MessageKeyQueryRequest,
    message_manager: State<'_, MessageManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<MessageSummaryListResponse> {
    authorize_command(&session_id, &session_state).await?;
    message_manager
        .query_message_by_topic_key(request)
        .await
        .map_err(Into::into)
}

#[tauri::command]
pub async fn query_message_by_id(
    session_id: String,
    request: MessageIdQueryRequest,
    message_manager: State<'_, MessageManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<MessageSummaryListResponse> {
    authorize_command(&session_id, &session_state).await?;
    message_manager.query_message_by_id(request).await.map_err(Into::into)
}

#[tauri::command]
pub async fn query_message_page_by_topic(
    session_id: String,
    request: MessagePageQueryRequest,
    message_manager: State<'_, MessageManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<MessagePageResponse> {
    authorize_command(&session_id, &session_state).await?;
    message_manager
        .query_message_page_by_topic(request)
        .await
        .map_err(Into::into)
}

#[tauri::command]
pub async fn query_dlq_message_by_consumer_group(
    session_id: String,
    request: DlqMessagePageQueryRequest,
    message_manager: State<'_, MessageManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<MessagePageResponse> {
    authorize_command(&session_id, &session_state).await?;
    message_manager
        .query_dlq_message_by_consumer_group(request)
        .await
        .map_err(Into::into)
}

#[tauri::command]
pub async fn view_message_detail(
    session_id: String,
    request: ViewMessageRequest,
    message_manager: State<'_, MessageManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<MessageDetailView> {
    authorize_command(&session_id, &session_state).await?;
    message_manager.view_message_detail(request).await.map_err(Into::into)
}

#[tauri::command]
pub async fn view_dlq_message_detail(
    session_id: String,
    request: DlqViewMessageRequest,
    message_manager: State<'_, MessageManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<MessageDetailView> {
    authorize_command(&session_id, &session_state).await?;
    message_manager
        .view_dlq_message_detail(request)
        .await
        .map_err(Into::into)
}

#[tauri::command]
pub async fn resend_dlq_message(
    session_id: String,
    request: DlqResendMessageRequest,
    message_manager: State<'_, MessageManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<MessageResendResult> {
    authorize_command(&session_id, &session_state).await?;
    message_manager.resend_dlq_message(request).await.map_err(Into::into)
}

#[tauri::command]
pub async fn batch_resend_dlq_message(
    session_id: String,
    request: DlqBatchResendMessageRequest,
    message_manager: State<'_, MessageManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<MessageBatchResendResponse> {
    authorize_command(&session_id, &session_state).await?;
    message_manager
        .batch_resend_dlq_message(request)
        .await
        .map_err(Into::into)
}

#[tauri::command]
pub async fn export_dlq_message(
    session_id: String,
    request: DlqViewMessageRequest,
    message_manager: State<'_, MessageManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<DlqMessageExportView> {
    authorize_command(&session_id, &session_state).await?;
    message_manager.export_dlq_message(request).await.map_err(Into::into)
}

#[tauri::command]
pub async fn batch_export_dlq_message(
    session_id: String,
    request: DlqBatchExportMessageRequest,
    message_manager: State<'_, MessageManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<DlqBatchMessageExportView> {
    authorize_command(&session_id, &session_state).await?;
    message_manager
        .batch_export_dlq_message(request)
        .await
        .map_err(Into::into)
}

#[tauri::command]
pub async fn consume_message_directly(
    session_id: String,
    request: MessageDirectConsumeRequest,
    message_manager: State<'_, MessageManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<MessageResendResult> {
    authorize_command(&session_id, &session_state).await?;
    message_manager
        .consume_message_directly(request)
        .await
        .map_err(Into::into)
}

#[tauri::command]
pub async fn query_message_trace_by_id(
    session_id: String,
    request: MessageTraceQueryRequest,
    message_manager: State<'_, MessageManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<MessageSummaryListResponse> {
    authorize_command(&session_id, &session_state).await?;
    message_manager
        .query_message_trace_by_id(request)
        .await
        .map_err(Into::into)
}

#[tauri::command]
pub async fn view_message_trace_detail(
    session_id: String,
    request: MessageTraceQueryRequest,
    message_manager: State<'_, MessageManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<MessageTraceDetailView> {
    authorize_command(&session_id, &session_state).await?;
    message_manager
        .view_message_trace_detail(request)
        .await
        .map_err(Into::into)
}
use crate::auth::SessionState;
use crate::error::CommandResult;
use crate::error::authorize_command;
