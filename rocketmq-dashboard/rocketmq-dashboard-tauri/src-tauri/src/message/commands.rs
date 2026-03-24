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
use crate::message::types::MessageDetailView;
use crate::message::types::MessagePageResponse;
use crate::message::types::MessageResendResult;
use crate::message::types::MessageSummaryListResponse;
use crate::message::types::MessageTraceDetailView;
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
    request: MessageKeyQueryRequest,
    message_manager: State<'_, MessageManager>,
) -> Result<MessageSummaryListResponse, String> {
    message_manager
        .query_message_by_topic_key(request)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn query_message_by_id(
    request: MessageIdQueryRequest,
    message_manager: State<'_, MessageManager>,
) -> Result<MessageSummaryListResponse, String> {
    message_manager
        .query_message_by_id(request)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn query_message_page_by_topic(
    request: MessagePageQueryRequest,
    message_manager: State<'_, MessageManager>,
) -> Result<MessagePageResponse, String> {
    message_manager
        .query_message_page_by_topic(request)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn query_dlq_message_by_consumer_group(
    request: DlqMessagePageQueryRequest,
    message_manager: State<'_, MessageManager>,
) -> Result<MessagePageResponse, String> {
    message_manager
        .query_dlq_message_by_consumer_group(request)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn view_message_detail(
    request: ViewMessageRequest,
    message_manager: State<'_, MessageManager>,
) -> Result<MessageDetailView, String> {
    message_manager
        .view_message_detail(request)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn view_dlq_message_detail(
    request: DlqViewMessageRequest,
    message_manager: State<'_, MessageManager>,
) -> Result<MessageDetailView, String> {
    message_manager
        .view_dlq_message_detail(request)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn resend_dlq_message(
    request: DlqResendMessageRequest,
    message_manager: State<'_, MessageManager>,
) -> Result<MessageResendResult, String> {
    message_manager
        .resend_dlq_message(request)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn consume_message_directly(
    request: MessageDirectConsumeRequest,
    message_manager: State<'_, MessageManager>,
) -> Result<MessageResendResult, String> {
    message_manager
        .consume_message_directly(request)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn query_message_trace_by_id(
    request: MessageTraceQueryRequest,
    message_manager: State<'_, MessageManager>,
) -> Result<MessageSummaryListResponse, String> {
    message_manager
        .query_message_trace_by_id(request)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn view_message_trace_detail(
    request: MessageTraceQueryRequest,
    message_manager: State<'_, MessageManager>,
) -> Result<MessageTraceDetailView, String> {
    message_manager
        .view_message_trace_detail(request)
        .await
        .map_err(|error| error.to_string())
}
