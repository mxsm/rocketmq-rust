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
use crate::error::DashboardError;
use crate::model::DlqBatchResendRequest;
use crate::model::DlqExportView;
use crate::model::DlqMessageQuery;
use crate::model::DlqMessageResendResult;
use crate::model::MessageListView;
use crate::model::MessageQuery;
use crate::model::MessageResendRequest;
use crate::model::MessageTraceView;
use crate::model::MutationResult;
use crate::state::AppState;

pub async fn query_messages(state: &AppState, query: MessageQuery) -> Result<MessageListView, DashboardError> {
    state
        .admin_client
        .query_messages(
            query.topic.as_deref(),
            query.key.as_deref(),
            query.message_id.as_deref(),
            query.begin,
            query.end,
            query.page_num,
            query.page_size,
        )
        .await
}

pub async fn query_message_by_key(state: &AppState, topic: &str, key: &str) -> Result<MessageListView, DashboardError> {
    state
        .admin_facade()
        .query_message_by_key(topic.to_string(), key.to_string())
        .await
}

pub async fn query_message_by_id(state: &AppState, message_id: &str) -> Result<MessageListView, DashboardError> {
    state.admin_facade().query_message_by_id(message_id.to_string()).await
}

pub async fn message_trace(
    state: &AppState,
    message_id: &str,
    topic: Option<&str>,
    trace_topic: &str,
) -> Result<MessageTraceView, DashboardError> {
    let topic = topic.ok_or_else(|| {
        DashboardError::Validation("Message trace requires topic. Use /api/messages/:id/trace?topic=...".to_string())
    })?;
    state
        .admin_facade()
        .message_trace(message_id.to_string(), topic.to_string(), Some(trace_topic.to_string()))
        .await
}

pub async fn resend_message(
    state: &AppState,
    message_id: &str,
    request: MessageResendRequest,
) -> Result<MutationResult, DashboardError> {
    state
        .admin_facade()
        .resend_message(message_id.to_string(), request)
        .await
}

pub async fn query_dlq_messages(state: &AppState, query: DlqMessageQuery) -> Result<MessageListView, DashboardError> {
    state.admin_client.query_dlq_messages(query).await
}

pub async fn resend_dlq_messages(
    state: &AppState,
    request: DlqBatchResendRequest,
) -> Result<Vec<DlqMessageResendResult>, DashboardError> {
    state.admin_client.resend_dlq_messages(request).await
}

pub async fn export_dlq_messages(state: &AppState, query: DlqMessageQuery) -> Result<DlqExportView, DashboardError> {
    state.admin_client.export_dlq_messages(query).await
}
