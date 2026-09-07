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
    request: ConsumerGroupListRequest,
    consumer_manager: State<'_, ConsumerManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<ConsumerGroupListResponse> {
    authorize_command(&session_id, &session_state)?;
    consumer_manager
        .query_consumer_groups(request)
        .await
        .map_err(Into::into)
}

#[tauri::command]
pub async fn refresh_consumer_group(
    session_id: String,
    request: ConsumerGroupRefreshRequest,
    consumer_manager: State<'_, ConsumerManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<ConsumerGroupListItem> {
    authorize_command(&session_id, &session_state)?;
    consumer_manager
        .refresh_consumer_group(request)
        .await
        .map_err(Into::into)
}

#[tauri::command]
pub async fn refresh_all_consumer_groups(
    session_id: String,
    request: ConsumerGroupListRequest,
    consumer_manager: State<'_, ConsumerManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<ConsumerGroupListResponse> {
    authorize_command(&session_id, &session_state)?;
    consumer_manager
        .refresh_all_consumer_groups(request)
        .await
        .map_err(Into::into)
}

#[tauri::command]
pub async fn query_consumer_connection(
    session_id: String,
    request: ConsumerConnectionQueryRequest,
    consumer_manager: State<'_, ConsumerManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<ConsumerConnectionView> {
    authorize_command(&session_id, &session_state)?;
    consumer_manager
        .query_consumer_connection(request)
        .await
        .map_err(Into::into)
}

#[tauri::command]
pub async fn query_consumer_topic_detail(
    session_id: String,
    request: ConsumerTopicDetailQueryRequest,
    consumer_manager: State<'_, ConsumerManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<ConsumerTopicDetailView> {
    authorize_command(&session_id, &session_state)?;
    consumer_manager
        .query_consumer_topic_detail(request)
        .await
        .map_err(Into::into)
}

#[tauri::command]
pub async fn query_consumer_config(
    session_id: String,
    request: ConsumerConfigQueryRequest,
    consumer_manager: State<'_, ConsumerManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<ConsumerConfigView> {
    authorize_command(&session_id, &session_state)?;
    consumer_manager
        .query_consumer_config(request)
        .await
        .map_err(Into::into)
}

#[tauri::command]
pub async fn create_or_update_consumer_group(
    session_id: String,
    request: ConsumerCreateOrUpdateRequest,
    consumer_manager: State<'_, ConsumerManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<ConsumerMutationResult> {
    authorize_command(&session_id, &session_state)?;
    consumer_manager
        .create_or_update_consumer_group(request)
        .await
        .map_err(Into::into)
}

#[tauri::command]
pub async fn delete_consumer_group(
    session_id: String,
    request: ConsumerDeleteRequest,
    consumer_manager: State<'_, ConsumerManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<ConsumerMutationResult> {
    authorize_command(&session_id, &session_state)?;
    consumer_manager
        .delete_consumer_group(request)
        .await
        .map_err(Into::into)
}
use crate::auth::SessionState;
