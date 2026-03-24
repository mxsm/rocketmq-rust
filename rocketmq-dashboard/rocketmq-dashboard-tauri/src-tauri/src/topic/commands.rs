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
    request: TopicListRequest,
    topic_manager: State<'_, TopicManager>,
) -> Result<TopicListResponse, String> {
    topic_manager
        .get_topic_list(request)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn get_topic_route(
    request: TopicQueryRequest,
    topic_manager: State<'_, TopicManager>,
) -> Result<TopicRouteView, String> {
    topic_manager
        .get_topic_route(request)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn get_topic_stats(
    request: TopicQueryRequest,
    topic_manager: State<'_, TopicManager>,
) -> Result<TopicStatusView, String> {
    topic_manager
        .get_topic_stats(request)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn get_topic_config(
    request: TopicConfigQueryRequest,
    topic_manager: State<'_, TopicManager>,
) -> Result<TopicConfigView, String> {
    topic_manager
        .get_topic_config(request)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn create_or_update_topic(
    request: TopicConfigRequest,
    topic_manager: State<'_, TopicManager>,
) -> Result<TopicMutationResult, String> {
    topic_manager
        .create_or_update_topic(request)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn delete_topic(
    request: DeleteTopicRequest,
    topic_manager: State<'_, TopicManager>,
) -> Result<TopicMutationResult, String> {
    topic_manager
        .delete_topic(request)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn delete_topic_by_broker(
    request: DeleteTopicByBrokerRequest,
    topic_manager: State<'_, TopicManager>,
) -> Result<TopicMutationResult, String> {
    topic_manager
        .delete_topic_by_broker(request)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn get_topic_consumer_groups(
    request: TopicQueryRequest,
    topic_manager: State<'_, TopicManager>,
) -> Result<TopicConsumerGroupListResponse, String> {
    topic_manager
        .get_topic_consumer_groups(request)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn get_topic_consumers(
    request: TopicQueryRequest,
    topic_manager: State<'_, TopicManager>,
) -> Result<TopicConsumerInfoResponse, String> {
    topic_manager
        .get_topic_consumers(request)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn reset_consumer_offset(
    request: ResetOffsetRequest,
    topic_manager: State<'_, TopicManager>,
) -> Result<TopicMutationResult, String> {
    topic_manager
        .reset_consumer_offset(request)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn skip_message_accumulate(
    request: ResetOffsetRequest,
    topic_manager: State<'_, TopicManager>,
) -> Result<TopicMutationResult, String> {
    topic_manager
        .skip_message_accumulate(request)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn send_topic_message(
    request: SendTopicMessageRequest,
    topic_manager: State<'_, TopicManager>,
) -> Result<TopicSendMessageResult, String> {
    topic_manager
        .send_topic_message(request)
        .await
        .map_err(|error| error.to_string())
}
