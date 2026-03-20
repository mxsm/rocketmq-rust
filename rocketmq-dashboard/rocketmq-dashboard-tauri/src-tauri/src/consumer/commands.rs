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
use crate::consumer::types::ConsumerConnectionView;
use crate::consumer::types::ConsumerGroupListItem;
use crate::consumer::types::ConsumerGroupListResponse;
use crate::consumer::types::ConsumerTopicDetailView;
use rocketmq_dashboard_common::ConsumerConnectionQueryRequest;
use rocketmq_dashboard_common::ConsumerGroupListRequest;
use rocketmq_dashboard_common::ConsumerGroupRefreshRequest;
use rocketmq_dashboard_common::ConsumerTopicDetailQueryRequest;
use tauri::State;

#[tauri::command]
pub async fn query_consumer_groups(
    request: ConsumerGroupListRequest,
    consumer_manager: State<'_, ConsumerManager>,
) -> Result<ConsumerGroupListResponse, String> {
    consumer_manager
        .query_consumer_groups(request)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn refresh_consumer_group(
    request: ConsumerGroupRefreshRequest,
    consumer_manager: State<'_, ConsumerManager>,
) -> Result<ConsumerGroupListItem, String> {
    consumer_manager
        .refresh_consumer_group(request)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn query_consumer_connection(
    request: ConsumerConnectionQueryRequest,
    consumer_manager: State<'_, ConsumerManager>,
) -> Result<ConsumerConnectionView, String> {
    consumer_manager
        .query_consumer_connection(request)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn query_consumer_topic_detail(
    request: ConsumerTopicDetailQueryRequest,
    consumer_manager: State<'_, ConsumerManager>,
) -> Result<ConsumerTopicDetailView, String> {
    consumer_manager
        .query_consumer_topic_detail(request)
        .await
        .map_err(|error| error.to_string())
}
