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
use crate::message::types::MessageSummaryListResponse;
use rocketmq_dashboard_common::MessageIdQueryRequest;
use rocketmq_dashboard_common::MessageKeyQueryRequest;
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
