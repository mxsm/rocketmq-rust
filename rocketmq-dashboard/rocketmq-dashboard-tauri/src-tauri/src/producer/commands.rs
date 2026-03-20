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

use crate::producer::service::ProducerManager;
use crate::producer::types::ProducerConnectionView;
use rocketmq_dashboard_common::ProducerConnectionQueryRequest;
use tauri::State;

#[tauri::command]
pub async fn query_producer_connections(
    request: ProducerConnectionQueryRequest,
    producer_manager: State<'_, ProducerManager>,
) -> Result<ProducerConnectionView, String> {
    producer_manager
        .query_producer_connections(request)
        .await
        .map_err(|error| error.to_string())
}
