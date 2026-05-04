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

use crate::cluster::service::ClusterManager;
use crate::dashboard::service;
use crate::dashboard::types::DashboardBrokerOverviewResponse;
use crate::dashboard::types::DashboardTopicCurrentResponse;
use crate::topic::service::TopicManager;
use rocketmq_dashboard_common::DashboardBrokerOverviewRequest;
use tauri::State;

#[tauri::command]
pub async fn get_dashboard_broker_overview(
    request: DashboardBrokerOverviewRequest,
    cluster_manager: State<'_, ClusterManager>,
) -> Result<DashboardBrokerOverviewResponse, String> {
    service::get_dashboard_broker_overview(&cluster_manager, request)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn query_dashboard_topic_current(
    topic_manager: State<'_, TopicManager>,
) -> Result<DashboardTopicCurrentResponse, String> {
    service::query_dashboard_topic_current(&topic_manager)
        .await
        .map_err(|error| error.to_string())
}
