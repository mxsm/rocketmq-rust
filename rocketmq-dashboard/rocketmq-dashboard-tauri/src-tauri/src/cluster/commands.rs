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

use crate::cluster::service::ClusterManager;
use crate::cluster::types::ClusterBrokerConfigView;
use crate::cluster::types::ClusterBrokerStatusView;
use crate::cluster::types::ClusterHomePageResponse;
use rocketmq_dashboard_common::ClusterBrokerConfigRequest;
use rocketmq_dashboard_common::ClusterBrokerStatusRequest;
use rocketmq_dashboard_common::ClusterHomePageRequest;
use tauri::State;

#[tauri::command]
pub async fn get_cluster_home_page(
    request: ClusterHomePageRequest,
    cluster_manager: State<'_, ClusterManager>,
) -> Result<ClusterHomePageResponse, String> {
    cluster_manager
        .get_cluster_home_page(request)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn get_cluster_broker_config(
    request: ClusterBrokerConfigRequest,
    cluster_manager: State<'_, ClusterManager>,
) -> Result<ClusterBrokerConfigView, String> {
    cluster_manager
        .get_cluster_broker_config(request)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn get_cluster_broker_status(
    request: ClusterBrokerStatusRequest,
    cluster_manager: State<'_, ClusterManager>,
) -> Result<ClusterBrokerStatusView, String> {
    cluster_manager
        .get_cluster_broker_status(request)
        .await
        .map_err(|error| error.to_string())
}
