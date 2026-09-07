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

use crate::auth::SessionState;
use crate::cluster::service::ClusterManager;
use crate::cluster::types::ClusterBrokerConfigView;
use crate::cluster::types::ClusterBrokerStatusView;
use crate::cluster::types::ClusterHomePageResponse;
use crate::error::CommandResult;
use crate::error::authorize_command;
use rocketmq_dashboard_common::ClusterBrokerConfigRequest;
use rocketmq_dashboard_common::ClusterBrokerStatusRequest;
use rocketmq_dashboard_common::ClusterHomePageRequest;
use tauri::State;

#[tauri::command]
pub async fn get_cluster_home_page(
    session_id: String,
    request: ClusterHomePageRequest,
    cluster_manager: State<'_, ClusterManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<ClusterHomePageResponse> {
    authorize_command(&session_id, &session_state)?;
    cluster_manager.get_cluster_home_page(request).await.map_err(Into::into)
}

#[tauri::command]
pub async fn get_cluster_broker_config(
    session_id: String,
    request: ClusterBrokerConfigRequest,
    cluster_manager: State<'_, ClusterManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<ClusterBrokerConfigView> {
    authorize_command(&session_id, &session_state)?;
    cluster_manager
        .get_cluster_broker_config(request)
        .await
        .map_err(Into::into)
}

#[tauri::command]
pub async fn get_cluster_broker_status(
    session_id: String,
    request: ClusterBrokerStatusRequest,
    cluster_manager: State<'_, ClusterManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<ClusterBrokerStatusView> {
    authorize_command(&session_id, &session_state)?;
    cluster_manager
        .get_cluster_broker_status(request)
        .await
        .map_err(Into::into)
}
