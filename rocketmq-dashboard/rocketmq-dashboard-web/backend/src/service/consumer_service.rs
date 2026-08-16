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
use crate::model::ConsumerBrokerListView;
use crate::model::ConsumerConfigView;
use crate::model::ConsumerConnectionView;
use crate::model::ConsumerDeleteView;
use crate::model::ConsumerGroupListView;
use crate::model::ConsumerJStackView;
use crate::model::ConsumerOperationResult;
use crate::model::ConsumerProgressView;
use crate::model::ConsumerQuery;
use crate::model::ConsumerResetOffsetRequest;
use crate::model::ConsumerRunningInfoView;
use crate::model::ConsumerSummaryView;
use crate::model::ConsumerUpsertView;
use crate::model::MutationResult;
use crate::state::AppState;

pub async fn list_consumers(state: &AppState, query: ConsumerQuery) -> Result<ConsumerGroupListView, DashboardError> {
    state.admin_client.consumer_group_list(query).await
}

pub async fn consumer_brokers(state: &AppState, group: &str) -> Result<ConsumerBrokerListView, DashboardError> {
    state.admin_client.consumer_brokers(group).await
}

pub async fn consumer_summary(
    state: &AppState,
    group: &str,
    query: ConsumerQuery,
) -> Result<ConsumerSummaryView, DashboardError> {
    state.admin_client.consumer_summary(group, query).await
}

pub async fn consumer_connections(
    state: &AppState,
    group: &str,
    query: ConsumerQuery,
) -> Result<ConsumerConnectionView, DashboardError> {
    state.admin_client.consumer_connections(group, query).await
}

pub async fn consumer_progress(
    state: &AppState,
    group: &str,
    query: ConsumerQuery,
) -> Result<ConsumerProgressView, DashboardError> {
    state.admin_client.consumer_progress_view(group, query).await
}

pub async fn consumer_config(
    state: &AppState,
    group: &str,
    query: ConsumerQuery,
) -> Result<ConsumerConfigView, DashboardError> {
    state.admin_client.consumer_config_view(group, query).await
}

pub async fn consumer_running_info(
    state: &AppState,
    group: &str,
    client_id: &str,
    query: ConsumerQuery,
) -> Result<ConsumerRunningInfoView, DashboardError> {
    state
        .admin_client
        .consumer_running_info(group, client_id, query, false)
        .await
}

pub async fn consumer_jstack(
    state: &AppState,
    group: &str,
    client_id: &str,
    query: ConsumerQuery,
) -> Result<ConsumerJStackView, DashboardError> {
    state.admin_client.consumer_jstack(group, client_id, query).await
}

pub async fn create_consumer(
    state: &AppState,
    group: &str,
    request: ConsumerUpsertView,
) -> Result<ConsumerOperationResult, DashboardError> {
    state.admin_client.create_consumer_group(group, request).await
}

pub async fn update_consumer(
    state: &AppState,
    group: &str,
    request: ConsumerUpsertView,
) -> Result<ConsumerOperationResult, DashboardError> {
    state.admin_client.update_consumer_group(group, request).await
}

pub async fn delete_consumer(
    state: &AppState,
    group: &str,
    request: ConsumerDeleteView,
) -> Result<ConsumerOperationResult, DashboardError> {
    state.admin_client.delete_consumer_group(group, request).await
}

pub async fn reset_consumer_offset(
    state: &AppState,
    group: &str,
    request: ConsumerResetOffsetRequest,
) -> Result<MutationResult, DashboardError> {
    state
        .admin_facade()
        .reset_consumer_offset(group.to_string(), request)
        .await
}
