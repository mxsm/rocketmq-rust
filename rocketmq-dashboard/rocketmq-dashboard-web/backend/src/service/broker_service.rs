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
use crate::model::BrokerConfigUpdateRequest;
use crate::model::BrokerConfigView;
use crate::model::BrokerListView;
use crate::model::BrokerRuntimeStats;
use crate::model::MutationResult;
use crate::state::AppState;

pub async fn list_brokers(state: &AppState) -> Result<BrokerListView, DashboardError> {
    state.admin_facade().list_brokers().await
}

pub async fn broker_runtime(state: &AppState, broker_name: &str) -> Result<BrokerRuntimeStats, DashboardError> {
    state.admin_facade().broker_runtime_stats(broker_name).await
}

pub async fn broker_config(state: &AppState, broker_name: &str) -> Result<BrokerConfigView, DashboardError> {
    state.admin_facade().broker_config(broker_name).await
}

pub async fn update_broker_config(
    state: &AppState,
    broker_name: &str,
    request: BrokerConfigUpdateRequest,
) -> Result<MutationResult, DashboardError> {
    state
        .admin_facade()
        .update_broker_config(broker_name.to_string(), request)
        .await
}
