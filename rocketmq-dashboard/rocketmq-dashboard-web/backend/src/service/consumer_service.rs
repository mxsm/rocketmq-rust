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
use crate::model::ConsumerListView;
use crate::model::ConsumerProgress;
use crate::model::ConsumerResetOffsetRequest;
use crate::model::MutationResult;
use crate::state::AppState;

pub async fn list_consumers(state: &AppState) -> Result<ConsumerListView, DashboardError> {
    state.admin_facade().list_consumer_groups().await
}

pub async fn consumer_progress(state: &AppState, group: &str) -> Result<ConsumerProgress, DashboardError> {
    state.admin_facade().consumer_progress(group).await
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
