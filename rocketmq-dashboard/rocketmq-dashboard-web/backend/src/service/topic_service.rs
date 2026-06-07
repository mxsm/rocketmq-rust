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
use crate::model::MutationResult;
use crate::model::TopicInfo;
use crate::model::TopicListView;
use crate::model::TopicMutationRequest;
use crate::model::TopicRouteInfo;
use crate::model::TopicStatsInfo;
use crate::state::AppState;

pub async fn list_topics(state: &AppState) -> Result<TopicListView, DashboardError> {
    state.admin_facade().list_topics().await
}

pub async fn get_topic(state: &AppState, topic: &str) -> Result<TopicInfo, DashboardError> {
    state.admin_facade().get_topic(topic).await
}

pub async fn topic_route(state: &AppState, topic: &str) -> Result<TopicRouteInfo, DashboardError> {
    state.admin_facade().topic_route(topic).await
}

pub async fn topic_stats(state: &AppState, topic: &str) -> Result<TopicStatsInfo, DashboardError> {
    state.admin_facade().topic_stats(topic).await
}

pub async fn create_or_update_topic(
    state: &AppState,
    request: TopicMutationRequest,
) -> Result<MutationResult, DashboardError> {
    state.admin_facade().create_or_update_topic(request).await
}

pub async fn delete_topic(state: &AppState, topic: &str) -> Result<MutationResult, DashboardError> {
    state.admin_facade().delete_topic(topic).await
}
