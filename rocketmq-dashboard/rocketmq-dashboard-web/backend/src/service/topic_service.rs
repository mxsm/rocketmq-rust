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
use crate::model::TopicConfigView;
use crate::model::TopicConsumersView;
use crate::model::TopicInfo;
use crate::model::TopicListView;
use crate::model::TopicMutationRequest;
use crate::model::TopicRouteInfo;
use crate::model::TopicStatsInfo;
use crate::state::AppState;

pub async fn list_topics(state: &AppState) -> Result<TopicListView, DashboardError> {
    state.admin_client.list_topics().await
}

pub async fn get_topic(state: &AppState, topic: &str) -> Result<TopicInfo, DashboardError> {
    state.admin_client.get_topic(topic).await
}

pub async fn topic_route(state: &AppState, topic: &str) -> Result<TopicRouteInfo, DashboardError> {
    state.admin_client.topic_route(topic).await
}

pub async fn topic_stats(state: &AppState, topic: &str) -> Result<TopicStatsInfo, DashboardError> {
    state.admin_client.topic_stats(topic).await
}

pub async fn topic_config(
    state: &AppState,
    topic: &str,
    broker_name: Option<&str>,
) -> Result<TopicConfigView, DashboardError> {
    state.admin_client.topic_config(topic, broker_name).await
}

pub async fn topic_consumers(state: &AppState, topic: &str) -> Result<TopicConsumersView, DashboardError> {
    state.admin_client.topic_consumers(topic).await
}

pub async fn create_or_update_topic(
    state: &AppState,
    request: TopicMutationRequest,
) -> Result<MutationResult, DashboardError> {
    let _mutation_guard = state.topic_mutation_lock.lock().await;
    state.admin_facade().create_or_update_topic(request).await
}

pub async fn create_topic(state: &AppState, request: TopicMutationRequest) -> Result<MutationResult, DashboardError> {
    let _mutation_guard = state.topic_mutation_lock.lock().await;
    let topics = state.admin_client.list_topics().await?;
    ensure_topic_does_not_exist(&topics, &request.topic)?;
    state.admin_facade().create_or_update_topic(request).await
}

fn ensure_topic_does_not_exist(topics: &TopicListView, topic: &str) -> Result<(), DashboardError> {
    if topics.items.iter().any(|item| item.topic == topic) {
        return Err(DashboardError::Validation(format!("Topic `{topic}` already exists")));
    }
    Ok(())
}

pub async fn delete_topic(state: &AppState, topic: &str) -> Result<MutationResult, DashboardError> {
    let _mutation_guard = state.topic_mutation_lock.lock().await;
    state.admin_facade().delete_topic(topic).await
}

#[cfg(test)]
mod tests {
    use super::ensure_topic_does_not_exist;
    use crate::error::DashboardError;
    use crate::model::TopicInfo;
    use crate::model::TopicListView;

    fn topic(name: &str) -> TopicInfo {
        TopicInfo {
            topic: name.to_string(),
            broker_name: Some("broker-a".to_string()),
            brokers: vec!["broker-a".to_string()],
            clusters: vec!["DefaultCluster".to_string()],
            read_queue_count: 8,
            write_queue_count: 8,
            perm: 6,
            category: "NORMAL".to_string(),
            message_type: "NORMAL".to_string(),
            order: false,
            system_topic: false,
        }
    }

    #[test]
    fn create_only_guard_rejects_an_existing_topic() {
        let topics = TopicListView {
            items: vec![topic("orders")],
            total: 1,
            targets: Vec::new(),
        };

        let error = ensure_topic_does_not_exist(&topics, "orders").expect_err("existing topic must be rejected");

        assert!(matches!(
            error,
            DashboardError::Validation(message) if message == "Topic `orders` already exists"
        ));
    }

    #[test]
    fn create_only_guard_allows_a_new_case_sensitive_topic_name() {
        let topics = TopicListView {
            items: vec![topic("orders")],
            total: 1,
            targets: Vec::new(),
        };

        ensure_topic_does_not_exist(&topics, "Orders").expect("RocketMQ topic names are case-sensitive");
    }
}
