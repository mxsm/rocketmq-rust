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
use crate::model::TopicConfigView;
use crate::model::TopicConsumersView;
use crate::model::TopicInfo;
use crate::model::TopicListView;
use crate::model::TopicMutationRequest;
use crate::model::TopicOffsetResult;
use crate::model::TopicOperationResult;
use crate::model::TopicResetOffsetRequest;
use crate::model::TopicRouteInfo;
use crate::model::TopicSendResultView;
use crate::model::TopicSkipOffsetRequest;
use crate::model::TopicStatsInfo;
use crate::model::TopicTestMessageRequest;
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
) -> Result<TopicOperationResult, DashboardError> {
    let _mutation_guard = state.topic_mutation_lock.lock().await;
    let request = normalize_topic_mutation(request)?;
    state.admin_client.create_or_update_topic(request).await
}

pub async fn create_topic(
    state: &AppState,
    request: TopicMutationRequest,
) -> Result<TopicOperationResult, DashboardError> {
    let _mutation_guard = state.topic_mutation_lock.lock().await;
    let request = normalize_topic_mutation(request)?;
    state.admin_client.create_topic(request).await
}

pub(crate) fn validate_topic_mutation(request: &TopicMutationRequest) -> Result<(), DashboardError> {
    validate_rocketmq_topic_name(&request.topic)?;
    if !(1..=128).contains(&request.read_queue_count) || !(1..=128).contains(&request.write_queue_count) {
        return Err(DashboardError::Validation(
            "Read and write queue counts must be between 1 and 128".to_string(),
        ));
    }
    if !(1..=7).contains(&request.perm) || request.perm & 0b110 == 0 {
        return Err(DashboardError::Validation(
            "Permission must be between 1 and 7 and include read or write access".to_string(),
        ));
    }
    if request.cluster_name_list.is_empty() && request.broker_name_list.is_empty() {
        return Err(DashboardError::Validation(
            "Select at least one cluster or broker before saving the topic".to_string(),
        ));
    }
    if request
        .cluster_name_list
        .iter()
        .chain(&request.broker_name_list)
        .any(|target| target.is_empty())
    {
        return Err(DashboardError::Validation(
            "Topic targets must not be empty".to_string(),
        ));
    }
    if let Some(message_type) = &request.message_type
        && !matches!(message_type.as_str(), "NORMAL" | "FIFO" | "DELAY" | "TRANSACTION")
    {
        return Err(DashboardError::Validation(
            "Message type must be NORMAL, FIFO, DELAY, or TRANSACTION".to_string(),
        ));
    }
    Ok(())
}

fn validate_rocketmq_topic_name(topic: &str) -> Result<(), DashboardError> {
    if topic.is_empty() {
        return Err(DashboardError::Validation("Topic must not be empty".to_string()));
    }
    if topic.len() > 127 {
        return Err(DashboardError::Validation(
            "Topic must not exceed 127 bytes".to_string(),
        ));
    }
    if topic
        .bytes()
        .any(|byte| !matches!(byte, b'%' | b'|' | b'-' | b'_') && !byte.is_ascii_alphanumeric())
    {
        return Err(DashboardError::Validation(
            "Topic contains illegal characters; allowed characters are ^[%|a-zA-Z0-9_-]+$".to_string(),
        ));
    }
    Ok(())
}

fn normalize_topic_mutation(mut request: TopicMutationRequest) -> Result<TopicMutationRequest, DashboardError> {
    request.topic = request.topic.trim().to_string();
    request.broker_name_list = request
        .broker_name_list
        .into_iter()
        .map(|target| target.trim().to_string())
        .collect();
    request.cluster_name_list = request
        .cluster_name_list
        .into_iter()
        .map(|target| target.trim().to_string())
        .collect();
    request.message_type = request.message_type.map(|message_type| message_type.trim().to_string());
    validate_topic_mutation(&request)?;
    Ok(request)
}

pub async fn send_topic_test_message(
    state: &AppState,
    topic: &str,
    request: TopicTestMessageRequest,
) -> Result<TopicSendResultView, DashboardError> {
    let _mutation_guard = state.topic_mutation_lock.lock().await;
    state.admin_client.send_topic_test_message(topic, request).await
}

pub async fn reset_topic_consumer_offset(
    state: &AppState,
    topic: &str,
    request: TopicResetOffsetRequest,
) -> Result<TopicOffsetResult, DashboardError> {
    let _mutation_guard = state.topic_mutation_lock.lock().await;
    state.admin_client.reset_topic_consumer_offset(topic, request).await
}

pub async fn skip_topic_consumer_offset(
    state: &AppState,
    topic: &str,
    request: TopicSkipOffsetRequest,
) -> Result<TopicOffsetResult, DashboardError> {
    let _mutation_guard = state.topic_mutation_lock.lock().await;
    state.admin_client.skip_topic_consumer_offset(topic, request).await
}

pub async fn delete_topic_from_broker(
    state: &AppState,
    topic: &str,
    broker_name: &str,
) -> Result<TopicOperationResult, DashboardError> {
    let _mutation_guard = state.topic_mutation_lock.lock().await;
    state.admin_client.delete_topic_from_broker(topic, broker_name).await
}

pub async fn delete_topic(state: &AppState, topic: &str) -> Result<TopicOperationResult, DashboardError> {
    let _mutation_guard = state.topic_mutation_lock.lock().await;
    state.admin_client.delete_topic(topic).await
}

#[cfg(test)]
mod tests {
    use super::validate_topic_mutation;
    use crate::error::DashboardError;

    #[test]
    fn rejects_queue_counts_outside_one_through_128() {
        let error = validate_topic_mutation(&crate::model::TopicMutationRequest {
            topic: "orders".into(),
            read_queue_count: 129,
            write_queue_count: 8,
            perm: 6,
            broker_name_list: vec!["broker-a".into()],
            cluster_name_list: vec![],
            order: Some(false),
            message_type: Some("NORMAL".into()),
        })
        .expect_err("invalid queue count");

        assert!(matches!(error, DashboardError::Validation(message) if message.contains("1 and 128")));
    }

    #[test]
    fn rejects_topic_names_outside_the_rocketmq_validator_contract() {
        for topic in ["orders topic", "orders@v1", &"a".repeat(128)] {
            let error = validate_topic_mutation(&crate::model::TopicMutationRequest {
                topic: topic.to_string(),
                read_queue_count: 8,
                write_queue_count: 8,
                perm: 6,
                broker_name_list: vec!["broker-a".into()],
                cluster_name_list: vec![],
                order: Some(false),
                message_type: Some("NORMAL".into()),
            })
            .expect_err("invalid RocketMQ topic name");

            assert!(matches!(error, DashboardError::Validation(message) if message.contains("Topic")));
        }
    }
}
