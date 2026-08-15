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
use std::collections::BTreeMap;

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicInfo {
    pub topic: String,
    pub broker_name: Option<String>,
    pub brokers: Vec<String>,
    pub clusters: Vec<String>,
    pub read_queue_count: u32,
    pub write_queue_count: u32,
    pub perm: u32,
    pub category: String,
    pub message_type: String,
    pub order: bool,
    pub system_topic: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicTargetOptionView {
    pub cluster_name: String,
    pub broker_names: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicListView {
    pub items: Vec<TopicInfo>,
    pub total: usize,
    pub targets: Vec<TopicTargetOptionView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicConfigView {
    pub topic_name: String,
    pub broker_name: String,
    pub cluster_name: Option<String>,
    pub broker_name_list: Vec<String>,
    pub cluster_name_list: Vec<String>,
    pub read_queue_nums: i32,
    pub write_queue_nums: i32,
    pub perm: i32,
    pub order: bool,
    pub message_type: String,
    pub attributes: BTreeMap<String, String>,
    pub inconsistent_fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TopicConsumerView {
    pub consumer_group: String,
    pub total_diff: i64,
    pub inflight_diff: i64,
    pub consume_tps: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TopicConsumersView {
    pub items: Vec<TopicConsumerView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicRouteInfo {
    pub topic: String,
    pub brokers: Vec<TopicRouteBroker>,
    pub queues: Vec<TopicRouteQueue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicRouteBroker {
    pub broker_name: String,
    pub broker_addrs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicRouteQueue {
    pub broker_name: String,
    pub read_queue_nums: u32,
    pub write_queue_nums: u32,
    pub perm: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicStatsInfo {
    pub topic: String,
    pub queue_count: usize,
    pub total_message_count: i64,
    pub total_min_offset: i64,
    pub total_max_offset: i64,
    pub offsets: Vec<TopicQueueOffsetView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicQueueOffsetView {
    pub broker_name: String,
    pub queue_id: i32,
    pub min_offset: i64,
    pub max_offset: i64,
    pub last_update_timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicMutationRequest {
    pub topic: String,
    pub read_queue_count: u32,
    pub write_queue_count: u32,
    pub perm: u32,
    pub broker_name_list: Vec<String>,
    pub cluster_name_list: Vec<String>,
    pub order: Option<bool>,
    pub message_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicTargetResult {
    pub target: String,
    pub success: bool,
    pub message: String,
}

impl TopicTargetResult {
    pub(crate) fn success(target: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            target: target.into(),
            success: true,
            message: message.into(),
        }
    }

    pub(crate) fn failure(target: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            target: target.into(),
            success: false,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicOperationResult {
    pub operation: String,
    pub topic: String,
    pub success: bool,
    pub target_count: usize,
    pub message: String,
    pub targets: Vec<TopicTargetResult>,
}

pub(crate) fn build_operation_result(
    operation: impl Into<String>,
    topic: impl Into<String>,
    targets: Vec<TopicTargetResult>,
) -> TopicOperationResult {
    let target_count = targets.len();
    let succeeded_count = targets.iter().filter(|target| target.success).count();
    let failed_count = target_count.saturating_sub(succeeded_count);
    TopicOperationResult {
        operation: operation.into(),
        topic: topic.into(),
        success: failed_count == 0,
        target_count,
        message: format!("{target_count} targets: {succeeded_count} succeeded, {failed_count} failed"),
        targets,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MutationResult {
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::TopicInfo;
    use super::TopicTargetResult;
    use super::build_operation_result;

    #[test]
    fn topic_catalog_dto_serializes_authoritative_metadata() {
        let topic = TopicInfo {
            topic: "orders".into(),
            broker_name: Some("broker-a".into()),
            brokers: vec!["broker-a".into()],
            clusters: vec!["DefaultCluster".into()],
            read_queue_count: 8,
            write_queue_count: 8,
            perm: 6,
            category: "NORMAL".into(),
            message_type: "NORMAL".into(),
            order: false,
            system_topic: false,
        };
        let json = serde_json::to_value(topic).expect("topic serializes");
        assert_eq!(json["messageType"], "NORMAL");
        assert_eq!(json["brokers"][0], "broker-a");
        assert_eq!(json["systemTopic"], false);
    }

    #[test]
    fn partial_target_failure_is_not_global_success() {
        let result = build_operation_result(
            "UPDATE",
            "orders",
            vec![
                TopicTargetResult::success("broker-a", "saved"),
                TopicTargetResult::failure("broker-b", "unavailable"),
            ],
        );

        assert!(!result.success);
        assert_eq!(result.target_count, 2);
    }
}
