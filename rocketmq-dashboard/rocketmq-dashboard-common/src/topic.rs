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

//! Shared Topic-domain request models for dashboard implementations.
//!
//! These structs mirror the request surface used by the official Java
//! `apache/rocketmq-dashboard` application so Tauri and future dashboard
//! frontends can share a stable protocol layer.

use serde::Deserialize;
use serde::Serialize;

/// Query parameters for `/topic/list.query`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicListRequest {
    pub skip_sys_process: bool,
    pub skip_retry_and_dlq: bool,
}

/// Common topic-only query used by route, stats, consumer, and config endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicQueryRequest {
    pub topic: String,
}

/// Query parameters for `/topic/examineTopicConfig.query`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicConfigQueryRequest {
    pub topic: String,
    pub broker_name: Option<String>,
}

/// Request body for `/topic/createOrUpdate.do`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicConfigRequest {
    pub cluster_name_list: Vec<String>,
    pub broker_name_list: Vec<String>,
    pub topic_name: String,
    pub write_queue_nums: i32,
    pub read_queue_nums: i32,
    pub perm: i32,
    pub order: bool,
    pub message_type: Option<String>,
}

/// Request body for `/topic/sendTopicMessage.do`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SendTopicMessageRequest {
    pub topic: String,
    pub key: String,
    pub tag: String,
    pub message_body: String,
    pub trace_enabled: bool,
}

/// Request parameters for `/topic/deleteTopic.do`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeleteTopicRequest {
    pub topic: String,
    pub cluster_name: Option<String>,
}

/// Request body used by frontend-new for `/topic/deleteTopicByBroker.do`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeleteTopicByBrokerRequest {
    pub broker_name: String,
    pub topic: String,
}

/// Request body for `/consumer/resetOffset.do` and `/consumer/skipAccumulate.do`,
/// both of which are triggered from the Topic page in the official dashboard.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResetOffsetRequest {
    pub consumer_group_list: Vec<String>,
    pub topic: String,
    pub reset_time: i64,
    pub force: bool,
}

#[cfg(test)]
mod tests {
    use super::ResetOffsetRequest;
    use super::SendTopicMessageRequest;
    use super::TopicConfigRequest;

    #[test]
    fn topic_config_request_uses_camel_case_fields() {
        let request = TopicConfigRequest {
            cluster_name_list: vec!["DefaultCluster".to_string()],
            broker_name_list: vec!["broker-a".to_string()],
            topic_name: "test-topic".to_string(),
            write_queue_nums: 8,
            read_queue_nums: 8,
            perm: 6,
            order: false,
            message_type: Some("NORMAL".to_string()),
        };

        let json = serde_json::to_string(&request).expect("serialize topic config request");

        assert!(json.contains("\"clusterNameList\""));
        assert!(json.contains("\"brokerNameList\""));
        assert!(json.contains("\"topicName\""));
        assert!(json.contains("\"messageType\""));
    }

    #[test]
    fn send_topic_message_request_uses_java_dashboard_field_names() {
        let request = SendTopicMessageRequest {
            topic: "test-topic".to_string(),
            key: "order-1".to_string(),
            tag: "TagA".to_string(),
            message_body: "hello".to_string(),
            trace_enabled: true,
        };

        let json = serde_json::to_string(&request).expect("serialize send topic message request");

        assert!(json.contains("\"messageBody\""));
        assert!(json.contains("\"traceEnabled\""));
    }

    #[test]
    fn reset_offset_request_uses_topic_page_payload_shape() {
        let request = ResetOffsetRequest {
            consumer_group_list: vec!["group-a".to_string()],
            topic: "test-topic".to_string(),
            reset_time: -1,
            force: true,
        };

        let json = serde_json::to_string(&request).expect("serialize reset offset request");

        assert!(json.contains("\"consumerGroupList\""));
        assert!(json.contains("\"resetTime\""));
        assert!(json.contains("\"force\":true"));
    }
}
