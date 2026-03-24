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

//! Shared Consumer-domain request models for dashboard implementations.

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerGroupListRequest {
    #[serde(default)]
    pub skip_sys_group: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerGroupRefreshRequest {
    pub consumer_group: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerConnectionQueryRequest {
    pub consumer_group: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerTopicDetailQueryRequest {
    pub consumer_group: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerConfigQueryRequest {
    pub consumer_group: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerCreateOrUpdateRequest {
    #[serde(default)]
    pub cluster_name_list: Vec<String>,
    #[serde(default)]
    pub broker_name_list: Vec<String>,
    pub consumer_group: String,
    pub consume_enable: bool,
    pub consume_from_min_enable: bool,
    pub consume_broadcast_enable: bool,
    pub consume_message_orderly: bool,
    pub retry_queue_nums: i32,
    pub retry_max_times: i32,
    pub broker_id: u64,
    pub which_broker_when_consume_slowly: u64,
    pub notify_consumer_ids_changed_enable: bool,
    pub group_sys_flag: i32,
    pub consume_timeout_minute: i32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerDeleteRequest {
    pub consumer_group: String,
    #[serde(default)]
    pub broker_name_list: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::ConsumerConfigQueryRequest;
    use super::ConsumerConnectionQueryRequest;
    use super::ConsumerCreateOrUpdateRequest;
    use super::ConsumerDeleteRequest;
    use super::ConsumerGroupListRequest;
    use super::ConsumerGroupRefreshRequest;
    use super::ConsumerTopicDetailQueryRequest;

    #[test]
    fn consumer_group_list_request_uses_expected_field_names() {
        let request = ConsumerGroupListRequest {
            skip_sys_group: false,
            address: Some("127.0.0.1:8080".to_string()),
        };

        let json = serde_json::to_string(&request).expect("serialize consumer group list request");
        assert!(json.contains("\"skipSysGroup\""));
        assert!(json.contains("\"address\""));
    }

    #[test]
    fn consumer_group_refresh_request_uses_expected_field_names() {
        let request = ConsumerGroupRefreshRequest {
            consumer_group: "group-a".to_string(),
            address: None,
        };

        let json = serde_json::to_string(&request).expect("serialize consumer refresh request");
        assert!(json.contains("\"consumerGroup\""));
    }

    #[test]
    fn consumer_connection_query_request_uses_expected_field_names() {
        let request = ConsumerConnectionQueryRequest {
            consumer_group: "group-a".to_string(),
            address: Some("127.0.0.1:10911".to_string()),
        };

        let json = serde_json::to_string(&request).expect("serialize consumer connection request");
        assert!(json.contains("\"consumerGroup\""));
        assert!(json.contains("\"address\""));
    }

    #[test]
    fn consumer_topic_detail_query_request_uses_expected_field_names() {
        let request = ConsumerTopicDetailQueryRequest {
            consumer_group: "group-a".to_string(),
            address: Some("127.0.0.1:10911".to_string()),
        };

        let json = serde_json::to_string(&request).expect("serialize consumer topic detail request");
        assert!(json.contains("\"consumerGroup\""));
        assert!(json.contains("\"address\""));
    }

    #[test]
    fn consumer_config_query_request_uses_expected_field_names() {
        let request = ConsumerConfigQueryRequest {
            consumer_group: "group-a".to_string(),
            address: Some("127.0.0.1:10911".to_string()),
        };

        let json = serde_json::to_string(&request).expect("serialize consumer config request");
        assert!(json.contains("\"consumerGroup\""));
        assert!(json.contains("\"address\""));
    }

    #[test]
    fn consumer_create_or_update_request_uses_expected_field_names() {
        let request = ConsumerCreateOrUpdateRequest {
            cluster_name_list: vec!["DefaultCluster".to_string()],
            broker_name_list: vec!["broker-a".to_string()],
            consumer_group: "group-a".to_string(),
            consume_enable: true,
            consume_from_min_enable: true,
            consume_broadcast_enable: false,
            consume_message_orderly: true,
            retry_queue_nums: 1,
            retry_max_times: 16,
            broker_id: 0,
            which_broker_when_consume_slowly: 1,
            notify_consumer_ids_changed_enable: true,
            group_sys_flag: 0,
            consume_timeout_minute: 15,
        };

        let json = serde_json::to_string(&request).expect("serialize consumer create or update request");
        assert!(json.contains("\"clusterNameList\""));
        assert!(json.contains("\"brokerNameList\""));
        assert!(json.contains("\"consumerGroup\""));
        assert!(json.contains("\"consumeEnable\""));
        assert!(json.contains("\"consumeFromMinEnable\""));
        assert!(json.contains("\"consumeBroadcastEnable\""));
        assert!(json.contains("\"consumeMessageOrderly\""));
        assert!(json.contains("\"retryQueueNums\""));
        assert!(json.contains("\"retryMaxTimes\""));
        assert!(json.contains("\"brokerId\""));
        assert!(json.contains("\"whichBrokerWhenConsumeSlowly\""));
        assert!(json.contains("\"notifyConsumerIdsChangedEnable\""));
        assert!(json.contains("\"groupSysFlag\""));
        assert!(json.contains("\"consumeTimeoutMinute\""));
    }

    #[test]
    fn consumer_delete_request_uses_expected_field_names() {
        let request = ConsumerDeleteRequest {
            consumer_group: "group-a".to_string(),
            broker_name_list: vec!["broker-a".to_string()],
        };

        let json = serde_json::to_string(&request).expect("serialize consumer delete request");
        assert!(json.contains("\"consumerGroup\""));
        assert!(json.contains("\"brokerNameList\""));
    }
}
