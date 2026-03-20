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

#[cfg(test)]
mod tests {
    use super::ConsumerConnectionQueryRequest;
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
}
