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

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MessageKeyQueryRequest {
    pub topic: String,
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MessageIdQueryRequest {
    pub topic: String,
    pub message_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ViewMessageRequest {
    pub topic: String,
    pub message_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MessageTraceQueryRequest {
    pub trace_topic: String,
    pub message_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MessagePageQueryRequest {
    pub topic: String,
    pub begin: i64,
    pub end: i64,
    pub page_num: u32,
    pub page_size: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DlqMessagePageQueryRequest {
    pub consumer_group: String,
    pub begin: i64,
    pub end: i64,
    pub page_num: u32,
    pub page_size: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DlqViewMessageRequest {
    pub consumer_group: String,
    pub message_id: String,
}

#[cfg(test)]
mod tests {
    #[test]
    fn message_key_query_request_uses_java_dashboard_field_names() {
        let request = super::MessageKeyQueryRequest {
            topic: "TopicTest".to_string(),
            key: "order-1".to_string(),
        };

        let json = serde_json::to_string(&request).expect("serialize key request");

        assert!(json.contains("\"topic\""));
        assert!(json.contains("\"key\""));
    }

    #[test]
    fn message_id_query_request_uses_java_dashboard_field_names() {
        let request = super::MessageIdQueryRequest {
            topic: "TopicTest".to_string(),
            message_id: "msg-1".to_string(),
        };

        let json = serde_json::to_string(&request).expect("serialize id request");

        assert!(json.contains("\"topic\""));
        assert!(json.contains("\"messageId\""));
    }

    #[test]
    fn message_page_query_request_uses_java_dashboard_field_names() {
        let request = super::MessagePageQueryRequest {
            topic: "TopicTest".to_string(),
            begin: 1_700_000_000_000,
            end: 1_700_000_100_000,
            page_num: 2,
            page_size: 25,
            task_id: Some("task-1".to_string()),
        };

        let json = serde_json::to_string(&request).expect("serialize page request");

        assert!(json.contains("\"begin\""));
        assert!(json.contains("\"end\""));
        assert!(json.contains("\"pageNum\""));
        assert!(json.contains("\"pageSize\""));
        assert!(json.contains("\"taskId\""));
    }

    #[test]
    fn message_trace_query_request_uses_java_dashboard_field_names() {
        let request = super::MessageTraceQueryRequest {
            trace_topic: "RMQ_SYS_TRACE_TOPIC".to_string(),
            message_id: "msg-1".to_string(),
        };

        let json = serde_json::to_string(&request).expect("serialize trace query request");

        assert!(json.contains("\"traceTopic\""));
        assert!(json.contains("\"messageId\""));
    }

    #[test]
    fn dlq_message_page_query_request_uses_java_dashboard_field_names() {
        let request = super::DlqMessagePageQueryRequest {
            consumer_group: "group-a".to_string(),
            begin: 1_700_000_000_000,
            end: 1_700_000_100_000,
            page_num: 2,
            page_size: 25,
            task_id: Some("task-1".to_string()),
        };

        let json = serde_json::to_string(&request).expect("serialize dlq page request");

        assert!(json.contains("\"consumerGroup\""));
        assert!(json.contains("\"begin\""));
        assert!(json.contains("\"end\""));
        assert!(json.contains("\"pageNum\""));
        assert!(json.contains("\"pageSize\""));
        assert!(json.contains("\"taskId\""));
    }

    #[test]
    fn dlq_view_message_request_uses_java_dashboard_field_names() {
        let request = super::DlqViewMessageRequest {
            consumer_group: "group-a".to_string(),
            message_id: "msg-1".to_string(),
        };

        let json = serde_json::to_string(&request).expect("serialize dlq detail request");

        assert!(json.contains("\"consumerGroup\""));
        assert!(json.contains("\"messageId\""));
    }
}
