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

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::heartbeat::message_model::MessageModel;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct QueryAssignmentRequestBody {
    pub topic: CheetahString,
    pub consumer_group: CheetahString,
    pub client_id: CheetahString,
    pub strategy_name: CheetahString,
    pub message_model: MessageModel,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    fn create_full_request() -> QueryAssignmentRequestBody {
        QueryAssignmentRequestBody {
            topic: CheetahString::from("test-topic"),
            consumer_group: CheetahString::from("group-a"),
            client_id: CheetahString::from("client-123"),
            strategy_name: CheetahString::from("avg"),
            message_model: MessageModel::Clustering,
        }
    }

    #[test]
    fn test_struct_creation_all_fields() {
        let body = QueryAssignmentRequestBody {
            topic: CheetahString::from("my-topic"),
            consumer_group: CheetahString::from("my-group"),
            client_id: CheetahString::from("my-client"),
            strategy_name: CheetahString::from("AVG"),
            message_model: MessageModel::Clustering,
        };

        assert_eq!(body.topic.as_str(), "my-topic");
        assert_eq!(body.consumer_group.as_str(), "my-group");
        assert_eq!(body.client_id.as_str(), "my-client");
        assert_eq!(body.strategy_name.as_str(), "AVG");
        assert_eq!(body.message_model, MessageModel::Clustering);
    }

    #[test]
    fn test_default_topic_is_empty() {
        let body = QueryAssignmentRequestBody::default();
        assert!(body.topic.is_empty());
    }

    #[test]
    fn test_default_consumer_group_is_empty() {
        let body = QueryAssignmentRequestBody::default();
        assert!(body.consumer_group.is_empty());
    }

    #[test]
    fn test_default_client_id_is_empty() {
        let body = QueryAssignmentRequestBody::default();
        assert!(body.client_id.is_empty());
    }

    #[test]
    fn test_default_strategy_name_is_empty() {
        let body = QueryAssignmentRequestBody::default();
        assert!(body.strategy_name.is_empty());
    }

    #[test]
    fn test_default_message_model() {
        let body = QueryAssignmentRequestBody::default();
        assert_eq!(body.message_model, MessageModel::default());
    }

    #[test]
    fn test_clone_topic() {
        let original = create_full_request();
        let cloned = original.clone();
        assert_eq!(original.topic, cloned.topic);
    }

    #[test]
    fn test_clone_consumer_group() {
        let original = create_full_request();
        let cloned = original.clone();
        assert_eq!(original.consumer_group, cloned.consumer_group);
    }

    #[test]
    fn test_clone_client_id() {
        let original = create_full_request();
        let cloned = original.clone();
        assert_eq!(original.client_id, cloned.client_id);
    }

    #[test]
    fn test_clone_strategy_name() {
        let original = create_full_request();
        let cloned = original.clone();
        assert_eq!(original.strategy_name, cloned.strategy_name);
    }

    #[test]
    fn test_clone_message_model() {
        let original = create_full_request();
        let cloned = original.clone();
        assert_eq!(original.message_model, cloned.message_model);
    }

    #[test]
    fn test_clone_is_independent() {
        let original = create_full_request();
        let mut cloned = original.clone();
        cloned.topic = CheetahString::from("mutated-topic");
        assert_ne!(original.topic, cloned.topic);
    }

    #[test]
    fn test_debug_contains_struct_name() {
        let body = create_full_request();
        let s = format!("{:?}", body);
        assert!(s.contains("QueryAssignmentRequestBody"));
    }

    #[test]
    fn test_debug_contains_topic_value() {
        let body = create_full_request();
        let s = format!("{:?}", body);
        assert!(s.contains("test-topic"));
    }

    #[test]
    fn test_debug_contains_consumer_group_value() {
        let body = create_full_request();
        let s = format!("{:?}", body);
        assert!(s.contains("group-a"));
    }

    #[test]
    fn test_debug_contains_client_id_value() {
        let body = create_full_request();
        let s = format!("{:?}", body);
        assert!(s.contains("client-123"));
    }

    #[test]
    fn test_debug_contains_strategy_name_value() {
        let body = create_full_request();
        let s = format!("{:?}", body);
        assert!(s.contains("avg"));
    }

    #[test]
    fn test_topic_valid_name() {
        let body = QueryAssignmentRequestBody {
            topic: CheetahString::from("orders-topic"),
            ..Default::default()
        };
        assert_eq!(body.topic.as_str(), "orders-topic");
    }

    #[test]
    fn test_topic_empty_string() {
        let body = QueryAssignmentRequestBody {
            topic: CheetahString::from(""),
            ..Default::default()
        };
        assert!(body.topic.is_empty());
    }

    #[test]
    fn test_topic_very_long_name() {
        let long = "t".repeat(4096);
        let body = QueryAssignmentRequestBody {
            topic: CheetahString::from(long.as_str()),
            ..Default::default()
        };
        assert_eq!(body.topic.len(), 4096);
    }

    #[test]
    fn test_consumer_group_valid_name() {
        let body = QueryAssignmentRequestBody {
            consumer_group: CheetahString::from("payment-consumers"),
            ..Default::default()
        };
        assert_eq!(body.consumer_group.as_str(), "payment-consumers");
    }

    #[test]
    fn test_consumer_group_empty_string() {
        let body = QueryAssignmentRequestBody {
            consumer_group: CheetahString::from(""),
            ..Default::default()
        };
        assert!(body.consumer_group.is_empty());
    }

    #[test]
    fn test_consumer_group_very_long_name() {
        let long = "g".repeat(4096);
        let body = QueryAssignmentRequestBody {
            consumer_group: CheetahString::from(long.as_str()),
            ..Default::default()
        };
        assert_eq!(body.consumer_group.len(), 4096);
    }

    #[test]
    fn test_client_id_valid_value() {
        let body = QueryAssignmentRequestBody {
            client_id: CheetahString::from("192.168.1.1@12345"),
            ..Default::default()
        };
        assert_eq!(body.client_id.as_str(), "192.168.1.1@12345");
    }

    #[test]
    fn test_client_id_empty_string() {
        let body = QueryAssignmentRequestBody {
            client_id: CheetahString::from(""),
            ..Default::default()
        };
        assert!(body.client_id.is_empty());
    }

    #[test]
    fn test_client_id_very_long_value() {
        let long = "c".repeat(4096);
        let body = QueryAssignmentRequestBody {
            client_id: CheetahString::from(long.as_str()),
            ..Default::default()
        };
        assert_eq!(body.client_id.len(), 4096);
    }

    #[test]
    fn test_strategy_name_avg() {
        let body = QueryAssignmentRequestBody {
            strategy_name: CheetahString::from("AVG"),
            ..Default::default()
        };
        assert_eq!(body.strategy_name.as_str(), "AVG");
    }

    #[test]
    fn test_strategy_name_circle() {
        let body = QueryAssignmentRequestBody {
            strategy_name: CheetahString::from("CIRCLE"),
            ..Default::default()
        };
        assert_eq!(body.strategy_name.as_str(), "CIRCLE");
    }

    #[test]
    fn test_strategy_name_custom() {
        let body = QueryAssignmentRequestBody {
            strategy_name: CheetahString::from("my-custom-strategy"),
            ..Default::default()
        };
        assert_eq!(body.strategy_name.as_str(), "my-custom-strategy");
    }

    #[test]
    fn test_strategy_name_empty_string() {
        let body = QueryAssignmentRequestBody {
            strategy_name: CheetahString::from(""),
            ..Default::default()
        };
        assert!(body.strategy_name.is_empty());
    }

    #[test]
    fn test_strategy_name_case_sensitivity() {
        let lower = QueryAssignmentRequestBody {
            strategy_name: CheetahString::from("avg"),
            ..Default::default()
        };
        let upper = QueryAssignmentRequestBody {
            strategy_name: CheetahString::from("AVG"),
            ..Default::default()
        };
        assert_ne!(lower.strategy_name, upper.strategy_name);
    }

    #[test]
    fn test_message_model_broadcasting() {
        let body = QueryAssignmentRequestBody {
            message_model: MessageModel::Broadcasting,
            ..Default::default()
        };
        assert_eq!(body.message_model, MessageModel::Broadcasting);
    }

    #[test]
    fn test_message_model_clustering() {
        let body = QueryAssignmentRequestBody {
            message_model: MessageModel::Clustering,
            ..Default::default()
        };
        assert_eq!(body.message_model, MessageModel::Clustering);
    }

    #[test]
    fn test_message_model_serialization_clustering() {
        let body = QueryAssignmentRequestBody {
            message_model: MessageModel::Clustering,
            ..Default::default()
        };
        let json = serde_json::to_value(&body).unwrap();
        assert!(!json["messageModel"].is_null());
        assert_eq!(json["messageModel"].as_str().unwrap_or(""), "CLUSTERING");
    }

    #[test]
    fn test_message_model_serialization_broadcasting() {
        let body = QueryAssignmentRequestBody {
            message_model: MessageModel::Broadcasting,
            ..Default::default()
        };
        let json = serde_json::to_value(&body).unwrap();
        assert_eq!(json["messageModel"].as_str().unwrap_or(""), "BROADCASTING");
    }

    #[test]
    fn test_message_model_deserialization_clustering() {
        let json = r#"{"topic":"","consumerGroup":"","clientId":"","strategyName":"","messageModel":"CLUSTERING"}"#;
        let body: QueryAssignmentRequestBody = serde_json::from_str(json).unwrap();
        assert_eq!(body.message_model, MessageModel::Clustering);
    }

    #[test]
    fn test_message_model_deserialization_broadcasting() {
        let json = r#"{"topic":"","consumerGroup":"","clientId":"","strategyName":"","messageModel":"BROADCASTING"}"#;
        let body: QueryAssignmentRequestBody = serde_json::from_str(json).unwrap();
        assert_eq!(body.message_model, MessageModel::Broadcasting);
    }

    #[test]
    fn test_serialization_produces_camel_case_consumer_group() {
        let body = create_full_request();
        let json = serde_json::to_value(&body).unwrap();
        assert!(
            json.get("consumerGroup").is_some(),
            "expected camelCase key 'consumerGroup'"
        );
        assert!(json.get("consumer_group").is_none(), "snake_case key must not appear");
    }

    #[test]
    fn test_serialization_produces_camel_case_strategy_name() {
        let body = create_full_request();
        let json = serde_json::to_value(&body).unwrap();
        assert!(
            json.get("strategyName").is_some(),
            "expected camelCase key 'strategyName'"
        );
        assert!(json.get("strategy_name").is_none());
    }

    #[test]
    fn test_serialization_produces_camel_case_client_id() {
        let body = create_full_request();
        let json = serde_json::to_value(&body).unwrap();
        assert!(json.get("clientId").is_some(), "expected camelCase key 'clientId'");
        assert!(json.get("client_id").is_none());
    }

    #[test]
    fn test_serialization_produces_camel_case_message_model() {
        let body = create_full_request();
        let json = serde_json::to_value(&body).unwrap();
        assert!(
            json.get("messageModel").is_some(),
            "expected camelCase key 'messageModel'"
        );
        assert!(json.get("message_model").is_none());
    }

    #[test]
    fn test_serde_roundtrip_topic() {
        let original = create_full_request();
        let json = serde_json::to_string(&original).unwrap();
        let recovered: QueryAssignmentRequestBody = serde_json::from_str(&json).unwrap();
        assert_eq!(original.topic, recovered.topic);
    }

    #[test]
    fn test_serde_roundtrip_consumer_group() {
        let original = create_full_request();
        let json = serde_json::to_string(&original).unwrap();
        let recovered: QueryAssignmentRequestBody = serde_json::from_str(&json).unwrap();
        assert_eq!(original.consumer_group, recovered.consumer_group);
    }

    #[test]
    fn test_serde_roundtrip_client_id() {
        let original = create_full_request();
        let json = serde_json::to_string(&original).unwrap();
        let recovered: QueryAssignmentRequestBody = serde_json::from_str(&json).unwrap();
        assert_eq!(original.client_id, recovered.client_id);
    }

    #[test]
    fn test_serde_roundtrip_strategy_name() {
        let original = create_full_request();
        let json = serde_json::to_string(&original).unwrap();
        let recovered: QueryAssignmentRequestBody = serde_json::from_str(&json).unwrap();
        assert_eq!(original.strategy_name, recovered.strategy_name);
    }

    #[test]
    fn test_serde_roundtrip_message_model() {
        let original = create_full_request();
        let json = serde_json::to_string(&original).unwrap();
        let recovered: QueryAssignmentRequestBody = serde_json::from_str(&json).unwrap();
        assert_eq!(original.message_model, recovered.message_model);
    }

    #[test]
    fn test_deserialization_full_valid_json() {
        let json = r#"{
            "topic": "rocketmq-rust",
            "consumerGroup": "test-group",
            "clientId": "node-1",
            "strategyName": "round-robin",
            "messageModel": "CLUSTERING"
        }"#;
        let body: QueryAssignmentRequestBody = serde_json::from_str(json).unwrap();
        assert_eq!(body.topic.as_str(), "rocketmq-rust");
        assert_eq!(body.consumer_group.as_str(), "test-group");
        assert_eq!(body.client_id.as_str(), "node-1");
        assert_eq!(body.strategy_name.as_str(), "round-robin");
        assert_eq!(body.message_model, MessageModel::Clustering);
    }

    #[test]
    fn test_deserialization_missing_optional_fields_uses_defaults() {
        let json =
            r#"{"topic":"only-topic","consumerGroup":"","clientId":"","strategyName":"","messageModel":"CLUSTERING"}"#;
        let body: QueryAssignmentRequestBody = serde_json::from_str(json).unwrap();
        assert_eq!(body.topic.as_str(), "only-topic");
        assert!(body.consumer_group.is_empty());
        assert!(body.client_id.is_empty());
        assert!(body.strategy_name.is_empty());
    }

    #[test]
    fn test_deserialization_extra_fields_are_ignored() {
        let json = r#"{
            "topic": "t",
            "consumerGroup": "g",
            "clientId": "c",
            "strategyName": "s",
            "messageModel": "CLUSTERING",
            "unknownField": "should-be-ignored",
            "anotherExtra": 1
        }"#;
        let body: QueryAssignmentRequestBody = serde_json::from_str(json).unwrap();
        assert_eq!(body.topic.as_str(), "t");
    }

    #[test]
    fn test_deserialization_malformed_json_returns_error() {
        let bad_json = r#"{ "topic": "broken", "consumerGroup": "#;
        let result: Result<QueryAssignmentRequestBody, _> = serde_json::from_str(bad_json);
        assert!(result.is_err(), "malformed JSON should return an error");
    }

    #[test]
    fn test_deserialization_wrong_type_for_topic_returns_error() {
        let json = r#"{"topic":123,"consumerGroup":"g","clientId":"c","strategyName":"s","messageModel":"CLUSTERING"}"#;
        let result: Result<QueryAssignmentRequestBody, _> = serde_json::from_str(json);
        assert!(result.is_err(), "integer for topic should fail deserialization");
    }

    #[test]
    fn test_deserialization_wrong_type_for_message_model_returns_error() {
        let json =
            r#"{"topic":"t","consumerGroup":"g","clientId":"c","strategyName":"s","messageModel":"INVALID_VARIANT"}"#;
        let result: Result<QueryAssignmentRequestBody, _> = serde_json::from_str(json);
        assert!(
            result.is_err(),
            "unknown MessageModel variant should fail deserialization"
        );
    }

    #[test]
    fn test_special_characters_in_topic() {
        let body = QueryAssignmentRequestBody {
            topic: CheetahString::from("topic/with:special@chars#"),
            ..Default::default()
        };
        let json = serde_json::to_string(&body).unwrap();
        let decoded: QueryAssignmentRequestBody = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.topic.as_str(), "topic/with:special@chars#");
    }

    #[test]
    fn test_special_characters_in_consumer_group() {
        let body = QueryAssignmentRequestBody {
            consumer_group: CheetahString::from("!@#$%^&*()"),
            ..Default::default()
        };
        let json = serde_json::to_string(&body).unwrap();
        let decoded: QueryAssignmentRequestBody = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.consumer_group.as_str(), "!@#$%^&*()");
    }

    #[test]
    fn test_unicode_characters_in_fields() {
        let body = QueryAssignmentRequestBody {
            topic: CheetahString::from("主题-🚀"),
            consumer_group: CheetahString::from("消费者组"),
            client_id: CheetahString::from("клиент"),
            strategy_name: CheetahString::from("🚀-strategy"),
            message_model: MessageModel::Broadcasting,
        };
        let json = serde_json::to_string(&body).unwrap();
        let decoded: QueryAssignmentRequestBody = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.topic.as_str(), "主题-🚀");
        assert_eq!(decoded.consumer_group.as_str(), "消费者组");
        assert_eq!(decoded.client_id.as_str(), "клиент");
        assert_eq!(decoded.strategy_name.as_str(), "🚀-strategy");
        assert_eq!(decoded.message_model, MessageModel::Broadcasting);
    }

    #[test]
    fn test_whitespace_only_client_id() {
        let body = QueryAssignmentRequestBody {
            client_id: CheetahString::from("   "),
            ..Default::default()
        };
        let json = serde_json::to_string(&body).unwrap();
        let decoded: QueryAssignmentRequestBody = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.client_id.as_str(), "   ");
    }

    #[test]
    fn test_empty_struct_serialization() {
        let body = QueryAssignmentRequestBody::default();
        let json = serde_json::to_string(&body).unwrap();
        let decoded: QueryAssignmentRequestBody = serde_json::from_str(&json).unwrap();
        assert!(decoded.topic.is_empty());
        assert!(decoded.consumer_group.is_empty());
        assert!(decoded.client_id.is_empty());
        assert!(decoded.strategy_name.is_empty());
    }

    #[test]
    fn test_very_long_fields_roundtrip() {
        let long_topic = "a".repeat(8192);
        let long_group = "b".repeat(8192);
        let body = QueryAssignmentRequestBody {
            topic: CheetahString::from(long_topic.as_str()),
            consumer_group: CheetahString::from(long_group.as_str()),
            ..Default::default()
        };
        let json = serde_json::to_string(&body).unwrap();
        let decoded: QueryAssignmentRequestBody = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.topic.len(), 8192);
        assert_eq!(decoded.consumer_group.len(), 8192);
    }

    #[test]
    fn test_clustering_with_avg_strategy() {
        let body = QueryAssignmentRequestBody {
            topic: CheetahString::from("orders"),
            consumer_group: CheetahString::from("order-consumers"),
            client_id: CheetahString::from("node-1"),
            strategy_name: CheetahString::from("AVG"),
            message_model: MessageModel::Clustering,
        };
        assert_eq!(body.message_model, MessageModel::Clustering);
        assert_eq!(body.strategy_name.as_str(), "AVG");
    }

    #[test]
    fn test_broadcasting_with_circle_strategy() {
        let body = QueryAssignmentRequestBody {
            topic: CheetahString::from("broadcast-topic"),
            consumer_group: CheetahString::from("broadcast-group"),
            client_id: CheetahString::from("node-2"),
            strategy_name: CheetahString::from("CIRCLE"),
            message_model: MessageModel::Broadcasting,
        };
        assert_eq!(body.message_model, MessageModel::Broadcasting);
        assert_eq!(body.strategy_name.as_str(), "CIRCLE");
    }

    #[test]
    fn test_all_empty_strings_with_clustering() {
        let body = QueryAssignmentRequestBody {
            topic: CheetahString::from(""),
            consumer_group: CheetahString::from(""),
            client_id: CheetahString::from(""),
            strategy_name: CheetahString::from(""),
            message_model: MessageModel::Clustering,
        };
        let json = serde_json::to_string(&body).unwrap();
        let decoded: QueryAssignmentRequestBody = serde_json::from_str(&json).unwrap();
        assert!(decoded.topic.is_empty());
        assert_eq!(decoded.message_model, MessageModel::Clustering);
    }

    #[test]
    fn test_two_instances_with_same_values_are_field_equal() {
        let a = create_full_request();
        let b = create_full_request();
        assert_eq!(a.topic, b.topic);
        assert_eq!(a.consumer_group, b.consumer_group);
        assert_eq!(a.client_id, b.client_id);
        assert_eq!(a.strategy_name, b.strategy_name);
        assert_eq!(a.message_model, b.message_model);
    }

    #[test]
    fn test_two_instances_with_different_message_model_differ() {
        let a = QueryAssignmentRequestBody {
            message_model: MessageModel::Clustering,
            ..Default::default()
        };
        let b = QueryAssignmentRequestBody {
            message_model: MessageModel::Broadcasting,
            ..Default::default()
        };
        assert_ne!(a.message_model, b.message_model);
    }

    #[test]
    fn test_strategy_avg_uppercase() {
        let body = QueryAssignmentRequestBody {
            strategy_name: CheetahString::from("AVG"),
            ..Default::default()
        };
        assert_eq!(body.strategy_name.as_str(), "AVG");
    }

    #[test]
    fn test_strategy_avg_lowercase() {
        let body = QueryAssignmentRequestBody {
            strategy_name: CheetahString::from("avg"),
            ..Default::default()
        };
        assert_eq!(body.strategy_name.as_str(), "avg");
    }

    #[test]
    fn test_strategy_circle() {
        let body = QueryAssignmentRequestBody {
            strategy_name: CheetahString::from("CIRCLE"),
            ..Default::default()
        };
        assert_eq!(body.strategy_name.as_str(), "CIRCLE");
    }

    #[test]
    fn test_strategy_name_survives_roundtrip() {
        for name in &["AVG", "CIRCLE", "custom-strategy", "", "🚀"] {
            let body = QueryAssignmentRequestBody {
                strategy_name: CheetahString::from(*name),
                ..Default::default()
            };
            let json = serde_json::to_string(&body).unwrap();
            let decoded: QueryAssignmentRequestBody = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded.strategy_name.as_str(), *name);
        }
    }
}
