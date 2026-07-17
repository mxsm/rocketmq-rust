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

use rocketmq_admin_core::core::topic::TopicSendResult;

use super::map_send_result;
use super::normalize_topic_message_body;
use super::quote_numeric_object_keys;

#[test]
fn normalize_topic_message_body_accepts_standard_json() {
    assert_eq!(
        normalize_topic_message_body(r#"{"1":"value","name":"rocketmq"}"#).unwrap(),
        r#"{"1":"value","name":"rocketmq"}"#
    );
}

#[test]
fn normalize_topic_message_body_accepts_relaxed_json_with_numeric_keys() {
    assert_eq!(
        normalize_topic_message_body(r#"{1: 'value', nested: {2: true}}"#).unwrap(),
        r#"{"1":"value","nested":{"2":true}}"#
    );
}

#[test]
fn quote_numeric_object_keys_only_changes_object_keys() {
    assert_eq!(
        quote_numeric_object_keys(r#"{1: 'value', nested: {2: true}, list: [1, 2, 3]}"#),
        r#"{"1": 'value', nested: {"2": true}, list: [1, 2, 3]}"#
    );
}

#[test]
fn normalize_topic_message_body_preserves_plain_text() {
    assert_eq!(
        normalize_topic_message_body("plain text body").unwrap(),
        "plain text body"
    );
}

#[test]
fn owned_send_result_preserves_queue_and_transaction_metadata() {
    let result = map_send_result(TopicSendResult {
        topic: "TopicTest".to_string(),
        send_status: "SendOk (COMMIT_MESSAGE)".to_string(),
        message_id: Some("msg-1".to_string()),
        broker_name: Some("broker-a".to_string()),
        queue_id: Some(3),
        queue_offset: 42,
        transaction_id: Some("tx-1".to_string()),
        region_id: Some("region-a".to_string()),
        local_transaction_state: Some("COMMIT_MESSAGE".to_string()),
    });
    assert_eq!(result.topic, "TopicTest");
    assert_eq!(result.send_status, "SendOk (COMMIT_MESSAGE)");
    assert_eq!(result.message_id.as_deref(), Some("msg-1"));
    assert_eq!(result.broker_name.as_deref(), Some("broker-a"));
    assert_eq!(result.queue_id, Some(3));
    assert_eq!(result.queue_offset, 42);
    assert_eq!(result.transaction_id.as_deref(), Some("tx-1"));
    assert_eq!(result.region_id.as_deref(), Some("region-a"));
    assert_eq!(result.local_transaction_state.as_deref(), Some("COMMIT_MESSAGE"));
}
