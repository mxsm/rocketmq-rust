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

use super::types::MessageResult;
use crate::error::DashboardError;
use rocketmq_admin_core::core::message::{DirectConsumeRequest, MessageRecord};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DlqMessagePageQueryRequest {
    pub(crate) consumer_group: String,
    pub(crate) begin: i64,
    pub(crate) end: i64,
    pub(crate) page_num: u32,
    pub(crate) page_size: u32,
    pub(crate) task_id: Option<String>,
    pub(crate) key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DlqResendMessageRequest {
    pub(crate) consumer_group: String,
    pub(crate) message_id: String,
    pub(crate) client_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DlqBatchResendMessageRequest {
    pub(crate) messages: Vec<DlqResendMessageRequest>,
}

pub(super) fn direct_request(
    group: String,
    client_id: Option<String>,
    message: &MessageRecord,
) -> MessageResult<DirectConsumeRequest> {
    if message.topic != format!("%DLQ%{group}") {
        return Err(DashboardError::Validation(
            "The selected message does not belong to this group's DLQ.".into(),
        ));
    }
    let property = |key: &str| {
        message
            .properties
            .get(key)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
    };
    let topic = property("RETRY_TOPIC")
        .filter(|topic| !topic.starts_with("%DLQ%") && !topic.starts_with("%RETRY%"))
        .ok_or_else(|| DashboardError::Validation("The DLQ message has no valid original Topic.".into()))?;
    let id = property("ORIGIN_MESSAGE_ID")
        .or_else(|| property("DLQ_ORIGIN_MESSAGE_ID"))
        // Rust send-back preserves the original producer's unique ID even when
        // no physical origin ID is attached. Resolve it within the original Topic.
        .or_else(|| property("UNIQ_KEY"))
        .ok_or_else(|| DashboardError::Validation("The DLQ message has no original message ID.".into()))?;
    Ok(DirectConsumeRequest {
        topic: topic.into(),
        consumer_group: group,
        message_id: id.into(),
        client_id: client_id.and_then(|value| {
            let value = value.trim();
            (!value.is_empty()).then(|| value.to_string())
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn message() -> MessageRecord {
        MessageRecord {
            topic: "%DLQ%g".into(),
            message_id: "dlq-id".into(),
            unique_message_id: None,
            keys: None,
            tags: None,
            born_timestamp: 0,
            store_timestamp: 0,
            born_host: String::new(),
            store_host: String::new(),
            queue_id: 0,
            queue_offset: 0,
            store_size: 0,
            reconsume_times: 0,
            body_crc: 0,
            sys_flag: 0,
            flag: 0,
            prepared_transaction_offset: 0,
            body: vec![],
            properties: [
                ("RETRY_TOPIC".into(), "orders".into()),
                ("ORIGIN_MESSAGE_ID".into(), "original-id".into()),
            ]
            .into(),
        }
    }
    #[test]
    fn dlq_resend_uses_authoritative_origin_and_optional_client() {
        let message = message();
        let request = direct_request("g".into(), Some(" client ".into()), &message).unwrap();
        assert_eq!(request.topic, "orders");
        assert_eq!(request.message_id, "original-id");
        assert_eq!(request.client_id.as_deref(), Some("client"));
        let old: DlqResendMessageRequest =
            serde_json::from_str(r#"{"consumerGroup":"g","messageId":"dlq-id"}"#).unwrap();
        assert!(
            direct_request(old.consumer_group, old.client_id, &message)
                .unwrap()
                .client_id
                .is_none()
        );
    }
    #[test]
    fn dlq_resend_resolves_preserved_rust_unique_id_in_original_topic() {
        let mut message = message();
        message.properties.remove("ORIGIN_MESSAGE_ID");
        message
            .properties
            .insert("UNIQ_KEY".into(), "producer-unique-id".into());
        let request = direct_request("g".into(), None, &message).unwrap();
        assert_eq!(request.topic, "orders");
        assert_eq!(request.message_id, "producer-unique-id");
        message
            .properties
            .insert("ORIGIN_MESSAGE_ID".into(), "physical-origin".into());
        assert_eq!(
            direct_request("g".into(), None, &message).unwrap().message_id,
            "physical-origin"
        );
        message.properties.remove("ORIGIN_MESSAGE_ID");
        message.properties.insert("UNIQ_KEY".into(), "  ".into());
        assert!(direct_request("g".into(), None, &message).is_err());
    }

    #[test]
    fn dlq_resend_rejects_wrong_group_missing_origin_and_dlq_as_original_topic() {
        let mut message = message();
        assert!(direct_request("other".into(), None, &message).is_err());
        message.properties.remove("ORIGIN_MESSAGE_ID");
        assert!(direct_request("g".into(), None, &message).is_err());
        message
            .properties
            .insert("DLQ_ORIGIN_MESSAGE_ID".into(), "fallback-id".into());
        assert_eq!(
            direct_request("g".into(), None, &message).unwrap().message_id,
            "fallback-id"
        );
        message.properties.insert("RETRY_TOPIC".into(), "%DLQ%g".into());
        assert!(direct_request("g".into(), None, &message).is_err());
    }
}
