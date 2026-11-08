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

//! Native Message queries and bounded, explicitly clearable detail payloads.

use std::{collections::BTreeMap, fmt};

use crate::{ConnectionScope, TopicIdentity};

pub const MESSAGE_PAGE_SIZE_MAX: u32 = 50;
pub const MESSAGE_KEY_MATCH_LIMIT: usize = 64;
pub const MESSAGE_BODY_MAX_BYTES: usize = 256 * 1024;
pub const MESSAGE_PROPERTIES_MAX_BYTES: usize = 64 * 1024;

#[derive(Clone, PartialEq, Eq)]
pub enum MessageSearchQuery {
    TopicRange {
        topic: String,
        begin: i64,
        end: i64,
        page: u32,
        page_size: u32,
    },
    Key {
        topic: String,
        key: String,
    },
    MessageId {
        topic: String,
        message_id: String,
    },
}

impl MessageSearchQuery {
    pub fn topic(&self) -> &str {
        match self {
            Self::TopicRange { topic, .. } | Self::Key { topic, .. } | Self::MessageId { topic, .. } => topic,
        }
    }
    pub fn validate(&self) -> Result<(), MessageValidationError> {
        TopicIdentity::parse(self.topic()).map_err(|_| MessageValidationError::Topic)?;
        match self {
            Self::TopicRange {
                begin,
                end,
                page,
                page_size,
                ..
            } => {
                if *begin < 0 || end < begin {
                    return Err(MessageValidationError::TimeRange);
                }
                if *page == 0 || !(1..=MESSAGE_PAGE_SIZE_MAX).contains(page_size) {
                    return Err(MessageValidationError::Pagination);
                }
            }
            Self::Key { key, .. } if !valid_identity(key) => return Err(MessageValidationError::Key),
            Self::MessageId { message_id, .. } if !valid_identity(message_id) => {
                return Err(MessageValidationError::MessageId)
            }
            Self::Key { .. } | Self::MessageId { .. } => {}
        }
        Ok(())
    }
    pub fn page(&self) -> u32 {
        match self {
            Self::TopicRange { page, .. } => *page,
            _ => 1,
        }
    }
    pub fn with_page(&self, number: u32) -> Result<Self, MessageValidationError> {
        let Self::TopicRange {
            topic,
            begin,
            end,
            page_size,
            ..
        } = self
        else {
            return Err(MessageValidationError::Pagination);
        };
        let query = Self::TopicRange {
            topic: topic.clone(),
            begin: *begin,
            end: *end,
            page: number,
            page_size: *page_size,
        };
        query.validate()?;
        Ok(query)
    }
}

impl fmt::Debug for MessageSearchQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MessageSearchQuery")
            .field(
                "mode",
                &match self {
                    Self::TopicRange { .. } => "TopicRange",
                    Self::Key { .. } => "Key",
                    Self::MessageId { .. } => "MessageId",
                },
            )
            .field("page", &self.page())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MessageIdentity {
    pub topic: String,
    pub message_id: String,
}

impl fmt::Debug for MessageIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MessageIdentity { .. }")
    }
}

/// A bounded metadata label. Debug retains only size and truncation information.
#[derive(Clone, PartialEq, Eq)]
pub struct MessageTextPreview {
    pub text: String,
    pub truncated: bool,
}
impl MessageTextPreview {
    pub fn bounded(value: &str) -> Self {
        const MAX_BYTES: usize = 1024;
        Self {
            text: value[..value.floor_char_boundary(MAX_BYTES)].to_string(),
            truncated: value.len() > MAX_BYTES,
        }
    }
    pub fn display(&self) -> String {
        if self.truncated {
            format!("{}…", self.text)
        } else {
            self.text.clone()
        }
    }
}
impl fmt::Debug for MessageTextPreview {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MessageTextPreview")
            .field("bytes", &self.text.len())
            .field("truncated", &self.truncated)
            .finish()
    }
}

/// A list row intentionally contains neither properties nor body bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageMetadataRow {
    pub identity: MessageIdentity,
    pub tags: Option<MessageTextPreview>,
    pub keys: Option<MessageTextPreview>,
    pub born_host: MessageTextPreview,
    pub store_host: MessageTextPreview,
    pub born_timestamp: i64,
    pub store_timestamp: i64,
    pub queue_id: i32,
    pub queue_offset: i64,
    pub store_size: i32,
    pub reconsume_times: i32,
    pub sys_flag: i32,
    pub flag: i32,
    pub prepared_transaction_offset: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessagePageExtent {
    End,
    MoreMayExist,
    KeyLimitReached,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageSearchPage {
    pub rows: Vec<MessageMetadataRow>,
    pub extent: MessagePageExtent,
}

/// One detail's sensitive cache. It is never serializable or printed by Debug.
pub struct MessagePayload {
    pub identity: MessageIdentity,
    body: Vec<u8>,
    properties: BTreeMap<String, String>,
    pub original_body_bytes: usize,
    pub body_truncated: bool,
    pub properties_truncated: bool,
}

impl MessagePayload {
    pub fn bounded(identity: MessageIdentity, mut body: Vec<u8>, properties: BTreeMap<String, String>) -> Self {
        let original_body_bytes = body.len();
        body.truncate(MESSAGE_BODY_MAX_BYTES);
        if original_body_bytes > MESSAGE_BODY_MAX_BYTES {
            body.shrink_to_fit();
        }
        let mut retained = BTreeMap::new();
        let mut size = 0usize;
        let mut properties_truncated = false;
        for (key, value) in properties {
            let next = size.saturating_add(key.len()).saturating_add(value.len());
            if next > MESSAGE_PROPERTIES_MAX_BYTES || retained.len() >= 128 {
                properties_truncated = true;
                continue;
            }
            size = next;
            retained.insert(key, value);
        }
        Self {
            identity,
            body,
            properties: retained,
            original_body_bytes,
            body_truncated: original_body_bytes > MESSAGE_BODY_MAX_BYTES,
            properties_truncated,
        }
    }
    pub fn body(&self) -> &[u8] {
        &self.body
    }
    pub fn properties(&self) -> &BTreeMap<String, String> {
        &self.properties
    }
}

impl fmt::Debug for MessagePayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MessagePayload")
            .field("body_bytes", &self.body.len())
            .field("property_count", &self.properties.len())
            .finish_non_exhaustive()
    }
}

impl Drop for MessagePayload {
    fn drop(&mut self) {
        self.body.clear();
        self.properties.clear();
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageConsumeCommand {
    pub identity: MessageIdentity,
    pub consumer_group: String,
    pub client_id: Option<String>,
    pub scope: ConnectionScope,
}

impl MessageConsumeCommand {
    pub fn validate(&self) -> Result<(), MessageValidationError> {
        TopicIdentity::parse(&self.identity.topic).map_err(|_| MessageValidationError::Topic)?;
        if !valid_identity(&self.identity.message_id) {
            return Err(MessageValidationError::MessageId);
        }
        if !valid_identity(&self.consumer_group) {
            return Err(MessageValidationError::ConsumerGroup);
        }
        if self.client_id.as_ref().is_some_and(|id| !valid_identity(id)) {
            return Err(MessageValidationError::ClientId);
        }
        if self.scope != ConnectionScope::NameServer {
            return Err(MessageValidationError::Scope);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageConsumeCode {
    Success,
    Later,
    Rollback,
    Commit,
    ThrewException,
    ReturnedNull,
    Unknown,
}

impl MessageConsumeCode {
    pub fn from_reported(value: Option<&str>) -> Self {
        match value {
            Some("CR_SUCCESS") => Self::Success,
            Some("CR_LATER") => Self::Later,
            Some("CR_ROLLBACK") => Self::Rollback,
            Some("CR_COMMIT") => Self::Commit,
            Some("CR_THROW_EXCEPTION") => Self::ThrewException,
            Some("CR_RETURN_NULL") => Self::ReturnedNull,
            _ => Self::Unknown,
        }
    }
    pub const fn label(self) -> &'static str {
        match self {
            Self::Success => "CR_SUCCESS",
            Self::Later => "CR_LATER",
            Self::Rollback => "CR_ROLLBACK",
            Self::Commit => "CR_COMMIT",
            Self::ThrewException => "CR_THROW_EXCEPTION",
            Self::ReturnedNull => "CR_RETURN_NULL",
            Self::Unknown => "UNKNOWN",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MessageConsumeReceipt {
    pub code: MessageConsumeCode,
    pub reported_success: bool,
    pub remark_returned: bool,
}
impl MessageConsumeReceipt {
    pub const fn is_success(self) -> bool {
        self.reported_success && matches!(self.code, MessageConsumeCode::Success)
    }
    pub const fn remark(self) -> &'static str {
        if self.remark_returned {
            "The Consumer returned a remark; its private content is not displayed."
        } else {
            "No remark returned."
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum MessageValidationError {
    #[error("A valid Topic is required.")]
    Topic,
    #[error("A non-empty Key is required (up to 1,024 bytes).")]
    Key,
    #[error("A non-empty Message ID is required (up to 1,024 bytes).")]
    MessageId,
    #[error("Begin and end must form an ordered UTC time range on or after 1970-01-01.")]
    TimeRange,
    #[error("Range pages start at 1 with 1–50 rows per page.")]
    Pagination,
    #[error("A valid Consumer group is required.")]
    ConsumerGroup,
    #[error("Client ID must be empty or a valid identifier.")]
    ClientId,
    #[error("Message operations require NameServer Direct scope; Proxy forwarding is unavailable.")]
    Scope,
}

impl MessageValidationError {
    pub const fn summary(self) -> &'static str {
        match self {
            Self::Topic => "A valid Topic is required.",
            Self::Key => "A non-empty Key is required (up to 1,024 bytes).",
            Self::MessageId => "A non-empty Message ID is required (up to 1,024 bytes).",
            Self::TimeRange => "Begin and end must form an ordered UTC time range on or after 1970-01-01.",
            Self::Pagination => "Range pages start at 1 with 1–50 rows per page.",
            Self::ConsumerGroup => "A valid Consumer group is required.",
            Self::ClientId => "Client ID must be empty or a valid identifier.",
            Self::Scope => "Message operations require NameServer Direct scope; Proxy forwarding is unavailable.",
        }
    }
}

fn valid_identity(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 1_024 && !value.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn identity() -> MessageIdentity {
        MessageIdentity {
            topic: "orders".into(),
            message_id: "private-id".into(),
        }
    }

    #[test]
    fn message_modes_require_topic_and_preserve_range_when_paging() {
        for query in [
            MessageSearchQuery::Key {
                topic: String::new(),
                key: "key".into(),
            },
            MessageSearchQuery::MessageId {
                topic: String::new(),
                message_id: "id".into(),
            },
        ] {
            assert_eq!(query.validate(), Err(MessageValidationError::Topic));
        }
        let first = MessageSearchQuery::TopicRange {
            topic: "orders".into(),
            begin: 123,
            end: 456,
            page: 1,
            page_size: 25,
        };
        assert_eq!(
            first.with_page(2).unwrap(),
            MessageSearchQuery::TopicRange {
                topic: "orders".into(),
                begin: 123,
                end: 456,
                page: 2,
                page_size: 25
            }
        );
        assert!(first.with_page(0).is_err());
        assert!(first.with_page(101).is_ok());
        assert!(MessageSearchQuery::TopicRange {
            topic: "orders".into(),
            begin: 456,
            end: 123,
            page: 1,
            page_size: 25
        }
        .validate()
        .is_err());
    }

    #[test]
    fn message_payload_is_bounded_and_debug_never_contains_body_or_properties() {
        let body = vec![b'x'; MESSAGE_BODY_MAX_BYTES + 9];
        let payload = MessagePayload::bounded(
            identity(),
            body,
            BTreeMap::from([
                ("private-key".into(), "private-value".into()),
                ("too-large".into(), "z".repeat(MESSAGE_PROPERTIES_MAX_BYTES + 1)),
            ]),
        );
        assert_eq!(payload.body().len(), MESSAGE_BODY_MAX_BYTES);
        assert_eq!(payload.original_body_bytes, MESSAGE_BODY_MAX_BYTES + 9);
        assert!(payload.body_truncated && payload.properties_truncated);
        assert_eq!(payload.properties().get("private-key").unwrap(), "private-value");
        let debug = format!("{payload:?}");
        for secret in ["private-key", "private-value", "private-id", "orders", "xxxx"] {
            assert!(!debug.contains(secret));
        }
    }

    #[test]
    fn message_direct_consume_only_confirms_success_for_reported_cr_success() {
        for reported in [
            "CR_LATER",
            "CR_ROLLBACK",
            "CR_COMMIT",
            "CR_THROW_EXCEPTION",
            "CR_RETURN_NULL",
            "private-unknown-result",
        ] {
            let receipt = MessageConsumeReceipt {
                code: MessageConsumeCode::from_reported(Some(reported)),
                reported_success: true,
                remark_returned: true,
            };
            assert!(!receipt.is_success());
            assert!(!receipt.remark().contains(reported));
        }
        let receipt = MessageConsumeReceipt {
            code: MessageConsumeCode::Success,
            reported_success: true,
            remark_returned: false,
        };
        assert!(receipt.is_success());
        assert!(!MessageConsumeReceipt {
            reported_success: false,
            ..receipt
        }
        .is_success());
    }
}
