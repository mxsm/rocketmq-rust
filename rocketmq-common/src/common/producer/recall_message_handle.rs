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

use std::fmt;

use base64::engine::general_purpose::URL_SAFE;
use base64::Engine;
use cheetah_string::CheetahString;

use crate::RocketMQError;
use crate::RocketMQResult;

const SEPARATOR: &str = " ";
const VERSION_1: &str = "v1";

/// Handle to recall a message, currently only supports delay messages.
///
/// The v1 pattern encodes the following fields in Base64:
/// version topic brokerName timestamp messageId
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecallMessageHandle {
    V1(HandleV1),
}

impl RecallMessageHandle {
    /// Decode a recall handle string into a RecallMessageHandle.
    ///
    /// # Arguments
    ///
    /// * `handle` - Base64 encoded handle string
    ///
    /// # Returns
    ///
    /// * `Ok(RecallMessageHandle)` - Successfully decoded handle
    /// * `Err(RocketMQError)` - Error if decoding fails
    ///
    /// # Examples
    ///
    /// ```
    /// use rocketmq_common::common::producer::recall_message_handle::RecallMessageHandle;
    ///
    /// let handle_str = "djEgdG9waWMgYnJva2VyLTAgMTcwNzExMTExMTExMSBtc2dJZA==";
    /// let handle = RecallMessageHandle::decode_handle(handle_str);
    /// assert!(handle.is_ok());
    /// ```
    pub fn decode_handle(handle: &str) -> RocketMQResult<Self> {
        if handle.is_empty() {
            return Err(RocketMQError::deserialization_failed("RecallHandle", "handle is empty"));
        }

        let raw_bytes = URL_SAFE
            .decode(handle.as_bytes())
            .map_err(|_| RocketMQError::deserialization_failed("RecallHandle", "invalid base64 encoding"))?;

        let raw_string = String::from_utf8(raw_bytes)
            .map_err(|_| RocketMQError::deserialization_failed("RecallHandle", "invalid UTF-8 encoding"))?;

        let items: Vec<&str> = raw_string.split(SEPARATOR).collect();

        if items.is_empty() || items[0] != VERSION_1 || items.len() < 5 {
            return Err(RocketMQError::deserialization_failed(
                "RecallHandle",
                format!(
                    "invalid format: expected 'v1 topic broker timestamp msgid', got {} parts",
                    items.len()
                ),
            ));
        }

        Ok(RecallMessageHandle::V1(HandleV1::new(
            CheetahString::from_slice(items[1]),
            CheetahString::from_slice(items[2]),
            CheetahString::from_slice(items[3]),
            CheetahString::from_slice(items[4]),
        )))
    }

    /// Get the topic from the handle.
    pub fn topic(&self) -> &str {
        match self {
            RecallMessageHandle::V1(v1) => &v1.topic,
        }
    }

    /// Get the broker name from the handle.
    pub fn broker_name(&self) -> &str {
        match self {
            RecallMessageHandle::V1(v1) => &v1.broker_name,
        }
    }

    /// Get the timestamp string from the handle.
    pub fn timestamp_str(&self) -> &str {
        match self {
            RecallMessageHandle::V1(v1) => &v1.timestamp_str,
        }
    }

    /// Get the message ID from the handle.
    pub fn message_id(&self) -> &str {
        match self {
            RecallMessageHandle::V1(v1) => &v1.message_id,
        }
    }

    /// Get the version string from the handle.
    pub fn version(&self) -> &str {
        match self {
            RecallMessageHandle::V1(_) => VERSION_1,
        }
    }
}

impl fmt::Display for RecallMessageHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecallMessageHandle::V1(v1) => write!(
                f,
                "HandleV1 {{ version: {}, topic: {}, broker_name: {}, timestamp_str: {}, message_id: {} }}",
                VERSION_1, v1.topic, v1.broker_name, v1.timestamp_str, v1.message_id
            ),
        }
    }
}

/// Version 1 of the recall message handle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandleV1 {
    topic: CheetahString,
    broker_name: CheetahString,
    timestamp_str: CheetahString,
    message_id: CheetahString,
}

impl HandleV1 {
    /// Create a new HandleV1 instance.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic name
    /// * `broker_name` - The broker name
    /// * `timestamp_str` - The timestamp as a string
    /// * `message_id` - The message ID (unique key)
    pub fn new(
        topic: impl Into<CheetahString>,
        broker_name: impl Into<CheetahString>,
        timestamp_str: impl Into<CheetahString>,
        message_id: impl Into<CheetahString>,
    ) -> Self {
        Self {
            topic: topic.into(),
            broker_name: broker_name.into(),
            timestamp_str: timestamp_str.into(),
            message_id: message_id.into(),
        }
    }

    /// Build a Base64 encoded handle string from components.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic name
    /// * `broker_name` - The broker name
    /// * `timestamp_str` - The timestamp as a string
    /// * `message_id` - The message ID
    ///
    /// # Returns
    ///
    /// Base64 encoded handle string
    ///
    /// # Examples
    ///
    /// ```
    /// use rocketmq_common::common::producer::recall_message_handle::HandleV1;
    ///
    /// let handle = HandleV1::build_handle("test_topic", "broker-0", "1707111111111", "msgId123");
    /// assert!(!handle.is_empty());
    /// ```
    pub fn build_handle(
        topic: impl Into<CheetahString>,
        broker_name: impl Into<CheetahString>,
        timestamp_str: impl Into<CheetahString>,
        message_id: impl Into<CheetahString>,
    ) -> String {
        let topic = topic.into();
        let broker_name = broker_name.into();
        let timestamp_str = timestamp_str.into();
        let message_id = message_id.into();

        let raw_string = format!(
            "{}{}{}{}{}{}{}{}{}",
            VERSION_1, SEPARATOR, topic, SEPARATOR, broker_name, SEPARATOR, timestamp_str, SEPARATOR, message_id
        );
        URL_SAFE.encode(raw_string.as_bytes())
    }

    /// Get the topic.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Get the broker name.
    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }

    /// Get the timestamp string.
    pub fn timestamp_str(&self) -> &str {
        &self.timestamp_str
    }

    /// Get the message ID.
    pub fn message_id(&self) -> &str {
        &self.message_id
    }

    /// Get the version.
    pub fn version(&self) -> &str {
        VERSION_1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handle_invalid_empty() {
        let result = RecallMessageHandle::decode_handle("");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn test_handle_invalid_not_base64() {
        let result = RecallMessageHandle::decode_handle("invalid base64!");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("base64") || err.to_string().contains("Decoding"));
    }

    #[test]
    fn test_handle_invalid_version() {
        let invalid_handle = URL_SAFE.encode("v2 a b c d");
        let result = RecallMessageHandle::decode_handle(&invalid_handle);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("invalid format") || err.to_string().contains("Decoding"));
    }

    #[test]
    fn test_handle_invalid_too_few_parts() {
        let invalid_handle = URL_SAFE.encode("v1 a b c");
        let result = RecallMessageHandle::decode_handle(&invalid_handle);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("4 parts") || err.to_string().contains("invalid format"));
    }

    #[test]
    fn test_encode_and_decode_v1() {
        let topic = "test_topic";
        let broker_name = "broker-0";
        let timestamp_str = "1707111111111";
        let message_id = "msgId123";

        let handle = HandleV1::build_handle(topic, broker_name, timestamp_str, message_id);
        assert!(!handle.is_empty());

        let decoded = RecallMessageHandle::decode_handle(&handle).unwrap();

        assert_eq!(decoded.version(), VERSION_1);
        assert_eq!(decoded.topic(), topic);
        assert_eq!(decoded.broker_name(), broker_name);
        assert_eq!(decoded.timestamp_str(), timestamp_str);
        assert_eq!(decoded.message_id(), message_id);

        let RecallMessageHandle::V1(v1) = decoded;
        assert_eq!(v1.topic(), topic);
        assert_eq!(v1.broker_name(), broker_name);
        assert_eq!(v1.timestamp_str(), timestamp_str);
        assert_eq!(v1.message_id(), message_id);
        assert_eq!(v1.version(), VERSION_1);
    }

    #[test]
    fn test_handle_v1_new() {
        let topic = "test_topic";
        let broker_name = "broker-0";
        let timestamp_str = "1707111111111";
        let message_id = "msgId123";

        let handle = HandleV1::new(topic, broker_name, timestamp_str, message_id);

        assert_eq!(handle.topic(), topic);
        assert_eq!(handle.broker_name(), broker_name);
        assert_eq!(handle.timestamp_str(), timestamp_str);
        assert_eq!(handle.message_id(), message_id);
        assert_eq!(handle.version(), VERSION_1);
    }

    #[test]
    fn test_round_trip() {
        let topic = "my_topic";
        let broker = "broker-1";
        let timestamp = "1707222222222";
        let msg_id = "uniqueMsgId";

        let encoded = HandleV1::build_handle(topic, broker, timestamp, msg_id);
        let decoded = RecallMessageHandle::decode_handle(&encoded).unwrap();

        assert_eq!(decoded.topic(), topic);
        assert_eq!(decoded.broker_name(), broker);
        assert_eq!(decoded.timestamp_str(), timestamp);
        assert_eq!(decoded.message_id(), msg_id);
    }

    #[test]
    fn test_display_format() {
        let handle = HandleV1::new("topic", "broker", "123456", "msgId");
        let recall_handle = RecallMessageHandle::V1(handle);
        let display = format!("{}", recall_handle);
        assert!(display.contains("HandleV1"));
        assert!(display.contains("topic"));
        assert!(display.contains("broker"));
        assert!(display.contains("123456"));
        assert!(display.contains("msgId"));
    }

    #[test]
    fn test_clone_and_equality() {
        let handle1 = HandleV1::new("topic", "broker", "123456", "msgId");
        let handle2 = handle1.clone();
        assert_eq!(handle1, handle2);

        let recall1 = RecallMessageHandle::V1(handle1);
        let recall2 = recall1.clone();
        assert_eq!(recall1, recall2);
    }
}
