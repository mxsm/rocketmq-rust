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
use std::fmt::Debug;
use std::fmt::Display;

use bytes::BytesMut;
use cheetah_string::CheetahString;

use crate::common::hasher::string_hasher::JavaStringHasher;
use crate::common::message::message_envelope::MessageEnvelope;
use crate::common::message::MessageTrait;
use crate::common::message::MessageVersion;
use crate::common::mix_all;
use crate::MessageUtils;

/// Broker internal message representation
///
/// Contains extension information needed by the Broker when processing messages, such as
/// tags_code, encoding cache, etc. Only used internally by the Broker, not exposed to clients.
///
/// # Use Cases
///
/// - Broker receives messages and constructs BrokerMessage for writing to CommitLog
/// - Store module internal message passing
/// - Message filtering and index building
pub struct BrokerMessage {
    /// Message envelope (complete message + metadata)
    envelope: MessageEnvelope,

    /// Tags hash code (used for message filtering)
    tags_code: i64,

    /// Serialized properties string (performance optimization to avoid repeated serialization)
    properties_string: CheetahString,

    /// Message version
    version: MessageVersion,

    /// Encoding cache (avoids repeated serialization)
    encoded_buff: Option<BytesMut>,

    /// Whether encoding is completed
    encode_completed: bool,
}

impl BrokerMessage {
    /// Creates a new broker message
    pub fn new(
        envelope: MessageEnvelope,
        tags_code: i64,
        properties_string: CheetahString,
        version: MessageVersion,
    ) -> Self {
        Self {
            envelope,
            tags_code,
            properties_string,
            version,
            encoded_buff: None,
            encode_completed: false,
        }
    }

    /// Creates from message envelope (automatically calculates tags_code and properties_string)
    pub fn from_envelope(envelope: MessageEnvelope) -> Self {
        let tags_code = envelope
            .tags()
            .map(|tags| Self::calculate_tags_code(&tags))
            .unwrap_or(0);

        let properties_string = Self::serialize_properties(envelope.properties());

        Self {
            envelope,
            tags_code,
            properties_string,
            version: MessageVersion::V1,
            encoded_buff: None,
            encode_completed: false,
        }
    }

    // ===== Envelope Access =====

    /// Gets the message envelope
    #[inline]
    pub fn envelope(&self) -> &MessageEnvelope {
        &self.envelope
    }

    /// Gets the mutable message envelope
    #[inline]
    pub fn envelope_mut(&mut self) -> &mut MessageEnvelope {
        &mut self.envelope
    }

    /// Consumes self and returns the envelope
    #[inline]
    pub fn into_envelope(self) -> MessageEnvelope {
        self.envelope
    }

    // ===== Broker-Specific Field Access =====

    /// Gets the tags hash code
    #[inline]
    pub fn tags_code(&self) -> i64 {
        self.tags_code
    }

    /// Gets the serialized properties string
    #[inline]
    pub fn properties_string(&self) -> &str {
        &self.properties_string
    }

    /// Gets the message version
    #[inline]
    pub fn version(&self) -> MessageVersion {
        self.version
    }

    /// Gets the encoding cache
    #[inline]
    pub fn encoded_buff(&self) -> Option<&BytesMut> {
        self.encoded_buff.as_ref()
    }

    /// Gets the mutable encoding cache
    #[inline]
    pub fn encoded_buff_mut(&mut self) -> Option<&mut BytesMut> {
        self.encoded_buff.as_mut()
    }

    /// Checks if encoding is completed
    #[inline]
    pub fn is_encode_completed(&self) -> bool {
        self.encode_completed
    }

    // ===== Modification Methods =====

    /// Sets the encoding cache
    pub fn set_encoded_buff(&mut self, buff: BytesMut) {
        self.encoded_buff = Some(buff);
        self.encode_completed = true;
    }

    /// Takes the encoding cache
    pub fn take_encoded_buff(&mut self) -> Option<BytesMut> {
        self.encode_completed = false;
        self.encoded_buff.take()
    }

    /// Clears the encoding cache
    pub fn clear_encoded_buff(&mut self) {
        self.encoded_buff = None;
        self.encode_completed = false;
    }

    /// Sets the message version
    pub fn set_version(&mut self, version: MessageVersion) {
        self.version = version;
    }

    /// Sets the tags code
    pub fn set_tags_code(&mut self, tags_code: i64) {
        self.tags_code = tags_code;
    }

    /// Sets the properties string
    pub fn set_properties_string(&mut self, properties_string: CheetahString) {
        self.properties_string = properties_string;
    }

    // ===== Utility Methods =====

    /// Calculates the hash code for tags
    pub fn calculate_tags_code(tags: &str) -> i64 {
        if tags.is_empty() {
            return 0;
        }
        JavaStringHasher::hash_str(tags) as i64
    }

    /// Serializes properties to string
    fn serialize_properties(properties: &std::collections::HashMap<CheetahString, CheetahString>) -> CheetahString {
        mix_all::properties_to_string(properties)
    }

    /// Deletes a property (synchronizes properties_string)
    pub fn delete_property(&mut self, name: &str) {
        self.envelope.message_mut().clear_property(name);

        self.properties_string =
            CheetahString::from_string(MessageUtils::delete_property(&self.properties_string, name));
    }

    /// Gets a property value
    pub fn property(&self, name: &str) -> Option<CheetahString> {
        self.envelope
            .properties()
            .get(&CheetahString::from_string(name.to_string()))
            .cloned()
    }
}

impl Debug for BrokerMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrokerMessage")
            .field("envelope", &self.envelope)
            .field("tags_code", &self.tags_code)
            .field("properties_string", &self.properties_string)
            .field("version", &self.version)
            .field("encoded_buff", &self.encoded_buff.as_ref().map(|_| "****"))
            .field("encode_completed", &self.encode_completed)
            .finish()
    }
}

impl Display for BrokerMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BrokerMessage {{ envelope: {}, tags_code: {}, version: {} }}",
            self.envelope, self.tags_code, self.version
        )
    }
}

impl Default for BrokerMessage {
    fn default() -> Self {
        Self {
            envelope: MessageEnvelope::default(),
            tags_code: 0,
            properties_string: CheetahString::new(),
            version: MessageVersion::V1,
            encoded_buff: None,
            encode_completed: false,
        }
    }
}

// Delegate common trait implementations to envelope
impl MessageTrait for BrokerMessage {
    fn put_property(&mut self, key: CheetahString, value: CheetahString) {
        self.envelope.message_mut().put_property(key, value);
        // Update properties_string
        self.properties_string = Self::serialize_properties(self.envelope.properties());
    }

    fn clear_property(&mut self, name: &str) {
        self.delete_property(name);
    }

    fn get_property(&self, name: &CheetahString) -> Option<CheetahString> {
        self.envelope.properties().get(name).cloned()
    }

    fn get_property_ref(&self, name: &CheetahString) -> Option<&CheetahString> {
        self.envelope.properties().get(name)
    }

    fn get_topic(&self) -> &CheetahString {
        self.envelope.topic()
    }

    fn set_topic(&mut self, topic: CheetahString) {
        self.envelope.message_mut().set_topic(topic);
    }

    fn get_flag(&self) -> i32 {
        self.envelope.flag()
    }

    fn set_flag(&mut self, flag: i32) {
        self.envelope.message_mut().set_flag(flag);
    }

    fn get_body(&self) -> Option<&bytes::Bytes> {
        self.envelope.message().get_body()
    }

    fn set_body(&mut self, body: bytes::Bytes) {
        self.envelope.message_mut().set_body(Some(body));
    }

    fn get_properties(&self) -> &std::collections::HashMap<CheetahString, CheetahString> {
        self.envelope.properties()
    }

    fn set_properties(&mut self, properties: std::collections::HashMap<CheetahString, CheetahString>) {
        self.envelope.message_mut().set_properties(properties.clone());
        self.properties_string = Self::serialize_properties(&properties);
    }

    fn get_transaction_id(&self) -> Option<&CheetahString> {
        self.envelope.message().get_transaction_id()
    }

    fn set_transaction_id(&mut self, transaction_id: CheetahString) {
        self.envelope.message_mut().set_transaction_id(transaction_id);
    }

    fn get_compressed_body_mut(&mut self) -> Option<&mut bytes::Bytes> {
        self.envelope.message_mut().get_compressed_body_mut()
    }

    fn get_compressed_body(&self) -> Option<&bytes::Bytes> {
        self.envelope.message().get_compressed_body()
    }

    fn set_compressed_body_mut(&mut self, compressed_body: bytes::Bytes) {
        self.envelope.message_mut().set_compressed_body_mut(compressed_body);
    }

    fn take_body(&mut self) -> Option<bytes::Bytes> {
        self.envelope.message_mut().take_body()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broker_message_creation() {
        let envelope = MessageEnvelope::default();
        let broker_msg = BrokerMessage::new(
            envelope,
            123,
            CheetahString::from_static_str("key=value"),
            MessageVersion::V1,
        );

        assert_eq!(broker_msg.tags_code(), 123);
        assert_eq!(broker_msg.properties_string(), "key=value");
        assert_eq!(broker_msg.version(), MessageVersion::V1);
        assert!(!broker_msg.is_encode_completed());
    }

    #[test]
    fn test_from_envelope() {
        let envelope = MessageEnvelope::default();
        let broker_msg = BrokerMessage::from_envelope(envelope);

        assert_eq!(broker_msg.tags_code(), 0);
        assert!(!broker_msg.is_encode_completed());
    }

    #[test]
    fn test_encoded_buff() {
        let mut broker_msg = BrokerMessage::default();
        assert!(broker_msg.encoded_buff().is_none());

        let buff = BytesMut::from(&b"test data"[..]);
        broker_msg.set_encoded_buff(buff);

        assert!(broker_msg.encoded_buff().is_some());
        assert!(broker_msg.is_encode_completed());

        broker_msg.clear_encoded_buff();
        assert!(broker_msg.encoded_buff().is_none());
        assert!(!broker_msg.is_encode_completed());
    }

    #[test]
    fn test_calculate_tags_code() {
        let code = BrokerMessage::calculate_tags_code("TagA");
        assert_ne!(code, 0);

        let empty_code = BrokerMessage::calculate_tags_code("");
        assert_eq!(empty_code, 0);
    }

    #[test]
    fn test_display() {
        let broker_msg = BrokerMessage::default();
        let display_str = format!("{}", broker_msg);
        assert!(display_str.contains("BrokerMessage"));
    }
}
