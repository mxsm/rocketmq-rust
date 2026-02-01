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

use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::net::SocketAddr;

use bytes::Bytes;
use cheetah_string::CheetahString;

use crate::common::message::message_client_id_setter::MessageClientIDSetter;
use crate::common::message::message_single::Message;
use crate::common::message::routing_context::RoutingContext;
use crate::common::message::storage_metadata::StorageMetadata;

/// Complete message envelope (stored message)
///
/// Represents a message that has been received and stored by the Broker, containing complete
/// lifecycle information.
///
/// # Use Cases
///
/// - Consumer receiving messages
/// - Message query and tracking
/// - Message passing between Broker internal modules
#[derive(Clone, Debug)]
pub struct MessageEnvelope {
    /// Message content
    message: Message,

    /// Routing context (network layer)
    routing: RoutingContext,

    /// Storage metadata (persistence layer)
    storage: StorageMetadata,

    /// Message ID (calculated from store_host + commit_log_offset)
    msg_id: CheetahString,

    /// Message body CRC checksum
    body_crc: u32,

    /// Reconsume times
    reconsume_times: i32,

    /// Prepared transaction offset (only valid for transaction messages)
    prepared_transaction_offset: i64,
}

impl MessageEnvelope {
    /// Creates a new message envelope
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        message: Message,
        routing: RoutingContext,
        storage: StorageMetadata,
        msg_id: CheetahString,
        body_crc: u32,
        reconsume_times: i32,
        prepared_transaction_offset: i64,
    ) -> Self {
        Self {
            message,
            routing,
            storage,
            msg_id,
            body_crc,
            reconsume_times,
            prepared_transaction_offset,
        }
    }

    // ===== Message Access =====

    /// Gets the message reference
    #[inline]
    pub fn message(&self) -> &Message {
        &self.message
    }

    /// Gets the mutable message reference
    #[inline]
    pub fn message_mut(&mut self) -> &mut Message {
        &mut self.message
    }

    /// Gets the message topic
    #[inline]
    pub fn topic(&self) -> &CheetahString {
        self.message.topic()
    }

    /// Gets the message body
    #[inline]
    pub fn body(&self) -> Option<Bytes> {
        self.message.body()
    }

    /// Gets the message properties
    #[inline]
    pub fn properties(&self) -> &HashMap<CheetahString, CheetahString> {
        self.message.properties().as_map()
    }

    /// Gets the message tags
    #[inline]
    pub fn tags(&self) -> Option<CheetahString> {
        self.message.get_tags()
    }

    /// Gets the message keys
    #[inline]
    pub fn keys(&self) -> Option<Vec<String>> {
        self.message.keys()
    }

    /// Gets the message flag
    #[inline]
    pub fn flag(&self) -> i32 {
        self.message.flag()
    }

    // ===== Routing Access =====

    /// Gets the routing context
    #[inline]
    pub fn routing(&self) -> &RoutingContext {
        &self.routing
    }

    /// Gets the mutable routing context (internal use)
    #[inline]
    pub(crate) fn routing_mut(&mut self) -> &mut RoutingContext {
        &mut self.routing
    }

    /// Gets the message sender's client address
    #[inline]
    pub fn born_host(&self) -> SocketAddr {
        self.routing.born_host()
    }

    /// Gets the message creation timestamp
    #[inline]
    pub fn born_timestamp(&self) -> i64 {
        self.routing.born_timestamp()
    }

    /// Gets the system flag bits
    #[inline]
    pub fn sys_flag(&self) -> i32 {
        self.routing.sys_flag()
    }

    /// Gets the born host bytes
    #[inline]
    pub fn born_host_bytes(&self) -> Bytes {
        self.routing.born_host_bytes()
    }

    // ===== Storage Access =====

    /// Gets the storage metadata
    #[inline]
    pub fn storage(&self) -> &StorageMetadata {
        &self.storage
    }

    /// Gets the mutable storage metadata (internal use)
    #[inline]
    pub(crate) fn storage_mut(&mut self) -> &mut StorageMetadata {
        &mut self.storage
    }

    /// Gets the broker name
    #[inline]
    pub fn broker_name(&self) -> &str {
        self.storage.broker_name()
    }

    /// Gets the queue ID
    #[inline]
    pub fn queue_id(&self) -> i32 {
        self.storage.queue_id()
    }

    /// Gets the queue logical offset
    #[inline]
    pub fn queue_offset(&self) -> i64 {
        self.storage.queue_offset()
    }

    /// Gets the CommitLog physical offset
    #[inline]
    pub fn commit_log_offset(&self) -> i64 {
        self.storage.commit_log_offset()
    }

    /// Gets the storage timestamp
    #[inline]
    pub fn store_timestamp(&self) -> i64 {
        self.storage.store_timestamp()
    }

    /// Gets the storage host address
    #[inline]
    pub fn store_host(&self) -> SocketAddr {
        self.storage.store_host()
    }

    /// Gets the storage size in bytes
    #[inline]
    pub fn store_size(&self) -> i32 {
        self.storage.store_size()
    }

    /// Gets the store host bytes
    #[inline]
    pub fn store_host_bytes(&self) -> Bytes {
        self.storage.store_host_bytes()
    }

    // ===== Other Metadata Access =====

    /// Gets the message ID (offset-based)
    #[inline]
    pub fn msg_id(&self) -> &CheetahString {
        &self.msg_id
    }

    /// Gets the client message ID (UNIQ_ID preferred, otherwise returns offset msg_id)
    ///
    /// This is the client-side message ID, preferring UNIQ_ID (globally unique)
    pub fn client_msg_id(&self) -> CheetahString {
        MessageClientIDSetter::get_uniq_id(self).unwrap_or_else(|| self.msg_id.clone())
    }

    /// Gets the message body CRC
    #[inline]
    pub fn body_crc(&self) -> u32 {
        self.body_crc
    }

    /// Gets the reconsume times
    #[inline]
    pub fn reconsume_times(&self) -> i32 {
        self.reconsume_times
    }

    /// Gets the prepared transaction offset
    #[inline]
    pub fn prepared_transaction_offset(&self) -> i64 {
        self.prepared_transaction_offset
    }

    // ===== Modification Methods (Internal Use) =====

    /// Sets the message ID
    #[inline]
    pub(crate) fn set_msg_id(&mut self, msg_id: CheetahString) {
        self.msg_id = msg_id;
    }

    /// Sets the reconsume times
    #[inline]
    pub(crate) fn set_reconsume_times(&mut self, reconsume_times: i32) {
        self.reconsume_times = reconsume_times;
    }

    /// Sets the body CRC
    #[inline]
    pub(crate) fn set_body_crc(&mut self, body_crc: u32) {
        self.body_crc = body_crc;
    }

    /// Sets the prepared transaction offset
    #[inline]
    pub(crate) fn set_prepared_transaction_offset(&mut self, offset: i64) {
        self.prepared_transaction_offset = offset;
    }

    /// Sets the born host v6 flag
    #[inline]
    pub(crate) fn with_born_host_v6_flag(&mut self) {
        self.routing.with_born_host_v6_flag();
    }

    /// Sets the store host v6 flag
    #[inline]
    pub(crate) fn with_store_host_v6_flag(&mut self) {
        let mut sys_flag = self.sys_flag();
        self.storage.with_store_host_v6_flag(&mut sys_flag);
    }
}

impl Default for MessageEnvelope {
    fn default() -> Self {
        Self {
            message: Message::default(),
            routing: RoutingContext::default(),
            storage: StorageMetadata::default(),
            msg_id: CheetahString::new(),
            body_crc: 0,
            reconsume_times: 0,
            prepared_transaction_offset: 0,
        }
    }
}

impl Display for MessageEnvelope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MessageEnvelope {{ topic: {}, queue_id: {}, queue_offset: {}, commit_log_offset: {}, msg_id: {}, \
             reconsume_times: {} }}",
            self.topic(),
            self.queue_id(),
            self.queue_offset(),
            self.commit_log_offset(),
            self.msg_id,
            self.reconsume_times
        )
    }
}

// Implement MessageTrait for MessageEnvelope
impl crate::common::message::MessageTrait for MessageEnvelope {
    fn put_property(&mut self, key: CheetahString, value: CheetahString) {
        self.message.put_property(key, value);
    }

    fn clear_property(&mut self, name: &str) {
        self.message.clear_property(name);
    }

    fn property(&self, name: &CheetahString) -> Option<CheetahString> {
        self.message.get_property(name)
    }

    fn property_ref(&self, name: &CheetahString) -> Option<&CheetahString> {
        self.message.property_ref(name)
    }

    fn get_topic(&self) -> &CheetahString {
        self.topic()
    }

    fn set_topic(&mut self, topic: CheetahString) {
        self.message.set_topic(topic);
    }

    fn get_flag(&self) -> i32 {
        self.flag()
    }

    fn set_flag(&mut self, flag: i32) {
        self.message.set_flag(flag);
    }

    fn get_body(&self) -> Option<&Bytes> {
        self.message.get_body()
    }

    fn set_body(&mut self, body: Bytes) {
        self.message.set_body(Some(body));
    }

    fn get_properties(&self) -> &HashMap<CheetahString, CheetahString> {
        self.properties()
    }

    fn set_properties(&mut self, properties: HashMap<CheetahString, CheetahString>) {
        self.message.set_properties(properties);
    }

    fn get_transaction_id(&self) -> Option<&CheetahString> {
        self.message.get_transaction_id()
    }

    fn set_transaction_id(&mut self, transaction_id: CheetahString) {
        self.message.set_transaction_id(transaction_id);
    }

    fn get_compressed_body_mut(&mut self) -> Option<&mut Bytes> {
        self.message.get_compressed_body_mut()
    }

    fn get_compressed_body(&self) -> Option<&Bytes> {
        self.message.get_compressed_body()
    }

    fn set_compressed_body_mut(&mut self, compressed_body: Bytes) {
        self.message.set_compressed_body_mut(compressed_body);
    }

    fn take_body(&mut self) -> Option<Bytes> {
        self.message.take_body()
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

    fn create_test_envelope() -> MessageEnvelope {
        let message = Message::default();
        let routing = RoutingContext::default();
        let storage = StorageMetadata::default();

        MessageEnvelope::new(
            message,
            routing,
            storage,
            CheetahString::from_static_str("test-msg-id"),
            12345,
            0,
            0,
        )
    }

    #[test]
    fn test_envelope_creation() {
        let envelope = create_test_envelope();
        assert_eq!(envelope.msg_id().as_str(), "test-msg-id");
        assert_eq!(envelope.body_crc(), 12345);
        assert_eq!(envelope.reconsume_times(), 0);
    }

    #[test]
    fn test_message_access() {
        let envelope = create_test_envelope();
        let _message = envelope.message();
        let _topic = envelope.topic();
        let _body = envelope.body();
        let _properties = envelope.properties();
    }

    #[test]
    fn test_routing_access() {
        let envelope = create_test_envelope();
        let _born_host = envelope.born_host();
        let _born_timestamp = envelope.born_timestamp();
        let _sys_flag = envelope.sys_flag();
    }

    #[test]
    fn test_storage_access() {
        let envelope = create_test_envelope();
        let _broker_name = envelope.broker_name();
        let _queue_id = envelope.queue_id();
        let _queue_offset = envelope.queue_offset();
        let _commit_log_offset = envelope.commit_log_offset();
    }

    #[test]
    fn test_setters() {
        let mut envelope = create_test_envelope();

        envelope.set_msg_id(CheetahString::from_static_str("new-msg-id"));
        envelope.set_reconsume_times(5);
        envelope.set_body_crc(99999);
        envelope.set_prepared_transaction_offset(1000);

        assert_eq!(envelope.msg_id().as_str(), "new-msg-id");
        assert_eq!(envelope.reconsume_times(), 5);
        assert_eq!(envelope.body_crc(), 99999);
        assert_eq!(envelope.prepared_transaction_offset(), 1000);
    }

    #[test]
    fn test_display() {
        let envelope = create_test_envelope();
        let display_str = format!("{}", envelope);
        assert!(display_str.contains("MessageEnvelope"));
        assert!(display_str.contains("test-msg-id"));
    }
}
