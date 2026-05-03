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

use std::any::Any;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::string::ToString;
use std::sync::LazyLock;

use bytes::Buf;
use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

pub mod message_accessor;
pub mod message_batch;
pub mod message_batch_v2;
pub mod message_body;
pub mod message_builder;
pub mod message_client_ext;
pub mod message_client_id_setter;
pub mod message_decoder;
pub mod message_enum;
pub mod message_ext;
pub mod message_ext_broker_inner;
pub mod message_flag;
pub mod message_id;
pub mod message_property;
pub mod message_queue;
pub mod message_queue_assignment;
pub mod message_queue_for_c;
pub mod message_single;

// New refactored message types
pub mod broker_message;
pub mod message_envelope;
pub mod routing_context;
pub mod storage_metadata;

/// Defines the interface for RocketMQ message operations.
///
/// Provides methods for managing message properties, keys, tags, body, and metadata.
pub trait MessageTrait: Any + Display + Debug {
    /// Sets the keys for the message.
    #[inline]
    fn set_keys(&mut self, keys: CheetahString) {
        self.put_property(CheetahString::from_static_str(MessageConst::PROPERTY_KEYS), keys);
    }

    /// Adds a property to the message.
    fn put_property(&mut self, key: CheetahString, value: CheetahString);

    /// Removes the specified property from the message.
    fn clear_property(&mut self, name: &str);

    /// Adds a user-defined property to the message.
    ///
    /// # Errors
    ///
    /// Returns an error if the property name is reserved by the system or if the name or value is
    /// empty.
    fn put_user_property(&mut self, name: CheetahString, value: CheetahString) -> RocketMQResult<()> {
        if name.is_empty() || value.is_empty() {
            return Err(RocketMQError::InvalidProperty(
                "The name or value of property can not be null or blank string!".to_string(),
            ));
        }
        if STRING_HASH_SET.contains(name.as_str()) {
            return Err(RocketMQError::InvalidProperty(format!(
                "The Property<{name}> is used by system, input another please"
            )));
        }
        self.put_property(name, value);
        Ok(())
    }

    /// Retrieves a user-defined property value.
    fn user_property(&self, name: &CheetahString) -> Option<CheetahString> {
        self.property(name)
    }

    /// Retrieves a reference to a user-defined property value.
    fn user_property_ref(&self, name: &CheetahString) -> Option<&CheetahString> {
        self.property_ref(name)
    }

    /// Retrieves a property value.
    fn property(&self, name: &CheetahString) -> Option<CheetahString>;

    /// Retrieves a reference to a property value.
    fn property_ref(&self, name: &CheetahString) -> Option<&CheetahString>;

    /// Returns the topic of the message.
    fn topic(&self) -> &CheetahString;

    /// Sets the topic for the message.
    fn set_topic(&mut self, topic: CheetahString);

    /// Returns the tags associated with the message.
    fn tags(&self) -> Option<CheetahString> {
        self.property(&CheetahString::from_static_str(MessageConst::PROPERTY_TAGS))
    }

    /// Returns a reference to the tags associated with the message.
    fn tags_ref(&self) -> Option<&CheetahString> {
        self.property_ref(&CheetahString::from_static_str(MessageConst::PROPERTY_TAGS))
    }

    /// Sets the tags for the message.
    fn set_tags(&mut self, tags: CheetahString) {
        self.put_property(CheetahString::from_static_str(MessageConst::PROPERTY_TAGS), tags);
    }

    /// Returns the keys associated with the message.
    fn get_keys(&self) -> Option<CheetahString> {
        self.property(&CheetahString::from_static_str(MessageConst::PROPERTY_KEYS))
    }
    /// Returns a reference to the keys associated with the message.
    fn get_keys_ref(&self) -> Option<&CheetahString> {
        self.property_ref(&CheetahString::from_static_str(MessageConst::PROPERTY_KEYS))
    }
    /// Sets the message keys from a collection, joining them with spaces.
    fn set_keys_from_collection(&mut self, key_collection: Vec<String>) {
        let keys = key_collection.join(MessageConst::KEY_SEPARATOR);
        self.set_keys(CheetahString::from_string(keys));
    }

    /// Returns the delay time level of the message, or 0 if not set.
    fn delay_time_level(&self) -> i32 {
        self.property(&CheetahString::from_static_str(MessageConst::PROPERTY_DELAY_TIME_LEVEL))
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    }

    /// Sets the delay time level for the message.
    fn set_delay_time_level(&mut self, level: i32) {
        self.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_DELAY_TIME_LEVEL),
            CheetahString::from_string(level.to_string()),
        );
    }

    /// Returns whether the message should wait for store acknowledgment.
    ///
    /// Defaults to `true` if not set.
    fn is_wait_store_msg_ok(&self) -> bool {
        self.property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_WAIT_STORE_MSG_OK,
        ))
        .map(|v| v.as_str() != "false")
        .unwrap_or(true)
    }

    /// Sets whether the message should wait for store acknowledgment.
    fn set_wait_store_msg_ok(&mut self, wait_store_msg_ok: bool) {
        self.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_WAIT_STORE_MSG_OK),
            CheetahString::from_string(wait_store_msg_ok.to_string()),
        );
    }

    /// Sets the instance ID for the message.
    fn set_instance_id(&mut self, instance_id: CheetahString) {
        self.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_INSTANCE_ID),
            instance_id,
        );
    }

    /// Returns the flag associated with the message.
    fn get_flag(&self) -> i32;

    /// Sets the flag for the message.
    fn set_flag(&mut self, flag: i32);

    /// Returns the body of the message.
    fn get_body(&self) -> Option<&Bytes>;

    /// Sets the body of the message.
    fn set_body(&mut self, body: Bytes);

    /// Returns all properties associated with the message.
    fn get_properties(&self) -> &HashMap<CheetahString, CheetahString>;

    /// Sets multiple properties for the message.
    fn set_properties(&mut self, properties: HashMap<CheetahString, CheetahString>);

    /// Returns the buyer ID associated with the message.
    fn buyer_id(&self) -> Option<CheetahString> {
        self.property(&CheetahString::from_static_str(MessageConst::PROPERTY_BUYER_ID))
    }

    /// Returns a reference to the buyer ID associated with the message.
    fn buyer_id_ref(&self) -> Option<&CheetahString> {
        self.property_ref(&CheetahString::from_static_str(MessageConst::PROPERTY_BUYER_ID))
    }

    /// Sets the buyer ID for the message.
    fn set_buyer_id(&mut self, buyer_id: CheetahString) {
        self.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_BUYER_ID),
            buyer_id,
        );
    }

    /// Retrieves the transaction ID associated with the message.
    ///
    /// # Returns
    ///
    /// A reference to the transaction ID as a `&str`.
    fn transaction_id(&self) -> Option<&CheetahString>;

    /// Sets the transaction ID for the message.
    fn set_transaction_id(&mut self, transaction_id: CheetahString);

    /// Sets the delay time for the message in seconds.
    fn set_delay_time_sec(&mut self, sec: u64) {
        self.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELAY_SEC),
            CheetahString::from_string(sec.to_string()),
        );
    }

    /// Returns the delay time for the message in seconds, or 0 if not set.
    fn get_delay_time_sec(&self) -> u64 {
        self.property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELAY_SEC))
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    }

    /// Sets the delay time for the message in milliseconds.
    fn set_delay_time_ms(&mut self, time_ms: u64) {
        self.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELAY_MS),
            CheetahString::from_string(time_ms.to_string()),
        );
    }

    /// Returns the delay time for the message in milliseconds, or 0 if not set.
    fn get_delay_time_ms(&self) -> u64 {
        self.property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELAY_MS))
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    }

    /// Sets the delivery time for the message in milliseconds.
    fn set_deliver_time_ms(&mut self, time_ms: u64) {
        self.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELIVER_MS),
            CheetahString::from_string(time_ms.to_string()),
        );
    }

    /// Returns the delivery time for the message in milliseconds, or 0 if not set.
    fn get_deliver_time_ms(&self) -> u64 {
        self.property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELIVER_MS))
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    }

    /// Returns a mutable reference to the compressed body of the message.
    fn get_compressed_body_mut(&mut self) -> Option<&mut Bytes>;

    /// Returns a reference to the compressed body of the message.
    fn get_compressed_body(&self) -> Option<&Bytes>;

    /// Sets the compressed body of the message.
    fn set_compressed_body_mut(&mut self, compressed_body: Bytes);

    /// Takes ownership of the message body, leaving it empty.
    fn take_body(&mut self) -> Option<Bytes>;

    /// Returns a reference to the message as a trait object.
    fn as_any(&self) -> &dyn Any;

    /// Returns a mutable reference to the message as a trait object.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

#[cfg(test)]
mod tests {
    use super::MessageConst;
    use super::MessageTrait;
    use crate::common::message::message_builder::MessageBuilder;
    use crate::common::message::message_single::Message;
    use cheetah_string::CheetahString;

    #[test]
    fn trait_object_get_transaction_id_uses_default_compat_accessor() {
        let mut message: Message = MessageBuilder::new()
            .topic("TopicTest")
            .body_slice(b"payload")
            .build_unchecked();
        MessageTrait::set_transaction_id(&mut message, CheetahString::from("tx-123"));

        let message_trait: &dyn MessageTrait = &message;

        assert_eq!(message_trait.transaction_id(), Some(&CheetahString::from("tx-123")));
    }

    #[test]
    fn timer_engine_type_uses_java_protocol_key() {
        assert_eq!(MessageConst::TIMER_ENGINE_TYPE, "E_T");
        assert_eq!(MessageConst::TIMER_ENGINE_FILE_TIME_WHEEL, "F");
        assert_eq!(MessageConst::TIMER_ENGINE_ROCKSDB_TIMELINE, "R");
    }
}

/// Magic code for message format version 1.
pub const MESSAGE_MAGIC_CODE_V1: i32 = -626843481;

/// Magic code for message format version 2.
pub const MESSAGE_MAGIC_CODE_V2: i32 = -626843477;

/// Represents the message format version.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Default)]
pub enum MessageVersion {
    #[default]
    V1,
    V2,
}

impl fmt::Display for MessageVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MessageVersion::V1 => write!(f, "V1"),
            MessageVersion::V2 => write!(f, "V2"),
        }
    }
}

impl MessageVersion {
    /// Returns the message version corresponding to the given magic code.
    ///
    /// # Errors
    ///
    /// Returns an error if the magic code is not recognized.
    pub fn value_of_magic_code(magic_code: i32) -> Result<MessageVersion, &'static str> {
        match magic_code {
            MESSAGE_MAGIC_CODE_V1 => Ok(MessageVersion::V1),
            MESSAGE_MAGIC_CODE_V2 => Ok(MessageVersion::V2),
            _ => Err("Invalid magicCode"),
        }
    }

    /// Returns the magic code for this message version.
    pub fn get_magic_code(&self) -> i32 {
        match self {
            MessageVersion::V1 => MESSAGE_MAGIC_CODE_V1,
            MessageVersion::V2 => MESSAGE_MAGIC_CODE_V2,
        }
    }

    /// Returns the number of bytes used to encode the topic length.
    pub fn get_topic_length_size(&self) -> usize {
        match self {
            MessageVersion::V1 => 1,
            MessageVersion::V2 => 2,
        }
    }

    /// Reads and returns the topic length from the buffer, advancing the buffer position.
    pub fn get_topic_length(&self, buffer: &mut Bytes) -> usize {
        match self {
            MessageVersion::V1 => buffer.get_u8() as usize,
            MessageVersion::V2 => buffer.get_i16() as usize,
        }
    }

    /// Returns the topic length from the buffer at the specified index without advancing the
    /// position.
    pub fn get_topic_length_at_index(&self, buffer: &[u8], index: usize) -> usize {
        match self {
            MessageVersion::V1 => buffer[index] as usize,
            MessageVersion::V2 => ((buffer[index] as usize) << 8) | (buffer[index + 1] as usize),
        }
    }

    /// Writes the topic length to the buffer according to the message version.
    pub fn put_topic_length(&self, buffer: &mut Vec<u8>, topic_length: usize) {
        match self {
            MessageVersion::V1 => buffer.push(topic_length as u8),
            MessageVersion::V2 => {
                buffer.push((topic_length >> 8) as u8);
                buffer.push((topic_length & 0xFF) as u8);
            }
        }
    }

    /// Returns `true` if this is message format version 1.
    pub fn is_v1(&self) -> bool {
        match self {
            MessageVersion::V1 => true,
            MessageVersion::V2 => false,
        }
    }

    /// Returns `true` if this is message format version 2.
    pub fn is_v2(&self) -> bool {
        match self {
            MessageVersion::V1 => false,
            MessageVersion::V2 => true,
        }
    }
}

/// Defines constants for message property names and configuration values.
pub struct MessageConst;

impl MessageConst {
    pub const DUP_INFO: &'static str = "DUP_INFO";
    pub const KEY_SEPARATOR: &'static str = " ";
    /// Host address where the message was born.
    pub const PROPERTY_BORN_HOST: &'static str = "__BORNHOST";
    /// Timestamp when the message was born.
    pub const PROPERTY_BORN_TIMESTAMP: &'static str = "BORN_TIMESTAMP";
    pub const PROPERTY_BUYER_ID: &'static str = "BUYER_ID";
    /// Time in seconds during which transaction checks are suppressed.
    pub const PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS: &'static str = "CHECK_IMMUNITY_TIME_IN_SECONDS";
    pub const PROPERTY_CLUSTER: &'static str = "CLUSTER";
    pub const PROPERTY_CONSUME_START_TIMESTAMP: &'static str = "CONSUME_START_TIME";
    pub const PROPERTY_CORRECTION_FLAG: &'static str = "CORRECTION_FLAG";
    pub const PROPERTY_CORRELATION_ID: &'static str = "CORRELATION_ID";
    pub const PROPERTY_CRC32: &'static str = "__CRC32#";
    pub const PROPERTY_DELAY_TIME_LEVEL: &'static str = "DELAY";
    pub const PROPERTY_DLQ_ORIGIN_MESSAGE_ID: &'static str = "DLQ_ORIGIN_MESSAGE_ID";
    pub const PROPERTY_STARTDE_LIVER_TIME: &'static str = "__STARTDELIVERTIME";
    /// Original topic name for messages in the dead-letter queue.
    pub const PROPERTY_DLQ_ORIGIN_TOPIC: &'static str = "DLQ_ORIGIN_TOPIC";
    pub const PROPERTY_EXTEND_UNIQ_INFO: &'static str = "EXTEND_UNIQ_INFO";
    pub const PROPERTY_FIRST_POP_TIME: &'static str = "1ST_POP_TIME";
    pub const PROPERTY_FORWARD_QUEUE_ID: &'static str = "PROPERTY_FORWARD_QUEUE_ID";
    pub const PROPERTY_INNER_BASE: &'static str = "INNER_BASE";
    pub const PROPERTY_INNER_MULTI_DISPATCH: &'static str = "INNER_MULTI_DISPATCH";
    pub const PROPERTY_INNER_MULTI_QUEUE_OFFSET: &'static str = "INNER_MULTI_QUEUE_OFFSET";
    pub const PROPERTY_INNER_NUM: &'static str = "INNER_NUM";
    pub const PROPERTY_INSTANCE_ID: &'static str = "INSTANCE_ID";
    pub const PROPERTY_KEYS: &'static str = "KEYS";
    pub const PROPERTY_MAX_OFFSET: &'static str = "MAX_OFFSET";
    pub const PROPERTY_MAX_RECONSUME_TIMES: &'static str = "MAX_RECONSUME_TIMES";
    pub const PROPERTY_MESSAGE_REPLY_TO_CLIENT: &'static str = "REPLY_TO_CLIENT";
    pub const PROPERTY_MESSAGE_TTL: &'static str = "TTL";
    pub const PROPERTY_MESSAGE_TYPE: &'static str = "MSG_TYPE";
    pub const PROPERTY_MIN_OFFSET: &'static str = "MIN_OFFSET";
    pub const PROPERTY_MQ2_FLAG: &'static str = "MQ2_FLAG";
    pub const PROPERTY_MSG_REGION: &'static str = "MSG_REGION";
    pub const PROPERTY_ORIGIN_MESSAGE_ID: &'static str = "ORIGIN_MESSAGE_ID";
    pub const PROPERTY_POP_CK: &'static str = "POP_CK";
    pub const PROPERTY_POP_CK_OFFSET: &'static str = "POP_CK_OFFSET";
    pub const PROPERTY_PRODUCER_GROUP: &'static str = "PGROUP";
    pub const PROPERTY_PUSH_REPLY_TIME: &'static str = "PUSH_REPLY_TIME";
    pub const PROPERTY_REAL_QUEUE_ID: &'static str = "REAL_QID";
    pub const PROPERTY_REAL_TOPIC: &'static str = "REAL_TOPIC";
    pub const PROPERTY_RECONSUME_TIME: &'static str = "RECONSUME_TIME";
    pub const PROPERTY_REDIRECT: &'static str = "REDIRECT";
    pub const PROPERTY_REPLY_MESSAGE_ARRIVE_TIME: &'static str = "ARRIVE_TIME";
    pub const PROPERTY_RETRY_TOPIC: &'static str = "RETRY_TOPIC";
    pub const PROPERTY_SHARDING_KEY: &'static str = "__SHARDINGKEY";
    pub const PROPERTY_LITE_TOPIC: &'static str = "__LITE_TOPIC";
    pub const PROPERTY_TAGS: &'static str = "TAGS";
    pub const PROPERTY_TIMER_DELAY_LEVEL: &'static str = "TIMER_DELAY_LEVEL";
    pub const PROPERTY_TIMER_DELAY_MS: &'static str = "TIMER_DELAY_MS";
    pub const PROPERTY_TIMER_DELAY_SEC: &'static str = "TIMER_DELAY_SEC";
    pub const PROPERTY_TIMER_DELIVER_MS: &'static str = "TIMER_DELIVER_MS";
    pub const PROPERTY_TIMER_DEL_UNIQKEY: &'static str = "TIMER_DEL_UNIQKEY";
    pub const PROPERTY_TIMER_DEQUEUE_MS: &'static str = "TIMER_DEQUEUE_MS";
    pub const PROPERTY_TIMER_ENQUEUE_MS: &'static str = "TIMER_ENQUEUE_MS";
    pub const PROPERTY_TIMER_OUT_MS: &'static str = "TIMER_OUT_MS";
    pub const PROPERTY_TIMER_ROLL_TIMES: &'static str = "TIMER_ROLL_TIMES";
    pub const PROPERTY_TRACE_CONTEXT: &'static str = "TRACE_CONTEXT";
    pub const PROPERTY_TRACE_SWITCH: &'static str = "TRACE_ON";
    pub const PROPERTY_TRANSACTION_CHECK_TIMES: &'static str = "TRANSACTION_CHECK_TIMES";
    pub const PROPERTY_TRANSACTION_ID: &'static str = "__transactionId__";
    pub const PROPERTY_TRANSACTION_PREPARED: &'static str = "TRAN_MSG";
    pub const PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET: &'static str = "TRAN_PREPARED_QUEUE_OFFSET";
    pub const PROPERTY_TRANSFER_FLAG: &'static str = "TRANSFER_FLAG";
    /// Transient property for group system flags set by the client when pulling messages.
    pub const PROPERTY_TRANSIENT_GROUP_CONFIG: &'static str = "__RMQ.TRANSIENT.GROUP_SYS_FLAG";
    /// Prefix for transient properties that are not persisted to broker disk.
    pub const PROPERTY_TRANSIENT_PREFIX: &'static str = "__RMQ.TRANSIENT.";
    /// Transient property for topic system flags set by the client when pulling messages.
    pub const PROPERTY_TRANSIENT_TOPIC_CONFIG: &'static str = "__RMQ.TRANSIENT.TOPIC_SYS_FLAG";
    pub const PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX: &'static str = "UNIQ_KEY";
    pub const PROPERTY_WAIT_STORE_MSG_OK: &'static str = "WAIT";

    /// Timer engine type identifier for RocksDB timeline implementation.
    pub const TIMER_ENGINE_ROCKSDB_TIMELINE: &'static str = "R";
    /// Timer engine type identifier for file-based time wheel implementation.
    pub const TIMER_ENGINE_FILE_TIME_WHEEL: &'static str = "F";
    /// Property name for timer engine type.
    pub const TIMER_ENGINE_TYPE: &'static str = "E_T";

    /// Index type identifier for message key indexing.
    pub const INDEX_KEY_TYPE: &'static str = "K";
    /// Index type identifier for unique indexing.
    pub const INDEX_UNIQUE_TYPE: &'static str = "U";
    /// Index type identifier for tag indexing.
    pub const INDEX_TAG_TYPE: &'static str = "T";
}

/// Set of system-reserved property names that cannot be used as user-defined properties.
pub static STRING_HASH_SET: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut set = HashSet::with_capacity(64);
    set.insert(MessageConst::PROPERTY_TRACE_SWITCH);
    set.insert(MessageConst::PROPERTY_MSG_REGION);
    set.insert(MessageConst::PROPERTY_KEYS);
    set.insert(MessageConst::PROPERTY_TAGS);
    set.insert(MessageConst::PROPERTY_WAIT_STORE_MSG_OK);
    set.insert(MessageConst::PROPERTY_DELAY_TIME_LEVEL);
    set.insert(MessageConst::PROPERTY_RETRY_TOPIC);
    set.insert(MessageConst::PROPERTY_REAL_TOPIC);
    set.insert(MessageConst::PROPERTY_REAL_QUEUE_ID);
    set.insert(MessageConst::PROPERTY_TRANSACTION_PREPARED);
    set.insert(MessageConst::PROPERTY_PRODUCER_GROUP);
    set.insert(MessageConst::PROPERTY_MIN_OFFSET);
    set.insert(MessageConst::PROPERTY_MAX_OFFSET);
    set.insert(MessageConst::PROPERTY_BUYER_ID);
    set.insert(MessageConst::PROPERTY_ORIGIN_MESSAGE_ID);
    set.insert(MessageConst::PROPERTY_TRANSFER_FLAG);
    set.insert(MessageConst::PROPERTY_CORRECTION_FLAG);
    set.insert(MessageConst::PROPERTY_MQ2_FLAG);
    set.insert(MessageConst::PROPERTY_RECONSUME_TIME);
    set.insert(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
    set.insert(MessageConst::PROPERTY_MAX_RECONSUME_TIMES);
    set.insert(MessageConst::PROPERTY_CONSUME_START_TIMESTAMP);
    set.insert(MessageConst::PROPERTY_POP_CK);
    set.insert(MessageConst::PROPERTY_POP_CK_OFFSET);
    set.insert(MessageConst::PROPERTY_FIRST_POP_TIME);
    set.insert(MessageConst::PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
    set.insert(MessageConst::DUP_INFO);
    set.insert(MessageConst::PROPERTY_EXTEND_UNIQ_INFO);
    set.insert(MessageConst::PROPERTY_INSTANCE_ID);
    set.insert(MessageConst::PROPERTY_CORRELATION_ID);
    set.insert(MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT);
    set.insert(MessageConst::PROPERTY_MESSAGE_TTL);
    set.insert(MessageConst::PROPERTY_REPLY_MESSAGE_ARRIVE_TIME);
    set.insert(MessageConst::PROPERTY_PUSH_REPLY_TIME);
    set.insert(MessageConst::PROPERTY_CLUSTER);
    set.insert(MessageConst::PROPERTY_MESSAGE_TYPE);
    set.insert(MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET);
    set.insert(MessageConst::PROPERTY_TIMER_DELAY_MS);
    set.insert(MessageConst::PROPERTY_TIMER_DELAY_SEC);
    set.insert(MessageConst::PROPERTY_TIMER_DELIVER_MS);
    set.insert(MessageConst::PROPERTY_TIMER_ENQUEUE_MS);
    set.insert(MessageConst::PROPERTY_TIMER_DEQUEUE_MS);
    set.insert(MessageConst::PROPERTY_TIMER_ROLL_TIMES);
    set.insert(MessageConst::PROPERTY_TIMER_OUT_MS);
    set.insert(MessageConst::PROPERTY_TIMER_DEL_UNIQKEY);
    set.insert(MessageConst::PROPERTY_TIMER_DELAY_LEVEL);
    set.insert(MessageConst::PROPERTY_BORN_HOST);
    set.insert(MessageConst::PROPERTY_BORN_TIMESTAMP);
    set.insert(MessageConst::PROPERTY_DLQ_ORIGIN_TOPIC);
    set.insert(MessageConst::PROPERTY_DLQ_ORIGIN_MESSAGE_ID);
    set.insert(MessageConst::PROPERTY_CRC32);
    set.insert(MessageConst::PROPERTY_LITE_TOPIC);
    set
});
