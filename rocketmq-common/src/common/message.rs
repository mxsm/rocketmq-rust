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

pub mod message_accessor;
pub mod message_batch;
pub mod message_client_ext;
pub mod message_client_id_setter;
pub mod message_decoder;
pub mod message_enum;
pub mod message_ext;
pub mod message_ext_broker_inner;
pub mod message_id;
pub mod message_queue;
pub mod message_queue_assignment;
pub mod message_queue_for_c;
pub mod message_single;

/// This module defines the `MessageTrait` trait, which provides a flexible interface for working
///
/// with message objects in RocketMQ. It includes methods for managing message properties, keys,
/// tags, body, and other metadata related to the message.
pub trait MessageTrait: Any + Display + Debug {
    /// Sets the keys for the message.
    ///
    /// # Arguments
    ///
    /// * `keys` - The keys to set, converted into a `String`.
    fn set_keys(&mut self, keys: CheetahString) {
        self.put_property(CheetahString::from_static_str(MessageConst::PROPERTY_KEYS), keys);
    }

    /// Adds a property to the message.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key, converted into a `String`.
    /// * `value` - The property value, converted into a `String`.
    fn put_property(&mut self, key: CheetahString, value: CheetahString);

    /// Clears a specific property from the message.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property to clear.
    fn clear_property(&mut self, name: &str);

    /// Adds a user-defined property to the message.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the user property, converted into a `String`.
    /// * `value` - The value of the user property, converted into a `String`.
    fn put_user_property(&mut self, name: CheetahString, value: CheetahString) {
        if STRING_HASH_SET.contains(name.as_str()) {
            panic!("The Property<{name}> is used by system, input another please");
        }
        if value.is_empty() || name.is_empty() {
            panic!("The name or value of property can not be null or blank string!");
        }
        self.put_property(name, value);
    }

    /// Retrieves a user-defined property from the message.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the user property to retrieve.
    ///
    /// # Returns
    ///
    /// An `Option<String>` containing the property value if it exists, otherwise `None`.
    fn get_user_property(&self, name: &CheetahString) -> Option<CheetahString> {
        self.get_property(name)
    }

    /// Retrieves a property from the message.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property to retrieve.
    ///
    /// # Returns
    ///
    /// An `Option<String>` containing the property value if it exists, otherwise `None`.
    fn get_property(&self, name: &CheetahString) -> Option<CheetahString>;

    /// Retrieves the topic of the message.
    ///
    /// # Returns
    ///
    /// A reference to the topic as a `&str`.
    fn get_topic(&self) -> &CheetahString;

    /// Sets the topic for the message.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic to set, converted into a `String`.
    fn set_topic(&mut self, topic: CheetahString);

    /// Retrieves the tags associated with the message.
    ///
    /// # Returns
    ///
    /// An `Option<String>` containing the tags if they exist, otherwise `None`.
    fn get_tags(&self) -> Option<CheetahString> {
        self.get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_TAGS))
    }

    /// Sets the tags for the message.
    ///
    /// # Arguments
    ///
    /// * `tags` - The tags to set, converted into a `String`.
    fn set_tags(&mut self, tags: CheetahString) {
        self.put_property(CheetahString::from_static_str(MessageConst::PROPERTY_TAGS), tags);
    }

    /// Retrieves the keys associated with the message.
    ///
    /// # Returns
    ///
    /// An `Option<String>` containing the keys if they exist, otherwise `None`.
    fn get_keys(&self) -> Option<CheetahString> {
        self.get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_KEYS))
    }

    /// Sets multiple keys from a collection for the message.
    ///
    /// # Arguments
    ///
    /// * `key_collection` - A vector of keys to set.
    fn set_keys_from_collection(&mut self, key_collection: Vec<String>) {
        let keys = key_collection.join(MessageConst::KEY_SEPARATOR);
        self.set_keys(CheetahString::from_string(keys));
    }

    /// Retrieves the delay time level of the message.
    ///
    /// # Returns
    ///
    /// An `i32` representing the delay time level.
    fn get_delay_time_level(&self) -> i32 {
        self.get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_DELAY_TIME_LEVEL))
            .unwrap_or(CheetahString::from_slice("0"))
            .parse()
            .unwrap_or(0)
    }

    /// Sets the delay time level for the message.
    ///
    /// # Arguments
    ///
    /// * `level` - The delay time level to set.
    fn set_delay_time_level(&mut self, level: i32) {
        self.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_DELAY_TIME_LEVEL),
            CheetahString::from_string(level.to_string()),
        );
    }

    /// Checks if the message should wait for store acknowledgment.
    ///
    /// # Returns
    ///
    /// `true` if the message should wait for store acknowledgment; `false` otherwise.
    fn is_wait_store_msg_ok(&self) -> bool {
        self.get_property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_WAIT_STORE_MSG_OK,
        ))
        .unwrap_or(CheetahString::from_slice("true"))
        .parse()
        .unwrap_or(true)
    }

    /// Sets whether the message should wait for store acknowledgment.
    ///
    /// # Arguments
    ///
    /// * `wait_store_msg_ok` - A boolean indicating whether to wait for store acknowledgment.
    fn set_wait_store_msg_ok(&mut self, wait_store_msg_ok: bool) {
        self.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_WAIT_STORE_MSG_OK),
            CheetahString::from_string(wait_store_msg_ok.to_string()),
        );
    }

    /// Sets the instance ID for the message.
    ///
    /// # Arguments
    ///
    /// * `instance_id` - The instance ID to set.
    fn set_instance_id(&mut self, instance_id: CheetahString) {
        self.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_INSTANCE_ID),
            instance_id,
        );
    }

    /// Retrieves the flag associated with the message.
    ///
    /// # Returns
    ///
    /// An `i32` representing the flag.
    fn get_flag(&self) -> i32;

    /// Sets the flag for the message.
    ///
    /// # Arguments
    ///
    /// * `flag` - The flag to set.
    fn set_flag(&mut self, flag: i32);

    /// Retrieves the body of the message.
    ///
    /// # Returns
    ///
    /// A byte slice (`&[u8]`) representing the body of the message.
    fn get_body(&self) -> Option<&Bytes>;

    /// Sets the body of the message.
    ///
    /// # Arguments
    ///
    /// * `body` - The byte slice (`&[u8]`) to set as the body.
    fn set_body(&mut self, body: Bytes);

    /// Retrieves all properties associated with the message.
    ///
    /// # Returns
    ///
    /// A reference to a `HashMap<String, String>` containing the properties.
    fn get_properties(&self) -> &HashMap<CheetahString, CheetahString>;

    /// Sets multiple properties for the message.
    ///
    /// # Arguments
    ///
    /// * `properties` - A `HashMap<String, String>` containing the properties to set.
    fn set_properties(&mut self, properties: HashMap<CheetahString, CheetahString>);

    /// Retrieves the buyer ID associated with the message.
    ///
    /// # Returns
    ///
    /// An `Option<String>` containing the buyer ID if it exists, otherwise `None`.
    fn get_buyer_id(&self) -> Option<CheetahString> {
        self.get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_BUYER_ID))
    }

    /// Sets the buyer ID for the message.
    ///
    /// # Arguments
    ///
    /// * `buyer_id` - The buyer ID to set.
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
    fn get_transaction_id(&self) -> Option<&CheetahString>;

    /// Sets the transaction ID for the message.
    ///
    /// # Arguments
    ///
    /// * `transaction_id` - The transaction ID to set.
    fn set_transaction_id(&mut self, transaction_id: CheetahString);

    /// Sets the delay time for the message in seconds.
    ///
    /// # Arguments
    ///
    /// * `sec` - The delay time in seconds.
    fn set_delay_time_sec(&mut self, sec: u64) {
        self.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELAY_SEC),
            CheetahString::from_string(sec.to_string()),
        );
    }

    /// Retrieves the delay time for the message in seconds.
    ///
    /// # Returns
    ///
    /// The delay time in seconds.
    fn get_delay_time_sec(&self) -> u64 {
        self.get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELAY_SEC))
            .unwrap_or(CheetahString::from_slice("0"))
            .parse()
            .unwrap_or(0)
    }

    /// Sets the delay time for the message in milliseconds.
    ///
    /// # Arguments
    ///
    /// * `time_ms` - The delay time in milliseconds.
    fn set_delay_time_ms(&mut self, time_ms: u64) {
        self.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELAY_MS),
            CheetahString::from_string(time_ms.to_string()),
        );
    }

    /// Retrieves the delay time for the message in milliseconds.
    ///
    /// # Returns
    ///
    /// The delay time in milliseconds.
    fn get_delay_time_ms(&self) -> u64 {
        self.get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELAY_MS))
            .unwrap_or(CheetahString::from_slice("0"))
            .parse()
            .unwrap_or(0)
    }

    /// Sets the delivery time for the message in milliseconds.
    ///
    /// # Arguments
    ///
    /// * `time_ms` - The delivery time in milliseconds.
    fn set_deliver_time_ms(&mut self, time_ms: u64) {
        self.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELIVER_MS),
            CheetahString::from_string(time_ms.to_string()),
        );
    }

    /// Retrieves the delivery time for the message in milliseconds.
    ///
    /// # Returns
    ///
    /// The delivery time in milliseconds.
    fn get_deliver_time_ms(&self) -> u64 {
        self.get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELIVER_MS))
            .unwrap_or(CheetahString::from_slice("0"))
            .parse()
            .unwrap_or(0)
    }

    /// Retrieves a mutable reference to the compressed body of the message.
    ///
    /// # Returns
    /// A mutable reference to an `Option<Bytes>` containing the compressed body, if it exists.
    fn get_compressed_body_mut(&mut self) -> &mut Option<Bytes>;

    /// Retrieves an immutable reference to the compressed body of the message.
    ///
    /// # Returns
    /// An `Option<&Bytes>` containing the compressed body, if it exists.
    fn get_compressed_body(&self) -> Option<&Bytes>;

    /// Sets the compressed body of the message.
    ///
    /// # Arguments
    /// * `compressed_body` - A `Bytes` object representing the compressed body to set.
    fn set_compressed_body_mut(&mut self, compressed_body: Bytes);

    /// Takes ownership of the message body, leaving it empty.
    ///
    /// # Returns
    /// An `Option<Bytes>` containing the message body if it exists, otherwise `None`.
    fn take_body(&mut self) -> Option<Bytes>;

    /// Converts the message into a dynamic `Any` type.
    ///
    /// # Returns
    ///
    /// A reference to the message as `&dyn Any`.
    fn as_any(&self) -> &dyn Any;

    /// Converts the message into a mutable dynamic `Any` type.
    ///
    /// # Returns
    ///
    /// A mutable reference to the message as `&mut dyn Any`.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

pub const MESSAGE_MAGIC_CODE_V1: i32 = -626843481;
pub const MESSAGE_MAGIC_CODE_V2: i32 = -626843477;

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
    pub fn value_of_magic_code(magic_code: i32) -> Result<MessageVersion, &'static str> {
        match magic_code {
            MESSAGE_MAGIC_CODE_V1 => Ok(MessageVersion::V1),
            MESSAGE_MAGIC_CODE_V2 => Ok(MessageVersion::V2),
            _ => Err("Invalid magicCode"),
        }
    }

    pub fn get_magic_code(&self) -> i32 {
        match self {
            MessageVersion::V1 => MESSAGE_MAGIC_CODE_V1,
            MessageVersion::V2 => MESSAGE_MAGIC_CODE_V2,
        }
    }

    pub fn get_topic_length_size(&self) -> usize {
        match self {
            MessageVersion::V1 => 1,
            MessageVersion::V2 => 2,
        }
    }

    pub fn get_topic_length(&self, buffer: &mut Bytes) -> usize {
        match self {
            MessageVersion::V1 => buffer.get_u8() as usize,
            MessageVersion::V2 => buffer.get_i16() as usize,
        }
    }

    pub fn get_topic_length_at_index(&self, buffer: &[u8], index: usize) -> usize {
        match self {
            MessageVersion::V1 => buffer[index] as usize,
            MessageVersion::V2 => ((buffer[index] as usize) << 8) | (buffer[index + 1] as usize),
        }
    }

    pub fn put_topic_length(&self, buffer: &mut Vec<u8>, topic_length: usize) {
        match self {
            MessageVersion::V1 => buffer.push(topic_length as u8),
            MessageVersion::V2 => {
                buffer.push((topic_length >> 8) as u8);
                buffer.push((topic_length & 0xFF) as u8);
            }
        }
    }

    pub fn is_v1(&self) -> bool {
        match self {
            MessageVersion::V1 => true,
            MessageVersion::V2 => false,
        }
    }

    pub fn is_v2(&self) -> bool {
        match self {
            MessageVersion::V1 => false,
            MessageVersion::V2 => true,
        }
    }
}

pub struct MessageConst;

impl MessageConst {
    pub const DUP_INFO: &'static str = "DUP_INFO";
    pub const KEY_SEPARATOR: &'static str = " ";
    pub const PROPERTY_BORN_HOST: &'static str = "__BORNHOST";
    pub const PROPERTY_BORN_TIMESTAMP: &'static str = "BORN_TIMESTAMP";
    pub const PROPERTY_BUYER_ID: &'static str = "BUYER_ID";
    pub const PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS: &'static str = "CHECK_IMMUNITY_TIME_IN_SECONDS";
    pub const PROPERTY_CLUSTER: &'static str = "CLUSTER";
    pub const PROPERTY_CONSUME_START_TIMESTAMP: &'static str = "CONSUME_START_TIME";
    pub const PROPERTY_CORRECTION_FLAG: &'static str = "CORRECTION_FLAG";
    pub const PROPERTY_CORRELATION_ID: &'static str = "CORRELATION_ID";
    pub const PROPERTY_CRC32: &'static str = "__CRC32#";
    pub const PROPERTY_DELAY_TIME_LEVEL: &'static str = "DELAY";
    pub const PROPERTY_DLQ_ORIGIN_MESSAGE_ID: &'static str = "DLQ_ORIGIN_MESSAGE_ID";
    pub const PROPERTY_STARTDE_LIVER_TIME: &'static str = "__STARTDELIVERTIME";
    /**
     * properties for DLQ
     */
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
    /**
     * the transient property key of groupSysFlag (set by the client when pulling messages)
     */
    pub const PROPERTY_TRANSIENT_GROUP_CONFIG: &'static str = "__RMQ.TRANSIENT.GROUP_SYS_FLAG";
    /**
     * property which name starts with "__RMQ.TRANSIENT." is called transient one that will not
     * be stored in broker disks.
     */
    pub const PROPERTY_TRANSIENT_PREFIX: &'static str = "__RMQ.TRANSIENT.";
    /**
     * the transient property key of topicSysFlag (set by the client when pulling messages)
     */
    pub const PROPERTY_TRANSIENT_TOPIC_CONFIG: &'static str = "__RMQ.TRANSIENT.TOPIC_SYS_FLAG";
    pub const PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX: &'static str = "UNIQ_KEY";
    pub const PROPERTY_WAIT_STORE_MSG_OK: &'static str = "WAIT";

    // Index type constants
    pub const INDEX_KEY_TYPE: &'static str = "K";
    pub const INDEX_UNIQUE_TYPE: &'static str = "U";
    pub const INDEX_TAG_TYPE: &'static str = "T";
}

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
    set
});
