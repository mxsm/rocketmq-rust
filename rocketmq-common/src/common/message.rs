/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

pub mod message_batch;
pub mod message_id;
pub mod message_queue;
pub mod message_single;

use std::collections::HashMap;

pub trait MessageTrait {
    fn get_topic(&self) -> &str;

    fn set_topic(&mut self, topic: impl Into<String>);

    fn get_tags(&self) -> Option<&str>;

    fn set_tags(&mut self, tags: impl Into<String>);

    fn put_property(&mut self, key: impl Into<String>, value: impl Into<String>);

    fn get_properties(&self) -> &HashMap<String, String>;

    fn put_user_property(&mut self, name: impl Into<String>, value: impl Into<String>);

    fn get_delay_time_level(&self) -> i32;

    fn set_delay_time_level(&self, level: i32) -> i32;
}

pub const MESSAGE_MAGIC_CODE_V1: i32 = -626843481;
pub const MESSAGE_MAGIC_CODE_V2: i32 = -626843477;

#[derive(Debug, PartialEq, Eq, Hash)]
enum MessageVersion {
    V1(i32),
    V2(i32),
}

impl MessageVersion {
    fn value_of_magic_code(magic_code: i32) -> Result<MessageVersion, &'static str> {
        match magic_code {
            MESSAGE_MAGIC_CODE_V1 => Ok(MessageVersion::V1(MESSAGE_MAGIC_CODE_V1)),
            MESSAGE_MAGIC_CODE_V2 => Ok(MessageVersion::V2(MESSAGE_MAGIC_CODE_V2)),
            _ => Err("Invalid magicCode"),
        }
    }

    fn get_magic_code(&self) -> i32 {
        match self {
            MessageVersion::V1(value) => *value,
            MessageVersion::V2(value) => *value,
        }
    }

    fn get_topic_length_size(&self) -> usize {
        match self {
            MessageVersion::V1(_) => 1,
            MessageVersion::V2(_) => 2,
        }
    }

    fn get_topic_length(&self, buffer: &[u8]) -> usize {
        match self {
            MessageVersion::V1(_) => buffer[0] as usize,
            MessageVersion::V2(_) => ((buffer[0] as usize) << 8) | (buffer[1] as usize),
        }
    }

    fn get_topic_length_at_index(&self, buffer: &[u8], index: usize) -> usize {
        match self {
            MessageVersion::V1(_) => buffer[index] as usize,
            MessageVersion::V2(_) => ((buffer[index] as usize) << 8) | (buffer[index + 1] as usize),
        }
    }

    fn put_topic_length(&self, buffer: &mut Vec<u8>, topic_length: usize) {
        match self {
            MessageVersion::V1(_) => buffer.push(topic_length as u8),
            MessageVersion::V2(_) => {
                buffer.push((topic_length >> 8) as u8);
                buffer.push((topic_length & 0xFF) as u8);
            }
        }
    }
}

use std::{collections::HashSet, string::ToString};

use lazy_static::lazy_static;

pub struct MessageConst;

impl MessageConst {
    pub const PROPERTY_KEYS: &'static str = "KEYS";
    pub const PROPERTY_TAGS: &'static str = "TAGS";
    pub const PROPERTY_WAIT_STORE_MSG_OK: &'static str = "WAIT";
    pub const PROPERTY_DELAY_TIME_LEVEL: &'static str = "DELAY";
    pub const PROPERTY_RETRY_TOPIC: &'static str = "RETRY_TOPIC";
    pub const PROPERTY_REAL_TOPIC: &'static str = "REAL_TOPIC";
    pub const PROPERTY_REAL_QUEUE_ID: &'static str = "REAL_QID";
    pub const PROPERTY_TRANSACTION_PREPARED: &'static str = "TRAN_MSG";
    pub const PROPERTY_PRODUCER_GROUP: &'static str = "PGROUP";
    pub const PROPERTY_MIN_OFFSET: &'static str = "MIN_OFFSET";
    pub const PROPERTY_MAX_OFFSET: &'static str = "MAX_OFFSET";
    pub const PROPERTY_BUYER_ID: &'static str = "BUYER_ID";
    pub const PROPERTY_ORIGIN_MESSAGE_ID: &'static str = "ORIGIN_MESSAGE_ID";
    pub const PROPERTY_TRANSFER_FLAG: &'static str = "TRANSFER_FLAG";
    pub const PROPERTY_CORRECTION_FLAG: &'static str = "CORRECTION_FLAG";
    pub const PROPERTY_MQ2_FLAG: &'static str = "MQ2_FLAG";
    pub const PROPERTY_RECONSUME_TIME: &'static str = "RECONSUME_TIME";
    pub const PROPERTY_MSG_REGION: &'static str = "MSG_REGION";
    pub const PROPERTY_TRACE_SWITCH: &'static str = "TRACE_ON";
    pub const PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX: &'static str = "UNIQ_KEY";
    pub const PROPERTY_EXTEND_UNIQ_INFO: &'static str = "EXTEND_UNIQ_INFO";
    pub const PROPERTY_MAX_RECONSUME_TIMES: &'static str = "MAX_RECONSUME_TIMES";
    pub const PROPERTY_CONSUME_START_TIMESTAMP: &'static str = "CONSUME_START_TIME";
    pub const PROPERTY_INNER_NUM: &'static str = "INNER_NUM";
    pub const PROPERTY_INNER_BASE: &'static str = "INNER_BASE";
    pub const DUP_INFO: &'static str = "DUP_INFO";
    pub const PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS: &'static str =
        "CHECK_IMMUNITY_TIME_IN_SECONDS";
    pub const PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET: &'static str =
        "TRAN_PREPARED_QUEUE_OFFSET";
    pub const PROPERTY_TRANSACTION_ID: &'static str = "__transactionId__";
    pub const PROPERTY_TRANSACTION_CHECK_TIMES: &'static str = "TRANSACTION_CHECK_TIMES";
    pub const PROPERTY_INSTANCE_ID: &'static str = "INSTANCE_ID";
    pub const PROPERTY_CORRELATION_ID: &'static str = "CORRELATION_ID";
    pub const PROPERTY_MESSAGE_REPLY_TO_CLIENT: &'static str = "REPLY_TO_CLIENT";
    pub const PROPERTY_MESSAGE_TTL: &'static str = "TTL";
    pub const PROPERTY_REPLY_MESSAGE_ARRIVE_TIME: &'static str = "ARRIVE_TIME";
    pub const PROPERTY_PUSH_REPLY_TIME: &'static str = "PUSH_REPLY_TIME";
    pub const PROPERTY_CLUSTER: &'static str = "CLUSTER";
    pub const PROPERTY_MESSAGE_TYPE: &'static str = "MSG_TYPE";
    pub const PROPERTY_POP_CK: &'static str = "POP_CK";
    pub const PROPERTY_POP_CK_OFFSET: &'static str = "POP_CK_OFFSET";
    pub const PROPERTY_FIRST_POP_TIME: &'static str = "1ST_POP_TIME";
    pub const PROPERTY_SHARDING_KEY: &'static str = "__SHARDINGKEY";
    pub const PROPERTY_FORWARD_QUEUE_ID: &'static str = "PROPERTY_FORWARD_QUEUE_ID";
    pub const PROPERTY_REDIRECT: &'static str = "REDIRECT";
    pub const PROPERTY_INNER_MULTI_DISPATCH: &'static str = "INNER_MULTI_DISPATCH";
    pub const PROPERTY_INNER_MULTI_QUEUE_OFFSET: &'static str = "INNER_MULTI_QUEUE_OFFSET";
    pub const PROPERTY_TRACE_CONTEXT: &'static str = "TRACE_CONTEXT";
    pub const PROPERTY_TIMER_DELAY_SEC: &'static str = "TIMER_DELAY_SEC";
    pub const PROPERTY_TIMER_DELIVER_MS: &'static str = "TIMER_DELIVER_MS";
    pub const PROPERTY_BORN_HOST: &'static str = "__BORNHOST";
    pub const PROPERTY_BORN_TIMESTAMP: &'static str = "BORN_TIMESTAMP";

    /**
     * property which name starts with "__RMQ.TRANSIENT." is called transient one that will not
     * be stored in broker disks.
     */
    pub const PROPERTY_TRANSIENT_PREFIX: &'static str = "__RMQ.TRANSIENT.";

    /**
     * the transient property key of topicSysFlag (set by the client when pulling messages)
     */
    pub const PROPERTY_TRANSIENT_TOPIC_CONFIG: &'static str = "__RMQ.TRANSIENT.TOPIC_SYS_FLAG";

    /**
     * the transient property key of groupSysFlag (set by the client when pulling messages)
     */
    pub const PROPERTY_TRANSIENT_GROUP_CONFIG: &'static str = "__RMQ.TRANSIENT.GROUP_SYS_FLAG";

    pub const KEY_SEPARATOR: &'static str = " ";

    pub const PROPERTY_TIMER_ENQUEUE_MS: &'static str = "TIMER_ENQUEUE_MS";
    pub const PROPERTY_TIMER_DEQUEUE_MS: &'static str = "TIMER_DEQUEUE_MS";
    pub const PROPERTY_TIMER_ROLL_TIMES: &'static str = "TIMER_ROLL_TIMES";
    pub const PROPERTY_TIMER_OUT_MS: &'static str = "TIMER_OUT_MS";
    pub const PROPERTY_TIMER_DEL_UNIQKEY: &'static str = "TIMER_DEL_UNIQKEY";
    pub const PROPERTY_TIMER_DELAY_LEVEL: &'static str = "TIMER_DELAY_LEVEL";
    pub const PROPERTY_TIMER_DELAY_MS: &'static str = "TIMER_DELAY_MS";
    pub const PROPERTY_CRC32: &'static str = "__CRC32#";

    /**
     * properties for DLQ
     */
    pub const PROPERTY_DLQ_ORIGIN_TOPIC: &'static str = "DLQ_ORIGIN_TOPIC";
    pub const PROPERTY_DLQ_ORIGIN_MESSAGE_ID: &'static str = "DLQ_ORIGIN_MESSAGE_ID";
}

lazy_static! {
    pub static ref STRING_HASH_SET: HashSet<&'static str> = {
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
    };
}
