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

use cheetah_string::CheetahString;

use crate::common::message::MessageConst;

/// Type-safe message properties.
#[derive(Clone, Debug, Default)]
pub struct MessageProperties {
    inner: HashMap<CheetahString, CheetahString>,
}

impl MessageProperties {
    /// Creates a new empty properties collection.
    pub fn new() -> Self {
        Self::default()
    }

    /// Gets a property value by key.
    #[inline]
    pub fn get(&self, key: MessagePropertyKey) -> Option<&str> {
        self.inner.get(key.as_str()).map(|s| s.as_str())
    }

    /// Sets a property value.
    pub(crate) fn insert(&mut self, key: MessagePropertyKey, value: impl Into<CheetahString>) {
        self.inner.insert(key.to_cheetah_string(), value.into());
    }

    /// Removes a property.
    pub(crate) fn remove(&mut self, key: MessagePropertyKey) -> Option<CheetahString> {
        self.inner.remove(key.as_str())
    }

    /// Returns all properties as a map (for serialization and external crates).
    #[inline]
    pub fn as_map(&self) -> &HashMap<CheetahString, CheetahString> {
        &self.inner
    }

    /// Returns all properties as a mutable map (for internal use).
    #[doc(hidden)]
    #[inline]
    pub fn as_map_mut(&mut self) -> &mut HashMap<CheetahString, CheetahString> {
        &mut self.inner
    }

    /// Creates MessageProperties from a HashMap.
    #[inline]
    pub fn from_map(map: HashMap<CheetahString, CheetahString>) -> Self {
        Self { inner: map }
    }

    /// Returns the number of properties.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if there are no properties.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    // Convenience accessors for well-known properties

    /// Returns the tags, if any.
    #[inline]
    pub fn tags(&self) -> Option<&str> {
        self.get(MessagePropertyKey::Tags)
    }

    /// Returns the keys as a vector, if any.
    pub fn keys(&self) -> Option<Vec<String>> {
        self.get(MessagePropertyKey::Keys).map(|s| {
            s.split(MessageConst::KEY_SEPARATOR)
                .filter(|k| !k.is_empty())
                .map(|k| k.to_string())
                .collect()
        })
    }

    /// Returns the delay time level.
    pub fn delay_level(&self) -> Option<i32> {
        self.get(MessagePropertyKey::DelayTimeLevel)
            .and_then(|s| s.parse().ok())
    }

    /// Returns the delay time in seconds.
    pub fn delay_time_sec(&self) -> Option<u64> {
        self.get(MessagePropertyKey::DelayTimeSec).and_then(|s| s.parse().ok())
    }

    /// Returns the delay time in milliseconds.
    pub fn delay_time_ms(&self) -> Option<u64> {
        self.get(MessagePropertyKey::DelayTimeMs).and_then(|s| s.parse().ok())
    }

    /// Returns the delivery time in milliseconds.
    pub fn deliver_time_ms(&self) -> Option<u64> {
        self.get(MessagePropertyKey::DeliverTimeMs).and_then(|s| s.parse().ok())
    }

    /// Returns the buyer ID.
    pub fn buyer_id(&self) -> Option<&str> {
        self.get(MessagePropertyKey::BuyerId)
    }

    /// Returns whether to wait for store confirmation.
    pub fn wait_store_msg_ok(&self) -> bool {
        self.get(MessagePropertyKey::WaitStoreMsgOk)
            .map(|s| s != "false")
            .unwrap_or(true)
    }

    /// Returns the origin message ID.
    pub fn origin_message_id(&self) -> Option<&str> {
        self.get(MessagePropertyKey::OriginMessageId)
    }

    /// Returns the retry topic.
    pub fn retry_topic(&self) -> Option<&str> {
        self.get(MessagePropertyKey::RetryTopic)
    }

    /// Returns the real topic.
    pub fn real_topic(&self) -> Option<&str> {
        self.get(MessagePropertyKey::RealTopic)
    }

    /// Returns the real queue ID.
    pub fn real_queue_id(&self) -> Option<i32> {
        self.get(MessagePropertyKey::RealQueueId).and_then(|s| s.parse().ok())
    }

    /// Returns the unique client message ID.
    pub fn unique_client_msg_id(&self) -> Option<&str> {
        self.get(MessagePropertyKey::UniqueClientMsgId)
    }

    /// Returns the producer group.
    pub fn producer_group(&self) -> Option<&str> {
        self.get(MessagePropertyKey::ProducerGroup)
    }

    /// Returns the instance ID.
    pub fn instance_id(&self) -> Option<&str> {
        self.get(MessagePropertyKey::InstanceId)
    }

    /// Returns the correlation ID.
    pub fn correlation_id(&self) -> Option<&str> {
        self.get(MessagePropertyKey::CorrelationId)
    }

    /// Returns the message type.
    pub fn message_type(&self) -> Option<&str> {
        self.get(MessagePropertyKey::MessageType)
    }

    /// Returns the trace switch.
    pub fn trace_switch(&self) -> bool {
        self.get(MessagePropertyKey::TraceSwitch)
            .map(|s| s == "true")
            .unwrap_or(false)
    }
}

/// Type-safe property keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MessagePropertyKey {
    Tags,
    Keys,
    WaitStoreMsgOk,
    DelayTimeLevel,
    DelayTimeSec,
    DelayTimeMs,
    DeliverTimeMs,
    RetryTopic,
    RealTopic,
    RealQueueId,
    TransactionPrepared,
    ProducerGroup,
    MinOffset,
    MaxOffset,
    BuyerId,
    OriginMessageId,
    TransferFlag,
    CorrectionFlag,
    MqReplyTopic,
    MqReplyQueueId,
    UniqueClientMsgId,
    ReconsumeTime,
    MsgRegion,
    TraceSwitch,
    UniqueKey,
    MaxReconsumeTimes,
    ConsumeStartTimestamp,
    PopCk,
    PopCkOffset,
    FirstPopTime,
    TransactionPreparedQueueOffset,
    DupInfo,
    ExtendUniqInfo,
    InstanceId,
    CorrelationId,
    MessageReplyToClient,
    MessageTtl,
    ReplyMessageArriveTime,
    PushReplyTime,
    Cluster,
    MessageType,
    InnerMultiQueueOffset,
    TimerDelayLevel,
    TimerEnqueueMs,
    TimerDequeueMs,
    TimerRollTimes,
    TimerOutMs,
    TimerDelUniqkey,
    BornHost,
    BornTimestamp,
    DlqOriginTopic,
    DlqOriginMessageId,
    Crc32,
    Redirect,
    ForwardQueueId,
    InnerBase,
    InnerMultiDispatch,
    InnerNum,
    Mq2Flag,
    ShardingKey,
    TransactionId,
    TransactionCheckTimes,
    TransientGroupConfig,
    TransientTopicConfig,
    TraceContext,
}

impl MessagePropertyKey {
    /// Returns the string representation of this key.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Tags => MessageConst::PROPERTY_TAGS,
            Self::Keys => MessageConst::PROPERTY_KEYS,
            Self::WaitStoreMsgOk => MessageConst::PROPERTY_WAIT_STORE_MSG_OK,
            Self::DelayTimeLevel => MessageConst::PROPERTY_DELAY_TIME_LEVEL,
            Self::DelayTimeSec => MessageConst::PROPERTY_TIMER_DELAY_SEC,
            Self::DelayTimeMs => MessageConst::PROPERTY_TIMER_DELAY_MS,
            Self::DeliverTimeMs => MessageConst::PROPERTY_TIMER_DELIVER_MS,
            Self::RetryTopic => MessageConst::PROPERTY_RETRY_TOPIC,
            Self::RealTopic => MessageConst::PROPERTY_REAL_TOPIC,
            Self::RealQueueId => MessageConst::PROPERTY_REAL_QUEUE_ID,
            Self::TransactionPrepared => MessageConst::PROPERTY_TRANSACTION_PREPARED,
            Self::ProducerGroup => MessageConst::PROPERTY_PRODUCER_GROUP,
            Self::MinOffset => MessageConst::PROPERTY_MIN_OFFSET,
            Self::MaxOffset => MessageConst::PROPERTY_MAX_OFFSET,
            Self::BuyerId => MessageConst::PROPERTY_BUYER_ID,
            Self::OriginMessageId => MessageConst::PROPERTY_ORIGIN_MESSAGE_ID,
            Self::TransferFlag => MessageConst::PROPERTY_TRANSFER_FLAG,
            Self::CorrectionFlag => MessageConst::PROPERTY_CORRECTION_FLAG,
            Self::MqReplyTopic => MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT,
            Self::MqReplyQueueId => MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT,
            Self::UniqueClientMsgId => MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            Self::ReconsumeTime => MessageConst::PROPERTY_RECONSUME_TIME,
            Self::MsgRegion => MessageConst::PROPERTY_MSG_REGION,
            Self::TraceSwitch => MessageConst::PROPERTY_TRACE_SWITCH,
            Self::UniqueKey => MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            Self::MaxReconsumeTimes => MessageConst::PROPERTY_MAX_RECONSUME_TIMES,
            Self::ConsumeStartTimestamp => MessageConst::PROPERTY_CONSUME_START_TIMESTAMP,
            Self::PopCk => MessageConst::PROPERTY_POP_CK,
            Self::PopCkOffset => MessageConst::PROPERTY_POP_CK_OFFSET,
            Self::FirstPopTime => MessageConst::PROPERTY_FIRST_POP_TIME,
            Self::TransactionPreparedQueueOffset => MessageConst::PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET,
            Self::DupInfo => MessageConst::DUP_INFO,
            Self::ExtendUniqInfo => MessageConst::PROPERTY_EXTEND_UNIQ_INFO,
            Self::InstanceId => MessageConst::PROPERTY_INSTANCE_ID,
            Self::CorrelationId => MessageConst::PROPERTY_CORRELATION_ID,
            Self::MessageReplyToClient => MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT,
            Self::MessageTtl => MessageConst::PROPERTY_MESSAGE_TTL,
            Self::ReplyMessageArriveTime => MessageConst::PROPERTY_REPLY_MESSAGE_ARRIVE_TIME,
            Self::PushReplyTime => MessageConst::PROPERTY_PUSH_REPLY_TIME,
            Self::Cluster => MessageConst::PROPERTY_CLUSTER,
            Self::MessageType => MessageConst::PROPERTY_MESSAGE_TYPE,
            Self::InnerMultiQueueOffset => MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET,
            Self::TimerDelayLevel => MessageConst::PROPERTY_TIMER_DELAY_LEVEL,
            Self::TimerEnqueueMs => MessageConst::PROPERTY_TIMER_ENQUEUE_MS,
            Self::TimerDequeueMs => MessageConst::PROPERTY_TIMER_DEQUEUE_MS,
            Self::TimerRollTimes => MessageConst::PROPERTY_TIMER_ROLL_TIMES,
            Self::TimerOutMs => MessageConst::PROPERTY_TIMER_OUT_MS,
            Self::TimerDelUniqkey => MessageConst::PROPERTY_TIMER_DEL_UNIQKEY,
            Self::BornHost => MessageConst::PROPERTY_BORN_HOST,
            Self::BornTimestamp => MessageConst::PROPERTY_BORN_TIMESTAMP,
            Self::DlqOriginTopic => MessageConst::PROPERTY_DLQ_ORIGIN_TOPIC,
            Self::DlqOriginMessageId => MessageConst::PROPERTY_DLQ_ORIGIN_MESSAGE_ID,
            Self::Crc32 => MessageConst::PROPERTY_CRC32,
            Self::Redirect => MessageConst::PROPERTY_REDIRECT,
            Self::ForwardQueueId => MessageConst::PROPERTY_FORWARD_QUEUE_ID,
            Self::InnerBase => MessageConst::PROPERTY_INNER_BASE,
            Self::InnerMultiDispatch => MessageConst::PROPERTY_INNER_MULTI_DISPATCH,
            Self::InnerNum => MessageConst::PROPERTY_INNER_NUM,
            Self::Mq2Flag => MessageConst::PROPERTY_MQ2_FLAG,
            Self::ShardingKey => MessageConst::PROPERTY_SHARDING_KEY,
            Self::TransactionId => MessageConst::PROPERTY_TRANSACTION_ID,
            Self::TransactionCheckTimes => MessageConst::PROPERTY_TRANSACTION_CHECK_TIMES,
            Self::TransientGroupConfig => MessageConst::PROPERTY_TRANSIENT_GROUP_CONFIG,
            Self::TransientTopicConfig => MessageConst::PROPERTY_TRANSIENT_TOPIC_CONFIG,
            Self::TraceContext => MessageConst::PROPERTY_TRACE_CONTEXT,
        }
    }

    pub(crate) fn to_cheetah_string(self) -> CheetahString {
        CheetahString::from_static_str(self.as_str())
    }
}
