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
use std::sync::OnceLock;

use cheetah_string::CheetahString;

use crate::common::message::MessageConst;

/// Separates the entries of encoded properties text.
const PROPERTY_SEPARATOR: char = '\u{2}';
/// Separates a key from its value in encoded properties text.
const NAME_VALUE_SEPARATOR: char = '\u{1}';

/// Type-safe message properties.
///
/// Properties decoded from a message keep their encoded `key\u{1}value\u{2}` text and are parsed
/// into a map only when the whole map is needed: [`Self::as_map`], [`Self::as_map_mut`],
/// [`Self::len`] or a removal. Until then, single-key reads scan the text and writes collect in a
/// small overlay that takes precedence over it, so a consumer that reads a few properties and sets
/// a few more never builds a map, and cloning the message copies no map. Lookups return the same
/// values the map would: entries with an empty key or value are skipped, and the last of
/// duplicate keys wins.
#[derive(Clone, Default)]
pub struct MessageProperties {
    /// Encoded properties not parsed into `map` yet; always valid UTF-8.
    encoded: Option<CheetahString>,
    /// Entries set since decoding, at most one per key, taking precedence over `encoded`.
    overlay: Vec<(CheetahString, CheetahString)>,
    /// The authoritative map once a mutable whole-map access or a removal built it.
    map: Option<HashMap<CheetahString, CheetahString>>,
    /// The map built for a shared whole-map access while `map` is `None`.
    parsed: OnceLock<HashMap<CheetahString, CheetahString>>,
}

impl fmt::Debug for MessageProperties {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MessageProperties")
            .field("inner", self.as_map())
            .finish()
    }
}

impl MessageProperties {
    /// Creates a new empty properties collection.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates properties from their encoded `key\u{1}value\u{2}` form, as stored in a message.
    ///
    /// Valid UTF-8 is kept as text and parsed on demand. Other input is parsed at once, replacing
    /// invalid sequences as `String::from_utf8_lossy` does.
    pub fn from_encoded(encoded: bytes::Bytes) -> Self {
        match CheetahString::try_copy_from_bytes(encoded) {
            Ok(text) if text.is_empty() => Self::default(),
            Ok(text) => Self {
                encoded: Some(text),
                ..Self::default()
            },
            Err(error) => Self::from_map(parse_lossy(&error.into_parts().0)),
        }
    }

    /// Gets a property value by key.
    #[inline]
    pub fn get(&self, key: MessagePropertyKey) -> Option<&str> {
        self.get_str(key.as_str())
    }

    /// Gets a property value by its key name without building the whole map.
    pub fn get_str(&self, key: &str) -> Option<&str> {
        if let Some(map) = self.built() {
            return map.get(key).map(CheetahString::as_str);
        }
        if let Some((_, value)) = self.overlay.iter().find(|(name, _)| name.as_str() == key) {
            return Some(value.as_str());
        }
        self.encoded.as_deref().and_then(|encoded| find_encoded(encoded, key))
    }

    /// Gets an owned copy of a property value without building the whole map.
    pub fn get_owned(&self, key: &str) -> Option<CheetahString> {
        if let Some(map) = self.built() {
            return map.get(key).cloned();
        }
        if let Some((_, value)) = self.overlay.iter().find(|(name, _)| name.as_str() == key) {
            return Some(value.clone());
        }
        self.encoded
            .as_deref()
            .and_then(|encoded| find_encoded(encoded, key))
            .map(CheetahString::from_slice)
    }

    /// Sets a property, replacing any earlier value of `key`.
    pub fn put(&mut self, key: CheetahString, value: CheetahString) {
        if self.map.is_some() || self.parsed.get().is_some() {
            self.map_mut().insert(key, value);
            return;
        }
        match self.overlay.iter_mut().find(|(name, _)| *name == key) {
            Some((_, existing)) => *existing = value,
            None => self.overlay.push((key, value)),
        }
    }

    /// Removes a property and returns its value.
    pub fn remove(&mut self, key: &str) -> Option<CheetahString> {
        self.map_mut().remove(key)
    }

    /// Sets a property value.
    pub(crate) fn insert(&mut self, key: MessagePropertyKey, value: impl Into<CheetahString>) {
        self.put(key.to_cheetah_string(), value.into());
    }

    /// Returns all properties as a map (for serialization and external crates).
    ///
    /// The first call on decoded properties parses them.
    #[inline]
    pub fn as_map(&self) -> &HashMap<CheetahString, CheetahString> {
        match &self.map {
            Some(map) => map,
            None => self
                .parsed
                .get_or_init(|| build_map(self.encoded.as_deref(), &self.overlay)),
        }
    }

    /// Returns all properties as a mutable map (for internal use).
    #[doc(hidden)]
    #[inline]
    pub fn as_map_mut(&mut self) -> &mut HashMap<CheetahString, CheetahString> {
        self.map_mut()
    }

    /// Creates MessageProperties from a HashMap.
    #[inline]
    pub fn from_map(map: HashMap<CheetahString, CheetahString>) -> Self {
        Self {
            map: Some(map),
            ..Self::default()
        }
    }

    /// Returns the number of properties.
    #[inline]
    pub fn len(&self) -> usize {
        self.as_map().len()
    }

    /// Returns true if there are no properties.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.as_map().is_empty()
    }

    /// The map, if a whole-map access already built it.
    fn built(&self) -> Option<&HashMap<CheetahString, CheetahString>> {
        self.map.as_ref().or_else(|| self.parsed.get())
    }

    /// The authoritative map, built first if needed; the encoded text and overlay are dropped.
    fn map_mut(&mut self) -> &mut HashMap<CheetahString, CheetahString> {
        if self.map.is_none() {
            let map = self
                .parsed
                .take()
                .unwrap_or_else(|| build_map(self.encoded.as_deref(), &self.overlay));
            self.encoded = None;
            self.overlay = Vec::new();
            self.map = Some(map);
        }
        self.map.get_or_insert_with(HashMap::new)
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
            .map(|s| s.eq_ignore_ascii_case("true"))
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

    /// Returns the message priority, or `-1` like Java when missing or invalid.
    pub fn priority(&self) -> i32 {
        self.get(MessagePropertyKey::Priority)
            .and_then(|s| s.parse().ok())
            .unwrap_or(-1)
    }
}

/// The entries of encoded properties text that a parsed map keeps: an empty key or value drops an
/// entry, as the Java decoder does.
fn encoded_entries(encoded: &str) -> impl Iterator<Item = (&str, &str)> {
    encoded.split(PROPERTY_SEPARATOR).filter_map(|entry| {
        let (key, value) = entry.split_once(NAME_VALUE_SEPARATOR)?;
        (!key.is_empty() && !value.is_empty()).then_some((key, value))
    })
}

/// The value the parsed map would hold for `key`: the last entry wins.
fn find_encoded<'a>(encoded: &'a str, key: &str) -> Option<&'a str> {
    encoded_entries(encoded)
        .filter(|(name, _)| *name == key)
        .last()
        .map(|(_, value)| value)
}

fn build_map(
    encoded: Option<&str>,
    overlay: &[(CheetahString, CheetahString)],
) -> HashMap<CheetahString, CheetahString> {
    let mut map =
        HashMap::with_capacity(overlay.len() + encoded.map_or(0, |text| text.matches(PROPERTY_SEPARATOR).count() + 1));
    if let Some(encoded) = encoded {
        for (key, value) in encoded_entries(encoded) {
            map.insert(CheetahString::from_slice(key), CheetahString::from_slice(value));
        }
    }
    for (key, value) in overlay {
        map.insert(key.clone(), value.clone());
    }
    map
}

/// Parses encoded properties that are not valid UTF-8, replacing invalid sequences.
fn parse_lossy(encoded: &[u8]) -> HashMap<CheetahString, CheetahString> {
    encoded
        .split(|byte| *byte == PROPERTY_SEPARATOR as u8)
        .filter_map(|entry| {
            let separator = entry.iter().position(|byte| *byte == NAME_VALUE_SEPARATOR as u8)?;
            if separator == 0 || separator + 1 >= entry.len() {
                return None;
            }
            let key = String::from_utf8_lossy(&entry[..separator]);
            let value = String::from_utf8_lossy(&entry[separator + 1..]);
            Some((CheetahString::from(key.as_ref()), CheetahString::from(value.as_ref())))
        })
        .collect()
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
    OriginGroup,
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
    Priority,
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
    TimerRollLabel,
    TimerOutMs,
    TimerDelUniqkey,
    TimerOriginalDeliverMs,
    TimerDeliveryToken,
    TimerId,
    TimerOwnerEpoch,
    TimerLane,
    TimerGeneration,
    TimerEngineType,
    TimerFormatVersion,
    TimerPolicyFingerprint,
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
    TransOffset,
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
            Self::OriginGroup => MessageConst::PROPERTY_ORIGIN_GROUP,
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
            Self::Priority => MessageConst::PROPERTY_PRIORITY,
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
            Self::TimerRollLabel => MessageConst::PROPERTY_TIMER_ROLL_LABEL,
            Self::TimerOutMs => MessageConst::PROPERTY_TIMER_OUT_MS,
            Self::TimerDelUniqkey => MessageConst::PROPERTY_TIMER_DEL_UNIQKEY,
            Self::TimerOriginalDeliverMs => MessageConst::PROPERTY_TIMER_ORIGINAL_DELIVER_MS,
            Self::TimerDeliveryToken => MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN,
            Self::TimerId => MessageConst::PROPERTY_TIMER_ID,
            Self::TimerOwnerEpoch => MessageConst::PROPERTY_TIMER_OWNER_EPOCH,
            Self::TimerLane => MessageConst::PROPERTY_TIMER_LANE,
            Self::TimerGeneration => MessageConst::PROPERTY_TIMER_GENERATION,
            Self::TimerEngineType => MessageConst::TIMER_ENGINE_TYPE,
            Self::TimerFormatVersion => MessageConst::PROPERTY_TIMER_FORMAT_VERSION,
            Self::TimerPolicyFingerprint => MessageConst::PROPERTY_TIMER_POLICY_FINGERPRINT,
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
            Self::TransOffset => MessageConst::PROPERTY_TRANS_OFFSET,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wait_store_msg_ok_matches_java_boolean_parse_semantics() {
        let mut properties = MessageProperties::new();
        assert!(properties.wait_store_msg_ok());

        properties.insert(MessagePropertyKey::WaitStoreMsgOk, "true");
        assert!(properties.wait_store_msg_ok());

        properties.insert(MessagePropertyKey::WaitStoreMsgOk, "TRUE");
        assert!(properties.wait_store_msg_ok());

        properties.insert(MessagePropertyKey::WaitStoreMsgOk, "false");
        assert!(!properties.wait_store_msg_ok());

        properties.insert(MessagePropertyKey::WaitStoreMsgOk, "yes");
        assert!(!properties.wait_store_msg_ok());
    }

    #[test]
    fn priority_matches_java_default_and_parse_semantics() {
        let mut properties = MessageProperties::new();
        assert_eq!(properties.priority(), -1);

        properties.insert(MessagePropertyKey::Priority, "7");
        assert_eq!(properties.priority(), 7);

        properties.insert(MessagePropertyKey::Priority, "invalid");
        assert_eq!(properties.priority(), -1);
    }

    /// The map the decoder built before properties were lazy.
    fn reference(encoded: &[u8]) -> HashMap<CheetahString, CheetahString> {
        parse_lossy(encoded)
    }

    #[test]
    fn encoded_lookups_and_maps_match_the_eager_decoder() {
        // A small alphabet makes separators, duplicates and empty parts frequent.
        const ALPHABET: &[u8] = b"ab\x01\x02";
        let mut state: u64 = 0x2545_f491_4f6c_dd1d;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..5_000 {
            let len = (next() % 24) as usize;
            let encoded: Vec<u8> = (0..len).map(|_| ALPHABET[(next() % 4) as usize]).collect();
            let expected = reference(&encoded);
            let properties = MessageProperties::from_encoded(bytes::Bytes::from(encoded.clone()));

            for key in ["a", "b", "ab", "ba", "aa", "bb", "aab", "c", ""] {
                assert_eq!(
                    properties.get_str(key),
                    expected.get(key).map(CheetahString::as_str),
                    "key {key:?} in {encoded:?}"
                );
            }
            assert!(properties.built().is_none(), "lookups must not build the map");
            assert_eq!(properties.as_map(), &expected, "map of {encoded:?}");
            assert_eq!(properties.len(), expected.len());
        }
    }

    #[test]
    fn invalid_utf8_is_parsed_like_from_utf8_lossy() {
        let encoded = b"KEY\x01\xffvalue\x02\xfeK\x01v".to_vec();
        let properties = MessageProperties::from_encoded(bytes::Bytes::from(encoded.clone()));
        assert_eq!(properties.as_map(), &reference(&encoded));
        assert_eq!(properties.get_str("KEY"), Some("\u{fffd}value"));
    }

    #[test]
    fn overlay_takes_precedence_until_and_after_the_map_is_built() {
        let mut properties = MessageProperties::from_encoded(bytes::Bytes::from_static(b"TAGS\x01a\x02KEYS\x01k"));
        properties.put(
            CheetahString::from_static_str("TAGS"),
            CheetahString::from_static_str("b"),
        );
        properties.put(
            CheetahString::from_static_str("MIN_OFFSET"),
            CheetahString::from_static_str("7"),
        );
        properties.put(
            CheetahString::from_static_str("MIN_OFFSET"),
            CheetahString::from_static_str("8"),
        );
        assert_eq!(properties.get_str("TAGS"), Some("b"));
        assert_eq!(
            properties.get_owned("MIN_OFFSET"),
            Some(CheetahString::from_static_str("8"))
        );
        assert_eq!(properties.get_str("KEYS"), Some("k"));
        assert!(properties.built().is_none());

        assert_eq!(properties.as_map().len(), 3);
        assert_eq!(properties.as_map().get("TAGS").map(CheetahString::as_str), Some("b"));
        properties.put(
            CheetahString::from_static_str("KEYS"),
            CheetahString::from_static_str("z"),
        );
        assert_eq!(properties.get_str("KEYS"), Some("z"));
        assert_eq!(properties.as_map().get("KEYS").map(CheetahString::as_str), Some("z"));

        assert_eq!(properties.remove("TAGS"), Some(CheetahString::from_static_str("b")));
        assert_eq!(properties.get_str("TAGS"), None);
        assert_eq!(properties.len(), 2);
    }

    #[test]
    fn clones_of_unparsed_properties_build_no_map() {
        let mut properties = MessageProperties::from_encoded(bytes::Bytes::from_static(b"TAGS\x01a"));
        properties.put(CheetahString::from_static_str("X"), CheetahString::from_static_str("1"));
        let clone = properties.clone();
        assert!(clone.built().is_none());
        assert_eq!(clone.get_str("TAGS"), Some("a"));
        assert_eq!(clone.get_str("X"), Some("1"));
    }

    #[test]
    fn debug_output_shows_the_properties_like_the_map_did() {
        let properties = MessageProperties::from_encoded(bytes::Bytes::from_static(b"TAGS\x01a"));
        let eager = MessageProperties::from_map(HashMap::from([(
            CheetahString::from_static_str("TAGS"),
            CheetahString::from_static_str("a"),
        )]));
        assert_eq!(format!("{properties:?}"), format!("{eager:?}"));
        assert_eq!(format!("{eager:?}"), r#"MessageProperties { inner: {"TAGS": "a"} }"#);
    }

    #[test]
    fn empty_encoded_properties_are_empty() {
        let properties = MessageProperties::from_encoded(bytes::Bytes::new());
        assert!(properties.is_empty());
        assert_eq!(properties.get_str("TAGS"), None);
    }

    #[test]
    fn protocol_property_keys_include_java_message_const_gaps() {
        assert_eq!(MessagePropertyKey::OriginGroup.as_str(), "ORIGIN_GROUP");
        assert_eq!(MessagePropertyKey::TransOffset.as_str(), "TRANS_OFFSET");
        assert_eq!(MessagePropertyKey::TimerRollLabel.as_str(), "TIMER_ROLL_LABEL");
    }
}
