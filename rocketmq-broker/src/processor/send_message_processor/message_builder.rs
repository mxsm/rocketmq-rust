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

use std::collections::HashMap;

use cheetah_string::CheetahString;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::timer_request::normalize_timer_request;
use rocketmq_model::common::message::timer_request::TimerPolicySnapshot;
use rocketmq_model::common::message::MessageConst;
use rocketmq_protocol::common::message::message_decoder::message_properties_to_string;
use rocketmq_protocol::common::message::message_decoder::string_to_message_properties;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_runtime::common::time_utils;
use rocketmq_store::TIMER_TOPIC;

pub(super) fn recall_handle_topic_and_timestamp(
    message: &MessageExtBrokerInner,
    timer_max_delay_sec: u64,
    timer_precision_ms: u64,
) -> Option<(CheetahString, i64)> {
    if let Some(real_topic) = message.property(MessageConst::PROPERTY_REAL_TOPIC) {
        let timestamp = message
            .property(MessageConst::PROPERTY_TIMER_ORIGINAL_DELIVER_MS)
            .and_then(|value| value.parse::<i64>().ok())
            .or_else(|| {
                message
                    .property(MessageConst::PROPERTY_TIMER_OUT_MS)
                    .and_then(|value| value.parse::<i64>().ok())
                    .and_then(|timer_out_ms| i64::try_from(timer_precision_ms).ok()?.checked_add(timer_out_ms))
            })?;
        return Some((real_topic, timestamp));
    }

    if message.message_ext_inner.message.delay_time_level() > 0 || message.topic().as_str() == TIMER_TOPIC {
        return None;
    }

    let max_delay_ms = timer_max_delay_sec.checked_mul(1000)?;
    let policy = TimerPolicySnapshot::try_new(timer_precision_ms, max_delay_ms).ok()?;
    let now = time_utils::current_millis();
    let normalized =
        normalize_timer_request(message.message_ext_inner.message.properties().as_map(), now, policy).ok()?;
    let timestamp = i64::try_from(normalized.original_deliver_ms).ok()?;
    Some((message.topic().clone(), timestamp))
}

pub(super) fn should_create_uniq_key(properties: &HashMap<CheetahString, CheetahString>) -> bool {
    properties
        .get(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
        .map(|uniq_key| uniq_key.is_empty())
        .unwrap_or(true)
}

pub(super) fn enrich_send_message_request_properties(
    request_header: &mut SendMessageRequestHeader,
    region_id: &str,
    trace_on: bool,
) -> HashMap<CheetahString, CheetahString> {
    let properties = string_to_message_properties(request_header.properties.as_ref());
    enrich_parsed_send_message_request_properties(request_header, properties, region_id, trace_on)
}

pub(super) fn enrich_parsed_send_message_request_properties(
    request_header: &mut SendMessageRequestHeader,
    mut properties: HashMap<CheetahString, CheetahString>,
    region_id: &str,
    trace_on: bool,
) -> HashMap<CheetahString, CheetahString> {
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_MSG_REGION),
        CheetahString::from_slice(region_id),
    );
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_TRACE_SWITCH),
        CheetahString::from_static_str(if trace_on { "true" } else { "false" }),
    );
    request_header.properties = Some(message_properties_to_string(&properties));
    properties
}

pub(super) fn clear_reserved_properties(
    request_header: &mut SendMessageRequestHeader,
    request_properties: &mut HashMap<CheetahString, CheetahString>,
) {
    let mut changed = request_properties.remove(MessageConst::PROPERTY_POP_CK).is_some();
    for property in [
        MessageConst::PROPERTY_TIMER_ORIGINAL_DELIVER_MS,
        MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN,
        MessageConst::PROPERTY_TIMER_ID,
        MessageConst::PROPERTY_TIMER_OWNER_EPOCH,
        MessageConst::PROPERTY_TIMER_LANE,
        MessageConst::PROPERTY_TIMER_GENERATION,
        MessageConst::TIMER_ENGINE_TYPE,
        MessageConst::PROPERTY_TIMER_FORMAT_VERSION,
        MessageConst::PROPERTY_TIMER_POLICY_FINGERPRINT,
    ] {
        changed |= request_properties.remove(property).is_some();
    }
    if changed {
        request_header.properties = Some(message_properties_to_string(request_properties));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_store::MessageStoreConfig;

    fn recall_handle_topic_and_timestamp_with_defaults(
        message: &MessageExtBrokerInner,
    ) -> Option<(CheetahString, i64)> {
        let config = MessageStoreConfig::default();
        recall_handle_topic_and_timestamp(message, config.timer_max_delay_sec, config.timer_precision_ms)
    }

    fn message_with_topic(topic: &str) -> MessageExtBrokerInner {
        let mut message = MessageExtBrokerInner::default();
        message
            .message_ext_inner
            .message
            .set_topic(CheetahString::from_slice(topic));
        message
    }

    #[test]
    fn recall_handle_timestamp_uses_transformed_timer_properties() {
        let mut message = message_with_topic(TIMER_TOPIC);
        message.message_ext_inner.message.properties_mut().as_map_mut().insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_OUT_MS),
            CheetahString::from_static_str("123000"),
        );
        message.message_ext_inner.message.properties_mut().as_map_mut().insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC),
            CheetahString::from_static_str("RecallTopic"),
        );

        let (topic, timestamp) = recall_handle_topic_and_timestamp_with_defaults(&message).expect("recall data");

        assert_eq!(topic, "RecallTopic");
        assert_eq!(timestamp, 124000);
    }

    #[test]
    fn recall_handle_timestamp_uses_absolute_deliver_time_before_store_transform() {
        let now = time_utils::current_millis();
        let deliver_ms = ((now + 60_000) / 1000) * 1000;
        let mut message = message_with_topic("RecallTopic");
        message.message_ext_inner.message.properties_mut().as_map_mut().insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELIVER_MS),
            CheetahString::from_string(deliver_ms.to_string()),
        );

        let (topic, timestamp) = recall_handle_topic_and_timestamp_with_defaults(&message).expect("recall data");

        assert_eq!(topic, "RecallTopic");
        assert_eq!(timestamp, i64::try_from(deliver_ms).unwrap());
    }

    #[test]
    fn recall_handle_timestamp_uses_timer_delay_ms_before_store_transform() {
        let mut message = message_with_topic("RecallTopic");
        message.message_ext_inner.message.properties_mut().as_map_mut().insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELAY_MS),
            CheetahString::from_static_str("60000"),
        );

        let now = time_utils::current_millis();
        let (topic, timestamp) = recall_handle_topic_and_timestamp_with_defaults(&message).expect("recall data");

        assert_eq!(topic, "RecallTopic");
        let min_expected = i64::try_from(now + 60_000).unwrap();
        let max_expected = i64::try_from(time_utils::current_millis() + 60_000).unwrap();
        assert!(
            (min_expected..=max_expected).contains(&timestamp),
            "timestamp {timestamp} should be derived from timer delay ms"
        );
    }

    #[test]
    fn recall_handle_timestamp_uses_timer_delay_sec_before_store_transform() {
        let mut message = message_with_topic("RecallTopic");
        message.message_ext_inner.message.properties_mut().as_map_mut().insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELAY_SEC),
            CheetahString::from_static_str("60"),
        );

        let now = time_utils::current_millis();
        let (topic, timestamp) = recall_handle_topic_and_timestamp_with_defaults(&message).expect("recall data");

        assert_eq!(topic, "RecallTopic");
        let min_expected = i64::try_from(now + 60_000).unwrap();
        let max_expected = i64::try_from(time_utils::current_millis() + 60_000).unwrap();
        assert!(
            (min_expected..=max_expected).contains(&timestamp),
            "timestamp {timestamp} should be derived from timer delay sec"
        );
    }

    #[test]
    fn should_create_uniq_key_only_when_missing_or_empty() {
        let mut properties = HashMap::new();
        assert!(should_create_uniq_key(&properties));

        properties.insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            CheetahString::new(),
        );
        assert!(should_create_uniq_key(&properties));

        properties.insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            CheetahString::from_static_str("java-uniq-id"),
        );
        assert!(!should_create_uniq_key(&properties));
    }

    #[test]
    fn enrich_send_message_request_properties_returns_map_and_updates_header() {
        let mut initial_properties = HashMap::new();
        initial_properties.insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_KEYS),
            CheetahString::from_static_str("order-1"),
        );
        initial_properties.insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            CheetahString::from_static_str("uniq-1"),
        );
        let mut request_header = SendMessageRequestHeader {
            properties: Some(message_properties_to_string(&initial_properties)),
            ..SendMessageRequestHeader::default()
        };

        let properties = enrich_send_message_request_properties(&mut request_header, "region-a", true);

        assert_eq!(
            properties.get(MessageConst::PROPERTY_KEYS),
            Some(&CheetahString::from_static_str("order-1"))
        );
        assert_eq!(
            properties.get(MessageConst::PROPERTY_MSG_REGION),
            Some(&CheetahString::from_static_str("region-a"))
        );
        assert_eq!(
            properties.get(MessageConst::PROPERTY_TRACE_SWITCH),
            Some(&CheetahString::from_static_str("true"))
        );
        let header_properties = string_to_message_properties(request_header.properties.as_ref());
        assert_eq!(header_properties, properties);
    }

    #[test]
    fn clear_reserved_properties_removes_pop_ck_from_header_and_reused_map() {
        let mut request_properties = HashMap::new();
        request_properties.insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_KEYS),
            CheetahString::from_static_str("order-1"),
        );
        request_properties.insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK),
            CheetahString::from_static_str("broker-only"),
        );
        for property in [
            MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN,
            MessageConst::PROPERTY_TIMER_ID,
            MessageConst::PROPERTY_TIMER_OWNER_EPOCH,
            MessageConst::PROPERTY_TIMER_LANE,
            MessageConst::PROPERTY_TIMER_GENERATION,
            MessageConst::TIMER_ENGINE_TYPE,
            MessageConst::PROPERTY_TIMER_FORMAT_VERSION,
            MessageConst::PROPERTY_TIMER_POLICY_FINGERPRINT,
        ] {
            request_properties.insert(
                CheetahString::from_static_str(property),
                CheetahString::from_static_str("spoofed"),
            );
        }
        let mut request_header = SendMessageRequestHeader {
            properties: Some(message_properties_to_string(&request_properties)),
            ..SendMessageRequestHeader::default()
        };

        clear_reserved_properties(&mut request_header, &mut request_properties);

        assert!(!request_properties.contains_key(MessageConst::PROPERTY_POP_CK));
        for property in [
            MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN,
            MessageConst::PROPERTY_TIMER_ID,
            MessageConst::PROPERTY_TIMER_OWNER_EPOCH,
            MessageConst::PROPERTY_TIMER_LANE,
            MessageConst::PROPERTY_TIMER_GENERATION,
            MessageConst::TIMER_ENGINE_TYPE,
            MessageConst::PROPERTY_TIMER_FORMAT_VERSION,
            MessageConst::PROPERTY_TIMER_POLICY_FINGERPRINT,
        ] {
            assert!(!request_properties.contains_key(property));
        }
        let header_properties = string_to_message_properties(request_header.properties.as_ref());
        assert_eq!(header_properties, request_properties);
        assert_eq!(
            header_properties.get(MessageConst::PROPERTY_KEYS),
            Some(&CheetahString::from_static_str("order-1"))
        );
    }
}
