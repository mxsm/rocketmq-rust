use std::collections::HashMap;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::MessageDecoder::message_properties_to_string;
use rocketmq_common::MessageDecoder::string_to_message_properties;
use rocketmq_common::TimeUtils;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::timer::timer_message_store::TIMER_TOPIC;

pub(super) fn recall_handle_topic_and_timestamp(
    message: &MessageExtBrokerInner,
    message_store_config: &MessageStoreConfig,
) -> Option<(CheetahString, i64)> {
    if let (Some(timestamp_str), Some(real_topic)) = (
        message.property(MessageConst::PROPERTY_TIMER_OUT_MS),
        message.property(MessageConst::PROPERTY_REAL_TOPIC),
    ) {
        let timestamp = timestamp_str.parse::<i64>().ok()?.checked_add(1)?;
        return Some((real_topic, timestamp));
    }

    if message.message_ext_inner.message.delay_time_level() > 0 || message.topic().as_str() == TIMER_TOPIC {
        return None;
    }

    let now = TimeUtils::current_millis();
    let deliver_ms = if let Some(delay_sec) = message.property(MessageConst::PROPERTY_TIMER_DELAY_SEC) {
        now.checked_add(delay_sec.parse::<u64>().ok()?.checked_mul(1000)?)?
    } else if let Some(delay_ms) = message.property(MessageConst::PROPERTY_TIMER_DELAY_MS) {
        now.checked_add(delay_ms.parse::<u64>().ok()?)?
    } else {
        message
            .property(MessageConst::PROPERTY_TIMER_DELIVER_MS)?
            .parse::<u64>()
            .ok()?
    };
    if deliver_ms <= now {
        return None;
    }

    let max_delay_ms = message_store_config.timer_max_delay_sec.checked_mul(1000)?;
    if deliver_ms.saturating_sub(now) > max_delay_ms {
        return None;
    }

    let precision_ms = message_store_config.timer_precision_ms;
    let timer_out_ms = if precision_ms == 0 {
        deliver_ms
    } else if deliver_ms % precision_ms == 0 {
        deliver_ms.saturating_sub(precision_ms)
    } else {
        (deliver_ms / precision_ms) * precision_ms
    };
    let timestamp = i64::try_from(timer_out_ms).ok()?.checked_add(1)?;
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
    let mut properties = string_to_message_properties(request_header.properties.as_ref());
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
    if request_properties.remove(MessageConst::PROPERTY_POP_CK).is_some() {
        request_header.properties = Some(message_properties_to_string(request_properties));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        let (topic, timestamp) =
            recall_handle_topic_and_timestamp(&message, &MessageStoreConfig::default()).expect("recall data");

        assert_eq!(topic, "RecallTopic");
        assert_eq!(timestamp, 123001);
    }

    #[test]
    fn recall_handle_timestamp_uses_absolute_deliver_time_before_store_transform() {
        let now = TimeUtils::current_millis();
        let deliver_ms = ((now + 60_000) / 1000) * 1000;
        let mut message = message_with_topic("RecallTopic");
        message.message_ext_inner.message.properties_mut().as_map_mut().insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELIVER_MS),
            CheetahString::from_string(deliver_ms.to_string()),
        );

        let (topic, timestamp) =
            recall_handle_topic_and_timestamp(&message, &MessageStoreConfig::default()).expect("recall data");

        assert_eq!(topic, "RecallTopic");
        assert_eq!(timestamp, i64::try_from(deliver_ms - 1000).unwrap() + 1);
    }

    #[test]
    fn recall_handle_timestamp_uses_timer_delay_ms_before_store_transform() {
        let mut message = message_with_topic("RecallTopic");
        message.message_ext_inner.message.properties_mut().as_map_mut().insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELAY_MS),
            CheetahString::from_static_str("60000"),
        );

        let now = TimeUtils::current_millis();
        let (topic, timestamp) =
            recall_handle_topic_and_timestamp(&message, &MessageStoreConfig::default()).expect("recall data");

        assert_eq!(topic, "RecallTopic");
        let min_expected = i64::try_from(((now + 60_000) / 1000) * 1000).unwrap();
        let max_expected = i64::try_from(((TimeUtils::current_millis() + 60_000) / 1000) * 1000).unwrap() + 1;
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

        let now = TimeUtils::current_millis();
        let (topic, timestamp) =
            recall_handle_topic_and_timestamp(&message, &MessageStoreConfig::default()).expect("recall data");

        assert_eq!(topic, "RecallTopic");
        let min_expected = i64::try_from(((now + 60_000) / 1000) * 1000).unwrap();
        let max_expected = i64::try_from(((TimeUtils::current_millis() + 60_000) / 1000) * 1000).unwrap() + 1;
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
        let mut request_header = SendMessageRequestHeader {
            properties: Some(message_properties_to_string(&request_properties)),
            ..SendMessageRequestHeader::default()
        };

        clear_reserved_properties(&mut request_header, &mut request_properties);

        assert!(!request_properties.contains_key(MessageConst::PROPERTY_POP_CK));
        let header_properties = string_to_message_properties(request_header.properties.as_ref());
        assert_eq!(header_properties, request_properties);
        assert_eq!(
            header_properties.get(MessageConst::PROPERTY_KEYS),
            Some(&CheetahString::from_static_str("order-1"))
        );
    }
}
