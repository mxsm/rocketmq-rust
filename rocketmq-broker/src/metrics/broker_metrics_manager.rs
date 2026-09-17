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

use rocketmq_model::common::attribute::topic_message_type::TopicMessageType;
use rocketmq_protocol::common::message::message_decoder::NAME_VALUE_SEPARATOR;
use rocketmq_protocol::common::message::message_decoder::PROPERTY_SEPARATOR;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;

#[cfg(feature = "otel-metrics")]
pub(crate) use rocketmq_observability::metrics::broker_manager::*;

/// Get message type from send message request header.
pub(crate) fn get_message_type(request_header: &SendMessageRequestHeader) -> TopicMessageType {
    let Some(properties) = request_header.properties.as_deref() else {
        return TopicMessageType::Normal;
    };
    let mut transaction = None;
    let mut fifo = false;
    let mut delay = false;

    for property in properties.split(PROPERTY_SEPARATOR) {
        let Some((key, value)) = property.split_once(NAME_VALUE_SEPARATOR) else {
            continue;
        };
        if key.is_empty() || value.is_empty() {
            continue;
        }
        match key {
            "TRAN_MSG" => transaction = Some(value.eq_ignore_ascii_case("true")),
            "SHARDING_KEY" => fifo = true,
            "__STARTDELIVERTIME" | "DELAY" | "TIMER_DELIVER_MS" | "TIMER_DELAY_SEC" | "TIMER_DELAY_MS" => {
                delay = true;
            }
            _ => {}
        }
    }

    if transaction == Some(true) {
        return TopicMessageType::Transaction;
    }
    if fifo {
        return TopicMessageType::Fifo;
    }
    if delay {
        return TopicMessageType::Delay;
    }

    TopicMessageType::Normal
}

#[cfg(not(feature = "otel-metrics"))]
pub(crate) struct BrokerMetricsManager;

#[cfg(not(feature = "otel-metrics"))]
impl BrokerMetricsManager {
    pub(crate) fn record_messages_in_success(
        &self,
        _topic: &str,
        _message_type: &TopicMessageType,
        _num: u64,
        _bytes: u64,
        _message_size: u64,
        _is_system: bool,
    ) {
    }

    pub(crate) fn inc_messages_out_total(&self, _topic: &str, _consumer_group: &str, _num: u64, _is_retry: bool) {}

    pub(crate) fn inc_throughput_out_total(&self, _topic: &str, _consumer_group: &str, _bytes: u64, _is_retry: bool) {}

    pub(crate) fn record_send_message_latency(&self, _topic: &str, _latency_ms: u64) {}

    pub(crate) fn record_topic_create_time(&self, _time_ms: u64) {}

    pub(crate) fn record_consumer_group_create_time(&self, _time_ms: u64) {}

    pub(crate) fn inc_send_to_dlq_messages(&self, _topic: &str, _consumer_group: &str, _num: u64) {}

    pub(crate) fn inc_commit_messages(&self, _topic: &str, _num: u64) {}

    pub(crate) fn inc_rollback_messages(&self, _topic: &str, _num: u64) {}

    pub(crate) fn record_transaction_finish_latency(&self, _topic: &str, _latency_ms: u64) {}

    pub(crate) fn register_auth_observable_gauge<F, T>(&self, _auth_snapshot_fn: F)
    where
        F: Fn() -> Option<T> + Send + Sync + 'static,
    {
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use rocketmq_model::common::attribute::topic_message_type::TopicMessageType;
    use rocketmq_protocol::common::message::message_decoder::NAME_VALUE_SEPARATOR;
    use rocketmq_protocol::common::message::message_decoder::PROPERTY_SEPARATOR;
    use rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;

    use super::get_message_type;

    fn request_header(properties: &[(&str, &str)]) -> SendMessageRequestHeader {
        let mut encoded = String::new();
        for (key, value) in properties {
            encoded.push_str(key);
            encoded.push(NAME_VALUE_SEPARATOR);
            encoded.push_str(value);
            encoded.push(PROPERTY_SEPARATOR);
        }
        SendMessageRequestHeader {
            properties: Some(CheetahString::from_string(encoded)),
            ..Default::default()
        }
    }

    #[test]
    fn message_type_classification_preserves_priority_and_all_outcomes() {
        assert_eq!(
            get_message_type(&request_header(&[("TRAN_MSG", "TrUe"), ("SHARDING_KEY", "queue")])),
            TopicMessageType::Transaction
        );
        assert_eq!(
            get_message_type(&request_header(&[("SHARDING_KEY", "queue"), ("DELAY", "3")])),
            TopicMessageType::Fifo
        );
        for key in [
            "__STARTDELIVERTIME",
            "DELAY",
            "TIMER_DELIVER_MS",
            "TIMER_DELAY_SEC",
            "TIMER_DELAY_MS",
        ] {
            assert_eq!(
                get_message_type(&request_header(&[(key, "1")])),
                TopicMessageType::Delay
            );
        }
        assert_eq!(
            get_message_type(&request_header(&[("KEYS", "value")])),
            TopicMessageType::Normal
        );
        assert_eq!(
            get_message_type(&request_header(&[("TRAN_MSG", "true"), ("TRAN_MSG", "false")])),
            TopicMessageType::Normal
        );
        assert_eq!(
            get_message_type(&request_header(&[("SHARDING_KEY", "")])),
            TopicMessageType::Normal
        );
        assert_eq!(
            get_message_type(&SendMessageRequestHeader::default()),
            TopicMessageType::Normal
        );
    }

    #[test]
    fn message_type_classification_does_not_match_keys_inside_values() {
        let header = request_header(&[("KEYS", "TRAN_MSG\u{1}true\u{2}SHARDING_KEY")]);

        assert_eq!(get_message_type(&header), TopicMessageType::Normal);
    }

    #[test]
    fn broker_metrics_sources_have_no_global_manager_access() {
        let manager_source = include_str!("../../../rocketmq-observability/src/metrics/broker_manager.rs");
        for forbidden in [
            concat!("static ", "BROKER_METRICS_MANAGER"),
            concat!("static ", "LABEL_MAP"),
            concat!("static ", "ATTRIBUTES_BUILDER_SUPPLIER"),
            concat!("BrokerMetricsManager::", "try_global"),
            concat!("new_attributes", "_builder"),
        ] {
            assert!(
                !manager_source.contains(forbidden),
                "Broker metrics manager must remain instance-scoped: {forbidden}"
            );
        }

        for source in [
            include_str!("../broker_runtime/control_plane.rs"),
            include_str!("../processor/default_pull_message_result_handler.rs"),
            include_str!("../processor/end_transaction_processor.rs"),
            include_str!("../processor/send_message_processor.rs"),
            include_str!("../subscription/manager/subscription_group_manager.rs"),
            include_str!("../topic/manager/topic_config_manager.rs"),
        ] {
            let global_access = concat!("BrokerMetricsManager::", "try_global");
            assert!(
                !source.contains(global_access),
                "Broker business metrics must be constructor-injected"
            );
        }
    }
}
