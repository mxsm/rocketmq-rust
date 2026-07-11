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

use rocketmq_common::common::attribute::topic_message_type::TopicMessageType;
use rocketmq_common::common::message::message_decoder;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;

#[cfg(feature = "otel-metrics")]
#[path = "broker_metrics_manager_impl.rs"]
mod owner_manager;
#[cfg(feature = "otel-metrics")]
pub(crate) use owner_manager::*;

/// Get message type from send message request header.
pub(crate) fn get_message_type(request_header: &SendMessageRequestHeader) -> TopicMessageType {
    let properties = if let Some(props) = &request_header.properties {
        message_decoder::string_to_message_properties(Some(props))
    } else {
        HashMap::new()
    };

    if let Some(tra_flag) = properties.get("TRAN_MSG") {
        if tra_flag.eq_ignore_ascii_case("true") {
            return TopicMessageType::Transaction;
        }
    }

    if properties.contains_key("SHARDING_KEY") {
        return TopicMessageType::Fifo;
    }

    if properties.contains_key("__STARTDELIVERTIME")
        || properties.contains_key("DELAY")
        || properties.contains_key("TIMER_DELIVER_MS")
        || properties.contains_key("TIMER_DELAY_SEC")
        || properties.contains_key("TIMER_DELAY_MS")
    {
        return TopicMessageType::Delay;
    }

    TopicMessageType::Normal
}

#[cfg(not(feature = "otel-metrics"))]
pub(crate) struct BrokerMetricsManager;

#[cfg(not(feature = "otel-metrics"))]
impl BrokerMetricsManager {
    pub(crate) fn try_global() -> Option<&'static Self> {
        None
    }

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
