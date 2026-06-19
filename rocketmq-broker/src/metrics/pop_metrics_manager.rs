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

use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::pop::ack_msg::AckMsg;
use rocketmq_store::pop::pop_check_point::PopCheckPoint;

use super::pop_revive_message_type::PopReviveMessageType;

#[cfg(feature = "otel-metrics")]
pub(crate) use rocketmq_observability::metrics::pop_manager::BrokerAttributesSupplier;
#[cfg(feature = "otel-metrics")]
pub(crate) use rocketmq_observability::metrics::pop_manager::PopMetricsManager;

#[cfg(feature = "otel-metrics")]
#[inline]
pub(crate) fn inc_pop_revive_ack_put_count(ack_msg: &AckMsg, status: PutMessageStatus) {
    rocketmq_observability::metrics::pop_manager::inc_pop_revive_ack_put_count(
        ack_msg.consumer_group.as_str(),
        ack_msg.topic.as_str(),
        status.to_string(),
    );
}

#[cfg(not(feature = "otel-metrics"))]
#[inline]
pub(crate) fn inc_pop_revive_ack_put_count(_ack_msg: &AckMsg, _status: PutMessageStatus) {}

#[cfg(feature = "otel-metrics")]
#[inline]
pub(crate) fn inc_pop_revive_ck_put_count(check_point: &PopCheckPoint, status: PutMessageStatus) {
    rocketmq_observability::metrics::pop_manager::inc_pop_revive_ck_put_count(
        check_point.cid.as_str(),
        check_point.topic.as_str(),
        status.to_string(),
    );
}

#[cfg(not(feature = "otel-metrics"))]
#[inline]
pub(crate) fn inc_pop_revive_ck_put_count(_check_point: &PopCheckPoint, _status: PutMessageStatus) {}

#[cfg(feature = "otel-metrics")]
#[inline]
pub(crate) fn inc_pop_revive_put_count(
    group: &str,
    topic: &str,
    message_type: PopReviveMessageType,
    status: PutMessageStatus,
    num: u64,
) {
    rocketmq_observability::metrics::pop_manager::inc_pop_revive_put_count(
        group,
        topic,
        message_type,
        status.to_string(),
        num,
    );
}

#[cfg(not(feature = "otel-metrics"))]
#[inline]
pub(crate) fn inc_pop_revive_put_count(
    _group: &str,
    _topic: &str,
    _message_type: PopReviveMessageType,
    _status: PutMessageStatus,
    _num: u64,
) {
}

#[cfg(feature = "otel-metrics")]
#[inline]
pub(crate) fn inc_pop_revive_ack_get_count(ack_msg: &AckMsg, queue_id: i32) {
    rocketmq_observability::metrics::pop_manager::inc_pop_revive_ack_get_count(
        ack_msg.consumer_group.as_str(),
        ack_msg.topic.as_str(),
        queue_id,
    );
}

#[cfg(not(feature = "otel-metrics"))]
#[inline]
pub(crate) fn inc_pop_revive_ack_get_count(_ack_msg: &AckMsg, _queue_id: i32) {}

#[cfg(feature = "otel-metrics")]
#[inline]
pub(crate) fn inc_pop_revive_ck_get_count(check_point: &PopCheckPoint, queue_id: i32) {
    rocketmq_observability::metrics::pop_manager::inc_pop_revive_ck_get_count(
        check_point.cid.as_str(),
        check_point.topic.as_str(),
        queue_id,
    );
}

#[cfg(not(feature = "otel-metrics"))]
#[inline]
pub(crate) fn inc_pop_revive_ck_get_count(_check_point: &PopCheckPoint, _queue_id: i32) {}

#[cfg(feature = "otel-metrics")]
#[inline]
pub(crate) fn inc_pop_revive_get_count(
    group: &str,
    topic: &str,
    message_type: PopReviveMessageType,
    queue_id: i32,
    num: u64,
) {
    rocketmq_observability::metrics::pop_manager::inc_pop_revive_get_count(group, topic, message_type, queue_id, num);
}

#[cfg(not(feature = "otel-metrics"))]
#[inline]
pub(crate) fn inc_pop_revive_get_count(
    _group: &str,
    _topic: &str,
    _message_type: PopReviveMessageType,
    _queue_id: i32,
    _num: u64,
) {
}

#[cfg(feature = "otel-metrics")]
#[inline]
pub(crate) fn inc_pop_revive_retry_message_count(check_point: &PopCheckPoint, status: PutMessageStatus) {
    rocketmq_observability::metrics::pop_manager::inc_pop_revive_retry_message_count(
        check_point.cid.as_str(),
        check_point.topic.as_str(),
        status.to_string(),
    );
}

#[cfg(not(feature = "otel-metrics"))]
#[inline]
pub(crate) fn inc_pop_revive_retry_message_count(_check_point: &PopCheckPoint, _status: PutMessageStatus) {}

#[cfg(feature = "otel-metrics")]
#[inline]
pub(crate) fn record_pop_buffer_scan_time_consume(time_ms: u64) {
    rocketmq_observability::metrics::pop_manager::record_pop_buffer_scan_time_consume(time_ms);
}

#[cfg(not(feature = "otel-metrics"))]
#[inline]
pub(crate) fn record_pop_buffer_scan_time_consume(_time_ms: u64) {}
