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

#[inline]
pub(crate) fn record_append_latency(latency_ms: u64) {
    rocketmq_observability::metrics::store::record_append_latency(latency_ms);
}

#[inline]
pub(crate) fn record_flush_latency(latency_ms: u64) {
    rocketmq_observability::metrics::store::record_flush_latency(latency_ms);
}

#[inline]
pub(crate) fn record_dispatch_latency(latency_ms: u64) {
    rocketmq_observability::metrics::store::record_dispatch_latency(latency_ms);
}

#[inline]
pub(crate) fn record_delay_message_latency(latency_seconds: u64, topic: Option<&str>) {
    rocketmq_observability::metrics::store::record_delay_message_latency_with_topic(latency_seconds, topic);
}

#[inline]
pub(crate) fn record_timer_enqueue_total(topic: Option<&str>) {
    rocketmq_observability::metrics::timer::record_enqueue_total(topic);
}

#[inline]
pub(crate) fn record_timer_dequeue_total(topic: &str) {
    rocketmq_observability::metrics::timer::record_dequeue_total(topic);
}
