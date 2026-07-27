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

#[cfg(feature = "otel-metrics")]
pub(crate) use rocketmq_observability::metrics::pop_manager::BrokerAttributesSupplier;
#[cfg(feature = "otel-metrics")]
pub(crate) use rocketmq_observability::metrics::pop_manager::PopMetricsManager;

#[cfg(not(feature = "otel-metrics"))]
pub(crate) struct PopMetricsManager;

#[cfg(not(feature = "otel-metrics"))]
impl PopMetricsManager {
    pub(crate) fn inc_pop_revive_ack_get_count(&self, _group: &str, _topic: &str, _queue_id: i32) {}

    pub(crate) fn inc_pop_revive_ck_get_count(&self, _group: &str, _topic: &str, _queue_id: i32) {}

    pub(crate) fn inc_pop_revive_retry_message_count(&self, _group: &str, _topic: &str, _status: impl Into<String>) {}
}
