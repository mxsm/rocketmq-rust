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

const PRODUCER_SEND: &str =
    include_str!("../../rocketmq-client/src/producer/producer_impl/default_mq_producer_impl/send.rs");
const BROKER_SEND: &str = include_str!("../src/processor/send_message_processor.rs");
const BROKER_SEND_CAPABILITY: &str = include_str!("../src/processor/send_message_processor/capability.rs");
const BROKER_COMPOSITION: &str = include_str!("../src/broker_runtime/composition.rs");
const BROKER_DATA_PLANE: &str = include_str!("../src/broker_runtime/data_plane.rs");
const BROKER_CONTROL_PLANE: &str = include_str!("../src/broker_runtime/control_plane.rs");
const COMMIT_LOG: &str = include_str!("../../rocketmq-store/src/log_file/commit_log.rs");
const APPEND_SEQUENCER: &str = include_str!("../../rocketmq-store/src/log_file/commit_log/append_sequencer.rs");

#[test]
fn producer_broker_store_append_and_flush_use_one_explicit_telemetry_chain() {
    assert!(PRODUCER_SEND.contains("inject_current_context_with_handle(telemetry_handle"));
    assert!(BROKER_SEND_CAPABILITY.contains("telemetry: TelemetryHandle"));
    assert!(BROKER_SEND.contains("trace::broker::receive_send_span("));

    assert!(BROKER_COMPOSITION.contains("StoreTelemetry::from_handle(&telemetry_handle)"));
    assert!(BROKER_DATA_PLANE.contains("state.store_telemetry.clone()"));
    assert!(!BROKER_CONTROL_PLANE.contains("metrics::store::init_global"));
    assert!(!BROKER_CONTROL_PLANE.contains("metrics::timer::init_global"));
    assert!(!BROKER_CONTROL_PLANE.contains("metrics::rocksdb::init_global"));
    assert!(!BROKER_CONTROL_PLANE.contains("metrics::tiered_store::init_global"));

    assert!(COMMIT_LOG.contains("telemetry_handle: rocketmq_observability::TelemetryHandle"));
    assert!(COMMIT_LOG.contains("trace::store::append_span(&self.telemetry_handle)"));
    assert!(COMMIT_LOG.contains("trace::record_message_properties_with_handle("));
    assert!(COMMIT_LOG.contains(".instrument(append_span)"));
    assert!(COMMIT_LOG.contains("trace::store::flush_span("));
    assert!(COMMIT_LOG.contains(".record_flush_latency("));
    assert!(APPEND_SEQUENCER.contains("store_metrics: rocketmq_observability::metrics::store::StoreMetricsRecorder"));
    assert!(APPEND_SEQUENCER.contains(".record_append_latency("));
}
