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

use crate::semantic::labels;
use crate::semantic::metrics;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Counter,
    Histogram,
    ObservableGauge,
    UpDownCounter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricSource {
    Broker,
    Pop,
    Remoting,
    Store,
    Timer,
    RocksDb,
    TieredStore,
    Proxy,
    Controller,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricDescriptor {
    pub name: &'static str,
    pub kind: MetricKind,
    pub unit: &'static str,
    pub labels: &'static [&'static str],
    pub source: MetricSource,
}

const BASE_NODE_LABELS: &[&str] = &[labels::CLUSTER, labels::NODE_TYPE, labels::NODE_ID];
const BROKER_TOPIC_LABELS: &[&str] = &[labels::CLUSTER, labels::NODE_TYPE, labels::NODE_ID, labels::TOPIC];
const BROKER_IN_LABELS: &[&str] = &[
    labels::CLUSTER,
    labels::NODE_TYPE,
    labels::NODE_ID,
    labels::TOPIC,
    labels::MESSAGE_TYPE,
    labels::IS_SYSTEM,
];
const BROKER_OUT_LABELS: &[&str] = &[
    labels::CLUSTER,
    labels::NODE_TYPE,
    labels::NODE_ID,
    labels::TOPIC,
    labels::CONSUMER_GROUP,
    labels::IS_RETRY,
    labels::IS_SYSTEM,
];
const BROKER_CONNECTION_LABELS: &[&str] = &[
    labels::CLUSTER,
    labels::NODE_TYPE,
    labels::NODE_ID,
    labels::LANGUAGE,
    labels::VERSION,
    labels::PROTOCOL_TYPE,
];
const BROKER_CONSUMER_CONNECTION_LABELS: &[&str] = &[
    labels::CLUSTER,
    labels::NODE_TYPE,
    labels::NODE_ID,
    labels::CONSUMER_GROUP,
    labels::LANGUAGE,
    labels::VERSION,
    labels::PROTOCOL_TYPE,
    labels::CONSUME_MODE,
    labels::IS_SYSTEM,
];
const POP_REVIVE_IN_LABELS: &[&str] = &[
    labels::CLUSTER,
    labels::NODE_TYPE,
    labels::NODE_ID,
    labels::CONSUMER_GROUP,
    labels::TOPIC,
    labels::REVIVE_MESSAGE_TYPE,
    labels::PUT_STATUS,
];
const POP_REVIVE_OUT_LABELS: &[&str] = &[
    labels::CLUSTER,
    labels::NODE_TYPE,
    labels::NODE_ID,
    labels::CONSUMER_GROUP,
    labels::TOPIC,
    labels::QUEUE_ID,
    labels::REVIVE_MESSAGE_TYPE,
];
const POP_QUEUE_LABELS: &[&str] = &[labels::CLUSTER, labels::NODE_TYPE, labels::NODE_ID, labels::QUEUE_ID];
const REMOTING_RPC_LABELS: &[&str] = &[
    labels::PROTOCOL_TYPE,
    labels::REQUEST_CODE,
    labels::RESPONSE_CODE,
    labels::IS_LONG_POLLING,
    labels::RESULT,
];
const STORE_STORAGE_LABELS: &[&str] = &[labels::STORAGE_TYPE, labels::STORAGE_MEDIUM];
const STORE_TOPIC_LABELS: &[&str] = &[labels::STORAGE_TYPE, labels::STORAGE_MEDIUM, labels::TOPIC];
const TIMER_BOUND_LABELS: &[&str] = &[
    labels::STORAGE_TYPE,
    labels::STORAGE_MEDIUM,
    labels::TIMER_BOUND_SECONDS,
];
const ROCKSDB_STORAGE_LABELS: &[&str] = &[labels::STORAGE_TYPE, labels::STORAGE_MEDIUM, labels::TYPE];
const TIERED_PROVIDER_LABELS: &[&str] = &[labels::OPERATION, labels::SUCCESS, labels::PATH];
const TIERED_MESSAGE_OUT_LABELS: &[&str] = &[labels::TOPIC, labels::GROUP];
const TIERED_DISPATCH_LABELS: &[&str] = &[labels::TOPIC, labels::QUEUE_ID, labels::FILE_TYPE];
const PROXY_UP_LABELS: &[&str] = &[labels::NODE_TYPE, labels::CLUSTER, labels::NODE_ID, labels::PROXY_MODE];
const CONTROLLER_BASE_LABELS: &[&str] = &[labels::ADDRESS, labels::GROUP, labels::PEER_ID];
const CONTROLLER_REQUEST_LABELS: &[&str] = &[
    labels::ADDRESS,
    labels::GROUP,
    labels::PEER_ID,
    labels::REQUEST_TYPE,
    labels::REQUEST_HANDLE_STATUS,
];
const CONTROLLER_DLEDGER_LABELS: &[&str] = &[
    labels::ADDRESS,
    labels::GROUP,
    labels::PEER_ID,
    labels::DLEDGER_OPERATION,
    labels::DLEDGER_OPERATION_STATUS,
];
const CONTROLLER_ELECTION_LABELS: &[&str] = &[labels::ADDRESS, labels::GROUP, labels::PEER_ID, labels::ELECTION_RESULT];

pub const JAVA_METRICS: &[MetricDescriptor] = &[
    MetricDescriptor {
        name: metrics::PROCESSOR_WATERMARK,
        kind: MetricKind::ObservableGauge,
        unit: "{request}",
        labels: &[labels::CLUSTER, labels::NODE_TYPE, labels::NODE_ID, labels::PROCESSOR],
        source: MetricSource::Broker,
    },
    MetricDescriptor {
        name: metrics::BROKER_PERMISSION,
        kind: MetricKind::ObservableGauge,
        unit: "1",
        labels: BASE_NODE_LABELS,
        source: MetricSource::Broker,
    },
    MetricDescriptor {
        name: metrics::TOPIC_NUMBER,
        kind: MetricKind::ObservableGauge,
        unit: "{topic}",
        labels: BASE_NODE_LABELS,
        source: MetricSource::Broker,
    },
    MetricDescriptor {
        name: metrics::CONSUMER_GROUP_NUMBER,
        kind: MetricKind::ObservableGauge,
        unit: "{consumer_group}",
        labels: BASE_NODE_LABELS,
        source: MetricSource::Broker,
    },
    MetricDescriptor {
        name: metrics::MESSAGES_IN_TOTAL,
        kind: MetricKind::Counter,
        unit: "{message}",
        labels: BROKER_IN_LABELS,
        source: MetricSource::Broker,
    },
    MetricDescriptor {
        name: metrics::MESSAGES_OUT_TOTAL,
        kind: MetricKind::Counter,
        unit: "{message}",
        labels: BROKER_OUT_LABELS,
        source: MetricSource::Broker,
    },
    MetricDescriptor {
        name: metrics::THROUGHPUT_IN_TOTAL,
        kind: MetricKind::Counter,
        unit: "By",
        labels: BROKER_TOPIC_LABELS,
        source: MetricSource::Broker,
    },
    MetricDescriptor {
        name: metrics::THROUGHPUT_OUT_TOTAL,
        kind: MetricKind::Counter,
        unit: "By",
        labels: BROKER_OUT_LABELS,
        source: MetricSource::Broker,
    },
    MetricDescriptor {
        name: metrics::MESSAGE_SIZE,
        kind: MetricKind::Histogram,
        unit: "By",
        labels: &[
            labels::CLUSTER,
            labels::NODE_TYPE,
            labels::NODE_ID,
            labels::TOPIC,
            labels::MESSAGE_TYPE,
        ],
        source: MetricSource::Broker,
    },
    MetricDescriptor {
        name: metrics::TOPIC_CREATE_EXECUTION_TIME,
        kind: MetricKind::Histogram,
        unit: "ms",
        labels: BASE_NODE_LABELS,
        source: MetricSource::Broker,
    },
    MetricDescriptor {
        name: metrics::CONSUMER_GROUP_CREATE_EXECUTION_TIME,
        kind: MetricKind::Histogram,
        unit: "ms",
        labels: BASE_NODE_LABELS,
        source: MetricSource::Broker,
    },
    MetricDescriptor {
        name: metrics::PRODUCER_CONNECTIONS,
        kind: MetricKind::ObservableGauge,
        unit: "{connection}",
        labels: BROKER_CONNECTION_LABELS,
        source: MetricSource::Broker,
    },
    MetricDescriptor {
        name: metrics::CONSUMER_CONNECTIONS,
        kind: MetricKind::ObservableGauge,
        unit: "{connection}",
        labels: BROKER_CONSUMER_CONNECTION_LABELS,
        source: MetricSource::Broker,
    },
    MetricDescriptor {
        name: metrics::CONSUMER_LAG_MESSAGES,
        kind: MetricKind::ObservableGauge,
        unit: "{message}",
        labels: BROKER_OUT_LABELS,
        source: MetricSource::Broker,
    },
    MetricDescriptor {
        name: metrics::CONSUMER_LAG_LATENCY,
        kind: MetricKind::ObservableGauge,
        unit: "ms",
        labels: BROKER_OUT_LABELS,
        source: MetricSource::Broker,
    },
    MetricDescriptor {
        name: metrics::CONSUMER_INFLIGHT_MESSAGES,
        kind: MetricKind::ObservableGauge,
        unit: "{message}",
        labels: BROKER_OUT_LABELS,
        source: MetricSource::Broker,
    },
    MetricDescriptor {
        name: metrics::CONSUMER_QUEUEING_LATENCY,
        kind: MetricKind::ObservableGauge,
        unit: "ms",
        labels: BROKER_OUT_LABELS,
        source: MetricSource::Broker,
    },
    MetricDescriptor {
        name: metrics::CONSUMER_READY_MESSAGES,
        kind: MetricKind::ObservableGauge,
        unit: "{message}",
        labels: BROKER_OUT_LABELS,
        source: MetricSource::Broker,
    },
    MetricDescriptor {
        name: metrics::SEND_TO_DLQ_MESSAGES_TOTAL,
        kind: MetricKind::Counter,
        unit: "{message}",
        labels: BROKER_OUT_LABELS,
        source: MetricSource::Broker,
    },
    MetricDescriptor {
        name: metrics::COMMIT_MESSAGES_TOTAL,
        kind: MetricKind::Counter,
        unit: "{message}",
        labels: BROKER_TOPIC_LABELS,
        source: MetricSource::Broker,
    },
    MetricDescriptor {
        name: metrics::ROLLBACK_MESSAGES_TOTAL,
        kind: MetricKind::Counter,
        unit: "{message}",
        labels: BROKER_TOPIC_LABELS,
        source: MetricSource::Broker,
    },
    MetricDescriptor {
        name: metrics::FINISH_MESSAGE_LATENCY,
        kind: MetricKind::Histogram,
        unit: "ms",
        labels: BROKER_TOPIC_LABELS,
        source: MetricSource::Broker,
    },
    MetricDescriptor {
        name: metrics::HALF_MESSAGES,
        kind: MetricKind::ObservableGauge,
        unit: "{message}",
        labels: BROKER_TOPIC_LABELS,
        source: MetricSource::Broker,
    },
    MetricDescriptor {
        name: metrics::POP_BUFFER_SCAN_TIME_CONSUME,
        kind: MetricKind::Histogram,
        unit: "ms",
        labels: BASE_NODE_LABELS,
        source: MetricSource::Pop,
    },
    MetricDescriptor {
        name: metrics::POP_REVIVE_IN_MESSAGE_TOTAL,
        kind: MetricKind::Counter,
        unit: "{message}",
        labels: POP_REVIVE_IN_LABELS,
        source: MetricSource::Pop,
    },
    MetricDescriptor {
        name: metrics::POP_REVIVE_OUT_MESSAGE_TOTAL,
        kind: MetricKind::Counter,
        unit: "{message}",
        labels: POP_REVIVE_OUT_LABELS,
        source: MetricSource::Pop,
    },
    MetricDescriptor {
        name: metrics::POP_REVIVE_RETRY_MESSAGES_TOTAL,
        kind: MetricKind::Counter,
        unit: "{message}",
        labels: POP_REVIVE_IN_LABELS,
        source: MetricSource::Pop,
    },
    MetricDescriptor {
        name: metrics::POP_REVIVE_LAG,
        kind: MetricKind::ObservableGauge,
        unit: "{message}",
        labels: POP_QUEUE_LABELS,
        source: MetricSource::Pop,
    },
    MetricDescriptor {
        name: metrics::POP_REVIVE_LATENCY,
        kind: MetricKind::ObservableGauge,
        unit: "ms",
        labels: POP_QUEUE_LABELS,
        source: MetricSource::Pop,
    },
    MetricDescriptor {
        name: metrics::POP_OFFSET_BUFFER_SIZE,
        kind: MetricKind::ObservableGauge,
        unit: "{offset}",
        labels: BASE_NODE_LABELS,
        source: MetricSource::Pop,
    },
    MetricDescriptor {
        name: metrics::POP_CHECKPOINT_BUFFER_SIZE,
        kind: MetricKind::ObservableGauge,
        unit: "{checkpoint}",
        labels: BASE_NODE_LABELS,
        source: MetricSource::Pop,
    },
    MetricDescriptor {
        name: metrics::RPC_LATENCY,
        kind: MetricKind::Histogram,
        unit: "ms",
        labels: REMOTING_RPC_LABELS,
        source: MetricSource::Remoting,
    },
    MetricDescriptor {
        name: metrics::STORAGE_SIZE,
        kind: MetricKind::ObservableGauge,
        unit: "By",
        labels: STORE_STORAGE_LABELS,
        source: MetricSource::Store,
    },
    MetricDescriptor {
        name: metrics::STORAGE_FLUSH_BEHIND_BYTES,
        kind: MetricKind::ObservableGauge,
        unit: "By",
        labels: STORE_STORAGE_LABELS,
        source: MetricSource::Store,
    },
    MetricDescriptor {
        name: metrics::STORAGE_DISPATCH_BEHIND_BYTES,
        kind: MetricKind::ObservableGauge,
        unit: "By",
        labels: STORE_STORAGE_LABELS,
        source: MetricSource::Store,
    },
    MetricDescriptor {
        name: metrics::STORAGE_MESSAGE_RESERVE_TIME,
        kind: MetricKind::ObservableGauge,
        unit: "ms",
        labels: STORE_STORAGE_LABELS,
        source: MetricSource::Store,
    },
    MetricDescriptor {
        name: metrics::DELAY_MESSAGE_LATENCY,
        kind: MetricKind::Histogram,
        unit: "seconds",
        labels: STORE_TOPIC_LABELS,
        source: MetricSource::Store,
    },
    MetricDescriptor {
        name: metrics::TIMER_ENQUEUE_LAG,
        kind: MetricKind::ObservableGauge,
        unit: "ms",
        labels: TIMER_BOUND_LABELS,
        source: MetricSource::Timer,
    },
    MetricDescriptor {
        name: metrics::TIMER_ENQUEUE_LATENCY,
        kind: MetricKind::ObservableGauge,
        unit: "ms",
        labels: TIMER_BOUND_LABELS,
        source: MetricSource::Timer,
    },
    MetricDescriptor {
        name: metrics::TIMER_DEQUEUE_LAG,
        kind: MetricKind::ObservableGauge,
        unit: "ms",
        labels: TIMER_BOUND_LABELS,
        source: MetricSource::Timer,
    },
    MetricDescriptor {
        name: metrics::TIMER_DEQUEUE_LATENCY,
        kind: MetricKind::ObservableGauge,
        unit: "ms",
        labels: TIMER_BOUND_LABELS,
        source: MetricSource::Timer,
    },
    MetricDescriptor {
        name: metrics::TIMING_MESSAGES,
        kind: MetricKind::ObservableGauge,
        unit: "{message}",
        labels: TIMER_BOUND_LABELS,
        source: MetricSource::Timer,
    },
    MetricDescriptor {
        name: metrics::TIMER_ENQUEUE_TOTAL,
        kind: MetricKind::Counter,
        unit: "{message}",
        labels: STORE_TOPIC_LABELS,
        source: MetricSource::Timer,
    },
    MetricDescriptor {
        name: metrics::TIMER_DEQUEUE_TOTAL,
        kind: MetricKind::Counter,
        unit: "{message}",
        labels: STORE_TOPIC_LABELS,
        source: MetricSource::Timer,
    },
    MetricDescriptor {
        name: metrics::TIMER_MESSAGE_SNAPSHOT,
        kind: MetricKind::ObservableGauge,
        unit: "{message}",
        labels: TIMER_BOUND_LABELS,
        source: MetricSource::Timer,
    },
    MetricDescriptor {
        name: metrics::ROCKSDB_BYTES_WRITTEN,
        kind: MetricKind::ObservableGauge,
        unit: "By",
        labels: ROCKSDB_STORAGE_LABELS,
        source: MetricSource::RocksDb,
    },
    MetricDescriptor {
        name: metrics::ROCKSDB_BYTES_READ,
        kind: MetricKind::ObservableGauge,
        unit: "By",
        labels: ROCKSDB_STORAGE_LABELS,
        source: MetricSource::RocksDb,
    },
    MetricDescriptor {
        name: metrics::ROCKSDB_TIMES_WRITTEN_SELF,
        kind: MetricKind::ObservableGauge,
        unit: "{operation}",
        labels: ROCKSDB_STORAGE_LABELS,
        source: MetricSource::RocksDb,
    },
    MetricDescriptor {
        name: metrics::ROCKSDB_TIMES_WRITTEN_OTHER,
        kind: MetricKind::ObservableGauge,
        unit: "{operation}",
        labels: ROCKSDB_STORAGE_LABELS,
        source: MetricSource::RocksDb,
    },
    MetricDescriptor {
        name: metrics::ROCKSDB_RATE_CACHE_HIT,
        kind: MetricKind::ObservableGauge,
        unit: "1",
        labels: ROCKSDB_STORAGE_LABELS,
        source: MetricSource::RocksDb,
    },
    MetricDescriptor {
        name: metrics::ROCKSDB_TIMES_COMPRESSED,
        kind: MetricKind::ObservableGauge,
        unit: "{operation}",
        labels: ROCKSDB_STORAGE_LABELS,
        source: MetricSource::RocksDb,
    },
    MetricDescriptor {
        name: metrics::ROCKSDB_READ_AMPLIFICATION_BYTES,
        kind: MetricKind::ObservableGauge,
        unit: "By",
        labels: ROCKSDB_STORAGE_LABELS,
        source: MetricSource::RocksDb,
    },
    MetricDescriptor {
        name: metrics::ROCKSDB_TIMES_READ,
        kind: MetricKind::ObservableGauge,
        unit: "{operation}",
        labels: ROCKSDB_STORAGE_LABELS,
        source: MetricSource::RocksDb,
    },
    MetricDescriptor {
        name: metrics::TIERED_STORE_MESSAGES_DISPATCH_TOTAL,
        kind: MetricKind::Counter,
        unit: "{message}",
        labels: TIERED_DISPATCH_LABELS,
        source: MetricSource::TieredStore,
    },
    MetricDescriptor {
        name: metrics::TIERED_STORE_MESSAGES_OUT_TOTAL,
        kind: MetricKind::Counter,
        unit: "{message}",
        labels: TIERED_MESSAGE_OUT_LABELS,
        source: MetricSource::TieredStore,
    },
    MetricDescriptor {
        name: metrics::TIERED_STORE_GET_MESSAGE_FALLBACK_TOTAL,
        kind: MetricKind::Counter,
        unit: "{request}",
        labels: TIERED_MESSAGE_OUT_LABELS,
        source: MetricSource::TieredStore,
    },
    MetricDescriptor {
        name: metrics::TIERED_STORE_PROVIDER_UPLOAD_BYTES,
        kind: MetricKind::Counter,
        unit: "By",
        labels: TIERED_PROVIDER_LABELS,
        source: MetricSource::TieredStore,
    },
    MetricDescriptor {
        name: metrics::TIERED_STORE_PROVIDER_DOWNLOAD_BYTES,
        kind: MetricKind::Counter,
        unit: "By",
        labels: TIERED_PROVIDER_LABELS,
        source: MetricSource::TieredStore,
    },
    MetricDescriptor {
        name: metrics::TIERED_STORE_PROVIDER_RPC_LATENCY,
        kind: MetricKind::Histogram,
        unit: "ms",
        labels: TIERED_PROVIDER_LABELS,
        source: MetricSource::TieredStore,
    },
    MetricDescriptor {
        name: metrics::TIERED_STORE_API_LATENCY,
        kind: MetricKind::Histogram,
        unit: "ms",
        labels: &[labels::OPERATION, labels::SUCCESS],
        source: MetricSource::TieredStore,
    },
    MetricDescriptor {
        name: metrics::TIERED_STORE_DISPATCH_LATENCY,
        kind: MetricKind::Histogram,
        unit: "ms",
        labels: TIERED_DISPATCH_LABELS,
        source: MetricSource::TieredStore,
    },
    MetricDescriptor {
        name: metrics::TIERED_STORE_DISPATCH_BEHIND,
        kind: MetricKind::ObservableGauge,
        unit: "{message}",
        labels: TIERED_DISPATCH_LABELS,
        source: MetricSource::TieredStore,
    },
    MetricDescriptor {
        name: metrics::TIERED_STORE_READ_AHEAD_CACHE_COUNT,
        kind: MetricKind::ObservableGauge,
        unit: "{entry}",
        labels: &[],
        source: MetricSource::TieredStore,
    },
    MetricDescriptor {
        name: metrics::TIERED_STORE_READ_AHEAD_CACHE_BYTES,
        kind: MetricKind::ObservableGauge,
        unit: "By",
        labels: &[],
        source: MetricSource::TieredStore,
    },
    MetricDescriptor {
        name: metrics::TIERED_STORE_READ_AHEAD_CACHE_ACCESS_TOTAL,
        kind: MetricKind::Counter,
        unit: "{access}",
        labels: &[labels::SUCCESS],
        source: MetricSource::TieredStore,
    },
    MetricDescriptor {
        name: metrics::TIERED_STORE_READ_AHEAD_CACHE_HIT_TOTAL,
        kind: MetricKind::Counter,
        unit: "{hit}",
        labels: &[labels::SUCCESS],
        source: MetricSource::TieredStore,
    },
    MetricDescriptor {
        name: metrics::PROXY_UP,
        kind: MetricKind::ObservableGauge,
        unit: "1",
        labels: PROXY_UP_LABELS,
        source: MetricSource::Proxy,
    },
    MetricDescriptor {
        name: metrics::CONTROLLER_ROLE,
        kind: MetricKind::UpDownCounter,
        unit: "1",
        labels: CONTROLLER_BASE_LABELS,
        source: MetricSource::Controller,
    },
    MetricDescriptor {
        name: metrics::CONTROLLER_DLEDGER_DISK_USAGE,
        kind: MetricKind::ObservableGauge,
        unit: "By",
        labels: CONTROLLER_BASE_LABELS,
        source: MetricSource::Controller,
    },
    MetricDescriptor {
        name: metrics::CONTROLLER_ACTIVE_BROKER_NUM,
        kind: MetricKind::ObservableGauge,
        unit: "{broker}",
        labels: CONTROLLER_BASE_LABELS,
        source: MetricSource::Controller,
    },
    MetricDescriptor {
        name: metrics::CONTROLLER_REQUEST_TOTAL,
        kind: MetricKind::Counter,
        unit: "{request}",
        labels: CONTROLLER_REQUEST_LABELS,
        source: MetricSource::Controller,
    },
    MetricDescriptor {
        name: metrics::CONTROLLER_DLEDGER_OP_TOTAL,
        kind: MetricKind::Counter,
        unit: "{operation}",
        labels: CONTROLLER_DLEDGER_LABELS,
        source: MetricSource::Controller,
    },
    MetricDescriptor {
        name: metrics::CONTROLLER_ELECTION_TOTAL_JAVA,
        kind: MetricKind::Counter,
        unit: "{election}",
        labels: CONTROLLER_ELECTION_LABELS,
        source: MetricSource::Controller,
    },
    MetricDescriptor {
        name: metrics::CONTROLLER_REQUEST_LATENCY,
        kind: MetricKind::Histogram,
        unit: "us",
        labels: &[labels::ADDRESS, labels::GROUP, labels::PEER_ID, labels::REQUEST_TYPE],
        source: MetricSource::Controller,
    },
    MetricDescriptor {
        name: metrics::CONTROLLER_DLEDGER_OP_LATENCY,
        kind: MetricKind::Histogram,
        unit: "us",
        labels: &[
            labels::ADDRESS,
            labels::GROUP,
            labels::PEER_ID,
            labels::DLEDGER_OPERATION,
        ],
        source: MetricSource::Controller,
    },
];

pub const fn java_metrics() -> &'static [MetricDescriptor] {
    JAVA_METRICS
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    const EXPECTED_JAVA_METRIC_NAMES: &[&str] = &[
        "active_broker_num",
        "dledger_disk_usage",
        "dledger_op_latency",
        "dledger_op_total",
        "election_total",
        "request_latency",
        "request_total",
        "rocketmq_broker_permission",
        "rocketmq_commit_messages_total",
        "rocketmq_consumer_connections",
        "rocketmq_consumer_group_create_execution_time",
        "rocketmq_consumer_group_number",
        "rocketmq_consumer_inflight_messages",
        "rocketmq_consumer_lag_latency",
        "rocketmq_consumer_lag_messages",
        "rocketmq_consumer_queueing_latency",
        "rocketmq_consumer_ready_messages",
        "rocketmq_delay_message_latency",
        "rocketmq_finish_message_latency",
        "rocketmq_half_messages",
        "rocketmq_message_size",
        "rocketmq_messages_in_total",
        "rocketmq_messages_out_total",
        "rocketmq_pop_buffer_scan_time_consume",
        "rocketmq_pop_checkpoint_buffer_size",
        "rocketmq_pop_offset_buffer_size",
        "rocketmq_pop_revive_in_message_total",
        "rocketmq_pop_revive_lag",
        "rocketmq_pop_revive_latency",
        "rocketmq_pop_revive_out_message_total",
        "rocketmq_pop_revive_retry_messages_total",
        "rocketmq_processor_watermark",
        "rocketmq_producer_connections",
        "rocketmq_proxy_up",
        "rocketmq_rocksdb_bytes_read",
        "rocketmq_rocksdb_bytes_written",
        "rocketmq_rocksdb_rate_cache_hit",
        "rocketmq_rocksdb_read_amplification_bytes",
        "rocketmq_rocksdb_times_compressed",
        "rocketmq_rocksdb_times_read",
        "rocketmq_rocksdb_times_written_other",
        "rocketmq_rocksdb_times_written_self",
        "rocketmq_rollback_messages_total",
        "rocketmq_rpc_latency",
        "rocketmq_send_to_dlq_messages_total",
        "rocketmq_storage_dispatch_behind_bytes",
        "rocketmq_storage_flush_behind_bytes",
        "rocketmq_storage_message_reserve_time",
        "rocketmq_storage_size",
        "rocketmq_throughput_in_total",
        "rocketmq_throughput_out_total",
        "rocketmq_tiered_store_api_latency",
        "rocketmq_tiered_store_dispatch_behind",
        "rocketmq_tiered_store_dispatch_latency",
        "rocketmq_tiered_store_get_message_fallback_total",
        "rocketmq_tiered_store_messages_dispatch_total",
        "rocketmq_tiered_store_messages_out_total",
        "rocketmq_tiered_store_provider_download_bytes",
        "rocketmq_tiered_store_provider_rpc_latency",
        "rocketmq_tiered_store_provider_upload_bytes",
        "rocketmq_tiered_store_read_ahead_cache_access_total",
        "rocketmq_tiered_store_read_ahead_cache_bytes",
        "rocketmq_tiered_store_read_ahead_cache_count",
        "rocketmq_tiered_store_read_ahead_cache_hit_total",
        "rocketmq_timer_dequeue_lag",
        "rocketmq_timer_dequeue_latency",
        "rocketmq_timer_dequeue_total",
        "rocketmq_timer_enqueue_lag",
        "rocketmq_timer_enqueue_latency",
        "rocketmq_timer_enqueue_total",
        "rocketmq_timer_message_snapshot",
        "rocketmq_timing_messages",
        "rocketmq_topic_create_execution_time",
        "rocketmq_topic_number",
        "role",
    ];

    #[test]
    fn java_metric_catalog_contains_every_java_metric_once() {
        let actual = JAVA_METRICS
            .iter()
            .map(|descriptor| descriptor.name)
            .collect::<HashSet<_>>();
        let expected = EXPECTED_JAVA_METRIC_NAMES.iter().copied().collect::<HashSet<_>>();

        assert_eq!(actual, expected);
        assert_eq!(JAVA_METRICS.len(), actual.len(), "duplicate metric names in catalog");
    }

    #[test]
    fn java_metric_catalog_has_required_label_sets() {
        let rpc_latency = JAVA_METRICS
            .iter()
            .find(|descriptor| descriptor.name == metrics::RPC_LATENCY)
            .expect("rpc latency descriptor");
        assert_eq!(rpc_latency.kind, MetricKind::Histogram);
        assert_eq!(rpc_latency.labels, REMOTING_RPC_LABELS);

        let proxy_up = JAVA_METRICS
            .iter()
            .find(|descriptor| descriptor.name == metrics::PROXY_UP)
            .expect("proxy up descriptor");
        assert_eq!(proxy_up.kind, MetricKind::ObservableGauge);
        assert_eq!(proxy_up.labels, PROXY_UP_LABELS);

        let controller_role = JAVA_METRICS
            .iter()
            .find(|descriptor| descriptor.name == metrics::CONTROLLER_ROLE)
            .expect("controller role descriptor");
        assert_eq!(controller_role.kind, MetricKind::UpDownCounter);

        let producer_connections = JAVA_METRICS
            .iter()
            .find(|descriptor| descriptor.name == metrics::PRODUCER_CONNECTIONS)
            .expect("producer connections descriptor");
        assert!(producer_connections.labels.contains(&labels::PROTOCOL_TYPE));

        let consumer_connections = JAVA_METRICS
            .iter()
            .find(|descriptor| descriptor.name == metrics::CONSUMER_CONNECTIONS)
            .expect("consumer connections descriptor");
        assert!(consumer_connections.labels.contains(&labels::PROTOCOL_TYPE));

        let delay_message_latency = JAVA_METRICS
            .iter()
            .find(|descriptor| descriptor.name == metrics::DELAY_MESSAGE_LATENCY)
            .expect("delay message latency descriptor");
        assert_eq!(delay_message_latency.unit, "seconds");
        assert_eq!(delay_message_latency.labels, STORE_TOPIC_LABELS);
    }
}
