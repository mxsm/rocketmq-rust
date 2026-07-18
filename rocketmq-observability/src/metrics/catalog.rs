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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricSource {
    Broker,
    Client,
    NameServer,
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
const STORE_MEMORY_LOCK_CATEGORY_LABELS: &[&str] = &[labels::CATEGORY];
const STORE_MEMORY_LOCK_ERRNO_LABELS: &[&str] = &[labels::CATEGORY, labels::ERRNO];
const STORE_MEMORY_LOCK_SKIP_LABELS: &[&str] = &[labels::CATEGORY, labels::REASON];
const STORE_LINUX_STORAGE_DEGRADATION_LABELS: &[&str] = &[labels::OPERATION, labels::REASON, labels::ERRNO];
const STORE_TRANSFER_ENGINE_LABELS: &[&str] = &[labels::ENGINE];
const STORE_TRANSFER_FALLBACK_LABELS: &[&str] = &[labels::FROM, labels::TO, labels::REASON];
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
const BROKER_LABEL_DROPPED_LABELS: &[&str] = &[labels::CLUSTER, labels::NODE_TYPE, labels::NODE_ID, labels::LABEL_KEY];

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
        name: metrics::STORE_COMMITLOG_SEGMENT_LEASE_ACTIVE,
        kind: MetricKind::ObservableGauge,
        unit: "{lease}",
        labels: &[],
        source: MetricSource::Store,
    },
    MetricDescriptor {
        name: metrics::STORE_HA_ACK_LATENCY_MILLIS,
        kind: MetricKind::Histogram,
        unit: "ms",
        labels: &[],
        source: MetricSource::Store,
    },
    MetricDescriptor {
        name: metrics::STORE_HA_REPLICATION_LAG_BYTES,
        kind: MetricKind::ObservableGauge,
        unit: "By",
        labels: &[],
        source: MetricSource::Store,
    },
    MetricDescriptor {
        name: metrics::STORE_LINUX_SENDFILE_BYTES_TOTAL,
        kind: MetricKind::Counter,
        unit: "By",
        labels: &[],
        source: MetricSource::Store,
    },
    MetricDescriptor {
        name: metrics::STORE_LINUX_MLOCK_BYTES,
        kind: MetricKind::ObservableGauge,
        unit: "By",
        labels: &[],
        source: MetricSource::Store,
    },
    MetricDescriptor {
        name: metrics::STORE_LINUX_MLOCK_ATTEMPT_TOTAL,
        kind: MetricKind::Counter,
        unit: "{operation}",
        labels: STORE_MEMORY_LOCK_CATEGORY_LABELS,
        source: MetricSource::Store,
    },
    MetricDescriptor {
        name: metrics::STORE_LINUX_MLOCK_SUCCESS_TOTAL,
        kind: MetricKind::Counter,
        unit: "{operation}",
        labels: STORE_MEMORY_LOCK_CATEGORY_LABELS,
        source: MetricSource::Store,
    },
    MetricDescriptor {
        name: metrics::STORE_LINUX_MLOCK_FAILURE_TOTAL,
        kind: MetricKind::Counter,
        unit: "{operation}",
        labels: STORE_MEMORY_LOCK_ERRNO_LABELS,
        source: MetricSource::Store,
    },
    MetricDescriptor {
        name: metrics::STORE_LINUX_MLOCK_SKIPPED_TOTAL,
        kind: MetricKind::Counter,
        unit: "{operation}",
        labels: STORE_MEMORY_LOCK_SKIP_LABELS,
        source: MetricSource::Store,
    },
    MetricDescriptor {
        name: metrics::STORE_LINUX_LOCKED_BYTES,
        kind: MetricKind::ObservableGauge,
        unit: "By",
        labels: STORE_MEMORY_LOCK_CATEGORY_LABELS,
        source: MetricSource::Store,
    },
    MetricDescriptor {
        name: metrics::STORE_LINUX_MUNLOCK_FAILURE_TOTAL,
        kind: MetricKind::Counter,
        unit: "{operation}",
        labels: STORE_MEMORY_LOCK_ERRNO_LABELS,
        source: MetricSource::Store,
    },
    MetricDescriptor {
        name: metrics::STORE_LINUX_PAGE_CACHE_WARMUP_MILLIS,
        kind: MetricKind::Histogram,
        unit: "ms",
        labels: &[],
        source: MetricSource::Store,
    },
    MetricDescriptor {
        name: metrics::STORE_LINUX_STORAGE_DEGRADATION_TOTAL,
        kind: MetricKind::Counter,
        unit: "{operation}",
        labels: STORE_LINUX_STORAGE_DEGRADATION_LABELS,
        source: MetricSource::Store,
    },
    MetricDescriptor {
        name: metrics::STORE_TRANSFER_BATCH_TOTAL,
        kind: MetricKind::Counter,
        unit: "{batch}",
        labels: &[],
        source: MetricSource::Store,
    },
    MetricDescriptor {
        name: metrics::STORE_TRANSFER_BYTES_TOTAL,
        kind: MetricKind::Counter,
        unit: "By",
        labels: &[],
        source: MetricSource::Store,
    },
    MetricDescriptor {
        name: metrics::STORE_TRANSFER_ENGINE_TOTAL,
        kind: MetricKind::Counter,
        unit: "{transfer}",
        labels: STORE_TRANSFER_ENGINE_LABELS,
        source: MetricSource::Store,
    },
    MetricDescriptor {
        name: metrics::STORE_TRANSFER_FALLBACK_TOTAL,
        kind: MetricKind::Counter,
        unit: "{fallback}",
        labels: STORE_TRANSFER_FALLBACK_LABELS,
        source: MetricSource::Store,
    },
    MetricDescriptor {
        name: metrics::STORE_TRANSFER_PARTIAL_WRITE_TOTAL,
        kind: MetricKind::Counter,
        unit: "{write}",
        labels: &[],
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

/// Rust-native metrics that are not part of the Java compatibility catalog.
pub const RUST_METRICS: &[MetricDescriptor] = &[
    MetricDescriptor {
        name: metrics::SEND_MESSAGE_LATENCY,
        kind: MetricKind::Histogram,
        unit: "ms",
        labels: BROKER_TOPIC_LABELS,
        source: MetricSource::Broker,
    },
    MetricDescriptor {
        name: metrics::METRICS_LABEL_DROPPED_TOTAL,
        kind: MetricKind::Counter,
        unit: "{label}",
        labels: BROKER_LABEL_DROPPED_LABELS,
        source: MetricSource::Broker,
    },
    MetricDescriptor {
        name: metrics::STORE_APPEND_LATENCY,
        kind: MetricKind::Histogram,
        unit: "ms",
        labels: &[],
        source: MetricSource::Store,
    },
    MetricDescriptor {
        name: metrics::STORE_FLUSH_LATENCY,
        kind: MetricKind::Histogram,
        unit: "ms",
        labels: &[],
        source: MetricSource::Store,
    },
    MetricDescriptor {
        name: metrics::STORE_DISPATCH_LATENCY,
        kind: MetricKind::Histogram,
        unit: "ms",
        labels: &[],
        source: MetricSource::Store,
    },
    MetricDescriptor {
        name: metrics::STORE_DISK_USAGE,
        kind: MetricKind::ObservableGauge,
        unit: "By",
        labels: &[],
        source: MetricSource::Store,
    },
    MetricDescriptor {
        name: metrics::REMOTING_REQUESTS_TOTAL,
        kind: MetricKind::Counter,
        unit: "{request}",
        labels: &[],
        source: MetricSource::Remoting,
    },
    MetricDescriptor {
        name: metrics::REMOTING_REQUEST_LATENCY,
        kind: MetricKind::Histogram,
        unit: "ms",
        labels: &[],
        source: MetricSource::Remoting,
    },
    MetricDescriptor {
        name: metrics::REMOTING_NETWORK_BYTES,
        kind: MetricKind::Counter,
        unit: "By",
        labels: &[],
        source: MetricSource::Remoting,
    },
    MetricDescriptor {
        name: metrics::CLIENT_SEND_TOTAL,
        kind: MetricKind::Counter,
        unit: "{message}",
        labels: &[],
        source: MetricSource::Client,
    },
    MetricDescriptor {
        name: metrics::CLIENT_SEND_LATENCY,
        kind: MetricKind::Histogram,
        unit: "ms",
        labels: &[],
        source: MetricSource::Client,
    },
    MetricDescriptor {
        name: metrics::CLIENT_CONSUME_TOTAL,
        kind: MetricKind::Counter,
        unit: "{message}",
        labels: &[],
        source: MetricSource::Client,
    },
    MetricDescriptor {
        name: metrics::CLIENT_CONSUME_LATENCY,
        kind: MetricKind::Histogram,
        unit: "ms",
        labels: &[],
        source: MetricSource::Client,
    },
    MetricDescriptor {
        name: metrics::CLIENT_REBALANCE_TOTAL,
        kind: MetricKind::Counter,
        unit: "{event}",
        labels: &[],
        source: MetricSource::Client,
    },
    MetricDescriptor {
        name: metrics::NAMESRV_ROUTE_REQUEST_TOTAL,
        kind: MetricKind::Counter,
        unit: "{request}",
        labels: &[],
        source: MetricSource::NameServer,
    },
    MetricDescriptor {
        name: metrics::NAMESRV_ROUTE_REQUEST_LATENCY,
        kind: MetricKind::Histogram,
        unit: "ms",
        labels: &[],
        source: MetricSource::NameServer,
    },
    MetricDescriptor {
        name: metrics::NAMESRV_BROKER_REGISTRATIONS,
        kind: MetricKind::Counter,
        unit: "{registration}",
        labels: &[],
        source: MetricSource::NameServer,
    },
    MetricDescriptor {
        name: metrics::NAMESRV_ACTIVE_BROKERS,
        kind: MetricKind::ObservableGauge,
        unit: "{broker}",
        labels: &[],
        source: MetricSource::NameServer,
    },
    MetricDescriptor {
        name: metrics::CONTROLLER_ELECTION_TOTAL,
        kind: MetricKind::Counter,
        unit: "{election}",
        labels: &[],
        source: MetricSource::Controller,
    },
    MetricDescriptor {
        name: metrics::CONTROLLER_ELECTION_LATENCY,
        kind: MetricKind::Histogram,
        unit: "ms",
        labels: &[],
        source: MetricSource::Controller,
    },
    MetricDescriptor {
        name: metrics::CONTROLLER_LEADER_CHANGES_TOTAL,
        kind: MetricKind::Counter,
        unit: "{change}",
        labels: &[],
        source: MetricSource::Controller,
    },
    MetricDescriptor {
        name: metrics::CONTROLLER_ACTIVE_BROKERS,
        kind: MetricKind::ObservableGauge,
        unit: "{broker}",
        labels: &[],
        source: MetricSource::Controller,
    },
    MetricDescriptor {
        name: metrics::PROXY_GRPC_REQUESTS_TOTAL,
        kind: MetricKind::Counter,
        unit: "{request}",
        labels: &[],
        source: MetricSource::Proxy,
    },
    MetricDescriptor {
        name: metrics::PROXY_GRPC_REQUEST_LATENCY,
        kind: MetricKind::Histogram,
        unit: "ms",
        labels: &[],
        source: MetricSource::Proxy,
    },
    MetricDescriptor {
        name: metrics::PROXY_FORWARD_LATENCY,
        kind: MetricKind::Histogram,
        unit: "ms",
        labels: &[],
        source: MetricSource::Proxy,
    },
    MetricDescriptor {
        name: metrics::PROXY_ACTIVE_CONNECTIONS,
        kind: MetricKind::ObservableGauge,
        unit: "{connection}",
        labels: &[],
        source: MetricSource::Proxy,
    },
];

pub const fn java_metrics() -> &'static [MetricDescriptor] {
    JAVA_METRICS
}

pub const fn rust_metrics() -> &'static [MetricDescriptor] {
    RUST_METRICS
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
        "rocketmq_store_commitlog_segment_lease_active",
        "rocketmq_store_ha_ack_latency_millis",
        "rocketmq_store_ha_replication_lag_bytes",
        "rocketmq_store_linux_sendfile_bytes_total",
        "rocketmq_store_linux_locked_bytes",
        "rocketmq_store_linux_mlock_attempt_total",
        "rocketmq_store_linux_mlock_bytes",
        "rocketmq_store_linux_mlock_failure_total",
        "rocketmq_store_linux_mlock_skipped_total",
        "rocketmq_store_linux_mlock_success_total",
        "rocketmq_store_linux_munlock_failure_total",
        "rocketmq_store_linux_page_cache_warmup_millis",
        "rocketmq_store_linux_storage_degradation_total",
        "rocketmq_store_transfer_batch_total",
        "rocketmq_store_transfer_bytes_total",
        "rocketmq_store_transfer_engine_total",
        "rocketmq_store_transfer_fallback_total",
        "rocketmq_store_transfer_partial_write_total",
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
    fn combined_catalog_contains_every_semantic_metric_once() {
        let combined = JAVA_METRICS
            .iter()
            .chain(RUST_METRICS)
            .map(|descriptor| descriptor.name)
            .collect::<HashSet<_>>();

        assert_eq!(JAVA_METRICS.len(), 93);
        assert_eq!(RUST_METRICS.len(), 26);
        assert_eq!(combined.len(), 119, "duplicate metric names across catalogs");
    }

    #[test]
    fn rust_catalog_covers_native_sources() {
        let sources = RUST_METRICS
            .iter()
            .map(|descriptor| descriptor.source)
            .collect::<HashSet<_>>();

        assert!(sources.contains(&MetricSource::Broker));
        assert!(sources.contains(&MetricSource::Client));
        assert!(sources.contains(&MetricSource::NameServer));
        assert!(sources.contains(&MetricSource::Remoting));
        assert!(sources.contains(&MetricSource::Store));
        assert!(sources.contains(&MetricSource::Proxy));
        assert!(sources.contains(&MetricSource::Controller));
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

        let transfer_batch = JAVA_METRICS
            .iter()
            .find(|descriptor| descriptor.name == metrics::STORE_TRANSFER_BATCH_TOTAL)
            .expect("transfer batch descriptor");
        assert_eq!(transfer_batch.kind, MetricKind::Counter);
        assert_eq!(transfer_batch.unit, "{batch}");
        assert_eq!(transfer_batch.source, MetricSource::Store);
        assert!(transfer_batch.labels.is_empty());

        let transfer_engine = JAVA_METRICS
            .iter()
            .find(|descriptor| descriptor.name == metrics::STORE_TRANSFER_ENGINE_TOTAL)
            .expect("transfer engine descriptor");
        assert_eq!(transfer_engine.kind, MetricKind::Counter);
        assert_eq!(transfer_engine.labels, &[labels::ENGINE]);

        let transfer_fallback = JAVA_METRICS
            .iter()
            .find(|descriptor| descriptor.name == metrics::STORE_TRANSFER_FALLBACK_TOTAL)
            .expect("transfer fallback descriptor");
        assert_eq!(transfer_fallback.kind, MetricKind::Counter);
        assert_eq!(transfer_fallback.labels, &[labels::FROM, labels::TO, labels::REASON]);

        let linux_sendfile_bytes = JAVA_METRICS
            .iter()
            .find(|descriptor| descriptor.name == metrics::STORE_LINUX_SENDFILE_BYTES_TOTAL)
            .expect("linux sendfile bytes descriptor");
        assert_eq!(linux_sendfile_bytes.kind, MetricKind::Counter);
        assert_eq!(linux_sendfile_bytes.unit, "By");
        assert_eq!(linux_sendfile_bytes.source, MetricSource::Store);

        let ha_replication_lag = JAVA_METRICS
            .iter()
            .find(|descriptor| descriptor.name == metrics::STORE_HA_REPLICATION_LAG_BYTES)
            .expect("ha replication lag descriptor");
        assert_eq!(ha_replication_lag.kind, MetricKind::ObservableGauge);
        assert_eq!(ha_replication_lag.unit, "By");
        assert_eq!(ha_replication_lag.source, MetricSource::Store);
        assert!(ha_replication_lag.labels.is_empty());

        let ha_ack_latency = JAVA_METRICS
            .iter()
            .find(|descriptor| descriptor.name == metrics::STORE_HA_ACK_LATENCY_MILLIS)
            .expect("ha ack latency descriptor");
        assert_eq!(ha_ack_latency.kind, MetricKind::Histogram);
        assert_eq!(ha_ack_latency.unit, "ms");
        assert_eq!(ha_ack_latency.source, MetricSource::Store);
        assert!(ha_ack_latency.labels.is_empty());

        let linux_mlock_bytes = JAVA_METRICS
            .iter()
            .find(|descriptor| descriptor.name == metrics::STORE_LINUX_MLOCK_BYTES)
            .expect("linux mlock bytes descriptor");
        assert_eq!(linux_mlock_bytes.kind, MetricKind::ObservableGauge);
        assert_eq!(linux_mlock_bytes.unit, "By");
        assert_eq!(linux_mlock_bytes.source, MetricSource::Store);
        assert!(linux_mlock_bytes.labels.is_empty());

        let warmup_latency = JAVA_METRICS
            .iter()
            .find(|descriptor| descriptor.name == metrics::STORE_LINUX_PAGE_CACHE_WARMUP_MILLIS)
            .expect("linux page-cache warmup descriptor");
        assert_eq!(warmup_latency.kind, MetricKind::Histogram);
        assert_eq!(warmup_latency.unit, "ms");
        assert_eq!(warmup_latency.source, MetricSource::Store);
        assert!(warmup_latency.labels.is_empty());

        let lease_active = JAVA_METRICS
            .iter()
            .find(|descriptor| descriptor.name == metrics::STORE_COMMITLOG_SEGMENT_LEASE_ACTIVE)
            .expect("commitlog segment lease descriptor");
        assert_eq!(lease_active.kind, MetricKind::ObservableGauge);
        assert_eq!(lease_active.unit, "{lease}");
        assert_eq!(lease_active.source, MetricSource::Store);
        assert!(lease_active.labels.is_empty());

        let mlock_attempt = JAVA_METRICS
            .iter()
            .find(|descriptor| descriptor.name == metrics::STORE_LINUX_MLOCK_ATTEMPT_TOTAL)
            .expect("linux mlock attempt descriptor");
        assert_eq!(mlock_attempt.kind, MetricKind::Counter);
        assert_eq!(mlock_attempt.unit, "{operation}");
        assert_eq!(mlock_attempt.labels, &[labels::CATEGORY]);
        assert_eq!(mlock_attempt.source, MetricSource::Store);

        let mlock_success = JAVA_METRICS
            .iter()
            .find(|descriptor| descriptor.name == metrics::STORE_LINUX_MLOCK_SUCCESS_TOTAL)
            .expect("linux mlock success descriptor");
        assert_eq!(mlock_success.kind, MetricKind::Counter);
        assert_eq!(mlock_success.labels, &[labels::CATEGORY]);

        let mlock_failure = JAVA_METRICS
            .iter()
            .find(|descriptor| descriptor.name == metrics::STORE_LINUX_MLOCK_FAILURE_TOTAL)
            .expect("linux mlock failure descriptor");
        assert_eq!(mlock_failure.kind, MetricKind::Counter);
        assert_eq!(mlock_failure.labels, &[labels::CATEGORY, labels::ERRNO]);

        let mlock_skipped = JAVA_METRICS
            .iter()
            .find(|descriptor| descriptor.name == metrics::STORE_LINUX_MLOCK_SKIPPED_TOTAL)
            .expect("linux mlock skipped descriptor");
        assert_eq!(mlock_skipped.kind, MetricKind::Counter);
        assert_eq!(mlock_skipped.labels, &[labels::CATEGORY, labels::REASON]);

        let locked_bytes = JAVA_METRICS
            .iter()
            .find(|descriptor| descriptor.name == metrics::STORE_LINUX_LOCKED_BYTES)
            .expect("linux locked bytes descriptor");
        assert_eq!(locked_bytes.kind, MetricKind::ObservableGauge);
        assert_eq!(locked_bytes.unit, "By");
        assert_eq!(locked_bytes.labels, &[labels::CATEGORY]);

        let munlock_failure = JAVA_METRICS
            .iter()
            .find(|descriptor| descriptor.name == metrics::STORE_LINUX_MUNLOCK_FAILURE_TOTAL)
            .expect("linux munlock failure descriptor");
        assert_eq!(munlock_failure.kind, MetricKind::Counter);
        assert_eq!(munlock_failure.labels, &[labels::CATEGORY, labels::ERRNO]);

        let storage_degradation = JAVA_METRICS
            .iter()
            .find(|descriptor| descriptor.name == metrics::STORE_LINUX_STORAGE_DEGRADATION_TOTAL)
            .expect("linux storage degradation descriptor");
        assert_eq!(storage_degradation.kind, MetricKind::Counter);
        assert_eq!(storage_degradation.unit, "{operation}");
        assert_eq!(
            storage_degradation.labels,
            &[labels::OPERATION, labels::REASON, labels::ERRNO]
        );
    }
}
