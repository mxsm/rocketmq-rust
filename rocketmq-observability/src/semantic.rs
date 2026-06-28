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

pub mod metrics {
    pub const PROCESSOR_WATERMARK: &str = "rocketmq_processor_watermark";
    pub const BROKER_PERMISSION: &str = "rocketmq_broker_permission";
    pub const TOPIC_NUMBER: &str = "rocketmq_topic_number";
    pub const CONSUMER_GROUP_NUMBER: &str = "rocketmq_consumer_group_number";
    pub const MESSAGES_IN_TOTAL: &str = "rocketmq_messages_in_total";
    pub const MESSAGES_OUT_TOTAL: &str = "rocketmq_messages_out_total";
    pub const THROUGHPUT_IN_TOTAL: &str = "rocketmq_throughput_in_total";
    pub const THROUGHPUT_OUT_TOTAL: &str = "rocketmq_throughput_out_total";
    pub const MESSAGE_SIZE: &str = "rocketmq_message_size";
    pub const TOPIC_CREATE_EXECUTION_TIME: &str = "rocketmq_topic_create_execution_time";
    pub const CONSUMER_GROUP_CREATE_EXECUTION_TIME: &str = "rocketmq_consumer_group_create_execution_time";
    pub const PRODUCER_CONNECTIONS: &str = "rocketmq_producer_connections";
    pub const CONSUMER_CONNECTIONS: &str = "rocketmq_consumer_connections";
    pub const CONSUMER_LAG_MESSAGES: &str = "rocketmq_consumer_lag_messages";
    pub const CONSUMER_LAG_LATENCY: &str = "rocketmq_consumer_lag_latency";
    pub const CONSUMER_INFLIGHT_MESSAGES: &str = "rocketmq_consumer_inflight_messages";
    pub const CONSUMER_QUEUEING_LATENCY: &str = "rocketmq_consumer_queueing_latency";
    pub const CONSUMER_READY_MESSAGES: &str = "rocketmq_consumer_ready_messages";
    pub const SEND_TO_DLQ_MESSAGES_TOTAL: &str = "rocketmq_send_to_dlq_messages_total";
    pub const COMMIT_MESSAGES_TOTAL: &str = "rocketmq_commit_messages_total";
    pub const ROLLBACK_MESSAGES_TOTAL: &str = "rocketmq_rollback_messages_total";
    pub const FINISH_MESSAGE_LATENCY: &str = "rocketmq_finish_message_latency";
    pub const HALF_MESSAGES: &str = "rocketmq_half_messages";
    pub const SEND_MESSAGE_LATENCY: &str = "rocketmq_send_message_latency";
    pub const METRICS_LABEL_DROPPED_TOTAL: &str = "rocketmq_metrics_label_dropped_total";
    pub const POP_BUFFER_SCAN_TIME_CONSUME: &str = "rocketmq_pop_buffer_scan_time_consume";
    pub const POP_REVIVE_IN_MESSAGE_TOTAL: &str = "rocketmq_pop_revive_in_message_total";
    pub const POP_REVIVE_OUT_MESSAGE_TOTAL: &str = "rocketmq_pop_revive_out_message_total";
    pub const POP_REVIVE_RETRY_MESSAGES_TOTAL: &str = "rocketmq_pop_revive_retry_messages_total";
    pub const POP_REVIVE_LAG: &str = "rocketmq_pop_revive_lag";
    pub const POP_REVIVE_LATENCY: &str = "rocketmq_pop_revive_latency";
    pub const POP_OFFSET_BUFFER_SIZE: &str = "rocketmq_pop_offset_buffer_size";
    pub const POP_CHECKPOINT_BUFFER_SIZE: &str = "rocketmq_pop_checkpoint_buffer_size";
    pub const STORE_APPEND_LATENCY: &str = "rocketmq_store_append_latency";
    pub const STORE_FLUSH_LATENCY: &str = "rocketmq_store_flush_latency";
    pub const STORE_DISPATCH_LATENCY: &str = "rocketmq_store_dispatch_latency";
    pub const STORE_DISK_USAGE: &str = "rocketmq_store_disk_usage";
    pub const STORE_LINUX_SENDFILE_BYTES_TOTAL: &str = "rocketmq_store_linux_sendfile_bytes_total";
    pub const STORE_TRANSFER_BATCH_TOTAL: &str = "rocketmq_store_transfer_batch_total";
    pub const STORE_TRANSFER_BYTES_TOTAL: &str = "rocketmq_store_transfer_bytes_total";
    pub const STORE_TRANSFER_ENGINE_TOTAL: &str = "rocketmq_store_transfer_engine_total";
    pub const STORE_TRANSFER_FALLBACK_TOTAL: &str = "rocketmq_store_transfer_fallback_total";
    pub const STORE_TRANSFER_PARTIAL_WRITE_TOTAL: &str = "rocketmq_store_transfer_partial_write_total";
    pub const STORAGE_SIZE: &str = "rocketmq_storage_size";
    pub const STORAGE_FLUSH_BEHIND_BYTES: &str = "rocketmq_storage_flush_behind_bytes";
    pub const STORAGE_DISPATCH_BEHIND_BYTES: &str = "rocketmq_storage_dispatch_behind_bytes";
    pub const STORAGE_MESSAGE_RESERVE_TIME: &str = "rocketmq_storage_message_reserve_time";
    pub const DELAY_MESSAGE_LATENCY: &str = "rocketmq_delay_message_latency";
    pub const TIMER_ENQUEUE_LAG: &str = "rocketmq_timer_enqueue_lag";
    pub const TIMER_ENQUEUE_LATENCY: &str = "rocketmq_timer_enqueue_latency";
    pub const TIMER_DEQUEUE_LAG: &str = "rocketmq_timer_dequeue_lag";
    pub const TIMER_DEQUEUE_LATENCY: &str = "rocketmq_timer_dequeue_latency";
    pub const TIMING_MESSAGES: &str = "rocketmq_timing_messages";
    pub const TIMER_ENQUEUE_TOTAL: &str = "rocketmq_timer_enqueue_total";
    pub const TIMER_DEQUEUE_TOTAL: &str = "rocketmq_timer_dequeue_total";
    pub const TIMER_MESSAGE_SNAPSHOT: &str = "rocketmq_timer_message_snapshot";
    pub const ROCKSDB_BYTES_WRITTEN: &str = "rocketmq_rocksdb_bytes_written";
    pub const ROCKSDB_BYTES_READ: &str = "rocketmq_rocksdb_bytes_read";
    pub const ROCKSDB_TIMES_WRITTEN_SELF: &str = "rocketmq_rocksdb_times_written_self";
    pub const ROCKSDB_TIMES_WRITTEN_OTHER: &str = "rocketmq_rocksdb_times_written_other";
    pub const ROCKSDB_RATE_CACHE_HIT: &str = "rocketmq_rocksdb_rate_cache_hit";
    pub const ROCKSDB_TIMES_COMPRESSED: &str = "rocketmq_rocksdb_times_compressed";
    pub const ROCKSDB_READ_AMPLIFICATION_BYTES: &str = "rocketmq_rocksdb_read_amplification_bytes";
    pub const ROCKSDB_TIMES_READ: &str = "rocketmq_rocksdb_times_read";
    pub const REMOTING_REQUESTS_TOTAL: &str = "rocketmq_remoting_requests_total";
    pub const REMOTING_REQUEST_LATENCY: &str = "rocketmq_remoting_request_latency";
    pub const REMOTING_NETWORK_BYTES: &str = "rocketmq_remoting_network_bytes";
    pub const RPC_LATENCY: &str = "rocketmq_rpc_latency";
    pub const TIERED_STORE_MESSAGES_DISPATCH_TOTAL: &str = "rocketmq_tiered_store_messages_dispatch_total";
    pub const TIERED_STORE_MESSAGES_OUT_TOTAL: &str = "rocketmq_tiered_store_messages_out_total";
    pub const TIERED_STORE_GET_MESSAGE_FALLBACK_TOTAL: &str = "rocketmq_tiered_store_get_message_fallback_total";
    pub const TIERED_STORE_PROVIDER_UPLOAD_BYTES: &str = "rocketmq_tiered_store_provider_upload_bytes";
    pub const TIERED_STORE_PROVIDER_DOWNLOAD_BYTES: &str = "rocketmq_tiered_store_provider_download_bytes";
    pub const TIERED_STORE_PROVIDER_RPC_LATENCY: &str = "rocketmq_tiered_store_provider_rpc_latency";
    pub const TIERED_STORE_API_LATENCY: &str = "rocketmq_tiered_store_api_latency";
    pub const TIERED_STORE_DISPATCH_LATENCY: &str = "rocketmq_tiered_store_dispatch_latency";
    pub const TIERED_STORE_DISPATCH_BEHIND: &str = "rocketmq_tiered_store_dispatch_behind";
    pub const TIERED_STORE_READ_AHEAD_CACHE_COUNT: &str = "rocketmq_tiered_store_read_ahead_cache_count";
    pub const TIERED_STORE_READ_AHEAD_CACHE_BYTES: &str = "rocketmq_tiered_store_read_ahead_cache_bytes";
    pub const TIERED_STORE_READ_AHEAD_CACHE_ACCESS_TOTAL: &str = "rocketmq_tiered_store_read_ahead_cache_access_total";
    pub const TIERED_STORE_READ_AHEAD_CACHE_HIT_TOTAL: &str = "rocketmq_tiered_store_read_ahead_cache_hit_total";
    pub const CLIENT_SEND_TOTAL: &str = "rocketmq_client_send_total";
    pub const CLIENT_SEND_LATENCY: &str = "rocketmq_client_send_latency";
    pub const CLIENT_CONSUME_TOTAL: &str = "rocketmq_client_consume_total";
    pub const CLIENT_CONSUME_LATENCY: &str = "rocketmq_client_consume_latency";
    pub const CLIENT_REBALANCE_TOTAL: &str = "rocketmq_client_rebalance_total";
    pub const NAMESRV_ROUTE_REQUEST_TOTAL: &str = "rocketmq_namesrv_route_request_total";
    pub const NAMESRV_ROUTE_REQUEST_LATENCY: &str = "rocketmq_namesrv_route_request_latency";
    pub const NAMESRV_BROKER_REGISTRATIONS: &str = "rocketmq_namesrv_broker_registrations";
    pub const NAMESRV_ACTIVE_BROKERS: &str = "rocketmq_namesrv_active_brokers";
    pub const CONTROLLER_ELECTION_TOTAL: &str = "rocketmq_controller_election_total";
    pub const CONTROLLER_ELECTION_LATENCY: &str = "rocketmq_controller_election_latency";
    pub const CONTROLLER_LEADER_CHANGES_TOTAL: &str = "rocketmq_controller_leader_changes_total";
    pub const CONTROLLER_ACTIVE_BROKERS: &str = "rocketmq_controller_active_brokers";
    pub const CONTROLLER_ROLE: &str = "role";
    pub const CONTROLLER_DLEDGER_DISK_USAGE: &str = "dledger_disk_usage";
    pub const CONTROLLER_ACTIVE_BROKER_NUM: &str = "active_broker_num";
    pub const CONTROLLER_REQUEST_TOTAL: &str = "request_total";
    pub const CONTROLLER_DLEDGER_OP_TOTAL: &str = "dledger_op_total";
    pub const CONTROLLER_ELECTION_TOTAL_JAVA: &str = "election_total";
    pub const CONTROLLER_REQUEST_LATENCY: &str = "request_latency";
    pub const CONTROLLER_DLEDGER_OP_LATENCY: &str = "dledger_op_latency";
    pub const PROXY_GRPC_REQUESTS_TOTAL: &str = "rocketmq_proxy_grpc_requests_total";
    pub const PROXY_GRPC_REQUEST_LATENCY: &str = "rocketmq_proxy_grpc_request_latency";
    pub const PROXY_FORWARD_LATENCY: &str = "rocketmq_proxy_forward_latency";
    pub const PROXY_ACTIVE_CONNECTIONS: &str = "rocketmq_proxy_active_connections";
    pub const PROXY_UP: &str = "rocketmq_proxy_up";
}

pub mod labels {
    pub const ADDRESS: &str = "address";
    pub const CLUSTER: &str = "cluster";
    pub const NODE_TYPE: &str = "node_type";
    pub const NODE_ID: &str = "node_id";
    pub const AGGREGATION: &str = "aggregation";
    pub const PROCESSOR: &str = "processor";
    pub const TOPIC: &str = "topic";
    pub const GROUP: &str = "group";
    pub const CONSUMER_GROUP: &str = "consumer_group";
    pub const INVOCATION_STATUS: &str = "invocation_status";
    pub const IS_RETRY: &str = "is_retry";
    pub const IS_SYSTEM: &str = "is_system";
    pub const MESSAGE_TYPE: &str = "message_type";
    pub const LANGUAGE: &str = "language";
    pub const VERSION: &str = "version";
    pub const CONSUME_MODE: &str = "consume_mode";
    pub const REVIVE_MESSAGE_TYPE: &str = "revive_message_type";
    pub const PUT_STATUS: &str = "put_status";
    pub const QUEUE_ID: &str = "queue_id";
    pub const PROTOCOL_TYPE: &str = "protocol_type";
    pub const REQUEST_CODE: &str = "request_code";
    pub const RESPONSE_CODE: &str = "response_code";
    pub const IS_LONG_POLLING: &str = "is_long_polling";
    pub const RESULT: &str = "result";
    pub const STORAGE_TYPE: &str = "storage_type";
    pub const STORAGE_MEDIUM: &str = "storage_medium";
    pub const TYPE: &str = "type";
    pub const ENGINE: &str = "engine";
    pub const FROM: &str = "from";
    pub const TO: &str = "to";
    pub const REASON: &str = "reason";
    pub const TIMER_BOUND_SECONDS: &str = "timer_bound_s";
    pub const OPERATION: &str = "operation";
    pub const SUCCESS: &str = "success";
    pub const PATH: &str = "path";
    pub const FILE_TYPE: &str = "file_type";
    pub const PROXY_MODE: &str = "proxy_mode";
    pub const PEER_ID: &str = "peer_id";
    pub const BROKER_SET: &str = "broker_set";
    pub const REQUEST_TYPE: &str = "request_type";
    pub const REQUEST_HANDLE_STATUS: &str = "request_handle_status";
    pub const DLEDGER_OPERATION: &str = "dledger_operation";
    pub const DLEDGER_OPERATION_STATUS: &str = "dLedger_operation_status";
    pub const ELECTION_RESULT: &str = "election_result";
}

pub mod trace {
    pub const TRACEPARENT: &str = "traceparent";
    pub const TRACESTATE: &str = "tracestate";
    pub const BAGGAGE: &str = "baggage";
    pub const MESSAGING_MESSAGE_ID: &str = "messaging.message.id";
    pub const MESSAGING_MESSAGE_BODY_SIZE: &str = "messaging.message.body.size";
    pub const MESSAGING_ROCKETMQ_MESSAGE_KEYS: &str = "messaging.rocketmq.message.keys";
}
