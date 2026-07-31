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
    pub const BROKER_UP: &str = "rocketmq_broker_up";
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
    pub const STORE_COMMITLOG_SEGMENT_LEASE_ACTIVE: &str = "rocketmq_store_commitlog_segment_lease_active";
    pub const STORE_HA_ACK_LATENCY_MILLIS: &str = "rocketmq_store_ha_ack_latency_millis";
    pub const STORE_HA_REPLICATION_LAG_BYTES: &str = "rocketmq_store_ha_replication_lag_bytes";
    pub const STORE_LINUX_SENDFILE_BYTES_TOTAL: &str = "rocketmq_store_linux_sendfile_bytes_total";
    pub const STORE_LINUX_MLOCK_BYTES: &str = "rocketmq_store_linux_mlock_bytes";
    pub const STORE_LINUX_MLOCK_ATTEMPT_TOTAL: &str = "rocketmq_store_linux_mlock_attempt_total";
    pub const STORE_LINUX_MLOCK_SUCCESS_TOTAL: &str = "rocketmq_store_linux_mlock_success_total";
    pub const STORE_LINUX_MLOCK_FAILURE_TOTAL: &str = "rocketmq_store_linux_mlock_failure_total";
    pub const STORE_LINUX_MLOCK_SKIPPED_TOTAL: &str = "rocketmq_store_linux_mlock_skipped_total";
    pub const STORE_LINUX_LOCKED_BYTES: &str = "rocketmq_store_linux_locked_bytes";
    pub const STORE_LINUX_MUNLOCK_FAILURE_TOTAL: &str = "rocketmq_store_linux_munlock_failure_total";
    pub const STORE_LINUX_PAGE_CACHE_WARMUP_MILLIS: &str = "rocketmq_store_linux_page_cache_warmup_millis";
    pub const STORE_LINUX_STORAGE_DEGRADATION_TOTAL: &str = "rocketmq_store_linux_storage_degradation_total";
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
    pub const TRANSPORT_REQUESTS_TOTAL: &str = "rocketmq_transport_requests_total";
    pub const TRANSPORT_REQUEST_LATENCY: &str = "rocketmq_transport_request_latency";
    pub const TRANSPORT_NETWORK_BYTES: &str = "rocketmq_transport_network_bytes";
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
    pub const CLIENT_ONEWAY_EGRESS_ITEMS: &str = "rocketmq_client_oneway_egress_items";
    pub const CLIENT_ONEWAY_EGRESS_BYTES: &str = "rocketmq_client_oneway_egress_bytes";
    pub const CLIENT_ONEWAY_EGRESS_OLDEST_AGE: &str = "rocketmq_client_oneway_egress_oldest_age";
    pub const CLIENT_ONEWAY_EGRESS_WAITERS: &str = "rocketmq_client_oneway_egress_waiters";
    pub const CLIENT_ONEWAY_EGRESS_EVENTS_TOTAL: &str = "rocketmq_client_oneway_egress_events_total";
    pub const NAMESRV_ROUTE_REQUEST_TOTAL: &str = "rocketmq_namesrv_route_request_total";
    pub const NAMESRV_ROUTE_REQUEST_LATENCY: &str = "rocketmq_namesrv_route_request_latency";
    pub const NAMESRV_BROKER_REGISTRATIONS: &str = "rocketmq_namesrv_broker_registrations";
    pub const NAMESRV_ACTIVE_BROKERS: &str = "rocketmq_namesrv_active_brokers";
    pub const NAMESRV_ROUTE_ERRORS_TOTAL: &str = "rocketmq_namesrv_route_errors_total";
    pub const NAMESRV_ROUTE_FRESHNESS: &str = "rocketmq_namesrv_route_freshness";
    pub const CONTROLLER_ELECTION_TOTAL: &str = "rocketmq_controller_election_total";
    pub const CONTROLLER_ELECTION_LATENCY: &str = "rocketmq_controller_election_latency";
    pub const CONTROLLER_LEADER_CHANGES_TOTAL: &str = "rocketmq_controller_leader_changes_total";
    pub const CONTROLLER_ACTIVE_BROKERS: &str = "rocketmq_controller_active_brokers";
    pub const CONTROLLER_QUORUM_HEALTH: &str = "rocketmq_controller_quorum_health";
    pub const CONTROLLER_HEARTBEAT_AGE: &str = "rocketmq_controller_heartbeat_age";
    pub const CONTROLLER_STALE_BROKERS: &str = "rocketmq_controller_stale_brokers";
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
    pub const PROXY_GRPC_ERRORS_TOTAL: &str = "rocketmq_proxy_grpc_errors_total";
    pub const PROXY_UP: &str = "rocketmq_proxy_up";
    pub const LOG_FILTER_RELOAD_TOTAL: &str = "rocketmq_observability_log_filter_reload_total";
    pub const LOG_FILTER_ACTIVE: &str = "rocketmq_observability_log_filter_active";
    pub const LOG_FILTER_EXPIRY_TIMESTAMP_SECONDS: &str = "rocketmq_observability_log_filter_expiry_timestamp_seconds";
    pub const LOG_FILTER_AUDIT_FAILURE_TOTAL: &str = "rocketmq_observability_log_filter_audit_failure_total";
    pub const LOG_FILTER_AUTO_RESTORE_FAILURE_TOTAL: &str =
        "rocketmq_observability_log_filter_auto_restore_failure_total";
    pub const LOG_FILTER_ROLLBACK_FAILURE_TOTAL: &str = "rocketmq_observability_log_filter_rollback_failure_total";
    pub const RELEASE_INFO: &str = "rocketmq_release_info";
    pub const MCP_REQUESTS_TOTAL: &str = "rocketmq_mcp_requests_total";
    pub const MCP_REQUEST_LATENCY: &str = "rocketmq_mcp_request_latency";
    pub const MCP_ERRORS_TOTAL: &str = "rocketmq_mcp_errors_total";
    pub const MCP_CACHE_OPERATIONS_TOTAL: &str = "rocketmq_mcp_cache_operations_total";
    pub const MCP_RATE_LIMIT_TOTAL: &str = "rocketmq_mcp_rate_limit_total";
    pub const MCP_AUDIT_BACKLOG: &str = "rocketmq_mcp_audit_backlog";
    pub const MCP_AUDIT_DROPPED_TOTAL: &str = "rocketmq_mcp_audit_dropped_total";
    pub const MCP_AUDIT_FAILURES_TOTAL: &str = "rocketmq_mcp_audit_failures_total";
    pub const RUNTIME_TASKS: &str = "rocketmq_runtime_tasks";
    pub const RUNTIME_TASK_GROUPS: &str = "rocketmq_runtime_task_groups";
    pub const RUNTIME_LONG_RUNNING_TASKS: &str = "rocketmq_runtime_long_running_tasks";
    pub const RUNTIME_BLOCKING_QUEUED: &str = "rocketmq_runtime_blocking_queued";
    pub const RUNTIME_BLOCKING_RUNNING: &str = "rocketmq_runtime_blocking_running";
    pub const RUNTIME_BLOCKING_TIMEOUTS: &str = "rocketmq_runtime_blocking_timeouts";
    pub const RUNTIME_LIFECYCLE_TRANSITIONS_TOTAL: &str = "rocketmq_runtime_lifecycle_transitions_total";
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
    pub const STATE: &str = "state";
    pub const STORAGE_TYPE: &str = "storage_type";
    pub const STORAGE_MEDIUM: &str = "storage_medium";
    pub const TYPE: &str = "type";
    pub const CATEGORY: &str = "category";
    pub const ERRNO: &str = "errno";
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
    pub const LABEL_KEY: &str = "label_key";
    pub const SERVICE: &str = "service";
    pub const SOURCE: &str = "source";
    pub const RELEASE_COMMIT: &str = "release_commit";
    pub const RELEASE_NONCE: &str = "release_nonce";
    pub const OPERATION_KIND: &str = "operation_kind";
    pub const COMPONENT: &str = "component";
    pub const TASK_TYPE: &str = "task_type";
    pub const BLOCKING_LANE: &str = "blocking_lane";
}

/// Stable event identifiers consumed by structured-log exporters and guards.
pub mod events {
    pub const AUTH_DECISION: &str = "rocketmq.auth.decision";
    pub const AUTH_RELOAD: &str = "rocketmq.auth.reload";
    pub const MCP_ACTION: &str = "rocketmq.mcp.action";
    pub const TASK_LIFECYCLE: &str = "rocketmq.task.lifecycle";
    pub const RECOVERY_STATE: &str = "rocketmq.recovery.state";
    pub const EXPORTER_DROP: &str = "rocketmq.exporter.drop";
    pub const EXPORTER_SHUTDOWN: &str = "rocketmq.exporter.shutdown";
    pub const BROKER_LIFECYCLE: &str = "rocketmq.broker.lifecycle";
    pub const RUNTIME_LIFECYCLE: &str = "rocketmq.runtime.lifecycle";
    pub const CONTROLLER_HEARTBEAT: &str = "rocketmq.controller.heartbeat";
    pub const CONTROLLER_ELECTION: &str = "rocketmq.controller.election";
}

pub mod trace {
    pub const TRACEPARENT: &str = "traceparent";
    pub const TRACESTATE: &str = "tracestate";
    pub const MESSAGING_MESSAGE_ID: &str = "messaging.message.id";
    pub const MESSAGING_MESSAGE_BODY_SIZE: &str = "messaging.message.body.size";
    pub const MESSAGING_ROCKETMQ_MESSAGE_KEYS: &str = "messaging.rocketmq.message.keys";
}
