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
    pub const MESSAGES_IN_TOTAL: &str = "rocketmq_messages_in_total";
    pub const MESSAGES_OUT_TOTAL: &str = "rocketmq_messages_out_total";
    pub const THROUGHPUT_IN_TOTAL: &str = "rocketmq_throughput_in_total";
    pub const THROUGHPUT_OUT_TOTAL: &str = "rocketmq_throughput_out_total";
    pub const MESSAGE_SIZE: &str = "rocketmq_message_size";
    pub const SEND_MESSAGE_LATENCY: &str = "rocketmq_send_message_latency";
    pub const METRICS_LABEL_DROPPED_TOTAL: &str = "rocketmq_metrics_label_dropped_total";
    pub const STORE_APPEND_LATENCY: &str = "rocketmq_store_append_latency";
    pub const STORE_FLUSH_LATENCY: &str = "rocketmq_store_flush_latency";
    pub const STORE_DISPATCH_LATENCY: &str = "rocketmq_store_dispatch_latency";
    pub const STORE_DISK_USAGE: &str = "rocketmq_store_disk_usage";
    pub const REMOTING_REQUESTS_TOTAL: &str = "rocketmq_remoting_requests_total";
    pub const REMOTING_REQUEST_LATENCY: &str = "rocketmq_remoting_request_latency";
    pub const REMOTING_NETWORK_BYTES: &str = "rocketmq_remoting_network_bytes";
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
    pub const PROXY_GRPC_REQUESTS_TOTAL: &str = "rocketmq_proxy_grpc_requests_total";
    pub const PROXY_GRPC_REQUEST_LATENCY: &str = "rocketmq_proxy_grpc_request_latency";
    pub const PROXY_FORWARD_LATENCY: &str = "rocketmq_proxy_forward_latency";
    pub const PROXY_ACTIVE_CONNECTIONS: &str = "rocketmq_proxy_active_connections";
}

pub mod labels {
    pub const CLUSTER: &str = "cluster";
    pub const NODE_TYPE: &str = "node_type";
    pub const NODE_ID: &str = "node_id";
    pub const TOPIC: &str = "topic";
    pub const CONSUMER_GROUP: &str = "consumer_group";
    pub const INVOCATION_STATUS: &str = "invocation_status";
}

pub mod trace {
    pub const TRACEPARENT: &str = "traceparent";
    pub const TRACESTATE: &str = "tracestate";
    pub const BAGGAGE: &str = "baggage";
    pub const MESSAGING_MESSAGE_ID: &str = "messaging.message.id";
    pub const MESSAGING_MESSAGE_BODY_SIZE: &str = "messaging.message.body.size";
    pub const MESSAGING_ROCKETMQ_MESSAGE_KEYS: &str = "messaging.rocketmq.message.keys";
}
