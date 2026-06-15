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
}
