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

use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodecV3;
use serde::Deserialize;
use serde::Serialize;

use crate::rpc::topic_request_header::TopicRequestHeader;

#[derive(Clone, Debug, Default, Serialize, Deserialize, RequestHeaderCodecV3)]
#[header(
    type_id = "rocketmq_protocol::protocol::header::query_consume_queue_request_header::QueryConsumeQueueRequestHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.QueryConsumeQueueRequestHeader",
    lookup = "scan",
    fast
)]
#[serde(rename_all = "camelCase")]
pub struct QueryConsumeQueueRequestHeader {
    #[header(default, default_semantic = "literal:")]
    pub topic: CheetahString,

    #[header(default, default_semantic = "literal:0")]
    pub queue_id: i32,

    #[header(default, default_semantic = "literal:0")]
    pub index: i64,

    #[header(default, default_semantic = "literal:0")]
    pub count: i32,

    pub consumer_group: Option<CheetahString>,

    #[serde(flatten)]
    #[header(flatten, presence = "always")]
    pub topic_request_header: Option<TopicRequestHeader>,
}
