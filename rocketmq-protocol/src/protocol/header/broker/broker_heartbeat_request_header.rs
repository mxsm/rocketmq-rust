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

#[derive(Debug, Serialize, Deserialize, Default, RequestHeaderCodecV3)]
#[header(
    type_id = "rocketmq_protocol::protocol::header::broker::broker_heartbeat_request_header::BrokerHeartbeatRequestHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.namesrv.BrokerHeartbeatRequestHeader"
)]
pub struct BrokerHeartbeatRequestHeader {
    #[serde(rename = "clusterName")]
    #[header(required)]
    pub cluster_name: CheetahString,

    #[serde(rename = "brokerAddr")]
    #[header(required)]
    pub broker_addr: CheetahString,

    #[serde(rename = "brokerName")]
    #[header(required)]
    pub broker_name: CheetahString,

    #[serde(rename = "brokerId")]
    pub broker_id: Option<i64>,

    pub epoch: Option<i32>,

    #[serde(rename = "maxOffset")]
    pub max_offset: Option<i64>,

    #[serde(rename = "confirmOffset")]
    pub confirm_offset: Option<i64>,

    #[serde(rename = "heartbeatTimeoutMills")]
    pub heartbeat_timeout_mills: Option<i64>,

    #[serde(rename = "electionPriority")]
    pub election_priority: Option<i32>,
}
