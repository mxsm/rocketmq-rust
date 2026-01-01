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
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

use crate::rpc::rpc_request_header::RpcRequestHeader;

/// Request header for getting consumer status from client.
#[derive(Clone, Debug, Serialize, Deserialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct GetConsumerStatusRequestHeader {
    #[required]
    pub topic: CheetahString,
    #[required]
    pub group: CheetahString,
    pub client_addr: Option<CheetahString>,

    #[serde(flatten)]
    pub rpc_request_header: Option<RpcRequestHeader>,
}

impl GetConsumerStatusRequestHeader {
    pub fn new(topic: CheetahString, group: CheetahString) -> Self {
        Self {
            topic,
            group,
            client_addr: None,
            rpc_request_header: None,
        }
    }
}
