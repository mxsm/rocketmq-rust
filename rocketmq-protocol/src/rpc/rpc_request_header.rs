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

#[derive(Clone, Debug, Serialize, Deserialize, Default, RequestHeaderCodecV2)]
pub struct RpcRequestHeader {
    // the namespace name
    #[serde(rename = "namespace")]
    pub namespace: Option<CheetahString>,
    // if the data has been namespaced
    #[serde(rename = "namespaced")]
    pub namespaced: Option<bool>,
    // the abstract remote addr name, usually the physical broker name
    #[serde(rename = "brokerName")]
    pub broker_name: Option<CheetahString>,
    // oneway
    #[serde(rename = "oneway")]
    pub oneway: Option<bool>,
}

impl RpcRequestHeader {
    pub fn new(
        namespace: Option<CheetahString>,
        namespaced: Option<bool>,
        broker_name: Option<CheetahString>,
        oneway: Option<bool>,
    ) -> Self {
        Self {
            namespace,
            namespaced,
            broker_name,
            oneway,
        }
    }
}
