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

use crate::rpc::rpc_request_header::RpcRequestHeader;

#[derive(Serialize, Deserialize, Debug, RequestHeaderCodecV3)]
#[header(
    type_id = "rocketmq_protocol::protocol::header::get_consumer_connection_list_request_header::GetConsumerConnectionListRequestHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.GetConsumerConnectionListRequestHeader"
)]
pub struct GetConsumerConnectionListRequestHeader {
    #[header(required)]
    #[serde(rename = "consumerGroup")]
    pub consumer_group: CheetahString,

    #[serde(flatten)]
    #[header(flatten, presence = "always")]
    pub rpc_request_header: Option<RpcRequestHeader>,
}

impl GetConsumerConnectionListRequestHeader {
    pub fn get_consumer_group(&self) -> &CheetahString {
        &self.consumer_group
    }
    pub fn set_consumer_group(&mut self, consumer_group: CheetahString) {
        self.consumer_group = consumer_group;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_preserves_the_consumer_group() {
        let mut header = GetConsumerConnectionListRequestHeader {
            consumer_group: CheetahString::new(),
            rpc_request_header: None,
        };
        header.set_consumer_group(CheetahString::from("group-a"));

        let json = serde_json::to_string(&header).unwrap();
        assert_eq!(json, r#"{"consumerGroup":"group-a"}"#);

        let decoded: GetConsumerConnectionListRequestHeader = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.get_consumer_group(), "group-a");
    }
}
