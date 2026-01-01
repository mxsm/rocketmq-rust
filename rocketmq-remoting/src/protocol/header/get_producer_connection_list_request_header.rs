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

#[derive(Serialize, Deserialize, Debug, RequestHeaderCodecV2, Clone, Default)]
pub struct GetProducerConnectionListRequestHeader {
    #[required]
    #[serde(rename = "producerGroup")]
    pub producer_group: CheetahString,

    #[serde(flatten)]
    pub rpc_request_header: Option<RpcRequestHeader>,
}

impl GetProducerConnectionListRequestHeader {
    pub fn producer_group(&self) -> &CheetahString {
        &self.producer_group
    }

    pub fn set_producer_group(&mut self, producer_group: CheetahString) {
        self.producer_group = producer_group;
    }
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn get_producer_connection_list_request_header_serialize() {
        let mut header = GetProducerConnectionListRequestHeader::default();
        header.set_producer_group(CheetahString::from_static_str("test_group"));
        let json = serde_json::to_string(&header).unwrap();
        assert_eq!(json, r#"{"producerGroup":"test_group"}"#);

        let mut header = GetProducerConnectionListRequestHeader::default();
        header.set_producer_group(CheetahString::from_static_str("test_group"));
        let rpc_header = RpcRequestHeader {
            broker_name: Some(CheetahString::from_static_str("broker_a")),
            ..Default::default()
        };
        header.rpc_request_header = Some(rpc_header);
        let json = serde_json::to_string(&header).unwrap();
        assert!(json.contains("\"producerGroup\":\"test_group\""));
        assert!(json.contains("\"brokerName\":\"broker_a\""));
    }

    #[test]
    fn get_producer_connection_list_request_header_deserialize() {
        let json = r#"{"producerGroup":"test_group"}"#;
        let header: GetProducerConnectionListRequestHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.producer_group(), "test_group");
    }

    #[test]
    fn get_producer_connection_list_request_header_default() {
        let header = GetProducerConnectionListRequestHeader::default();
        assert_eq!(header.producer_group(), "");
    }

    #[test]
    fn get_producer_connection_list_request_header_clone() {
        let mut header = GetProducerConnectionListRequestHeader::default();
        header.set_producer_group(CheetahString::from_static_str("test_group"));
        let cloned = header.clone();
        assert_eq!(cloned.producer_group(), header.producer_group());
    }
}
