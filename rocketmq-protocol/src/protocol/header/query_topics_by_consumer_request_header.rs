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

#[derive(Serialize, Deserialize, Debug, RequestHeaderCodecV3, Default)]
#[header(
    type_id = "rocketmq_protocol::protocol::header::query_topics_by_consumer_request_header::QueryTopicsByConsumerRequestHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.QueryTopicsByConsumerRequestHeader"
)]
pub struct QueryTopicsByConsumerRequestHeader {
    #[header(required)]
    #[serde(rename = "group")]
    pub group: CheetahString,

    #[serde(flatten)]
    #[header(flatten, presence = "always")]
    pub rpc_request_header: Option<RpcRequestHeader>,
}

impl QueryTopicsByConsumerRequestHeader {
    pub fn new(group: impl Into<CheetahString>) -> Self {
        Self {
            group: group.into(),
            rpc_request_header: None,
        }
    }

    pub fn get_group(&self) -> &CheetahString {
        &self.group
    }

    pub fn set_group(&mut self, group: CheetahString) {
        self.group = group;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn serde_flattens_the_java_rpc_keys() {
        let mut header = QueryTopicsByConsumerRequestHeader::new("initial-group");
        assert_eq!(header.get_group(), "initial-group");
        assert!(header.rpc_request_header.is_none());

        header.set_group(CheetahString::from("group-a"));
        header.rpc_request_header = Some(RpcRequestHeader {
            namespace: Some(CheetahString::from("namespace-a")),
            namespaced: Some(true),
            broker_name: None,
            oneway: None,
        });
        let value = serde_json::to_value(&header).unwrap();

        assert_eq!(
            value,
            serde_json::json!({
                "group": "group-a",
                "ns": "namespace-a",
                "nsd": true,
                "bname": null,
                "oway": null
            })
        );

        let decoded: QueryTopicsByConsumerRequestHeader = serde_json::from_value(value).unwrap();
        assert_eq!(decoded.get_group(), "group-a");
        assert_eq!(
            decoded.rpc_request_header.unwrap().namespace.as_deref(),
            Some("namespace-a")
        );
    }

    #[test]
    fn v3_codec_requires_the_group_and_decodes_the_rpc_envelope() {
        let map = HashMap::from([
            ("group".into(), "group-a".into()),
            ("namespace".into(), "namespace-a".into()),
        ]);
        let decoded = <QueryTopicsByConsumerRequestHeader as FromMap>::from(&map).unwrap();

        assert_eq!(decoded.group, "group-a");
        assert_eq!(
            decoded.rpc_request_header.unwrap().namespace.as_deref(),
            Some("namespace-a")
        );
        assert!(<QueryTopicsByConsumerRequestHeader as FromMap>::from(&HashMap::new()).is_err());
    }
}
