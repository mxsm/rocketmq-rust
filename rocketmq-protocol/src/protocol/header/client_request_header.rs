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

#[derive(Clone, Debug, Serialize, Deserialize, Default, RequestHeaderCodecV3)]
#[header(
    type_id = "rocketmq_protocol::protocol::header::client_request_header::GetRouteInfoRequestHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader"
)]
pub struct GetRouteInfoRequestHeader {
    #[header(required)]
    pub topic: CheetahString,

    #[serde(rename = "acceptStandardJsonOnly")]
    pub accept_standard_json_only: Option<bool>,

    #[serde(flatten)]
    #[header(flatten, presence = "always")]
    pub topic_request_header: Option<TopicRequestHeader>,
}

impl GetRouteInfoRequestHeader {
    pub fn new(topic: impl Into<CheetahString>, accept_standard_json_only: Option<bool>) -> Self {
        GetRouteInfoRequestHeader {
            topic: topic.into(),
            accept_standard_json_only,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_sets_its_arguments_and_leaves_the_rpc_envelope_absent() {
        let header = GetRouteInfoRequestHeader::new("topic-a", Some(true));

        assert_eq!(header.topic, "topic-a");
        assert_eq!(header.accept_standard_json_only, Some(true));
        assert!(header.topic_request_header.is_none());
    }
}
