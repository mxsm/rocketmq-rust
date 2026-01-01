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

use crate::rpc::topic_request_header::TopicRequestHeader;

#[derive(Clone, Debug, Serialize, Deserialize, Default, RequestHeaderCodecV2)]
pub struct GetRouteInfoRequestHeader {
    #[required]
    pub topic: CheetahString,

    #[serde(rename = "acceptStandardJsonOnly")]
    pub accept_standard_json_only: Option<bool>,

    #[serde(flatten)]
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
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn get_route_info_request_header_with_required_topic() {
        let header = crate::protocol::header::client_request_header::GetRouteInfoRequestHeader {
            topic: CheetahString::from("testTopic"),
            accept_standard_json_only: Some(true),
            topic_request_header: None,
        };
        assert_eq!(header.topic, CheetahString::from("testTopic"));
        assert_eq!(header.accept_standard_json_only, Some(true));
        assert!(header.topic_request_header.is_none());
    }

    #[test]
    fn get_route_info_request_header_with_optional_fields() {
        let header = crate::protocol::header::client_request_header::GetRouteInfoRequestHeader {
            topic: CheetahString::from("testTopic"),
            accept_standard_json_only: None,
            topic_request_header: Some(TopicRequestHeader::default()),
        };
        assert_eq!(header.topic, CheetahString::from("testTopic"));
        assert!(header.accept_standard_json_only.is_none());
        assert!(header.topic_request_header.is_some());
    }

    #[test]
    fn get_route_info_request_header_with_empty_topic() {
        let header = crate::protocol::header::client_request_header::GetRouteInfoRequestHeader {
            topic: CheetahString::from(""),
            accept_standard_json_only: Some(false),
            topic_request_header: None,
        };
        assert_eq!(header.topic, CheetahString::from(""));
        assert_eq!(header.accept_standard_json_only, Some(false));
        assert!(header.topic_request_header.is_none());
    }

    #[test]
    fn get_route_info_request_header_with_long_topic() {
        let long_topic = "a".repeat(1000);
        let header = crate::protocol::header::client_request_header::GetRouteInfoRequestHeader {
            topic: CheetahString::from(&long_topic),
            accept_standard_json_only: Some(true),
            topic_request_header: None,
        };
        assert_eq!(header.topic, CheetahString::from(&long_topic));
        assert_eq!(header.accept_standard_json_only, Some(true));
        assert!(header.topic_request_header.is_none());
    }
}
