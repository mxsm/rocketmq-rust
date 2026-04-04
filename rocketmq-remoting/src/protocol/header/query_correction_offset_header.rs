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

#[derive(Clone, Debug, Default, Serialize, Deserialize, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct QueryCorrectionOffsetHeader {
    pub filter_groups: Option<CheetahString>,
    #[required]
    pub compare_group: CheetahString,
    #[required]
    pub topic: CheetahString,
    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::QueryCorrectionOffsetHeader;

    #[test]
    fn query_correction_offset_header_round_trips() {
        let header = QueryCorrectionOffsetHeader {
            filter_groups: Some(CheetahString::from_static_str("group-a,group-b")),
            compare_group: CheetahString::from_static_str("group-c"),
            topic: CheetahString::from_static_str("topic-a"),
            topic_request_header: None,
        };

        let json = serde_json::to_string(&header).expect("serialize query correction offset header");
        let decoded: QueryCorrectionOffsetHeader =
            serde_json::from_str(&json).expect("deserialize query correction offset header");
        assert_eq!(decoded.filter_groups.as_deref(), Some("group-a,group-b"));
        assert_eq!(decoded.compare_group, "group-c");
        assert_eq!(decoded.topic, "topic-a");
    }
}
