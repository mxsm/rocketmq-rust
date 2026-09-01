// Copyright 2026 The RocketMQ Rust Authors
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

/// One exact queue reset guarded by the offset observed during preflight.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, RequestHeaderCodecV3)]
#[header(
    type_id = "rocketmq_protocol::protocol::header::update_consumer_offset_conditional_header::UpdateConsumerOffsetConditionalHeader"
)]
#[serde(rename_all = "camelCase")]
pub struct UpdateConsumerOffsetConditionalHeader {
    #[header(required)]
    pub consumer_group: CheetahString,
    #[header(required)]
    pub topic: CheetahString,
    #[header(required)]
    pub queue_id: i32,
    #[header(required)]
    pub expected_offset: i64,
    #[header(required)]
    pub new_offset: i64,
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::UpdateConsumerOffsetConditionalHeader;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn exact_offset_condition_round_trips() {
        let header = UpdateConsumerOffsetConditionalHeader {
            consumer_group: "group-a".into(),
            topic: "topic-a".into(),
            queue_id: 3,
            expected_offset: 41,
            new_offset: 12,
        };
        let map = header.to_map().expect("header should encode");
        assert_eq!(map.get("expectedOffset").map(CheetahString::as_str), Some("41"));
        assert_eq!(map.get("newOffset").map(CheetahString::as_str), Some("12"));
        assert_eq!(
            <UpdateConsumerOffsetConditionalHeader as FromMap>::from(&map).expect("header should decode"),
            header
        );
    }
}
