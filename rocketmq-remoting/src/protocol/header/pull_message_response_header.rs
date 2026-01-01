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

use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Default, Clone, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct PullMessageResponseHeader {
    #[required]
    pub suggest_which_broker_id: u64,

    #[required]
    pub next_begin_offset: i64,

    #[required]
    pub min_offset: i64,

    #[required]
    pub max_offset: i64,

    pub offset_delta: Option<i64>,
    pub topic_sys_flag: Option<i32>,
    pub group_sys_flag: Option<i32>,
    pub forbidden_type: Option<i32>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn pull_message_response_header_serializes_correctly() {
        let header = PullMessageResponseHeader {
            suggest_which_broker_id: 123,
            next_begin_offset: 456,
            min_offset: 789,
            max_offset: 101112,
            offset_delta: Some(131415),
            topic_sys_flag: Some(161718),
            group_sys_flag: Some(192021),
            forbidden_type: Some(222324),
        };
        let map = header.to_map().unwrap();
        assert_eq!(
            map.get(&CheetahString::from_static_str("suggestWhichBrokerId"))
                .unwrap(),
            "123"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("nextBeginOffset")).unwrap(),
            "456"
        );
        assert_eq!(map.get(&CheetahString::from_static_str("minOffset")).unwrap(), "789");
        assert_eq!(map.get(&CheetahString::from_static_str("maxOffset")).unwrap(), "101112");
        assert_eq!(
            map.get(&CheetahString::from_static_str("offsetDelta")).unwrap(),
            "131415"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("topicSysFlag")).unwrap(),
            "161718"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("groupSysFlag")).unwrap(),
            "192021"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("forbiddenType")).unwrap(),
            "222324"
        );
    }

    #[test]
    fn pull_message_response_header_deserializes_correctly() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("suggestWhichBrokerId"),
            CheetahString::from_static_str("123"),
        );
        map.insert(
            CheetahString::from_static_str("nextBeginOffset"),
            CheetahString::from_static_str("456"),
        );
        map.insert(
            CheetahString::from_static_str("minOffset"),
            CheetahString::from_static_str("789"),
        );
        map.insert(
            CheetahString::from_static_str("maxOffset"),
            CheetahString::from_static_str("101112"),
        );
        map.insert(
            CheetahString::from_static_str("offsetDelta"),
            CheetahString::from_static_str("131415"),
        );
        map.insert(
            CheetahString::from_static_str("topicSysFlag"),
            CheetahString::from_static_str("161718"),
        );
        map.insert(
            CheetahString::from_static_str("groupSysFlag"),
            CheetahString::from_static_str("192021"),
        );
        map.insert(
            CheetahString::from_static_str("forbiddenType"),
            CheetahString::from_static_str("222324"),
        );

        let header: PullMessageResponseHeader = <PullMessageResponseHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.suggest_which_broker_id, 123);
        assert_eq!(header.next_begin_offset, 456);
        assert_eq!(header.min_offset, 789);
        assert_eq!(header.max_offset, 101112);
        assert_eq!(header.offset_delta.unwrap(), 131415);
        assert_eq!(header.topic_sys_flag.unwrap(), 161718);
        assert_eq!(header.group_sys_flag.unwrap(), 192021);
        assert_eq!(header.forbidden_type.unwrap(), 222324);
    }

    #[test]
    fn pull_message_response_header_handles_missing_optional_fields() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("suggestWhichBrokerId"),
            CheetahString::from_static_str("123"),
        );
        map.insert(
            CheetahString::from_static_str("nextBeginOffset"),
            CheetahString::from_static_str("456"),
        );
        map.insert(
            CheetahString::from_static_str("minOffset"),
            CheetahString::from_static_str("789"),
        );
        map.insert(
            CheetahString::from_static_str("maxOffset"),
            CheetahString::from_static_str("101112"),
        );

        let header = <PullMessageResponseHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.suggest_which_broker_id, 123);
        assert_eq!(header.next_begin_offset, 456);
        assert_eq!(header.min_offset, 789);
        assert_eq!(header.max_offset, 101112);
        assert!(header.offset_delta.is_none());
        assert!(header.topic_sys_flag.is_none());
        assert!(header.group_sys_flag.is_none());
        assert!(header.forbidden_type.is_none());
    }

    #[test]
    fn pull_message_response_header_handles_invalid_data() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("suggestWhichBrokerId"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("nextBeginOffset"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("minOffset"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("maxOffset"),
            CheetahString::from_static_str("invalid"),
        );

        let result = <PullMessageResponseHeader as FromMap>::from(&map);
        assert!(result.is_err());
    }
}
