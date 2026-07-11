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
#[serde(rename_all = "camelCase")]
pub struct UpdateGroupForbiddenRequestHeader {
    #[required]
    pub group: CheetahString,

    #[required]
    pub topic: CheetahString,

    pub readable: Option<bool>,

    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn update_group_forbidden_request_header_deserializes_from_map() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("group"),
            CheetahString::from_static_str("group-a"),
        );
        map.insert(
            CheetahString::from_static_str("topic"),
            CheetahString::from_static_str("topic-a"),
        );
        map.insert(
            CheetahString::from_static_str("readable"),
            CheetahString::from_static_str("false"),
        );

        let header = <UpdateGroupForbiddenRequestHeader as FromMap>::from(&map).expect("header from map");
        assert_eq!(header.group, CheetahString::from_static_str("group-a"));
        assert_eq!(header.topic, CheetahString::from_static_str("topic-a"));
        assert_eq!(header.readable, Some(false));
    }
}
