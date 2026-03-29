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

use std::fmt::Display;

use cheetah_string::CheetahString;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

use crate::rpc::rpc_request_header::RpcRequestHeader;

#[derive(Clone, Debug, Serialize, Deserialize, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct PopLiteMessageRequestHeader {
    #[required]
    pub client_id: CheetahString,
    #[required]
    pub consumer_group: CheetahString,
    #[required]
    pub topic: CheetahString,
    #[required]
    pub max_msg_num: i32,
    #[required]
    pub invisible_time: i64,
    #[required]
    pub poll_time: i64,
    #[required]
    pub born_time: i64,
    pub attempt_id: Option<CheetahString>,

    #[serde(flatten)]
    pub rpc: Option<RpcRequestHeader>,
}

impl PopLiteMessageRequestHeader {
    pub fn is_timeout_too_much(&self) -> bool {
        current_millis() as i64 - self.born_time - self.poll_time > 500
    }
}

impl Default for PopLiteMessageRequestHeader {
    fn default() -> Self {
        Self {
            client_id: CheetahString::new(),
            consumer_group: CheetahString::new(),
            topic: CheetahString::new(),
            max_msg_num: 0,
            invisible_time: 0,
            poll_time: 0,
            born_time: 0,
            attempt_id: None,
            rpc: None,
        }
    }
}

impl Display for PopLiteMessageRequestHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PopLiteMessageRequestHeader [client_id={}, consumer_group={}, topic={}, max_msg_num={}, \
             invisible_time={}, poll_time={}, born_time={}, attempt_id={}]",
            self.client_id,
            self.consumer_group,
            self.topic,
            self.max_msg_num,
            self.invisible_time,
            self.poll_time,
            self.born_time,
            self.attempt_id.as_ref().unwrap_or(&CheetahString::new())
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn pop_lite_message_request_header_serializes_to_map() {
        let header = PopLiteMessageRequestHeader {
            client_id: "client".into(),
            consumer_group: "group".into(),
            topic: "topic".into(),
            max_msg_num: 32,
            invisible_time: 1000,
            poll_time: 3000,
            born_time: current_millis() as i64,
            attempt_id: Some("attempt".into()),
            rpc: None,
        };

        let map = header.to_map().unwrap();
        assert_eq!(map.get(&CheetahString::from_static_str("clientId")).unwrap(), "client");
        assert_eq!(
            map.get(&CheetahString::from_static_str("consumerGroup")).unwrap(),
            "group"
        );
        assert_eq!(map.get(&CheetahString::from_static_str("topic")).unwrap(), "topic");
        assert_eq!(map.get(&CheetahString::from_static_str("maxMsgNum")).unwrap(), "32");
    }

    #[test]
    fn pop_lite_message_request_header_deserializes_from_map() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("clientId"),
            CheetahString::from_static_str("client"),
        );
        map.insert(
            CheetahString::from_static_str("consumerGroup"),
            CheetahString::from_static_str("group"),
        );
        map.insert(
            CheetahString::from_static_str("topic"),
            CheetahString::from_static_str("topic"),
        );
        map.insert(
            CheetahString::from_static_str("maxMsgNum"),
            CheetahString::from_static_str("16"),
        );
        map.insert(
            CheetahString::from_static_str("invisibleTime"),
            CheetahString::from_static_str("1000"),
        );
        map.insert(
            CheetahString::from_static_str("pollTime"),
            CheetahString::from_static_str("3000"),
        );
        map.insert(
            CheetahString::from_static_str("bornTime"),
            CheetahString::from_static_str("100"),
        );

        let header = <PopLiteMessageRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.client_id, "client");
        assert_eq!(header.consumer_group, "group");
        assert_eq!(header.topic, "topic");
        assert_eq!(header.max_msg_num, 16);
        assert_eq!(header.invisible_time, 1000);
        assert_eq!(header.poll_time, 3000);
        assert_eq!(header.born_time, 100);
    }
}
