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
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Default, Serialize, Deserialize, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct PopLiteMessageResponseHeader {
    #[required]
    pub pop_time: i64,
    #[required]
    pub invisible_time: i64,
    #[required]
    pub revive_qid: i32,
    pub start_offset_info: Option<CheetahString>,
    pub msg_offset_info: Option<CheetahString>,
    pub order_count_info: Option<CheetahString>,
}

impl Display for PopLiteMessageResponseHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PopLiteMessageResponseHeader [pop_time={}, invisible_time={}, revive_qid={}, start_offset_info={:?}, \
             msg_offset_info={:?}, order_count_info={:?}]",
            self.pop_time,
            self.invisible_time,
            self.revive_qid,
            self.start_offset_info,
            self.msg_offset_info,
            self.order_count_info
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;

    #[test]
    fn pop_lite_message_response_header_serializes_to_map() {
        let header = PopLiteMessageResponseHeader {
            pop_time: 123,
            invisible_time: 456,
            revive_qid: 7,
            start_offset_info: Some("start".into()),
            msg_offset_info: Some("msg".into()),
            order_count_info: Some("order".into()),
        };

        let map = header.to_map().unwrap();
        assert_eq!(map.get(&CheetahString::from_static_str("popTime")).unwrap(), "123");
        assert_eq!(
            map.get(&CheetahString::from_static_str("invisibleTime")).unwrap(),
            "456"
        );
        assert_eq!(map.get(&CheetahString::from_static_str("reviveQid")).unwrap(), "7");
    }

    #[test]
    fn pop_lite_message_response_header_display_contains_fields() {
        let header = PopLiteMessageResponseHeader {
            pop_time: 1,
            invisible_time: 2,
            revive_qid: 3,
            start_offset_info: None,
            msg_offset_info: None,
            order_count_info: None,
        };

        let display = format!("{header}");
        assert!(display.contains("pop_time=1"));
        assert!(display.contains("invisible_time=2"));
        assert!(display.contains("revive_qid=3"));
    }
}
