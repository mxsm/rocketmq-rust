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

#[derive(Debug, Serialize, Deserialize, Default, RequestHeaderCodecV2, Clone)]
pub struct PopMessageResponseHeader {
    #[serde(rename = "popTime")]
    #[required]
    pub pop_time: u64,

    #[serde(rename = "invisibleTime")]
    #[required]
    pub invisible_time: u64,

    #[serde(rename = "reviveQid")]
    #[required]
    pub revive_qid: u32,

    #[serde(rename = "restNum")]
    #[required]
    pub rest_num: u64,

    #[serde(rename = "startOffsetInfo", skip_serializing_if = "Option::is_none")]
    pub start_offset_info: Option<CheetahString>,

    #[serde(rename = "msgOffsetInfo", skip_serializing_if = "Option::is_none")]
    pub msg_offset_info: Option<CheetahString>,

    #[serde(rename = "orderCountInfo", skip_serializing_if = "Option::is_none")]
    pub order_count_info: Option<CheetahString>,
}

impl Display for PopMessageResponseHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PopMessageResponseHeader [pop_time={}, invisible_time={}, revive_qid={}, rest_num={}, \
             start_offset_info={:?}, msg_offset_info={:?}, order_count_info={:?}]",
            self.pop_time,
            self.invisible_time,
            self.revive_qid,
            self.rest_num,
            self.start_offset_info,
            self.msg_offset_info,
            self.order_count_info
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_formatting() {
        let header = PopMessageResponseHeader {
            pop_time: 123456789,
            invisible_time: 987654321,
            revive_qid: 42,
            rest_num: 10,
            start_offset_info: Some("start_offset".into()),
            msg_offset_info: Some("msg_offset".into()),
            order_count_info: Some("order_count".into()),
        };
        let expected = "PopMessageResponseHeader [pop_time=123456789, invisible_time=987654321, revive_qid=42, \
                        rest_num=10, start_offset_info=Some(\"start_offset\"), msg_offset_info=Some(\"msg_offset\"), \
                        order_count_info=Some(\"order_count\")]";
        assert_eq!(format!("{}", header), expected);
    }

    #[test]
    fn display_formatting_with_none_values() {
        let header = PopMessageResponseHeader {
            pop_time: 123456789,
            invisible_time: 987654321,
            revive_qid: 42,
            rest_num: 10,
            start_offset_info: None,
            msg_offset_info: None,
            order_count_info: None,
        };
        let expected = "PopMessageResponseHeader [pop_time=123456789, invisible_time=987654321, revive_qid=42, \
                        rest_num=10, start_offset_info=None, msg_offset_info=None, order_count_info=None]";
        assert_eq!(format!("{}", header), expected);
    }

    #[test]
    fn serialize_to_json() {
        let header = PopMessageResponseHeader {
            pop_time: 123456789,
            invisible_time: 987654321,
            revive_qid: 42,
            rest_num: 10,
            start_offset_info: Some("start_offset".into()),
            msg_offset_info: Some("msg_offset".into()),
            order_count_info: Some("order_count".into()),
        };
        let json = serde_json::to_string(&header).unwrap();
        let expected = r#"{"popTime":123456789,"invisibleTime":987654321,"reviveQid":42,"restNum":10,"startOffsetInfo":"start_offset","msgOffsetInfo":"msg_offset","orderCountInfo":"order_count"}"#;
        assert_eq!(json, expected);
    }

    #[test]
    fn deserialize_from_json() {
        let json = r#"{"popTime":123456789,"invisibleTime":987654321,"reviveQid":42,"restNum":10,"startOffsetInfo":"start_offset","msgOffsetInfo":"msg_offset","orderCountInfo":"order_count"}"#;
        let header: PopMessageResponseHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.pop_time, 123456789);
        assert_eq!(header.invisible_time, 987654321);
        assert_eq!(header.revive_qid, 42);
        assert_eq!(header.rest_num, 10);
        assert_eq!(header.start_offset_info, Some("start_offset".into()));
        assert_eq!(header.msg_offset_info, Some("msg_offset".into()));
        assert_eq!(header.order_count_info, Some("order_count".into()));
    }

    #[test]
    fn deserialize_from_json_with_none_values() {
        let json = r#"{"popTime":123456789,"invisibleTime":987654321,"reviveQid":42,"restNum":10}"#;
        let header: PopMessageResponseHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.pop_time, 123456789);
        assert_eq!(header.invisible_time, 987654321);
        assert_eq!(header.revive_qid, 42);
        assert_eq!(header.rest_num, 10);
        assert_eq!(header.start_offset_info, None);
        assert_eq!(header.msg_offset_info, None);
        assert_eq!(header.order_count_info, None);
    }
}
