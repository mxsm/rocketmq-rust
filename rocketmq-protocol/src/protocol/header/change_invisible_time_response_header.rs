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

#[derive(Serialize, Deserialize, Debug, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct ChangeInvisibleTimeResponseHeader {
    #[required]
    pub pop_time: u64,

    #[required]
    pub revive_qid: i32,

    #[required]
    pub invisible_time: i64,
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn change_invisible_time_response_header_display_format() {
        let header = ChangeInvisibleTimeResponseHeader {
            pop_time: 123456789,
            revive_qid: 1,
            invisible_time: 987654321,
        };
        assert_eq!(
            format!("{:?}", header),
            "ChangeInvisibleTimeResponseHeader { pop_time: 123456789, revive_qid: 1, invisible_time: 987654321 }"
        );
    }

    #[test]
    fn change_invisible_time_response_header_serialize() {
        let header = ChangeInvisibleTimeResponseHeader {
            pop_time: 123456789,
            revive_qid: 1,
            invisible_time: 987654321,
        };
        let serialized = serde_json::to_string(&header).unwrap();
        assert_eq!(
            serialized,
            r#"{"popTime":123456789,"reviveQid":1,"invisibleTime":987654321}"#
        );
    }

    #[test]
    fn change_invisible_time_response_header_deserialize() {
        let json = r#"{"popTime":123456789,"reviveQid":1,"invisibleTime":987654321}"#;
        let header: ChangeInvisibleTimeResponseHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.pop_time, 123456789);
        assert_eq!(header.revive_qid, 1);
        assert_eq!(header.invisible_time, 987654321);
    }

    #[test]
    fn change_invisible_time_response_header_default() {
        let header = ChangeInvisibleTimeResponseHeader::default();
        assert_eq!(header.pop_time, 0);
        assert_eq!(header.revive_qid, 0);
        assert_eq!(header.invisible_time, 0);
    }
}
