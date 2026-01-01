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

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodecV2)]
pub struct GetEarliestMsgStoretimeResponseHeader {
    #[required]
    pub timestamp: i64,
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn get_earliest_msg_storetime_response_header_display_format() {
        let header = GetEarliestMsgStoretimeResponseHeader { timestamp: 1234567890 };
        assert_eq!(
            format!("{:?}", header),
            "GetEarliestMsgStoretimeResponseHeader { timestamp: 1234567890 }"
        );
    }

    #[test]
    fn get_earliest_msg_storetime_response_header_serialize() {
        let header = GetEarliestMsgStoretimeResponseHeader { timestamp: 1234567890 };
        let serialized = serde_json::to_string(&header).unwrap();
        assert_eq!(serialized, r#"{"timestamp":1234567890}"#);
    }

    #[test]
    fn get_earliest_msg_storetime_response_header_deserialize() {
        let json = r#"{"timestamp":1234567890}"#;
        let header: GetEarliestMsgStoretimeResponseHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.timestamp, 1234567890);
    }

    #[test]
    fn get_earliest_msg_storetime_response_header_default() {
        let header = GetEarliestMsgStoretimeResponseHeader::default();
        assert_eq!(header.timestamp, 0);
    }
}
