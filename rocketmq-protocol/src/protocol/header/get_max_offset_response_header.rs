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
pub struct GetMaxOffsetResponseHeader {
    pub offset: i64,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::*;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn get_max_offset_response_header_default() {
        let header = GetMaxOffsetResponseHeader::default();
        assert_eq!(header.offset, 0);
    }

    #[test]
    fn get_max_offset_response_header_serialization() {
        let header = GetMaxOffsetResponseHeader { offset: 12345 };
        let json = serde_json::to_string(&header).unwrap();
        assert_eq!(json, r#"{"offset":12345}"#);
    }

    #[test]
    fn get_max_offset_response_header_deserialization() {
        let json = r#"{"offset":12345}"#;
        let header: GetMaxOffsetResponseHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.offset, 12345);
    }

    #[test]
    fn get_max_offset_response_header_from_map() {
        let mut map = HashMap::new();
        map.insert(CheetahString::from("offset"), CheetahString::from("12345"));
        let header = <GetMaxOffsetResponseHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.offset, 12345);
    }
}
