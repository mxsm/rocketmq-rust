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
pub struct GetMinOffsetResponseHeader {
    pub offset: i64,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;
    use serde_json;

    use super::*;
    use crate::protocol::command_custom_header::FromMap;

    // Basic Structure Tests
    #[test]
    fn get_min_offset_response_header_new_with_value() {
        let header = GetMinOffsetResponseHeader { offset: 100 };
        assert_eq!(header.offset, 100);
    }

    #[test]
    fn get_min_offset_response_header_default() {
        let header = GetMinOffsetResponseHeader::default();
        assert_eq!(header.offset, 0);
    }

    #[test]
    fn get_min_offset_response_header_clone() {
        let header1 = GetMinOffsetResponseHeader { offset: 12345 };
        let header2 = header1.clone();
        assert_eq!(header1.offset, header2.offset);
        assert_eq!(header2.offset, 12345);
    }

    #[test]
    fn get_min_offset_response_header_debug_format() {
        let header = GetMinOffsetResponseHeader { offset: 1234567890 };
        assert_eq!(
            format!("{:?}", header),
            "GetMinOffsetResponseHeader { offset: 1234567890 }"
        );
    }

    // Serialization & Deserialization Tests
    #[test]
    fn get_min_offset_response_header_serialization() {
        let header = GetMinOffsetResponseHeader { offset: 12345 };
        let json = serde_json::to_string(&header).unwrap();
        assert_eq!(json, r#"{"offset":12345}"#);
    }

    #[test]
    fn get_min_offset_response_header_deserialization() {
        let json = r#"{"offset":12345}"#;
        let header: GetMinOffsetResponseHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.offset, 12345);
    }

    #[test]
    fn get_min_offset_response_header_round_trip() {
        let original = GetMinOffsetResponseHeader { offset: 999999 };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: GetMinOffsetResponseHeader = serde_json::from_str(&json).unwrap();
        assert_eq!(original.offset, deserialized.offset);
    }

    #[test]
    fn get_min_offset_response_header_deserialization_with_missing_fields() {
        let json = r#"{}"#;
        let result: Result<GetMinOffsetResponseHeader, _> = serde_json::from_str(json);
        assert!(result.is_err()); // field is required by serde
    }

    #[test]
    fn get_min_offset_response_header_deserialization_with_extra_fields() {
        let json = r#"{"offset":12345,"extraField":"ignored"}"#;
        let header: GetMinOffsetResponseHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.offset, 12345);
    }

    // Offset Value Tests
    #[test]
    fn get_min_offset_response_header_zero_offset() {
        let header = GetMinOffsetResponseHeader { offset: 0 };
        assert_eq!(header.offset, 0);
    }

    #[test]
    fn get_min_offset_response_header_positive_offset_values() {
        let test_values = vec![1, 100, 1000, 10000, 1000000];
        for value in test_values {
            let header = GetMinOffsetResponseHeader { offset: value };
            assert_eq!(header.offset, value);
        }
    }

    #[test]
    fn get_min_offset_response_header_negative_offset() {
        let header = GetMinOffsetResponseHeader { offset: -1 };
        assert_eq!(header.offset, -1);
    }

    #[test]
    fn get_min_offset_response_header_max_offset() {
        let header = GetMinOffsetResponseHeader { offset: i64::MAX };
        assert_eq!(header.offset, i64::MAX);
    }

    #[test]
    fn get_min_offset_response_header_min_offset() {
        let header = GetMinOffsetResponseHeader { offset: i64::MIN };
        assert_eq!(header.offset, i64::MIN);
    }

    #[test]
    fn get_min_offset_response_header_offset_preservation_after_serialization() {
        let test_values = vec![0, -1, 100, i64::MAX, i64::MIN];
        for value in test_values {
            let header = GetMinOffsetResponseHeader { offset: value };
            let json = serde_json::to_string(&header).unwrap();
            let deserialized: GetMinOffsetResponseHeader = serde_json::from_str(&json).unwrap();
            assert_eq!(header.offset, deserialized.offset);
        }
    }

    // Integration with RequestHeaderCodecV2 Macro Tests
    #[test]
    fn get_min_offset_response_header_from_map() {
        let mut map = HashMap::new();
        map.insert(CheetahString::from("offset"), CheetahString::from("12345"));
        let header = <GetMinOffsetResponseHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.offset, 12345);
    }

    #[test]
    fn get_min_offset_response_header_from_map_with_negative_offset() {
        let mut map = HashMap::new();
        map.insert(CheetahString::from("offset"), CheetahString::from("-1"));
        let header = <GetMinOffsetResponseHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.offset, -1);
    }

    #[test]
    fn get_min_offset_response_header_from_map_with_max_offset() {
        let mut map = HashMap::new();
        map.insert(CheetahString::from("offset"), CheetahString::from(i64::MAX.to_string()));
        let header = <GetMinOffsetResponseHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.offset, i64::MAX);
    }

    #[test]
    fn get_min_offset_response_header_from_empty_map() {
        let map = HashMap::new();
        let header = <GetMinOffsetResponseHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.offset, 0); // should use default
    }

    // Edge Cases & Error Handling Tests
    #[test]
    fn get_min_offset_response_header_deserialization_malformed_json() {
        let json = r#"{"offset":"not_a_number"}"#;
        let result: Result<GetMinOffsetResponseHeader, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn get_min_offset_response_header_deserialization_invalid_json() {
        let json = r#"{"offset":}"#;
        let result: Result<GetMinOffsetResponseHeader, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn get_min_offset_response_header_from_map_invalid_offset() {
        let mut map = HashMap::new();
        map.insert(CheetahString::from("offset"), CheetahString::from("not_a_number"));
        let result = <GetMinOffsetResponseHeader as FromMap>::from(&map);
        // The macro-generated FromMap uses parse() which defaults to 0 on parse failure for non-required
        // fields
        assert_eq!(result.unwrap().offset, 0);
    }

    #[test]
    fn get_min_offset_response_header_struct_size() {
        use std::mem;
        assert_eq!(mem::size_of::<GetMinOffsetResponseHeader>(), 8);
    }
}
