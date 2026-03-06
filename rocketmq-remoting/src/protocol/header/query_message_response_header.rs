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

#[derive(Debug, Clone, Serialize, Deserialize, RequestHeaderCodecV2, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
#[serde(default)]
pub struct QueryMessageResponseHeader {
    pub index_last_update_timestamp: i64,
    pub index_last_update_phyoffset: i64,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::mem;

    use crate::protocol::remoting_command::RemotingCommand;
    use crate::CommandCustomHeader;
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn explicit_construction_sets_fields_correctly() {
        let header = QueryMessageResponseHeader {
            index_last_update_timestamp: 10,
            index_last_update_phyoffset: 20,
        };

        assert_eq!(header.index_last_update_timestamp, 10);
        assert_eq!(header.index_last_update_phyoffset, 20);
    }

    #[test]
    fn query_message_response_header_default_values_are_zero() {
        let header = QueryMessageResponseHeader::default();

        assert_eq!(header.index_last_update_timestamp, 0);
        assert_eq!(header.index_last_update_phyoffset, 0);
    }

    #[test]
    fn clone_creates_independent_copy() {
        let original = QueryMessageResponseHeader {
            index_last_update_timestamp: 100,
            index_last_update_phyoffset: 200,
        };

        let mut cloned = original.clone();
        cloned.index_last_update_timestamp = 999;

        assert_eq!(original.index_last_update_timestamp, 100);
        assert_eq!(cloned.index_last_update_timestamp, 999);

        assert_eq!(cloned.index_last_update_phyoffset, 200);
    }

    #[test]
    fn debug_format_contains_field_names_and_values() {
        let header = QueryMessageResponseHeader {
            index_last_update_timestamp: 1,
            index_last_update_phyoffset: 2,
        };

        let debug_output = format!("{:?}", header);

        assert!(debug_output.contains("index_last_update_timestamp"));
        assert!(debug_output.contains("index_last_update_phyoffset"));
        assert!(debug_output.contains("1"));
        assert!(debug_output.contains("2"));
    }

    #[test]
    fn timestamp_with_positive_value() {
        let header = QueryMessageResponseHeader {
            index_last_update_phyoffset: 0,
            index_last_update_timestamp: 42,
        };

        assert_eq!(header.index_last_update_timestamp, 42);

        let map = header.to_map().unwrap();

        assert_eq!(
            map.get(&CheetahString::from_static_str("indexLastUpdateTimestamp"))
                .unwrap(),
            "42"
        );
    }

    #[test]
    fn timestamp_with_zero() {
        let header = QueryMessageResponseHeader {
            index_last_update_timestamp: 0,
            index_last_update_phyoffset: 0,
        };

        let map = header.to_map().unwrap();

        assert_eq!(
            map.get(&CheetahString::from_static_str("indexLastUpdateTimestamp"))
                .unwrap(),
            "0"
        );
    }

    #[test]
    fn timestamp_with_negative_value() {
        let header = QueryMessageResponseHeader {
            index_last_update_timestamp: -456,
            index_last_update_phyoffset: 0,
        };

        let map = header.to_map().unwrap();

        assert_eq!(
            map.get(&CheetahString::from_static_str("indexLastUpdateTimestamp"))
                .unwrap(),
            "-456"
        );
    }

    #[test]
    fn timestamp_with_i64_min() {
        let header = QueryMessageResponseHeader {
            index_last_update_timestamp: i64::MIN,
            index_last_update_phyoffset: 0,
        };

        let map = header.to_map().unwrap();

        assert_eq!(
            map.get(&CheetahString::from_static_str("indexLastUpdateTimestamp"))
                .unwrap()
                .to_string(),
            i64::MIN.to_string()
        );
    }

    #[test]
    fn phyoffset_with_positive_value() {
        let header = QueryMessageResponseHeader {
            index_last_update_phyoffset: 123,
            index_last_update_timestamp: 0,
        };

        let map = header.to_map().unwrap();

        assert_eq!(
            map.get(&CheetahString::from_static_str("indexLastUpdatePhyoffset"))
                .unwrap(),
            "123"
        );
    }

    #[test]
    fn phyoffset_with_zero() {
        let header = QueryMessageResponseHeader {
            index_last_update_phyoffset: 0,
            index_last_update_timestamp: 0,
        };

        let map = header.to_map().unwrap();

        assert_eq!(
            map.get(&CheetahString::from_static_str("indexLastUpdatePhyoffset"))
                .unwrap(),
            "0"
        );
    }

    #[test]
    fn phyoffset_with_negative_value() {
        let header = QueryMessageResponseHeader {
            index_last_update_phyoffset: -456,
            index_last_update_timestamp: 0,
        };

        let map = header.to_map().unwrap();

        assert_eq!(
            map.get(&CheetahString::from_static_str("indexLastUpdatePhyoffset"))
                .unwrap(),
            "-456"
        );
    }

    #[test]
    fn phyoffset_with_i64_min() {
        let header = QueryMessageResponseHeader {
            index_last_update_phyoffset: i64::MIN,
            index_last_update_timestamp: 0,
        };

        let map = header.to_map().unwrap();

        assert_eq!(
            map.get(&CheetahString::from_static_str("indexLastUpdatePhyoffset"))
                .unwrap()
                .to_string(),
            i64::MIN.to_string()
        );
    }

    #[test]
    fn phyoffset_with_i64_max() {
        let header = QueryMessageResponseHeader {
            index_last_update_phyoffset: i64::MAX,
            index_last_update_timestamp: 0,
        };

        let map = header.to_map().unwrap();

        assert_eq!(
            map.get(&CheetahString::from_static_str("indexLastUpdatePhyoffset"))
                .unwrap()
                .to_string(),
            i64::MAX.to_string()
        );
    }

    #[test]
    fn serde_json_serialization() {
        let header = QueryMessageResponseHeader {
            index_last_update_timestamp: 131415,
            index_last_update_phyoffset: 131496,
        };

        let json = serde_json::to_string(&header).unwrap();
        assert_eq!(
            json,
            r#"{"indexLastUpdateTimestamp":131415,"indexLastUpdatePhyoffset":131496}"#
        );
    }

    #[test]
    fn serde_json_deserialization() {
        let json = r#"{"indexLastUpdateTimestamp":131415,"indexLastUpdatePhyoffset":131496}"#;
        let header: QueryMessageResponseHeader = serde_json::from_str(json).unwrap();

        assert_eq!(header.index_last_update_timestamp, 131415);
        assert_eq!(header.index_last_update_phyoffset, 131496);
    }

    #[test]
    fn serde_json_round_trip() {
        let header = QueryMessageResponseHeader {
            index_last_update_timestamp: 131415,
            index_last_update_phyoffset: 131496,
        };

        let json = serde_json::to_string(&header).unwrap();
        let deserialized_header: QueryMessageResponseHeader = serde_json::from_str(&json).unwrap();

        assert_eq!(header, deserialized_header);
    }

    #[test]
    fn serde_json_deserialization_with_missing_fields() {
        let json = "{}";
        let header: QueryMessageResponseHeader = serde_json::from_str(json).unwrap();

        assert_eq!(header.index_last_update_timestamp, 0);
        assert_eq!(header.index_last_update_phyoffset, 0);
    }

    #[test]
    fn serde_json_deserialization_with_extra_fields() {
        let json = r#"{"indexLastUpdateTimestamp":131415,"indexLastUpdatePhyoffset":131496,"extraField":"extraValue"}"#;
        let header: QueryMessageResponseHeader = serde_json::from_str(json).unwrap();

        assert_eq!(header.index_last_update_timestamp, 131415);
        assert_eq!(header.index_last_update_phyoffset, 131496);
    }

    #[test]
    fn test_encoding_of_the_header_to_remoting_command_format() {
        let header = QueryMessageResponseHeader {
            index_last_update_timestamp: 131415,
            index_last_update_phyoffset: 131496,
        };

        let mut command = RemotingCommand::default();
        command.set_command_custom_header_ref(header);
        command.make_custom_header_to_net();

        let ext_fields = command.ext_fields().unwrap();

        assert_eq!(
            ext_fields
                .get(&CheetahString::from_static_str("indexLastUpdateTimestamp"))
                .unwrap(),
            "131415"
        );
        assert_eq!(
            ext_fields
                .get(&CheetahString::from_static_str("indexLastUpdatePhyoffset"))
                .unwrap(),
            "131496"
        )
    }

    #[test]
    fn test_decoding_of_the_header_from_remoting_command_format() {
        let mut command = RemotingCommand::default().set_ext_fields(HashMap::new());
        command.add_ext_field("indexLastUpdateTimestamp", "131415");
        command.add_ext_field("indexLastUpdatePhyoffset", "131496");

        let header: QueryMessageResponseHeader = command.decode_command_custom_header().unwrap();

        assert_eq!(header.index_last_update_timestamp, 131415);
        assert_eq!(header.index_last_update_phyoffset, 131496);
    }

    #[test]
    fn test_codec_compatibility_with_query_message_response() {
        let mut command = RemotingCommand::default().set_ext_fields(HashMap::new());
        command.add_ext_field("indexLastUpdateTimestamp", "131415");
        command.add_ext_field("indexLastUpdatePhyoffset", "131496");

        let header_fast: QueryMessageResponseHeader = command.decode_command_custom_header_fast().unwrap();

        let header_normal: QueryMessageResponseHeader = command.decode_command_custom_header().unwrap();

        assert_eq!(header_fast, header_normal);
        assert_eq!(header_fast.index_last_update_timestamp, 131415);
        assert_eq!(header_fast.index_last_update_phyoffset, 131496);
    }
    #[test]
    fn test_codec_behavior_with_various_timestamp_and_offset_values() {
        let test_cases = vec![
            (0, 0),
            (-1, -1),
            (i64::MAX, i64::MAX),
            (i64::MIN, i64::MIN),
            (1234567890, 987654321),
            (-1234567890, -987654321),
        ];

        for (timestamp, offset) in test_cases {
            let mut command = RemotingCommand::default().set_ext_fields(HashMap::new());
            command.add_ext_field("indexLastUpdateTimestamp", timestamp.to_string());
            command.add_ext_field("indexLastUpdatePhyoffset", offset.to_string());

            let header: QueryMessageResponseHeader = command.decode_command_custom_header_fast().unwrap();

            assert_eq!(header.index_last_update_timestamp, timestamp);
            assert_eq!(header.index_last_update_phyoffset, offset);
        }
    }

    #[test]
    fn query_message_response_header_deserialization_malformed_json() {
        let json = r#"{"indexLastUpdateTimestamp":"not_a_number"}"#;
        let result: Result<QueryMessageResponseHeader, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn query_message_response_header_deserialization_with_wrong_data_type_for_timestamp() {
        let json = r#"{"indexLastUpdateTimestamp":true,"indexLastUpdatePhyoffset":131496}"#;
        let result: Result<QueryMessageResponseHeader, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn query_message_response_header_deserialization_with_wrong_data_type_for_phyoffset() {
        let json = r#"{"indexLastUpdateTimestamp":131415,"indexLastUpdatePhyoffset":"string_instead_of_number"}"#;
        let result: Result<QueryMessageResponseHeader, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn query_message_response_header_struct_size() {
        // Two i64 fields (8 bytes each) = 16 bytes total size
        assert_eq!(mem::size_of::<QueryMessageResponseHeader>(), 16);
    }

    #[test]
    fn test_compatibility_with_brokers_query_message_processor_usage() {
        let header = QueryMessageResponseHeader {
            index_last_update_timestamp: 123456789,
            index_last_update_phyoffset: 987654321,
        };

        let mut command = RemotingCommand::create_response_command_with_header(header);
        command.make_custom_header_to_net();

        let ext_fields = command.ext_fields().unwrap();
        assert_eq!(
            ext_fields
                .get(&CheetahString::from_static_str("indexLastUpdateTimestamp"))
                .unwrap(),
            "123456789"
        );
        assert_eq!(
            ext_fields
                .get(&CheetahString::from_static_str("indexLastUpdatePhyoffset"))
                .unwrap(),
            "987654321"
        );
    }

    #[test]
    fn test_usage_in_remoting_command_create_response_command_with_header() {
        let header = QueryMessageResponseHeader {
            index_last_update_timestamp: 11111,
            index_last_update_phyoffset: 22222,
        };

        let command = RemotingCommand::create_response_command_with_header(header);

        let extracted_header = command.read_custom_header_ref::<QueryMessageResponseHeader>().unwrap();

        assert_eq!(extracted_header.index_last_update_timestamp, 11111);
        assert_eq!(extracted_header.index_last_update_phyoffset, 22222);
    }

    #[test]
    fn test_read_custom_header_mut_integration() {
        let header = QueryMessageResponseHeader {
            index_last_update_timestamp: 100,
            index_last_update_phyoffset: 200,
        };

        let mut command = RemotingCommand::create_response_command_with_header(header);

        if let Some(mut_header) = command.read_custom_header_mut::<QueryMessageResponseHeader>() {
            mut_header.index_last_update_timestamp = 300;
            mut_header.index_last_update_phyoffset = 400;
        }

        let extracted_header = command.read_custom_header_ref::<QueryMessageResponseHeader>().unwrap();
        assert_eq!(extracted_header.index_last_update_timestamp, 300);
        assert_eq!(extracted_header.index_last_update_phyoffset, 400);
    }

    #[test]
    fn test_default_usage_in_response_creation() {
        let header = QueryMessageResponseHeader::default();
        let mut command = RemotingCommand::create_response_command_with_header(header);

        let extracted_header = command.read_custom_header_ref::<QueryMessageResponseHeader>().unwrap();
        assert_eq!(extracted_header.index_last_update_timestamp, 0);
        assert_eq!(extracted_header.index_last_update_phyoffset, 0);

        if let Some(mut_header) = command.read_custom_header_mut::<QueryMessageResponseHeader>() {
            mut_header.index_last_update_timestamp = 1314;
        }

        let updated_header = command.read_custom_header_ref::<QueryMessageResponseHeader>().unwrap();
        assert_eq!(updated_header.index_last_update_timestamp, 1314);
        assert_eq!(updated_header.index_last_update_phyoffset, 0);
    }

    // Timestamp and Offset Relationship Tests
    #[test]
    fn test_both_fields_set_to_same_value() {
        let header = QueryMessageResponseHeader {
            index_last_update_timestamp: 12345,
            index_last_update_phyoffset: 12345,
        };
        assert_eq!(header.index_last_update_timestamp, header.index_last_update_phyoffset);
    }

    #[test]
    fn test_timestamp_greater_than_phyoffset_normal_case() {
        let header = QueryMessageResponseHeader {
            index_last_update_timestamp: 1684300000000,
            index_last_update_phyoffset: 50000,
        };
        assert!(header.index_last_update_timestamp > header.index_last_update_phyoffset);
    }

    #[test]
    fn test_timestamp_less_than_phyoffset() {
        let header = QueryMessageResponseHeader {
            index_last_update_timestamp: 1000,
            index_last_update_phyoffset: 999999999,
        };
        assert!(header.index_last_update_timestamp < header.index_last_update_phyoffset);
    }

    #[test]
    fn test_both_fields_set_to_zero_initial_state() {
        let header = QueryMessageResponseHeader {
            index_last_update_timestamp: 0,
            index_last_update_phyoffset: 0,
        };
        assert_eq!(header.index_last_update_timestamp, 0);
        assert_eq!(header.index_last_update_phyoffset, 0);

        let default_header = QueryMessageResponseHeader::default();
        assert_eq!(header, default_header);
    }

    #[test]
    fn test_updating_index_last_update_timestamp_after_creation() {
        let mut header = QueryMessageResponseHeader {
            index_last_update_timestamp: 100,
            index_last_update_phyoffset: 0,
        };

        header.index_last_update_timestamp = 500;
        assert_eq!(header.index_last_update_timestamp, 500);
        assert_eq!(header.index_last_update_phyoffset, 0);
    }

    #[test]
    fn test_updating_index_last_update_phyoffset_after_creation() {
        let mut header = QueryMessageResponseHeader {
            index_last_update_timestamp: 0,
            index_last_update_phyoffset: 200,
        };

        header.index_last_update_phyoffset = 800;
        assert_eq!(header.index_last_update_phyoffset, 800);
        assert_eq!(header.index_last_update_timestamp, 0);
    }

    #[test]
    fn test_updating_both_fields_simultaneously() {
        let mut header = QueryMessageResponseHeader {
            index_last_update_timestamp: 10,
            index_last_update_phyoffset: 20,
        };

        header.index_last_update_timestamp = 999;
        header.index_last_update_phyoffset = 888;

        assert_eq!(header.index_last_update_timestamp, 999);
        assert_eq!(header.index_last_update_phyoffset, 888);
    }
}
