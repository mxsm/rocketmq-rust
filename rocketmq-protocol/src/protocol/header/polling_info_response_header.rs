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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct PollingInfoResponseHeader {
    #[required]
    pub polling_num: i32,
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn test_polling_info_response_header_default() {
        let header = PollingInfoResponseHeader::default();
        assert_eq!(header.polling_num, 0);
    }

    #[test]
    fn test_polling_info_response_header_new_with_positive_value() {
        let header = PollingInfoResponseHeader { polling_num: 100 };
        assert_eq!(header.polling_num, 100);
    }

    #[test]
    fn test_polling_info_response_header_new_with_negative_value() {
        let header = PollingInfoResponseHeader { polling_num: -1 };
        assert_eq!(header.polling_num, -1);
    }

    #[test]
    fn test_polling_info_response_header_new_with_zero() {
        let header = PollingInfoResponseHeader { polling_num: 0 };
        assert_eq!(header.polling_num, 0);
    }

    #[test]
    fn test_polling_info_response_header_new_with_max_value() {
        let header = PollingInfoResponseHeader { polling_num: i32::MAX };
        assert_eq!(header.polling_num, i32::MAX);
    }

    #[test]
    fn test_polling_info_response_header_new_with_min_value() {
        let header = PollingInfoResponseHeader { polling_num: i32::MIN };
        assert_eq!(header.polling_num, i32::MIN);
    }

    #[test]
    fn test_polling_info_response_header_clone() {
        let header1 = PollingInfoResponseHeader { polling_num: 42 };
        let header2 = header1.clone();
        assert_eq!(header1, header2);
        assert_eq!(header2.polling_num, 42);
    }

    #[test]
    fn test_polling_info_response_header_equality() {
        let header1 = PollingInfoResponseHeader { polling_num: 100 };
        let header2 = PollingInfoResponseHeader { polling_num: 100 };
        let header3 = PollingInfoResponseHeader { polling_num: 200 };

        assert_eq!(header1, header2);
        assert_ne!(header1, header3);
    }

    #[test]
    fn test_polling_info_response_header_debug() {
        let header = PollingInfoResponseHeader { polling_num: 123 };
        let debug_str = format!("{:?}", header);
        assert!(debug_str.contains("PollingInfoResponseHeader"));
        assert!(debug_str.contains("123"));
    }

    #[test]
    fn test_polling_info_response_header_serialize_to_json() {
        let header = PollingInfoResponseHeader { polling_num: 99 };
        let json = serde_json::to_string(&header).unwrap();
        assert_eq!(json, r#"{"pollingNum":99}"#);
    }

    #[test]
    fn test_polling_info_response_header_serialize_with_zero() {
        let header = PollingInfoResponseHeader { polling_num: 0 };
        let json = serde_json::to_string(&header).unwrap();
        assert_eq!(json, r#"{"pollingNum":0}"#);
    }

    #[test]
    fn test_polling_info_response_header_serialize_with_negative() {
        let header = PollingInfoResponseHeader { polling_num: -50 };
        let json = serde_json::to_string(&header).unwrap();
        assert_eq!(json, r#"{"pollingNum":-50}"#);
    }

    #[test]
    fn test_polling_info_response_header_deserialize_from_json() {
        let json = r#"{"pollingNum":88}"#;
        let header: PollingInfoResponseHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.polling_num, 88);
    }

    #[test]
    fn test_polling_info_response_header_deserialize_with_zero() {
        let json = r#"{"pollingNum":0}"#;
        let header: PollingInfoResponseHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.polling_num, 0);
    }

    #[test]
    fn test_polling_info_response_header_deserialize_with_negative() {
        let json = r#"{"pollingNum":-100}"#;
        let header: PollingInfoResponseHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.polling_num, -100);
    }

    #[test]
    fn test_polling_info_response_header_serialize_deserialize_roundtrip() {
        let original = PollingInfoResponseHeader { polling_num: 777 };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: PollingInfoResponseHeader = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_polling_info_response_header_serialize_deserialize_max_value() {
        let original = PollingInfoResponseHeader { polling_num: i32::MAX };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: PollingInfoResponseHeader = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_polling_info_response_header_serialize_deserialize_min_value() {
        let original = PollingInfoResponseHeader { polling_num: i32::MIN };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: PollingInfoResponseHeader = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_polling_info_response_header_camel_case_field_name() {
        let header = PollingInfoResponseHeader { polling_num: 50 };
        let json = serde_json::to_string(&header).unwrap();
        // 验证字段名是 camelCase 格式（pollingNum 而不是 polling_num）
        assert!(json.contains("pollingNum"));
        assert!(!json.contains("polling_num"));
    }

    #[test]
    fn test_polling_info_response_header_deserialize_invalid_json() {
        let invalid_json = r#"{"invalidField":123}"#;
        let result: Result<PollingInfoResponseHeader, _> = serde_json::from_str(invalid_json);
        // 应该失败，因为缺少必需的 pollingNum 字段
        assert!(result.is_err());
    }

    #[test]
    fn test_polling_info_response_header_deserialize_wrong_type() {
        let invalid_json = r#"{"pollingNum":"not_a_number"}"#;
        let result: Result<PollingInfoResponseHeader, _> = serde_json::from_str(invalid_json);
        // 应该失败，因为类型不匹配
        assert!(result.is_err());
    }

    #[test]
    fn test_polling_info_response_header_multiple_instances() {
        let headers = [
            PollingInfoResponseHeader { polling_num: 1 },
            PollingInfoResponseHeader { polling_num: 2 },
            PollingInfoResponseHeader { polling_num: 3 },
        ];

        assert_eq!(headers.len(), 3);
        assert_eq!(headers[0].polling_num, 1);
        assert_eq!(headers[1].polling_num, 2);
        assert_eq!(headers[2].polling_num, 3);
    }

    #[test]
    fn test_polling_info_response_header_mutation() {
        let mut header = PollingInfoResponseHeader { polling_num: 10 };
        assert_eq!(header.polling_num, 10);

        header.polling_num = 20;
        assert_eq!(header.polling_num, 20);

        header.polling_num += 5;
        assert_eq!(header.polling_num, 25);
    }

    #[test]
    fn test_polling_info_response_header_pretty_print_json() {
        let header = PollingInfoResponseHeader { polling_num: 456 };
        let json = serde_json::to_string_pretty(&header).unwrap();
        assert!(json.contains("pollingNum"));
        assert!(json.contains("456"));
    }

    #[test]
    fn test_polling_info_response_header_from_value() {
        let json_value = serde_json::json!({
            "pollingNum": 888
        });
        let header: PollingInfoResponseHeader = serde_json::from_value(json_value).unwrap();
        assert_eq!(header.polling_num, 888);
    }

    #[test]
    fn test_polling_info_response_header_to_value() {
        let header = PollingInfoResponseHeader { polling_num: 999 };
        let json_value = serde_json::to_value(&header).unwrap();
        assert_eq!(json_value["pollingNum"], 999);
    }

    #[test]
    fn test_polling_info_response_header_typical_use_cases() {
        // 测试典型的轮询场景
        let no_polling = PollingInfoResponseHeader { polling_num: 0 };
        let low_polling = PollingInfoResponseHeader { polling_num: 5 };
        let medium_polling = PollingInfoResponseHeader { polling_num: 50 };
        let high_polling = PollingInfoResponseHeader { polling_num: 500 };

        assert_eq!(no_polling.polling_num, 0);
        assert_eq!(low_polling.polling_num, 5);
        assert_eq!(medium_polling.polling_num, 50);
        assert_eq!(high_polling.polling_num, 500);
    }
}
