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
#[serde(rename_all = "camelCase")]
pub struct NotificationResponseHeader {
    #[required]
    pub has_msg: bool,
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn test_notification_response_header_initialization_true() {
        let header = NotificationResponseHeader { has_msg: true };
        assert!(header.has_msg);
    }

    #[test]
    fn test_notification_response_header_initialization_false() {
        let header = NotificationResponseHeader { has_msg: false };
        assert!(!header.has_msg);
    }

    #[test]
    fn test_notification_response_header_serialize_true() {
        let header = NotificationResponseHeader { has_msg: true };
        let serialized = serde_json::to_string(&header).unwrap();
        assert_eq!(serialized, r#"{"hasMsg":true}"#);
    }

    #[test]
    fn test_notification_response_header_serialize_false() {
        let header = NotificationResponseHeader { has_msg: false };
        let serialized = serde_json::to_string(&header).unwrap();
        assert_eq!(serialized, r#"{"hasMsg":false}"#);
    }

    #[test]
    fn test_notification_response_header_deserialize_true() {
        let json = r#"{"hasMsg":true}"#;
        let header: NotificationResponseHeader = serde_json::from_str(json).unwrap();
        assert!(header.has_msg);
    }

    #[test]
    fn test_notification_response_header_deserialize_false() {
        let json = r#"{"hasMsg":false}"#;
        let header: NotificationResponseHeader = serde_json::from_str(json).unwrap();
        assert!(!header.has_msg);
    }

    #[test]
    fn test_notification_response_header_roundtrip_true() {
        let original = NotificationResponseHeader { has_msg: true };
        let serialized = serde_json::to_string(&original).unwrap();
        let deserialized: NotificationResponseHeader = serde_json::from_str(&serialized).unwrap();
        assert_eq!(original.has_msg, deserialized.has_msg);
    }

    #[test]
    fn test_notification_response_header_roundtrip_false() {
        let original = NotificationResponseHeader { has_msg: false };
        let serialized = serde_json::to_string(&original).unwrap();
        let deserialized: NotificationResponseHeader = serde_json::from_str(&serialized).unwrap();
        assert_eq!(original.has_msg, deserialized.has_msg);
    }

    #[test]
    fn test_notification_response_header_field_accessibility() {
        let mut header = NotificationResponseHeader { has_msg: false };
        assert!(!header.has_msg);
        header.has_msg = true;
        assert!(header.has_msg);
    }

    #[test]
    fn test_notification_response_header_default() {
        let header = NotificationResponseHeader::default();
        assert!(!header.has_msg);
    }

    #[test]
    fn test_notification_response_header_clone() {
        let original = NotificationResponseHeader { has_msg: true };
        let cloned = original.clone();
        assert_eq!(original.has_msg, cloned.has_msg);
        assert!(cloned.has_msg);
    }

    #[test]
    fn test_notification_response_header_debug() {
        let header = NotificationResponseHeader { has_msg: true };
        let debug_str = format!("{:?}", header);
        assert_eq!(debug_str, "NotificationResponseHeader { has_msg: true }");
    }

    #[test]
    fn test_notification_response_header_debug_false() {
        let header = NotificationResponseHeader { has_msg: false };
        let debug_str = format!("{:?}", header);
        assert_eq!(debug_str, "NotificationResponseHeader { has_msg: false }");
    }

    #[test]
    fn test_notification_response_header_camel_case_serialization() {
        let header = NotificationResponseHeader { has_msg: true };
        let serialized = serde_json::to_string(&header).unwrap();
        // Verify that the field is serialized with camelCase
        assert!(serialized.contains("hasMsg"));
        assert!(!serialized.contains("has_msg"));
    }

    #[test]
    fn test_notification_response_header_camel_case_deserialization() {
        // Test that it can deserialize from camelCase
        let json_camel = r#"{"hasMsg":true}"#;
        let header: NotificationResponseHeader = serde_json::from_str(json_camel).unwrap();
        assert!(header.has_msg);
    }
}
