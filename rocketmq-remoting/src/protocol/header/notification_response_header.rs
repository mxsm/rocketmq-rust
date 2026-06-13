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

    #[serde(default)]
    pub polling_full: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notification_response_header_initializes_java_fields() {
        let header = NotificationResponseHeader {
            has_msg: true,
            polling_full: true,
        };

        assert!(header.has_msg);
        assert!(header.polling_full);
    }

    #[test]
    fn notification_response_header_serializes_java_camel_case_fields() {
        let header = NotificationResponseHeader {
            has_msg: true,
            polling_full: true,
        };

        let serialized = serde_json::to_string(&header).unwrap();

        assert_eq!(serialized, r#"{"hasMsg":true,"pollingFull":true}"#);
        assert!(!serialized.contains("has_msg"));
        assert!(!serialized.contains("polling_full"));
    }

    #[test]
    fn notification_response_header_deserializes_java_camel_case_fields() {
        let json = r#"{"hasMsg":true,"pollingFull":true}"#;

        let header: NotificationResponseHeader = serde_json::from_str(json).unwrap();

        assert!(header.has_msg);
        assert!(header.polling_full);
    }

    #[test]
    fn notification_response_header_defaults_missing_polling_full_like_java() {
        let json = r#"{"hasMsg":true}"#;

        let header: NotificationResponseHeader = serde_json::from_str(json).unwrap();

        assert!(header.has_msg);
        assert!(!header.polling_full);
    }

    #[test]
    fn notification_response_header_default_is_empty() {
        let header = NotificationResponseHeader::default();

        assert!(!header.has_msg);
        assert!(!header.polling_full);
    }
}
