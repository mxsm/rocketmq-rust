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

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_enum::MessageRequestMode;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetMessageRequestModeRequestBody {
    pub topic: CheetahString,
    pub consumer_group: CheetahString,
    pub mode: MessageRequestMode,
    /*
    consumer working in pop mode could share the MessageQueues assigned to
    the N (N = popShareQueueNum) consumers following it in the cid list
     */
    pub pop_share_queue_num: i32,
}

impl Default for SetMessageRequestModeRequestBody {
    fn default() -> Self {
        SetMessageRequestModeRequestBody {
            topic: CheetahString::new(),
            consumer_group: CheetahString::new(),
            mode: MessageRequestMode::Pull,
            pop_share_queue_num: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn serialize_set_message_request_mode_request_body() {
        let body = SetMessageRequestModeRequestBody {
            topic: CheetahString::from("test_topic"),
            consumer_group: CheetahString::from("test_group"),
            mode: MessageRequestMode::Pop,
            pop_share_queue_num: 5,
        };
        let serialized = serde_json::to_string(&body).unwrap();
        assert!(serialized.contains("\"topic\":\"test_topic\""));
        assert!(serialized.contains("\"consumerGroup\":\"test_group\""));
        assert!(serialized.contains("\"mode\":\"POP\""));
        assert!(serialized.contains("\"popShareQueueNum\":5"));
    }

    #[test]
    fn default_set_message_request_mode_request_body() {
        let default_body = SetMessageRequestModeRequestBody::default();
        assert_eq!(default_body.topic, CheetahString::new());
        assert_eq!(default_body.consumer_group, CheetahString::new());
        assert_eq!(default_body.mode, MessageRequestMode::Pull);
        assert_eq!(default_body.pop_share_queue_num, 0);
    }

    #[test]
    fn deserialize_set_message_request_mode_request_body() {
        let json = r#"
            {
            "topic": "test_topic",
            "consumerGroup": "test_group",
            "mode": "PULL",
            "popShareQueueNum": 3
        }"#;
        let deserialized: SetMessageRequestModeRequestBody = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.topic, CheetahString::from("test_topic"));
        assert_eq!(deserialized.consumer_group, CheetahString::from("test_group"));
        assert_eq!(deserialized.mode, MessageRequestMode::Pull);
        assert_eq!(deserialized.pop_share_queue_num, 3);
    }

    #[test]
    fn deserialize_set_message_request_mode_request_body_invalid_mode() {
        let json = r#"{
                                        "topic": "test_topic",
                                        "consumerGroup": "test_group",
                                        "mode": "INVALID",
                                        "popShareQueueNum": 3
                                    }"#;
        let deserialized: Result<SetMessageRequestModeRequestBody, _> = serde_json::from_str(json);
        assert!(deserialized.is_err());
    }
}
