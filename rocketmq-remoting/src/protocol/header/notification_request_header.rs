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
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::header::namesrv::topic_operation_header::TopicRequestHeader;

#[derive(Debug, Serialize, Deserialize, RequestHeaderCodecV2)]
pub struct NotificationRequestHeader {
    #[serde(rename = "consumerGroup")]
    #[required]
    pub consumer_group: CheetahString,

    #[serde(rename = "topic")]
    #[required]
    pub topic: CheetahString,

    #[serde(rename = "queueId")]
    #[required]
    pub queue_id: i32,

    #[serde(rename = "pollTime")]
    #[required]
    pub poll_time: i64,

    #[serde(rename = "bornTime")]
    #[required]
    pub born_time: i64,

    /// Indicates if the message is ordered; defaults to false.
    #[serde(default)]
    pub order: bool,

    /// Attempt ID
    #[serde(rename = "attemptId", skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<CheetahString>,

    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn test_notification_request_header_serialization() {
        let header = NotificationRequestHeader {
            consumer_group: CheetahString::from("consumer_group_1"),
            topic: CheetahString::from("test_topic"),
            queue_id: 10,
            poll_time: 1234567890,
            born_time: 1234567891,
            order: true,
            attempt_id: Some(CheetahString::from("attempt_1")),
            topic_request_header: None,
        };

        let serialized = serde_json::to_string(&header).expect("Failed to serialize header");

        let deserialized: NotificationRequestHeader =
            serde_json::from_str(&serialized).expect("Failed to deserialize header");
        assert_eq!(header.queue_id, deserialized.queue_id);
    }

    #[test]
    fn test_notification_request_header_default_order() {
        let header = NotificationRequestHeader {
            consumer_group: CheetahString::from("consumer_group_1"),
            topic: CheetahString::from("test_topic"),
            queue_id: 10,
            poll_time: 1234567890,
            born_time: 1234567891,
            order: false, // Defaults to false
            attempt_id: None,
            topic_request_header: None,
        };

        let serialized = serde_json::to_string(&header).expect("Failed to serialize header");

        let deserialized: NotificationRequestHeader =
            serde_json::from_str(&serialized).expect("Failed to deserialize header");
        assert_eq!(header.queue_id, deserialized.queue_id);
    }
}
