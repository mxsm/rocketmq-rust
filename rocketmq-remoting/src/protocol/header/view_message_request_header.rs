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

#[derive(Debug, Clone, Serialize, Deserialize, RequestHeaderCodecV2, Default)]
pub struct ViewMessageRequestHeader {
    pub topic: Option<CheetahString>,
    pub offset: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_view_message_request_header_with_none_topic() {
        let header = ViewMessageRequestHeader {
            topic: None,
            offset: 100,
        };

        assert!(header.topic.is_none());
        assert_eq!(header.offset, 100);
    }

    #[test]
    fn test_view_message_request_header_with_topic_value() {
        let topic_name = CheetahString::from("test_topic");
        let header = ViewMessageRequestHeader {
            topic: Some(topic_name.clone()),
            offset: 200,
        };

        assert!(header.topic.is_some());
        assert_eq!(header.topic.unwrap(), topic_name);
        assert_eq!(header.offset, 200);
    }
}
