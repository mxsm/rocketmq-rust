// Copyright 2026 The RocketMQ Rust Authors
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
use serde::Deserialize;
use serde::Serialize;

/// Java-compatible body for deleting multiple Broker topics in one request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteTopicListRequestBody {
    pub topic_list: Vec<CheetahString>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_list_body_uses_java_wire_field() {
        let body = DeleteTopicListRequestBody {
            topic_list: vec![
                CheetahString::from_static_str("TopicA"),
                CheetahString::from_static_str("TopicB"),
            ],
        };

        let encoded = serde_json::to_string(&body).expect("encode topic list");
        assert_eq!(r#"{"topicList":["TopicA","TopicB"]}"#, encoded);
        assert_eq!(body, serde_json::from_str(&encoded).expect("decode topic list"));
    }
}
