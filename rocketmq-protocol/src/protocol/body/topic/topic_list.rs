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
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TopicList {
    pub topic_list: Vec<CheetahString>,
    pub broker_addr: Option<CheetahString>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_contract_uses_java_field_names() {
        let body = TopicList {
            topic_list: vec!["topic-a".into(), "topic-b".into()],
            broker_addr: Some("127.0.0.1:10911".into()),
        };

        let value = serde_json::to_value(&body).expect("serialize topic list");
        assert_eq!(
            value,
            serde_json::json!({
                "topicList": ["topic-a", "topic-b"],
                "brokerAddr": "127.0.0.1:10911"
            })
        );

        let decoded: TopicList = serde_json::from_value(value).expect("deserialize topic list");
        assert_eq!(decoded.topic_list, body.topic_list);
        assert_eq!(decoded.broker_addr, body.broker_addr);
    }
}
