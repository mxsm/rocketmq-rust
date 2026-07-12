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
    fn topic_list_creation_empty() {
        let topic_list = TopicList::default();
        assert!(topic_list.topic_list.is_empty());
        assert!(topic_list.broker_addr.is_none());
    }

    #[test]
    fn topic_list_creation_with_data() {
        let topic_list = TopicList {
            topic_list: vec!["topic1".into(), "topic2".into()],
            broker_addr: Some("broker1".into()),
        };
        assert_eq!(
            topic_list.topic_list,
            vec![<&str as Into<CheetahString>>::into("topic1"), "topic2".into()]
        );
        assert_eq!(topic_list.broker_addr, Some("broker1".into()));
    }
}
