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

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TopicOffset {
    min_offset: i64,
    max_offset: i64,
    last_update_timestamp: i64,
}

impl TopicOffset {
    pub fn new() -> Self {
        Self {
            min_offset: 0,
            max_offset: 0,
            last_update_timestamp: 0,
        }
    }

    pub fn get_min_offset(&self) -> i64 {
        self.min_offset
    }

    pub fn set_min_offset(&mut self, min_offset: i64) {
        self.min_offset = min_offset;
    }

    pub fn get_max_offset(&self) -> i64 {
        self.max_offset
    }

    pub fn set_max_offset(&mut self, max_offset: i64) {
        self.max_offset = max_offset;
    }

    pub fn get_last_update_timestamp(&self) -> i64 {
        self.last_update_timestamp
    }

    pub fn set_last_update_timestamp(&mut self, last_update_timestamp: i64) {
        self.last_update_timestamp = last_update_timestamp;
    }
}

impl std::fmt::Display for TopicOffset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TopicOffset{{min_offset={}, max_offset={}, last_update_timestamp={}}}",
            self.min_offset, self.max_offset, self.last_update_timestamp
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_offset_default_and_new() {
        let offset = TopicOffset::default();
        assert_eq!(offset.get_min_offset(), 0);
        assert_eq!(offset.get_max_offset(), 0);
        assert_eq!(offset.get_last_update_timestamp(), 0);

        let offset = TopicOffset::new();
        assert_eq!(offset.get_min_offset(), 0);
        assert_eq!(offset.get_max_offset(), 0);
        assert_eq!(offset.get_last_update_timestamp(), 0);
    }

    #[test]
    fn test_topic_offset_setters_and_getters() {
        let mut offset = TopicOffset::new();
        offset.set_min_offset(10);
        offset.set_max_offset(20);
        offset.set_last_update_timestamp(1000);

        assert_eq!(offset.get_min_offset(), 10);
        assert_eq!(offset.get_max_offset(), 20);
        assert_eq!(offset.get_last_update_timestamp(), 1000);
    }

    #[test]
    fn test_topic_offset_serialization_and_deserialization() {
        let mut offset = TopicOffset::new();
        offset.set_min_offset(-10);
        offset.set_max_offset(-20);
        offset.set_last_update_timestamp(-1000);

        let serialized = serde_json::to_string(&offset).unwrap();
        let expected = r#"{"minOffset":-10,"maxOffset":-20,"lastUpdateTimestamp":-1000}"#;
        assert_eq!(serialized, expected);

        let deserialized: TopicOffset = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.get_min_offset(), -10);
        assert_eq!(deserialized.get_max_offset(), -20);
        assert_eq!(deserialized.get_last_update_timestamp(), -1000);
    }

    #[test]
    fn test_topic_offset_display() {
        let mut offset = TopicOffset::new();
        offset.set_min_offset(10);
        offset.set_max_offset(20);
        offset.set_last_update_timestamp(1000);

        let display = format!("{}", offset);
        assert_eq!(
            display,
            "TopicOffset{min_offset=10, max_offset=20, last_update_timestamp=1000}"
        );
    }
}
