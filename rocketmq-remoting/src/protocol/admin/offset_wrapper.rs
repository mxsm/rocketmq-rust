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
pub struct OffsetWrapper {
    broker_offset: i64,
    consumer_offset: i64,
    pull_offset: i64,
    last_timestamp: i64,
}

impl OffsetWrapper {
    pub fn new() -> Self {
        Self {
            broker_offset: 0,
            consumer_offset: 0,
            pull_offset: 0,
            last_timestamp: 0,
        }
    }

    pub fn get_broker_offset(&self) -> i64 {
        self.broker_offset
    }

    pub fn set_broker_offset(&mut self, broker_offset: i64) {
        self.broker_offset = broker_offset;
    }

    pub fn get_consumer_offset(&self) -> i64 {
        self.consumer_offset
    }

    pub fn set_consumer_offset(&mut self, consumer_offset: i64) {
        self.consumer_offset = consumer_offset;
    }

    pub fn get_pull_offset(&self) -> i64 {
        self.pull_offset
    }

    pub fn set_pull_offset(&mut self, pull_offset: i64) {
        self.pull_offset = pull_offset;
    }

    pub fn get_last_timestamp(&self) -> i64 {
        self.last_timestamp
    }

    pub fn set_last_timestamp(&mut self, last_timestamp: i64) {
        self.last_timestamp = last_timestamp;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_wrapper_default_and_new() {
        let wrapper = OffsetWrapper::default();
        assert_eq!(wrapper.get_broker_offset(), 0);
        assert_eq!(wrapper.get_consumer_offset(), 0);
        assert_eq!(wrapper.get_pull_offset(), 0);
        assert_eq!(wrapper.get_last_timestamp(), 0);

        let wrapper = OffsetWrapper::new();
        assert_eq!(wrapper.get_broker_offset(), 0);
        assert_eq!(wrapper.get_consumer_offset(), 0);
        assert_eq!(wrapper.get_pull_offset(), 0);
        assert_eq!(wrapper.get_last_timestamp(), 0);
    }

    #[test]
    fn test_offset_wrapper_setters_and_getters() {
        let mut wrapper = OffsetWrapper::new();
        wrapper.set_broker_offset(100);
        wrapper.set_consumer_offset(200);
        wrapper.set_pull_offset(300);
        wrapper.set_last_timestamp(400);

        assert_eq!(wrapper.get_broker_offset(), 100);
        assert_eq!(wrapper.get_consumer_offset(), 200);
        assert_eq!(wrapper.get_pull_offset(), 300);
        assert_eq!(wrapper.get_last_timestamp(), 400);
    }

    #[test]
    fn test_offset_wrapper_serialization_and_deserialization() {
        let mut wrapper = OffsetWrapper::new();
        wrapper.set_broker_offset(-100);
        wrapper.set_consumer_offset(-200);
        wrapper.set_pull_offset(-300);
        wrapper.set_last_timestamp(-400);

        let serialized = serde_json::to_string(&wrapper).unwrap();
        let expected = r#"{"broker_offset":-100,"consumer_offset":-200,"pull_offset":-300,"last_timestamp":-400}"#;
        assert_eq!(serialized, expected);

        let deserialized: OffsetWrapper = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.get_broker_offset(), -100);
        assert_eq!(deserialized.get_consumer_offset(), -200);
        assert_eq!(deserialized.get_pull_offset(), -300);
        assert_eq!(deserialized.get_last_timestamp(), -400);
    }
}
