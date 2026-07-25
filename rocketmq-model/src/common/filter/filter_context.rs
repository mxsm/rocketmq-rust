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

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FilterContext {
    consumer_group: Option<CheetahString>,
}

impl FilterContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_consumer_group(&self) -> Option<&CheetahString> {
        self.consumer_group.as_ref()
    }

    pub fn set_consumer_group(&mut self, consumer_group: CheetahString) {
        self.consumer_group = Some(consumer_group);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_context_default_and_new() {
        let filter_context = FilterContext::default();
        assert!(filter_context.get_consumer_group().is_none());

        let filter_context = FilterContext::new();
        assert!(filter_context.get_consumer_group().is_none());
    }

    #[test]
    fn test_filter_context_setters_and_getters() {
        let mut filter_context = FilterContext::new();
        let consumer_group = CheetahString::from("test_consumer_group");
        filter_context.set_consumer_group(consumer_group.clone());

        assert_eq!(filter_context.get_consumer_group(), Some(&consumer_group));
    }

    #[test]
    fn test_filter_context_serialization_and_deserialization() {
        let mut filter_context = FilterContext::new();
        let consumer_group = CheetahString::from("test_consumer_group");
        filter_context.set_consumer_group(consumer_group.clone());

        let serialized = serde_json::to_string(&filter_context).unwrap();
        let deserialized: FilterContext = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.get_consumer_group(), Some(&consumer_group));
    }
}
