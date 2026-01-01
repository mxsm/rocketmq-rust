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

use std::collections::HashMap;

use cheetah_string::CheetahString;
use rocketmq_filter::expression::evaluation_context::EvaluationContext;

pub struct MessageEvaluationContext<'a> {
    properties: &'a Option<HashMap<CheetahString, CheetahString>>,
}

impl<'a> MessageEvaluationContext<'a> {
    pub fn new(properties: &'a Option<HashMap<CheetahString, CheetahString>>) -> Self {
        Self { properties }
    }
}

impl<'a> EvaluationContext for MessageEvaluationContext<'a> {
    fn get(&self, name: &str) -> Option<&CheetahString> {
        self.properties
            .as_ref()
            .and_then(|props| props.get(&CheetahString::from_slice(name)))
    }

    fn key_values(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        self.properties.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::*;

    #[tokio::test]
    async fn get_returns_value_when_key_exists() {
        let mut properties = HashMap::new();
        properties.insert(CheetahString::from_slice("key1"), CheetahString::from_slice("value1"));
        let binding = Some(properties);
        let context = MessageEvaluationContext::new(&binding);

        let result = context.get("key1");

        assert_eq!(result, Some(&CheetahString::from_slice("value1")));
    }

    #[tokio::test]
    async fn get_returns_none_when_key_does_not_exist() {
        let mut properties = HashMap::new();
        properties.insert(CheetahString::from_slice("key1"), CheetahString::from_slice("value1"));
        let binding = Some(properties);
        let context = MessageEvaluationContext::new(&binding);

        let result = context.get("key2");

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn get_returns_none_when_properties_are_none() {
        let context = MessageEvaluationContext::new(&None);

        let result = context.get("key1");

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn key_values_returns_all_properties_when_present() {
        let mut properties = HashMap::new();
        properties.insert(CheetahString::from_slice("key1"), CheetahString::from_slice("value1"));
        properties.insert(CheetahString::from_slice("key2"), CheetahString::from_slice("value2"));
        let binding = Some(properties.clone());
        let context = MessageEvaluationContext::new(&binding);

        let result = context.key_values();

        assert_eq!(result, Some(properties));
    }

    #[tokio::test]
    async fn key_values_returns_none_when_properties_are_none() {
        let context = MessageEvaluationContext::new(&None);

        let result = context.key_values();

        assert!(result.is_none());
    }
}
