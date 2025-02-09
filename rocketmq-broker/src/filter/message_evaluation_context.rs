/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::collections::HashMap;

use cheetah_string::CheetahString;
use rocketmq_filter::expression::evaluation_context::EvaluationContext;

pub struct MessageEvaluationContext {
    properties: Option<HashMap<CheetahString, CheetahString>>,
}

impl MessageEvaluationContext {
    pub fn new(properties: Option<HashMap<CheetahString, CheetahString>>) -> Self {
        Self { properties }
    }
}

impl EvaluationContext for MessageEvaluationContext {
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

    #[test]
    fn new_creates_instance_with_properties() {
        let mut properties = HashMap::new();
        properties.insert(
            CheetahString::from_static_str("key"),
            CheetahString::from_static_str("value"),
        );
        let context = MessageEvaluationContext::new(Some(properties.clone()));
        assert_eq!(context.properties, Some(properties));
    }

    #[test]
    fn new_creates_instance_with_none_properties() {
        let context = MessageEvaluationContext::new(None);
        assert!(context.properties.is_none());
    }

    #[test]
    fn get_returns_value_if_key_exists() {
        let mut properties = HashMap::new();
        properties.insert(
            CheetahString::from_static_str("key"),
            CheetahString::from_static_str("value"),
        );
        let context = MessageEvaluationContext::new(Some(properties));
        assert_eq!(
            context.get("key"),
            Some(&CheetahString::from_static_str("value"))
        );
    }

    #[test]
    fn get_returns_none_if_key_does_not_exist() {
        let context = MessageEvaluationContext::new(Some(HashMap::new()));
        assert!(context.get("nonexistent").is_none());
    }

    #[test]
    fn get_returns_none_if_properties_is_none() {
        let context = MessageEvaluationContext::new(None);
        assert!(context.get("key").is_none());
    }

    #[test]
    fn key_values_returns_properties_if_present() {
        let mut properties = HashMap::new();
        properties.insert(
            CheetahString::from_static_str("key"),
            CheetahString::from_static_str("value"),
        );
        let context = MessageEvaluationContext::new(Some(properties.clone()));
        assert_eq!(context.key_values(), Some(properties));
    }

    #[test]
    fn key_values_returns_none_if_properties_is_none() {
        let context = MessageEvaluationContext::new(None);
        assert!(context.key_values().is_none());
    }
}
