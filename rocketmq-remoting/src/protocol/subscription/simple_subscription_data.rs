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

#[derive(Debug, Clone, Default, Serialize, Deserialize, Hash, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SimpleSubscriptionData {
    topic: String,
    expression_type: String,
    expression: String,
    version: u64,
}

impl SimpleSubscriptionData {
    pub fn new(topic: String, expression_type: String, expression: String, version: u64) -> Self {
        SimpleSubscriptionData {
            topic,
            expression_type,
            expression,
            version,
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn expression_type(&self) -> &str {
        &self.expression_type
    }

    pub fn expression(&self) -> &str {
        &self.expression
    }

    pub fn version(&self) -> u64 {
        self.version
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_subscription_data_default() {
        let data = SimpleSubscriptionData::default();
        assert_eq!(data.topic(), "");
        assert_eq!(data.expression_type(), "");
        assert_eq!(data.expression(), "");
        assert_eq!(data.version(), 0);
    }

    #[test]
    fn simple_subscription_data_new_and_getters() {
        let data = SimpleSubscriptionData::new("topic".to_string(), "TAG".to_string(), "*".to_string(), 123);
        assert_eq!(data.topic(), "topic");
        assert_eq!(data.expression_type(), "TAG");
        assert_eq!(data.expression(), "*");
        assert_eq!(data.version(), 123);
    }
}
