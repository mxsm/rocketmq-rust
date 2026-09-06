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

use crate::common::attribute::cq_type::CQType;
use crate::common::attribute::Attribute;
use crate::common::config::TopicConfig;
use crate::TopicAttributes::TopicAttributes;

pub struct QueueTypeUtils;

impl QueueTypeUtils {
    pub fn is_batch_cq(topic_config: Option<&TopicConfig>) -> bool {
        Self::get_cq_type(topic_config) == CQType::BatchCQ
    }

    pub fn get_cq_type(topic_config: Option<&TopicConfig>) -> CQType {
        match topic_config {
            Some(config) => {
                let default_value = TopicAttributes::queue_type_attribute().default_value();

                let attribute_name = TopicAttributes::queue_type_attribute().name();
                match config.attributes.get(attribute_name) {
                    Some(value) => value
                        .parse()
                        .unwrap_or(default_value.parse().unwrap_or(CQType::SimpleCQ)),
                    None => default_value.parse().unwrap_or(CQType::SimpleCQ),
                }
            }
            None => TopicAttributes::queue_type_attribute()
                .default_value()
                .parse()
                .unwrap_or(CQType::SimpleCQ),
        }
    }

    pub fn is_batch_cq_arc_mut<T>(topic_config: Option<&T>) -> bool
    where
        T: AsRef<TopicConfig>,
    {
        Self::get_cq_type_arc_mut(topic_config) == CQType::BatchCQ
    }

    pub fn get_cq_type_arc_mut<T>(topic_config: Option<&T>) -> CQType
    where
        T: AsRef<TopicConfig>,
    {
        Self::get_cq_type(topic_config.map(AsRef::as_ref))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_is_batch_cq() {
        let topic_config = None;
        assert!(!QueueTypeUtils::is_batch_cq(topic_config));

        let topic_config = Some(TopicConfig {
            attributes: HashMap::new(),
            ..TopicConfig::default()
        });
        assert!(!QueueTypeUtils::is_batch_cq(topic_config.as_ref()));

        let topic_config = Some(TopicConfig {
            attributes: HashMap::from_iter([(
                TopicAttributes::queue_type_attribute().name().to_string().into(),
                "BatchCQ".to_string().into(),
            )]),
            ..TopicConfig::default()
        });
        assert!(QueueTypeUtils::is_batch_cq(topic_config.as_ref()));

        let topic_config = Some(TopicConfig {
            attributes: HashMap::from_iter([(
                TopicAttributes::queue_type_attribute().name().to_string().into(),
                "InvalidCQ".to_string().into(),
            )]),
            ..TopicConfig::default()
        });
        assert!(!QueueTypeUtils::is_batch_cq(topic_config.as_ref()));
    }

    #[test]
    fn test_get_cq_type() {
        let topic_config = None;
        assert_eq!(QueueTypeUtils::get_cq_type(topic_config), CQType::SimpleCQ);

        let topic_config = Some(TopicConfig {
            attributes: HashMap::new(),
            ..TopicConfig::default()
        });
        assert_eq!(QueueTypeUtils::get_cq_type(topic_config.as_ref()), CQType::SimpleCQ);

        let topic_config = Some(TopicConfig {
            attributes: HashMap::from_iter([(
                TopicAttributes::queue_type_attribute().name().to_string().into(),
                "BatchCQ".to_string().into(),
            )]),
            ..TopicConfig::default()
        });
        assert_eq!(QueueTypeUtils::get_cq_type(topic_config.as_ref()), CQType::BatchCQ);

        let topic_config = Some(TopicConfig {
            attributes: HashMap::from_iter([(
                TopicAttributes::queue_type_attribute().name().to_string().into(),
                "InvalidCQ".to_string().into(),
            )]),
            ..TopicConfig::default()
        });
        assert_eq!(QueueTypeUtils::get_cq_type(topic_config.as_ref()), CQType::SimpleCQ);
    }
}
