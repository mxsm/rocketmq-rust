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

use std::str::FromStr;

use rocketmq_rust::ArcMut;

use crate::common::attribute::cleanup_policy::CleanupPolicy;
use crate::common::attribute::Attribute;
use crate::common::config::TopicConfig;
use crate::TopicAttributes::TopicAttributes;

pub fn is_compaction(topic_config: Option<&TopicConfig>) -> bool {
    match topic_config {
        Some(config) => CleanupPolicy::COMPACTION == get_delete_policy(Some(config)),
        None => false,
    }
}

pub fn get_delete_policy(topic_config: Option<&TopicConfig>) -> CleanupPolicy {
    match topic_config {
        Some(config) => {
            let attribute_name = TopicAttributes::cleanup_policy_attribute().name();
            match config.attributes.get(attribute_name) {
                Some(value) => CleanupPolicy::from_str(value.as_str()).unwrap(),
                None => CleanupPolicy::from_str(TopicAttributes::cleanup_policy_attribute().default_value()).unwrap(),
            }
        }
        None => CleanupPolicy::from_str(TopicAttributes::cleanup_policy_attribute().default_value()).unwrap(),
    }
}

pub fn get_delete_policy_arc_mut(topic_config: Option<&ArcMut<TopicConfig>>) -> CleanupPolicy {
    match topic_config {
        Some(config) => {
            let attribute_name = TopicAttributes::cleanup_policy_attribute().name();
            match config.attributes.get(attribute_name) {
                Some(value) => CleanupPolicy::from_str(value.as_str()).unwrap(),
                None => CleanupPolicy::from_str(TopicAttributes::cleanup_policy_attribute().default_value()).unwrap(),
            }
        }
        None => CleanupPolicy::from_str(TopicAttributes::cleanup_policy_attribute().default_value()).unwrap(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::attribute::cleanup_policy::CleanupPolicy;
    use crate::common::config::TopicConfig;

    #[test]
    fn is_compaction_returns_true_when_cleanup_policy_is_compaction() {
        let mut topic_config = TopicConfig::default();
        topic_config.attributes.insert(
            TopicAttributes::cleanup_policy_attribute().name().into(),
            CleanupPolicy::COMPACTION.to_string().into(),
        );
        assert!(is_compaction(Some(&topic_config)));
    }

    #[test]
    fn is_compaction_returns_false_when_cleanup_policy_is_not_compaction() {
        let mut topic_config = TopicConfig::default();
        topic_config.attributes.insert(
            TopicAttributes::cleanup_policy_attribute().name().to_string().into(),
            CleanupPolicy::DELETE.to_string().into(),
        );
        assert!(!is_compaction(Some(&topic_config)));
    }

    #[test]
    fn is_compaction_returns_false_when_topic_config_is_none() {
        assert!(!is_compaction(None));
    }

    #[test]
    fn get_delete_policy_returns_cleanup_policy_from_topic_config() {
        let mut topic_config = TopicConfig::default();
        topic_config.attributes.insert(
            TopicAttributes::cleanup_policy_attribute().name().to_string().into(),
            CleanupPolicy::DELETE.to_string().into(),
        );
        assert_eq!(get_delete_policy(Some(&topic_config)), CleanupPolicy::DELETE);
    }

    #[test]
    fn get_delete_policy_returns_default_cleanup_policy_when_not_set_in_topic_config() {
        let topic_config = TopicConfig::default();
        assert_eq!(
            get_delete_policy(Some(&topic_config)),
            CleanupPolicy::from_str(TopicAttributes::cleanup_policy_attribute().default_value()).unwrap()
        );
    }

    #[test]
    fn get_delete_policy_returns_default_cleanup_policy_when_topic_config_is_none() {
        assert_eq!(
            get_delete_policy(None),
            CleanupPolicy::from_str(TopicAttributes::cleanup_policy_attribute().default_value()).unwrap()
        );
    }
}
