use std::collections::HashMap;

use rocketmq_rust::ArcMut;

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

    pub fn is_batch_cq_arc_mut(topic_config: Option<&ArcMut<TopicConfig>>) -> bool {
        Self::get_cq_type_arc_mut(topic_config) == CQType::BatchCQ
    }

    pub fn get_cq_type_arc_mut(topic_config: Option<&ArcMut<TopicConfig>>) -> CQType {
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
}

#[cfg(test)]
mod tests {
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
