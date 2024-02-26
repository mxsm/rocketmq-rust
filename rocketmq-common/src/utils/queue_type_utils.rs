use std::collections::HashMap;

use crate::{
    common::{
        attribute::{cq_type::CQType, Attribute},
        config::TopicConfig,
    },
    TopicAttributes,
};

pub struct QueueTypeUtils;

impl QueueTypeUtils {
    pub fn is_batch_cq(topic_config: &Option<TopicConfig>) -> bool {
        Self::get_cq_type(topic_config) == CQType::BatchCQ
    }

    pub fn get_cq_type(topic_config: &Option<TopicConfig>) -> CQType {
        match topic_config {
            Some(config) => {
                let default_value = TopicAttributes::QUEUE_TYPE_ATTRIBUTE.get_default_value();

                let attribute_name = TopicAttributes::QUEUE_TYPE_ATTRIBUTE.get_name();
                match config.attributes.get(attribute_name) {
                    Some(value) => value
                        .parse()
                        .unwrap_or(default_value.parse().unwrap_or(CQType::SimpleCQ)),
                    None => default_value.parse().unwrap_or(CQType::SimpleCQ),
                }
            }
            None => TopicAttributes::QUEUE_TYPE_ATTRIBUTE
                .get_default_value()
                .parse()
                .unwrap_or(CQType::SimpleCQ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::TopicFilterType;

    #[test]
    fn test_is_batch_cq() {
        let topic_config = None;
        assert_eq!(QueueTypeUtils::is_batch_cq(&topic_config), false);

        let topic_config = Some(TopicConfig {
            attributes: HashMap::new(),
            ..TopicConfig::default()
        });
        assert_eq!(QueueTypeUtils::is_batch_cq(&topic_config), false);

        let topic_config = Some(TopicConfig {
            attributes: HashMap::from_iter([(
                TopicAttributes::QUEUE_TYPE_ATTRIBUTE.get_name().to_string(),
                "BatchCQ".to_string(),
            )]),
            ..TopicConfig::default()
        });
        assert_eq!(QueueTypeUtils::is_batch_cq(&topic_config), true);

        let topic_config = Some(TopicConfig {
            attributes: HashMap::from_iter([(
                TopicAttributes::QUEUE_TYPE_ATTRIBUTE.get_name().to_string(),
                "InvalidCQ".to_string(),
            )]),
            ..TopicConfig::default()
        });
        assert_eq!(QueueTypeUtils::is_batch_cq(&topic_config), false);
    }

    #[test]
    fn test_get_cq_type() {
        let topic_config = None;
        assert_eq!(QueueTypeUtils::get_cq_type(&topic_config), CQType::SimpleCQ);

        let topic_config = Some(TopicConfig {
            attributes: HashMap::new(),
            ..TopicConfig::default()
        });
        assert_eq!(QueueTypeUtils::get_cq_type(&topic_config), CQType::SimpleCQ);

        let topic_config = Some(TopicConfig {
            attributes: HashMap::from_iter([(
                TopicAttributes::QUEUE_TYPE_ATTRIBUTE.get_name().to_string(),
                "BatchCQ".to_string(),
            )]),
            ..TopicConfig::default()
        });
        assert_eq!(QueueTypeUtils::get_cq_type(&topic_config), CQType::BatchCQ);

        let topic_config = Some(TopicConfig {
            attributes: HashMap::from_iter([(
                TopicAttributes::QUEUE_TYPE_ATTRIBUTE.get_name().to_string(),
                "InvalidCQ".to_string(),
            )]),
            ..TopicConfig::default()
        });
        assert_eq!(QueueTypeUtils::get_cq_type(&topic_config), CQType::SimpleCQ);
    }
}
