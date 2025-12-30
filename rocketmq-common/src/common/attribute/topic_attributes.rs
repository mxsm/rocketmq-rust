use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::OnceLock;

use cheetah_string::CheetahString;

use crate::common::attribute::enum_attribute::EnumAttribute;
use crate::common::attribute::long_range_attribute::LongRangeAttribute;
use crate::common::attribute::topic_message_type::TopicMessageType;
use crate::common::attribute::Attribute;
use crate::hashset;

/// Defines attributes and configurations for RocketMQ topics
pub struct TopicAttributes;

impl TopicAttributes {
    /// Queue type attribute defining storage structure (BatchCQ or SimpleCQ)
    pub fn queue_type_attribute() -> &'static EnumAttribute {
        static INSTANCE: OnceLock<EnumAttribute> = OnceLock::new();
        INSTANCE.get_or_init(|| {
            let mut valid_values = HashSet::new();
            valid_values.insert("BatchCQ".into());
            valid_values.insert("SimpleCQ".into());

            EnumAttribute::new("queue.type".into(), false, valid_values, "SimpleCQ".into())
        })
    }

    /// Cleanup policy attribute defining how messages are cleaned up (DELETE or COMPACTION)
    pub fn cleanup_policy_attribute() -> &'static EnumAttribute {
        static INSTANCE: OnceLock<EnumAttribute> = OnceLock::new();
        INSTANCE.get_or_init(|| {
            let mut valid_values = HashSet::new();
            valid_values.insert("DELETE".into());
            valid_values.insert("COMPACTION".into());

            EnumAttribute::new("cleanup.policy".into(), false, valid_values, "DELETE".into())
        })
    }

    /// Message type attribute defining the type of messages stored in the topic
    pub fn topic_message_type_attribute() -> &'static EnumAttribute {
        static INSTANCE: OnceLock<EnumAttribute> = OnceLock::new();
        INSTANCE.get_or_init(|| {
            EnumAttribute::new(
                "message.type".into(),
                true,
                TopicMessageType::topic_message_type_set()
                    .into_iter()
                    .map(|item| item.into())
                    .collect(),
                TopicMessageType::Normal.to_string().into(),
            )
        })
    }

    /// Reserve time attribute defining how long messages are kept
    pub fn topic_reserve_time_attribute() -> &'static LongRangeAttribute {
        static INSTANCE: OnceLock<LongRangeAttribute> = OnceLock::new();
        INSTANCE.get_or_init(|| LongRangeAttribute::new("reserve.time".into(), true, -1, i64::MAX, -1))
    }

    /// Returns all defined attributes in a HashMap
    pub fn all() -> &'static HashMap<CheetahString, Arc<dyn Attribute>> {
        static ALL: OnceLock<HashMap<CheetahString, Arc<dyn Attribute>>> = OnceLock::new();
        ALL.get_or_init(|| {
            let mut map = HashMap::new();

            let queue_type = Self::queue_type_attribute();
            let cleanup_policy = Self::cleanup_policy_attribute();
            let message_type = Self::topic_message_type_attribute();
            let reserve_time = Self::topic_reserve_time_attribute();

            map.insert(
                queue_type.name().clone(),
                Arc::new(queue_type.clone()) as Arc<dyn Attribute>,
            );
            map.insert(
                cleanup_policy.name().clone(),
                Arc::new(cleanup_policy.clone()) as Arc<dyn Attribute>,
            );
            map.insert(
                message_type.name().clone(),
                Arc::new(message_type.clone()) as Arc<dyn Attribute>,
            );
            map.insert(
                reserve_time.name().clone(),
                Arc::new(reserve_time.clone()) as Arc<dyn Attribute>,
            );

            map
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn queue_type_attribute_default_value() {
        let attribute = TopicAttributes::queue_type_attribute();
        assert_eq!(attribute.default_value(), "SimpleCQ");
    }

    #[test]
    fn queue_type_attribute_valid_values() {
        let attribute = TopicAttributes::queue_type_attribute();
        let valid_values: HashSet<CheetahString> = ["BatchCQ", "SimpleCQ"]
            .iter()
            .cloned()
            .map(CheetahString::from)
            .collect();
        assert_eq!(attribute.universe(), &valid_values);
    }

    #[test]
    fn cleanup_policy_attribute_default_value() {
        let attribute = TopicAttributes::cleanup_policy_attribute();
        assert_eq!(attribute.default_value(), "DELETE");
    }

    #[test]
    fn cleanup_policy_attribute_valid_values() {
        let attribute = TopicAttributes::cleanup_policy_attribute();
        let valid_values: HashSet<CheetahString> = ["DELETE", "COMPACTION"]
            .iter()
            .cloned()
            .map(CheetahString::from)
            .collect();
        assert_eq!(attribute.universe(), &valid_values);
    }

    #[test]
    fn topic_message_type_attribute_default_value() {
        let attribute = TopicAttributes::topic_message_type_attribute();
        assert_eq!(attribute.default_value(), "NORMAL");
    }

    #[test]
    fn topic_message_type_attribute_valid_values() {
        let attribute = TopicAttributes::topic_message_type_attribute();
        let valid_values: HashSet<CheetahString> = TopicMessageType::topic_message_type_set()
            .into_iter()
            .map(CheetahString::from)
            .collect();
        assert_eq!(attribute.universe(), &valid_values);
    }

    #[test]
    fn topic_reserve_time_attribute_default_value() {
        let attribute = TopicAttributes::topic_reserve_time_attribute();
        assert_eq!(attribute.default_value(), -1);
    }

    #[test]
    fn topic_reserve_time_attribute_valid_range() {
        let attribute = TopicAttributes::topic_reserve_time_attribute();
        assert_eq!(attribute.min(), -1);
        assert_eq!(attribute.max(), i64::MAX);
    }

    #[test]
    fn all_attributes_contains_all_defined_attributes() {
        let all_attributes = TopicAttributes::all();
        assert!(all_attributes.contains_key("queue.type"));
        assert!(all_attributes.contains_key("cleanup.policy"));
        assert!(all_attributes.contains_key("message.type"));
        assert!(all_attributes.contains_key("reserve.time"));
    }
}
