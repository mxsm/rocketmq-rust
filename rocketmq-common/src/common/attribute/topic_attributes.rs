use std::collections::HashMap;

use lazy_static::lazy_static;

use crate::{
    common::attribute::{
        attribute_enum::EnumAttribute, topic_message_type::TopicMessageType, Attribute,
    },
    hashset,
};

lazy_static! {
    pub static ref CLEANUP_POLICY_ATTRIBUTE: EnumAttribute = EnumAttribute {
        attribute: Attribute {
            name: String::from("cleanup.policy"),
            changeable: false,
        },
        universe: hashset! {String::from("DELETE"), String::from("COMPACTION")},
        default_value: String::from("DELETE"),
    };
    pub static ref TOPIC_MESSAGE_TYPE_ATTRIBUTE: EnumAttribute = EnumAttribute {
        attribute: Attribute {
            name: String::from("message.type"),
            changeable: true,
        },
        universe: TopicMessageType::topic_message_type_set(),
        default_value: TopicMessageType::Normal.to_string(),
    };
    pub static ref QUEUE_TYPE_ATTRIBUTE: EnumAttribute = EnumAttribute {
        attribute: Attribute {
            name: String::from("queue.type"),
            changeable: false,
        },
        universe: hashset! {String::from("BatchCQ"), String::from("SimpleCQ")},
        default_value: String::from("SimpleCQ"),
    };
    pub static ref ALL: HashMap<String, EnumAttribute> = {
        let mut map = HashMap::<String, EnumAttribute>::new();
        map.insert(
            QUEUE_TYPE_ATTRIBUTE.get_name().to_string(),
            QUEUE_TYPE_ATTRIBUTE.clone(),
        );
        map.insert(
            CLEANUP_POLICY_ATTRIBUTE.get_name().to_string(),
            CLEANUP_POLICY_ATTRIBUTE.clone(),
        );
        map.insert(
            TOPIC_MESSAGE_TYPE_ATTRIBUTE.get_name().to_string(),
            TOPIC_MESSAGE_TYPE_ATTRIBUTE.clone(),
        );
        map
    };
}
