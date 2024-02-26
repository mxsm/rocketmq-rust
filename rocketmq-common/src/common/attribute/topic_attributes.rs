use std::collections::HashMap;

use lazy_static::lazy_static;

use crate::{
    common::attribute::{topic_message_type::TopicMessageType, Attribute, EnumAttribute},
    hashset,
};

lazy_static! {
    pub static ref CLEANUP_POLICY_ATTRIBUTE: EnumAttribute = EnumAttribute {
        name: String::from("cleanup.policy"),
        changeable: false,
        universe: hashset! {String::from("DELETE"), String::from("COMPACTION")},
        default_value: String::from("DELETE"),
    };
    pub static ref TOPIC_MESSAGE_TYPE_ATTRIBUTE: EnumAttribute = EnumAttribute {
        name: String::from("message.type"),
        changeable: true,
        universe: TopicMessageType::topic_message_type_set(),
        default_value: TopicMessageType::Normal.to_string(),
    };
    pub static ref QUEUE_TYPE_ATTRIBUTE: EnumAttribute = EnumAttribute {
        name: String::from("queue.type"),
        changeable: false,
        universe: hashset! {String::from("BatchCQ"), String::from("SimpleCQ")},
        default_value: String::from("SimpleCQ"),
    };
    pub static ref ALL: HashMap<&'static str, &'static (dyn Attribute + Send + Sync)> = {
        let mut map = HashMap::new();
        map.insert(
            QUEUE_TYPE_ATTRIBUTE.get_name(),
            QUEUE_TYPE_ATTRIBUTE.deref() as &(dyn Attribute + std::marker::Send + Sync),
        );
        map.insert(
            CLEANUP_POLICY_ATTRIBUTE.get_name(),
            CLEANUP_POLICY_ATTRIBUTE.deref() as &(dyn Attribute + std::marker::Send + Sync),
        );
        map.insert(
            TOPIC_MESSAGE_TYPE_ATTRIBUTE.get_name(),
            TOPIC_MESSAGE_TYPE_ATTRIBUTE.deref() as &(dyn Attribute + std::marker::Send + Sync),
        );
        map
    };
}
