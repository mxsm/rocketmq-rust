use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;

use cheetah_string::CheetahString;

use crate::common::attribute::string_attribute::StringAttribute;
use crate::common::attribute::Attribute;

pub const LITE_BIND_TOPIC_ATTRIBUTE_NAME: &str = "lite.bind.topic";

pub struct SubscriptionGroupAttributes {}

impl SubscriptionGroupAttributes {
    #[allow(clippy::redundant_closure)]
    pub fn all() -> &'static HashMap<CheetahString, Arc<dyn Attribute>> {
        static ALL: OnceLock<HashMap<CheetahString, Arc<dyn Attribute>>> = OnceLock::new();
        ALL.get_or_init(|| {
            HashMap::from([(
                CheetahString::from_static_str(LITE_BIND_TOPIC_ATTRIBUTE_NAME),
                Arc::new(StringAttribute::new(
                    CheetahString::from_static_str(LITE_BIND_TOPIC_ATTRIBUTE_NAME),
                    true,
                )) as Arc<dyn Attribute>,
            )])
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_contains_lite_bind_topic_attribute() {
        let all = SubscriptionGroupAttributes::all();

        assert!(all.contains_key(&CheetahString::from_static_str(LITE_BIND_TOPIC_ATTRIBUTE_NAME)));
    }
}
