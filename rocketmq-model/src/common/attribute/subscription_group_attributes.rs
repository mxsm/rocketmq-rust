use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::OnceLock;

use cheetah_string::CheetahString;

use crate::common::attribute::bool_attribute::BooleanAttribute;
use crate::common::attribute::enum_attribute::EnumAttribute;
use crate::common::attribute::long_range_attribute::LongRangeAttribute;
use crate::common::attribute::string_attribute::StringAttribute;
use crate::common::attribute::Attribute;

pub const LITE_BIND_TOPIC_ATTRIBUTE_NAME: &str = "lite.bind.topic";
pub const LITE_SUB_MODEL_ATTRIBUTE_NAME: &str = "lite.sub.model";
pub const LITE_SUB_RESET_OFFSET_EXCLUSIVE_ATTRIBUTE_NAME: &str = "lite.sub.reset.offset.exclusive";
pub const LITE_SUB_RESET_OFFSET_UNSUBSCRIBE_ATTRIBUTE_NAME: &str = "lite.sub.reset.offset.unsubscribe";
pub const LITE_SUB_CLIENT_QUOTA_ATTRIBUTE_NAME: &str = "lite.sub.client.quota";
pub const LITE_SUB_CLIENT_MAX_EVENT_COUNT_ATTRIBUTE_NAME: &str = "lite.sub.client.max.event.cnt";

pub struct SubscriptionGroupAttributes {}

impl SubscriptionGroupAttributes {
    pub fn lite_bind_topic_attribute() -> &'static StringAttribute {
        static INSTANCE: OnceLock<StringAttribute> = OnceLock::new();
        INSTANCE
            .get_or_init(|| StringAttribute::new(CheetahString::from_static_str(LITE_BIND_TOPIC_ATTRIBUTE_NAME), true))
    }

    pub fn lite_sub_model_attribute() -> &'static EnumAttribute {
        static INSTANCE: OnceLock<EnumAttribute> = OnceLock::new();
        INSTANCE.get_or_init(|| {
            let mut valid_values = HashSet::new();
            valid_values.insert(CheetahString::from_static_str("Shared"));
            valid_values.insert(CheetahString::from_static_str("Exclusive"));
            EnumAttribute::new(
                CheetahString::from_static_str(LITE_SUB_MODEL_ATTRIBUTE_NAME),
                true,
                valid_values,
                CheetahString::from_static_str("Shared"),
            )
        })
    }

    pub fn lite_sub_reset_offset_exclusive_attribute() -> &'static BooleanAttribute {
        static INSTANCE: OnceLock<BooleanAttribute> = OnceLock::new();
        INSTANCE.get_or_init(|| {
            BooleanAttribute::new(
                CheetahString::from_static_str(LITE_SUB_RESET_OFFSET_EXCLUSIVE_ATTRIBUTE_NAME),
                true,
                false,
            )
        })
    }

    pub fn lite_sub_reset_offset_unsubscribe_attribute() -> &'static BooleanAttribute {
        static INSTANCE: OnceLock<BooleanAttribute> = OnceLock::new();
        INSTANCE.get_or_init(|| {
            BooleanAttribute::new(
                CheetahString::from_static_str(LITE_SUB_RESET_OFFSET_UNSUBSCRIBE_ATTRIBUTE_NAME),
                true,
                false,
            )
        })
    }

    pub fn lite_sub_client_quota_attribute() -> &'static LongRangeAttribute {
        static INSTANCE: OnceLock<LongRangeAttribute> = OnceLock::new();
        INSTANCE.get_or_init(|| {
            LongRangeAttribute::new(
                CheetahString::from_static_str(LITE_SUB_CLIENT_QUOTA_ATTRIBUTE_NAME),
                true,
                -1,
                i64::MAX,
                2000,
            )
        })
    }

    pub fn lite_sub_client_max_event_count_attribute() -> &'static LongRangeAttribute {
        static INSTANCE: OnceLock<LongRangeAttribute> = OnceLock::new();
        INSTANCE.get_or_init(|| {
            LongRangeAttribute::new(
                CheetahString::from_static_str(LITE_SUB_CLIENT_MAX_EVENT_COUNT_ATTRIBUTE_NAME),
                true,
                10,
                i64::MAX,
                400,
            )
        })
    }

    #[allow(clippy::redundant_closure)]
    pub fn all() -> &'static HashMap<CheetahString, Arc<dyn Attribute>> {
        static ALL: OnceLock<HashMap<CheetahString, Arc<dyn Attribute>>> = OnceLock::new();
        ALL.get_or_init(|| {
            let mut all = HashMap::new();
            let lite_bind_topic = Self::lite_bind_topic_attribute();
            let lite_sub_model = Self::lite_sub_model_attribute();
            let lite_sub_reset_offset_exclusive = Self::lite_sub_reset_offset_exclusive_attribute();
            let lite_sub_reset_offset_unsubscribe = Self::lite_sub_reset_offset_unsubscribe_attribute();
            let lite_sub_client_quota = Self::lite_sub_client_quota_attribute();
            let lite_sub_client_max_event_count = Self::lite_sub_client_max_event_count_attribute();

            all.insert(
                lite_bind_topic.name().clone(),
                Arc::new(lite_bind_topic.clone()) as Arc<dyn Attribute>,
            );
            all.insert(
                lite_sub_model.name().clone(),
                Arc::new(lite_sub_model.clone()) as Arc<dyn Attribute>,
            );
            all.insert(
                lite_sub_reset_offset_exclusive.name().clone(),
                Arc::new(lite_sub_reset_offset_exclusive.clone()) as Arc<dyn Attribute>,
            );
            all.insert(
                lite_sub_reset_offset_unsubscribe.name().clone(),
                Arc::new(lite_sub_reset_offset_unsubscribe.clone()) as Arc<dyn Attribute>,
            );
            all.insert(
                lite_sub_client_quota.name().clone(),
                Arc::new(lite_sub_client_quota.clone()) as Arc<dyn Attribute>,
            );
            all.insert(
                lite_sub_client_max_event_count.name().clone(),
                Arc::new(lite_sub_client_max_event_count.clone()) as Arc<dyn Attribute>,
            );

            all
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
        assert!(all.contains_key(&CheetahString::from_static_str(LITE_SUB_MODEL_ATTRIBUTE_NAME)));
        assert!(all.contains_key(&CheetahString::from_static_str(
            LITE_SUB_RESET_OFFSET_EXCLUSIVE_ATTRIBUTE_NAME
        )));
        assert!(all.contains_key(&CheetahString::from_static_str(
            LITE_SUB_RESET_OFFSET_UNSUBSCRIBE_ATTRIBUTE_NAME
        )));
        assert!(all.contains_key(&CheetahString::from_static_str(LITE_SUB_CLIENT_QUOTA_ATTRIBUTE_NAME)));
        assert!(all.contains_key(&CheetahString::from_static_str(
            LITE_SUB_CLIENT_MAX_EVENT_COUNT_ATTRIBUTE_NAME
        )));
    }

    #[test]
    fn lite_sub_model_attribute_uses_java_defaults() {
        let attribute = SubscriptionGroupAttributes::lite_sub_model_attribute();

        assert_eq!(attribute.default_value(), "Shared");
        assert!(attribute.universe().contains(&CheetahString::from_static_str("Shared")));
        assert!(attribute
            .universe()
            .contains(&CheetahString::from_static_str("Exclusive")));
    }

    #[test]
    fn lite_sub_reset_offset_attributes_default_to_false() {
        let exclusive = SubscriptionGroupAttributes::lite_sub_reset_offset_exclusive_attribute();
        let unsubscribe = SubscriptionGroupAttributes::lite_sub_reset_offset_unsubscribe_attribute();

        assert!(!exclusive.default_value());
        assert!(!unsubscribe.default_value());
    }

    #[test]
    fn lite_sub_client_quota_attribute_uses_java_defaults() {
        let attribute = SubscriptionGroupAttributes::lite_sub_client_quota_attribute();

        assert_eq!(attribute.default_value(), 2000);
        assert_eq!(attribute.min(), -1);
        assert_eq!(attribute.max(), i64::MAX);
    }

    #[test]
    fn lite_sub_client_max_event_count_attribute_uses_java_defaults() {
        let attribute = SubscriptionGroupAttributes::lite_sub_client_max_event_count_attribute();

        assert_eq!(attribute.default_value(), 400);
        assert_eq!(attribute.min(), 10);
        assert_eq!(attribute.max(), i64::MAX);
    }
}
