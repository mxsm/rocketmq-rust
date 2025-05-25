use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;

use cheetah_string::CheetahString;

use crate::common::attribute::Attribute;

pub struct SubscriptionGroupAttributes {}

impl SubscriptionGroupAttributes {
    #[allow(clippy::redundant_closure)]
    pub fn all() -> &'static HashMap<CheetahString, Arc<dyn Attribute>> {
        static ALL: OnceLock<HashMap<CheetahString, Arc<dyn Attribute>>> = OnceLock::new();
        ALL.get_or_init(|| HashMap::new())
    }
}
