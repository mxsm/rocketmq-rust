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

use std::collections::HashSet;
use std::hash::Hash;
use std::hash::Hasher;

use cheetah_string::CheetahString;
use rocketmq_common::common::filter::expression_type::ExpressionType;
use rocketmq_common::TimeUtils::get_current_millis;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionData {
    pub class_filter_mode: bool,
    pub topic: CheetahString,
    pub sub_string: CheetahString,
    pub tags_set: HashSet<CheetahString>,
    pub code_set: HashSet<i32>,
    pub sub_version: i64,
    pub expression_type: CheetahString,
    // In Rust, attributes like `@JSONField(serialize = false)` are typically handled through
    // documentation or external crates.
    #[serde(skip)]
    pub filter_class_source: CheetahString, // This field is not used in this example.
}

impl Default for SubscriptionData {
    fn default() -> Self {
        SubscriptionData {
            class_filter_mode: false,
            topic: CheetahString::new(),
            sub_string: CheetahString::new(),
            tags_set: HashSet::new(),
            code_set: HashSet::new(),
            sub_version: get_current_millis() as i64,
            expression_type: CheetahString::from_static_str(ExpressionType::TAG),
            filter_class_source: CheetahString::new(),
        }
    }
}

impl SubscriptionData {
    pub const SUB_ALL: &'static str = "*";
}

impl Hash for SubscriptionData {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.class_filter_mode.hash(state);
        self.topic.hash(state);
        self.sub_string.hash(state);
        self.tags_set.iter().for_each(|tag| tag.hash(state));
        self.code_set.iter().for_each(|code| code.hash(state));
        self.sub_version.hash(state);
        self.expression_type.hash(state);
        self.filter_class_source.hash(state);
    }
}
