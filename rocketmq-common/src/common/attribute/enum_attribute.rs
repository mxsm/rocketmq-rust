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

use cheetah_string::CheetahString;

use crate::common::attribute::Attribute;
use crate::common::attribute::AttributeBase;

#[derive(Debug, Clone)]
pub struct EnumAttribute {
    attribute: AttributeBase,
    universe: HashSet<CheetahString>,
    default_value: CheetahString,
}

impl EnumAttribute {
    /// Create a new enum attribute with the specified properties
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the attribute
    /// * `changeable` - Whether the attribute can be changed after creation
    /// * `universe` - Set of valid values this attribute can take
    /// * `default_value` - Default value for this attribute (must be in universe)
    ///
    /// # Returns
    ///
    /// A new EnumAttribute instance, or an error if the default value is not in the universe
    pub fn new(
        name: CheetahString,
        changeable: bool,
        universe: HashSet<CheetahString>,
        default_value: CheetahString,
    ) -> Self {
        Self {
            attribute: AttributeBase::new(name, changeable),
            universe,
            default_value,
        }
    }

    /// Get the default value for this attribute
    #[inline]
    pub fn default_value(&self) -> &str {
        &self.default_value
    }

    /// Get the set of valid values for this attribute
    #[inline]
    pub fn universe(&self) -> &HashSet<CheetahString> {
        &self.universe
    }
}

impl Attribute for EnumAttribute {
    #[inline]
    fn verify(&self, value: &str) -> Result<(), String> {
        if !self.universe.contains(value) {
            return Err(format!(
                "Value '{}' is not in the valid set: {:?}",
                value, self.universe
            ));
        }
        Ok(())
    }

    #[inline]
    fn name(&self) -> &CheetahString {
        self.attribute.name()
    }

    #[inline]
    fn is_changeable(&self) -> bool {
        self.attribute.is_changeable()
    }
}
