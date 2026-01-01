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
pub struct LongRangeAttribute {
    attribute: AttributeBase,
    /// Minimum allowed value (inclusive)
    min: i64,

    /// Maximum allowed value (inclusive)
    max: i64,

    /// Default value for this attribute
    default_value: i64,
}

impl LongRangeAttribute {
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
    pub fn new(name: CheetahString, changeable: bool, min: i64, max: i64, default_value: i64) -> Self {
        Self {
            attribute: AttributeBase::new(name, changeable),
            min,
            max,
            default_value,
        }
    }

    /// Get the default value for this attribute
    #[inline]
    pub fn default_value(&self) -> i64 {
        self.default_value
    }

    /// Get the minimum allowed value
    pub fn min(&self) -> i64 {
        self.min
    }

    /// Get the maximum allowed value
    pub fn max(&self) -> i64 {
        self.max
    }

    /// Parse a string to a long integer
    pub fn parse_long(value: &str) -> Result<i64, String> {
        value.parse::<i64>().map_err(|e| format!("Invalid integer format: {e}"))
    }
}

impl Attribute for LongRangeAttribute {
    fn verify(&self, value: &str) -> Result<(), String> {
        // Parse the value to an i64
        let parsed_value = Self::parse_long(value)?;

        // Check if the value is in range
        if parsed_value < self.min || parsed_value > self.max {
            return Err(format!(
                "Value {} is not in range [{}, {}]",
                parsed_value, self.min, self.max
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

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn create_long_range_attribute() {
        let name = CheetahString::from_static_str("test_attribute");
        let attribute = LongRangeAttribute::new(name.clone(), true, 0, 100, 50);
        assert_eq!(attribute.name(), &name);
        assert!(attribute.is_changeable());
        assert_eq!(attribute.default_value(), 50);
    }

    #[test]
    fn parse_long_valid() {
        let result = LongRangeAttribute::parse_long("42");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn parse_long_invalid() {
        let result = LongRangeAttribute::parse_long("invalid");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Invalid integer format: invalid digit found in string"
        );
    }

    #[test]
    fn verify_value_in_range() {
        let attribute = LongRangeAttribute::new(CheetahString::from_static_str("test_attribute"), true, 0, 100, 50);
        let result = attribute.verify("42");
        assert!(result.is_ok());
    }

    #[test]
    fn verify_value_below_range() {
        let attribute = LongRangeAttribute::new(CheetahString::from_static_str("test_attribute"), true, 0, 100, 50);
        let result = attribute.verify("-1");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Value -1 is not in range [0, 100]");
    }

    #[test]
    fn verify_value_above_range() {
        let attribute = LongRangeAttribute::new(CheetahString::from_static_str("test_attribute"), true, 0, 100, 50);
        let result = attribute.verify("101");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Value 101 is not in range [0, 100]");
    }
}
