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
pub struct BooleanAttribute {
    attribute: AttributeBase,
    default_value: bool,
}

impl BooleanAttribute {
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
    pub fn new(name: CheetahString, changeable: bool, default_value: bool) -> Self {
        Self {
            attribute: AttributeBase::new(name, changeable),
            default_value,
        }
    }

    /// Get the default value for this attribute
    #[inline]
    pub fn default_value(&self) -> bool {
        self.default_value
    }

    /// Parse a string value to boolean
    ///
    /// Returns Ok(bool) if the string is "true" or "false" (case insensitive),
    /// or an Err with a descriptive message otherwise.
    pub fn parse_bool(value: &str) -> Result<bool, String> {
        match value.to_lowercase().as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(format!("Boolean attribute must be 'true' or 'false', got '{value}'",)),
        }
    }
}

impl Attribute for BooleanAttribute {
    #[inline]
    fn verify(&self, value: &str) -> Result<(), String> {
        if value.is_empty() {
            return Err("Boolean attribute value cannot be empty".to_string());
        }

        // Try parsing to ensure it's a valid boolean
        Self::parse_bool(value)?;
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
    fn create_boolean_attribute() {
        let name = CheetahString::from_static_str("test_attribute");
        let attribute = BooleanAttribute::new(name.clone(), true, true);
        assert_eq!(attribute.name(), &name);
        assert!(attribute.is_changeable());
        assert!(attribute.default_value());
    }

    #[test]
    fn parse_bool_true() {
        let result = BooleanAttribute::parse_bool("true");
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn parse_bool_false() {
        let result = BooleanAttribute::parse_bool("false");
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn parse_bool_invalid() {
        let result = BooleanAttribute::parse_bool("invalid");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Boolean attribute must be 'true' or 'false', got 'invalid'"
        );
    }

    #[test]
    fn verify_valid_true() {
        let attribute = BooleanAttribute::new(CheetahString::from_static_str("test_attribute"), true, true);
        let result = attribute.verify("true");
        assert!(result.is_ok());
    }

    #[test]
    fn verify_valid_false() {
        let attribute = BooleanAttribute::new(CheetahString::from_static_str("test_attribute"), true, false);
        let result = attribute.verify("false");
        assert!(result.is_ok());
    }

    #[test]
    fn verify_empty_value() {
        let attribute = BooleanAttribute::new(CheetahString::from_static_str("test_attribute"), true, true);
        let result = attribute.verify("");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Boolean attribute value cannot be empty");
    }

    #[test]
    fn verify_invalid_value() {
        let attribute = BooleanAttribute::new(CheetahString::from_static_str("test_attribute"), true, true);
        let result = attribute.verify("invalid");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Boolean attribute must be 'true' or 'false', got 'invalid'"
        );
    }
}
