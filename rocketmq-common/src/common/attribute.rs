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

use cheetah_string::CheetahString;

pub mod attribute_parser;
pub mod attribute_util;
pub mod bool_attribute;
pub mod cleanup_policy;
pub mod cq_type;
pub mod enum_attribute;
pub mod long_range_attribute;
pub mod subscription_group_attributes;
pub mod topic_attributes;
pub mod topic_message_type;

/// Trait representing an attribute with name and changeability properties.
///
/// This trait is the Rust equivalent of the Java abstract class Attribute.
/// It defines common functionality for all attribute types.
pub trait Attribute: Send + Sync {
    /// Verify that the provided string value is valid for this attribute.
    /// Implementations should validate according to their specific rules.
    fn verify(&self, value: &str) -> Result<(), String>;

    /// Get the name of this attribute.
    fn name(&self) -> &CheetahString;

    /// Check if this attribute can be changed after creation.
    fn is_changeable(&self) -> bool;
}

/// Base implementation of the Attribute trait that can be extended by concrete attribute types.
#[derive(Debug, Clone)]
pub struct AttributeBase {
    /// The name of the attribute.
    name: CheetahString,

    /// Whether the attribute can be changed after creation.
    changeable: bool,
}

impl AttributeBase {
    /// Create a new attribute base with the given name and changeability.
    pub fn new(name: CheetahString, changeable: bool) -> Self {
        Self { name, changeable }
    }

    /// Get the name of this attribute.
    pub fn name(&self) -> &CheetahString {
        &self.name
    }

    /// Set a new name for this attribute.
    pub fn set_name(&mut self, name: CheetahString) {
        self.name = name;
    }

    /// Check if this attribute can be changed after creation.
    pub fn is_changeable(&self) -> bool {
        self.changeable
    }

    /// Set whether this attribute can be changed.
    pub fn set_changeable(&mut self, changeable: bool) {
        self.changeable = changeable;
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn create_new_attribute_base() {
        let name = CheetahString::from_static_str("test_attribute");
        let attribute = AttributeBase::new(name.clone(), true);
        assert_eq!(attribute.name(), &name);
        assert!(attribute.is_changeable());
    }

    #[test]
    fn set_attribute_name() {
        let mut attribute = AttributeBase::new(CheetahString::from_static_str("old_name"), true);
        let new_name = CheetahString::from_static_str("new_name");
        attribute.set_name(new_name.clone());
        assert_eq!(attribute.name(), &new_name);
    }

    #[test]
    fn set_attribute_changeable() {
        let mut attribute = AttributeBase::new(CheetahString::from_static_str("test_attribute"), false);
        attribute.set_changeable(true);
        assert!(attribute.is_changeable());
    }

    #[test]
    fn attribute_not_changeable() {
        let attribute = AttributeBase::new(CheetahString::from_static_str("test_attribute"), false);
        assert!(!attribute.is_changeable());
    }
}
