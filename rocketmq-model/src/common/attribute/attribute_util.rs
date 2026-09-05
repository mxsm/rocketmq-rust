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

use std::collections::HashMap;
use std::collections::HashSet;
use std::string::ToString;
use std::sync::Arc;

use cheetah_string::CheetahString;
use tracing::info;

use crate::common::attribute::Attribute;
use crate::ModelContractViolation;

/// Utility for working with topic attributes
pub struct AttributeUtil;

impl AttributeUtil {
    /// Alter the current attributes based on new attribute requests
    ///
    /// # Arguments
    ///
    /// * `create` - If true, we're creating new attributes, otherwise updating existing ones
    /// * `all` - Map of all supported attributes
    /// * `current_attributes` - Current attribute values
    /// * `new_attributes` - New attribute operations (prefixed with + or -)
    ///
    /// # Returns
    ///
    /// A Result containing the final attribute map or an error
    #[allow(clippy::map_entry)]
    pub fn alter_current_attributes(
        create: bool,
        all: &HashMap<CheetahString, Arc<dyn Attribute>>,
        current_attributes: &HashMap<CheetahString, CheetahString>,
        new_attributes: &HashMap<CheetahString, CheetahString>,
    ) -> Result<HashMap<CheetahString, CheetahString>, ModelContractViolation> {
        let mut init = HashMap::new();
        let mut add = HashMap::new();
        let mut update = HashMap::new();
        let mut delete = HashMap::new();
        let mut keys = HashSet::new();

        // Process new attribute operations
        for (key, value) in new_attributes {
            let real_key = Self::real_key(key)?;

            Self::validate(&real_key)?;
            Self::duplication_check(&mut keys, &real_key)?;

            if create {
                if key.starts_with('+') {
                    init.insert(real_key, value.clone());
                } else {
                    return Err(ModelContractViolation::AttributeCreateRequiresAdd);
                }
            } else if key.starts_with('+') {
                if !current_attributes.contains_key(&real_key) {
                    add.insert(real_key, value.clone());
                } else {
                    update.insert(real_key, value.clone());
                }
            } else if key.starts_with('-') {
                if !current_attributes.contains_key(&real_key) {
                    return Err(ModelContractViolation::AttributeDeleteTargetsMissingKey);
                }
                delete.insert(real_key, value.clone());
            } else {
                return Err(ModelContractViolation::AttributeOperationKeyHasUnsupportedForm);
            }
        }

        // Validate all operations
        Self::validate_alter(all, &init, true, false)?;
        Self::validate_alter(all, &add, false, false)?;
        Self::validate_alter(all, &update, false, false)?;
        Self::validate_alter(all, &delete, false, true)?;

        info!("add: {:?}, update: {:?}, delete: {:?}", add, update, delete);

        // Create final attribute map
        let mut final_attributes = current_attributes.clone();

        // Apply changes
        for (k, v) in init {
            final_attributes.insert(k, v);
        }

        for (k, v) in add {
            final_attributes.insert(k, v);
        }

        for (k, v) in update {
            final_attributes.insert(k, v);
        }

        for k in delete.keys() {
            final_attributes.remove(k);
        }

        Ok(final_attributes)
    }

    /// Check for key duplication in the operation set
    fn duplication_check(keys: &mut HashSet<String>, key: &str) -> Result<(), ModelContractViolation> {
        if !keys.insert(key.to_string()) {
            return Err(ModelContractViolation::AttributeOperationSetContainsDuplicateKey);
        }
        Ok(())
    }

    /// Validate attribute key format
    fn validate(kv_attribute: &str) -> Result<(), ModelContractViolation> {
        if kv_attribute.is_empty() {
            return Err(ModelContractViolation::AttributeOperationKeyIsInvalid);
        }

        if kv_attribute.contains('+') {
            return Err(ModelContractViolation::AttributeOperationKeyIsInvalid);
        }

        if kv_attribute.contains('-') {
            return Err(ModelContractViolation::AttributeOperationKeyIsInvalid);
        }

        Ok(())
    }

    /// Validate attribute operations
    fn validate_alter(
        all: &HashMap<CheetahString, Arc<dyn Attribute>>,
        alter: &HashMap<CheetahString, CheetahString>,
        init: bool,
        delete: bool,
    ) -> Result<(), ModelContractViolation> {
        for (key, value) in alter {
            let attribute = match all.get(key) {
                Some(attr) => attr,
                None => return Err(ModelContractViolation::AttributeOperationTargetsUnsupportedKey),
            };

            if !init && !attribute.is_changeable() {
                return Err(ModelContractViolation::AttributeUpdateTargetsImmutableAttribute);
            }

            if !delete {
                attribute
                    .verify(value)
                    .map_err(|_| ModelContractViolation::AttributeValueDoesNotSatisfyRules)?;
            }
        }

        Ok(())
    }

    /// Extract the real key by removing the prefix (+ or -)
    fn real_key(key: &str) -> Result<CheetahString, ModelContractViolation> {
        if key.len() < 2 {
            return Err(ModelContractViolation::AttributeOperationKeyIsInvalid);
        }

        let Some(real_key) = key.strip_prefix('+').or_else(|| key.strip_prefix('-')) else {
            return Err(ModelContractViolation::AttributeOperationKeyHasUnsupportedForm);
        };
        if real_key.is_empty() {
            return Err(ModelContractViolation::AttributeOperationKeyIsInvalid);
        }
        Ok(real_key.to_string().into())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn alter_current_attributes_create_only_supports_add() {
        let all = HashMap::new();
        let current_attributes = HashMap::new();
        let mut new_attributes = HashMap::new();
        new_attributes.insert("-key1".into(), "".into());

        let result = AttributeUtil::alter_current_attributes(true, &all, &current_attributes, &new_attributes);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "topic creation attribute operation must use the add form"
        );
    }

    #[test]
    fn alter_current_attributes_delete_nonexistent_key() {
        let all = HashMap::new();
        let current_attributes = HashMap::new();
        let mut new_attributes = HashMap::new();
        new_attributes.insert("-key1".into(), "".into());

        let result = AttributeUtil::alter_current_attributes(false, &all, &current_attributes, &new_attributes);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "attribute delete operation targets a missing key"
        );
    }

    #[test]
    fn alter_current_attributes_wrong_format_key() {
        let all = HashMap::new();
        let current_attributes = HashMap::new();
        let mut new_attributes = HashMap::new();
        new_attributes.insert("key1".into(), "value1".into());

        let result = AttributeUtil::alter_current_attributes(false, &all, &current_attributes, &new_attributes);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "attribute operation key has an unsupported form"
        );
    }
}
