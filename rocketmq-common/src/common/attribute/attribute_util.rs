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

#[derive(Debug, thiserror::Error)]
pub enum AttributeError {
    #[error("Only add attribute is supported while creating topic. Key: {0}")]
    CreateOnlySupportsAdd(String),

    #[error("Attempt to delete a nonexistent key: {0}")]
    DeleteNonexistentKey(String),

    #[error("Wrong format key: {0}")]
    WrongFormatKey(String),

    #[error("Alter duplication key. Key: {0}")]
    DuplicateKey(String),

    #[error("KV string format wrong")]
    KvStringFormatWrong,

    #[error("Unsupported key: {0}")]
    UnsupportedKey(String),

    #[error("Attempt to update an unchangeable attribute. Key: {0}")]
    UnchangeableAttribute(String),

    #[error("Attribute verification failed: {0}")]
    AttributeVerificationFailed(String),
}

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
    ) -> Result<HashMap<CheetahString, CheetahString>, AttributeError> {
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
                    return Err(AttributeError::CreateOnlySupportsAdd(real_key.to_string()));
                }
            } else if key.starts_with('+') {
                if !current_attributes.contains_key(&real_key) {
                    add.insert(real_key, value.clone());
                } else {
                    update.insert(real_key, value.clone());
                }
            } else if key.starts_with('-') {
                if !current_attributes.contains_key(&real_key) {
                    return Err(AttributeError::DeleteNonexistentKey(real_key.to_string()));
                }
                delete.insert(real_key, value.clone());
            } else {
                return Err(AttributeError::WrongFormatKey(real_key.to_string()));
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
    fn duplication_check(keys: &mut HashSet<String>, key: &str) -> Result<(), AttributeError> {
        if !keys.insert(key.to_string()) {
            return Err(AttributeError::DuplicateKey(key.to_string()));
        }
        Ok(())
    }

    /// Validate attribute key format
    fn validate(kv_attribute: &str) -> Result<(), AttributeError> {
        if kv_attribute.is_empty() {
            return Err(AttributeError::KvStringFormatWrong);
        }

        if kv_attribute.contains('+') {
            return Err(AttributeError::KvStringFormatWrong);
        }

        if kv_attribute.contains('-') {
            return Err(AttributeError::KvStringFormatWrong);
        }

        Ok(())
    }

    /// Validate attribute operations
    fn validate_alter(
        all: &HashMap<CheetahString, Arc<dyn Attribute>>,
        alter: &HashMap<CheetahString, CheetahString>,
        init: bool,
        delete: bool,
    ) -> Result<(), AttributeError> {
        for (key, value) in alter {
            let attribute = match all.get(key) {
                Some(attr) => attr,
                None => return Err(AttributeError::UnsupportedKey(key.to_string())),
            };

            if !init && !attribute.is_changeable() {
                return Err(AttributeError::UnchangeableAttribute(key.to_string()));
            }

            if !delete {
                attribute
                    .verify(value)
                    .map_err(|e| AttributeError::AttributeVerificationFailed(format!("Key: {key}, Error: {e}")))?;
            }
        }

        Ok(())
    }

    /// Extract the real key by removing the prefix (+ or -)
    fn real_key(key: &str) -> Result<CheetahString, AttributeError> {
        if key.len() < 2 {
            return Err(AttributeError::KvStringFormatWrong);
        }

        let prefix = key.chars().next().unwrap();
        if prefix != '+' && prefix != '-' {
            return Err(AttributeError::WrongFormatKey(key.to_string()));
        }
        Ok(key[1..].to_string().into())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

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
            "Only add attribute is supported while creating topic. Key: key1"
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
            "Attempt to delete a nonexistent key: key1"
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
        assert_eq!(result.unwrap_err().to_string(), "Wrong format key: key1");
    }
}
