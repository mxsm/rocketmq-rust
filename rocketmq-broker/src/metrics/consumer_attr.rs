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

use std::fmt::Display;
use std::hash::Hash;
use std::hash::Hasher;

use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::LanguageCode;
use serde::Deserialize;
use serde::Serialize;

/// Consumer attributes for metrics tracking
/// Equivalent to Java's ConsumerAttr class
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerAttr {
    /// Consumer group name
    pub group: String,

    /// Programming language of the consumer client
    pub language: LanguageCode,

    /// Client version
    pub version: i32,

    /// Consume mode (push/pull)
    pub consume_mode: ConsumeType,
}

impl ConsumerAttr {
    /// Create new consumer attributes
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    /// * `language` - Programming language of the consumer client
    /// * `version` - Client version
    /// * `consume_mode` - Consume mode (push/pull)
    pub fn new(group: String, language: LanguageCode, version: i32, consume_mode: ConsumeType) -> Self {
        Self {
            group,
            language,
            version,
            consume_mode,
        }
    }

    /// Get consumer group name
    pub fn get_group(&self) -> &str {
        &self.group
    }

    /// Set consumer group name
    pub fn set_group(&mut self, group: String) {
        self.group = group;
    }

    /// Get language code
    pub fn get_language(&self) -> LanguageCode {
        self.language
    }

    /// Set language code
    pub fn set_language(&mut self, language: LanguageCode) {
        self.language = language;
    }

    /// Get client version
    pub fn get_version(&self) -> i32 {
        self.version
    }

    /// Set client version
    pub fn set_version(&mut self, version: i32) {
        self.version = version;
    }

    /// Get consume mode
    pub fn get_consume_mode(&self) -> ConsumeType {
        self.consume_mode
    }

    /// Set consume mode
    pub fn set_consume_mode(&mut self, consume_mode: ConsumeType) {
        self.consume_mode = consume_mode;
    }

    /// Create a builder for fluent construction
    pub fn builder() -> ConsumerAttrBuilder {
        ConsumerAttrBuilder::new()
    }
}

impl Display for ConsumerAttr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ConsumerAttr{{group: {}, language: {:?}, version: {}, consume_mode: {:?}}}",
            self.group, self.language, self.version, self.consume_mode
        )
    }
}

// Implement PartialEq for equality comparison (equivalent to Java's equals method)
impl PartialEq for ConsumerAttr {
    fn eq(&self, other: &Self) -> bool {
        self.group == other.group
            && self.language == other.language
            && self.version == other.version
            && self.consume_mode == other.consume_mode
    }
}

// Implement Eq since PartialEq is implemented and the relation is reflexive
impl Eq for ConsumerAttr {}

// Implement Hash for use in HashMap/HashSet (equivalent to Java's hashCode method)
impl Hash for ConsumerAttr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.group.hash(state);
        self.language.hash(state);
        self.version.hash(state);
        self.consume_mode.hash(state);
    }
}

/// Builder for ConsumerAttr with fluent API
#[derive(Debug, Default)]
pub struct ConsumerAttrBuilder {
    group: Option<String>,
    language: Option<LanguageCode>,
    version: Option<i32>,
    consume_mode: Option<ConsumeType>,
}

impl ConsumerAttrBuilder {
    /// Create new builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Set consumer group
    pub fn group<S: Into<String>>(mut self, group: S) -> Self {
        self.group = Some(group.into());
        self
    }

    /// Set language code
    pub fn language(mut self, language: LanguageCode) -> Self {
        self.language = Some(language);
        self
    }

    /// Set version
    pub fn version(mut self, version: i32) -> Self {
        self.version = Some(version);
        self
    }

    /// Set consume mode
    pub fn consume_mode(mut self, consume_mode: ConsumeType) -> Self {
        self.consume_mode = Some(consume_mode);
        self
    }

    /// Build the ConsumerAttr
    pub fn build(self) -> Result<ConsumerAttr, ConsumerAttrError> {
        Ok(ConsumerAttr {
            group: self.group.ok_or(ConsumerAttrError::MissingGroup)?,
            language: self.language.ok_or(ConsumerAttrError::MissingLanguage)?,
            version: self.version.ok_or(ConsumerAttrError::MissingVersion)?,
            consume_mode: self.consume_mode.ok_or(ConsumerAttrError::MissingConsumeMode)?,
        })
    }
}

/// Errors that can occur when building ConsumerAttr
#[allow(clippy::enum_variant_names)]
#[derive(Debug, thiserror::Error)]
pub enum ConsumerAttrError {
    #[error("Consumer group is required")]
    MissingGroup,

    #[error("Language code is required")]
    MissingLanguage,

    #[error("Version is required")]
    MissingVersion,

    #[error("Consume mode is required")]
    MissingConsumeMode,
}

/// Extension trait for ConsumerAttr collections
pub trait ConsumerAttrExt {
    /// Find consumer attributes by group name
    fn find_by_group(&self, group: &str) -> Vec<&ConsumerAttr>;

    /// Find consumer attributes by language
    fn find_by_language(&self, language: LanguageCode) -> Vec<&ConsumerAttr>;

    /// Find consumer attributes by consume mode
    fn find_by_consume_mode(&self, consume_mode: ConsumeType) -> Vec<&ConsumerAttr>;

    /// Get unique groups
    fn unique_groups(&self) -> Vec<String>;
}

impl ConsumerAttrExt for Vec<ConsumerAttr> {
    fn find_by_group(&self, group: &str) -> Vec<&ConsumerAttr> {
        self.iter().filter(|attr| attr.group == group).collect()
    }

    fn find_by_language(&self, language: LanguageCode) -> Vec<&ConsumerAttr> {
        self.iter().filter(|attr| attr.language == language).collect()
    }

    fn find_by_consume_mode(&self, consume_mode: ConsumeType) -> Vec<&ConsumerAttr> {
        self.iter().filter(|attr| attr.consume_mode == consume_mode).collect()
    }

    fn unique_groups(&self) -> Vec<String> {
        use std::collections::HashSet;
        let mut groups: HashSet<String> = HashSet::new();
        for attr in self {
            groups.insert(attr.group.clone());
        }
        groups.into_iter().collect()
    }
}

impl ConsumerAttrExt for &[ConsumerAttr] {
    fn find_by_group(&self, group: &str) -> Vec<&ConsumerAttr> {
        self.iter().filter(|attr| attr.group == group).collect()
    }

    fn find_by_language(&self, language: LanguageCode) -> Vec<&ConsumerAttr> {
        self.iter().filter(|attr| attr.language == language).collect()
    }

    fn find_by_consume_mode(&self, consume_mode: ConsumeType) -> Vec<&ConsumerAttr> {
        self.iter().filter(|attr| attr.consume_mode == consume_mode).collect()
    }

    fn unique_groups(&self) -> Vec<String> {
        use std::collections::HashSet;
        let mut groups: HashSet<String> = HashSet::new();
        for attr in *self {
            groups.insert(attr.group.clone());
        }
        groups.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn test_consumer_attr_creation() {
        let attr = ConsumerAttr::new(
            "test-group".to_string(),
            LanguageCode::RUST,
            123,
            ConsumeType::ConsumeActively,
        );

        assert_eq!(attr.group, "test-group");
        assert_eq!(attr.language, LanguageCode::RUST);
        assert_eq!(attr.version, 123);
        assert_eq!(attr.consume_mode, ConsumeType::ConsumeActively);
    }

    #[test]
    fn test_consumer_attr_equality() {
        let attr1 = ConsumerAttr::new(
            "test-group".to_string(),
            LanguageCode::JAVA,
            100,
            ConsumeType::ConsumeActively,
        );

        let attr2 = ConsumerAttr::new(
            "test-group".to_string(),
            LanguageCode::JAVA,
            100,
            ConsumeType::ConsumeActively,
        );

        let attr3 = ConsumerAttr::new(
            "different-group".to_string(),
            LanguageCode::JAVA,
            100,
            ConsumeType::ConsumeActively,
        );

        assert_eq!(attr1, attr2);
        assert_ne!(attr1, attr3);
    }

    #[test]
    fn test_consumer_attr_hash() {
        let mut attrs = HashSet::new();

        let attr1 = ConsumerAttr::new(
            "group1".to_string(),
            LanguageCode::JAVA,
            100,
            ConsumeType::ConsumeActively,
        );

        let attr2 = ConsumerAttr::new(
            "group1".to_string(),
            LanguageCode::JAVA,
            100,
            ConsumeType::ConsumeActively,
        );

        let attr3 = ConsumerAttr::new(
            "group2".to_string(),
            LanguageCode::CPP,
            200,
            ConsumeType::ConsumeActively,
        );

        attrs.insert(attr1);
        attrs.insert(attr2); // Should not be inserted (duplicate)
        attrs.insert(attr3);

        assert_eq!(attrs.len(), 2);
    }

    #[test]
    fn test_consumer_attr_as_map_key() {
        let mut map = HashMap::new();

        let attr1 = ConsumerAttr::new(
            "group1".to_string(),
            LanguageCode::PYTHON,
            150,
            ConsumeType::ConsumeActively,
        );

        let attr2 = ConsumerAttr::new(
            "group2".to_string(),
            LanguageCode::GO,
            200,
            ConsumeType::ConsumeActively,
        );

        map.insert(attr1.clone(), "value1");
        map.insert(attr2.clone(), "value2");

        assert_eq!(map.get(&attr1), Some(&"value1"));
        assert_eq!(map.get(&attr2), Some(&"value2"));
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn test_builder_pattern() {
        let attr = ConsumerAttr::builder()
            .group("test-group")
            .language(LanguageCode::RUST)
            .version(456)
            .consume_mode(ConsumeType::ConsumeActively)
            .build()
            .unwrap();

        assert_eq!(attr.group, "test-group");
        assert_eq!(attr.language, LanguageCode::RUST);
        assert_eq!(attr.version, 456);
        assert_eq!(attr.consume_mode, ConsumeType::ConsumeActively);
    }

    #[test]
    fn test_builder_missing_fields() {
        let result = ConsumerAttr::builder()
            .group("test-group")
            .language(LanguageCode::JAVA)
            // Missing version and consume_mode
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_getters_and_setters() {
        let mut attr = ConsumerAttr::new(
            "initial-group".to_string(),
            LanguageCode::JAVA,
            100,
            ConsumeType::ConsumeActively,
        );

        assert_eq!(attr.get_group(), "initial-group");
        assert_eq!(attr.get_language(), LanguageCode::JAVA);
        assert_eq!(attr.get_version(), 100);
        assert_eq!(attr.get_consume_mode(), ConsumeType::ConsumeActively);

        attr.set_group("updated-group".to_string());
        attr.set_language(LanguageCode::RUST);
        attr.set_version(200);
        attr.set_consume_mode(ConsumeType::ConsumeActively);

        assert_eq!(attr.get_group(), "updated-group");
        assert_eq!(attr.get_language(), LanguageCode::RUST);
        assert_eq!(attr.get_version(), 200);
        assert_eq!(attr.get_consume_mode(), ConsumeType::ConsumeActively);
    }

    #[test]
    fn test_extension_traits() {
        let attrs = vec![
            ConsumerAttr::new(
                "group1".to_string(),
                LanguageCode::JAVA,
                100,
                ConsumeType::ConsumeActively,
            ),
            ConsumerAttr::new(
                "group1".to_string(),
                LanguageCode::RUST,
                200,
                ConsumeType::ConsumeActively,
            ),
            ConsumerAttr::new(
                "group2".to_string(),
                LanguageCode::JAVA,
                150,
                ConsumeType::ConsumeActively,
            ),
        ];

        let group1_attrs = attrs.find_by_group("group1");
        assert_eq!(group1_attrs.len(), 2);

        let java_attrs = attrs.find_by_language(LanguageCode::JAVA);
        assert_eq!(java_attrs.len(), 2);

        let pull_attrs = attrs.find_by_consume_mode(ConsumeType::ConsumeActively);
        assert_eq!(pull_attrs.len(), 3);

        let unique_groups = attrs.unique_groups();
        assert_eq!(unique_groups.len(), 2);
        assert!(unique_groups.contains(&"group1".to_string()));
        assert!(unique_groups.contains(&"group2".to_string()));
    }

    #[test]
    fn test_language_code_conversion() {
        assert_eq!(LanguageCode::from(0), LanguageCode::JAVA);
        assert_eq!(LanguageCode::from(12), LanguageCode::RUST);
        assert_eq!(LanguageCode::from(999), LanguageCode::OTHER);

        assert_eq!(i32::from(LanguageCode::JAVA), 0);
        assert_eq!(i32::from(LanguageCode::RUST), 12);
    }

    #[test]
    fn test_to_string() {
        let attr = ConsumerAttr::new(
            "test-group".to_string(),
            LanguageCode::RUST,
            123,
            ConsumeType::ConsumeActively,
        );

        let string_repr = attr.to_string();
        assert!(string_repr.contains("test-group"));
        assert!(string_repr.contains("RUST"));
        assert!(string_repr.contains("123"));
    }

    #[test]
    fn test_serialization() {
        let attr = ConsumerAttr::new(
            "test-group".to_string(),
            LanguageCode::RUST,
            123,
            ConsumeType::ConsumeActively,
        );

        // Test JSON serialization (if serde_json is available)
        let json = serde_json::to_string(&attr).unwrap();
        let deserialized: ConsumerAttr = serde_json::from_str(&json).unwrap();

        assert_eq!(attr, deserialized);
    }
}
