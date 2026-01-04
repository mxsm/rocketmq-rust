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

use ahash::RandomState;
use cheetah_string::CheetahString;

/// Trait for expression evaluation context.
///
/// Provides an abstraction for accessing context variables during expression evaluation.
/// Implementations should provide efficient lookup of named values and access to the
/// complete key-value mapping.
///
/// # Thread Safety
///
/// Implementations are not required to be thread-safe by default. If cross-thread
/// sharing is needed, wrap the implementation in `Arc<Mutex<T>>` or similar synchronization
/// primitives.
///
/// # Example
///
/// ```ignore
/// let context = MessageEvaluationContext::default();
/// if let Some(value) = context.get("property_name") {
///     println!("Property value: {}", value);
/// }
/// ```
pub trait EvaluationContext {
    /// Get value by name from context.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the variable to retrieve
    ///
    /// # Returns
    ///
    /// Returns `Some(&CheetahString)` if the variable exists, `None` otherwise.
    fn get(&self, name: &str) -> Option<&CheetahString>;

    /// Get all context variables as a key-value mapping.
    ///
    /// # Returns
    ///
    /// Returns `Some(HashMap)` containing all variables, or `None` if the context
    /// has no variables or does not support bulk retrieval.
    fn key_values(&self) -> Option<HashMap<CheetahString, CheetahString>>;
}

/// Default implementation of `EvaluationContext` for message filtering.
///
/// This structure stores context variables (typically message properties) used during
/// expression evaluation in RocketMQ's message filtering system. It provides efficient
/// storage and retrieval of string key-value pairs.
///
/// # Features
///
/// - **Efficient Storage**: Uses `HashMap` with `ahash::RandomState` for fast lookups
/// - **Memory Efficient**: Leverages `CheetahString` for optimized string storage
/// - **Immutable After Construction**: Properties can be set during initialization, but the context
///   is typically used in a read-only manner during evaluation
///
/// # Thread Safety
///
/// This structure is not thread-safe by itself. For concurrent access, wrap it in
/// `Arc<Mutex<MessageEvaluationContext>>` or similar synchronization primitives.
///
/// # Example
///
/// ```ignore
/// use rocketmq_filter::expression::evaluation_context::MessageEvaluationContext;
/// use cheetah_string::CheetahString;
///
/// let mut context = MessageEvaluationContext::default();
/// context.put("key1", "value1");
/// context.put("key2", "value2");
///
/// assert_eq!(context.get("key1").map(|s| s.as_str()), Some("value1"));
/// assert_eq!(context.get("nonexistent"), None);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MessageEvaluationContext {
    /// Internal storage for context variables
    #[serde(with = "hashmap_serde")]
    properties: HashMap<CheetahString, CheetahString, RandomState>,
}

// Custom serde implementation for HashMap with RandomState
mod hashmap_serde {
    use super::*;
    use serde::Deserialize;
    use serde::Deserializer;
    use serde::Serialize;
    use serde::Serializer;

    pub fn serialize<S>(
        map: &HashMap<CheetahString, CheetahString, RandomState>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let standard_map: HashMap<CheetahString, CheetahString> =
            map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        standard_map.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<HashMap<CheetahString, CheetahString, RandomState>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let standard_map: HashMap<CheetahString, CheetahString> = HashMap::deserialize(deserializer)?;
        let mut ahash_map = HashMap::with_hasher(RandomState::default());
        ahash_map.extend(standard_map);
        Ok(ahash_map)
    }
}

impl Default for MessageEvaluationContext {
    /// Creates a new empty evaluation context.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let context = MessageEvaluationContext::default();
    /// assert!(context.is_empty());
    /// ```
    fn default() -> Self {
        Self {
            properties: HashMap::with_hasher(RandomState::default()),
        }
    }
}

impl MessageEvaluationContext {
    /// Creates a new empty evaluation context.
    ///
    /// This is equivalent to calling `MessageEvaluationContext::default()`.
    ///
    /// # Returns
    ///
    /// A new `MessageEvaluationContext` with no variables.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let context = MessageEvaluationContext::new();
    /// assert!(context.is_empty());
    /// ```
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new evaluation context with a pre-allocated capacity.
    ///
    /// This is useful when you know the approximate number of properties in advance,
    /// as it can reduce memory allocations during initialization.
    ///
    /// # Arguments
    ///
    /// * `capacity` - The initial capacity for the internal hash map
    ///
    /// # Returns
    ///
    /// A new `MessageEvaluationContext` with the specified capacity.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let context = MessageEvaluationContext::with_capacity(10);
    /// // Can efficiently store up to 10 properties without reallocation
    /// ```
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            properties: HashMap::with_capacity_and_hasher(capacity, RandomState::default()),
        }
    }

    /// Creates a new evaluation context from an existing hash map.
    ///
    /// # Arguments
    ///
    /// * `properties` - A hash map containing initial key-value pairs
    ///
    /// # Returns
    ///
    /// A new `MessageEvaluationContext` containing the provided properties.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::collections::HashMap;
    /// use ahash::RandomState;
    /// use cheetah_string::CheetahString;
    ///
    /// let mut props = HashMap::with_hasher(RandomState::default());
    /// props.insert(CheetahString::from("key"), CheetahString::from("value"));
    /// let context = MessageEvaluationContext::from_properties(props);
    /// ```
    #[inline]
    pub fn from_properties(properties: HashMap<CheetahString, CheetahString, RandomState>) -> Self {
        Self { properties }
    }

    /// Sets a variable in the context.
    ///
    /// If the key already exists, its value will be replaced with the new value.
    ///
    /// # Arguments
    ///
    /// * `key` - The name of the variable (can be any type convertible to `CheetahString`)
    /// * `value` - The value to associate with the key (can be any type convertible to
    ///   `CheetahString`)
    ///
    /// # Returns
    ///
    /// The previous value associated with the key, if any.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut context = MessageEvaluationContext::new();
    /// context.put("property1", "value1");
    /// context.put("property2", "value2");
    ///
    /// let old_value = context.put("property1", "new_value");
    /// assert_eq!(old_value.map(|s| s.as_str()), Some("value1"));
    /// ```
    #[inline]
    pub fn put(&mut self, key: impl Into<CheetahString>, value: impl Into<CheetahString>) -> Option<CheetahString> {
        self.properties.insert(key.into(), value.into())
    }

    /// Removes a variable from the context.
    ///
    /// # Arguments
    ///
    /// * `key` - The name of the variable to remove
    ///
    /// # Returns
    ///
    /// The value that was associated with the key, if it existed.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut context = MessageEvaluationContext::new();
    /// context.put("key", "value");
    /// let removed = context.remove("key");
    /// assert_eq!(removed.map(|s| s.as_str()), Some("value"));
    /// assert!(context.get("key").is_none());
    /// ```
    #[inline]
    pub fn remove(&mut self, key: &str) -> Option<CheetahString> {
        self.properties.remove(key)
    }

    /// Checks if the context contains a variable with the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The name of the variable to check
    ///
    /// # Returns
    ///
    /// `true` if the key exists in the context, `false` otherwise.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut context = MessageEvaluationContext::new();
    /// context.put("key", "value");
    /// assert!(context.contains_key("key"));
    /// assert!(!context.contains_key("nonexistent"));
    /// ```
    #[inline]
    pub fn contains_key(&self, key: &str) -> bool {
        self.properties.contains_key(key)
    }

    /// Returns the number of variables in the context.
    ///
    /// # Returns
    ///
    /// The count of key-value pairs in the context.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut context = MessageEvaluationContext::new();
    /// assert_eq!(context.len(), 0);
    /// context.put("key1", "value1");
    /// context.put("key2", "value2");
    /// assert_eq!(context.len(), 2);
    /// ```
    #[inline]
    pub fn len(&self) -> usize {
        self.properties.len()
    }

    /// Checks if the context is empty.
    ///
    /// # Returns
    ///
    /// `true` if the context contains no variables, `false` otherwise.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let context = MessageEvaluationContext::new();
    /// assert!(context.is_empty());
    /// ```
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.properties.is_empty()
    }

    /// Clears all variables from the context.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut context = MessageEvaluationContext::new();
    /// context.put("key", "value");
    /// context.clear();
    /// assert!(context.is_empty());
    /// ```
    #[inline]
    pub fn clear(&mut self) {
        self.properties.clear();
    }

    /// Returns an iterator over the key-value pairs in the context.
    ///
    /// # Returns
    ///
    /// An iterator yielding `(&CheetahString, &CheetahString)` tuples.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut context = MessageEvaluationContext::new();
    /// context.put("key1", "value1");
    /// context.put("key2", "value2");
    ///
    /// for (key, value) in context.iter() {
    ///     println!("{}: {}", key, value);
    /// }
    /// ```
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = (&CheetahString, &CheetahString)> {
        self.properties.iter()
    }

    /// Extends the context with key-value pairs from an iterator.
    ///
    /// # Arguments
    ///
    /// * `iter` - An iterator yielding key-value pairs
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::collections::HashMap;
    /// use ahash::RandomState;
    /// use cheetah_string::CheetahString;
    ///
    /// let mut context = MessageEvaluationContext::new();
    /// let mut props = HashMap::with_hasher(RandomState::default());
    /// props.insert(CheetahString::from("key1"), CheetahString::from("value1"));
    /// props.insert(CheetahString::from("key2"), CheetahString::from("value2"));
    ///
    /// context.extend(props);
    /// assert_eq!(context.len(), 2);
    /// ```
    #[inline]
    pub fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = (CheetahString, CheetahString)>,
    {
        self.properties.extend(iter);
    }
}

impl EvaluationContext for MessageEvaluationContext {
    /// Get value by name from context.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the variable to retrieve
    ///
    /// # Returns
    ///
    /// Returns `Some(&CheetahString)` if the variable exists, `None` otherwise.
    #[inline]
    fn get(&self, name: &str) -> Option<&CheetahString> {
        self.properties.get(name)
    }

    /// Get all context variables as a key-value mapping.
    ///
    /// Returns a copy of all variables in the context using standard `HashMap`.
    ///
    /// # Returns
    ///
    /// Returns `Some(HashMap)` containing all variables. Never returns `None`
    /// for this implementation (returns empty map if no variables exist).
    fn key_values(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        Some(self.properties.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_context_is_empty() {
        let context = MessageEvaluationContext::new();
        assert!(context.is_empty());
        assert_eq!(context.len(), 0);
    }

    #[test]
    fn test_default_context_is_empty() {
        let context = MessageEvaluationContext::default();
        assert!(context.is_empty());
        assert_eq!(context.len(), 0);
    }

    #[test]
    fn test_put_and_get() {
        let mut context = MessageEvaluationContext::new();
        context.put("key1", "value1");
        context.put("key2", "value2");

        assert_eq!(context.get("key1").map(|s| s.as_str()), Some("value1"));
        assert_eq!(context.get("key2").map(|s| s.as_str()), Some("value2"));
        assert_eq!(context.get("nonexistent"), None);
        assert_eq!(context.len(), 2);
    }

    #[test]
    fn test_put_overwrites_existing() {
        let mut context = MessageEvaluationContext::new();
        context.put("key", "value1");
        let old = context.put("key", "value2");

        assert_eq!(old.as_ref().map(|s| s.as_str()), Some("value1"));
        assert_eq!(context.get("key").map(|s| s.as_str()), Some("value2"));
        assert_eq!(context.len(), 1);
    }

    #[test]
    fn test_remove() {
        let mut context = MessageEvaluationContext::new();
        context.put("key", "value");

        let removed = context.remove("key");
        assert_eq!(removed.as_ref().map(|s| s.as_str()), Some("value"));
        assert!(context.get("key").is_none());
        assert!(context.is_empty());
    }

    #[test]
    fn test_contains_key() {
        let mut context = MessageEvaluationContext::new();
        context.put("key", "value");

        assert!(context.contains_key("key"));
        assert!(!context.contains_key("nonexistent"));
    }

    #[test]
    fn test_clear() {
        let mut context = MessageEvaluationContext::new();
        context.put("key1", "value1");
        context.put("key2", "value2");

        context.clear();
        assert!(context.is_empty());
        assert_eq!(context.len(), 0);
    }

    #[test]
    fn test_with_capacity() {
        let context = MessageEvaluationContext::with_capacity(10);
        assert!(context.is_empty());
    }

    #[test]
    fn test_from_properties() {
        let mut props = HashMap::with_hasher(RandomState::default());
        props.insert(CheetahString::from("key1"), CheetahString::from("value1"));
        props.insert(CheetahString::from("key2"), CheetahString::from("value2"));

        let context = MessageEvaluationContext::from_properties(props);
        assert_eq!(context.len(), 2);
        assert_eq!(context.get("key1").map(|s| s.as_str()), Some("value1"));
    }

    #[test]
    fn test_iter() {
        let mut context = MessageEvaluationContext::new();
        context.put("key1", "value1");
        context.put("key2", "value2");

        let mut count = 0;
        for (_key, _value) in context.iter() {
            count += 1;
        }
        assert_eq!(count, 2);
    }

    #[test]
    fn test_extend() {
        let mut context = MessageEvaluationContext::new();
        let mut props = HashMap::with_hasher(RandomState::default());
        props.insert(CheetahString::from("key1"), CheetahString::from("value1"));
        props.insert(CheetahString::from("key2"), CheetahString::from("value2"));

        context.extend(props);
        assert_eq!(context.len(), 2);
    }

    #[test]
    fn test_key_values() {
        let mut context = MessageEvaluationContext::new();
        context.put("key1", "value1");
        context.put("key2", "value2");

        let key_values = context.key_values();
        assert!(key_values.is_some());
        let map = key_values.unwrap();
        assert_eq!(map.len(), 2);
        assert_eq!(
            map.get(&CheetahString::from("key1")).map(|s| s.as_str()),
            Some("value1")
        );
    }

    #[test]
    fn test_evaluation_context_trait() {
        let mut context = MessageEvaluationContext::new();
        context.put("property", "value");

        let ctx: &dyn EvaluationContext = &context;
        assert_eq!(ctx.get("property").map(|s| s.as_str()), Some("value"));
        assert!(ctx.get("nonexistent").is_none());

        let key_values = ctx.key_values();
        assert!(key_values.is_some());
        assert_eq!(key_values.unwrap().len(), 1);
    }

    #[test]
    fn test_clone() {
        let mut context = MessageEvaluationContext::new();
        context.put("key", "value");

        let cloned = context.clone();
        assert_eq!(cloned.get("key").map(|s| s.as_str()), Some("value"));
        assert_eq!(context, cloned);
    }

    #[test]
    fn test_debug_format() {
        let mut context = MessageEvaluationContext::new();
        context.put("key", "value");

        let debug_str = format!("{:?}", context);
        assert!(debug_str.contains("MessageEvaluationContext"));
    }

    #[test]
    fn test_serde_serialize_deserialize() {
        let mut context = MessageEvaluationContext::new();
        context.put("key1", "value1");
        context.put("key2", "value2");

        // Serialize
        let json = serde_json::to_string(&context).unwrap();
        assert!(json.contains("key1"));
        assert!(json.contains("value1"));

        // Deserialize
        let deserialized: MessageEvaluationContext = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.len(), 2);
        assert_eq!(deserialized.get("key1").map(|s| s.as_str()), Some("value1"));
        assert_eq!(deserialized.get("key2").map(|s| s.as_str()), Some("value2"));
    }
}
