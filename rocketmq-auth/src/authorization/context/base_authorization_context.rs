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

/// Base authorization context containing common fields for all authorization contexts.
///
/// This struct provides the foundational properties that are shared across
/// all types of authorization contexts in the RocketMQ system.
#[derive(Debug, Clone, Default)]
pub struct BaseAuthorizationContext {
    /// Unique identifier for the communication channel
    channel_id: Option<String>,

    /// RPC code identifying the type of request
    rpc_code: Option<String>,

    /// Extension information as key-value pairs for additional context
    ext_info: Option<HashMap<String, String>>,
}

impl BaseAuthorizationContext {
    /// Creates a new empty `BaseAuthorizationContext`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Gets the channel ID.
    pub fn channel_id(&self) -> Option<&str> {
        self.channel_id.as_deref()
    }

    /// Sets the channel ID.
    pub fn set_channel_id(&mut self, channel_id: String) {
        self.channel_id = Some(channel_id);
    }

    /// Gets the RPC code.
    pub fn rpc_code(&self) -> Option<&str> {
        self.rpc_code.as_deref()
    }

    /// Sets the RPC code.
    pub fn set_rpc_code(&mut self, rpc_code: String) {
        self.rpc_code = Some(rpc_code);
    }

    /// Gets an extension info value by key.
    pub fn get_ext_info(&self, key: &str) -> Option<&str> {
        self.ext_info.as_ref()?.get(key).map(|s| s.as_str())
    }

    /// Sets an extension info key-value pair.
    pub fn set_ext_info(&mut self, key: String, value: String) {
        if key.is_empty() {
            return;
        }
        self.ext_info.get_or_insert_with(HashMap::new).insert(key, value);
    }

    /// Checks if an extension info key exists.
    pub fn has_ext_info(&self, key: &str) -> bool {
        self.ext_info.as_ref().is_some_and(|map| map.contains_key(key))
    }

    /// Gets a reference to all extension info.
    pub fn ext_info(&self) -> Option<&HashMap<String, String>> {
        self.ext_info.as_ref()
    }

    /// Sets the entire extension info map.
    pub fn set_ext_info_map(&mut self, ext_info: HashMap<String, String>) {
        self.ext_info = Some(ext_info);
    }

    /// Takes ownership of the extension info map, leaving `None` in its place.
    pub fn take_ext_info(&mut self) -> Option<HashMap<String, String>> {
        self.ext_info.take()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_context() {
        let context = BaseAuthorizationContext::new();
        assert!(context.channel_id().is_none());
        assert!(context.rpc_code().is_none());
        assert!(context.ext_info().is_none());
    }

    #[test]
    fn test_channel_id() {
        let mut context = BaseAuthorizationContext::new();
        context.set_channel_id("channel-123".to_string());
        assert_eq!(context.channel_id(), Some("channel-123"));
    }

    #[test]
    fn test_rpc_code() {
        let mut context = BaseAuthorizationContext::new();
        context.set_rpc_code("10".to_string());
        assert_eq!(context.rpc_code(), Some("10"));
    }

    #[test]
    fn test_ext_info_operations() {
        let mut context = BaseAuthorizationContext::new();

        // Initially empty
        assert!(!context.has_ext_info("key1"));
        assert!(context.get_ext_info("key1").is_none());

        // Add values
        context.set_ext_info("key1".to_string(), "value1".to_string());
        context.set_ext_info("key2".to_string(), "value2".to_string());

        // Check existence
        assert!(context.has_ext_info("key1"));
        assert!(context.has_ext_info("key2"));
        assert!(!context.has_ext_info("key3"));

        // Check values
        assert_eq!(context.get_ext_info("key1"), Some("value1"));
        assert_eq!(context.get_ext_info("key2"), Some("value2"));
        assert_eq!(context.get_ext_info("key3"), None);

        // Check map
        let map = context.ext_info().unwrap();
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("key1").unwrap(), "value1");
    }

    #[test]
    fn test_set_ext_info_map() {
        let mut context = BaseAuthorizationContext::new();
        let mut map = HashMap::new();
        map.insert("key1".to_string(), "value1".to_string());
        map.insert("key2".to_string(), "value2".to_string());

        context.set_ext_info_map(map);

        assert_eq!(context.get_ext_info("key1"), Some("value1"));
        assert_eq!(context.get_ext_info("key2"), Some("value2"));
        assert_eq!(context.ext_info().unwrap().len(), 2);
    }

    #[test]
    fn test_take_ext_info() {
        let mut context = BaseAuthorizationContext::new();
        context.set_ext_info("key1".to_string(), "value1".to_string());

        let taken = context.take_ext_info();
        assert!(taken.is_some());
        assert_eq!(taken.unwrap().get("key1").unwrap(), "value1");

        // After taking, ext_info should be None
        assert!(context.ext_info().is_none());
    }

    #[test]
    fn test_empty_key_ignored() {
        let mut context = BaseAuthorizationContext::new();
        context.set_ext_info("".to_string(), "value".to_string());

        // Empty key should be ignored
        assert!(context.ext_info().is_none());
    }
}
