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

/// Container for batched trace data ready for transmission.
///
/// Holds encoded trace data and associated message keys for dispatch to the trace backend.
#[derive(Debug, Clone, Default)]
pub struct TraceTransferBean {
    /// Encoded trace data
    pub trans_data: CheetahString,
    /// Message keys associated with this batch
    pub trans_key: HashSet<CheetahString>,
}

impl TraceTransferBean {
    /// Creates an empty trace transfer bean.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a trace transfer bean with encoded data and keys.
    #[inline]
    pub fn with_data(trans_data: CheetahString, trans_key: HashSet<CheetahString>) -> Self {
        Self { trans_data, trans_key }
    }

    /// Sets the encoded trace data.
    #[inline]
    pub fn set_trans_data(&mut self, trans_data: CheetahString) {
        self.trans_data = trans_data;
    }

    /// Returns a reference to the encoded trace data.
    #[inline]
    pub fn trans_data(&self) -> &CheetahString {
        &self.trans_data
    }

    /// Sets the message keys.
    #[inline]
    pub fn set_trans_key(&mut self, trans_key: HashSet<CheetahString>) {
        self.trans_key = trans_key;
    }

    /// Returns a reference to the message keys.
    #[inline]
    pub fn trans_key(&self) -> &HashSet<CheetahString> {
        &self.trans_key
    }

    /// Returns a mutable reference to the message keys.
    #[inline]
    pub fn trans_key_mut(&mut self) -> &mut HashSet<CheetahString> {
        &mut self.trans_key
    }

    /// Inserts a key into the set.
    ///
    /// Returns `true` if the key was newly inserted, `false` if it already existed.
    #[inline]
    pub fn add_key(&mut self, key: CheetahString) -> bool {
        self.trans_key.insert(key)
    }

    /// Returns `true` if both trace data and keys are empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.trans_data.is_empty() && self.trans_key.is_empty()
    }

    /// Returns the number of keys.
    #[inline]
    pub fn key_count(&self) -> usize {
        self.trans_key.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cheetah_string::CheetahString;
    use std::collections::HashSet;

    #[test]
    fn test_new_creates_empty_bean() {
        let bean = TraceTransferBean::new();
        assert!(bean.trans_data.is_empty());
        assert!(bean.trans_key.is_empty());
        assert!(bean.is_empty());
    }

    #[test]
    fn test_with_data_initializes_correctly() {
        let data = CheetahString::from("trace_payload");
        let mut keys = HashSet::new();
        keys.insert(CheetahString::from("key1"));

        let bean = TraceTransferBean::with_data(data.clone(), keys.clone());

        assert_eq!(bean.trans_data(), &data);
        assert_eq!(bean.trans_key(), &keys);
        assert!(!bean.is_empty());
    }

    #[test]
    fn test_setters_and_getters() {
        let mut bean = TraceTransferBean::new();
        let data = CheetahString::from("new_data");
        let mut keys = HashSet::new();
        keys.insert(CheetahString::from("key_a"));

        bean.set_trans_data(data.clone());
        bean.set_trans_key(keys.clone());

        assert_eq!(bean.trans_data(), &data);
        assert_eq!(bean.trans_key(), &keys);
    }

    #[test]
    fn test_add_key_logic() {
        let mut bean = TraceTransferBean::new();
        let key = CheetahString::from("unique_key");

        assert!(bean.add_key(key.clone()));
        assert_eq!(bean.key_count(), 1);

        assert!(!bean.add_key(key));
        assert_eq!(bean.key_count(), 1);
    }

    #[test]
    fn test_trans_key_mut() {
        let mut bean = TraceTransferBean::new();
        bean.trans_key_mut().insert(CheetahString::from("manual_key"));

        assert!(bean.trans_key().contains(&CheetahString::from("manual_key")));
        assert_eq!(bean.key_count(), 1);
    }

    #[test]
    fn test_is_empty_conditions() {
        let mut bean = TraceTransferBean::new();
        assert!(bean.is_empty(), "Should be empty initially");

        bean.set_trans_data(CheetahString::from("data"));
        assert!(!bean.is_empty());

        bean.set_trans_data(CheetahString::from(""));
        bean.add_key(CheetahString::from("key"));
        assert!(!bean.is_empty());
    }

    #[test]
    fn test_clone_and_debug() {
        let bean =
            TraceTransferBean::with_data(CheetahString::from("data"), HashSet::from([CheetahString::from("key")]));
        let cloned = bean.clone();

        assert_eq!(bean.trans_data, cloned.trans_data);
        let debug_str = format!("{:?}", bean);
        assert!(debug_str.contains("TraceTransferBean"));
    }
}
