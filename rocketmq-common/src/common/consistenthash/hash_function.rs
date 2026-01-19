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

//! Hash function trait for consistent hashing
//!
//! Provides an abstraction for hashing string keys to 64-bit integer values.

/// Trait for hashing string keys to long integer values
///
/// Implementations of this trait provide different hash algorithms
/// that can be used in consistent hashing rings to distribute keys
/// across nodes.
///
/// # Example
/// ```
/// use rocketmq_common::common::consistenthash::HashFunction;
///
/// struct SimpleHashFunction;
///
/// impl HashFunction for SimpleHashFunction {
///     fn hash(&self, key: &str) -> i64 {
///         // Simple example: sum of character codes
///         key.chars().map(|c| c as i64).sum()
///     }
/// }
///
/// let hasher = SimpleHashFunction;
/// let hash_value = hasher.hash("test-key");
/// assert!(hash_value > 0);
/// ```
pub trait HashFunction {
    /// Hash a string key to a 64-bit signed integer value
    ///
    /// # Arguments
    /// * `key` - The string key to hash
    ///
    /// # Returns
    /// A 64-bit signed integer hash value
    fn hash(&self, key: &str) -> i64;
}
