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

//! Node trait for consistent hashing
//!
//! Represents a node that should be mapped to a hash ring.

/// Trait representing a node in a consistent hash ring
///
/// Any type that implements this trait can be used as a node in consistent hashing.
/// The key returned by `get_key()` will be used for hash mapping on the ring.
///
/// # Example
/// ```
/// use rocketmq_common::common::consistenthash::Node;
///
/// struct ServerNode {
///     address: String,
/// }
///
/// impl Node for ServerNode {
///     fn get_key(&self) -> &str {
///         &self.address
///     }
/// }
///
/// let node = ServerNode {
///     address: "192.168.1.1:8080".to_string(),
/// };
/// assert_eq!(node.get_key(), "192.168.1.1:8080");
/// ```
pub trait Node {
    /// Returns the key which will be used for hash mapping
    ///
    /// This key uniquely identifies the node in the hash ring and is used
    /// to compute the node's position on the ring.
    ///
    /// # Returns
    /// A string slice representing the node's key
    fn get_key(&self) -> &str;
}
