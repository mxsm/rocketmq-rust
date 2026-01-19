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

//! Virtual node implementation for consistent hashing
//!
//! Virtual nodes are used to improve the distribution of keys across
//! physical nodes in a consistent hash ring.

use std::fmt;

use super::Node;

/// Virtual node wrapper for consistent hashing
///
/// A virtual node represents one of multiple replicas of a physical node
/// on the hash ring. By creating multiple virtual nodes for each physical
/// node, the key distribution becomes more uniform.
///
/// # Type Parameters
/// * `T` - The type of the physical node, must implement `Node` trait
///
/// # Example
/// ```
/// use rocketmq_common::common::consistenthash::Node;
/// use rocketmq_common::common::consistenthash::VirtualNode;
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
/// let physical = ServerNode {
///     address: "192.168.1.1:8080".to_string(),
/// };
///
/// let virtual_node = VirtualNode::new(physical, 0);
/// assert_eq!(virtual_node.get_key(), "192.168.1.1:8080-0");
/// assert_eq!(virtual_node.replica_index(), 0);
/// ```
#[derive(Debug, Clone)]
pub struct VirtualNode<T: Node> {
    /// The physical node this virtual node represents
    physical_node: T,
    /// The replica index of this virtual node
    replica_index: i32,
    /// Cached key combining physical node key and replica index
    key: String,
}

impl<T: Node> VirtualNode<T> {
    /// Create a new virtual node
    ///
    /// # Arguments
    /// * `physical_node` - The physical node to create a virtual node for
    /// * `replica_index` - The replica index (typically 0, 1, 2, ...)
    ///
    /// # Returns
    /// A new VirtualNode instance
    pub fn new(physical_node: T, replica_index: i32) -> Self {
        let key = format!("{}-{}", physical_node.get_key(), replica_index);
        Self {
            physical_node,
            replica_index,
            key,
        }
    }

    /// Check if this virtual node belongs to the given physical node
    ///
    /// # Arguments
    /// * `physical_node` - The physical node to check against
    ///
    /// # Returns
    /// `true` if this virtual node represents the given physical node
    pub fn is_virtual_node_of(&self, physical_node: &T) -> bool {
        self.physical_node.get_key() == physical_node.get_key()
    }

    /// Get a reference to the physical node
    ///
    /// # Returns
    /// A reference to the underlying physical node
    pub fn physical_node(&self) -> &T {
        &self.physical_node
    }

    /// Get the replica index
    ///
    /// # Returns
    /// The replica index of this virtual node
    pub fn replica_index(&self) -> i32 {
        self.replica_index
    }
}

impl<T: Node> Node for VirtualNode<T> {
    /// Returns the virtual node's key, which is the physical node's key
    /// combined with the replica index
    ///
    /// # Returns
    /// A string combining the physical node key and replica index (e.g., "node1-0")
    fn get_key(&self) -> &str {
        &self.key
    }
}

impl<T: Node + fmt::Display> fmt::Display for VirtualNode<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "VirtualNode{{physicalNode={}, replicaIndex={}}}",
            self.physical_node, self.replica_index
        )
    }
}
