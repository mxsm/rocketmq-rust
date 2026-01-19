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

//! Consistent hash router implementation
//!
//! To hash Node objects to a hash ring with a certain amount of virtual nodes.
//! Method `route_node` will return a Node instance which the object key should be
//! allocated to according to consistent hash algorithm.

use std::collections::BTreeMap;
use std::sync::Arc;

use md5::Digest;

use super::hash_function::HashFunction;
use super::node::Node;
use super::virtual_node::VirtualNode;

/// Consistent hash router that maps keys to nodes on a hash ring
///
/// This implementation uses virtual nodes to improve key distribution
/// across physical nodes. Each physical node is represented by multiple
/// virtual nodes on the ring.
///
/// # Type Parameters
/// * `T` - The type of physical nodes, must implement `Node` and `Clone` traits
///
/// # Example
/// ```
/// use rocketmq_common::common::consistenthash::ConsistentHashRouter;
/// use rocketmq_common::common::consistenthash::Node;
///
/// #[derive(Clone)]
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
/// let nodes = vec![
///     ServerNode {
///         address: "192.168.1.1:8080".to_string(),
///     },
///     ServerNode {
///         address: "192.168.1.2:8080".to_string(),
///     },
/// ];
///
/// let router = ConsistentHashRouter::new(nodes, 10);
/// let target_node = router.route_node("my-key").unwrap();
/// println!("Key 'my-key' routes to: {}", target_node.get_key());
/// ```
#[derive(Clone)]
pub struct ConsistentHashRouter<T: Node + Clone> {
    /// The hash ring mapping hash values to virtual nodes
    ring: BTreeMap<i64, VirtualNode<T>>,
    /// Hash function used to hash keys
    hash_function: Arc<dyn HashFunction>,
}

impl<T: Node + Clone> ConsistentHashRouter<T> {
    /// Create a new consistent hash router with the default MD5 hash function
    ///
    /// # Arguments
    /// * `physical_nodes` - Collection of physical nodes to add to the ring
    /// * `virtual_node_count` - Number of virtual nodes per physical node
    ///
    /// # Returns
    /// A new ConsistentHashRouter instance
    ///
    /// # Example
    /// ```
    /// use rocketmq_common::common::consistenthash::ConsistentHashRouter;
    /// use rocketmq_common::common::consistenthash::Node;
    ///
    /// #[derive(Clone)]
    /// struct ServerNode {
    ///     name: String,
    /// }
    ///
    /// impl Node for ServerNode {
    ///     fn get_key(&self) -> &str {
    ///         &self.name
    ///     }
    /// }
    ///
    /// let nodes = vec![
    ///     ServerNode {
    ///         name: "node1".to_string(),
    ///     },
    ///     ServerNode {
    ///         name: "node2".to_string(),
    ///     },
    /// ];
    ///
    /// let router = ConsistentHashRouter::new(nodes, 150);
    /// ```
    pub fn new(physical_nodes: Vec<T>, virtual_node_count: i32) -> Self {
        Self::new_with_hash_function(physical_nodes, virtual_node_count, Arc::new(MD5Hash::new()))
    }

    /// Create a new consistent hash router with a custom hash function
    ///
    /// # Arguments
    /// * `physical_nodes` - Collection of physical nodes to add to the ring
    /// * `virtual_node_count` - Number of virtual nodes per physical node
    /// * `hash_function` - Custom hash function to use
    ///
    /// # Returns
    /// A new ConsistentHashRouter instance
    ///
    /// # Panics
    /// Panics if `virtual_node_count` is negative
    pub fn new_with_hash_function(
        physical_nodes: Vec<T>,
        virtual_node_count: i32,
        hash_function: Arc<dyn HashFunction>,
    ) -> Self {
        let mut router = Self {
            ring: BTreeMap::new(),
            hash_function,
        };

        for node in physical_nodes {
            router.add_node(node, virtual_node_count);
        }

        router
    }

    /// Add a physical node to the hash ring with virtual nodes
    ///
    /// # Arguments
    /// * `physical_node` - Physical node to add to the hash ring
    /// * `virtual_node_count` - The number of virtual nodes for this physical node. Value should be
    ///   greater than or equal to 0
    ///
    /// # Panics
    /// Panics if `virtual_node_count` is negative
    ///
    /// # Example
    /// ```
    /// use rocketmq_common::common::consistenthash::ConsistentHashRouter;
    /// use rocketmq_common::common::consistenthash::Node;
    ///
    /// #[derive(Clone)]
    /// struct ServerNode {
    ///     name: String,
    /// }
    ///
    /// impl Node for ServerNode {
    ///     fn get_key(&self) -> &str {
    ///         &self.name
    ///     }
    /// }
    ///
    /// let mut router = ConsistentHashRouter::new(vec![], 0);
    /// let node = ServerNode {
    ///     name: "node1".to_string(),
    /// };
    /// router.add_node(node, 100);
    /// ```
    pub fn add_node(&mut self, physical_node: T, virtual_node_count: i32) {
        if virtual_node_count < 0 {
            panic!("illegal virtual node counts: {}", virtual_node_count);
        }

        let existing_replicas = self.get_existing_replicas(&physical_node);
        for i in 0..virtual_node_count {
            let virtual_node = VirtualNode::new(physical_node.clone(), i + existing_replicas);
            let hash = self.hash_function.hash(virtual_node.get_key());
            self.ring.insert(hash, virtual_node);
        }
    }

    /// Remove a physical node from the hash ring
    ///
    /// This removes all virtual nodes associated with the given physical node.
    ///
    /// # Arguments
    /// * `physical_node` - The physical node to remove
    ///
    /// # Example
    /// ```
    /// use rocketmq_common::common::consistenthash::ConsistentHashRouter;
    /// use rocketmq_common::common::consistenthash::Node;
    ///
    /// #[derive(Clone)]
    /// struct ServerNode {
    ///     name: String,
    /// }
    ///
    /// impl Node for ServerNode {
    ///     fn get_key(&self) -> &str {
    ///         &self.name
    ///     }
    /// }
    ///
    /// let node = ServerNode {
    ///     name: "node1".to_string(),
    /// };
    /// let mut router = ConsistentHashRouter::new(vec![node.clone()], 10);
    /// router.remove_node(&node);
    /// assert!(router.route_node("any-key").is_none());
    /// ```
    pub fn remove_node(&mut self, physical_node: &T) {
        self.ring
            .retain(|_, virtual_node| !virtual_node.is_virtual_node_of(physical_node));
    }

    /// Route a key to a node on the hash ring
    ///
    /// This method finds the nearest node in clockwise direction on the hash ring
    /// for the given key using consistent hashing algorithm.
    ///
    /// # Arguments
    /// * `object_key` - The object key to find a nearest node for
    ///
    /// # Returns
    /// * `Some(T)` - The physical node that the key should route to
    /// * `None` - If the ring is empty
    ///
    /// # Example
    /// ```
    /// use rocketmq_common::common::consistenthash::ConsistentHashRouter;
    /// use rocketmq_common::common::consistenthash::Node;
    ///
    /// #[derive(Clone)]
    /// struct ServerNode {
    ///     name: String,
    /// }
    ///
    /// impl Node for ServerNode {
    ///     fn get_key(&self) -> &str {
    ///         &self.name
    ///     }
    /// }
    ///
    /// let nodes = vec![
    ///     ServerNode {
    ///         name: "node1".to_string(),
    ///     },
    ///     ServerNode {
    ///         name: "node2".to_string(),
    ///     },
    /// ];
    ///
    /// let router = ConsistentHashRouter::new(nodes, 10);
    /// let node = router.route_node("my-key");
    /// assert!(node.is_some());
    /// ```
    pub fn route_node(&self, object_key: &str) -> Option<T> {
        if self.ring.is_empty() {
            return None;
        }

        let hash_val = self.hash_function.hash(object_key);

        // Find the first node with hash >= hash_val (tailMap in Java)
        let virtual_node = self
            .ring
            .range(hash_val..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, vnode)| vnode)?;

        Some(virtual_node.physical_node().clone())
    }

    /// Get the number of existing virtual nodes for a physical node
    ///
    /// # Arguments
    /// * `physical_node` - The physical node to count replicas for
    ///
    /// # Returns
    /// The count of virtual nodes for the given physical node
    pub fn get_existing_replicas(&self, physical_node: &T) -> i32 {
        self.ring
            .values()
            .filter(|vnode| vnode.is_virtual_node_of(physical_node))
            .count() as i32
    }

    /// Get the total number of virtual nodes in the ring
    ///
    /// # Returns
    /// The total count of virtual nodes
    pub fn size(&self) -> usize {
        self.ring.len()
    }

    /// Check if the ring is empty
    ///
    /// # Returns
    /// `true` if there are no nodes in the ring
    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }
}

/// Default MD5 hash function implementation
///
/// This is the default hash function used by ConsistentHashRouter.
/// It computes MD5 digest and uses the first 4 bytes as a long hash value.
#[derive(Debug, Clone)]
pub struct MD5Hash;

impl MD5Hash {
    /// Create a new MD5Hash instance
    pub fn new() -> Self {
        Self
    }
}

impl Default for MD5Hash {
    fn default() -> Self {
        Self::new()
    }
}

impl HashFunction for MD5Hash {
    /// Hash a key using MD5 algorithm
    ///
    /// Takes the first 4 bytes of the MD5 digest and combines them
    /// into a 32-bit signed integer value.
    ///
    /// # Arguments
    /// * `key` - The key to hash
    ///
    /// # Returns
    /// A 64-bit hash value derived from the MD5 digest
    fn hash(&self, key: &str) -> i64 {
        let mut hasher = md5::Md5::new();
        hasher.update(key.as_bytes());
        let result = hasher.finalize();

        let mut h: i64 = 0;
        for i in 0..4 {
            h <<= 8;
            h |= (result[i] as i64) & 0xFF;
        }
        h
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    struct TestNode {
        key: String,
    }

    impl TestNode {
        fn new(key: &str) -> Self {
            Self { key: key.to_string() }
        }
    }

    impl Node for TestNode {
        fn get_key(&self) -> &str {
            &self.key
        }
    }

    #[test]
    fn test_new_router() {
        let nodes = vec![TestNode::new("node1"), TestNode::new("node2")];
        let router = ConsistentHashRouter::new(nodes, 10);
        assert_eq!(router.size(), 20); // 2 nodes * 10 virtual nodes each
    }

    #[test]
    fn test_add_node() {
        let mut router = ConsistentHashRouter::new(vec![], 0);
        assert!(router.is_empty());

        router.add_node(TestNode::new("node1"), 5);
        assert_eq!(router.size(), 5);

        router.add_node(TestNode::new("node2"), 5);
        assert_eq!(router.size(), 10);
    }

    #[test]
    fn test_add_node_incremental() {
        let mut router = ConsistentHashRouter::new(vec![], 0);
        let node = TestNode::new("node1");

        router.add_node(node.clone(), 5);
        assert_eq!(router.get_existing_replicas(&node), 5);

        // Adding more virtual nodes should append, not replace
        router.add_node(node.clone(), 3);
        assert_eq!(router.get_existing_replicas(&node), 8);
    }

    #[test]
    #[should_panic(expected = "illegal virtual node counts")]
    fn test_add_node_negative_count() {
        let mut router = ConsistentHashRouter::new(vec![], 0);
        router.add_node(TestNode::new("node1"), -1);
    }

    #[test]
    fn test_remove_node() {
        let node1 = TestNode::new("node1");
        let node2 = TestNode::new("node2");
        let mut router = ConsistentHashRouter::new(vec![node1.clone(), node2.clone()], 10);
        assert_eq!(router.size(), 20);

        router.remove_node(&node1);
        assert_eq!(router.size(), 10);
        assert_eq!(router.get_existing_replicas(&node1), 0);
        assert_eq!(router.get_existing_replicas(&node2), 10);
    }

    #[test]
    fn test_route_node_empty() {
        let router: ConsistentHashRouter<TestNode> = ConsistentHashRouter::new(vec![], 0);
        assert!(router.route_node("any-key").is_none());
    }

    #[test]
    fn test_route_node_single() {
        let node = TestNode::new("node1");
        let router = ConsistentHashRouter::new(vec![node.clone()], 10);
        let routed = router.route_node("test-key").unwrap();
        assert_eq!(routed.get_key(), "node1");
    }

    #[test]
    fn test_route_node_consistency() {
        let nodes = vec![TestNode::new("node1"), TestNode::new("node2"), TestNode::new("node3")];
        let router = ConsistentHashRouter::new(nodes, 10);

        // Same key should always route to the same node
        let key = "consistent-key";
        let node1 = router.route_node(key).unwrap();
        let node2 = router.route_node(key).unwrap();
        assert_eq!(node1.get_key(), node2.get_key());
    }

    #[test]
    fn test_route_node_distribution() {
        let nodes = vec![TestNode::new("node1"), TestNode::new("node2"), TestNode::new("node3")];
        let router = ConsistentHashRouter::new(nodes, 100);

        // Test that different keys can route to different nodes
        let mut routed_nodes = std::collections::HashSet::new();
        for i in 0..100 {
            let key = format!("key-{}", i);
            if let Some(node) = router.route_node(&key) {
                routed_nodes.insert(node.get_key().to_string());
            }
        }

        // With 100 keys and 3 nodes with 100 virtual nodes each,
        // we should see distribution across nodes
        assert!(routed_nodes.len() > 1);
    }

    #[test]
    fn test_get_existing_replicas() {
        let node1 = TestNode::new("node1");
        let node2 = TestNode::new("node2");
        let router = ConsistentHashRouter::new(vec![node1.clone(), node2.clone()], 15);

        assert_eq!(router.get_existing_replicas(&node1), 15);
        assert_eq!(router.get_existing_replicas(&node2), 15);

        let node3 = TestNode::new("node3");
        assert_eq!(router.get_existing_replicas(&node3), 0);
    }

    #[test]
    fn test_md5_hash() {
        let hash_fn = MD5Hash::new();
        let hash1 = hash_fn.hash("test-key");
        let hash2 = hash_fn.hash("test-key");
        assert_eq!(hash1, hash2); // Same key should produce same hash

        let hash3 = hash_fn.hash("different-key");
        assert_ne!(hash1, hash3); // Different keys should (likely) produce different hashes
    }

    #[test]
    fn test_md5_hash_deterministic() {
        let hash_fn = MD5Hash::new();
        // Test that hash is deterministic
        for _ in 0..10 {
            let hash = hash_fn.hash("deterministic-test");
            assert_eq!(hash, hash_fn.hash("deterministic-test"));
        }
    }
}
