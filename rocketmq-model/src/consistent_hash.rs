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

use std::collections::BTreeMap;
use std::sync::Arc;

use md5::Digest;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

/// A physical or virtual node that can be placed on a hash ring.
pub trait Node {
    fn get_key(&self) -> &str;
}

/// Converts a stable string key into a ring position.
pub trait HashFunction {
    fn hash(&self, key: &str) -> i64;
}

/// A virtual replica of a physical node.
#[derive(Debug, Clone)]
pub struct VirtualNode<T: Node> {
    physical_node: T,
    replica_index: i32,
    key: String,
}

impl<T: Node> VirtualNode<T> {
    pub fn new(physical_node: T, replica_index: i32) -> Self {
        let key = format!("{}-{replica_index}", physical_node.get_key());
        Self {
            physical_node,
            replica_index,
            key,
        }
    }

    pub fn is_virtual_node_of(&self, physical_node: &T) -> bool {
        self.physical_node.get_key() == physical_node.get_key()
    }

    pub fn physical_node(&self) -> &T {
        &self.physical_node
    }

    pub fn replica_index(&self) -> i32 {
        self.replica_index
    }
}

impl<T: Node> Node for VirtualNode<T> {
    fn get_key(&self) -> &str {
        &self.key
    }
}

/// MD5-compatible hash used by the Java allocation algorithm.
#[derive(Debug, Clone, Default)]
pub struct MD5Hash;

impl MD5Hash {
    pub fn new() -> Self {
        Self
    }
}

impl HashFunction for MD5Hash {
    fn hash(&self, key: &str) -> i64 {
        let mut hasher = md5::Md5::new();
        hasher.update(key.as_bytes());
        let digest = hasher.finalize();
        digest[..4]
            .iter()
            .fold(0_i64, |hash, byte| (hash << 8) | i64::from(*byte))
    }
}

/// Consistent hash ring with a fixed number of virtual nodes per physical node.
#[derive(Clone)]
pub struct ConsistentHashRouter<T: Node + Clone> {
    ring: BTreeMap<i64, VirtualNode<T>>,
    hash_function: Arc<dyn HashFunction>,
}

impl<T: Node + Clone> ConsistentHashRouter<T> {
    pub fn new(physical_nodes: Vec<T>, virtual_node_count: i32) -> Self {
        Self::new_with_hash_function(physical_nodes, virtual_node_count, Arc::new(MD5Hash))
    }

    pub fn try_new(physical_nodes: Vec<T>, virtual_node_count: i32) -> RocketMQResult<Self> {
        Self::try_new_with_hash_function(physical_nodes, virtual_node_count, Arc::new(MD5Hash))
    }

    pub fn new_with_hash_function(
        physical_nodes: Vec<T>,
        virtual_node_count: i32,
        hash_function: Arc<dyn HashFunction>,
    ) -> Self {
        if virtual_node_count < 0 {
            return Self {
                ring: BTreeMap::new(),
                hash_function,
            };
        }
        let mut router = Self {
            ring: BTreeMap::new(),
            hash_function,
        };
        for node in physical_nodes {
            router.add_node(node, virtual_node_count);
        }
        router
    }

    pub fn try_new_with_hash_function(
        physical_nodes: Vec<T>,
        virtual_node_count: i32,
        hash_function: Arc<dyn HashFunction>,
    ) -> RocketMQResult<Self> {
        if virtual_node_count < 0 {
            return Err(RocketMQError::illegal_argument(format!(
                "illegal virtual node counts: {virtual_node_count}"
            )));
        }
        let mut router = Self {
            ring: BTreeMap::new(),
            hash_function,
        };
        for node in physical_nodes {
            router.try_add_node(node, virtual_node_count)?;
        }
        Ok(router)
    }

    pub fn add_node(&mut self, physical_node: T, virtual_node_count: i32) {
        let _ = self.try_add_node(physical_node, virtual_node_count);
    }

    pub fn try_add_node(&mut self, physical_node: T, virtual_node_count: i32) -> RocketMQResult<()> {
        if virtual_node_count < 0 {
            return Err(RocketMQError::illegal_argument(format!(
                "illegal virtual node counts: {virtual_node_count}"
            )));
        }
        let existing = self.get_existing_replicas(&physical_node);
        for replica in 0..virtual_node_count {
            let virtual_node = VirtualNode::new(physical_node.clone(), existing + replica);
            self.ring
                .insert(self.hash_function.hash(virtual_node.get_key()), virtual_node);
        }
        Ok(())
    }

    pub fn remove_node(&mut self, physical_node: &T) {
        self.ring
            .retain(|_, virtual_node| !virtual_node.is_virtual_node_of(physical_node));
    }

    pub fn route_node_ref(&self, object_key: &str) -> Option<&T> {
        let hash = self.hash_function.hash(object_key);
        self.ring
            .range(hash..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, node)| node.physical_node())
    }

    pub fn route_node(&self, object_key: &str) -> Option<T> {
        self.route_node_ref(object_key).cloned()
    }

    pub fn get_existing_replicas(&self, physical_node: &T) -> i32 {
        self.ring
            .values()
            .filter(|node| node.is_virtual_node_of(physical_node))
            .count() as i32
    }

    pub fn size(&self) -> usize {
        self.ring.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }
}
