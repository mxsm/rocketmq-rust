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

//! Consistent hashing module
//!
//! Provides traits and implementations for consistent hashing,
//! commonly used for distributed load balancing and data partitioning.

pub mod consistent_hash_router;
pub mod hash_function;
pub mod node;
pub mod virtual_node;

pub use consistent_hash_router::ConsistentHashRouter;
pub use consistent_hash_router::MD5Hash;
pub use hash_function::HashFunction;
pub use node::Node;
pub use virtual_node::VirtualNode;
