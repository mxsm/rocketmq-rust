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

//! Semantic type aliases for RocketMQ broker
//!
//! This module provides type aliases that give semantic meaning to CheetahString
//! usage throughout the rocketmq-broker crate. These types improve code readability
//! and documentation by explicitly distinguishing different string identifiers.
//!
//! These types are only visible within the `rocketmq-broker` crate.
//!
//! ## Available Types
//!
//! - `ClusterName` - Cluster name identifier
//! - `BrokerName` - Broker name identifier
//! - `BrokerAddr` - Broker address (IP:Port format)
//! - `TopicName` - Topic name identifier
//! - `ConsumerGroupName` - Consumer group name identifier
//! - `ProducerGroupName` - Producer group name identifier
//! - `Namespace` - Namespace identifier
//! - `ClientId` - Client identifier
//! - `RegionId` - Region identifier
//! - `MessageId` - Message identifier
//! - `TransactionId` - Transaction identifier
//! - `InstanceName` - Instance name identifier
//!
//! ## Usage
//!
//! ```ignore
//! use crate::types::{TopicName, BrokerName, ClusterName};
//!
//! fn process_topic(topic: TopicName, broker: BrokerName) {
//!     // ...
//! }
//!
//! let topic: TopicName = "my_topic".into();
//! let broker: BrokerName = "broker-a".into();
//! let cluster: ClusterName = "DefaultCluster".into();
//! ```

use cheetah_string::CheetahString;

/// Cluster name identifier.
pub(crate) type ClusterName = CheetahString;

/// Broker name identifier.
pub(crate) type BrokerName = CheetahString;

/// Broker address (IP:Port format).
pub(crate) type BrokerAddr = CheetahString;

/// Topic name identifier.
pub(crate) type TopicName = CheetahString;

/// Consumer group name identifier.
pub(crate) type ConsumerGroupName = CheetahString;

/// Producer group name identifier.
pub(crate) type ProducerGroupName = CheetahString;

/// Namespace identifier.
pub(crate) type Namespace = CheetahString;

/// Client identifier.
pub(crate) type ClientId = CheetahString;

/// Region identifier.
pub(crate) type RegionId = CheetahString;

/// Message identifier.
pub(crate) type MessageId = CheetahString;

/// Transaction identifier.
pub(crate) type TransactionId = CheetahString;

/// Instance name identifier.
pub(crate) type InstanceName = CheetahString;
