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

//! Semantic type aliases for RocketMQ client
//!
//! This module provides type aliases that give semantic meaning to CheetahString
//! usage throughout the rocketmq-client crate. These types improve code readability
//! and documentation by explicitly distinguishing different string identifiers.
//!
//! These types are only visible within the `rocketmq-client` crate.
//!
//! ## Available Types
//!
//! - `ProducerGroupName` - Producer group name identifier
//! - `ConsumerGroupName` - Consumer group name identifier
//! - `AdminGroupName` - Admin extension group name identifier
//! - `TopicName` - Topic name identifier
//! - `BrokerName` - Broker name identifier
//! - `BrokerAddr` - Broker address (IP:Port format)
//!
//! ## Usage
//!
//! ```ignore
//! use crate::types::{TopicName, BrokerName, ProducerGroupName};
//!
//! fn process_topic(topic: TopicName) {
//!     // ...
//! }
//!
//! let topic: TopicName = "my_topic".into();
//! let broker: BrokerName = "broker-a".into();
//! ```

use cheetah_string::CheetahString;

/// Producer group name identifier.
pub(crate) type ProducerGroupName = CheetahString;

/// Consumer group name identifier.
pub(crate) type ConsumerGroupName = CheetahString;

/// Admin extension group name identifier.
pub(crate) type AdminGroupName = CheetahString;

/// Topic name identifier.
pub(crate) type TopicName = CheetahString;

/// Broker name identifier.
pub(crate) type BrokerName = CheetahString;

/// Broker address (IP:Port format).
pub(crate) type BrokerAddr = CheetahString;
