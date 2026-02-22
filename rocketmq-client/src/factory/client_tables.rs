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

//! Type definitions for MQClientInstance internal tables
//!
//! This module defines type aliases for the complex concurrent data structures
//! used in MQClientInstance to manage producers, consumers, brokers, and topics.
//!
//! The semantic string type aliases (ProducerGroupName, ConsumerGroupName, TopicName, etc.)
//! are defined in the `crate::types` module.

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;

use crate::admin::mq_admin_ext_async_inner::MQAdminExtInnerImpl;
use crate::consumer::mq_consumer_inner::MQConsumerInnerImpl;
use crate::producer::producer_impl::mq_producer_inner::MQProducerInnerImpl;
use crate::types::AdminGroupName;
use crate::types::BrokerAddr;
use crate::types::BrokerName;
use crate::types::ConsumerGroupName;
use crate::types::ProducerGroupName;
use crate::types::TopicName;

// ===== Table Type Aliases =====

/// Container for registered producers, indexed by producer group name.
pub type ProducerTable = Arc<DashMap<ProducerGroupName, MQProducerInnerImpl>>;

/// Container for registered consumers, indexed by consumer group name.
pub type ConsumerTable = Arc<DashMap<ConsumerGroupName, MQConsumerInnerImpl>>;

/// Container for registered admin extensions, indexed by admin group name.
pub type AdminExtTable = Arc<DashMap<AdminGroupName, MQAdminExtInnerImpl>>;

/// Topic routing information table.
/// Maps topic name to its routing data (broker addresses, queue configuration, etc.).
pub type TopicRouteTable = Arc<DashMap<TopicName, TopicRouteData>>;

/// Topic endpoint mapping table for static topics.
/// Maps topic name to a mapping of message queues to their broker names.
pub type TopicEndPointsTable = Arc<DashMap<TopicName, HashMap<MessageQueue, BrokerName>>>;

/// Broker address table.
/// Maps broker name to a mapping of broker ID to broker address.
/// Broker ID 0 is master, others are slaves.
pub type BrokerAddrTable = Arc<DashMap<BrokerName, HashMap<u64, BrokerAddr>>>;

/// Broker version table.
/// Maps broker name to a mapping of broker address to version code.
pub type BrokerVersionTable = Arc<DashMap<BrokerName, HashMap<BrokerAddr, i32>>>;

/// Broker heartbeat fingerprint cache for HeartbeatV2 protocol.
/// Maps broker address to the last heartbeat fingerprint value.
/// Used to optimize heartbeat by sending minimal data when fingerprint unchanged.
pub type BrokerHeartbeatFingerprintTable = Arc<DashMap<BrokerAddr, i32>>;

/// Set of brokers that support HeartbeatV2 protocol.
/// Maps broker address to unit value (used as a set).
pub type BrokerSupportV2HeartbeatSet = Arc<DashMap<BrokerAddr, ()>>;
