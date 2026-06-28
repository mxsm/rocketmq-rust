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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use dashmap::DashMap;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use serde::Serialize;

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

/// Monotonic route snapshot versions, indexed by topic name.
pub type TopicRouteVersionTable = Arc<DashMap<TopicName, u64>>;

/// Broker address route reference counts, used for O(1) heartbeat failure checks.
pub type BrokerAddrRouteIndex = Arc<DashMap<BrokerAddr, usize>>;

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

/// Shared route-refresh state used to shard periodic refreshes and expose counters.
#[derive(Default)]
pub struct TopicRouteRefreshState {
    pub periodic_cursor: AtomicUsize,
    pub versions: TopicRouteVersionTable,
    pub broker_addr_route_index: BrokerAddrRouteIndex,
    pub metrics: TopicRouteRefreshMetrics,
}

/// Atomic route refresh counters. The snapshot type below is the stable observable shape.
#[derive(Default)]
pub struct TopicRouteRefreshMetrics {
    refresh_attempts_total: AtomicU64,
    refresh_success_total: AtomicU64,
    refresh_skipped_total: AtomicU64,
    refresh_failed_total: AtomicU64,
    periodic_batches_total: AtomicU64,
    periodic_topics_total: AtomicU64,
    periodic_skipped_topics_total: AtomicU64,
    last_periodic_total_topics: AtomicU64,
    last_periodic_batch_topics: AtomicU64,
    last_periodic_skipped_topics: AtomicU64,
    last_periodic_elapsed_us: AtomicU64,
    last_route_miss_elapsed_us: AtomicU64,
}

impl TopicRouteRefreshMetrics {
    #[inline]
    pub fn record_attempt(&self) {
        self.refresh_attempts_total.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_success(&self) {
        self.refresh_success_total.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_skip(&self) {
        self.refresh_skipped_total.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_failure(&self) {
        self.refresh_failed_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_periodic_batch(
        &self,
        total_topics: u64,
        refreshed_topics: u64,
        skipped_topics: u64,
        elapsed_us: u64,
    ) {
        self.periodic_batches_total.fetch_add(1, Ordering::Relaxed);
        self.periodic_topics_total
            .fetch_add(refreshed_topics, Ordering::Relaxed);
        self.periodic_skipped_topics_total
            .fetch_add(skipped_topics, Ordering::Relaxed);
        self.last_periodic_total_topics.store(total_topics, Ordering::Relaxed);
        self.last_periodic_batch_topics
            .store(refreshed_topics, Ordering::Relaxed);
        self.last_periodic_skipped_topics
            .store(skipped_topics, Ordering::Relaxed);
        self.last_periodic_elapsed_us.store(elapsed_us, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_route_miss_elapsed(&self, elapsed_us: u64) {
        self.last_route_miss_elapsed_us.store(elapsed_us, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> TopicRouteRefreshMetricsSnapshot {
        TopicRouteRefreshMetricsSnapshot {
            refresh_attempts_total: self.refresh_attempts_total.load(Ordering::Relaxed),
            refresh_success_total: self.refresh_success_total.load(Ordering::Relaxed),
            refresh_skipped_total: self.refresh_skipped_total.load(Ordering::Relaxed),
            refresh_failed_total: self.refresh_failed_total.load(Ordering::Relaxed),
            periodic_batches_total: self.periodic_batches_total.load(Ordering::Relaxed),
            periodic_topics_total: self.periodic_topics_total.load(Ordering::Relaxed),
            periodic_skipped_topics_total: self.periodic_skipped_topics_total.load(Ordering::Relaxed),
            last_periodic_total_topics: self.last_periodic_total_topics.load(Ordering::Relaxed),
            last_periodic_batch_topics: self.last_periodic_batch_topics.load(Ordering::Relaxed),
            last_periodic_skipped_topics: self.last_periodic_skipped_topics.load(Ordering::Relaxed),
            last_periodic_elapsed_us: self.last_periodic_elapsed_us.load(Ordering::Relaxed),
            last_route_miss_elapsed_us: self.last_route_miss_elapsed_us.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct TopicRouteRefreshMetricsSnapshot {
    pub refresh_attempts_total: u64,
    pub refresh_success_total: u64,
    pub refresh_skipped_total: u64,
    pub refresh_failed_total: u64,
    pub periodic_batches_total: u64,
    pub periodic_topics_total: u64,
    pub periodic_skipped_topics_total: u64,
    pub last_periodic_total_topics: u64,
    pub last_periodic_batch_topics: u64,
    pub last_periodic_skipped_topics: u64,
    pub last_periodic_elapsed_us: u64,
    pub last_route_miss_elapsed_us: u64,
}
