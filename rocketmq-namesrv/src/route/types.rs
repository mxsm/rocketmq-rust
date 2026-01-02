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

//! Type definitions for route management system
//!
//! This module contains optimized type definitions using:
//! - `CheetahString` instead of `String` for zero-copy sharing and consistency
//! - `Arc<T>` for immutable shared data
//! - Strong typing for better API safety

use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_remoting::protocol::route::route_data_view::BrokerData;
use rocketmq_remoting::protocol::route::route_data_view::QueueData;
use rocketmq_remoting::protocol::static_topic::topic_queue_info::TopicQueueMappingInfo;

use crate::route::tables::BrokerLiveInfo;
use crate::route_info::broker_addr_info::BrokerAddrInfo;

/// Topic name type - uses CheetahString for zero-copy sharing
pub type TopicName = CheetahString;

/// Broker name type
pub type BrokerName = CheetahString;

/// Cluster name type
pub type ClusterName = CheetahString;

/// Broker address string
pub type BrokerAddr = CheetahString;

/// Shared broker data
pub type SharedBrokerData = Arc<BrokerData>;

/// Shared queue data
pub type SharedQueueData = Arc<QueueData>;

/// Shared broker live info
pub type SharedBrokerLiveInfo = Arc<BrokerLiveInfo>;

/// Shared topic queue mapping info
pub type SharedTopicQueueMappingInfo = Arc<TopicQueueMappingInfo>;

/// Broker address information
pub type SharedBrokerAddrInfo = Arc<BrokerAddrInfo>;

/// Configuration for RouteInfoManager
#[derive(Clone, Debug)]
pub struct RouteManagerConfig {
    /// Broker channel expired time in milliseconds
    pub broker_channel_expired_time: i64,

    /// Enable automatic topic cleanup when broker unregisters
    pub delete_topic_with_broker_registration: bool,

    /// Enable batch unregistration
    pub enable_batch_unregistration: bool,

    /// Maximum batch size for unregistration
    pub max_batch_unregister_size: usize,

    /// Scan interval for inactive brokers (milliseconds)
    pub scan_not_active_broker_interval: u64,
}

impl Default for RouteManagerConfig {
    fn default() -> Self {
        Self {
            broker_channel_expired_time: 1000 * 60 * 2, // 2 minutes
            delete_topic_with_broker_registration: true,
            enable_batch_unregistration: true,
            max_batch_unregister_size: 100,
            scan_not_active_broker_interval: 5000, // 5 seconds
        }
    }
}

/// Broker registration information
#[derive(Clone, Debug)]
pub struct BrokerRegistration {
    pub cluster_name: CheetahString,
    pub broker_addr: CheetahString,
    pub broker_name: CheetahString,
    pub broker_id: u64,
    pub ha_server_addr: CheetahString,
    pub zone_name: Option<CheetahString>,
    pub timeout_millis: Option<i64>,
    pub enable_acting_master: Option<bool>,
}

impl BrokerRegistration {
    /// Create a new broker registration
    pub fn new(
        cluster_name: impl Into<CheetahString>,
        broker_addr: impl Into<CheetahString>,
        broker_name: impl Into<CheetahString>,
        broker_id: u64,
    ) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            broker_addr: broker_addr.into(),
            broker_name: broker_name.into(),
            broker_id,
            ha_server_addr: CheetahString::empty(),
            zone_name: None,
            timeout_millis: None,
            enable_acting_master: None,
        }
    }

    /// Set HA server address
    pub fn with_ha_server(mut self, ha_server_addr: impl Into<CheetahString>) -> Self {
        self.ha_server_addr = ha_server_addr.into();
        self
    }

    /// Set zone name
    pub fn with_zone(mut self, zone_name: impl Into<CheetahString>) -> Self {
        self.zone_name = Some(zone_name.into());
        self
    }
    /// Set timeout
    pub fn with_timeout(mut self, timeout_millis: i64) -> Self {
        self.timeout_millis = Some(timeout_millis);
        self
    }

    /// Enable acting master
    pub fn with_acting_master(mut self, enable: bool) -> Self {
        self.enable_acting_master = Some(enable);
        self
    }
}

/// Broker unregistration information
#[derive(Clone, Debug)]
pub struct BrokerUnregistration {
    pub cluster_name: CheetahString,
    pub broker_addr: CheetahString,
    pub broker_name: CheetahString,
    pub broker_id: u64,
}

impl BrokerUnregistration {
    /// Create a new broker unregistration
    pub fn new(
        cluster_name: impl Into<CheetahString>,
        broker_addr: impl Into<CheetahString>,
        broker_name: impl Into<CheetahString>,
        broker_id: u64,
    ) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            broker_addr: broker_addr.into(),
            broker_name: broker_name.into(),
            broker_id,
        }
    }
}

/// Topic routing query parameters
#[derive(Clone, Debug)]
pub struct TopicRouteQuery {
    pub topic: CheetahString,
    pub include_inactive_brokers: bool,
}

impl TopicRouteQuery {
    /// Create a new topic route query
    pub fn new(topic: impl Into<CheetahString>) -> Self {
        Self {
            topic: topic.into(),
            include_inactive_brokers: false,
        }
    }

    /// Include inactive brokers in the result
    pub fn include_inactive(mut self) -> Self {
        self.include_inactive_brokers = true;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broker_registration_builder() {
        let reg = BrokerRegistration::new("cluster1", "192.168.1.1:10911", "broker-a", 0)
            .with_ha_server("192.168.1.1:10912")
            .with_zone("zone1")
            .with_timeout(5000)
            .with_acting_master(true);

        assert_eq!(reg.cluster_name.as_str(), "cluster1");
        assert_eq!(reg.broker_id, 0);
        assert_eq!(reg.zone_name.as_ref().map(|s| s.as_str()), Some("zone1"));
        assert_eq!(reg.timeout_millis, Some(5000));
        assert_eq!(reg.enable_acting_master, Some(true));
    }

    #[test]
    fn test_topic_route_query() {
        let query = TopicRouteQuery::new("TestTopic").include_inactive();

        assert_eq!(query.topic.as_str(), "TestTopic");
        assert!(query.include_inactive_brokers);
    }
}
