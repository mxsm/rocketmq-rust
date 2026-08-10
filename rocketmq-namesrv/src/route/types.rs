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
//! - `CheetahString` instead of `String` for public route names
//! - `CheetahString` for immutable internal route table keys
//! - `Arc<T>` for immutable shared data
//! - Strong typing for better API safety

use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::route::route_data_view::BrokerData;
use rocketmq_protocol::protocol::route::route_data_view::QueueData;
use rocketmq_protocol::protocol::static_topic::topic_queue_mapping_info::TopicQueueMappingInfo;

use crate::route::tables::BrokerLiveInfo;
pub use crate::route_info::broker_addr_info::BrokerAddrInfo;

/// Public topic name type.
pub type TopicName = CheetahString;

/// Broker name type
pub type BrokerName = CheetahString;

/// Cluster name type
pub type ClusterName = CheetahString;

/// Internal route topic key type.
pub(crate) type RouteTopicName = CheetahString;

/// Internal route broker key type.
pub(crate) type RouteBrokerName = CheetahString;

/// Internal route cluster key type.
pub(crate) type RouteClusterName = CheetahString;

#[inline]
pub(crate) fn route_topic_name(topic: TopicName) -> RouteTopicName {
    topic
}

#[inline]
pub(crate) fn route_broker_name(broker_name: BrokerName) -> RouteBrokerName {
    broker_name
}

#[inline]
pub(crate) fn route_cluster_name(cluster_name: ClusterName) -> RouteClusterName {
    cluster_name
}

#[inline]
pub(crate) fn public_name_from_route(route_name: &CheetahString) -> CheetahString {
    route_name.clone()
}

/// Broker address string
pub type BrokerAddr = CheetahString;

/// NameServer-local identity for one concrete broker registration instance.
///
/// The address is part of the key so delayed events for an old master address
/// cannot remove a newer master that reused the same broker ID.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BrokerInstanceKey {
    pub cluster_name: CheetahString,
    pub broker_name: CheetahString,
    pub broker_id: u64,
    pub broker_addr: CheetahString,
}

impl BrokerInstanceKey {
    #[must_use]
    pub fn new(
        cluster_name: impl Into<CheetahString>,
        broker_name: impl Into<CheetahString>,
        broker_id: u64,
        broker_addr: impl Into<CheetahString>,
    ) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            broker_name: broker_name.into(),
            broker_id,
            broker_addr: broker_addr.into(),
        }
    }
}

/// Local fencing generation attached to liveness and cleanup events.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct BrokerGeneration {
    pub registration_epoch: u64,
    pub heartbeat_generation: u64,
}

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
///
/// Runtime configuration is owned by [`crate::NamesrvConfig`]. In particular,
/// `NamesrvConfig::unregister_broker_batch_size` is wired to the production
/// batch-unregistration service.
#[derive(Clone, Debug)]
#[deprecated(
    since = "1.0.0",
    note = "use NamesrvConfig; this compatibility DTO is not consumed by the production runtime"
)]
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

#[allow(deprecated, reason = "implements the retained compatibility DTO")]
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

    #[must_use]
    pub fn instance_key(&self) -> BrokerInstanceKey {
        BrokerInstanceKey::new(
            self.cluster_name.clone(),
            self.broker_name.clone(),
            self.broker_id,
            self.broker_addr.clone(),
        )
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
    #[allow(
        deprecated,
        reason = "covers the retained compatibility DTO until its next-major removal"
    )]
    fn test_route_manager_config_default() {
        let config = RouteManagerConfig::default();

        assert_eq!(config.broker_channel_expired_time, 1000 * 60 * 2);
        assert!(config.delete_topic_with_broker_registration);
        assert!(config.enable_batch_unregistration);
        assert_eq!(config.max_batch_unregister_size, 100);
        assert_eq!(config.scan_not_active_broker_interval, 5000);
    }

    #[test]
    fn test_broker_registration_builder() {
        let reg = BrokerRegistration::new("cluster1", "192.168.1.1:10911", "broker-a", 0)
            .with_ha_server("192.168.1.1:10912")
            .with_zone("zone1")
            .with_timeout(5000)
            .with_acting_master(true);

        assert_eq!(reg.cluster_name.as_str(), "cluster1");
        assert_eq!(reg.broker_addr.as_str(), "192.168.1.1:10911");
        assert_eq!(reg.broker_name.as_str(), "broker-a");
        assert_eq!(reg.broker_id, 0);
        assert_eq!(reg.ha_server_addr.as_str(), "192.168.1.1:10912");
        assert_eq!(reg.zone_name.as_ref().map(|s| s.as_str()), Some("zone1"));
        assert_eq!(reg.timeout_millis, Some(5000));
        assert_eq!(reg.enable_acting_master, Some(true));
    }

    #[test]
    fn test_broker_unregistration_new() {
        let unreg = BrokerUnregistration::new("cluster1", "127.0.0.1:10911", "broker-a", 1);

        assert_eq!(unreg.cluster_name.as_str(), "cluster1");
        assert_eq!(unreg.broker_addr.as_str(), "127.0.0.1:10911");
        assert_eq!(unreg.broker_name.as_str(), "broker-a");
        assert_eq!(unreg.broker_id, 1);
    }

    #[test]
    fn test_topic_route_query_builder() {
        let query = TopicRouteQuery::new("TestTopic").include_inactive();

        assert_eq!(query.topic.as_str(), "TestTopic");
        assert!(query.include_inactive_brokers);
    }

    #[test]
    fn broker_instance_identity_includes_address_and_generation_is_explicit() {
        let old = BrokerInstanceKey::new("cluster", "broker", 0, "127.0.0.1:10911");
        let new = BrokerInstanceKey::new("cluster", "broker", 0, "127.0.0.2:10911");
        assert_ne!(old, new);
        assert_eq!(
            BrokerGeneration {
                registration_epoch: 7,
                heartbeat_generation: 11,
            },
            BrokerGeneration {
                registration_epoch: 7,
                heartbeat_generation: 11,
            }
        );
    }
}
