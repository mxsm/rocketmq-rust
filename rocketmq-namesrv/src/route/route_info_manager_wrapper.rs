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

//! RouteInfoManager wrapper to support both v1 and v2 implementations
//!
//! This module provides a unified enum wrapper that allows the nameserver to use
//! either the legacy v1 implementation (with RwLock) or the new v2 implementation
//! (with DashMap tables) based on configuration.
//!
//! The wrapper provides direct access to the underlying implementations through
//! `as_v1()` and `as_v2()` methods, along with common lifecycle methods.

use std::net::SocketAddr;

use cheetah_string::CheetahString;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;
use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_remoting::protocol::body::topic::topic_list::TopicList;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
use rocketmq_remoting::protocol::header::namesrv::broker_request::UnRegisterBrokerRequestHeader;
use rocketmq_remoting::protocol::namesrv::RegisterBrokerResult;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::protocol::DataVersion;

use crate::route::error::RouteResult;
use crate::route::route_info_manager::RouteInfoManager;
use crate::route::route_info_manager_v2::RouteInfoManagerV2;

/// Wrapper enum that supports both v1 and v2 RouteInfoManager implementations
///
/// This enum allows the nameserver to use either implementation transparently.
/// All public methods from both implementations are available through forwarding.
///
/// Both variants are boxed to avoid large enum variant size issues:
/// - V1: ~72 bytes → Box<V1> = 8 bytes
/// - V2: ~352 bytes → Box<V2> = 8 bytes
///
/// This reduces stack allocation from 352 bytes to 16 bytes (8 byte pointer + 8 byte discriminant)
pub enum RouteInfoManagerWrapper {
    /// Legacy implementation using RwLock-based tables
    V1(Box<RouteInfoManager>),
    /// New implementation using DashMap-based concurrent tables
    V2(Box<RouteInfoManagerV2>),
}

impl RouteInfoManagerWrapper {
    /// Get a mutable reference to the underlying RouteInfoManager V1 implementation
    ///
    /// # Panics
    /// Panics if the wrapper contains V2 instead of V1
    pub fn as_v1_mut(&mut self) -> &mut RouteInfoManager {
        match self {
            RouteInfoManagerWrapper::V1(manager) => manager,
            RouteInfoManagerWrapper::V2(_) => panic!("Expected V1, found V2"),
        }
    }

    /// Get a mutable reference to the underlying RouteInfoManager V2 implementation
    ///
    /// # Panics
    /// Panics if the wrapper contains V1 instead of V2
    pub fn as_v2_mut(&mut self) -> &mut RouteInfoManagerV2 {
        match self {
            RouteInfoManagerWrapper::V1(_) => panic!("Expected V2, found V1"),
            RouteInfoManagerWrapper::V2(manager) => manager,
        }
    }

    /// Get an immutable reference to the underlying RouteInfoManager V1 implementation
    ///
    /// # Panics
    /// Panics if the wrapper contains V2 instead of V1
    pub fn as_v1(&self) -> &RouteInfoManager {
        match self {
            RouteInfoManagerWrapper::V1(manager) => manager,
            RouteInfoManagerWrapper::V2(_) => panic!("Expected V1, found V2"),
        }
    }

    /// Get an immutable reference to the underlying RouteInfoManager V2 implementation
    ///
    /// # Panics
    /// Panics if the wrapper contains V1 instead of V2
    pub fn as_v2(&self) -> &RouteInfoManagerV2 {
        match self {
            RouteInfoManagerWrapper::V1(_) => panic!("Expected V2, found V1"),
            RouteInfoManagerWrapper::V2(manager) => manager,
        }
    }

    /// Handle connection disconnection - called when a broker disconnects
    ///
    /// This method now delegates to on_channel_destroy for both v1 and v2
    pub fn connection_disconnected(&mut self, socket_addr: SocketAddr) {
        match self {
            RouteInfoManagerWrapper::V1(manager) => manager.connection_disconnected(socket_addr),
            RouteInfoManagerWrapper::V2(manager) => {
                // V2's on_channel_destroy matches Java's onChannelDestroy behavior
                manager.on_channel_destroy(socket_addr);
            }
        }
    }

    /// Handle channel destroy event
    pub fn on_channel_destroy(&self, channel: &Channel) {
        match self {
            RouteInfoManagerWrapper::V1(manager) => manager.on_channel_destroy(channel),
            RouteInfoManagerWrapper::V2(manager) => {
                let socket_addr = channel.remote_address();
                manager.on_channel_destroy(socket_addr);
            }
        }
    }

    /// Start the route info manager
    ///
    /// This matches Java's start() method. V2 only starts the unRegisterService.
    /// V1 still uses the receiver-based approach for backward compatibility.
    pub fn start(&self) {
        match self {
            RouteInfoManagerWrapper::V1(manager) => manager.start(),
            RouteInfoManagerWrapper::V2(manager) => manager.start(),
        }
    }

    /// Shutdown the route info manager
    pub fn shutdown(&self) {
        match self {
            RouteInfoManagerWrapper::V1(manager) => manager.shutdown(),
            RouteInfoManagerWrapper::V2(manager) => manager.shutdown(),
        }
    }

    /// Shutdown unregister service (for bootstrap)
    pub fn shutdown_unregister_service(&mut self) {
        match self {
            RouteInfoManagerWrapper::V1(manager) => manager.un_register_service.shutdown(),
            RouteInfoManagerWrapper::V2(manager) => {
                // V2's shutdown already handles this
                manager.shutdown();
            }
        }
    }

    /// Scan for inactive brokers and clean them up
    ///
    /// Returns the number of brokers that were cleaned up
    pub fn scan_not_active_broker(&mut self) -> usize {
        match self {
            RouteInfoManagerWrapper::V1(manager) => {
                manager.scan_not_active_broker();
                0 // V1 doesn't return count
            }
            RouteInfoManagerWrapper::V2(manager) => manager.scan_not_active_broker().unwrap_or(0),
        }
    }

    /// Register a broker with the nameserver
    pub fn register_broker(
        &self,
        cluster_name: CheetahString,
        broker_addr: CheetahString,
        broker_name: CheetahString,
        broker_id: u64,
        ha_server_addr: CheetahString,
        zone_name: Option<CheetahString>,
        timeout_millis: Option<i64>,
        enable_acting_master: Option<bool>,
        topic_config_wrapper: TopicConfigAndMappingSerializeWrapper,
        filter_server_list: Vec<CheetahString>,
        channel: Channel,
    ) -> Option<RegisterBrokerResult> {
        match self {
            RouteInfoManagerWrapper::V1(manager) => manager.register_broker(
                cluster_name,
                broker_addr,
                broker_name,
                broker_id,
                ha_server_addr,
                zone_name,
                timeout_millis,
                enable_acting_master,
                topic_config_wrapper,
                filter_server_list,
                channel,
            ),
            RouteInfoManagerWrapper::V2(manager) => manager
                .register_broker(
                    cluster_name,
                    broker_addr,
                    broker_name,
                    broker_id,
                    ha_server_addr,
                    zone_name,
                    timeout_millis.map(|t| t as u64),
                    enable_acting_master,
                    topic_config_wrapper,
                    filter_server_list,
                    channel,
                )
                .ok(),
        }
    }

    /// Submit unregister broker request
    pub fn submit_unregister_broker_request(&self, request: UnRegisterBrokerRequestHeader) -> bool {
        match self {
            RouteInfoManagerWrapper::V1(manager) => manager.submit_unregister_broker_request(request),
            RouteInfoManagerWrapper::V2(manager) => manager.submit_unregister_broker_request(request),
        }
    }

    /// Unregister a broker
    pub fn unregister_broker(
        &mut self,
        cluster_name: CheetahString,
        broker_addr: CheetahString,
        broker_name: CheetahString,
        broker_id: u64,
    ) -> RouteResult<()> {
        use rocketmq_remoting::protocol::header::namesrv::broker_request::UnRegisterBrokerRequestHeader;

        match self {
            RouteInfoManagerWrapper::V1(manager) => {
                // V1 uses batch unregister
                let header = UnRegisterBrokerRequestHeader {
                    cluster_name,
                    broker_addr,
                    broker_name,
                    broker_id,
                };
                manager.un_register_broker(vec![header]);
                Ok(())
            }
            RouteInfoManagerWrapper::V2(manager) => {
                manager.unregister_broker(cluster_name, broker_addr, broker_name, broker_id)
            }
        }
    }

    /// Pickup topic route data
    pub fn pickup_topic_route_data(&self, topic: &CheetahString) -> Option<TopicRouteData> {
        match self {
            RouteInfoManagerWrapper::V1(manager) => manager.pickup_topic_route_data(topic),
            RouteInfoManagerWrapper::V2(manager) => manager.pickup_topic_route_data(topic).ok(),
        }
    }

    /// Check if broker topic config changed
    pub fn is_broker_topic_config_changed(
        &self,
        cluster_name: &CheetahString,
        broker_addr: &CheetahString,
        data_version: &DataVersion,
    ) -> bool {
        match self {
            RouteInfoManagerWrapper::V1(manager) => {
                manager.is_broker_topic_config_changed(cluster_name, broker_addr, data_version)
            }
            RouteInfoManagerWrapper::V2(manager) => {
                manager.is_broker_topic_config_changed(cluster_name, broker_addr, data_version)
            }
        }
    }

    /// Update broker info update timestamp
    pub fn update_broker_info_update_timestamp(&mut self, cluster_name: CheetahString, broker_addr: CheetahString) {
        match self {
            RouteInfoManagerWrapper::V1(manager) => {
                manager.update_broker_info_update_timestamp(cluster_name, broker_addr)
            }
            RouteInfoManagerWrapper::V2(manager) => {
                manager.update_broker_info_update_timestamp(cluster_name, broker_addr)
            }
        }
    }

    /// Query broker topic config
    pub fn query_broker_topic_config(
        &self,
        cluster_name: CheetahString,
        broker_addr: CheetahString,
    ) -> Option<DataVersion> {
        match self {
            RouteInfoManagerWrapper::V1(manager) => {
                manager.query_broker_topic_config(cluster_name, broker_addr).cloned()
            }
            RouteInfoManagerWrapper::V2(manager) => manager.query_broker_topic_config(cluster_name, broker_addr),
        }
    }

    /// Get all cluster info
    pub fn get_all_cluster_info(&self) -> ClusterInfo {
        match self {
            RouteInfoManagerWrapper::V1(manager) => manager.get_all_cluster_info(),
            RouteInfoManagerWrapper::V2(manager) => manager
                .get_all_cluster_info()
                .unwrap_or_else(|_| ClusterInfo::default()),
        }
    }

    /// Wipe write permission of broker by lock
    pub fn wipe_write_perm_of_broker_by_lock(&self, broker_name: &str) -> i32 {
        use cheetah_string::CheetahString;
        match self {
            RouteInfoManagerWrapper::V1(manager) => {
                manager.wipe_write_perm_of_broker_by_lock(&CheetahString::from_string(broker_name.to_string()))
            }
            RouteInfoManagerWrapper::V2(manager) => manager
                .wipe_write_perm_of_broker_by_lock(broker_name.to_string())
                .unwrap_or(0),
        }
    }

    /// Add write permission of broker by lock
    pub fn add_write_perm_of_broker_by_lock(&self, broker_name: &str) -> i32 {
        use cheetah_string::CheetahString;
        match self {
            RouteInfoManagerWrapper::V1(manager) => {
                manager.add_write_perm_of_broker_by_lock(&CheetahString::from_string(broker_name.to_string()))
            }
            RouteInfoManagerWrapper::V2(manager) => manager
                .add_write_perm_of_broker_by_lock(broker_name.to_string())
                .unwrap_or(0),
        }
    }

    /// Get broker member group
    pub fn get_broker_member_group(
        &mut self,
        cluster_name: CheetahString,
        broker_name: CheetahString,
    ) -> Option<BrokerMemberGroup> {
        match self {
            RouteInfoManagerWrapper::V1(manager) => manager.get_broker_member_group(cluster_name, broker_name),
            RouteInfoManagerWrapper::V2(manager) => manager.get_broker_member_group(cluster_name, broker_name),
        }
    }

    /// Get all topic list
    pub fn get_all_topic_list(&self) -> TopicList {
        match self {
            RouteInfoManagerWrapper::V1(manager) => manager.get_all_topic_list(),
            RouteInfoManagerWrapper::V2(manager) => {
                let topics = manager.get_all_topics();
                TopicList {
                    topic_list: topics,
                    broker_addr: None,
                }
            }
        }
    }

    /// Delete topic
    pub fn delete_topic(&mut self, topic: CheetahString, cluster_name: Option<CheetahString>) {
        match self {
            RouteInfoManagerWrapper::V1(manager) => {
                manager.delete_topic(topic, cluster_name);
            }
            RouteInfoManagerWrapper::V2(manager) => {
                manager.delete_topic(topic, cluster_name);
            }
        }
    }

    /// Register topic
    pub fn register_topic(
        &self,
        topic: CheetahString,
        queue_data_vec: Vec<rocketmq_remoting::protocol::route::route_data_view::QueueData>,
    ) {
        match self {
            RouteInfoManagerWrapper::V1(manager) => {
                manager.register_topic(topic, queue_data_vec);
            }
            RouteInfoManagerWrapper::V2(manager) => {
                manager.register_topic(topic, queue_data_vec);
            }
        }
    }

    /// Get system topic list
    pub fn get_system_topic_list(&self) -> TopicList {
        match self {
            RouteInfoManagerWrapper::V1(manager) => manager.get_system_topic_list(),
            RouteInfoManagerWrapper::V2(manager) => manager.get_system_topic_list().unwrap_or_else(|_| TopicList {
                topic_list: vec![],
                broker_addr: None,
            }),
        }
    }

    /// Get topics by cluster
    pub fn get_topics_by_cluster(&self, cluster: &CheetahString) -> TopicList {
        match self {
            RouteInfoManagerWrapper::V1(manager) => manager.get_topics_by_cluster(cluster),
            RouteInfoManagerWrapper::V2(manager) => {
                let topics = manager.get_topics_by_cluster(cluster).unwrap_or_else(|_| vec![]);
                TopicList {
                    topic_list: topics,
                    broker_addr: None,
                }
            }
        }
    }

    /// Get unit topics
    pub fn get_unit_topics(&self) -> TopicList {
        match self {
            RouteInfoManagerWrapper::V1(manager) => manager.get_unit_topics(),
            RouteInfoManagerWrapper::V2(manager) => manager.get_unit_topics().unwrap_or_else(|_| TopicList {
                topic_list: vec![],
                broker_addr: None,
            }),
        }
    }

    /// Get has unit sub topic list
    pub fn get_has_unit_sub_topic_list(&self) -> TopicList {
        match self {
            RouteInfoManagerWrapper::V1(manager) => manager.get_has_unit_sub_topic_list(),
            RouteInfoManagerWrapper::V2(manager) => {
                manager.get_has_unit_sub_topic_list().unwrap_or_else(|_| TopicList {
                    topic_list: vec![],
                    broker_addr: None,
                })
            }
        }
    }

    /// Get has unit sub un unit topic list
    pub fn get_has_unit_sub_un_unit_topic_list(&self) -> TopicList {
        match self {
            RouteInfoManagerWrapper::V1(manager) => manager.get_has_unit_sub_un_unit_topic_list(),
            RouteInfoManagerWrapper::V2(manager) => {
                manager
                    .get_has_unit_sub_ununit_topic_list()
                    .unwrap_or_else(|_| TopicList {
                        topic_list: vec![],
                        broker_addr: None,
                    })
            }
        }
    }

    /// Unregister broker (internal method)
    pub fn un_register_broker(&mut self, requests: Vec<UnRegisterBrokerRequestHeader>) {
        match self {
            RouteInfoManagerWrapper::V1(manager) => {
                manager.un_register_broker(requests);
            }
            RouteInfoManagerWrapper::V2(manager) => {
                manager.un_register_broker(requests);
            }
        }
    }
}
