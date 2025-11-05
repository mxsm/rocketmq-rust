/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Common trait for RouteInfoManager implementations
//!
//! This trait provides a unified interface for both v1 and v2 implementations
//! of RouteInfoManager, allowing seamless migration and testing.

use cheetah_string::CheetahString;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
use rocketmq_remoting::protocol::header::namesrv::broker_request::UnRegisterBrokerRequestHeader;
use rocketmq_remoting::protocol::namesrv::RegisterBrokerResult;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;

use crate::route::error::RouteResult;

/// Common interface for RouteInfoManager implementations
pub trait IRouteInfoManager: Send + Sync {
    /// Register a broker with the name server
    fn register_broker(
        &self,
        cluster_name: CheetahString,
        broker_addr: CheetahString,
        broker_name: CheetahString,
        broker_id: u64,
        ha_server_addr: CheetahString,
        zone_name: Option<CheetahString>,
        timeout_millis: Option<u64>,
        enable_acting_master: Option<bool>,
        topic_config_wrapper: TopicConfigAndMappingSerializeWrapper,
        filter_server_list: Vec<CheetahString>,
        channel: Channel,
    ) -> RouteResult<RegisterBrokerResult>;

    /// Submit broker unregistration request (batched)
    fn submit_unregister_broker_request(&self, request: UnRegisterBrokerRequestHeader) -> bool;

    /// Unregister a broker from the name server
    fn unregister_broker(
        &self,
        cluster_name: &str,
        broker_addr: &str,
        broker_name: &str,
        broker_id: u64,
    ) -> RouteResult<()>;

    /// Get topic route data
    fn pickup_topic_route_data(&self, topic: &str) -> RouteResult<TopicRouteData>;

    /// Scan for inactive brokers and remove them
    fn scan_not_active_broker(&self) -> RouteResult<usize>;

    /// Get all topics
    fn get_all_topics(&self) -> Vec<String>;

    /// Get topics by cluster
    fn get_topics_by_cluster(&self, cluster_name: &str) -> RouteResult<Vec<String>>;

    /// Start the route manager
    fn start(&self);

    /// Shutdown the route manager
    fn shutdown(&self);
}
