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

//! Topic-related types and data structures

use std::collections::HashSet;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

/// Topic creation/update request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfig {
    pub topic_name: CheetahString,
    pub read_queue_nums: i32,
    pub write_queue_nums: i32,
    pub perm: i32,
    pub topic_filter_type: Option<String>,
    pub topic_sys_flag: Option<i32>,
    pub order: bool,
}

/// Topic route information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicRouteInfo {
    pub topic_name: CheetahString,
    pub broker_datas: Vec<BrokerData>,
    pub queue_datas: Vec<QueueData>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerData {
    pub cluster: CheetahString,
    pub broker_name: CheetahString,
    pub broker_addrs: std::collections::HashMap<i64, CheetahString>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueData {
    pub broker_name: CheetahString,
    pub read_queue_nums: i32,
    pub write_queue_nums: i32,
    pub perm: i32,
}

/// Topic cluster list
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicClusterList {
    pub clusters: HashSet<CheetahString>,
}

/// Topic status information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicStatus {
    pub topic_name: CheetahString,
    pub broker_offset: i64,
    pub consumer_offset: i64,
    pub last_update_timestamp: i64,
}

/// Target for topic creation/update operations
#[derive(Debug, Clone)]
pub enum TopicTarget {
    /// Create/update on specific broker
    Broker(CheetahString),
    /// Create/update on all master brokers in cluster
    Cluster(CheetahString),
}
