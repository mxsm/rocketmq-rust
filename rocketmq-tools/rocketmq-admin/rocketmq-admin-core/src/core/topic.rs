// Copyright 2026 The RocketMQ Rust Authors
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

//! Topic capability contracts.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde::Serialize;

use crate::core::error::required;
use crate::core::AdminFuture;
use crate::core::AdminResult;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListTopicsRequest {
    pub cluster: Option<String>,
}

impl ListTopicsRequest {
    pub fn new(cluster: Option<String>) -> Self {
        Self {
            cluster: cluster.and_then(|value| {
                let value = value.trim().to_string();
                (!value.is_empty()).then_some(value)
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicSummary {
    pub topic: String,
    pub cluster: Option<String>,
    pub consumer_group: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListTopicsResult {
    pub topics: Vec<TopicSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetTopicRouteRequest {
    pub topic: String,
}

impl GetTopicRouteRequest {
    pub fn try_new(topic: impl Into<String>) -> AdminResult<Self> {
        Ok(Self {
            topic: required("topic", topic)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicBroker {
    pub cluster: String,
    pub broker_name: String,
    pub broker_addrs: BTreeMap<u64, String>,
    pub zone_name: Option<String>,
    pub enable_acting_master: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicQueue {
    pub broker_name: String,
    pub read_queue_nums: u32,
    pub write_queue_nums: u32,
    pub perm: u32,
    pub topic_sys_flag: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicRoute {
    pub brokers: Vec<TopicBroker>,
    pub queues: Vec<TopicQueue>,
}

pub trait TopicAdmin: Send {
    fn list_topics<'a>(&'a mut self, request: &'a ListTopicsRequest) -> AdminFuture<'a, ListTopicsResult>;

    fn get_topic_route<'a>(&'a mut self, request: &'a GetTopicRouteRequest) -> AdminFuture<'a, Option<TopicRoute>>;
}

#[cfg(feature = "legacy-common-compat")]
pub use crate::client_adapter::legacy::core::topic::*;
