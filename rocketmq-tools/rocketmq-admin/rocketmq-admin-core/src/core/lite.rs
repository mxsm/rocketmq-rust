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

//! Lite-message administration contracts.

use serde::Deserialize;
use serde::Serialize;

use crate::core::error::required;
use crate::core::AdminFuture;
use crate::core::AdminResult;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetLiteBrokerInfoRequest {
    pub broker_addr: String,
}

impl GetLiteBrokerInfoRequest {
    pub fn try_new(broker_addr: impl Into<String>) -> AdminResult<Self> {
        Ok(Self {
            broker_addr: required("brokerAddr", broker_addr)?,
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiteBrokerInfo {
    pub store_type: Option<String>,
    pub max_lmq_num: i32,
    pub current_lmq_num: i32,
    pub lite_subscription_count: i32,
    pub order_info_count: i32,
    pub consume_queue_count: i32,
    pub offset_count: i32,
    pub event_count: i32,
}

pub trait LiteAdmin: Send {
    fn get_lite_broker_info<'a>(&'a mut self, request: &'a GetLiteBrokerInfoRequest)
        -> AdminFuture<'a, LiteBrokerInfo>;
}

/// Maximum Lite Topics retained by one Broker observation.
pub const MAX_LITE_TOPICS: usize = 100;
/// Maximum bound Consumer Groups retained for one Lite Topic.
pub const MAX_LITE_TOPIC_GROUPS: usize = 8;

/// Selects one Broker by its logical name. It carries no address.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiteBrokerQueryRequest {
    pub cluster: String,
    pub broker_name: String,
}

impl LiteBrokerQueryRequest {
    pub fn try_new(cluster: impl Into<String>, broker_name: impl Into<String>) -> AdminResult<Self> {
        Ok(Self {
            cluster: required("cluster", cluster)?,
            broker_name: required("broker_name", broker_name)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiteTopicObservation {
    pub topic: String,
    /// How long an idle Lite Topic is kept, in minutes; `-1` when no expiration is configured.
    pub expiration_minutes: i32,
    /// How many Consumer Groups are bound to the Topic, before the retained-name cap.
    pub bound_group_count: usize,
    /// The bound Consumer Groups in ascending order, at most [`MAX_LITE_TOPIC_GROUPS`].
    pub bound_groups: Vec<String>,
}

/// What the master of one Broker holds for Lite Topics: its limits, its counters and the Lite
/// Topics it knows. It carries no address.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiteBrokerObservation {
    pub broker_name: String,
    pub store_type: Option<String>,
    pub max_lmq_num: i32,
    pub current_lmq_num: i32,
    pub lite_subscription_count: i32,
    pub order_info_count: i32,
    pub consume_queue_count: i32,
    pub offset_count: i32,
    pub event_count: i32,
    /// How many Lite Topics the Broker knows, before the retained-row cap.
    pub lite_topic_count: usize,
    /// The Lite Topics in ascending order of name, at most [`MAX_LITE_TOPICS`].
    pub lite_topics: Vec<LiteTopicObservation>,
    /// Whether Topics or bound groups beyond a retained cap were left out.
    pub truncated: bool,
}

/// Read of the Lite Topic state of one Broker, selected by its logical name.
pub trait LiteBrokerQueryAdmin: Send {
    fn query_lite_broker_info<'a>(
        &'a mut self,
        request: &'a LiteBrokerQueryRequest,
    ) -> AdminFuture<'a, LiteBrokerObservation>;
}
