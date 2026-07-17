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

//! Consumer capability contracts.

use serde::Deserialize;
use serde::Serialize;

use crate::core::error::required;
use crate::core::AdminFuture;
use crate::core::AdminResult;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListConsumerGroupsRequest;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConsumerGroupSummary {
    pub group: String,
    pub version: i32,
    pub client_count: i32,
    pub consume_type: String,
    pub message_model: String,
    pub consume_tps: f64,
    pub diff_total: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ListConsumerGroupsResult {
    pub groups: Vec<ConsumerGroupSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryConsumerLagRequest {
    pub topic: String,
    pub consumer_group: String,
    pub include_client_ip: bool,
}

impl QueryConsumerLagRequest {
    pub fn try_new(
        topic: impl Into<String>,
        consumer_group: impl Into<String>,
        include_client_ip: bool,
    ) -> AdminResult<Self> {
        Ok(Self {
            topic: required("topic", topic)?,
            consumer_group: required("consumerGroup", consumer_group)?,
            include_client_ip,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerLagRow {
    pub topic: String,
    pub broker_name: String,
    pub queue_id: i32,
    pub broker_offset: i64,
    pub consumer_offset: i64,
    pub lag: i64,
    pub inflight: i64,
    pub last_timestamp: i64,
    pub client_ip: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct QueryConsumerLagResult {
    pub rows: Vec<ConsumerLagRow>,
    pub total_lag: i64,
    pub consume_tps: f64,
    pub inflight_total: i64,
}

pub trait ConsumerAdmin: Send {
    fn list_consumer_groups<'a>(
        &'a mut self,
        request: &'a ListConsumerGroupsRequest,
    ) -> AdminFuture<'a, ListConsumerGroupsResult>;

    fn query_consumer_lag<'a>(
        &'a mut self,
        request: &'a QueryConsumerLagRequest,
    ) -> AdminFuture<'a, QueryConsumerLagResult>;
}

#[cfg(feature = "legacy-common-compat")]
pub use crate::client_adapter::legacy::core::consumer::*;
