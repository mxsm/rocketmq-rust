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

//! Broker capability contracts.

use serde::Deserialize;
use serde::Serialize;

use crate::core::error::required;
use crate::core::AdminFuture;
use crate::core::AdminResult;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListBrokersRequest {
    pub cluster: String,
}

impl ListBrokersRequest {
    pub fn try_new(cluster: impl Into<String>) -> AdminResult<Self> {
        Ok(Self {
            cluster: required("cluster", cluster)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BrokerSummary {
    pub cluster: String,
    pub broker_name: String,
    pub broker_id: u64,
    pub broker_addr: String,
    pub version: String,
    pub in_tps: String,
    pub out_tps: String,
    pub timer_progress: String,
    pub page_cache_lock_time_millis: String,
    pub hour: String,
    pub space: String,
    pub broker_active: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ListBrokersResult {
    pub brokers: Vec<BrokerSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeBrokerRuntimeRequest {
    pub cluster: String,
}

impl ProbeBrokerRuntimeRequest {
    pub fn try_new(cluster: impl Into<String>) -> AdminResult<Self> {
        Ok(Self {
            cluster: required("cluster", cluster)?,
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeBrokerRuntimeResult {
    pub attempted: usize,
    pub failures: Vec<String>,
}

pub trait BrokerAdmin: Send {
    fn list_brokers<'a>(&'a mut self, request: &'a ListBrokersRequest) -> AdminFuture<'a, ListBrokersResult>;

    fn probe_broker_runtime<'a>(
        &'a mut self,
        request: &'a ProbeBrokerRuntimeRequest,
    ) -> AdminFuture<'a, ProbeBrokerRuntimeResult>;
}

#[cfg(feature = "legacy-common-compat")]
pub use crate::client_adapter::legacy::core::broker::*;
