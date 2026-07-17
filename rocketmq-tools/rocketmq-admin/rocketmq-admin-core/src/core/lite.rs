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

#[cfg(feature = "legacy-common-compat")]
pub use crate::client_adapter::legacy::core::lite::*;
